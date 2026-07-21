//! Build-time discovery of a filtergraph description's open pads WITHOUT
//! initializing the filters.
//!
//! `init_filter_graph` only needs to know which input/output pads a
//! `filter_desc` leaves open (label, media type, display name) so the rest of
//! `build()` can bind them to streams and outputs. The runtime parse in the
//! filter task performs the real, fully initialized configuration later.
//!
//! Going through `avfilter_graph_segment_apply` here would run every filter's
//! `init` at build() time, executing side effects the [`build()` contract]
//! forbids: `movie`/`amovie` open their file, model- and font-loading filters
//! (`arnndn`, `drawtext`, ...) read their resources, and destination-writing
//! filters (`metadata=file=...`, ...) create or truncate their target.
//! Calling `avfilter_graph_segment_link` without init is not possible either:
//! `avfilter_link` rejects uninitialized filters. This module therefore
//! mirrors the linking pass of libavfilter/graphparser.c (`link_inputs`,
//! `link_outputs`, `find_linklabel`; identical in FFmpeg 7.x and 8.x) over the
//! parsed segment, tracking would-be links in a table instead of creating
//! them.
//!
//! Pad counts come from the created (uninitialized) filter contexts, which
//! carry the static pads. Filters with `AVFILTER_FLAG_DYNAMIC_*` pads only
//! get their final pad list in `init`, so the probe initializes those — their
//! init is pure pad/memory work — EXCEPT the ones whose init performs I/O or
//! loads external resources (`movie`, `amovie`, `ladspa`, `lv2`,
//! `libplacebo`). For those the pad count is assumed from the parsed link
//! labels (at least one pad), and the media type is inferred from the filter
//! itself; consumed pads need no type, so this only affects their open pads.
//! An assumption that turns out wrong surfaces as a configuration error from
//! the runtime parse, not as a mis-built graph.
//!
//! [`build()` contract]: crate::core::context::ffmpeg_context_builder::FfmpegContextBuilder::build

use super::LOG_TARGET;
use crate::error::Error::{FilterDescUtf8, FilterNameUtf8};
use crate::error::{FilterGraphParseError, Result};
use ffmpeg_sys_next::AVMediaType::{AVMEDIA_TYPE_AUDIO, AVMEDIA_TYPE_UNKNOWN, AVMEDIA_TYPE_VIDEO};
use ffmpeg_sys_next::{
    av_freep, av_opt_get, avfilter_init_dict, avfilter_pad_get_name, avfilter_pad_get_type,
    AVFilterContext, AVFilterGraphSegment, AVFilterPadParams, AVMediaType,
    AVFILTER_FLAG_DYNAMIC_INPUTS, AVFILTER_FLAG_DYNAMIC_OUTPUTS, AV_OPT_SEARCH_CHILDREN,
};
use log::warn;
use std::ffi::{c_char, c_void, CStr};
use std::ptr::null_mut;

/// One open (unconnected) pad of the parsed graph, in upstream link order.
pub(super) struct ProbedPad {
    /// Link label without brackets, `""` for an unlabeled pad.
    pub(super) linklabel: String,
    pub(super) media_type: AVMediaType,
    /// Display name (`filter` or `filter:pad`), mirroring the naming the
    /// AVFilterInOut-based probe produced.
    pub(super) name: String,
    /// The pad itself, keyed like [`ProbedTopology::edges`] endpoints — lets
    /// consumers run reachability over the mirrored data flow.
    pub(super) pad_ref: PadRef,
}

pub(super) struct ProbedTopology {
    pub(super) inputs: Vec<ProbedPad>,
    pub(super) outputs: Vec<ProbedPad>,
    /// The mirrored data flow at PAD granularity, directed source ->
    /// destination: output-pad -> input-pad edges are the would-be links
    /// between filters, input-pad -> output-pad edges are in-filter routing
    /// (a full crossbar: every input pad of a filter counts as influencing
    /// every output pad, because libavfilter routing is not static — a
    /// selector's `map` can be rewritten mid-stream by `sendcmd`, so no
    /// applied option proves an input can never reach an output). Lets
    /// consumers check directed STRUCTURAL reachability (is an open input
    /// pad wired into the flow that feeds an open output pad?), which weak
    /// component counting cannot answer.
    pub(super) edges: Vec<PadEdge>,
    /// Number of weakly-connected components over the created filters, where
    /// the edges are the links the mirror pass established (labeled and
    /// implicit alike). `1` for every ordinary chain; a `;`-separated
    /// description whose parts share no link label yields one component per
    /// part. Consumers that require a single coherent graph (the writer's
    /// build path) reject anything else; the regular demuxer-driven path
    /// ignores this field, preserving its historical acceptance.
    pub(super) filter_components: usize,
}

/// Dynamic-pad filters whose `init` performs I/O or loads external resources
/// and therefore must not run at build() time. Everything else with dynamic
/// pads does pure pad/memory setup in init (audited across FFmpeg 7.x-8.x).
/// A future filter missing from this list merely keeps today's init-at-build
/// behavior for that one filter.
fn is_init_denied(filter_name: &[u8]) -> bool {
    matches!(
        filter_name,
        b"movie" | b"amovie" | b"ladspa" | b"lv2" | b"libplacebo"
    )
}

/// Media type for a synthetic pad of a never-initialized dynamic direction.
/// `movie`/`amovie` may refine per pad via their `streams` option; the
/// fallback is the filter's only possible (or by far dominant) type.
fn denied_pad_type(
    filter_name: &str,
    spec_types: Option<&[AVMediaType]>,
    pad_idx: usize,
) -> AVMediaType {
    if let Some(t) = spec_types.and_then(|types| types.get(pad_idx)) {
        if *t != AVMEDIA_TYPE_UNKNOWN {
            return *t;
        }
    }
    match filter_name {
        "movie" | "libplacebo" => AVMEDIA_TYPE_VIDEO,
        "amovie" | "ladspa" | "lv2" => AVMEDIA_TYPE_AUDIO,
        _ => AVMEDIA_TYPE_UNKNOWN,
    }
}

/// Media type of one `+`-separated `movie` stream spec: the documented
/// `d[av]N` default-stream shorthand or a generic specifier led by a type
/// character. Anything else (index-only specs, ...) is unknowable without
/// opening the file.
fn spec_media_type(spec: &str) -> AVMediaType {
    let rest = spec.strip_prefix('d').unwrap_or(spec);
    match rest.chars().next() {
        Some('v') => AVMEDIA_TYPE_VIDEO,
        Some('a') => AVMEDIA_TYPE_AUDIO,
        _ => AVMEDIA_TYPE_UNKNOWN,
    }
}

/// Read back `movie`/`amovie`'s applied `streams` option and map each spec to
/// a media type. `None` when unset (the filter then defaults to one stream).
unsafe fn movie_spec_types(f: *mut AVFilterContext) -> Option<Vec<AVMediaType>> {
    let mut out: *mut u8 = null_mut();
    let ret = av_opt_get(
        f as *mut c_void,
        c"streams".as_ptr(),
        AV_OPT_SEARCH_CHILDREN,
        &mut out,
    );
    if ret < 0 || out.is_null() {
        return None;
    }
    let spec = CStr::from_ptr(out as *const c_char)
        .to_str()
        .ok()
        .map(str::to_owned);
    av_freep(&mut out as *mut *mut u8 as *mut c_void);
    let spec = spec?;
    if spec.is_empty() {
        return None;
    }
    Some(spec.split('+').map(spec_media_type).collect())
}

/// Initialize only the filters whose pad topology depends on init (dynamic
/// pads), skipping the side-effectful ones. Options were already applied to
/// the contexts, so `avfilter_init_dict(f, NULL)` sees them — the same call
/// `avfilter_graph_segment_init` would make. Returns the first negative
/// libavfilter error code, mirroring the segment API.
///
/// # Safety
/// `seg` must be a valid segment whose filters were all created
/// (`avfilter_graph_segment_create_filters` succeeded).
pub(super) unsafe fn init_topology_filters(seg: *mut AVFilterGraphSegment) -> i32 {
    for ci in 0..(*seg).nb_chains {
        let ch = *(*seg).chains.add(ci);
        for fi in 0..(*ch).nb_filters {
            let p = *(*ch).filters.add(fi);
            let f = (*p).filter;
            if f.is_null() {
                continue;
            }
            let flags = (*(*f).filter).flags;
            if flags & (AVFILTER_FLAG_DYNAMIC_INPUTS | AVFILTER_FLAG_DYNAMIC_OUTPUTS) == 0 {
                continue;
            }
            if is_init_denied(CStr::from_ptr((*(*f).filter).name).to_bytes()) {
                continue;
            }
            let ret = avfilter_init_dict(f, null_mut());
            if ret < 0 {
                return ret;
            }
        }
    }
    0
}

/// One pad endpoint of the mirrored data-flow graph: pad index `pad` on the
/// input or output side of the filter at `node` (chain, filter) coordinates.
/// The side matters — input pad 0 and output pad 0 of the same filter are
/// distinct endpoints, connected only by that filter's in-filter routing.
#[derive(Clone, Copy, PartialEq, Eq, Hash, Debug)]
pub(super) struct PadRef {
    pub(super) node: (usize, usize),
    pub(super) is_output: bool,
    pub(super) pad: usize,
}

/// A directed data-flow edge between two pad endpoints: frames flow from the
/// first pad into the second. See [`ProbedTopology::edges`] for the two edge
/// kinds (between-filter links and in-filter routing).
pub(super) type PadEdge = (PadRef, PadRef);

fn in_pad(ci: usize, fi: usize, pad: usize) -> PadRef {
    PadRef {
        node: (ci, fi),
        is_output: false,
        pad,
    }
}

fn out_pad(ci: usize, fi: usize, pad: usize) -> PadRef {
    PadRef {
        node: (ci, fi),
        is_output: true,
        pad,
    }
}

/// Link-table mirror of one created filter.
struct NodeState {
    /// Null only for a caller-disabled AVFilterParams entry; such entries are
    /// skipped everywhere, like upstream's `if (!p->filter) continue;`.
    ctx: *mut AVFilterContext,
    filter_name: String,
    /// Parsed `[label]` pad params (upstream `p->inputs`/`p->outputs`); may be
    /// longer than the effective pad count — that is the arity error case.
    in_labels: Vec<Option<String>>,
    out_labels: Vec<Option<String>>,
    /// `false` for a denied dynamic direction whose count is assumed, which
    /// disables the (then meaningless) arity check.
    exact_in: bool,
    exact_out: bool,
    /// Would-be links, indexed by pad; the length is the effective pad count
    /// (upstream `f->nb_inputs`/`f->nb_outputs`).
    in_linked: Vec<bool>,
    out_linked: Vec<bool>,
    /// Per-pad types from `movie`/`amovie`'s `streams` option.
    spec_types: Option<Vec<AVMediaType>>,
}

unsafe fn pad_labels(pads: *mut *mut AVFilterPadParams, nb: u32) -> Result<Vec<Option<String>>> {
    let mut labels = Vec::with_capacity(nb as usize);
    for i in 0..nb as usize {
        let pp = *pads.add(i);
        let label = (*pp).label;
        if label.is_null() {
            labels.push(None);
        } else {
            let s = CStr::from_ptr(label).to_str().map_err(|_| FilterDescUtf8)?;
            labels.push(Some(s.to_string()));
        }
    }
    Ok(labels)
}

unsafe fn build_nodes(seg: *mut AVFilterGraphSegment) -> Result<Vec<Vec<NodeState>>> {
    let mut chains = Vec::with_capacity((*seg).nb_chains);
    for ci in 0..(*seg).nb_chains {
        let ch = *(*seg).chains.add(ci);
        let mut nodes = Vec::with_capacity((*ch).nb_filters);
        for fi in 0..(*ch).nb_filters {
            let p = *(*ch).filters.add(fi);
            let f = (*p).filter;
            if f.is_null() {
                nodes.push(NodeState {
                    ctx: null_mut(),
                    filter_name: String::new(),
                    in_labels: Vec::new(),
                    out_labels: Vec::new(),
                    exact_in: true,
                    exact_out: true,
                    in_linked: Vec::new(),
                    out_linked: Vec::new(),
                    spec_types: None,
                });
                continue;
            }

            let filter_name = CStr::from_ptr((*(*f).filter).name)
                .to_str()
                .map_err(|_| FilterNameUtf8)?
                .to_string();
            let in_labels = pad_labels((*p).inputs, (*p).nb_inputs)?;
            let out_labels = pad_labels((*p).outputs, (*p).nb_outputs)?;

            let flags = (*(*f).filter).flags;
            let denied = is_init_denied(filter_name.as_bytes());
            let real_in = (*f).nb_inputs as usize;
            let real_out = (*f).nb_outputs as usize;

            // A denied dynamic direction never ran init, so its real pad
            // count is unknowable here: assume the parsed labels, with a
            // one-pad minimum (movie's stream list refines the output count).
            // The minimum is a deliberate bet on effect posture: ladspa/lv2
            // effect plugins and libplacebo really do create a first pad, and
            // an unlabeled chain-head effect needs it to bind an input stream
            // (matching the pre-probe build()-time init behavior). Source-mode
            // ladspa/lv2 generator plugins create ZERO input pads, so for them
            // the assumed pad is a fabrication — the runtime parse then leaves
            // that slot's buffersrc null, which fg_send_frame rejects with a
            // typed error. Everything else is exact — static pads exist from
            // creation, dynamic ones from init_topology_filters.
            let (eff_in, exact_in) = if flags & AVFILTER_FLAG_DYNAMIC_INPUTS != 0 && denied {
                (in_labels.len().max(1), false)
            } else {
                (real_in, true)
            };
            let spec_types = if denied && matches!(filter_name.as_str(), "movie" | "amovie") {
                movie_spec_types(f)
            } else {
                None
            };
            let (eff_out, exact_out) = if flags & AVFILTER_FLAG_DYNAMIC_OUTPUTS != 0 && denied {
                let assumed = spec_types.as_ref().map_or(1, |t| t.len().max(1));
                (out_labels.len().max(assumed), false)
            } else {
                (real_out, true)
            };

            nodes.push(NodeState {
                ctx: f,
                filter_name,
                in_labels,
                out_labels,
                exact_in,
                exact_out,
                in_linked: vec![false; eff_in],
                out_linked: vec![false; eff_out],
                spec_types,
            });
        }
        chains.push(nodes);
    }
    Ok(chains)
}

/// Mirror of upstream `find_linklabel`: first unlinked pad labeled `label`,
/// searching forward from `(start_ci, start_fi)` inclusive. `want_outputs`
/// selects the direction searched.
fn find_unlinked_labeled(
    nodes: &[Vec<NodeState>],
    start_ci: usize,
    start_fi: usize,
    want_outputs: bool,
    label: &str,
) -> Option<(usize, usize, usize)> {
    for (ci, chain) in nodes.iter().enumerate().skip(start_ci) {
        let fi0 = if ci == start_ci { start_fi } else { 0 };
        for (fi, n) in chain.iter().enumerate().skip(fi0) {
            if n.ctx.is_null() {
                continue;
            }
            let (labels, linked) = if want_outputs {
                (&n.out_labels, &n.out_linked)
            } else {
                (&n.in_labels, &n.in_linked)
            };
            // Upstream bounds the scan by FFMIN(label params, real pads);
            // pads past the label list are unlabeled and can never match, so
            // bounding by the link table is equivalent.
            for pi in 0..linked.len().min(labels.len()) {
                if !linked[pi] && labels[pi].as_deref() == Some(label) {
                    return Some((ci, fi, pi));
                }
            }
        }
    }
    None
}

/// Media type of a real (created) pad, or `None` for a synthetic pad of a
/// never-initialized (denied) dynamic direction — whose type is unknowable
/// without init.
unsafe fn real_pad_type(n: &NodeState, is_output: bool, pad_idx: usize) -> Option<AVMediaType> {
    let f = n.ctx;
    let (pads, real_nb) = if is_output {
        ((*f).output_pads, (*f).nb_outputs as usize)
    } else {
        ((*f).input_pads, (*f).nb_inputs as usize)
    };
    (pad_idx < real_nb).then(|| avfilter_pad_get_type(pads, pad_idx as i32))
}

/// Mirror `avfilter_link`'s media-type gate (avfilter.c: an output pad may only
/// link to an input pad of the same type; a mismatch returns EINVAL, failing
/// the whole parse). `find_unlinked_labeled` matches on label alone, exactly
/// like upstream `find_linklabel`; this gate is the type check upstream defers
/// to `avfilter_link`, replayed here without initializing the filters. When
/// either pad's type is unknown (a denied dynamic pad), the check is skipped —
/// such a graph can only be fully validated once its filters init at runtime,
/// consistent with the rest of the denied-filter handling.
unsafe fn check_link_media_types(
    nodes: &[Vec<NodeState>],
    out: (usize, usize, usize),
    inp: (usize, usize, usize),
) -> Result<()> {
    let ot = real_pad_type(&nodes[out.0][out.1], true, out.2);
    let it = real_pad_type(&nodes[inp.0][inp.1], false, inp.2);
    if let (Some(ot), Some(it)) = (ot, it) {
        if ot != it {
            warn!(target: LOG_TARGET,
                "Media type mismatch between the '{}' filter output pad {} and the '{}' filter input pad {}",
                nodes[out.0][out.1].filter_name, out.2, nodes[inp.0][inp.1].filter_name, inp.2
            );
            return Err(FilterGraphParseError::InvalidArgument.into());
        }
    }
    Ok(())
}

unsafe fn describe_pad(
    n: &NodeState,
    node: (usize, usize),
    is_output: bool,
    pad_idx: usize,
    label: Option<String>,
) -> Result<ProbedPad> {
    let f = n.ctx;
    let (pads, real_nb) = if is_output {
        ((*f).output_pads, (*f).nb_outputs as usize)
    } else {
        ((*f).input_pads, (*f).nb_inputs as usize)
    };
    let (media_type, name) = if pad_idx < real_nb {
        let media_type = avfilter_pad_get_type(pads, pad_idx as i32);
        let name = if real_nb > 1 {
            n.filter_name.clone()
        } else {
            let pad_name = CStr::from_ptr(avfilter_pad_get_name(pads, pad_idx as i32))
                .to_str()
                .map_err(|_| FilterNameUtf8)?;
            format!("{}:{}", n.filter_name, pad_name)
        };
        (media_type, name)
    } else {
        // Synthetic pad of a never-initialized dynamic direction: no real pad
        // to query, so infer the type and use the bare filter name.
        (
            denied_pad_type(&n.filter_name, n.spec_types.as_deref(), pad_idx),
            n.filter_name.clone(),
        )
    };
    Ok(ProbedPad {
        linklabel: label.unwrap_or_default(),
        media_type,
        name,
        pad_ref: PadRef {
            node,
            is_output,
            pad: pad_idx,
        },
    })
}

fn link_inputs_mirror(
    nodes: &mut [Vec<NodeState>],
    ci: usize,
    fi: usize,
    open: &mut Vec<ProbedPad>,
    edges: &mut Vec<PadEdge>,
) -> Result<()> {
    let (eff, exact, nb_labels) = {
        let n = &nodes[ci][fi];
        (n.in_linked.len(), n.exact_in, n.in_labels.len())
    };
    if exact && eff < nb_labels {
        warn!(target: LOG_TARGET,
            "More input link labels specified for filter '{}' than it has inputs: {} > {}",
            nodes[ci][fi].filter_name, nb_labels, eff
        );
        return Err(FilterGraphParseError::InvalidArgument.into());
    }
    for pi in 0..eff {
        if nodes[ci][fi].in_linked[pi] {
            continue;
        }
        let label = nodes[ci][fi].in_labels.get(pi).cloned().flatten();
        if let Some(lab) = label.as_deref() {
            if let Some((cj, fj, pj)) = find_unlinked_labeled(nodes, ci, fi, true, lab) {
                // Producer = fj's output pad pj, consumer = fi's input pad pi.
                unsafe { check_link_media_types(nodes, (cj, fj, pj), (ci, fi, pi)) }?;
                nodes[cj][fj].out_linked[pj] = true;
                nodes[ci][fi].in_linked[pi] = true;
                edges.push((out_pad(cj, fj, pj), in_pad(ci, fi, pi)));
                continue;
            }
        }
        open.push(unsafe { describe_pad(&nodes[ci][fi], (ci, fi), false, pi, label) }?);
    }
    Ok(())
}

fn link_outputs_mirror(
    nodes: &mut [Vec<NodeState>],
    ci: usize,
    fi: usize,
    open: &mut Vec<ProbedPad>,
    edges: &mut Vec<PadEdge>,
) -> Result<()> {
    let (eff, exact, nb_labels) = {
        let n = &nodes[ci][fi];
        (n.out_linked.len(), n.exact_out, n.out_labels.len())
    };
    if exact && eff < nb_labels {
        warn!(target: LOG_TARGET,
            "More output link labels specified for filter '{}' than it has outputs: {} > {}",
            nodes[ci][fi].filter_name, nb_labels, eff
        );
        return Err(FilterGraphParseError::InvalidArgument.into());
    }
    'pads: for pi in 0..eff {
        if nodes[ci][fi].out_linked[pi] {
            continue;
        }
        let label = nodes[ci][fi].out_labels.get(pi).cloned().flatten();
        if let Some(lab) = label.as_deref() {
            if let Some((cj, fj, pj)) = find_unlinked_labeled(nodes, ci, fi, false, lab) {
                // Producer = fi's output pad pi, consumer = fj's input pad pj.
                unsafe { check_link_media_types(nodes, (ci, fi, pi), (cj, fj, pj)) }?;
                nodes[cj][fj].in_linked[pj] = true;
                nodes[ci][fi].out_linked[pi] = true;
                edges.push((out_pad(ci, fi, pi), in_pad(cj, fj, pj)));
                continue 'pads;
            }
        } else {
            // Unlabeled output: implicit link to the first unlinked,
            // unlabeled input of the NEXT created filter in this chain.
            // Upstream tries only that first candidate (`break` after it).
            for nfi in fi + 1..nodes[ci].len() {
                if nodes[ci][nfi].ctx.is_null() {
                    continue;
                }
                let target = {
                    let cand = &nodes[ci][nfi];
                    (0..cand.in_linked.len()).find(|&pj| {
                        !cand.in_linked[pj] && cand.in_labels.get(pj).is_none_or(|l| l.is_none())
                    })
                };
                if let Some(pj) = target {
                    // Upstream links this first candidate unconditionally and
                    // propagates avfilter_link's error; the type gate is that
                    // error replayed (producer = fi:pi, consumer = nfi:pj).
                    unsafe { check_link_media_types(nodes, (ci, fi, pi), (ci, nfi, pj)) }?;
                    nodes[ci][nfi].in_linked[pj] = true;
                    nodes[ci][fi].out_linked[pi] = true;
                    edges.push((out_pad(ci, fi, pi), in_pad(ci, nfi, pj)));
                    continue 'pads;
                }
                break;
            }
        }
        open.push(unsafe { describe_pad(&nodes[ci][fi], (ci, fi), true, pi, label) }?);
    }
    Ok(())
}

/// Compute the open input/output pads the graph description leaves
/// unconnected, in the exact order `avfilter_graph_segment_link` would return
/// them, without initializing (and thus without running the side effects of)
/// the filters.
///
/// # Safety
/// `seg` must be a valid segment whose filters were created, with options
/// applied and [`init_topology_filters`] run.
pub(super) unsafe fn probe_open_pads(seg: *mut AVFilterGraphSegment) -> Result<ProbedTopology> {
    let mut nodes = build_nodes(seg)?;
    let mut inputs = Vec::new();
    let mut outputs = Vec::new();
    let mut edges = Vec::new();
    for ci in 0..nodes.len() {
        for fi in 0..nodes[ci].len() {
            if nodes[ci][fi].ctx.is_null() {
                continue;
            }
            link_inputs_mirror(&mut nodes, ci, fi, &mut inputs, &mut edges)?;
            link_outputs_mirror(&mut nodes, ci, fi, &mut outputs, &mut edges)?;
        }
    }
    // In-filter routing edges, added after the links so `edges` describes the
    // complete pad-level data flow: every input pad of a filter is treated as
    // influencing every output pad (a full crossbar). This deliberately
    // includes filters whose CURRENT options would drop an input: a
    // `streamselect=map=0` graph relays only pad 0 today, but `map` is a
    // runtime command (`sendcmd`/`avfilter_graph_send_command` rewrite it
    // mid-stream, after which the other pad feeds the output) and ffmpeg
    // accepts such descriptions as-is — verified against ffmpeg 7.1.3, which
    // encodes "color[bg];[bg][in]streamselect=inputs=2:map=0" without
    // complaint and honors a later `map 1` command. Pruning by the applied
    // map would therefore reject valid graphs whose dropped input can still
    // reach the output at runtime. The crossbar keeps the walk structural: it
    // answers "is the pad wired into the flow that feeds the output?", never
    // "will frames survive the trip?".
    for (ci, chain) in nodes.iter().enumerate() {
        for (fi, n) in chain.iter().enumerate() {
            if n.ctx.is_null() {
                continue;
            }
            for pj in 0..n.out_linked.len() {
                for pi in 0..n.in_linked.len() {
                    edges.push((in_pad(ci, fi, pi), out_pad(ci, fi, pj)));
                }
            }
        }
    }
    let filter_components = count_components(&nodes, &edges);
    Ok(ProbedTopology {
        inputs,
        outputs,
        edges,
        filter_components,
    })
}

/// Weakly-connected components of the created filters under the mirrored
/// links, via a plain union-find over dense node ids. Skipped (null-ctx)
/// entries are not nodes. In-filter routing edges connect two pads of the
/// same node, so under the node-granular union they are no-ops and the count
/// stays the pure link-connectivity the field documents.
fn count_components(nodes: &[Vec<NodeState>], edges: &[PadEdge]) -> usize {
    // Dense id per created filter.
    let mut id = std::collections::HashMap::new();
    for (ci, chain) in nodes.iter().enumerate() {
        for (fi, n) in chain.iter().enumerate() {
            if !n.ctx.is_null() {
                let next = id.len();
                id.insert((ci, fi), next);
            }
        }
    }
    let mut parent: Vec<usize> = (0..id.len()).collect();
    fn find(parent: &mut [usize], mut x: usize) -> usize {
        while parent[x] != x {
            parent[x] = parent[parent[x]];
            x = parent[x];
        }
        x
    }
    for (a, b) in edges {
        let (Some(&ia), Some(&ib)) = (id.get(&a.node), id.get(&b.node)) else {
            continue;
        };
        let (ra, rb) = (find(&mut parent, ia), find(&mut parent, ib));
        if ra != rb {
            parent[ra] = rb;
        }
    }
    (0..parent.len())
        .filter(|&i| find(&mut parent, i) == i)
        .count()
}

#[cfg(test)]
mod tests {
    //! Differential tests: for side-effect-free graphs the probe must produce
    //! the same open pads — labels, media types, names, order — as the real
    //! `avfilter_graph_segment_init` + `avfilter_graph_segment_link` pass of
    //! the linked FFmpeg (7.x or 8.x). This pins the mirror to whatever
    //! graphparser semantics the build actually links.

    use super::*;
    use crate::core::scheduler::filter_task::graph_opts_apply;
    use ffmpeg_sys_next::{
        avfilter_get_by_name, avfilter_graph_segment_create_filters, avfilter_graph_segment_free,
        avfilter_graph_segment_init, avfilter_graph_segment_link, avfilter_graph_segment_parse,
        AVFilterInOut,
    };
    use std::ffi::CString;

    type PadSig = (String, AVMediaType, String);

    fn sigs(pads: &[ProbedPad]) -> Vec<PadSig> {
        pads.iter()
            .map(|p| (p.linklabel.clone(), p.media_type, p.name.clone()))
            .collect()
    }

    /// Parse + create + apply opts, shared by both paths. Returns the graph
    /// owner (frees every filter ctx on drop) and the raw segment (caller
    /// frees).
    unsafe fn parse_and_create(
        desc: &CString,
    ) -> Result<(crate::raw::FilterGraph, *mut AVFilterGraphSegment), i32> {
        let graph = crate::raw::FilterGraph::alloc().expect("graph alloc");
        (*graph.as_ptr()).nb_threads = 1;
        let mut seg = null_mut();
        let mut ret = avfilter_graph_segment_parse(graph.as_ptr(), desc.as_ptr(), 0, &mut seg);
        if ret < 0 {
            return Err(ret);
        }
        ret = avfilter_graph_segment_create_filters(seg, 0);
        if ret >= 0 {
            ret = graph_opts_apply(seg);
        }
        if ret < 0 {
            avfilter_graph_segment_free(&mut seg);
            return Err(ret);
        }
        Ok((graph, seg))
    }

    /// The upstream reference: real init + real link, converted with the same
    /// naming rules the probe uses.
    unsafe fn upstream_pads(desc: &str) -> Result<(Vec<PadSig>, Vec<PadSig>), i32> {
        let desc = CString::new(desc).unwrap();
        let (_graph, mut seg) = parse_and_create(&desc)?;
        let mut ret = avfilter_graph_segment_init(seg, 0);
        if ret < 0 {
            avfilter_graph_segment_free(&mut seg);
            return Err(ret);
        }
        let mut inputs = crate::raw::FilterInOut::empty();
        let mut outputs = crate::raw::FilterInOut::empty();
        ret = avfilter_graph_segment_link(seg, 0, inputs.as_out_ptr(), outputs.as_out_ptr());
        avfilter_graph_segment_free(&mut seg);
        if ret < 0 {
            return Err(ret);
        }
        Ok((
            inout_sigs(inputs.as_ptr(), false),
            inout_sigs(outputs.as_ptr(), true),
        ))
    }

    unsafe fn inout_sigs(mut cur: *mut AVFilterInOut, is_output: bool) -> Vec<PadSig> {
        let mut sigs = Vec::new();
        while !cur.is_null() {
            let label = if (*cur).name.is_null() {
                String::new()
            } else {
                CStr::from_ptr((*cur).name).to_str().unwrap().to_string()
            };
            let f = (*cur).filter_ctx;
            let (pads, nb) = if is_output {
                ((*f).output_pads, (*f).nb_outputs)
            } else {
                ((*f).input_pads, (*f).nb_inputs)
            };
            let media_type = avfilter_pad_get_type(pads, (*cur).pad_idx);
            let fname = CStr::from_ptr((*(*f).filter).name).to_str().unwrap();
            let name = if nb > 1 {
                fname.to_string()
            } else {
                let pad = CStr::from_ptr(avfilter_pad_get_name(pads, (*cur).pad_idx))
                    .to_str()
                    .unwrap();
                format!("{fname}:{pad}")
            };
            sigs.push((label, media_type, name));
            cur = (*cur).next;
        }
        sigs
    }

    /// The probe path exactly as `init_filter_graph` drives it.
    unsafe fn probe_pads(desc: &str) -> std::result::Result<ProbedTopology, String> {
        let desc = CString::new(desc).unwrap();
        let (_graph, mut seg) = parse_and_create(&desc).map_err(|e| format!("pre: {e}"))?;
        let ret = init_topology_filters(seg);
        if ret < 0 {
            avfilter_graph_segment_free(&mut seg);
            return Err(format!("init_topology_filters: {ret}"));
        }
        let topo = probe_open_pads(seg);
        avfilter_graph_segment_free(&mut seg);
        topo.map_err(|e| format!("probe: {e}"))
    }

    #[track_caller]
    fn assert_parity(desc: &str) {
        crate::core::initialize_ffmpeg();
        unsafe {
            let upstream = upstream_pads(desc).unwrap_or_else(|e| {
                panic!("upstream path failed for {desc:?} (averror {e}); fix the test graph")
            });
            let probed = probe_pads(desc)
                .unwrap_or_else(|e| panic!("probe failed for {desc:?} but upstream links: {e}"));
            assert_eq!(
                sigs(&probed.inputs),
                upstream.0,
                "open INPUT pads diverge from graphparser for {desc:?}"
            );
            assert_eq!(
                sigs(&probed.outputs),
                upstream.1,
                "open OUTPUT pads diverge from graphparser for {desc:?}"
            );
        }
    }

    #[track_caller]
    fn assert_both_reject(desc: &str) {
        crate::core::initialize_ffmpeg();
        unsafe {
            assert!(
                upstream_pads(desc).is_err(),
                "upstream unexpectedly links {desc:?}"
            );
            assert!(
                probe_pads(desc).is_err(),
                "probe accepted {desc:?} which upstream rejects at parse/link"
            );
        }
    }

    #[test]
    fn parity_single_filter_open_both_ends() {
        assert_parity("scale=320:240");
    }

    #[test]
    fn parity_implicit_chain() {
        assert_parity("scale=320:240,hflip");
    }

    #[test]
    fn parity_labeled_passthrough() {
        assert_parity("[in]yadif[out]");
    }

    #[test]
    fn parity_split_multichain_labels() {
        assert_parity("[0:v]split[a][b];[a]hflip[x];[x][b]overlay[out]");
    }

    #[test]
    fn parity_surplus_split_outputs_stay_open() {
        // split=3 leaves two unlabeled surplus outputs after the implicit link.
        assert_parity("split=3,hflip");
    }

    #[test]
    fn parity_amix_dynamic_inputs() {
        assert_parity("[0:a][1:a]amix=inputs=2[mixed]");
    }

    #[test]
    fn parity_concat_dynamic_both_directions() {
        assert_parity("[0:v][0:a][1:v][1:a]concat=n=2:v=1:a=1[v][a]");
    }

    #[test]
    fn parity_forward_cross_chain_label() {
        assert_parity("anull[l];[l]anull");
    }

    #[test]
    fn parity_sws_flags_prefix() {
        assert_parity("sws_flags=bilinear;[0:v]scale=100:100[out]");
    }

    #[test]
    fn parity_channelsplit_dynamic_outputs() {
        assert_parity("channelsplit=channel_layout=stereo");
    }

    #[test]
    fn parity_partially_labeled_dynamic_outputs() {
        // One label on a two-output split: pad 0 carries [a], pad 1 links
        // implicitly to the next filter.
        assert_parity("split[a],hflip;[a]vflip[out]");
    }

    #[test]
    fn arity_too_many_input_labels_rejected_by_both() {
        assert_both_reject("[0:v][1:v]hflip[out]");
    }

    #[test]
    fn arity_too_many_output_labels_rejected_by_both() {
        assert_both_reject("[x]hflip[a][b]");
    }

    #[test]
    fn cross_media_labeled_link_rejected_by_both() {
        // An audio output feeding a video input pad over a shared label:
        // upstream avfilter_link rejects the type mismatch (EINVAL), so the
        // mirror must too, at build() — not defer it to runtime.
        assert_both_reject("[0:a]anull[x];[x]hflip[out]");
    }

    #[test]
    fn cross_media_implicit_link_rejected_by_both() {
        // Same mismatch over an implicit (unlabeled) chain link: anull's audio
        // output implicitly links to hflip's video input.
        assert_both_reject("anull,hflip");
    }

    #[test]
    fn movie_probe_succeeds_without_touching_the_missing_file() {
        // The whole point of the probe: upstream init would avformat_open_input
        // the (missing) file and fail; the probe must succeed and type the
        // graph correctly. The consumed [wm] pad needs no inference.
        crate::core::initialize_ffmpeg();
        let desc = "movie=/nonexistent/ez_probe_fixture.mkv[wm];[base][wm]overlay[out]";
        unsafe {
            assert!(upstream_pads(desc).is_err(), "fixture unexpectedly exists");
            let topo = probe_pads(desc).expect("probe must not need the movie file");
            assert_eq!(
                sigs(&topo.inputs),
                vec![("base".into(), AVMEDIA_TYPE_VIDEO, "overlay".into())]
            );
            assert_eq!(
                sigs(&topo.outputs),
                vec![("out".into(), AVMEDIA_TYPE_VIDEO, "overlay:default".into())]
            );
        }
    }

    #[test]
    fn movie_open_pad_types_follow_the_streams_spec() {
        crate::core::initialize_ffmpeg();
        let desc = "movie=/nonexistent/ez_probe_fixture.mkv:s=dv+da[v][a]";
        unsafe {
            let topo = probe_pads(desc).expect("probe must not need the movie file");
            assert!(topo.inputs.is_empty());
            assert_eq!(
                sigs(&topo.outputs),
                vec![
                    ("v".into(), AVMEDIA_TYPE_VIDEO, "movie".into()),
                    ("a".into(), AVMEDIA_TYPE_AUDIO, "movie".into()),
                ]
            );
        }
    }

    #[test]
    fn amovie_open_pad_defaults_to_audio() {
        crate::core::initialize_ffmpeg();
        let desc = "amovie=/nonexistent/ez_probe_fixture.wav[m]";
        unsafe {
            let topo = probe_pads(desc).expect("probe must not need the amovie file");
            assert_eq!(
                sigs(&topo.outputs),
                vec![("m".into(), AVMEDIA_TYPE_AUDIO, "amovie".into())]
            );
        }
    }

    // Pins the deliberate effect-posture bet for denied dynamic-input
    // filters: an unlabeled chain-head lv2 probes to exactly ONE assumed
    // input pad, so effect plugins keep binding an input stream the way the
    // old build()-time init did. For source-mode (generator) plugins that
    // pad is a fabrication with no runtime buffersrc behind it — that
    // mismatch is caught by fg_send_frame's null-filter guard as a typed
    // error. Changing the assumption here must be a conscious decision.
    #[test]
    fn lv2_chain_head_assumes_exactly_one_input_pad() {
        crate::core::initialize_ffmpeg();
        unsafe {
            let name = CString::new("lv2").unwrap();
            if avfilter_get_by_name(name.as_ptr()).is_null() {
                return; // this FFmpeg build has no lv2 filter (optional lilv dependency)
            }
            let topo = probe_pads("lv2=plugin=x[out]")
                .expect("probe must not load the (bogus) LV2 plugin");
            assert_eq!(topo.inputs.len(), 1, "one assumed input pad, no more");
            assert_eq!(topo.inputs[0].name, "lv2");
            assert_eq!(topo.inputs[0].media_type, AVMEDIA_TYPE_AUDIO);
            assert_eq!(
                sigs(&topo.outputs),
                vec![("out".into(), AVMEDIA_TYPE_AUDIO, "lv2".into())]
            );
        }
    }
}
