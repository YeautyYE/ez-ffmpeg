//! Shared HDR-to-SDR routing and chain assembly for the `hdr_to_sdr` example.
//!
//! Included via `#[path = ...] mod routing;` by both the example binary and the
//! integration tests, so the routing decision has a single source of truth and
//! stays testable WITHOUT exposing a public tone-mapping API — this file is not
//! part of the crate's public surface.
#![allow(dead_code)] // each includer (example / tests) uses a different subset

use std::sync::OnceLock;

use ez_ffmpeg::capabilities::{is_filter_available, is_filter_option_available};
use ez_ffmpeg::{FfmpegContext, Input, Output};
use ffmpeg_sys_next::{AVColorPrimaries, AVColorSpace, AVColorTransferCharacteristic};

/// How the input's color should be treated, decided on the transfer axis.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ColorKind {
    /// PQ (SMPTE ST 2084) — HDR10/HDR10+/Dolby-Vision base layer. Tone-map.
    Pq,
    /// HLG (ARIB STD-B67) — broadcast / phone HDR. Tone-map (same chain, with
    /// the caveat that HLG's ideal curve may differ; see the module docs).
    Hlg,
    /// BT.2020 gamut but an SDR transfer: NOT HDR. Only the gamut needs
    /// converting; tone-mapping it would wrongly darken the picture.
    WideGamutSdr,
    /// Ordinary SDR (BT.709 or smaller). Nothing to convert.
    Sdr,
    /// BT.2020 primaries/matrix with an unspecified transfer. Could be missing-tag
    /// PQ/HLG or wide-gamut SDR; the cookbook will not guess.
    AmbiguousHdr,
}

/// Which conversion backend to build the chain for.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Backend {
    /// GPU: `libplacebo` (needs a Vulkan-capable FFmpeg *and* a runtime ICD).
    Libplacebo,
    /// CPU: `zscale` (+ `tonemap` for HDR). Needs a libzimg-enabled FFmpeg.
    Zscale,
    /// FFmpeg 8 libswscale perceptual mapping. Requires the `scale` options
    /// `out_primaries`, `out_transfer`, and child option `intent` — not just
    /// the `scale` filter name, which also exists in FFmpeg 7.1.
    SwscalePerceptual,
    /// Built-in `colorspace` gamut conversion. For wide-gamut SDR only; it
    /// cannot linearize PQ/HLG (FFmpeg 7.1 has no PQ/HLG EOTF in this filter).
    Colorspace,
}

/// How to order backend candidates. Kept example-local so the order can still
/// change without a public recipe API.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum RoutingPolicy {
    /// Preserve the historical example default: zscale first, then libplacebo,
    /// then the FFmpeg 8 `scale` fallback (or `colorspace` for wide-gamut SDR).
    Compatible,
    /// Prefer libplacebo when the user asked for it (`libplacebo` CLI argument).
    QualityFirst,
    /// Use exactly this backend or fail; never silently substitute.
    Exact(Backend),
}

/// Static filter/option bits used by the router. wgpu adapter readiness is
/// deliberately not in this set: wgpu is 8-bit BT.601/709 only and is never
/// an HDR auto-route candidate.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub struct HdrFilterCapabilities {
    pub has_libplacebo: bool,
    pub has_zscale: bool,
    pub has_tonemap: bool,
    pub has_colorspace: bool,
    pub has_swscale_perceptual: bool,
}

/// One backend considered during routing, for error text and tests.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct RouteAttempt {
    pub backend: Backend,
    pub available: bool,
    pub rejection: Option<&'static str>,
}

/// Successful routing result.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct RouteDecision {
    pub selected: Backend,
    pub attempts: Vec<RouteAttempt>,
}

/// FFmpeg 8 built-in HDR→SDR fallback. `intent=perceptual` is the option that
/// actually tone-maps; without it this would be a naive scale (gray output).
pub const SWSCALE_PERCEPTUAL_CHAIN: &str =
    "scale=out_color_matrix=bt709:out_primaries=bt709:out_transfer=bt709:out_range=tv:intent=perceptual,format=yuv420p";

/// Wide-gamut SDR gamut conversion on the built-in `colorspace` filter.
/// Available on FFmpeg 7.1 without zimg; must not be used for PQ/HLG.
pub const COLORSPACE_GAMUT_CHAIN: &str = "colorspace=all=bt709:range=tv:format=yuv420p";

/// Rejection recorded when a backend passes the static registry check but its
/// candidate chain fails the graph preflight (typical case: libplacebo is
/// compiled in but no Vulkan runtime/ICD is present at graph-config time).
pub const PREFLIGHT_REJECTION: &str =
    "filter is compiled in, but a minimal test graph would not configure (Vulkan/runtime missing?)";

/// Peak used when building the chain that is preflighted. The peak value only
/// shapes the tone-mapping curve, not whether the graph configures, so a
/// nominal 1000-nit value is fine here.
const PREFLIGHT_PEAK: u32 = 10;

static HDR_FILTER_CAPS: OnceLock<HdrFilterCapabilities> = OnceLock::new();

/// Classifies the stream's color on the TRANSFER axis first.
///
/// PQ and HLG are HDR. A known SDR transfer with BT.2020 primaries/matrix is
/// wide-gamut SDR (gamut conversion only). Fully untagged `(unspecified,
/// unspecified, unspecified)` is ordinary SDR. An unspecified transfer on a
/// BT.2020 container is [`ColorKind::AmbiguousHdr`]: the cookbook will not
/// guess missing-tag PQ versus wide-gamut SDR.
pub fn classify(color_transfer: i32, color_primaries: i32, color_space: i32) -> ColorKind {
    let pq = AVColorTransferCharacteristic::AVCOL_TRC_SMPTE2084 as i32;
    let hlg = AVColorTransferCharacteristic::AVCOL_TRC_ARIB_STD_B67 as i32;
    if color_transfer == pq {
        return ColorKind::Pq;
    }
    if color_transfer == hlg {
        return ColorKind::Hlg;
    }

    let pri_2020 = AVColorPrimaries::AVCOL_PRI_BT2020 as i32;
    let spc_2020_ncl = AVColorSpace::AVCOL_SPC_BT2020_NCL as i32;
    let spc_2020_cl = AVColorSpace::AVCOL_SPC_BT2020_CL as i32;
    let unspecified = AVColorTransferCharacteristic::AVCOL_TRC_UNSPECIFIED as i32;
    let bt2020_container =
        color_primaries == pri_2020 || color_space == spc_2020_ncl || color_space == spc_2020_cl;

    // Missing transfer on a BT.2020 container is not proof of wide-gamut SDR.
    // Treating it as PQ would wash out a tagged-SDR file; treating it as SDR
    // would skip tone mapping on missing-tag HDR. Fail closed instead.
    if color_transfer == unspecified && bt2020_container {
        return ColorKind::AmbiguousHdr;
    }

    // A known SDR transfer with a BT.2020 gamut: convert the gamut but do NOT
    // tone-map (that is the 34%-too-dark trap).
    if bt2020_container {
        return ColorKind::WideGamutSdr;
    }

    ColorKind::Sdr
}

/// Whether this kind needs a tone-mapping curve (as opposed to a gamut-only
/// conversion). Only PQ and HLG do.
pub fn needs_tone_map(kind: ColorKind) -> bool {
    matches!(kind, ColorKind::Pq | ColorKind::Hlg)
}

/// wgpu today accepts 8-bit YUV420/422/444/NV12 and BT.601/709 only.
/// Auto-routing PQ, HLG, or BT.2020 wide-gamut through that path would clamp
/// and retag incorrectly. SDR needs no conversion backend anyway.
pub fn wgpu_auto_route_allowed(kind: ColorKind) -> bool {
    kind == ColorKind::Sdr
}

/// True when `scale` exposes the FFmpeg 8 color-management options needed for
/// a non-gray HDR→SDR fallback. All three must be present; a partial set
/// (for example `out_color_matrix` on FFmpeg 7.1) is not enough.
pub fn swscale_perceptual_options_available() -> bool {
    is_filter_available("scale")
        && is_filter_option_available("scale", "out_primaries")
        && is_filter_option_available("scale", "out_transfer")
        && is_filter_option_available("scale", "intent")
}

/// Process-wide static probe. The linked FFmpeg cannot hot-swap filters, so
/// this is cached. wgpu adapter state is not included.
pub fn probe_hdr_caps() -> HdrFilterCapabilities {
    *HDR_FILTER_CAPS.get_or_init(probe_hdr_caps_uncached)
}

fn probe_hdr_caps_uncached() -> HdrFilterCapabilities {
    HdrFilterCapabilities {
        has_libplacebo: is_filter_available("libplacebo"),
        has_zscale: is_filter_available("zscale"),
        has_tonemap: is_filter_available("tonemap"),
        has_colorspace: is_filter_available("colorspace"),
        // Not `is_filter_available("scale")`: that name exists on FFmpeg 7.1
        // without out_primaries/out_transfer/intent, and a naive scale grays
        // out PQ/HLG. Probe those options (see swscale_perceptual_options_available).
        has_swscale_perceptual: swscale_perceptual_options_available(),
    }
}

/// Builds the tone-map chain for an HDR (PQ/HLG) input on the chosen backend.
/// `peak` is the normalized signal peak (`nits / 100`).
pub fn tone_map_chain(backend: Backend, peak: u32) -> String {
    match backend {
        // CPU chain: PQ->linear (npl=100 pins linear 1.0 to 100 cd/m2), gamut
        // to BT.709 in linear light, Hable tone-map with an EXPLICIT peak, then
        // re-tag BT.709 / limited range. The peak is explicit because on
        // FFmpeg 8 the zscale linearization strips the MaxCLL / mastering side
        // data that automatic peak detection reads (FFmpeg 7.1 kept it), so a
        // fixed peak is the only version-stable choice. desat=0 disables the
        // default desaturation that grays out highlights.
        Backend::Zscale => format!(
            "zscale=t=linear:npl=100,format=gbrpf32le,zscale=p=bt709,\
             tonemap=tonemap=hable:desat=0:peak={peak},\
             zscale=t=bt709:m=bt709:r=tv,format=yuv420p"
        ),
        // GPU chain: libplacebo does linearization, gamut and tone mapping in
        // one pass. tonemapping_param=0.5 restores the BT.2390 report curve's
        // knee (libplacebo's default knee is 1.0). range=tv tags limited range.
        Backend::Libplacebo => {
            "libplacebo=tonemapping=bt.2390:tonemapping_param=0.5:colorspace=bt709:\
             color_primaries=bt709:color_trc=bt709:range=tv:format=yuv420p"
                .to_string()
        }
        Backend::SwscalePerceptual => SWSCALE_PERCEPTUAL_CHAIN.to_string(),
        Backend::Colorspace => {
            // Not a tone-map backend. Returning the gamut chain keeps the
            // match exhaustive; callers should use `gamut_only_chain`.
            COLORSPACE_GAMUT_CHAIN.to_string()
        }
    }
}

/// Builds the gamut-only chain for wide-gamut SDR (no tone mapping).
pub fn gamut_only_chain(backend: Backend) -> String {
    match backend {
        // Convert primaries/matrix/transfer to BT.709 limited range without a
        // tone-mapping curve — the transfer is already SDR.
        Backend::Zscale => "zscale=p=bt709:t=bt709:m=bt709:r=tv,format=yuv420p".to_string(),
        Backend::Libplacebo => "libplacebo=colorspace=bt709:color_primaries=bt709:\
             color_trc=bt709:range=tv:format=yuv420p"
            .to_string(),
        Backend::Colorspace | Backend::SwscalePerceptual => COLORSPACE_GAMUT_CHAIN.to_string(),
    }
}

/// Whether the linked FFmpeg has the `sidedata` filter (used to delete HDR
/// frame side data after conversion). Cached: the linked build cannot change.
pub fn sidedata_delete_available() -> bool {
    static SIDEDATA: OnceLock<bool> = OnceLock::new();
    *SIDEDATA.get_or_init(|| is_filter_available("sidedata"))
}

/// HDR-only `sidedata` delete filters. Bare `sidedata=delete` would also
/// drop A53 captions, timecode, ROI, and other non-HDR entries.
pub const HDR_SIDEDATA_DELETE: &str = "\
sidedata=mode=delete:type=MASTERING_DISPLAY_METADATA,\
sidedata=mode=delete:type=CONTENT_LIGHT_LEVEL,\
sidedata=mode=delete:type=DYNAMIC_HDR_PLUS,\
sidedata=mode=delete:type=DYNAMIC_HDR_VIVID,\
sidedata=mode=delete:type=DOVI_METADATA,\
sidedata=mode=delete:type=DOVI_RPU_BUFFER";

/// Appends HDR-typed `sidedata` deletes when the filter exists. Tone-map /
/// gamut chains retag the output BT.709, but mastering-display,
/// content-light-level, and HDR10+/Dolby-Vision dynamic-metadata side data
/// would otherwise survive on the frames and could confuse players that
/// trust side data over tags. Non-HDR side data is left in place.
/// Missing filter is not an error: the route still runs, leftover side data
/// is only documented (see the example README).
pub fn append_sidedata_delete(chain: String, has_sidedata: bool) -> String {
    if has_sidedata {
        format!("{chain},{HDR_SIDEDATA_DELETE}")
    } else {
        chain
    }
}

/// [`build_chain`] with an explicit `sidedata` capability bit, split out so
/// the side-data cleanup is testable without a live FFmpeg probe.
pub fn build_chain_with_sidedata(
    kind: ColorKind,
    backend: Backend,
    peak: u32,
    has_sidedata: bool,
) -> Option<String> {
    let base = match kind {
        ColorKind::Pq | ColorKind::Hlg => match backend {
            Backend::Colorspace => None,
            _ => Some(tone_map_chain(backend, peak)),
        },
        ColorKind::WideGamutSdr => Some(gamut_only_chain(backend)),
        ColorKind::Sdr | ColorKind::AmbiguousHdr => None,
    };
    base.map(|chain| append_sidedata_delete(chain, has_sidedata))
}

/// Builds the filter chain for a classified input, or `None` for plain SDR
/// (which needs no conversion). HDR frame side data is deleted when this
/// FFmpeg build has the `sidedata` filter.
pub fn build_chain(kind: ColorKind, backend: Backend, peak: u32) -> Option<String> {
    build_chain_with_sidedata(kind, backend, peak, sidedata_delete_available())
}

/// Pure capability -> backend decision, split out so it can be tested without a
/// live FFmpeg build.
///
/// `prefer_gpu` maps to [`RoutingPolicy::QualityFirst`] (libplacebo first) vs
/// [`RoutingPolicy::Compatible`] (historical zscale-first default).
pub fn backend_from_caps(
    kind: ColorKind,
    prefer_gpu: bool,
    caps: HdrFilterCapabilities,
) -> Option<Backend> {
    let policy = if prefer_gpu {
        RoutingPolicy::QualityFirst
    } else {
        RoutingPolicy::Compatible
    };
    route(kind, policy, caps)
        .ok()
        .map(|decision| decision.selected)
}

/// Routes `kind` under `policy` using the supplied capability bits, with no
/// graph preflight. Selection is purely static; see [`route_with_preflight`]
/// for the variant that also configures a test graph per candidate.
pub fn route(
    kind: ColorKind,
    policy: RoutingPolicy,
    caps: HdrFilterCapabilities,
) -> Result<RouteDecision, String> {
    route_with_preflight(kind, policy, caps, None)
}

/// Per-candidate runnability probe: handed the [`ColorKind`] and the
/// candidate's would-be filter chain, returns whether the chain actually
/// configures on this host (see [`preflight_chain`]).
pub type PreflightFn = dyn Fn(ColorKind, &str) -> bool;

/// Routes `kind` under `policy` using the supplied capability bits.
///
/// Selection finishes before any output is written. A requested
/// [`RoutingPolicy::Exact`] backend that is missing is an error, not a
/// silent fallback.
///
/// When `preflight` is `Some`, each candidate that passes the static registry
/// check is additionally handed its would-be filter chain; returning `false`
/// marks the candidate unavailable ([`PREFLIGHT_REJECTION`]) and routing moves
/// on. This catches "registered but not runnable" backends — libplacebo can be
/// compiled in yet fail at graph-config time without a Vulkan runtime, and
/// without the preflight zscale/swscale would never be tried.
pub fn route_with_preflight(
    kind: ColorKind,
    policy: RoutingPolicy,
    caps: HdrFilterCapabilities,
    preflight: Option<&PreflightFn>,
) -> Result<RouteDecision, String> {
    if kind == ColorKind::Sdr {
        return Err(
            "input is already SDR (BT.709 or smaller); no conversion backend is selected".into(),
        );
    }
    if kind == ColorKind::AmbiguousHdr {
        return Err(format_ambiguous_hdr());
    }

    // Static registry check first; only candidates that pass it are worth the
    // cost of configuring a test graph.
    let status = |backend: Backend| -> (bool, Option<&'static str>) {
        let (available, rejection) = backend_status(backend, kind, caps);
        if !available {
            return (false, rejection);
        }
        if let Some(preflight) = preflight {
            if let Some(chain) = build_chain(kind, backend, PREFLIGHT_PEAK) {
                if !preflight(kind, &chain) {
                    return (false, Some(PREFLIGHT_REJECTION));
                }
            }
        }
        (true, None)
    };

    let candidates: &[Backend] = match policy {
        RoutingPolicy::Compatible => compatible_candidates(kind),
        RoutingPolicy::QualityFirst => quality_first_candidates(kind),
        RoutingPolicy::Exact(backend) => {
            let (available, rejection) = status(backend);
            let attempts = vec![RouteAttempt {
                backend,
                available,
                rejection,
            }];
            return if available {
                Ok(RouteDecision {
                    selected: backend,
                    attempts,
                })
            } else {
                // Keep the backend-specific reason. The generic checklist is
                // appended so Exact misses still list install options.
                Err(format_exact_miss(kind, caps, rejection))
            };
        }
    };

    let mut attempts = Vec::with_capacity(candidates.len());
    for backend in candidates {
        let (available, rejection) = status(*backend);
        attempts.push(RouteAttempt {
            backend: *backend,
            available,
            rejection,
        });
        if available {
            return Ok(RouteDecision {
                selected: *backend,
                attempts,
            });
        }
    }
    Err(format_no_backend(kind, caps, &attempts))
}

/// Whether `filter_desc` configures on the linked FFmpeg, checked with a tiny
/// lavfi graph (16x16, one frame) into the `null` muxer. This is the
/// "registered is not the same as runnable" probe: it exercises real filter
/// init/config, so a compiled-in libplacebo without a usable Vulkan runtime
/// fails here instead of after routing committed to it.
pub fn graph_configures(filter_desc: &str) -> bool {
    let context = FfmpegContext::builder()
        .input(Input::from("testsrc2=size=16x16:rate=1:duration=0.1").set_format("lavfi"))
        .filter_desc(filter_desc)
        // The null muxer writes nothing and opens no file; the URL is unused.
        .output(Output::from("preflight").set_format("null"))
        .build();
    let Ok(context) = context else {
        return false;
    };
    // Filter graphs may only fully configure once a frame flows, so run the
    // one-frame graph to completion rather than stopping at build().
    match context.start() {
        Ok(scheduler) => scheduler.wait().is_ok(),
        Err(_) => false,
    }
}

/// Source color tags injected ahead of the candidate chain in the preflight
/// graph, so filters that route on input tags (zscale linearization,
/// libplacebo) see a frame that looks like the classified input rather than
/// an untagged test pattern.
fn preflight_setparams(kind: ColorKind) -> &'static str {
    match kind {
        ColorKind::Pq => "setparams=colorspace=bt2020nc:color_primaries=bt2020:color_trc=smpte2084",
        ColorKind::Hlg => {
            "setparams=colorspace=bt2020nc:color_primaries=bt2020:color_trc=arib-std-b67"
        }
        ColorKind::WideGamutSdr => {
            "setparams=colorspace=bt2020nc:color_primaries=bt2020:color_trc=bt709"
        }
        // These kinds never reach a chain preflight (route rejects them
        // first); plain BT.709 keeps the function total.
        ColorKind::Sdr | ColorKind::AmbiguousHdr => {
            "setparams=colorspace=bt709:color_primaries=bt709:color_trc=bt709"
        }
    }
}

/// The live preflight hook for [`route_with_preflight`]: configures `chain`
/// against a tiny test frame tagged like `kind`.
pub fn preflight_chain(kind: ColorKind, chain: &str) -> bool {
    let desc = format!("format=yuv420p,{},{chain}", preflight_setparams(kind));
    graph_configures(&desc)
}

fn compatible_candidates(kind: ColorKind) -> &'static [Backend] {
    match kind {
        ColorKind::Pq | ColorKind::Hlg => &[
            Backend::Zscale,
            Backend::Libplacebo,
            Backend::SwscalePerceptual,
        ],
        ColorKind::WideGamutSdr => &[Backend::Zscale, Backend::Libplacebo, Backend::Colorspace],
        ColorKind::Sdr | ColorKind::AmbiguousHdr => &[],
    }
}

fn quality_first_candidates(kind: ColorKind) -> &'static [Backend] {
    match kind {
        ColorKind::Pq | ColorKind::Hlg => &[
            Backend::Libplacebo,
            Backend::Zscale,
            Backend::SwscalePerceptual,
        ],
        ColorKind::WideGamutSdr => &[Backend::Libplacebo, Backend::Zscale, Backend::Colorspace],
        ColorKind::Sdr | ColorKind::AmbiguousHdr => &[],
    }
}

fn backend_status(
    backend: Backend,
    kind: ColorKind,
    caps: HdrFilterCapabilities,
) -> (bool, Option<&'static str>) {
    match backend {
        Backend::Libplacebo => {
            if caps.has_libplacebo {
                (true, None)
            } else {
                (false, Some("libplacebo filter is not compiled in"))
            }
        }
        Backend::Zscale => {
            if !caps.has_zscale {
                (false, Some("zscale filter is not compiled in"))
            } else if needs_tone_map(kind) && !caps.has_tonemap {
                (
                    false,
                    Some("tonemap filter is not compiled in (required with zscale for PQ/HLG)"),
                )
            } else {
                (true, None)
            }
        }
        Backend::SwscalePerceptual => {
            if !needs_tone_map(kind) {
                (
                    false,
                    Some("scale/perceptual is an HDR tone-map fallback, not a gamut-only path"),
                )
            } else if caps.has_swscale_perceptual {
                (true, None)
            } else {
                (
                    false,
                    Some(
                        "scale is missing out_primaries, out_transfer, or intent (needs FFmpeg 8+)",
                    ),
                )
            }
        }
        Backend::Colorspace => {
            if kind != ColorKind::WideGamutSdr {
                (
                    false,
                    Some("colorspace is gamut-only; it cannot tone-map PQ/HLG"),
                )
            } else if caps.has_colorspace {
                (true, None)
            } else {
                (false, Some("colorspace filter is not compiled in"))
            }
        }
    }
}

/// Probes the linked FFmpeg and picks a backend for this `kind`.
///
/// Default (`prefer_gpu == false`) keeps the historical zscale-first order.
/// `prefer_gpu` selects libplacebo first when it is compiled in. FFmpeg 8
/// `scale`/`intent=perceptual` is a last resort for PQ/HLG; wide-gamut SDR
/// may use built-in `colorspace`. Returns `None` when nothing usable is
/// compiled in — the caller then fails closed rather than run a naive chain
/// that would gray out the picture.
pub fn pick_backend(kind: ColorKind, prefer_gpu: bool) -> Option<Backend> {
    backend_from_caps(kind, prefer_gpu, probe_hdr_caps())
}

/// The actionable error shown when no conversion backend is available.
pub fn no_backend_message(kind: ColorKind) -> String {
    if kind == ColorKind::AmbiguousHdr {
        return format_ambiguous_hdr();
    }
    format_no_backend(kind, probe_hdr_caps(), &[])
}

fn format_exact_miss(
    kind: ColorKind,
    caps: HdrFilterCapabilities,
    rejection: Option<&'static str>,
) -> String {
    match rejection {
        Some(reason) => format!("{reason}\n\n{}", format_no_backend(kind, caps, &[])),
        None => format_no_backend(kind, caps, &[]),
    }
}

fn format_ambiguous_hdr() -> String {
    "The input is tagged BT.2020 primaries or matrix, but the transfer \
     characteristic is unspecified. Treating it as PQ/HLG would wash out a \
     missing-tag SDR stream; treating it as wide-gamut SDR would skip tone \
     mapping if the stream is actually HDR. Set color_trc (smpte2084, \
     arib-std-b67, or bt709) before converting. No output was written."
        .into()
}

fn format_no_backend(
    kind: ColorKind,
    caps: HdrFilterCapabilities,
    attempts: &[RouteAttempt],
) -> String {
    if kind == ColorKind::AmbiguousHdr {
        return format_ambiguous_hdr();
    }
    let action = match kind {
        ColorKind::WideGamutSdr => "Wide-gamut SDR conversion",
        ColorKind::Sdr => "SDR conversion",
        ColorKind::Pq | ColorKind::Hlg => "HDR-to-SDR conversion",
        ColorKind::AmbiguousHdr => unreachable!("handled above"),
    };
    let source = match kind {
        ColorKind::Pq => "a PQ/BT.2020 source",
        ColorKind::Hlg => "an HLG/BT.2020 source",
        ColorKind::WideGamutSdr => "a wide-gamut SDR (BT.2020) source",
        ColorKind::Sdr => "an SDR source",
        ColorKind::AmbiguousHdr => unreachable!("handled above"),
    };

    let libplacebo = if caps.has_libplacebo {
        "libplacebo: filter is compiled in (a Vulkan runtime is still required at graph-config time)"
    } else {
        "libplacebo: filter not compiled in (requires FFmpeg --enable-libplacebo and a Vulkan runtime)"
    };

    let zscale = match (caps.has_zscale, caps.has_tonemap, needs_tone_map(kind)) {
        (true, true, _) => "zscale + tonemap: both filters are compiled in",
        (true, false, true) => {
            "zscale + tonemap: zscale is present but tonemap is missing \
             (tone mapping needs both; requires FFmpeg --enable-libzimg)"
        }
        (true, false, false) => "zscale: filter is compiled in",
        (false, _, _) => "zscale + tonemap: zscale is missing (requires FFmpeg --enable-libzimg)",
    };

    let swscale_line = if needs_tone_map(kind) {
        let swscale = if caps.has_swscale_perceptual {
            "FFmpeg scale/perceptual: required scale options are present \
             (out_primaries, out_transfer, intent)"
        } else {
            "FFmpeg scale/perceptual: required scale options are missing \
             (requires FFmpeg 8 or newer; probe with: ffmpeg -h filter=scale)"
        };
        format!("  - {swscale}\n")
    } else {
        String::new()
    };

    let colorspace_line = if kind == ColorKind::WideGamutSdr {
        if caps.has_colorspace {
            "  - colorspace: filter is compiled in\n"
        } else {
            "  - colorspace: filter not compiled in\n"
        }
        .to_string()
    } else {
        String::new()
    };

    let tried = if attempts.is_empty() {
        String::new()
    } else {
        let mut s = String::from("\nTried:\n");
        for attempt in attempts {
            match attempt.rejection {
                Some(reason) => {
                    s.push_str(&format!("  - {:?}: {reason}\n", attempt.backend));
                }
                None => s.push_str(&format!("  - {:?}: available\n", attempt.backend)),
            }
        }
        s
    };

    format!(
        "{action} was requested for {source}, but no usable backend was found.\n\
         \n\
         Checked:\n\
           - {libplacebo}\n\
           - {zscale}\n\
         {swscale_line}{colorspace_line}  - wgpu: not a usable HDR backend (current path is 8-bit BT.601/709 YUV/NV12 only)\n\
         {tried}\n\
         Install an FFmpeg build with --enable-libzimg or --enable-libplacebo, \
         or use FFmpeg 8 or newer whose scale filter exposes out_primaries, \
         out_transfer, and intent (ffmpeg -h filter=scale).\n\
         No output was written."
    )
}
