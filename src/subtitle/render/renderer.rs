//! [`PureRenderer`]: the pure-Rust [`SubtitleRenderer`] implementation.
//!
//! Owns the parsed script, the font store, and the frame geometry; per
//! frame it selects visible events, lays them out ([`super::layout`]),
//! applies simple collision stacking, and exposes the results as borrowed
//! [`OverlayImage`] nodes (valid until the next `render_frame`, enforced
//! by the borrow on `&mut self`).

use super::fonts::{FontStore, LoadedFace};
use super::layout::{
    apply_fade, render_event, unsupported, EventDynamics, EventRender, EventSamples, FrameContext,
    MaskSamples, RenderOptions, RenderedNode,
};
use crate::subtitle::ass::{Script, VALIGN_CENTER, VALIGN_TOP};
use crate::subtitle::backend::SubtitleRenderer;
use crate::subtitle::blend::OverlayImage;
use std::collections::HashMap;
use std::sync::Arc;

/// Memoized face lookups: values are `Arc`s, so per-worker clones during
/// parallel event rendering share the parsed faces.
type FaceCache = HashMap<(String, u16, bool), Option<Arc<LoadedFace>>>;

pub(crate) struct PureRenderer {
    script: Script,
    fonts: FontStore,
    opts: RenderOptions,
    frame_w: i32,
    frame_h: i32,
    storage_w: i32,
    storage_h: i32,
    par: f64,
    /// Face cache shared across frames (font selection is stable).
    face_cache: FaceCache,
    /// Nodes of the last rendered frame (backing store for the borrows).
    /// Colors are UNFADED; emission applies the per-frame fade, so fading
    /// frames reuse these nodes byte-for-byte.
    nodes: Vec<RenderedNode>,
    /// Visible-order of the event each node belongs to (parallel to
    /// `nodes`); indexes the per-frame fade values at emission. Valid
    /// whenever `cache.events` matches the current visible set.
    node_orders: Vec<u32>,
    /// Key describing exactly what `nodes` renders; see [`CacheKey`].
    cache: Option<CacheKey>,
    /// Time-dependent inputs captured per event index on its first cold
    /// render. Specs live in script coordinates/milliseconds, so geometry
    /// changes don't stale them; the script is immutable after parse.
    dynamics: HashMap<usize, EventDynamics>,
    /// Per-event rendered node templates (unshifted, pre-collision) keyed
    /// by the samples they were rendered with. On a frame-cache miss,
    /// events whose samples are unchanged reuse their template instead of
    /// re-running layout/shaping/rasterization; collision stacking is
    /// replayed over frame-local copies. Nodes are in output pixels, so
    /// geometry setters clear this map; eviction keeps only the visible
    /// set to bound memory.
    templates: HashMap<usize, EventTemplate>,
    lazy_inited: bool,
    /// `unsupported::*` features already warned about (once per feature).
    warned_unsupported: u32,
    /// Cold (frame-cache miss) render count, observable by cache tests.
    #[cfg(test)]
    cold_renders: u64,
    /// `render_event` call count, observable by template-cache tests.
    #[cfg(test)]
    event_renders: u64,
}

/// One event's rendered output before collision stacking, reusable while
/// its mask-affecting samples stay unchanged (colors are unfaded; the
/// fade value is emission-only).
struct EventTemplate {
    samples: MaskSamples,
    nodes: Vec<RenderedNode>,
    detect_collisions: bool,
}

/// Frame-cache identity: the visible event set, the output geometry, and
/// the per-event MASK samples. Equal keys imply byte-identical nodes, so
/// animated events hit the cache on every frame where their resolved
/// mask-affecting values (karaoke state, \move anchor, fade active or
/// saturated) are unchanged — an interpolating \fad hits every frame and
/// only re-emits colors; karaoke re-renders only at syllable boundaries.
#[derive(PartialEq, Eq)]
struct CacheKey {
    events: Vec<usize>,
    frame: (i32, i32),
    /// Effective storage size (falls back to the frame size): it scales
    /// borders/blur/shadows, so it must key the cache even though the
    /// public setter also invalidates on change.
    storage: (i32, i32),
    samples: Vec<MaskSamples>,
}

impl PureRenderer {
    pub(crate) fn new(script: Script, fonts: FontStore, opts: RenderOptions) -> Self {
        Self {
            script,
            fonts,
            opts,
            frame_w: 0,
            frame_h: 0,
            storage_w: 0,
            storage_h: 0,
            par: 1.0,
            face_cache: HashMap::new(),
            nodes: Vec::new(),
            node_orders: Vec::new(),
            cache: None,
            dynamics: HashMap::new(),
            templates: HashMap::new(),
            lazy_inited: false,
            warned_unsupported: 0,
            #[cfg(test)]
            cold_renders: 0,
            #[cfg(test)]
            event_renders: 0,
        }
    }

    /// Logs each unsupported feature exactly once per renderer, so scripts
    /// that rely on unimplemented tags degrade loudly instead of silently.
    fn warn_unsupported(&mut self, flags: u32) {
        let mut new_flags = flags & !self.warned_unsupported;
        while new_flags != 0 {
            let flag = 1 << new_flags.trailing_zeros();
            log::warn!("subtitle render: {}", unsupported::describe(flag));
            new_flags &= !flag;
        }
        self.warned_unsupported |= flags;
    }

    #[cfg(test)]
    pub(crate) fn script(&self) -> &Script {
        &self.script
    }

    /// `ass_lazy_track_init`: PlayRes defaults resolved on first render.
    fn lazy_track_init(&mut self) {
        if self.lazy_inited {
            return;
        }
        self.lazy_inited = true;
        let track = &mut self.script;
        if track.play_res_x > 0 && track.play_res_y > 0 {
            return;
        }
        if track.play_res_x <= 0 && track.play_res_y <= 0 {
            log::warn!("subtitle render: neither PlayResX nor PlayResY defined, assuming 384x288");
            track.play_res_x = 384;
            track.play_res_y = 288;
        } else if track.play_res_y <= 0 && track.play_res_x == 1280 {
            track.play_res_y = 1024;
        } else if track.play_res_y <= 0 {
            track.play_res_y = (track.play_res_x as i64 * 3 / 4).max(1) as i32;
        } else if track.play_res_x <= 0 && track.play_res_y == 1024 {
            track.play_res_x = 1280;
        } else if track.play_res_x <= 0 {
            track.play_res_x = (track.play_res_y as i64 * 4 / 3).max(1) as i32;
        }
    }

    /// Indices of events visible at `now_ms`, in layer order (stable over
    /// ReadOrder within a layer, like libass's qsort by layer/ReadOrder).
    fn visible_events(&self, now_ms: i64) -> Vec<usize> {
        let mut visible: Vec<usize> = self
            .script
            .events
            .iter()
            .enumerate()
            .filter(|(_, event)| {
                now_ms >= event.start_ms && now_ms < event.start_ms + event.duration_ms
            })
            .map(|(index, _)| index)
            .collect();
        visible.sort_by_key(|&index| {
            let event = &self.script.events[index];
            (event.layer, event.read_order)
        });
        visible
    }
}

impl SubtitleRenderer for PureRenderer {
    fn set_frame_size(&mut self, width: i32, height: i32) {
        if (self.frame_w, self.frame_h) != (width, height) {
            self.cache = None;
            self.templates.clear();
        }
        self.frame_w = width;
        self.frame_h = height;
    }

    fn set_storage_size(&mut self, width: i32, height: i32) {
        if (self.storage_w, self.storage_h) != (width, height) {
            self.cache = None;
            self.templates.clear();
        }
        self.storage_w = width;
        self.storage_h = height;
    }

    fn set_pixel_aspect(&mut self, par: f64) {
        if self.par != par {
            self.cache = None;
            self.templates.clear();
        }
        self.par = par;
    }

    fn render_frame(&mut self, now_ms: i64) -> Vec<OverlayImage<'_>> {
        self.lazy_track_init();
        if self.frame_w <= 0 || self.frame_h <= 0 {
            return Vec::new();
        }

        let visible = self.visible_events(now_ms);
        let frame = (self.frame_w, self.frame_h);
        let storage = (
            if self.storage_w > 0 {
                self.storage_w
            } else {
                self.frame_w
            },
            if self.storage_h > 0 {
                self.storage_h
            } else {
                self.frame_h
            },
        );
        // Resolve every visible event's dynamic samples. An event that was
        // never rendered has no captured record yet (None) and forces a
        // miss — a made-up "static" sample could wrongly hit for a dynamic
        // event.
        let samples: Vec<Option<EventSamples>> = visible
            .iter()
            .map(|&index| {
                self.dynamics.get(&index).map(|dynamics| {
                    let event = &self.script.events[index];
                    dynamics.sample(now_ms - event.start_ms, event.duration_ms)
                })
            })
            .collect();
        // Per-frame emission fades (deliberately NOT part of any cache
        // key: nodes are stored unfaded and faded at emission below).
        let mut fades: Vec<Option<i32>> = samples
            .iter()
            .map(|sample| sample.as_ref().and_then(EventSamples::fade))
            .collect();
        let mask_samples: Vec<Option<MaskSamples>> = samples
            .iter()
            .map(|sample| sample.as_ref().map(EventSamples::mask_key))
            .collect();

        let cache_valid = match &self.cache {
            Some(cache) => {
                cache.events == visible
                    && cache.frame == frame
                    && cache.storage == storage
                    && cache.samples.len() == mask_samples.len()
                    && cache
                        .samples
                        .iter()
                        .zip(&mask_samples)
                        .all(|(cached, now)| now.as_ref() == Some(cached))
            }
            None => false,
        };
        if !cache_valid {
            #[cfg(test)]
            {
                self.cold_renders += 1;
            }
            let ctx = FrameContext {
                script: &self.script,
                fonts: &self.fonts,
                frame_w: self.frame_w,
                frame_h: self.frame_h,
                storage_w: storage.0,
                storage_h: storage.1,
                par: self.par,
                opts: &self.opts,
            };

            // Reuse each event's template when its mask samples are
            // unchanged: layout, shaping and rasterization are then
            // skipped and only the node copy + stacking replay run.
            let mut prepared: Vec<Option<(Vec<RenderedNode>, bool)>> = visible
                .iter()
                .enumerate()
                .map(|(order, &index)| {
                    mask_samples[order]
                        .as_ref()
                        .and_then(|sample| {
                            self.templates
                                .get(&index)
                                .filter(|template| &template.samples == sample)
                        })
                        .map(|template| (template.nodes.clone(), template.detect_collisions))
                })
                .collect();
            let misses: Vec<usize> = (0..visible.len())
                .filter(|&order| prepared[order].is_none())
                .collect();
            #[cfg(test)]
            {
                self.event_renders += misses.len() as u64;
            }
            // Render the misses — in parallel when there is more than one:
            // event renders are independent (results merge in visible
            // order below, so the output is bit-identical to a sequential
            // pass); each worker gets its own face-cache clone (values are
            // `Arc`s) merged back afterwards.
            let mut rendered: Vec<Option<EventRender>> = (0..visible.len()).map(|_| None).collect();
            if misses.len() > 1 {
                let workers = std::thread::available_parallelism()
                    .map(|n| n.get())
                    .unwrap_or(1)
                    .min(misses.len());
                let per_worker = misses.len().div_ceil(workers);
                let results: Vec<(Vec<(usize, EventRender)>, FaceCache)> =
                    std::thread::scope(|scope| {
                        let ctx = &ctx;
                        let visible = &visible;
                        let handles: Vec<_> = misses
                            .chunks(per_worker)
                            .map(|orders| {
                                let mut face_cache = self.face_cache.clone();
                                scope.spawn(move || {
                                    let renders: Vec<(usize, EventRender)> = orders
                                        .iter()
                                        .map(|&order| {
                                            let event = &ctx.script.events[visible[order]];
                                            let render =
                                                render_event(ctx, event, now_ms, &mut face_cache);
                                            (order, render)
                                        })
                                        .collect();
                                    (renders, face_cache)
                                })
                            })
                            .collect();
                        handles
                            .into_iter()
                            .map(|handle| handle.join().expect("event render worker panicked"))
                            .collect()
                    });
                for (renders, face_cache) in results {
                    for (order, render) in renders {
                        rendered[order] = Some(render);
                    }
                    for (key, face) in face_cache {
                        self.face_cache.entry(key).or_insert(face);
                    }
                }
            } else if let Some(&order) = misses.first() {
                let event = &self.script.events[visible[order]];
                rendered[order] = Some(render_event(&ctx, event, now_ms, &mut self.face_cache));
            }

            let mut all_nodes: Vec<RenderedNode> = Vec::new();
            let mut node_orders: Vec<u32> = Vec::new();
            let mut occupied: Vec<(i32, i32, i32, i32)> = Vec::new();
            let mut seen_unsupported = 0u32;
            for (order, &index) in visible.iter().enumerate() {
                let event = &self.script.events[index];
                let (mut nodes, detect_collisions) = match prepared[order].take() {
                    Some(hit) => hit,
                    None => {
                        let render = rendered[order]
                            .take()
                            .expect("every template miss was rendered above");
                        seen_unsupported |= render.unsupported;
                        self.dynamics.insert(index, render.dynamics);
                        let full = self
                            .dynamics
                            .get(&index)
                            .expect("dynamics recorded just above")
                            .sample(now_ms - event.start_ms, event.duration_ms);
                        // First render of this event: the pre-loop fade was
                        // None (no record yet); emission needs the real one.
                        fades[order] = full.fade();
                        self.templates.insert(
                            index,
                            EventTemplate {
                                samples: full.mask_key(),
                                nodes: render.nodes.clone(),
                                detect_collisions: render.detect_collisions,
                            },
                        );
                        (render.nodes, render.detect_collisions)
                    }
                };
                // A saturated fade (>= 255 pre-clamp) turns every node
                // fully transparent; before the emission split those nodes
                // were dropped ahead of collision stacking, so the event
                // must contribute neither nodes nor an occupied rectangle.
                if fades[order].is_some_and(|fade| fade >= 255) {
                    continue;
                }
                if nodes.is_empty() {
                    continue;
                }
                // Collision stacking for unpositioned events: shift the
                // whole block off previously occupied rectangles.
                if detect_collisions {
                    stack_block(&mut nodes, &mut occupied, &self.script, event, self.frame_h);
                } else if let Some(bbox) = block_bbox(&nodes) {
                    occupied.push(bbox);
                }
                node_orders.extend(std::iter::repeat_n(order as u32, nodes.len()));
                all_nodes.append(&mut nodes);
            }
            self.nodes = all_nodes;
            self.node_orders = node_orders;
            // Bound template memory to the visible set (masks are the big
            // allocation; dynamics records are tiny and stay).
            self.templates.retain(|index, _| visible.contains(index));
            // Key the result by the mask samples the render actually used —
            // the records are complete now, so resolve any missing ones.
            let samples = visible
                .iter()
                .zip(mask_samples)
                .map(|(&index, mask)| {
                    mask.unwrap_or_else(|| {
                        let event = &self.script.events[index];
                        self.dynamics
                            .get(&index)
                            .expect("dynamics recorded during this render pass")
                            .sample(now_ms - event.start_ms, event.duration_ms)
                            .mask_key()
                    })
                })
                .collect();
            self.cache = Some(CacheKey {
                events: visible,
                frame,
                storage,
                samples,
            });
            self.warn_unsupported(seen_unsupported);
        }

        self.nodes
            .iter()
            .zip(&self.node_orders)
            .filter(|(node, _)| !node.bitmap.is_empty())
            .map(|(node, &order)| OverlayImage {
                w: node.bitmap.w,
                h: node.bitmap.h,
                stride: node.bitmap.w,
                bitmap: node.bitmap.data.as_slice(),
                color: apply_fade(node.color, fades[order as usize]),
                dst_x: node.bitmap.x,
                dst_y: node.bitmap.y,
            })
            .collect()
    }

    fn teardown(&mut self) {
        self.nodes.clear();
        self.node_orders.clear();
        self.face_cache.clear();
        self.cache = None;
        self.dynamics.clear();
        self.templates.clear();
    }
}

fn block_bbox(nodes: &[RenderedNode]) -> Option<(i32, i32, i32, i32)> {
    let mut bbox: Option<(i32, i32, i32, i32)> = None;
    for node in nodes {
        if node.bitmap.is_empty() {
            continue;
        }
        let (x0, y0) = (node.bitmap.x, node.bitmap.y);
        let (x1, y1) = (x0 + node.bitmap.w as i32, y0 + node.bitmap.h as i32);
        bbox = Some(match bbox {
            None => (x0, y0, x1, y1),
            Some((a, b, c, d)) => (a.min(x0), b.min(y0), c.max(x1), d.max(y1)),
        });
    }
    bbox
}

fn intersects(a: (i32, i32, i32, i32), b: (i32, i32, i32, i32)) -> bool {
    a.0 < b.2 && b.0 < a.2 && a.1 < b.3 && b.1 < a.3
}

/// Shifts an event block vertically off occupied rectangles: sub-aligned
/// blocks move up, top-aligned move down, centered stay (libass's shift
/// direction convention), then records the final rectangle.
fn stack_block(
    nodes: &mut [RenderedNode],
    occupied: &mut Vec<(i32, i32, i32, i32)>,
    script: &Script,
    event: &crate::subtitle::ass::Event,
    frame_h: i32,
) {
    let Some(mut bbox) = block_bbox(nodes) else {
        return;
    };
    let style = &script.styles[event.style.min(script.styles.len() - 1)];
    let valign = style.alignment & 12;
    let mut shift = 0i32;
    let mut guard = 0;
    loop {
        let hit = occupied
            .iter()
            .copied()
            .find(|&rect| intersects(bbox, rect));
        let Some(rect) = hit else { break };
        let delta = match valign {
            VALIGN_TOP => rect.3 - bbox.1, // move below the obstacle
            VALIGN_CENTER => break,        // centered events overlap (libass-ish)
            _ => rect.1 - bbox.3,          // move above the obstacle
        };
        shift += delta;
        bbox.1 += delta;
        bbox.3 += delta;
        guard += 1;
        if guard > 64 || bbox.3 < 0 || bbox.1 > frame_h {
            break;
        }
    }
    if shift != 0 {
        for node in nodes.iter_mut() {
            node.bitmap.y += shift;
        }
    }
    occupied.push(bbox);
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::subtitle::ass;
    use crate::subtitle::test_util;

    fn renderer_with(events: &str) -> Option<PureRenderer> {
        let path = test_util::test_font()?;
        let script = ass::parse(&test_util::minimal_ass(events)).expect("parse");
        let mut fonts = FontStore::new(false);
        assert!(fonts.load_default_font_file(std::path::Path::new(path)));
        let mut renderer = PureRenderer::new(script, fonts, RenderOptions::default());
        renderer.set_frame_size(640, 360);
        renderer.set_storage_size(640, 360);
        Some(renderer)
    }

    #[test]
    fn renders_text_event_inside_frame() {
        let Some(mut renderer) = renderer_with(test_util::HELLO_EVENT) else {
            eprintln!("skipping: no known test font present on this machine");
            return;
        };
        let overlays = renderer.render_frame(1_000);
        assert!(!overlays.is_empty(), "hello event must produce overlays");
        for overlay in &overlays {
            assert!(overlay.w > 0 && overlay.h > 0);
            assert!(overlay.stride >= overlay.w);
            assert!(overlay.bitmap.len() >= overlay.stride * (overlay.h - 1) + overlay.w);
            assert!(overlay.dst_x >= -64 && overlay.dst_x < 640 + 64);
            assert!(overlay.dst_y >= -64 && overlay.dst_y < 360 + 64);
        }
        // Default style is bottom-center: the block must live in the lower
        // half of the frame.
        let max_bottom = overlays.iter().map(|o| o.dst_y + o.h as i32).max().unwrap();
        assert!(max_bottom > 180, "bottom-aligned text is in the lower half");
        // Outside the event window: nothing.
        assert!(renderer.render_frame(20_000).is_empty());
    }

    /// Hostile numeric override values must not panic the renderer (debug
    /// builds trap integer overflow; release would silently wrap or, for
    /// unbounded allocations, OOM). Covers the border/blur allocation clamps
    /// and the widened \p-scale shift, drawing cbox, \fade time, and margin
    /// arithmetic. All render frames must complete.
    #[test]
    fn hostile_numeric_overrides_do_not_panic() {
        let events = concat!(
            // Huge outline: stroke-mask allocation is clamped to the frame.
            "Dialogue: 0,0:00:00.00,0:00:10.00,Default,,0,0,0,,{\\bord100000}A\n",
            // \p99: drawing-scale shift was `1u32 << 98`.
            "Dialogue: 0,0:00:00.00,0:00:10.00,Default,,0,0,0,,{\\p99}m 0 0 l 5 5{\\p0}\n",
            // Extreme drawing coordinates: cbox subtraction is widened.
            "Dialogue: 0,0:00:00.00,0:00:10.00,Default,,0,0,0,,{\\p1}m -2000000000 0 l 2000000000 0 2000000000 5{\\p0}\n",
            // Extreme \fade times, plus a negative alpha (mask-active bit).
            "Dialogue: 0,0:00:00.00,0:00:10.00,Default,,0,0,0,,{\\fade(255,0,255,-2000000000,2000000000,0,10)}B\n",
            "Dialogue: 0,0:00:00.00,0:00:10.00,Default,,0,0,0,,{\\fade(-5,0,0,0,100,9000,10000)\\bord4}C\n",
            // Extreme margins: wrap-width arithmetic is widened.
            "Dialogue: 0,0:00:00.00,0:00:10.00,Default,,-2000000000,2000000000,0,,D E F G H\n",
        );
        let Some(mut renderer) = renderer_with(events) else {
            eprintln!("skipping: no known test font present on this machine");
            return;
        };
        // Timestamps across the fade interpolation branches.
        for ms in [0, 50, 500, 1_000, 5_000, 9_999] {
            let _ = renderer.render_frame(ms);
        }
    }

    #[test]
    fn renders_drawing_event_at_position() {
        let Some(mut renderer) = renderer_with(test_util::DRAWING_EVENT) else {
            eprintln!("skipping: no known test font present on this machine");
            return;
        };
        let overlays = renderer.render_frame(1_000);
        assert!(!overlays.is_empty(), "drawing must produce overlays");
        // \an7\pos(100,100): the fill's top-left lands at (100,100); the
        // 100x50 rectangle spans to (200,150).
        let fill = overlays.last().expect("at least the fill node");
        assert!((fill.dst_x - 100).abs() <= 2, "x {}", fill.dst_x);
        assert!((fill.dst_y - 100).abs() <= 2, "y {}", fill.dst_y);
        assert!((fill.w as i32 - 100).unsigned_abs() <= 4, "w {}", fill.w);
        assert!((fill.h as i32 - 50).unsigned_abs() <= 4, "h {}", fill.h);
    }

    #[test]
    fn simultaneous_events_stack_instead_of_overlapping() {
        let events = "Dialogue: 0,0:00:00.00,0:00:05.00,Default,,0,0,0,,First\n\
                      Dialogue: 0,0:00:00.00,0:00:05.00,Default,,0,0,0,,Second\n";
        let Some(mut renderer) = renderer_with(events) else {
            eprintln!("skipping: no known test font present on this machine");
            return;
        };
        let overlays = renderer.render_frame(1_000);
        assert!(overlays.len() >= 2);
        // Group nodes into two clusters by y and require disjoint ranges.
        let mut tops: Vec<i32> = overlays.iter().map(|o| o.dst_y).collect();
        tops.sort_unstable();
        tops.dedup();
        assert!(
            tops.len() >= 2,
            "two stacked lines must not share the same top: {tops:?}"
        );
    }

    #[test]
    fn static_frames_are_cached_and_time_frames_are_not() {
        let Some(mut renderer) = renderer_with(test_util::HELLO_EVENT) else {
            eprintln!("skipping: no known test font present on this machine");
            return;
        };
        assert!(!renderer.render_frame(1_000).is_empty());
        assert!(renderer.cache.is_some(), "static event set caches");
        assert!(!renderer.render_frame(2_000).is_empty());
        assert_eq!(renderer.cold_renders, 1, "static frames must cache-hit");

        // A mid-flight \move changes its anchor sample every frame, so each
        // frame re-renders — while the cache stays warm for repeats of the
        // same timestamp and after the move window ends.
        let moving =
            "Dialogue: 0,0:00:00.00,0:00:05.00,Default,,0,0,0,,{\\move(0,0,100,100,500,4500)}Go\n";
        let mut renderer = renderer_with(moving).expect("font probed above");
        let first: Vec<i32> = renderer
            .render_frame(1_000)
            .iter()
            .map(|o| o.dst_x)
            .collect();
        assert!(!first.is_empty());
        assert_eq!(renderer.cold_renders, 1);
        let repeat: Vec<i32> = renderer
            .render_frame(1_000)
            .iter()
            .map(|o| o.dst_x)
            .collect();
        assert_eq!(first, repeat);
        assert_eq!(renderer.cold_renders, 1, "same timestamp must hit");
        let later: Vec<i32> = renderer
            .render_frame(3_000)
            .iter()
            .map(|o| o.dst_x)
            .collect();
        assert_eq!(renderer.cold_renders, 2, "moved anchor must re-render");
        assert_ne!(first, later, "\\move must move the block");
        // After t2 the anchor is settled at `to`: samples stop changing and
        // the cache holds again.
        let _ = renderer.render_frame(4_700);
        assert_eq!(renderer.cold_renders, 3);
        let _ = renderer.render_frame(4_900);
        assert_eq!(renderer.cold_renders, 3, "settled \\move must cache-hit");
    }

    /// Fading only changes the emitted colors: masks are cached across the
    /// whole fade (the mask key tracks just fade-active and saturation),
    /// so interpolating frames hit the cache and re-emit — and the emitted
    /// alpha still tracks the fade value frame by frame.
    #[test]
    fn interpolating_fade_hits_the_cache_and_re_emits() {
        let fading = "Dialogue: 0,0:00:00.00,0:00:05.00,Default,,0,0,0,,{\\fad(500,500)}Plateau\n";
        let Some(mut renderer) = renderer_with(fading) else {
            eprintln!("skipping: no known test font present on this machine");
            return;
        };
        let alpha_max = |renderer: &mut PureRenderer, ms: i64| -> u32 {
            renderer
                .render_frame(ms)
                .iter()
                .map(|o| o.color & 0xFF)
                .max()
                .expect("some overlay")
        };
        let early = alpha_max(&mut renderer, 100); // fade-in, mostly transparent
        assert_eq!(renderer.cold_renders, 1);
        let later = alpha_max(&mut renderer, 400); // fade-in, mostly opaque
        assert_eq!(
            renderer.cold_renders, 1,
            "interpolating fade must reuse the cached masks"
        );
        assert!(
            early > later,
            "fade-in must reduce transparency over time ({early} !> {later})"
        );
        let _ = alpha_max(&mut renderer, 2_000);
        assert_eq!(
            renderer.cold_renders, 2,
            "fade-active flip (slope -> plateau) re-renders masks once"
        );
        let _ = alpha_max(&mut renderer, 3_000);
        assert_eq!(renderer.cold_renders, 2, "plateau frames cache-hit");
        // Warmed emission must equal a cold render at the same timestamp.
        let warmed: Vec<u32> = renderer
            .render_frame(4_800)
            .iter()
            .map(|o| o.color)
            .collect();
        let mut fresh = renderer_with(fading).expect("font probed above");
        let cold: Vec<u32> = fresh.render_frame(4_800).iter().map(|o| o.color).collect();
        assert_eq!(warmed, cold, "faded emission must match a cold render");
    }

    /// A saturated fade (255) drops every node before stacking — the event
    /// must vanish entirely, exactly like the pre-split renderer.
    #[test]
    fn saturated_fade_contributes_nothing() {
        // \fad(1000,1000): at t=0 the fade multiplier is exactly 255.
        let fading = "Dialogue: 0,0:00:00.00,0:00:05.00,Default,,0,0,0,,{\\fad(1000,1000)}Gone\n";
        let Some(mut renderer) = renderer_with(fading) else {
            eprintln!("skipping: no known test font present on this machine");
            return;
        };
        assert!(
            renderer.render_frame(0).is_empty(),
            "fully faded-out frame renders nothing"
        );
        assert!(!renderer.render_frame(1_000).is_empty());
    }

    /// Stepwise karaoke flags flip only at syllable boundaries; frames in
    /// between must hit the cache.
    #[test]
    fn karaoke_steady_frames_hit_the_cache() {
        let events = "Dialogue: 0,0:00:00.00,0:00:05.00,Default,,0,0,0,,{\\k100}Ka{\\k100}ra\n";
        let Some(mut renderer) = renderer_with(events) else {
            eprintln!("skipping: no known test font present on this machine");
            return;
        };
        let _ = renderer.render_frame(300);
        let _ = renderer.render_frame(600); // same syllable state
        let _ = renderer.render_frame(900); // same syllable state
        assert_eq!(renderer.cold_renders, 1, "steady karaoke must cache-hit");
        let _ = renderer.render_frame(1_200); // second syllable now primary
        assert_eq!(renderer.cold_renders, 2, "syllable boundary re-renders");
        let _ = renderer.render_frame(1_900);
        assert_eq!(renderer.cold_renders, 2, "steady again after boundary");
    }

    /// On a frame-cache miss caused by one animated event, static sibling
    /// events must reuse their node templates instead of re-rendering.
    #[test]
    fn static_siblings_reuse_templates_next_to_animated_events() {
        // \move changes the mask key every frame (unlike \fad, whose value
        // is emission-only), so it exercises the per-event template path.
        let events = "Dialogue: 0,0:00:00.00,0:00:05.00,Default,,0,0,0,,Static line\n\
                      Dialogue: 0,0:00:00.00,0:00:05.00,Default,,0,0,0,,{\\move(100,100,600,100)}Moving\n";
        let Some(mut renderer) = renderer_with(events) else {
            eprintln!("skipping: no known test font present on this machine");
            return;
        };
        let _ = renderer.render_frame(1_000);
        assert_eq!(renderer.event_renders, 2, "first frame renders both");
        let _ = renderer.render_frame(1_100); // anchor sample moved
        assert_eq!(renderer.cold_renders, 2, "moving anchor misses");
        assert_eq!(
            renderer.event_renders, 3,
            "only the moving event re-renders; the static line reuses its template"
        );
        // Template reuse must compose byte-identically to a cold render of
        // the same timestamp.
        let warmed: Vec<(i32, i32, usize, usize, u32, Vec<u8>)> = renderer
            .render_frame(1_200)
            .into_iter()
            .map(|o| (o.dst_x, o.dst_y, o.w, o.h, o.color, o.bitmap.to_vec()))
            .collect();
        let mut fresh = renderer_with(events).expect("font probed above");
        let cold: Vec<(i32, i32, usize, usize, u32, Vec<u8>)> = fresh
            .render_frame(1_200)
            .into_iter()
            .map(|o| (o.dst_x, o.dst_y, o.w, o.h, o.color, o.bitmap.to_vec()))
            .collect();
        assert_eq!(warmed, cold, "template compose must match a cold render");
    }

    /// Templates are evicted once their event leaves the visible set.
    #[test]
    fn templates_are_evicted_with_visibility() {
        let events = "Dialogue: 0,0:00:00.00,0:00:02.00,Default,,0,0,0,,First\n\
                      Dialogue: 0,0:00:03.00,0:00:05.00,Default,,0,0,0,,Second\n";
        let Some(mut renderer) = renderer_with(events) else {
            eprintln!("skipping: no known test font present on this machine");
            return;
        };
        let _ = renderer.render_frame(1_000);
        assert_eq!(renderer.templates.len(), 1);
        let _ = renderer.render_frame(4_000);
        assert_eq!(
            renderer.templates.len(),
            1,
            "hidden event's template evicted"
        );
        assert!(renderer.templates.contains_key(&1));
    }

    /// The storage size scales borders/blur/shadows, so changing it must
    /// invalidate the cache even when the frame size is unchanged.
    #[test]
    fn storage_size_change_invalidates_cache() {
        let Some(mut renderer) = renderer_with(test_util::HELLO_EVENT) else {
            eprintln!("skipping: no known test font present on this machine");
            return;
        };
        let _ = renderer.render_frame(1_000);
        assert_eq!(renderer.cold_renders, 1);
        renderer.set_storage_size(1280, 720);
        let _ = renderer.render_frame(1_000);
        assert_eq!(renderer.cold_renders, 2, "storage change must re-render");
        renderer.set_storage_size(1280, 720);
        let _ = renderer.render_frame(1_000);
        assert_eq!(renderer.cold_renders, 2, "unchanged storage keeps cache");
    }

    /// transform_is_noop_initial_state: while \t interpolation is
    /// unsupported the output is time-invariant, so \t must not poison the
    /// static-frame cache — and a cold render at another timestamp must
    /// produce the identical overlay set. Update this test when real \t
    /// interpolation lands.
    #[test]
    fn noop_transform_keeps_static_cache() {
        let events = "Dialogue: 0,0:00:00.00,0:00:05.00,Default,,0,0,0,,{\\t(0,500,\\fs30)}Anim\n";
        let Some(mut renderer) = renderer_with(events) else {
            eprintln!("skipping: no known test font present on this machine");
            return;
        };
        let snapshot = |overlays: Vec<crate::subtitle::blend::OverlayImage<'_>>| -> Vec<(i32, i32, usize, usize, u32, Vec<u8>)> {
            overlays
                .into_iter()
                .map(|o| (o.dst_x, o.dst_y, o.w, o.h, o.color, o.bitmap.to_vec()))
                .collect()
        };
        let early = snapshot(renderer.render_frame(1_000));
        assert!(!early.is_empty());
        assert!(renderer.cache.is_some(), "noop \\t must keep the cache");
        // A fresh renderer cold-rendering at a different timestamp must
        // agree byte-for-byte (time-invariance, not just cache reuse).
        let mut fresh = renderer_with(events).expect("font probed above");
        let late = snapshot(fresh.render_frame(3_500));
        assert_eq!(early, late, "noop \\t output must not depend on time");
    }

    #[test]
    fn fade_reduces_alpha_over_time() {
        let fading = "Dialogue: 0,0:00:00.00,0:00:05.00,Default,,0,0,0,,{\\fad(0,4000)}Bye\n";
        let Some(mut renderer) = renderer_with(fading) else {
            eprintln!("skipping: no known test font present on this machine");
            return;
        };
        let early: Vec<u32> = renderer
            .render_frame(500)
            .iter()
            .map(|o| o.color & 0xFF)
            .collect();
        let late: Vec<u32> = renderer
            .render_frame(4_500)
            .iter()
            .map(|o| o.color & 0xFF)
            .collect();
        assert!(!early.is_empty() && !late.is_empty());
        let early_max = early.iter().max().copied().unwrap_or(0);
        let late_min = late.iter().min().copied().unwrap_or(0);
        assert!(
            late_min > early_max,
            "fade-out increases transparency (early {early_max}, late {late_min})"
        );
    }

    /// One-off diagnostic (dev tool, keep #[ignore]): dumps the pure
    /// renderer's overlay list for a given script so glyph geometry can be
    /// compared against a libass CLI render offline.
    #[test]
    #[ignore = "diagnostic; run explicitly"]
    fn dump_overlays_for_diag() {
        let Some(script_path) = std::env::var_os("EZ_DIAG_SCRIPT") else {
            eprintln!("set EZ_DIAG_SCRIPT to an .ass file");
            return;
        };
        let content = std::fs::read_to_string(script_path).expect("read script");
        let script = ass::parse(&content).expect("parse");
        let font = test_util::test_font().expect("font");
        let mut fonts = FontStore::new(false);
        assert!(fonts.load_default_font_file(std::path::Path::new(font)));
        let mut renderer = PureRenderer::new(script, fonts, RenderOptions::default());
        renderer.set_frame_size(320, 240);
        renderer.set_storage_size(320, 240);
        for overlay in renderer.render_frame(500) {
            let ink: usize = overlay.bitmap.iter().filter(|&&v| v > 0).count();
            println!(
                "node: dst=({}, {}) size={}x{} color={:08x} ink={}",
                overlay.dst_x, overlay.dst_y, overlay.w, overlay.h, overlay.color, ink
            );
        }
    }

    #[test]
    fn unsupported_features_warn_once_per_renderer() {
        let events = "Dialogue: 0,0:00:00.00,0:00:05.00,Default,,0,0,0,,{\\frz45}Rotated\n\
                      Dialogue: 0,0:00:00.00,0:00:05.00,Default,,0,0,0,,{\\kf50}Kara{\\kf50}oke\n\
                      Dialogue: 0,0:00:00.00,0:00:05.00,Default,,0,0,0,,{\\t(0,500,\\fs30)}Anim\n";
        let Some(mut renderer) = renderer_with(events) else {
            eprintln!("skipping: no known test font present on this machine");
            return;
        };
        assert_eq!(renderer.warned_unsupported, 0);
        let _ = renderer.render_frame(1_000);
        let flags = renderer.warned_unsupported;
        assert!(flags & unsupported::ROTATION != 0, "\\frz must be flagged");
        assert!(
            flags & unsupported::KARAOKE != 0,
            "\\kf sweep approximation must be flagged"
        );
        assert!(flags & unsupported::ANIMATION != 0, "\\t must be flagged");
        // Supported-only scripts stay silent — including plain \k karaoke,
        // whose stepwise coloring is exact.
        let plain_k = "Dialogue: 0,0:00:00.00,0:00:05.00,Default,,0,0,0,,{\\k50}Kara{\\k50}oke\n";
        let mut clean = renderer_with(plain_k).expect("font probed above");
        let _ = clean.render_frame(1_000);
        assert_eq!(clean.warned_unsupported, 0, "\\k must not warn");
        // A bare \frz reset with zero style angle is a genuine no-op.
        let noop = "Dialogue: 0,0:00:00.00,0:00:05.00,Default,,0,0,0,,{\\frz}Reset\n";
        let mut noop_renderer = renderer_with(noop).expect("font probed above");
        let _ = noop_renderer.render_frame(1_000);
        assert_eq!(
            noop_renderer.warned_unsupported, 0,
            "\\frz reset-to-0 must not warn"
        );
    }

    #[test]
    fn karaoke_steps_secondary_to_primary_at_syllable_start() {
        // {\k100}A{\k100}B: at t=0.5s libass paints A primary and B
        // secondary; at t=1.5s both are primary. Karaoke also disables the
        // static-frame cache (time-dependent).
        let events = "Dialogue: 0,0:00:00.00,0:00:05.00,Default,,0,0,0,,{\\k100}A{\\k100}B\n";
        let Some(mut renderer) = renderer_with(events) else {
            eprintln!("skipping: no known test font present on this machine");
            return;
        };
        let count_secondary = |renderer: &mut PureRenderer, ms: i64| -> usize {
            renderer
                .render_frame(ms)
                .iter()
                // Default style: primary white (FFFFFF), secondary red (FF0000
                // in RGB — libass default secondary_colour is 0x00FFFF00 BGR).
                .filter(|image| image.color >> 8 == 0xFF0000)
                .count()
        };
        assert!(
            count_secondary(&mut renderer, 500) > 0,
            "unsung syllable must use SecondaryColour"
        );
        assert_eq!(
            count_secondary(&mut renderer, 1_500),
            0,
            "after its start every syllable is primary"
        );
    }

    #[test]
    fn repeated_hard_breaks_keep_empty_lines() {
        // libass FORCEBREAK: A\N\NB spans three lines — the empty middle
        // line keeps its height, pushing A higher than in A\NB.
        let double = "Dialogue: 0,0:00:00.00,0:00:05.00,Default,,0,0,0,,A\\N\\NB\n";
        let single = "Dialogue: 0,0:00:00.00,0:00:05.00,Default,,0,0,0,,A\\NB\n";
        let (Some(mut with_gap), Some(mut without_gap)) =
            (renderer_with(double), renderer_with(single))
        else {
            eprintln!("skipping: no known test font present on this machine");
            return;
        };
        let top = |renderer: &mut PureRenderer| {
            renderer
                .render_frame(1_000)
                .iter()
                .map(|o| o.dst_y)
                .min()
                .expect("some overlay")
        };
        let gap_top = top(&mut with_gap);
        let plain_top = top(&mut without_gap);
        assert!(
            gap_top < plain_top,
            "empty line must add height: {gap_top} !< {plain_top}"
        );
    }

    #[test]
    fn play_res_defaults_match_lazy_track_init() {
        let path = test_util::test_font();
        let Some(path) = path else {
            eprintln!("skipping: no known test font present on this machine");
            return;
        };
        let mut script = ass::parse(
            "[Script Info]\nScriptType: v4.00+\n[Events]\nDialogue: 0,0:00:00.00,0:00:01.00,Default,,0,0,0,,x\n",
        )
        .expect("parse");
        script.play_res_x = 0;
        script.play_res_y = 0;
        let mut fonts = FontStore::new(false);
        fonts.load_default_font_file(std::path::Path::new(path));
        let mut renderer = PureRenderer::new(script, fonts, RenderOptions::default());
        renderer.set_frame_size(640, 360);
        let _ = renderer.render_frame(500);
        assert_eq!(
            (renderer.script().play_res_x, renderer.script().play_res_y),
            (384, 288)
        );
    }
}
