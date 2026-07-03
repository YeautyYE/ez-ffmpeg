//! [`PureRenderer`]: the pure-Rust [`SubtitleRenderer`] implementation.
//!
//! Owns the parsed script, the font store, and the frame geometry; per
//! frame it selects visible events, lays them out ([`super::layout`]),
//! applies simple collision stacking, and exposes the results as borrowed
//! [`OverlayImage`] nodes (valid until the next `render_frame`, enforced
//! by the borrow on `&mut self`).

use super::fonts::{FontStore, LoadedFace};
use super::layout::{
    render_event, unsupported, EventDynamics, EventSamples, FrameContext, RenderOptions,
    RenderedNode,
};
use crate::subtitle::ass::{Script, VALIGN_CENTER, VALIGN_TOP};
use crate::subtitle::backend::SubtitleRenderer;
use crate::subtitle::blend::OverlayImage;
use std::collections::HashMap;
use std::sync::Arc;

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
    face_cache: HashMap<(String, u16, bool), Option<Arc<LoadedFace>>>,
    /// Nodes of the last rendered frame (backing store for the borrows).
    nodes: Vec<RenderedNode>,
    /// Key describing exactly what `nodes` renders; see [`CacheKey`].
    cache: Option<CacheKey>,
    /// Time-dependent inputs captured per event index on its first cold
    /// render. Specs live in script coordinates/milliseconds, so geometry
    /// changes don't stale them; the script is immutable after parse.
    dynamics: HashMap<usize, EventDynamics>,
    lazy_inited: bool,
    /// `unsupported::*` features already warned about (once per feature).
    warned_unsupported: u32,
    /// Cold (cache-miss) render count, observable by cache tests.
    #[cfg(test)]
    cold_renders: u64,
}

/// Frame-cache identity: the visible event set, the output geometry, and
/// the per-event time samples. Equal keys imply byte-identical output, so
/// animated events hit the cache on every frame where their resolved
/// dynamic values (fade plateau, karaoke steady state, settled \move) are
/// unchanged — only frames where a sample actually moves re-render.
#[derive(PartialEq, Eq)]
struct CacheKey {
    events: Vec<usize>,
    frame: (i32, i32),
    /// Effective storage size (falls back to the frame size): it scales
    /// borders/blur/shadows, so it must key the cache even though the
    /// public setter also invalidates on change.
    storage: (i32, i32),
    samples: Vec<EventSamples>,
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
            cache: None,
            dynamics: HashMap::new(),
            lazy_inited: false,
            warned_unsupported: 0,
            #[cfg(test)]
            cold_renders: 0,
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
        }
        self.frame_w = width;
        self.frame_h = height;
    }

    fn set_storage_size(&mut self, width: i32, height: i32) {
        if (self.storage_w, self.storage_h) != (width, height) {
            self.cache = None;
        }
        self.storage_w = width;
        self.storage_h = height;
    }

    fn set_pixel_aspect(&mut self, par: f64) {
        if self.par != par {
            self.cache = None;
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
        // never rendered has no captured record yet and forces a miss — a
        // made-up "static" sample could wrongly hit for a dynamic event.
        let samples: Option<Vec<EventSamples>> = visible
            .iter()
            .map(|&index| {
                self.dynamics.get(&index).map(|dynamics| {
                    let event = &self.script.events[index];
                    dynamics.sample(now_ms - event.start_ms, event.duration_ms)
                })
            })
            .collect();

        let cache_valid = match (&self.cache, &samples) {
            (Some(cache), Some(samples)) => {
                cache.events == visible
                    && cache.frame == frame
                    && cache.storage == storage
                    && &cache.samples == samples
            }
            _ => false,
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

            let mut all_nodes: Vec<RenderedNode> = Vec::new();
            let mut occupied: Vec<(i32, i32, i32, i32)> = Vec::new();
            let mut seen_unsupported = 0u32;
            for &index in &visible {
                let event = &self.script.events[index];
                let mut rendered = render_event(&ctx, event, now_ms, &mut self.face_cache);
                seen_unsupported |= rendered.unsupported;
                let detect_collisions = rendered.detect_collisions;
                self.dynamics.insert(index, rendered.dynamics);
                let nodes = &mut rendered.nodes;
                if nodes.is_empty() {
                    continue;
                }
                // Collision stacking for unpositioned events: shift the
                // whole block off previously occupied rectangles.
                if detect_collisions {
                    stack_block(nodes, &mut occupied, &self.script, event, self.frame_h);
                } else if let Some(bbox) = block_bbox(nodes) {
                    occupied.push(bbox);
                }
                all_nodes.append(nodes);
            }
            self.nodes = all_nodes;
            // Key the result by the samples the render actually used — the
            // records are complete now, so re-resolve any that were missing.
            let samples = visible
                .iter()
                .map(|&index| {
                    let event = &self.script.events[index];
                    self.dynamics[&index].sample(now_ms - event.start_ms, event.duration_ms)
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
            .filter(|node| !node.bitmap.is_empty())
            .map(|node| OverlayImage {
                w: node.bitmap.w,
                h: node.bitmap.h,
                stride: node.bitmap.w,
                bitmap: node.bitmap.data.as_slice(),
                color: node.color,
                dst_x: node.bitmap.x,
                dst_y: node.bitmap.y,
            })
            .collect()
    }

    fn teardown(&mut self) {
        self.nodes.clear();
        self.face_cache.clear();
        self.cache = None;
        self.dynamics.clear();
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

    /// \fad plateau frames resolve to a constant fade sample, so only the
    /// interpolating fade-in/out frames pay a cold render.
    #[test]
    fn fade_plateau_frames_hit_the_cache() {
        let fading = "Dialogue: 0,0:00:00.00,0:00:05.00,Default,,0,0,0,,{\\fad(500,500)}Plateau\n";
        let Some(mut renderer) = renderer_with(fading) else {
            eprintln!("skipping: no known test font present on this machine");
            return;
        };
        let _ = renderer.render_frame(1_000); // plateau
        let _ = renderer.render_frame(2_000); // plateau: same sample
        let _ = renderer.render_frame(3_500); // plateau: same sample
        assert_eq!(renderer.cold_renders, 1, "plateau frames must cache-hit");
        let _ = renderer.render_frame(100); // fade-in: interpolating
        let _ = renderer.render_frame(200); // fade-in: different sample
        assert_eq!(renderer.cold_renders, 3, "interpolating frames re-render");
        // NOT 4_800: its fade-out sample (255*0.6) equals the 200ms fade-in
        // sample and legitimately cache-hits — equal samples, equal bytes.
        let _ = renderer.render_frame(4_700); // fade-out: 255*0.4
        assert_eq!(renderer.cold_renders, 4);
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
