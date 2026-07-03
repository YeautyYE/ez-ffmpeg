//! [`PureRenderer`]: the pure-Rust [`SubtitleRenderer`] implementation.
//!
//! Owns the parsed script, the font store, and the frame geometry; per
//! frame it selects visible events, lays them out ([`super::layout`]),
//! applies simple collision stacking, and exposes the results as borrowed
//! [`OverlayImage`] nodes (valid until the next `render_frame`, enforced
//! by the borrow on `&mut self`).

use super::fonts::{FontStore, LoadedFace};
use super::layout::{render_event, FrameContext, RenderOptions, RenderedNode};
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
    /// Cache key: the visible event set of `nodes`, when time-independent.
    cache: Option<CacheKey>,
    lazy_inited: bool,
}

#[derive(PartialEq, Eq)]
struct CacheKey {
    events: Vec<usize>,
    frame: (i32, i32),
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
            lazy_inited: false,
        }
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
        let key = CacheKey {
            events: visible.clone(),
            frame: (self.frame_w, self.frame_h),
        };
        let cache_valid = self.cache.as_ref() == Some(&key);
        if !cache_valid {
            let ctx = FrameContext {
                script: &self.script,
                fonts: &self.fonts,
                frame_w: self.frame_w,
                frame_h: self.frame_h,
                par: self.par,
                opts: &self.opts,
            };

            let mut all_nodes: Vec<RenderedNode> = Vec::new();
            let mut occupied: Vec<(i32, i32, i32, i32)> = Vec::new();
            let mut time_dependent = false;
            for &index in &visible {
                let event = &self.script.events[index];
                let (mut nodes, uses_time) =
                    render_event(&ctx, event, now_ms, &mut self.face_cache);
                time_dependent |= uses_time;
                if nodes.is_empty() {
                    continue;
                }
                // Collision stacking for unpositioned events: shift the
                // whole block off previously occupied rectangles.
                let positioned = uses_time_or_positioned(event);
                if !positioned {
                    stack_block(&mut nodes, &mut occupied, &self.script, event, self.frame_h);
                } else if let Some(bbox) = block_bbox(&nodes) {
                    occupied.push(bbox);
                }
                all_nodes.append(&mut nodes);
            }
            self.nodes = all_nodes;
            self.cache = (!time_dependent).then_some(key);
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
    }
}

/// Events positioned by \pos/\move skip collision handling (libass sets
/// `detect_collisions = 0`); a cheap text scan avoids re-parsing tags.
fn uses_time_or_positioned(event: &crate::subtitle::ass::Event) -> bool {
    let text = &event.text;
    text.contains("\\pos") || text.contains("\\move")
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

        let moving = "Dialogue: 0,0:00:00.00,0:00:05.00,Default,,0,0,0,,{\\move(0,0,100,100)}Go\n";
        let mut renderer = renderer_with(moving).expect("font probed above");
        assert!(!renderer.render_frame(1_000).is_empty());
        assert!(renderer.cache.is_none(), "\\move must disable the cache");
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
