//! Event layout: the styling state machine driven by override tags, text
//! chunking, line wrapping, alignment, and rasterization orchestration.
//!
//! The semantics follow libass 0.17.1 `ass_render.c` / `ass_parse.c` where
//! implemented; deliberate M1 simplifications (documented in the module
//! README): `\t` renders the initial state, karaoke renders unswept text,
//! `\frx/\fry/\frz/\fax/\fay` rotation/shear are not applied, vector clips
//! are ignored, and Banner/Scroll effects render as static dialogue.

use super::fonts::{FaceRequest, FontStore, LoadedFace};
use super::raster::{
    be_blur, border_path, drawing_commands, fill_path, gaussian_blur, CoverageBitmap, OutlinePath,
};
use super::shape::{bidi_runs, shape_complex, shape_simple};
use crate::subtitle::ass::{
    self, numpad2align, parse_drawing, parse_tag_block, Color, Event, FontSizeArg, Script, Tag,
    VALIGN_CENTER, VALIGN_SUB, VALIGN_TOP,
};
use std::collections::HashMap;

/// Renderer presentation options (the builder knobs that libass renderer
/// setters used to receive).
#[derive(Debug, Clone)]
pub(crate) struct RenderOptions {
    pub font_scale: f64,
    /// Percent (0 = author position, 100 = top) for regular sub-aligned events.
    pub line_position: f64,
    /// Letterbox margins (top, bottom, left, right), if declared.
    pub margins: Option<(i32, i32, i32, i32)>,
    /// Allow regular events onto the letterbox bars.
    pub use_margins: bool,
    /// false = simple shaper (`TextShaping::Simple`).
    pub complex_shaping: bool,
    /// Unicode line-breaking for wrapping (FFmpeg `wrap_unicode` semantics).
    pub wrap_unicode: bool,
}

impl Default for RenderOptions {
    fn default() -> Self {
        Self {
            font_scale: 1.0,
            line_position: 0.0,
            margins: None,
            use_margins: false,
            complex_shaping: true,
            wrap_unicode: false,
        }
    }
}

/// One positioned, colored coverage bitmap — owned storage behind the
/// borrowed `OverlayImage` list the backend trait hands out.
#[derive(Debug, Clone)]
pub(crate) struct RenderedNode {
    pub bitmap: CoverageBitmap,
    /// 0xRRGGBBAA with AA = ASS transparency (0 opaque, 255 invisible).
    pub color: u32,
}

/// Frame geometry shared by every event of one `render_frame` call.
pub(crate) struct FrameContext<'a> {
    pub script: &'a Script,
    pub fonts: &'a FontStore,
    pub frame_w: i32,
    pub frame_h: i32,
    /// Pixel-aspect compensation (libass `ass_set_pixel_aspect`).
    pub par: f64,
    pub opts: &'a RenderOptions,
}

impl FrameContext<'_> {
    fn scale_x(&self) -> f64 {
        f64::from(self.frame_w) / f64::from(self.script.play_res_x.max(1))
    }

    fn scale_y(&self) -> f64 {
        f64::from(self.frame_h) / f64::from(self.script.play_res_y.max(1))
    }

    /// Border/shadow scale: screen scale when ScaledBorderAndShadow, else
    /// script pixels are frame pixels.
    fn border_scale(&self) -> f64 {
        if self.script.scaled_border_and_shadow {
            self.scale_y()
        } else {
            1.0
        }
    }

    /// The rectangle regular events are laid out against: the video
    /// rectangle (frame inset by the letterbox margins) unless
    /// `use_margins` allows the whole frame.
    fn placement_rect(&self) -> (i32, i32, i32, i32) {
        match self.opts.margins {
            Some((top, bottom, left, right)) if !self.opts.use_margins => (
                left,
                top,
                (self.frame_w - left - right).max(1),
                (self.frame_h - top - bottom).max(1),
            ),
            _ => (0, 0, self.frame_w, self.frame_h),
        }
    }
}

/// The per-event styling state (libass `RenderContext`), reset from the
/// event's style and mutated by override tags.
#[derive(Debug, Clone)]
struct State {
    family: String,
    font_size: f64,
    bold: i32,
    italic: i32,
    underline: bool,
    strike: bool,
    scale_x: f64,
    scale_y: f64,
    spacing: f64,
    border_x: f64,
    border_y: f64,
    shadow_x: f64,
    shadow_y: f64,
    border_style: i32,
    blur: f64,
    be: i32,
    colors: [Color; 4],
    wrap_style: i32,
    drawing_scale: i32,
    pbo: f64,
    style_index: usize,
}

impl State {
    fn from_style(script: &Script, style_index: usize, wrap_style: i32) -> Self {
        let style = &script.styles[style_index.min(script.styles.len() - 1)];
        Self {
            family: style.font_name.clone(),
            font_size: style.font_size,
            bold: style.bold,
            italic: style.italic,
            underline: style.underline != 0,
            strike: style.strike_out != 0,
            scale_x: style.scale_x,
            scale_y: style.scale_y,
            spacing: style.spacing,
            border_x: style.outline,
            border_y: style.outline,
            shadow_x: style.shadow,
            shadow_y: style.shadow,
            border_style: style.border_style,
            blur: style.blur,
            be: 0,
            colors: [
                style.primary_colour,
                style.secondary_colour,
                style.outline_colour,
                style.back_colour,
            ],
            wrap_style,
            drawing_scale: 0,
            pbo: 0.0,
            style_index,
        }
    }
}

/// Features the renderer does not implement yet. Collected per event as a
/// bitmask so the renderer can warn exactly once per feature (silent
/// degradation would hide fidelity gaps from users).
pub(crate) mod unsupported {
    pub(crate) const ROTATION: u32 = 1 << 0;
    pub(crate) const SHEAR: u32 = 1 << 1;
    pub(crate) const ANIMATION: u32 = 1 << 2;
    pub(crate) const KARAOKE: u32 = 1 << 3;
    pub(crate) const VECTOR_CLIP: u32 = 1 << 4;
    pub(crate) const SCROLL_EFFECT: u32 = 1 << 5;

    /// One human-readable line per flag, for the once-per-feature warning.
    pub(crate) fn describe(flag: u32) -> &'static str {
        match flag {
            ROTATION => "\\frx/\\fry/\\frz rotation (and style Angle) is not applied yet; text renders unrotated",
            SHEAR => "\\fax/\\fay shear is not applied yet; text renders unsheared",
            ANIMATION => "\\t animation is not applied yet; events render in their initial state",
            KARAOKE => "\\k/\\kf/\\ko karaoke sweep is not applied yet; text renders fully filled",
            VECTOR_CLIP => "vector \\clip/\\iclip is not applied yet; only rectangular clips work",
            SCROLL_EFFECT => "Banner/Scroll transition effects are not applied yet; the event renders statically",
            _ => "unknown unsupported feature",
        }
    }
}

/// Raw `\fad`/`\fade` parameters awaiting the event-duration fixup.
#[derive(Debug, Clone, Copy)]
struct FadeSpec {
    a1: i32,
    a2: i32,
    a3: i32,
    t1: i32,
    t2: i32,
    t3: i32,
    t4: i32,
    two_arg: bool,
}

/// `\move` parameters: from, to, and the time window (0,0 = whole event).
#[derive(Debug, Clone, Copy)]
struct MoveSpec {
    from: (f64, f64),
    to: (f64, f64),
    t1: i32,
    t2: i32,
}

/// Event-level results of the tag pass (first-wins rules live here).
#[derive(Debug, Default, Clone)]
struct EventOverrides {
    pos: Option<(f64, f64)>,
    movement: Option<MoveSpec>,
    fade: Option<FadeSpec>,
    alignment: Option<i32>,
    clip: Option<(i32, i32, i32, i32, bool)>,
    uses_time: bool,
    /// `unsupported::*` flags seen while parsing this event's tags.
    unsupported: u32,
}

/// One shaped-and-styled piece of one display line.
struct Segment {
    state: State,
    kind: SegmentKind,
    /// Pixel width including inter-glyph spacing.
    width: f64,
    ascent: f64,
    descent: f64,
    /// True when this segment may be dropped at a line edge (whitespace).
    is_space: bool,
}

enum SegmentKind {
    Text {
        face: std::sync::Arc<LoadedFace>,
        glyphs: Vec<super::shape::ShapedGlyph>,
        /// Font-units -> pixel scales for this segment.
        x_scale: f32,
        y_scale: f32,
        /// Extra per-glyph spacing in pixels (`\fsp` + tracking).
        letter_spacing: f64,
    },
    Drawing {
        drawing: ass::Drawing,
        /// Drawing-units -> pixel scales (includes the `\p` power).
        x_scale: f32,
        y_scale: f32,
        /// Vertical shift (\pbo) in pixels.
        baseline_offset: f64,
    },
}

/// What one event rendered to, plus rendering metadata for the caller.
pub(crate) struct EventRender {
    pub nodes: Vec<RenderedNode>,
    /// The output depends on `now_ms` (disables the static-frame cache).
    pub uses_time: bool,
    /// `unsupported::*` features this event asked for.
    pub unsupported: u32,
}

/// Renders one event at `now_ms` into positioned nodes.
pub(crate) fn render_event(
    ctx: &FrameContext<'_>,
    event: &Event,
    now_ms: i64,
    face_cache: &mut HashMap<(String, u16, bool), Option<std::sync::Arc<LoadedFace>>>,
) -> EventRender {
    let script = ctx.script;
    let base_wrap = script.wrap_style;
    let mut state = State::from_style(script, event.style, base_wrap);
    let mut overrides = EventOverrides::default();
    if script.styles[event.style.min(script.styles.len() - 1)].angle != 0.0 {
        overrides.unsupported |= unsupported::ROTATION;
    }
    if event.effect.starts_with("Banner;")
        || event.effect.starts_with("Scroll up;")
        || event.effect.starts_with("Scroll down;")
    {
        overrides.unsupported |= unsupported::SCROLL_EFFECT;
    }

    // ---- pass 1: chunk the text, applying tags ----
    let mut segments: Vec<Segment> = Vec::new();
    let mut hard_breaks: Vec<usize> = Vec::new(); // segment index a new line starts at
    let mut pending_text = String::new();

    let push_text = |text: &mut String,
                     state: &State,
                     segments: &mut Vec<Segment>,
                     face_cache: &mut HashMap<
        (String, u16, bool),
        Option<std::sync::Arc<LoadedFace>>,
    >| {
        if text.is_empty() {
            return;
        }
        build_text_segments(ctx, state, text, segments, face_cache);
        text.clear();
    };

    let bytes = event.text.as_bytes();
    let mut i = 0usize;
    while i < bytes.len() {
        if bytes[i] == b'{' {
            if let Some(close) = event.text[i..].find('}') {
                // Tag block: flush pending text under the current state.
                push_text(&mut pending_text, &state, &mut segments, face_cache);
                let block = &event.text[i + 1..i + close];
                apply_tags(
                    script,
                    &mut state,
                    &mut overrides,
                    parse_tag_block(block),
                    base_wrap,
                );
                i += close + 1;
                continue;
            }
        }
        if state.drawing_scale > 0 {
            // Drawing mode: capture everything to the next '{' as drawing
            // text (libass: `while (*q != '{' && *q != 0) ++q`).
            let rest = &event.text[i..];
            let end = rest.find('{').unwrap_or(rest.len());
            let drawing_text = &rest[..end];
            push_text(&mut pending_text, &state, &mut segments, face_cache);
            build_drawing_segment(ctx, &state, drawing_text, &mut segments);
            i += end;
            continue;
        }
        // ass_get_next_char escapes.
        let rest = &event.text[i..];
        if let Some(stripped) = rest.strip_prefix('\\') {
            let mut chars = stripped.chars();
            match chars.next() {
                Some('N') => {
                    push_text(&mut pending_text, &state, &mut segments, face_cache);
                    hard_breaks.push(segments.len());
                    i += 2;
                    continue;
                }
                Some('n') => {
                    if state.wrap_style == 2 {
                        push_text(&mut pending_text, &state, &mut segments, face_cache);
                        hard_breaks.push(segments.len());
                    } else {
                        pending_text.push(' ');
                    }
                    i += 2;
                    continue;
                }
                Some('h') => {
                    pending_text.push('\u{a0}');
                    i += 2;
                    continue;
                }
                Some(c @ ('{' | '}')) => {
                    pending_text.push(c);
                    i += 2;
                    continue;
                }
                _ => {}
            }
        }
        let ch = rest.chars().next().expect("non-empty rest");
        pending_text.push(if ch == '\t' { ' ' } else { ch });
        i += ch.len_utf8();
    }
    push_text(&mut pending_text, &state, &mut segments, face_cache);

    if segments.is_empty() {
        return EventRender {
            nodes: Vec::new(),
            uses_time: overrides.uses_time,
            unsupported: overrides.unsupported,
        };
    }

    // ---- pass 2: wrap into lines ----
    let alignment = overrides
        .alignment
        .unwrap_or_else(|| script.styles[event.style.min(script.styles.len() - 1)].alignment);
    let (rect_x, rect_y, rect_w, rect_h) = ctx.placement_rect();

    let style = &script.styles[event.style.min(script.styles.len() - 1)];
    let margin_l = (f64::from(pick_margin(event.margin_l, style.margin_l)) * ctx.scale_x()) as i32;
    let margin_r = (f64::from(pick_margin(event.margin_r, style.margin_r)) * ctx.scale_x()) as i32;
    let margin_v = (f64::from(pick_margin(event.margin_v, style.margin_v)) * ctx.scale_y()) as i32;

    let max_width = if overrides.pos.is_some() || overrides.movement.is_some() {
        // Positioned events never wrap against the margins (VSFilter).
        f64::from(rect_w.max(ctx.frame_w))
    } else {
        f64::from((rect_w - margin_l - margin_r).max(1))
    };
    let wrap = wrap_lines(&segments, &hard_breaks, max_width, state.wrap_style);

    // ---- pass 3: position the block ----
    let line_metrics: Vec<(f64, f64, f64)> = wrap
        .iter()
        .map(|line| {
            let width: f64 = line.iter().map(|&s| segments[s].width).sum();
            let ascent = line
                .iter()
                .map(|&s| segments[s].ascent)
                .fold(0.0f64, f64::max);
            let descent = line
                .iter()
                .map(|&s| segments[s].descent)
                .fold(0.0f64, f64::max);
            (width, ascent, descent)
        })
        .collect();
    let block_h: f64 = line_metrics.iter().map(|(_, a, d)| a + d).sum();
    let block_w: f64 = line_metrics
        .iter()
        .map(|(w, _, _)| *w)
        .fold(0.0f64, f64::max);

    let halign = alignment & 3;
    let valign = alignment & 12;

    let now_rel = now_ms - event.start_ms;
    let anchor = match (overrides.pos, overrides.movement) {
        (Some((x, y)), _) => Some((x, y)),
        (
            None,
            Some(MoveSpec {
                from: (x1, y1),
                to: (x2, y2),
                t1,
                t2,
            }),
        ) => {
            let (mut t1, mut t2) = (i64::from(t1), i64::from(t2));
            if t1 <= 0 && t2 <= 0 {
                t1 = 0;
                t2 = event.duration_ms;
            }
            let k = if now_rel <= t1 {
                0.0
            } else if now_rel >= t2 {
                1.0
            } else {
                (now_rel - t1) as f64 / (t2 - t1).max(1) as f64
            };
            Some((x1 + k * (x2 - x1), y1 + k * (y2 - y1)))
        }
        _ => None,
    };

    let (block_x, block_y) = match anchor {
        Some((ax, ay)) => {
            let ax = ax * ctx.scale_x();
            let ay = ay * ctx.scale_y();
            let x = match halign {
                1 => ax,
                3 => ax - block_w,
                _ => ax - block_w / 2.0,
            };
            let y = match valign {
                VALIGN_TOP => ay,
                VALIGN_CENTER => ay - block_h / 2.0,
                _ => ay - block_h,
            };
            (x, y)
        }
        None => {
            let left = f64::from(rect_x + margin_l);
            let right = f64::from(rect_x + rect_w - margin_r);
            let x = match halign {
                1 => left,
                3 => right - block_w,
                _ => (left + right - block_w) / 2.0,
            };
            let mut y = match valign {
                VALIGN_TOP => f64::from(rect_y + margin_v),
                VALIGN_CENTER => f64::from(rect_y) + (f64::from(rect_h) - block_h) / 2.0,
                _ => f64::from(rect_y + rect_h - margin_v) - block_h,
            };
            if valign == VALIGN_SUB && ctx.opts.line_position > 0.0 {
                y -= ctx.opts.line_position / 100.0 * f64::from(ctx.frame_h);
            }
            (x, y)
        }
    };

    // ---- pass 4: rasterize lines ----
    let fade = overrides
        .fade
        .map(|f| fade_alpha(f, now_rel, event.duration_ms));
    let mut nodes: Vec<RenderedNode> = Vec::new();
    let mut shadows: Vec<RenderedNode> = Vec::new();
    let mut borders: Vec<RenderedNode> = Vec::new();
    let mut fills: Vec<RenderedNode> = Vec::new();

    let mut y_cursor = block_y;
    for (line, &(line_w, line_asc, line_desc)) in wrap.iter().zip(&line_metrics) {
        let line_x = match halign {
            1 => block_x,
            3 => block_x + (block_w - line_w),
            _ => block_x + (block_w - line_w) / 2.0,
        };
        let baseline = y_cursor + line_asc;
        let mut x_cursor = line_x;
        for &seg_index in line {
            let segment = &segments[seg_index];
            rasterize_segment(
                ctx,
                segment,
                x_cursor,
                baseline,
                fade,
                &mut shadows,
                &mut borders,
                &mut fills,
            );
            x_cursor += segment.width;
        }
        y_cursor += line_asc + line_desc;
    }
    nodes.append(&mut shadows);
    nodes.append(&mut borders);
    nodes.append(&mut fills);

    // ---- pass 5: clip ----
    if let Some((x0, y0, x1, y1, inverse)) = overrides.clip {
        let sx = ctx.scale_x();
        let sy = ctx.scale_y();
        let (cx0, cy0) = ((f64::from(x0) * sx) as i32, (f64::from(y0) * sy) as i32);
        let (cx1, cy1) = ((f64::from(x1) * sx) as i32, (f64::from(y1) * sy) as i32);
        for node in &mut nodes {
            node.bitmap.clip_rect(cx0, cy0, cx1, cy1, inverse);
        }
    }

    nodes.retain(|node| !node.bitmap.is_empty() && (node.color & 0xFF) != 0xFF);
    EventRender {
        nodes,
        uses_time: overrides.uses_time,
        unsupported: overrides.unsupported,
    }
}

/// Non-zero event margins override the style margins (libass `halign` use).
fn pick_margin(event_margin: i32, style_margin: i32) -> i32 {
    if event_margin != 0 {
        event_margin
    } else {
        style_margin
    }
}

/// Applies one tag block to the state (libass `ass_parse_tags` application
/// side, minus animation).
fn apply_tags(
    script: &Script,
    state: &mut State,
    overrides: &mut EventOverrides,
    tags: Vec<Tag<'_>>,
    base_wrap: i32,
) {
    let style = |s: &State| script.styles[s.style_index.min(script.styles.len() - 1)].clone();
    for tag in tags {
        match tag {
            Tag::XBord(v) => state.border_x = v.map_or(style(state).outline, |v| v.max(0.0)),
            Tag::YBord(v) => state.border_y = v.map_or(style(state).outline, |v| v.max(0.0)),
            Tag::XShad(v) => state.shadow_x = v.unwrap_or(style(state).shadow),
            Tag::YShad(v) => state.shadow_y = v.unwrap_or(style(state).shadow),
            Tag::Bord(v) => {
                let val = v.map_or(style(state).outline, |v| v.max(0.0));
                state.border_x = val;
                state.border_y = val;
            }
            Tag::Shad(v) => {
                let val = v.map_or(style(state).shadow, |v| v.max(0.0));
                state.shadow_x = val;
                state.shadow_y = val;
            }
            Tag::Blur(v) => state.blur = v.map_or(0.0, |v| v.clamp(0.0, 100.0)),
            Tag::Be(v) => {
                state.be = v.map_or(0, |v| ((v + 0.5) as i32).clamp(0, 127));
            }
            Tag::FontScaleX(v) => state.scale_x = v.map_or(style(state).scale_x, |v| v.max(0.0)),
            Tag::FontScaleY(v) => state.scale_y = v.map_or(style(state).scale_y, |v| v.max(0.0)),
            Tag::ResetFontScale => {
                let s = style(state);
                state.scale_x = s.scale_x;
                state.scale_y = s.scale_y;
            }
            Tag::Spacing(v) => state.spacing = v.unwrap_or(style(state).spacing),
            Tag::FontSize(arg) => {
                let val = match arg {
                    Some(FontSizeArg::Absolute(v)) => v,
                    Some(FontSizeArg::RelativeFactor(v)) => state.font_size * (1.0 + v / 10.0),
                    None => 0.0,
                };
                state.font_size = if val > 0.0 {
                    val
                } else {
                    style(state).font_size
                };
            }
            Tag::FontName(name) => {
                state.family = name
                    .map(str::to_string)
                    .unwrap_or_else(|| style(state).font_name);
            }
            Tag::Bold(v) => {
                let val = v.filter(|v| *v == 0 || *v == 1 || *v >= 100);
                state.bold = val.unwrap_or(style(state).bold);
            }
            Tag::Italic(v) => {
                let val = v.filter(|v| *v == 0 || *v == 1);
                state.italic = val.unwrap_or(style(state).italic);
            }
            Tag::Underline(v) => {
                let val = v.filter(|v| *v == 0 || *v == 1);
                state.underline = val.unwrap_or(style(state).underline) != 0;
            }
            Tag::Strike(v) => {
                let val = v.filter(|v| *v == 0 || *v == 1);
                state.strike = val.unwrap_or(style(state).strike_out) != 0;
            }
            Tag::Color(index, color) => {
                let base = style(state);
                let fallback = [
                    base.primary_colour,
                    base.secondary_colour,
                    base.outline_colour,
                    base.back_colour,
                ][index];
                let new = color.unwrap_or(fallback);
                let t = state.colors[index].t;
                state.colors[index] = Color { t, ..new };
            }
            Tag::Alpha(index, alpha) => {
                let base = style(state);
                let fallback = [
                    base.primary_colour,
                    base.secondary_colour,
                    base.outline_colour,
                    base.back_colour,
                ][index]
                    .t;
                state.colors[index].t = alpha.map_or(fallback, |a| a.clamp(0, 255) as u8);
            }
            Tag::AlphaAll(alpha) => match alpha {
                Some(a) => {
                    let a = a.clamp(0, 255) as u8;
                    for c in &mut state.colors {
                        c.t = a;
                    }
                }
                None => {
                    let base = style(state);
                    state.colors[0].t = base.primary_colour.t;
                    state.colors[1].t = base.secondary_colour.t;
                    state.colors[2].t = base.outline_colour.t;
                    state.colors[3].t = base.back_colour.t;
                }
            },
            Tag::AlignNumpad(v) => {
                if overrides.alignment.is_none() {
                    overrides.alignment = Some(if (1..=9).contains(&v) {
                        numpad2align(v)
                    } else {
                        style(state).alignment
                    });
                }
            }
            Tag::AlignLegacy(v) => {
                if overrides.alignment.is_none() {
                    overrides.alignment = Some(if (1..=11).contains(&v) {
                        // VSFilter quirk: illegal \a8 and \a4 act like \a5.
                        if v & 3 == 0 {
                            5
                        } else {
                            v
                        }
                    } else {
                        style(state).alignment
                    });
                }
            }
            Tag::Pos { x, y } => {
                if overrides.pos.is_none() && overrides.movement.is_none() {
                    overrides.pos = Some((x, y));
                }
            }
            Tag::Move {
                x1,
                y1,
                x2,
                y2,
                t1,
                t2,
            } => {
                if overrides.pos.is_none() && overrides.movement.is_none() {
                    overrides.movement = Some(MoveSpec {
                        from: (x1, y1),
                        to: (x2, y2),
                        t1,
                        t2,
                    });
                    overrides.uses_time = true;
                }
            }
            Tag::Fade {
                a1,
                a2,
                a3,
                t1,
                t2,
                t3,
                t4,
                two_arg,
            } => {
                if overrides.fade.is_none() {
                    overrides.fade = Some(FadeSpec {
                        a1,
                        a2,
                        a3,
                        t1,
                        t2,
                        t3,
                        t4,
                        two_arg,
                    });
                    overrides.uses_time = true;
                }
            }
            Tag::Org { .. } => {} // only meaningful with rotation; covered by ROTATION
            Tag::Transform => {
                overrides.uses_time = true;
                overrides.unsupported |= unsupported::ANIMATION;
            }
            Tag::ClipRect {
                inverse,
                x0,
                y0,
                x1,
                y1,
            } => {
                overrides.clip = Some((x0, y0, x1, y1, inverse));
            }
            Tag::ClipVector { .. } => overrides.unsupported |= unsupported::VECTOR_CLIP,
            Tag::Reset(style_name) => {
                let index = match style_name {
                    Some(name) => lookup_style_strict(script, name).unwrap_or(state.style_index),
                    None => state.style_index,
                };
                let wrap = state.wrap_style;
                *state = State::from_style(script, index, wrap);
            }
            Tag::WrapStyle(v) => {
                state.wrap_style = v.filter(|v| (0..=3).contains(v)).unwrap_or(base_wrap);
            }
            Tag::DrawScale(v) => state.drawing_scale = v,
            Tag::DrawBaselineOffset(v) => state.pbo = v,
            Tag::RotX(v) | Tag::RotY(v) | Tag::RotZ(v) => {
                // A bare reset (or explicit 0) with a zero style angle is a
                // true no-op; anything else would rotate.
                let effective = v.unwrap_or(style(state).angle);
                if effective != 0.0 {
                    overrides.unsupported |= unsupported::ROTATION;
                }
            }
            Tag::ShearX(v) | Tag::ShearY(v) => {
                if v.unwrap_or(0.0) != 0.0 {
                    overrides.unsupported |= unsupported::SHEAR;
                }
            }
            Tag::Karaoke => overrides.unsupported |= unsupported::KARAOKE,
            Tag::FontEncoding(_) => {} // font-matching hint; harmless to ignore
        }
    }
}

/// `lookup_style_strict` (\r): exact-name backwards search, no fallback.
fn lookup_style_strict(script: &Script, name: &str) -> Option<usize> {
    script.styles.iter().rposition(|style| style.name == name)
}

/// `interpolate_alpha` + the \fad fixup: returns the fade transparency
/// multiplier (0 = no fade) at `now` (ms relative to event start).
fn fade_alpha(spec: FadeSpec, now: i64, duration: i64) -> i32 {
    let FadeSpec {
        a1,
        a2,
        a3,
        t1,
        t2,
        t3,
        t4,
        two_arg,
    } = spec;
    let (t1, t4, t3) = if two_arg && t1 == -1 && t4 == -1 {
        (0, duration as i32, duration as i32 - t3)
    } else {
        (t1, t4, t3)
    };
    let now = now as i32;
    if now < t1 {
        a1
    } else if now < t2 {
        let cf = f64::from(now - t1) / f64::from((t2 - t1).max(1));
        (f64::from(a1) * (1.0 - cf) + f64::from(a2) * cf) as i32
    } else if now < t3 {
        a2
    } else if now < t4 {
        let cf = f64::from(now - t3) / f64::from((t4 - t3).max(1));
        (f64::from(a2) * (1.0 - cf) + f64::from(a3) * cf) as i32
    } else {
        a3
    }
}

/// `mult_alpha`: transparency composition.
fn mult_alpha(a: u32, b: u32) -> u32 {
    a - (a * b + 0x7F) / 0xFF + b
}

fn node_color(color: Color, fade: Option<i32>) -> u32 {
    let mut t = u32::from(color.t);
    if let Some(fade) = fade {
        // VSFilter compatibility: apply fade only when positive.
        if fade > 0 {
            t = mult_alpha(t, fade.clamp(0, 255) as u32).min(255);
        }
    }
    u32::from(color.r) << 24 | u32::from(color.g) << 16 | u32::from(color.b) << 8 | t
}

/// Splits `text` into word/space segments under one state, shaping each.
fn build_text_segments(
    ctx: &FrameContext<'_>,
    state: &State,
    text: &str,
    segments: &mut Vec<Segment>,
    face_cache: &mut HashMap<(String, u16, bool), Option<std::sync::Arc<LoadedFace>>>,
) {
    let request = FaceRequest::from_ass(state.bold, state.italic);
    let key = (state.family.clone(), request.weight, request.italic);
    let entry = face_cache.entry(key).or_insert_with(|| {
        let face_ref = ctx.fonts.select(&state.family, request)?;
        ctx.fonts.load(&face_ref).map(std::sync::Arc::new)
    });
    let Some(face) = entry.clone() else {
        log::warn!(
            "subtitle render: no usable font for family '{}'; text dropped",
            state.family
        );
        return;
    };

    // Pixel scales: REAL_DIM sizing — the requested size is the full
    // ascender-to-descender height (ass_face_set_size).
    let metrics_face = face.face();
    let asc = f64::from(metrics_face.ascender());
    let desc = f64::from(-metrics_face.descender());
    let extent = (asc + desc).max(1.0);
    let pixel_size = state.font_size * ctx.scale_y() * ctx.opts.font_scale;
    let unit_scale = pixel_size / extent;
    // VSFilter compatibility (ass_render.c:2166): font glyphs use PlayResY
    // scaling in BOTH dimensions — text is NOT stretched by the
    // PlayResX/PlayResY ratio; `par` compensates non-square output pixels.
    let y_scale = (unit_scale * state.scale_y) as f32;
    let x_scale = (unit_scale * state.scale_x * ctx.par) as f32;
    let ascent_px = asc * f64::from(y_scale);
    let descent_px = desc * f64::from(y_scale);
    let letter_spacing = state.spacing * ctx.scale_x() * state.scale_x;

    // Break the run into words and spaces; each becomes one segment. With
    // wrap_unicode, Unicode break opportunities (e.g. between CJK chars)
    // also split, so the wrapper can break there.
    let break_after = break_opportunities(text, ctx.opts.wrap_unicode);
    let mut start = 0usize;
    for piece in split_words(text, &break_after) {
        let piece_text = &text[start..start + piece.len()];
        let is_space = piece_text.chars().all(|c| c == ' ');
        let glyphs = shape_piece(ctx, &face, piece_text);
        let width: f64 = glyphs
            .iter()
            .map(|g| f64::from(g.x_advance) * f64::from(x_scale) + letter_spacing)
            .sum();
        let end = start + piece.len();
        segments.push(Segment {
            state: state.clone(),
            kind: SegmentKind::Text {
                face: std::sync::Arc::clone(&face),
                glyphs,
                x_scale,
                y_scale,
                letter_spacing,
            },
            width,
            ascent: ascent_px,
            descent: descent_px,
            is_space,
        });
        start = end;
    }
}

fn shape_piece(
    ctx: &FrameContext<'_>,
    face: &LoadedFace,
    text: &str,
) -> Vec<super::shape::ShapedGlyph> {
    if !ctx.opts.complex_shaping {
        return shape_simple(face, text);
    }
    let mut shaped = Vec::new();
    for (range, rtl) in bidi_runs(text) {
        let mut run = shape_complex(face, &text[range.clone()], rtl);
        for glyph in &mut run {
            glyph.cluster += range.start as u32;
        }
        shaped.extend(run);
    }
    shaped
}

/// Splits into alternating non-space/space pieces (both kept); additional
/// cut points (Unicode break opportunities) split non-space pieces so the
/// wrapper can break between CJK characters.
fn split_words<'a>(text: &'a str, extra_cuts: &std::collections::HashSet<usize>) -> Vec<&'a str> {
    let mut pieces = Vec::new();
    let bytes = text.as_bytes();
    let mut i = 0usize;
    while i < bytes.len() {
        let is_space = bytes[i] == b' ';
        let mut j = i;
        while j < bytes.len() && (bytes[j] == b' ') == is_space {
            j += 1;
            if !is_space && j < bytes.len() && extra_cuts.contains(&j) {
                break;
            }
        }
        pieces.push(&text[i..j]);
        i = j;
    }
    pieces.retain(|p| !p.is_empty());
    pieces
}

/// Byte offsets after which a line break is allowed.
fn break_opportunities(text: &str, wrap_unicode: bool) -> std::collections::HashSet<usize> {
    let mut set = std::collections::HashSet::new();
    if wrap_unicode {
        for (offset, _) in unicode_linebreak::linebreaks(text) {
            set.insert(offset);
        }
    }
    set
}

fn build_drawing_segment(
    ctx: &FrameContext<'_>,
    state: &State,
    drawing_text: &str,
    segments: &mut Vec<Segment>,
) {
    let drawing = parse_drawing(drawing_text);
    let Some((min, max)) = drawing.cbox else {
        return;
    };
    // \p scale: coordinates are divided by 2^(scale-1).
    let power = f64::from(1u32 << (state.drawing_scale.max(1) - 1) as u32);
    let aspect = ctx.scale_x() / ctx.scale_y() * ctx.par;
    let x_scale = (ctx.scale_y() * ctx.opts.font_scale * state.scale_x * aspect / power) as f32;
    let y_scale = (ctx.scale_y() * ctx.opts.font_scale * state.scale_y / power) as f32;
    let width = f64::from(max.x - min.x) / 64.0 * f64::from(x_scale);
    let height = f64::from(max.y - min.y) / 64.0 * f64::from(y_scale);
    let baseline_offset = state.pbo * ctx.scale_y() * state.scale_y;
    segments.push(Segment {
        state: state.clone(),
        kind: SegmentKind::Drawing {
            drawing,
            x_scale,
            y_scale,
            baseline_offset,
        },
        width,
        ascent: height,
        descent: 0.0,
        is_space: false,
    });
}

/// Greedy wrap with hard breaks; WrapStyle 2 disables soft wrapping;
/// styles 0/3 get a balancing pass (libass smart wrapping approximation).
fn wrap_lines(
    segments: &[Segment],
    hard_breaks: &[usize],
    max_width: f64,
    wrap_style: i32,
) -> Vec<Vec<usize>> {
    let mut lines: Vec<Vec<usize>> = vec![Vec::new()];
    let mut width = 0.0f64;
    for (index, segment) in segments.iter().enumerate() {
        if hard_breaks.contains(&index) && !lines.last().expect("non-empty").is_empty() {
            lines.push(Vec::new());
            width = 0.0;
        }
        let line = lines.last_mut().expect("non-empty");
        let soft_wrap = wrap_style != 2;
        if soft_wrap && !line.is_empty() && !segment.is_space && width + segment.width > max_width {
            // Drop trailing spaces of the finished line.
            while lines
                .last()
                .expect("non-empty")
                .last()
                .is_some_and(|&s| segments[s].is_space)
            {
                lines.last_mut().expect("non-empty").pop();
            }
            lines.push(Vec::new());
            width = 0.0;
        }
        let line = lines.last_mut().expect("non-empty");
        if segment.is_space && line.is_empty() {
            continue; // leading spaces vanish at wraps
        }
        line.push(index);
        width += segment.width;
    }
    // Trailing spaces of the final line.
    while lines
        .last()
        .expect("non-empty")
        .last()
        .is_some_and(|&s| segments[s].is_space)
    {
        lines.last_mut().expect("non-empty").pop();
    }
    lines.retain(|line| !line.is_empty());
    if lines.is_empty() {
        lines.push(Vec::new());
    }

    // Balancing pass for smart wrap styles: move the last word of a wider
    // line down while it reduces the overall maximum width.
    if wrap_style == 0 || wrap_style == 3 {
        balance_lines(segments, &mut lines, max_width);
    }
    lines
}

fn balance_lines(segments: &[Segment], lines: &mut [Vec<usize>], max_width: f64) {
    let width_of = |line: &[usize]| -> f64 { line.iter().map(|&s| segments[s].width).sum() };
    loop {
        let mut improved = false;
        for i in 0..lines.len().saturating_sub(1) {
            let (head, tail) = lines.split_at_mut(i + 1);
            let upper = &mut head[i];
            let lower = &mut tail[0];
            // Candidate: move the upper line's last word (with its leading
            // space) down.
            let mut take = upper.len();
            while take > 0 && segments[upper[take - 1]].is_space {
                take -= 1;
            }
            let mut word_start = take;
            while word_start > 0 && !segments[upper[word_start - 1]].is_space {
                word_start -= 1;
            }
            if word_start == 0 {
                continue; // cannot empty a line
            }
            let moved: Vec<usize> = upper[word_start..].to_vec();
            let moved_width: f64 = moved
                .iter()
                .filter(|&&s| !segments[s].is_space)
                .map(|&s| segments[s].width)
                .sum();
            let upper_now = width_of(upper);
            let lower_now = width_of(lower);
            let upper_after = width_of(&upper[..word_start]);
            let lower_after = lower_now + moved_width + spaces_width(segments, &moved);
            let old_max = upper_now.max(lower_now);
            let new_max = upper_after.max(lower_after);
            if new_max < old_max && lower_after <= max_width {
                let moved: Vec<usize> = upper.drain(word_start..).collect();
                // Trim the trailing spaces that led the moved word.
                while upper.last().is_some_and(|&s| segments[s].is_space) {
                    upper.pop();
                }
                let mut new_lower = moved;
                new_lower.extend(lower.iter().copied());
                *lower = new_lower;
                improved = true;
            }
        }
        if !improved {
            break;
        }
    }
}

fn spaces_width(segments: &[Segment], moved: &[usize]) -> f64 {
    moved
        .iter()
        .filter(|&&s| segments[s].is_space)
        .map(|&s| segments[s].width)
        .sum()
}

/// Rasterizes one segment at `(x, baseline)` into shadow/border/fill nodes.
#[allow(clippy::too_many_arguments)]
fn rasterize_segment(
    ctx: &FrameContext<'_>,
    segment: &Segment,
    x: f64,
    baseline: f64,
    fade: Option<i32>,
    shadows: &mut Vec<RenderedNode>,
    borders: &mut Vec<RenderedNode>,
    fills: &mut Vec<RenderedNode>,
) {
    let state = &segment.state;
    let commands = match &segment.kind {
        SegmentKind::Text {
            face: loaded,
            glyphs,
            x_scale,
            y_scale,
            letter_spacing,
        } => {
            let mut all = Vec::new();
            let mut pen_x = x;
            for glyph in glyphs {
                let mut path = OutlinePath::new(
                    *x_scale,
                    *y_scale,
                    (pen_x + f64::from(glyph.x_offset) * f64::from(*x_scale)) as f32,
                    (baseline - f64::from(glyph.y_offset) * f64::from(*y_scale)) as f32,
                );
                loaded
                    .face()
                    .outline_glyph(ttf_parser::GlyphId(glyph.glyph_id), &mut path);
                all.extend(path.commands);
                pen_x += f64::from(glyph.x_advance) * f64::from(*x_scale) + letter_spacing;
            }
            // Underline / strikethrough as filled rectangles.
            if state.underline || state.strike {
                let face = loaded.face();
                let upem_scale = f64::from(*y_scale);
                if state.underline {
                    let metrics = face.underline_metrics();
                    let (pos, thick) = metrics
                        .map(|m| (f64::from(m.position), f64::from(m.thickness)))
                        .unwrap_or((-100.0, 50.0));
                    push_rect(
                        &mut all,
                        x,
                        baseline - pos * upem_scale,
                        segment.width,
                        (thick * upem_scale).max(1.0),
                    );
                }
                if state.strike {
                    let metrics = face.strikeout_metrics();
                    let (pos, thick) = metrics
                        .map(|m| (f64::from(m.position), f64::from(m.thickness)))
                        .unwrap_or((250.0, 50.0));
                    push_rect(
                        &mut all,
                        x,
                        baseline - pos * upem_scale,
                        segment.width,
                        (thick * upem_scale).max(1.0),
                    );
                }
            }
            all
        }
        SegmentKind::Drawing {
            drawing,
            x_scale,
            y_scale,
            baseline_offset,
        } => {
            let Some((min, _)) = drawing.cbox else {
                return;
            };
            let dx = x - f64::from(min.x) / 64.0 * f64::from(*x_scale);
            let dy = baseline - segment.ascent - f64::from(min.y) / 64.0 * f64::from(*y_scale)
                + baseline_offset;
            drawing_commands(drawing, *x_scale, *y_scale, dx as f32, dy as f32)
        }
    };
    if commands.is_empty() {
        return;
    }

    let fill = fill_path(&commands);
    if fill.is_empty() {
        return;
    }
    let border_scale = ctx.border_scale();
    let border_px = (state.border_x.max(state.border_y) * border_scale) as f32;
    let shadow_dx = state.shadow_x * border_scale;
    let shadow_dy = state.shadow_y * border_scale;

    let mut border_bitmap = CoverageBitmap::default();
    if state.border_style == 3 {
        // Opaque box: the segment's bounding rectangle in the outline color.
        let mut rect = Vec::new();
        push_rect(
            &mut rect,
            x - f64::from(border_px),
            baseline - segment.ascent - f64::from(border_px),
            segment.width + 2.0 * f64::from(border_px),
            segment.ascent + segment.descent + 2.0 * f64::from(border_px),
        );
        border_bitmap = fill_path(&rect);
    } else if border_px > 0.0 {
        border_bitmap = border_path(&commands, &fill, border_px);
    }

    // Effects: \be then \blur, applied to the outermost shape (border when
    // present, else the fill), like libass.
    let mut effect_target = if border_bitmap.is_empty() {
        fill.clone()
    } else {
        border_bitmap.clone()
    };
    if state.be > 0 {
        be_blur(&mut effect_target, state.be);
    }
    if state.blur > 0.0 {
        gaussian_blur(&mut effect_target, state.blur * border_scale);
    }
    let effects_applied = state.be > 0 || state.blur > 0.0;
    if effects_applied {
        if border_bitmap.is_empty() {
            // No border: the blurred fill replaces the fill.
            fills.push(RenderedNode {
                bitmap: effect_target.clone(),
                color: node_color(state.colors[0], fade),
            });
        } else {
            border_bitmap = effect_target.clone();
        }
    }

    // Shadow: the outermost shape offset, in the back color.
    if (shadow_dx != 0.0 || shadow_dy != 0.0) && state.shadow_x.max(state.shadow_y) > 0.0 {
        let source = if border_bitmap.is_empty() {
            &fill
        } else {
            &border_bitmap
        };
        let mut shadow = source.clone();
        shadow.x += shadow_dx as i32;
        shadow.y += shadow_dy as i32;
        shadows.push(RenderedNode {
            bitmap: shadow,
            color: node_color(state.colors[3], fade),
        });
    }

    if !border_bitmap.is_empty() {
        borders.push(RenderedNode {
            bitmap: border_bitmap,
            color: node_color(state.colors[2], fade),
        });
    }
    if !(effects_applied && border_px <= 0.0 && state.border_style != 3) {
        fills.push(RenderedNode {
            bitmap: fill,
            color: node_color(state.colors[0], fade),
        });
    }
}

fn push_rect(commands: &mut Vec<zeno::Command>, x: f64, y: f64, w: f64, h: f64) {
    use zeno::{Command, Vector};
    commands.push(Command::MoveTo(Vector::new(x as f32, y as f32)));
    commands.push(Command::LineTo(Vector::new((x + w) as f32, y as f32)));
    commands.push(Command::LineTo(Vector::new((x + w) as f32, (y + h) as f32)));
    commands.push(Command::LineTo(Vector::new(x as f32, (y + h) as f32)));
    commands.push(Command::Close);
}
