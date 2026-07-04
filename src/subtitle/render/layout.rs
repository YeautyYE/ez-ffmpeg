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
    be_blur, border_path, drawing_commands, fill_path, fix_outline, gaussian_blur, CoverageBitmap,
    OutlinePath, SharedBitmap,
};
use super::shape::{bidi_runs, shape_complex, shape_simple};
use crate::subtitle::ass::{
    self, numpad2align, parse_drawing, parse_tag_block, Color, Event, FontSizeArg, KaraokeKind,
    Script, Tag, VALIGN_CENTER, VALIGN_SUB, VALIGN_TOP,
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
/// borrowed `OverlayImage` list the backend trait hands out. The pixel
/// payload is refcounted (`SharedBitmap`) so template stores and replays
/// clone nodes without copying coverage data.
#[derive(Debug, Clone)]
pub(crate) struct RenderedNode {
    pub bitmap: SharedBitmap,
    /// 0xRRGGBBAA with AA = ASS transparency (0 opaque, 255 invisible).
    pub color: u32,
}

/// Frame geometry shared by every event of one `render_frame` call.
pub(crate) struct FrameContext<'a> {
    pub script: &'a Script,
    pub fonts: &'a FontStore,
    pub frame_w: i32,
    pub frame_h: i32,
    /// `ass_set_storage_size` — libass `ass_layout_res` resolves to this
    /// (vf_subtitles always sets it: original_size or the frame size).
    pub storage_w: i32,
    pub storage_h: i32,
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

    /// libass `blur_scale_*`: frame over layout resolution (the storage
    /// size in the vf_subtitles wiring).
    fn blur_scale_x(&self) -> f64 {
        f64::from(self.frame_w) / f64::from(self.storage_w.max(1))
    }

    fn blur_scale_y(&self) -> f64 {
        f64::from(self.frame_h) / f64::from(self.storage_h.max(1))
    }

    /// Border/shadow scales (libass `init_font_scale`): the screen scale
    /// under ScaledBorderAndShadow, else the blur scale (script border
    /// values are storage pixels).
    fn border_scale_x(&self) -> f64 {
        if self.script.scaled_border_and_shadow {
            self.scale_x()
        } else {
            self.blur_scale_x()
        }
    }

    fn border_scale_y(&self) -> f64 {
        if self.script.scaled_border_and_shadow {
            self.scale_y()
        } else {
            self.blur_scale_y()
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
    /// Karaoke registers (libass `effect_*`): sticky effect kind plus the
    /// pending syllable duration / skip offset, snapshotted into each
    /// produced segment and then cleared — exactly like GlyphInfo capture.
    effect_kind: Option<KaraokeKind>,
    effect_timing_ms: i64,
    effect_skip_ms: i64,
    effect_reset: bool,
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
            // Match the inline \blur clamp (apply_tags): a force-style or
            // nonstandard style Blur is otherwise unbounded and drives the
            // gaussian padding allocation.
            blur: style.blur.clamp(0.0, 100.0),
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
            effect_kind: None,
            effect_timing_ms: 0,
            effect_skip_ms: 0,
            effect_reset: false,
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
    pub(crate) const VERTICAL_TEXT: u32 = 1 << 6;

    /// One human-readable line per flag, for the once-per-feature warning.
    pub(crate) fn describe(flag: u32) -> &'static str {
        match flag {
            ROTATION => "\\frx/\\fry/\\frz rotation (and style Angle) is not applied yet; text renders unrotated",
            SHEAR => "\\fax/\\fay shear is not applied yet; text renders unsheared",
            ANIMATION => "\\t animation is not applied yet; events render in their initial state",
            KARAOKE => "\\kf/\\K karaoke sweep is approximated stepwise (switches at the syllable midpoint)",
            VECTOR_CLIP => "vector \\clip/\\iclip is not applied yet; only rectangular clips work",
            SCROLL_EFFECT => "Banner/Scroll transition effects are not applied yet; the event renders statically",
            VERTICAL_TEXT => "@-prefixed vertical layout is not applied yet; text renders horizontally",
            _ => "unknown unsupported feature",
        }
    }
}

/// Raw `\fad`/`\fade` parameters awaiting the event-duration fixup.
#[derive(Debug, Clone, Copy, PartialEq)]
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
#[derive(Debug, Clone, Copy, PartialEq)]
struct MoveSpec {
    from: (f64, f64),
    to: (f64, f64),
    t1: i32,
    t2: i32,
}

/// The per-segment karaoke inputs — exactly the fields the karaoke flag
/// evaluator reads. Captured from the real segment stream, NOT re-derived
/// from tags: `split_words` spreads one run across several segments and
/// only the first carries the timing snapshot, and `\kt` resets live on
/// the segment too.
#[derive(Debug, Clone, Copy, PartialEq)]
pub(crate) struct KaraokeSeg {
    run_start: bool,
    kind: Option<KaraokeKind>,
    reset: bool,
    skip_ms: i64,
    timing_ms: i64,
}

impl KaraokeSeg {
    fn of(segment: &Segment) -> Self {
        Self {
            run_start: segment.run_start,
            kind: segment.state.effect_kind,
            reset: segment.karaoke_reset,
            skip_ms: segment.karaoke_skip_ms,
            timing_ms: segment.karaoke_timing_ms,
        }
    }
}

/// The time-dependent inputs of one rendered event. Everything else that
/// reaches the rasterizer is frame-invariant for a given event (time enters
/// layout only through `eval_move_anchor`, `fade_alpha` and the karaoke
/// flags), so equal [`EventSamples`] imply byte-identical rendered nodes.
#[derive(Debug, Default, Clone, PartialEq)]
pub(crate) struct EventDynamics {
    fade: Option<FadeSpec>,
    /// Only set when the movement actually drives the anchor (`\pos` wins).
    movement: Option<MoveSpec>,
    /// Per-segment karaoke snapshot; empty when no segment carries a
    /// karaoke effect (the flags are then all-false at every timestamp).
    karaoke: Vec<KaraokeSeg>,
}

/// Time-resolved values of [`EventDynamics`] at one timestamp.
#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct EventSamples {
    fade: Option<i32>,
    /// `to_bits` of the interpolated anchor — exact-phase comparison, no
    /// quantization (NaN/negative zero can only cause a conservative miss).
    anchor_bits: Option<(u64, u64)>,
    karaoke: Vec<(bool, bool)>,
}

/// The mask-affecting subset of [`EventSamples`]: nodes are rendered with
/// unfaded colors and the fade VALUE is applied at emission, so masks (and
/// stacking) depend on the fade only through two booleans — whether it is
/// active (`fill_in_border`/`fill_in_shadow` gating) and whether it is
/// saturated at 255 (every node goes fully transparent and is dropped
/// before stacking).
#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct MaskSamples {
    fade: Option<(bool, bool)>,
    anchor_bits: Option<(u64, u64)>,
    karaoke: Vec<(bool, bool)>,
}

impl EventSamples {
    /// The mask-identity view of these samples (see [`MaskSamples`]).
    ///
    /// Known narrow divergence from the pre-split renderer: a node whose
    /// unfaded transparency saturates to 255 mid-fade (near-transparent
    /// colors, `(255 - t) * (255 - fade) <= 127`) used to be dropped
    /// before collision stacking, shrinking the occupied box for that
    /// frame; it now keeps its bbox (pixels are identical — emission
    /// alpha 0 is skipped by the blend). The new behavior is stable
    /// across the fade and matches libass, whose collision handling is
    /// computed from layout geometry and never varies with fade.
    pub(crate) fn mask_key(&self) -> MaskSamples {
        MaskSamples {
            // The mask-active bit must match apply_fade (fade > 0 dims the
            // fill; <= 0 is a no-op). Saturation means the post-clamp
            // multiplier is 255 (every node fully transparent), so >=.
            fade: self.fade.map(|f| (f > 0, f >= 255)),
            anchor_bits: self.anchor_bits,
            karaoke: self.karaoke.clone(),
        }
    }

    /// The emission-only fade multiplier at this timestamp.
    pub(crate) fn fade(&self) -> Option<i32> {
        self.fade
    }
}

impl EventDynamics {
    /// Resolves the dynamic inputs at `now_rel` ms since event start. Uses
    /// the same evaluators as the render path, so equal samples guarantee
    /// an equal render.
    pub(crate) fn sample(&self, now_rel: i64, duration_ms: i64) -> EventSamples {
        EventSamples {
            fade: self.fade.map(|f| fade_alpha(f, now_rel, duration_ms)),
            anchor_bits: self.movement.map(|m| {
                let (x, y) = eval_move_anchor(m, now_rel, duration_ms);
                (x.to_bits(), y.to_bits())
            }),
            karaoke: if self.karaoke.is_empty() {
                Vec::new()
            } else {
                karaoke_flags_from(&self.karaoke, now_rel)
            },
        }
    }
}

/// Event-level results of the tag pass (first-wins rules live here).
#[derive(Debug, Default, Clone)]
struct EventOverrides {
    pos: Option<(f64, f64)>,
    movement: Option<MoveSpec>,
    fade: Option<FadeSpec>,
    alignment: Option<i32>,
    clip: Option<(i32, i32, i32, i32, bool)>,
    /// \org or \t seen: libass sets `detect_collisions = 0` for these too,
    /// even when the tag is otherwise a no-op (bare \t, \org without
    /// rotation).
    disable_collisions: bool,
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
    /// First segment of a style run (a `push_text` batch or a drawing) —
    /// the karaoke grouping boundary, like libass `starts_new_run`.
    run_start: bool,
    /// Karaoke snapshot (libass GlyphInfo effect capture): the syllable
    /// duration/offset land on the first segment produced after a karaoke
    /// tag.
    karaoke_timing_ms: i64,
    karaoke_skip_ms: i64,
    karaoke_reset: bool,
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
    /// Time-dependent inputs captured from the rendered segments; the
    /// renderer keys its frame cache on their per-frame samples.
    pub dynamics: EventDynamics,
    /// libass `detect_collisions`: false when \pos, \move, \org, or \t
    /// appeared, in which case the event skips collision stacking.
    pub detect_collisions: bool,
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
                     state: &mut State,
                     overrides: &mut EventOverrides,
                     segments: &mut Vec<Segment>,
                     face_cache: &mut HashMap<
        (String, u16, bool),
        Option<std::sync::Arc<LoadedFace>>,
    >| {
        if text.is_empty() {
            return;
        }
        overrides.unsupported |= build_text_segments(ctx, state, text, segments, face_cache);
        text.clear();
    };

    let bytes = event.text.as_bytes();
    let mut i = 0usize;
    while i < bytes.len() {
        if bytes[i] == b'{' {
            if let Some(close) = event.text[i..].find('}') {
                // Tag block: flush pending text under the current state.
                push_text(
                    &mut pending_text,
                    &mut state,
                    &mut overrides,
                    &mut segments,
                    face_cache,
                );
                let block = &event.text[i + 1..i + close];
                apply_tags(
                    script,
                    &mut state,
                    &mut overrides,
                    parse_tag_block(block),
                    base_wrap,
                    event.style,
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
            push_text(
                &mut pending_text,
                &mut state,
                &mut overrides,
                &mut segments,
                face_cache,
            );
            build_drawing_segment(ctx, &mut state, drawing_text, &mut segments);
            i += end;
            continue;
        }
        // ass_get_next_char escapes.
        let rest = &event.text[i..];
        if let Some(stripped) = rest.strip_prefix('\\') {
            let mut chars = stripped.chars();
            match chars.next() {
                Some('N') => {
                    push_text(
                        &mut pending_text,
                        &mut state,
                        &mut overrides,
                        &mut segments,
                        face_cache,
                    );
                    hard_breaks.push(segments.len());
                    i += 2;
                    continue;
                }
                Some('n') => {
                    if state.wrap_style == 2 {
                        push_text(
                            &mut pending_text,
                            &mut state,
                            &mut overrides,
                            &mut segments,
                            face_cache,
                        );
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
    push_text(
        &mut pending_text,
        &mut state,
        &mut overrides,
        &mut segments,
        face_cache,
    );

    let detect_collisions =
        overrides.pos.is_none() && overrides.movement.is_none() && !overrides.disable_collisions;

    if segments.is_empty() {
        return EventRender {
            nodes: Vec::new(),
            // No segments -> no output at any timestamp: statically empty.
            dynamics: EventDynamics::default(),
            detect_collisions,
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
        // Widen before subtracting: margins come from untrusted fields (wrapping
        // i32, so as extreme as i32::MIN), and `rect_w - margin` in i32 can
        // overflow. f64 is exact here for normal margins.
        (f64::from(rect_w) - f64::from(margin_l) - f64::from(margin_r)).max(1.0)
    };
    let wrap = wrap_lines(&segments, &hard_breaks, max_width, state.wrap_style);

    // ---- pass 3: position the block ----
    let mut line_metrics: Vec<(f64, f64, f64)> = wrap
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
    // Empty lines (repeated \N) keep the height of the nearest text line
    // (libass measures the trimmed empty line with the current font).
    let fallback_metric = line_metrics
        .iter()
        .find(|(_, a, d)| a + d > 0.0)
        .map(|&(_, a, d)| (a, d))
        .unwrap_or((0.0, 0.0));
    let mut last_metric = fallback_metric;
    for metric in &mut line_metrics {
        if metric.1 + metric.2 <= 0.0 {
            (metric.1, metric.2) = last_metric;
        } else {
            last_metric = (metric.1, metric.2);
        }
    }
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
        (None, Some(spec)) => Some(eval_move_anchor(spec, now_rel, event.duration_ms)),
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
    // Only the fade's mask-affecting bit is resolved here; the fade VALUE
    // is applied at emission (renderer) so cached nodes survive fading.
    // Only a positive fade multiplier actually dims the fill (apply_fade is a
    // no-op for `fade <= 0`), so a non-positive sample must NOT mark the mask
    // active — otherwise a negative \fade alpha would hollow the border under
    // a still-opaque fill.
    let fade_active = overrides
        .fade
        .map(|f| fade_alpha(f, now_rel, event.duration_ms))
        .is_some_and(|f| f > 0);
    let karaoke_view: Vec<KaraokeSeg> = segments.iter().map(KaraokeSeg::of).collect();
    let karaoke = karaoke_flags_from(&karaoke_view, now_rel);
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
                fade_active,
                karaoke[seg_index],
                &mut shadows,
                &mut borders,
                &mut fills,
            );
            x_cursor += segment.width;
        }
        y_cursor += line_asc + line_desc;
    }
    // BorderStyle 4 (libass add_background): one BackColour rectangle
    // behind the whole event, expanded by positive scaled \shad values and
    // clamped to the frame; per-glyph shadows were suppressed above.
    let border_style_4 = segments
        .iter()
        .any(|segment| segment.state.border_style == 4);
    if border_style_4 {
        let boxes: Vec<(i32, i32, i32, i32)> = shadows
            .iter()
            .chain(borders.iter())
            .chain(fills.iter())
            .filter(|node| !node.bitmap.is_empty())
            .map(|node| {
                let b = &node.bitmap;
                (b.x, b.y, b.x + b.w as i32, b.y + b.h as i32)
            })
            .collect();
        if let Some(&(first_x, first_y, first_r, first_b)) = boxes.first() {
            let (mut left, mut top, mut right, mut bottom) = (first_x, first_y, first_r, first_b);
            for &(x0, y0, x1, y1) in &boxes[1..] {
                left = left.min(x0);
                top = top.min(y0);
                right = right.max(x1);
                bottom = bottom.max(y1);
            }
            let back_segment = segments
                .iter()
                .find(|segment| segment.state.border_style == 4)
                .expect("border_style_4 implies a matching segment");
            let back = back_segment.state.colors[3];
            let back_shadow_x = back_segment.state.shadow_x;
            let back_shadow_y = back_segment.state.shadow_y;
            let size_x = if back_shadow_x > 0.0 {
                (back_shadow_x * ctx.border_scale_x()).round() as i32
            } else {
                0
            };
            let size_y = if back_shadow_y > 0.0 {
                (back_shadow_y * ctx.border_scale_y()).round() as i32
            } else {
                0
            };
            let left = (left - size_x).clamp(0, ctx.frame_w);
            let top = (top - size_y).clamp(0, ctx.frame_h);
            let right = (right + size_x).clamp(0, ctx.frame_w);
            let bottom = (bottom + size_y).clamp(0, ctx.frame_h);
            let (w, h) = ((right - left) as usize, (bottom - top) as usize);
            if w > 0 && h > 0 {
                nodes.push(RenderedNode {
                    bitmap: CoverageBitmap {
                        w,
                        h,
                        x: left,
                        y: top,
                        data: vec![0xFF; w * h],
                    }
                    .into(),
                    color: node_color(back),
                });
            }
        }
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
    let dynamics = EventDynamics {
        fade: overrides.fade,
        // \pos wins over \move in the anchor selection above; a shadowed
        // movement never influences the output.
        movement: if overrides.pos.is_none() {
            overrides.movement
        } else {
            None
        },
        karaoke: if karaoke_view.iter().any(|seg| seg.kind.is_some()) {
            karaoke_view
        } else {
            Vec::new()
        },
    };
    EventRender {
        nodes,
        dynamics,
        detect_collisions,
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

/// libass `dtoi32(val * 10)`: karaoke centiseconds to milliseconds with a
/// saturating double-to-int32 conversion.
fn dtoi32_ms(centisec: f64) -> i64 {
    let ms = centisec * 10.0;
    if ms >= f64::from(i32::MAX) {
        i64::from(i32::MAX)
    } else if ms <= f64::from(i32::MIN) {
        i64::from(i32::MIN)
    } else {
        ms as i64
    }
}

/// Per-segment karaoke decision, ported from libass
/// `ass_process_karaoke_effects` at run granularity (`run_start` marks the
/// same boundaries karaoke tags induce). Returns
/// `(use_secondary, hide_outline)` per segment: before a syllable's start
/// time the fill uses SecondaryColour, and `\ko` also hides the outline.
/// The `\kf` sweep is approximated stepwise at the syllable midpoint.
///
/// Single evaluator shared by the render path and the cache sampler
/// ([`EventDynamics::sample`]) so both resolve identical flags.
fn karaoke_flags_from(segments: &[KaraokeSeg], tm_current: i64) -> Vec<(bool, bool)> {
    let mut out = vec![(false, false); segments.len()];
    let mut timing: i64 = 0;
    let mut i = 0;
    while i < segments.len() {
        let mut j = i + 1;
        while j < segments.len() && !segments[j].run_start {
            j += 1;
        }
        let start = &segments[i];
        if let Some(kind) = start.kind {
            if start.reset {
                timing = 0;
            }
            let tm_start = timing + start.skip_ms;
            let tm_end = tm_start + start.timing_ms;
            timing = tm_end;
            let boundary = match kind {
                KaraokeKind::Fill => (tm_start + tm_end) / 2,
                _ => tm_start,
            };
            let not_reached = tm_current < boundary;
            for flags in &mut out[i..j] {
                *flags = (not_reached, not_reached && kind == KaraokeKind::Outline);
            }
        }
        i = j;
    }
    out
}

/// `\move` anchor at `now_rel` ms since event start — shared by the layout
/// pass and the cache sampler so both resolve bit-identical positions.
fn eval_move_anchor(spec: MoveSpec, now_rel: i64, duration_ms: i64) -> (f64, f64) {
    let MoveSpec {
        from: (x1, y1),
        to: (x2, y2),
        t1,
        t2,
    } = spec;
    let (mut t1, mut t2) = (i64::from(t1), i64::from(t2));
    if t1 <= 0 && t2 <= 0 {
        t1 = 0;
        t2 = duration_ms;
    }
    let k = if now_rel <= t1 {
        0.0
    } else if now_rel >= t2 {
        1.0
    } else {
        (now_rel - t1) as f64 / (t2 - t1).max(1) as f64
    };
    (x1 + k * (x2 - x1), y1 + k * (y2 - y1))
}

/// Applies one tag block to the state (libass `ass_parse_tags` application
/// side, minus animation).
fn apply_tags(
    script: &Script,
    state: &mut State,
    overrides: &mut EventOverrides,
    tags: Vec<Tag<'_>>,
    base_wrap: i32,
    base_style: usize,
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
                }
            }
            Tag::Org { .. } => {
                // Rotation origin is only visible with rotation (covered by
                // ROTATION), but libass still disables collision stacking.
                overrides.disable_collisions = true;
            }
            Tag::Transform => {
                // transform_is_noop_initial_state: \t interpolation is
                // unsupported — the event renders its initial state, so the
                // output does NOT depend on time and must not poison the
                // static-frame cache. Remove this special case when real \t
                // interpolation lands. Collision disabling and the warn-once
                // stay (libass disables collisions for bare \t too).
                overrides.disable_collisions = true;
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
                // ass_reset_render_context(state, NULL) falls back to the
                // EVENT's declared style — not whatever a previous \r<name>
                // switched to. A bare \r and an unknown style name both
                // return there. Karaoke registers survive \r (libass resets
                // them only in init_render_context).
                let index = match style_name {
                    Some(name) => lookup_style_strict(script, name).unwrap_or(base_style),
                    None => base_style,
                };
                let wrap = state.wrap_style;
                let effect_kind = state.effect_kind;
                let effect_timing_ms = state.effect_timing_ms;
                let effect_skip_ms = state.effect_skip_ms;
                let effect_reset = state.effect_reset;
                *state = State::from_style(script, index, wrap);
                state.effect_kind = effect_kind;
                state.effect_timing_ms = effect_timing_ms;
                state.effect_skip_ms = effect_skip_ms;
                state.effect_reset = effect_reset;
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
            Tag::Karaoke { kind, centisec } => {
                // parse_tags: skip += previous timing, timing = val*10;
                // the segment builder snapshots and clears these.
                state.effect_skip_ms += state.effect_timing_ms;
                state.effect_timing_ms = dtoi32_ms(centisec);
                state.effect_kind = Some(kind);
                if kind == KaraokeKind::Fill {
                    // The \kf sweep is approximated stepwise; keep warning.
                    overrides.unsupported |= unsupported::KARAOKE;
                }
            }
            Tag::KaraokeSet(centisec) => {
                // \kt: absolute start offset; resets accumulated timing.
                state.effect_skip_ms = dtoi32_ms(centisec);
                state.effect_timing_ms = 0;
                state.effect_reset = true;
            }
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
    // All time math in i64: t1..t4 are attacker-controlled i32 fade times, so
    // differences like `now - t1` and `t2 - t1` overflow i32 on extreme
    // inputs. Widening is exact for normal fades (results unchanged).
    let (t1, t4, t3) = if two_arg && t1 == -1 && t4 == -1 {
        (0i64, duration, duration - i64::from(t3))
    } else {
        (i64::from(t1), i64::from(t4), i64::from(t3))
    };
    let t2 = i64::from(t2);
    if now < t1 {
        a1
    } else if now < t2 {
        let cf = (now - t1) as f64 / (t2 - t1).max(1) as f64;
        (f64::from(a1) * (1.0 - cf) + f64::from(a2) * cf) as i32
    } else if now < t3 {
        a2
    } else if now < t4 {
        let cf = (now - t3) as f64 / (t4 - t3).max(1) as f64;
        (f64::from(a2) * (1.0 - cf) + f64::from(a3) * cf) as i32
    } else {
        a3
    }
}

/// `mult_alpha`: transparency composition.
fn mult_alpha(a: u32, b: u32) -> u32 {
    a - (a * b + 0x7F) / 0xFF + b
}

/// Packs an UNFADED node color; the per-frame fade multiplier is applied
/// at emission by [`apply_fade`] so fading frames can reuse cached nodes.
fn node_color(color: Color) -> u32 {
    u32::from(color.r) << 24
        | u32::from(color.g) << 16
        | u32::from(color.b) << 8
        | u32::from(color.t)
}

/// Applies the fade multiplier to a packed node color — bit-identical to
/// the transparency math `node_color` used to inline (VSFilter
/// compatibility: fade applies only when positive).
pub(crate) fn apply_fade(color: u32, fade: Option<i32>) -> u32 {
    let Some(fade) = fade else { return color };
    if fade <= 0 {
        return color;
    }
    let t = mult_alpha(color & 0xFF, fade.clamp(0, 255) as u32).min(255);
    (color & !0xFF) | t
}

/// Splits `text` into word/space segments under one state, shaping each.
/// The karaoke registers are snapshotted into the FIRST segment produced
/// and cleared (libass captures them into GlyphInfo and zeroes the state).
fn build_text_segments(
    ctx: &FrameContext<'_>,
    state: &mut State,
    text: &str,
    segments: &mut Vec<Segment>,
    face_cache: &mut HashMap<(String, u16, bool), Option<std::sync::Arc<LoadedFace>>>,
) -> u32 {
    // libass ass_update_font: a leading '@' selects vertical layout and is
    // stripped before font matching. Vertical layout itself is not
    // implemented; strip the marker so the family still resolves, and
    // surface the gap.
    let (family, unsupported_flags) = match state.family.strip_prefix('@') {
        Some(stripped) => (stripped.to_string(), unsupported::VERTICAL_TEXT),
        None => (state.family.clone(), 0),
    };
    let request = FaceRequest::from_ass(state.bold, state.italic);
    let key = (family.clone(), request.weight, request.italic);
    let entry = face_cache.entry(key).or_insert_with(|| {
        let face_ref = ctx.fonts.select(&family, request)?;
        ctx.fonts.load(&face_ref).map(std::sync::Arc::new)
    });
    let Some(face) = entry.clone() else {
        log::warn!("subtitle render: no usable font for family '{family}'; text dropped");
        return unsupported_flags;
    };

    // Pixel scales: REAL_DIM sizing — the requested size is the full
    // ascender-to-descender height (ass_face_set_size). The metrics follow
    // GDI like libass set_font_metrics: OS/2 usWin values when non-zero,
    // else the face's hhea/typo values.
    let metrics_face = face.face();
    let win_metrics = metrics_face.tables().os2.and_then(|os2| {
        let asc = i32::from(os2.windows_ascender());
        // ttf-parser negates usWinDescent; undo that — like libass, `desc`
        // here is the positive usWinDescent distance.
        let desc = -i32::from(os2.windows_descender());
        (asc + desc != 0).then_some((f64::from(asc), f64::from(desc)))
    });
    let (asc, desc) = win_metrics.unwrap_or_else(|| {
        (
            f64::from(metrics_face.ascender()),
            f64::from(-metrics_face.descender()),
        )
    });
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
    let mut first = true;
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
            run_start: first,
            karaoke_timing_ms: state.effect_timing_ms,
            karaoke_skip_ms: state.effect_skip_ms,
            karaoke_reset: state.effect_reset,
        });
        first = false;
        state.effect_timing_ms = 0;
        state.effect_skip_ms = 0;
        state.effect_reset = false;
        start = end;
    }
    unsupported_flags
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
    state: &mut State,
    drawing_text: &str,
    segments: &mut Vec<Segment>,
) {
    let drawing = parse_drawing(drawing_text);
    let Some((min, max)) = drawing.cbox else {
        return;
    };
    // \p scale: coordinates are divided by 2^(scale-1). Clamp the shift into
    // a valid u32 range so a hostile \p (e.g. \p99) cannot overflow the shift
    // (real drawings use scale 1-4).
    let shift = (state.drawing_scale.max(1) - 1).clamp(0, 31) as u32;
    let power = f64::from(1u32 << shift);
    // Drawings scale by the PlayRes aspect WITHOUT par: libass divides the
    // drawing scale by par_scale_x (get_outline_glyph) and multiplies it
    // back during compositing, so par cancels for drawings (it applies to
    // font glyphs only).
    let aspect = ctx.scale_x() / ctx.scale_y();
    let x_scale = (ctx.scale_y() * ctx.opts.font_scale * state.scale_x * aspect / power) as f32;
    let y_scale = (ctx.scale_y() * ctx.opts.font_scale * state.scale_y / power) as f32;
    // Widen before subtracting: coordinates are individually clamped to i32,
    // so `max - min` in i32 can overflow on extreme drawings. f64 is exact
    // for the difference of two i32s, so normal drawings are unaffected.
    let width = (f64::from(max.x) - f64::from(min.x)) / 64.0 * f64::from(x_scale);
    let height = (f64::from(max.y) - f64::from(min.y)) / 64.0 * f64::from(y_scale);
    // A drawing whose scaled extent dwarfs the frame is hostile or degenerate
    // (coordinates near i32::MAX, NaN scales): rasterizing it would allocate
    // an enormous bitmap or overflow the rasterizer. Skip it like an empty
    // drawing. The bound is generous — real drawings fit within the frame.
    let max_extent = f64::from(ctx.frame_w.max(ctx.frame_h)) * 8.0;
    if !width.is_finite()
        || !height.is_finite()
        || width.abs() > max_extent
        || height.abs() > max_extent
    {
        return;
    }
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
        run_start: true,
        karaoke_timing_ms: state.effect_timing_ms,
        karaoke_skip_ms: state.effect_skip_ms,
        karaoke_reset: state.effect_reset,
    });
    state.effect_timing_ms = 0;
    state.effect_skip_ms = 0;
    state.effect_reset = false;
}

/// Greedy wrap with hard breaks; WrapStyle 2 disables soft wrapping;
/// styles 0/3 get a balancing pass (libass smart wrapping approximation).
/// Every hard break opens a new line unconditionally (libass FORCEBREAK),
/// so repeated `\N` produce empty lines that keep their vertical space.
fn wrap_lines(
    segments: &[Segment],
    hard_breaks: &[usize],
    max_width: f64,
    wrap_style: i32,
) -> Vec<Vec<usize>> {
    let mut lines: Vec<Vec<usize>> = vec![Vec::new()];
    let mut width = 0.0f64;
    let breaks_at = |index: usize| hard_breaks.iter().filter(|&&b| b == index).count();
    for (index, segment) in segments.iter().enumerate() {
        for _ in 0..breaks_at(index) {
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
    // Trailing hard breaks (e.g. "text\N") still add empty lines.
    for _ in 0..breaks_at(segments.len()) {
        lines.push(Vec::new());
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
    fade_active: bool,
    karaoke: (bool, bool),
    shadows: &mut Vec<RenderedNode>,
    borders: &mut Vec<RenderedNode>,
    fills: &mut Vec<RenderedNode>,
) {
    let (karaoke_secondary, karaoke_hide_outline) = karaoke;
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
    if commands.is_empty() && !(state.border_style == 3 && segment.width > 0.0) {
        return;
    }

    let fill = fill_path(&commands);
    // BorderStyle 3 opaque boxes cover glyphless segments too — libass
    // builds an OUTLINE_BOX for every advance, spaces included.
    if fill.is_empty() && !(state.border_style == 3 && segment.width > 0.0) {
        return;
    }
    // A border cannot sanely exceed the frame extent (a wider stroke paints
    // the whole frame anyway); clamping here bounds the stroke-mask
    // allocation for hostile \bord / \xbord / \ybord / Outline values, which
    // are otherwise only lower-clamped to 0. No real subtitle is affected.
    let max_border = f64::from(ctx.frame_w.max(ctx.frame_h));
    let border_px_x = (state.border_x * ctx.border_scale_x()).min(max_border) as f32;
    let border_px_y = (state.border_y * ctx.border_scale_y()).min(max_border) as f32;
    let border_px = border_px_x.max(border_px_y);
    let shadow_dx = state.shadow_x * ctx.border_scale_x();
    let shadow_dy = state.shadow_y * ctx.border_scale_y();

    let mut border_bitmap = if state.border_style == 3 {
        // Opaque box: the segment's bounding rectangle in the outline color.
        let mut rect = Vec::new();
        push_rect(
            &mut rect,
            x - f64::from(border_px_x),
            baseline - segment.ascent - f64::from(border_px_y),
            segment.width + 2.0 * f64::from(border_px_x),
            segment.ascent + segment.descent + 2.0 * f64::from(border_px_y),
        );
        fill_path(&rect)
    } else if border_px > 0.0 {
        border_path(&commands, &fill, border_px_x, border_px_y)
    } else {
        CoverageBitmap::default()
    };

    // Effects on the outermost shape: gaussian first, then \be, matching
    // libass ass_synth_blur. Sigma per axis is blur * blur_scale * the
    // 2/sqrt(log(256)) radius factor from ass_render.c.
    let fill_color = state.colors[usize::from(karaoke_secondary)];
    let effects_applied = state.be > 0 || state.blur > 0.0;
    if effects_applied {
        let blur_sigma_factor = 2.0 / (256.0f64.ln()).sqrt();
        let sigma_x = state.blur * ctx.blur_scale_x() * blur_sigma_factor;
        let sigma_y = state.blur * ctx.blur_scale_y() * blur_sigma_factor;
        let mut effect_target = if border_bitmap.is_empty() {
            fill.clone()
        } else {
            border_bitmap.clone()
        };
        if state.blur > 0.0 {
            gaussian_blur(&mut effect_target, sigma_x, sigma_y);
        }
        if state.be > 0 {
            be_blur(&mut effect_target, state.be);
        }
        if border_bitmap.is_empty() {
            // No border: the blurred fill replaces the fill.
            fills.push(RenderedNode {
                bitmap: effect_target.into(),
                color: node_color(fill_color),
            });
        } else {
            border_bitmap = effect_target;
        }
    }

    // libass FILTER_FILL_IN_BORDER / FILTER_FILL_IN_SHADOW: the border
    // keeps the fill only under a fully opaque, unfaded primary+secondary
    // (or BorderStyle 3); the shadow keeps it while the fill is visible at
    // all. When neither wants the fill, ass_fix_outline hollows the border.
    let has_shadow = shadow_dx != 0.0 || shadow_dy != 0.0;
    let fill_in_border = state.border_style == 3
        || (state.colors[0].t == 0 && state.colors[1].t == 0 && !fade_active);
    let fill_in_shadow = has_shadow && (state.colors[0].t != 0xFF || state.border_style == 3);

    // Shadow: the outermost shape offset by the scaled \shad values in the
    // back color. libass keeps signed offsets (26.6, floored to pixels).
    // BorderStyle 4 suppresses per-glyph shadows (render_text skips bm_s);
    // the event-level background rectangle replaces them.
    let wants_shadow = has_shadow && state.border_style != 4;
    // Only when the shadow drops the fill does ass_fix_outline hollow it
    // (or, below, the border), making the two diverge — the shadow needs
    // its own buffer. Otherwise it is a byte-identical copy of the border
    // (or fill) node and can alias that payload; only the offset differs.
    if wants_shadow && !fill_in_shadow {
        let mut shadow = if border_bitmap.is_empty() {
            fill.clone()
        } else {
            border_bitmap.clone()
        };
        if !border_bitmap.is_empty() && fill_in_border {
            fix_outline(&fill, &mut shadow);
        }
        shadow.x += shadow_dx.floor() as i32;
        shadow.y += shadow_dy.floor() as i32;
        shadows.push(RenderedNode {
            bitmap: shadow.into(),
            color: node_color(state.colors[3]),
        });
    }

    if !border_bitmap.is_empty() && !fill_in_border && !fill_in_shadow {
        fix_outline(&fill, &mut border_bitmap);
    }

    let border_shared: SharedBitmap = border_bitmap.into();
    let fill_shared: SharedBitmap = fill.into();
    if wants_shadow && fill_in_shadow {
        let mut shadow = if border_shared.is_empty() {
            fill_shared.clone()
        } else {
            border_shared.clone()
        };
        shadow.x += shadow_dx.floor() as i32;
        shadow.y += shadow_dy.floor() as i32;
        shadows.push(RenderedNode {
            bitmap: shadow,
            color: node_color(state.colors[3]),
        });
    }

    if !border_shared.is_empty() && !karaoke_hide_outline {
        borders.push(RenderedNode {
            bitmap: border_shared,
            color: node_color(state.colors[2]),
        });
    }
    if !(effects_applied && border_px <= 0.0 && state.border_style != 3) {
        fills.push(RenderedNode {
            bitmap: fill_shared,
            color: node_color(fill_color),
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
