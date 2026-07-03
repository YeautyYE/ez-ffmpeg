//! Format-driven `Style:` / `Dialogue:` field parsing and force-style
//! overrides — ports of libass 0.17.1 `process_style`,
//! `process_event_tail`, `format_line_compare` and
//! `ass_process_force_style` (ass.c).

use super::types::{Event, Script, Style, TrackType};
use super::value::{
    numpad2align, parse_bool, parse_color, parse_f64, parse_int, parse_timecode_ms,
    parse_ycbcr_matrix, ASS_SPACES,
};

/// The standard `Format:` lines (ass.c file-scope constants).
pub(super) const ASS_STYLE_FORMAT: &str =
    "Name, Fontname, Fontsize, PrimaryColour, SecondaryColour, \
     OutlineColour, BackColour, Bold, Italic, Underline, StrikeOut, \
     ScaleX, ScaleY, Spacing, Angle, BorderStyle, Outline, Shadow, \
     Alignment, MarginL, MarginR, MarginV, Encoding";
pub(super) const ASS_EVENT_FORMAT: &str =
    "Layer, Start, End, Style, Name, MarginL, MarginR, MarginV, Effect, Text";
pub(super) const SSA_STYLE_FORMAT: &str =
    "Name, Fontname, Fontsize, PrimaryColour, SecondaryColour, \
     TertiaryColour, BackColour, Bold, Italic, BorderStyle, Outline, \
     Shadow, Alignment, MarginL, MarginR, MarginV, AlphaLevel, Encoding";
pub(super) const SSA_EVENT_FORMAT: &str =
    "Marked, Start, End, Style, Name, MarginL, MarginR, MarginV, Effect, Text";

/// libass `next_token` over one line: comma-separated tokens with leading
/// and trailing spaces/tabs trimmed. Empty tokens between commas are real
/// tokens; a trailing comma yields no extra token.
pub(super) struct Cursor<'a> {
    rest: &'a str,
}

impl<'a> Cursor<'a> {
    pub(super) fn new(s: &'a str) -> Self {
        Self { rest: s }
    }

    pub(super) fn next_token(&mut self) -> Option<&'a str> {
        self.rest = self.rest.trim_start_matches(ASS_SPACES);
        if self.rest.is_empty() {
            return None;
        }
        let (token, rest) = match self.rest.find(',') {
            Some(comma) => (&self.rest[..comma], &self.rest[comma + 1..]),
            None => (self.rest, ""),
        };
        self.rest = rest;
        Some(token.trim_end_matches(ASS_SPACES))
    }

    /// The raw remainder, positioned right after the last consumed comma —
    /// what libass's `p` points at when the Text field is reached (leading
    /// whitespace preserved).
    pub(super) fn remainder(&self) -> &'a str {
        self.rest
    }
}

/// `format_line_compare`: token-wise, case-insensitive comparison with the
/// (case-sensitive) `Actor` → `Name` alias; equal only when both lines end
/// simultaneously.
pub(super) fn format_line_compare(fmt1: &str, fmt2: &str) -> bool {
    let mut left = Cursor::new(fmt1);
    let mut right = Cursor::new(fmt2);
    loop {
        match (left.next_token(), right.next_token()) {
            (Some(token1), Some(token2)) => {
                let token1 = if token1 == "Actor" { "Name" } else { token1 };
                let token2 = if token2 == "Actor" { "Name" } else { token2 };
                if !token1.eq_ignore_ascii_case(token2) {
                    return false;
                }
            }
            (None, None) => return true,
            _ => return false,
        }
    }
}

/// `set_style_alpha`: clamped alphas; the front alpha lands on
/// primary/secondary/outline, the back alpha on the back colour.
fn set_style_alpha(style: &mut Style, front_alpha: i32, back_alpha: i32) {
    let front = front_alpha.clamp(0, 0xFF) as u8;
    let back = back_alpha.clamp(0, 0xFF) as u8;
    style.primary_colour.t = front;
    style.secondary_colour.t = front;
    style.outline_colour.t = front;
    style.back_colour.t = back;
}

fn timecode_or_zero(token: &str) -> i64 {
    parse_timecode_ms(token).unwrap_or_else(|| {
        log::warn!("ass parser: bad timestamp '{token}'");
        0
    })
}

/// `process_style`: parses one `Style:` body against `format` and appends
/// the normalized style to the script.
pub(super) fn process_style_line(script: &mut Script, format: &str, body: &str) {
    let mut style = Style::zeroed();
    let mut name: Option<String> = None;
    let mut font_name: Option<String> = None;
    let mut ssa_alpha: i32 = 0;
    let ssa = script.track_type == TrackType::Ssa;

    let mut fields = Cursor::new(format);
    let mut values = Cursor::new(body);
    while let Some(field) = fields.next_token() {
        let Some(token) = values.next_token() else {
            break;
        };

        if field.eq_ignore_ascii_case("Name") {
            // STARREDSTRVAL: VSFilter treats leading '*' as junk.
            name = Some(token.trim_start_matches('*').to_string());
        } else if field.eq_ignore_ascii_case("Fontname") {
            font_name = Some(token.to_string());
        } else if field.eq_ignore_ascii_case("PrimaryColour") {
            style.primary_colour = parse_color(token);
        } else if field.eq_ignore_ascii_case("SecondaryColour") {
            style.secondary_colour = parse_color(token);
        } else if field.eq_ignore_ascii_case("OutlineColour") {
            style.outline_colour = parse_color(token);
        } else if field.eq_ignore_ascii_case("BackColour") {
            style.back_colour = parse_color(token);
            // SSA uses BackColour for both outline and shadow; the SSA
            // TertiaryColour field is not read at all (same as libass).
            if ssa {
                style.outline_colour = style.back_colour;
            }
        } else if field.eq_ignore_ascii_case("AlphaLevel") {
            ssa_alpha = parse_int(token);
        } else if field.eq_ignore_ascii_case("Fontsize") {
            style.font_size = parse_f64(token);
        } else if field.eq_ignore_ascii_case("Bold") {
            style.bold = parse_int(token);
        } else if field.eq_ignore_ascii_case("Italic") {
            style.italic = parse_int(token);
        } else if field.eq_ignore_ascii_case("Underline") {
            style.underline = parse_int(token);
        } else if field.eq_ignore_ascii_case("StrikeOut") {
            style.strike_out = parse_int(token);
        } else if field.eq_ignore_ascii_case("Spacing") {
            style.spacing = parse_f64(token);
        } else if field.eq_ignore_ascii_case("Angle") {
            style.angle = parse_f64(token);
        } else if field.eq_ignore_ascii_case("BorderStyle") {
            style.border_style = parse_int(token);
        } else if field.eq_ignore_ascii_case("Alignment") {
            let raw = parse_int(token);
            style.alignment = if script.track_type == TrackType::Ass {
                numpad2align(raw)
            } else if raw == 8 {
                // VSFilter compatibility remaps for SSA scripts.
                3
            } else if raw == 4 {
                11
            } else {
                raw
            };
        } else if field.eq_ignore_ascii_case("MarginL") {
            style.margin_l = parse_int(token);
        } else if field.eq_ignore_ascii_case("MarginR") {
            style.margin_r = parse_int(token);
        } else if field.eq_ignore_ascii_case("MarginV") {
            style.margin_v = parse_int(token);
        } else if field.eq_ignore_ascii_case("Encoding") {
            style.encoding = parse_int(token);
        } else if field.eq_ignore_ascii_case("ScaleX") {
            style.scale_x = parse_f64(token);
        } else if field.eq_ignore_ascii_case("ScaleY") {
            style.scale_y = parse_f64(token);
        } else if field.eq_ignore_ascii_case("Outline") {
            style.outline = parse_f64(token);
        } else if field.eq_ignore_ascii_case("Shadow") {
            style.shadow = parse_f64(token);
        }
        // Unknown field names consume their token and are ignored.
    }

    // process_style post-normalization, in the exact libass order.
    if ssa {
        // VSFilter compat: SSA BackColour alpha is always 0x80.
        set_style_alpha(&mut style, ssa_alpha, 0x80);
    }
    style.scale_x = style.scale_x.max(0.0) / 100.0;
    style.scale_y = style.scale_y.max(0.0) / 100.0;
    style.spacing = style.spacing.max(0.0);
    style.outline = style.outline.max(0.0);
    style.shadow = style.shadow.max(0.0);
    style.bold = i32::from(style.bold != 0);
    style.italic = i32::from(style.italic != 0);
    style.underline = i32::from(style.underline != 0);
    style.strike_out = i32::from(style.strike_out != 0);
    style.name = name.unwrap_or_else(|| "Default".to_string());
    style.font_name = font_name.unwrap_or_else(|| "Arial".to_string());
    // A script style named exactly "Default" becomes the lookup fallback.
    if style.name == "Default" {
        script.default_style = script.styles.len();
    }
    script.styles.push(style);
}

/// `process_event_tail`. `None` means the event is discarded (data ran out
/// before the Text field, or the format has no Text field) — libass frees
/// the event in that case. `skip_fields` is the `n_ignored` parameter: the
/// embedded-chunk path consumes ReadOrder/Layer itself and skips the first
/// three format fields (Layer, Start, End).
pub(super) fn parse_event_tail(
    script: &Script,
    format: &str,
    body: &str,
    skip_fields: usize,
) -> Option<Event> {
    let mut event = Event::zeroed();
    let mut end_ms: i64 = 0;

    let mut fields = Cursor::new(format);
    let mut values = Cursor::new(body);
    for _ in 0..skip_fields {
        if fields.next_token().is_none() {
            break;
        }
    }
    loop {
        let field = fields.next_token()?;
        if field.eq_ignore_ascii_case("Text") {
            // Text is always last: the raw remainder, override tags and
            // commas included, with trailing CR/tab/space stripped.
            const TEXT_TRAIL: &[char] = &['\r', '\t', ' '];
            event.text = values.remainder().trim_end_matches(TEXT_TRAIL).to_string();
            event.duration_ms = end_ms - event.start_ms;
            return Some(event);
        }
        let token = values.next_token()?;

        // ALIAS(End, Duration) / ALIAS(Actor, Name) — case-insensitive here.
        let field = if field.eq_ignore_ascii_case("End") {
            "Duration"
        } else if field.eq_ignore_ascii_case("Actor") {
            "Name"
        } else {
            field
        };

        if field.eq_ignore_ascii_case("Layer") {
            event.layer = parse_int(token);
        } else if field.eq_ignore_ascii_case("Style") {
            event.style = script.lookup_style(token);
        } else if field.eq_ignore_ascii_case("Name") {
            event.name = token.to_string();
        } else if field.eq_ignore_ascii_case("Effect") {
            event.effect = token.to_string();
        } else if field.eq_ignore_ascii_case("MarginL") {
            event.margin_l = parse_int(token);
        } else if field.eq_ignore_ascii_case("MarginR") {
            event.margin_r = parse_int(token);
        } else if field.eq_ignore_ascii_case("MarginV") {
            event.margin_v = parse_int(token);
        } else if field.eq_ignore_ascii_case("Start") {
            event.start_ms = timecode_or_zero(token);
        } else if field.eq_ignore_ascii_case("Duration") {
            // "End" lands here through the alias; converted to a duration
            // when the Text field is reached.
            end_ms = timecode_or_zero(token);
        }
        // Unknown fields (e.g. SSA "Marked") consume their token silently.
    }
}

/// `ass_process_force_style`: `[StyleName.]Field=Value` overrides, applied
/// to script headers and matching styles. Values are applied RAW — no /100
/// scale normalization, no boolean squashing, no numpad alignment
/// conversion — bug-for-bug with libass (and therefore FFmpeg force_style).
pub(crate) fn apply_force_style(script: &mut Script, overrides: &[&str]) {
    for entry in overrides {
        let Some((name_part, value)) = entry.rsplit_once('=') else {
            continue;
        };

        // Script-header overrides use the full (undotted) name part and are
        // checked case-insensitively, unlike the Script Info section.
        if name_part.eq_ignore_ascii_case("PlayResX") {
            script.play_res_x = parse_int(value);
        } else if name_part.eq_ignore_ascii_case("PlayResY") {
            script.play_res_y = parse_int(value);
        } else if name_part.eq_ignore_ascii_case("LayoutResX") {
            script.layout_res_x = parse_int(value);
        } else if name_part.eq_ignore_ascii_case("LayoutResY") {
            script.layout_res_y = parse_int(value);
        } else if name_part.eq_ignore_ascii_case("Timer") {
            script.timer = parse_f64(value);
        } else if name_part.eq_ignore_ascii_case("WrapStyle") {
            script.wrap_style = parse_int(value);
        } else if name_part.eq_ignore_ascii_case("ScaledBorderAndShadow") {
            script.scaled_border_and_shadow = parse_bool(value);
        } else if name_part.eq_ignore_ascii_case("Kerning") {
            script.kerning = parse_bool(value);
        } else if name_part.eq_ignore_ascii_case("YCbCr Matrix") {
            script.ycbcr_matrix = parse_ycbcr_matrix(value);
        }

        // Optional "Style." scope (split at the LAST dot); no scope applies
        // to every style, including the built-in default.
        let (scope, field) = match name_part.rsplit_once('.') {
            Some((scope, field)) => (Some(scope), field),
            None => (None, name_part),
        };
        for style in &mut script.styles {
            let applies = match scope {
                None => true,
                Some(scope) => style.name.eq_ignore_ascii_case(scope),
            };
            if applies {
                apply_force_style_field(style, field, value);
            }
        }
    }
}

fn apply_force_style_field(style: &mut Style, field: &str, value: &str) {
    if field.eq_ignore_ascii_case("FontName") {
        style.font_name = value.to_string();
    } else if field.eq_ignore_ascii_case("PrimaryColour") {
        style.primary_colour = parse_color(value);
    } else if field.eq_ignore_ascii_case("SecondaryColour") {
        style.secondary_colour = parse_color(value);
    } else if field.eq_ignore_ascii_case("OutlineColour") {
        style.outline_colour = parse_color(value);
    } else if field.eq_ignore_ascii_case("BackColour") {
        style.back_colour = parse_color(value);
    } else if field.eq_ignore_ascii_case("AlphaLevel") {
        let alpha = parse_int(value);
        set_style_alpha(style, alpha, alpha);
    } else if field.eq_ignore_ascii_case("FontSize") {
        style.font_size = parse_f64(value);
    } else if field.eq_ignore_ascii_case("Bold") {
        style.bold = parse_int(value);
    } else if field.eq_ignore_ascii_case("Italic") {
        style.italic = parse_int(value);
    } else if field.eq_ignore_ascii_case("Underline") {
        style.underline = parse_int(value);
    } else if field.eq_ignore_ascii_case("StrikeOut") {
        style.strike_out = parse_int(value);
    } else if field.eq_ignore_ascii_case("Spacing") {
        style.spacing = parse_f64(value);
    } else if field.eq_ignore_ascii_case("Angle") {
        style.angle = parse_f64(value);
    } else if field.eq_ignore_ascii_case("BorderStyle") {
        style.border_style = parse_int(value);
    } else if field.eq_ignore_ascii_case("Alignment") {
        style.alignment = parse_int(value);
    } else if field.eq_ignore_ascii_case("Justify") {
        style.justify = parse_int(value);
    } else if field.eq_ignore_ascii_case("MarginL") {
        style.margin_l = parse_int(value);
    } else if field.eq_ignore_ascii_case("MarginR") {
        style.margin_r = parse_int(value);
    } else if field.eq_ignore_ascii_case("MarginV") {
        style.margin_v = parse_int(value);
    } else if field.eq_ignore_ascii_case("Encoding") {
        style.encoding = parse_int(value);
    } else if field.eq_ignore_ascii_case("ScaleX") {
        style.scale_x = parse_f64(value);
    } else if field.eq_ignore_ascii_case("ScaleY") {
        style.scale_y = parse_f64(value);
    } else if field.eq_ignore_ascii_case("Outline") {
        style.outline = parse_f64(value);
    } else if field.eq_ignore_ascii_case("Shadow") {
        style.shadow = parse_f64(value);
    } else if field.eq_ignore_ascii_case("Blur") {
        style.blur = parse_f64(value);
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn cursor_matches_next_token() {
        let mut cursor = Cursor::new(" a , b\t,,d,");
        assert_eq!(cursor.next_token(), Some("a"));
        assert_eq!(cursor.next_token(), Some("b"));
        assert_eq!(cursor.next_token(), Some("")); // empty token between commas
        assert_eq!(cursor.remainder(), "d,");
        assert_eq!(cursor.next_token(), Some("d"));
        assert_eq!(cursor.next_token(), None); // trailing comma: no extra token
        assert_eq!(Cursor::new("   ").next_token(), None);
    }

    #[test]
    fn cursor_remainder_preserves_leading_space() {
        let mut cursor = Cursor::new("0, second, rest of, line ");
        cursor.next_token();
        cursor.next_token();
        assert_eq!(cursor.remainder(), " rest of, line ");
    }

    #[test]
    fn format_compare_handles_alias_and_end() {
        // "Actor" aliases to "Name" (exact case only).
        let with_actor = ASS_EVENT_FORMAT.replace("Name", "Actor");
        assert!(format_line_compare(&with_actor, ASS_EVENT_FORMAT));
        let with_lower_actor = ASS_EVENT_FORMAT.replace("Name", "actor");
        assert!(!format_line_compare(&with_lower_actor, ASS_EVENT_FORMAT));
        // Case-insensitive token comparison.
        assert!(format_line_compare(
            &ASS_STYLE_FORMAT.to_ascii_uppercase(),
            ASS_STYLE_FORMAT
        ));
        // Both sides must end simultaneously.
        assert!(!format_line_compare("Name, Fontname", "Name"));
        // A trailing comma does not create a token.
        assert!(format_line_compare("Name, Fontname,", "Name, Fontname"));
    }
}
