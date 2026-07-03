//! Parsed ASS script data model, mirroring libass 0.17.1 `ASS_Track` /
//! `ASS_Style` / `ASS_Event` (ass_types.h) with the same defaults and the
//! same normalization the libass parser applies.

use super::value::{Color, YcbcrMatrix};

/// `[V4 Styles]`/`ScriptType: v4.00` mark SSA, `[V4+ Styles]`/`v4.00+` mark
/// ASS. A script that never reveals its type is rejected by [`super::parse`].
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub(crate) enum TrackType {
    #[default]
    Unknown,
    Ass,
    Ssa,
}

/// One `[V4+ Styles]` entry after libass normalization: scales are ratios
/// (1.0 = 100%), flags are 0/1, spacing/outline/shadow are clamped to >= 0,
/// alignment uses the legacy VSFilter encoding (see
/// [`super::value::numpad2align`]).
#[derive(Debug, Clone, PartialEq)]
pub(crate) struct Style {
    pub name: String,
    pub font_name: String,
    pub font_size: f64,
    pub primary_colour: Color,
    pub secondary_colour: Color,
    pub outline_colour: Color,
    pub back_colour: Color,
    /// 0/1 for parsed styles; the built-in default style keeps the raw
    /// VSFilter value 200 (a weight-ish sentinel), exactly like libass.
    pub bold: i32,
    pub italic: i32,
    pub underline: i32,
    pub strike_out: i32,
    pub scale_x: f64,
    pub scale_y: f64,
    pub spacing: f64,
    pub angle: f64,
    pub border_style: i32,
    pub outline: f64,
    pub shadow: f64,
    /// Legacy encoding: `halign(1..=3) | valign(0 sub / 4 top / 8 center)`.
    pub alignment: i32,
    pub margin_l: i32,
    pub margin_r: i32,
    pub margin_v: i32,
    pub encoding: i32,
    /// Only settable through force-style overrides, like libass.
    pub justify: i32,
    /// Only settable through force-style overrides, like libass.
    pub blur: f64,
}

impl Style {
    /// `ass_alloc_style` + the `process_style` preamble: everything zeroed
    /// except the scales, which are pre-set to 100 (percent, normalized to a
    /// ratio after parsing).
    pub(super) fn zeroed() -> Self {
        Self {
            name: String::new(),
            font_name: String::new(),
            font_size: 0.0,
            primary_colour: Color::default(),
            secondary_colour: Color::default(),
            outline_colour: Color::default(),
            back_colour: Color::default(),
            bold: 0,
            italic: 0,
            underline: 0,
            strike_out: 0,
            scale_x: 100.0,
            scale_y: 100.0,
            spacing: 0.0,
            angle: 0.0,
            border_style: 0,
            outline: 0.0,
            shadow: 0.0,
            alignment: 0,
            margin_l: 0,
            margin_r: 0,
            margin_v: 0,
            encoding: 0,
            justify: 0,
            blur: 0.0,
        }
    }

    /// `set_default_style`: the style every track starts with at index 0
    /// (values taken from VSFilter for best compatibility).
    pub(crate) fn builtin_default() -> Self {
        Self {
            name: "Default".to_string(),
            font_name: "Arial".to_string(),
            font_size: 18.0,
            primary_colour: Color::from_packed(0xffff_ff00), // white, opaque
            secondary_colour: Color::from_packed(0x00ff_ff00), // cyan, opaque
            outline_colour: Color::from_packed(0x0000_0000), // black, opaque
            back_colour: Color::from_packed(0x0000_0080),    // black, half-transparent
            bold: 200,
            scale_x: 1.0,
            scale_y: 1.0,
            border_style: 1,
            outline: 2.0,
            shadow: 3.0,
            alignment: 2, // bottom-center in the legacy encoding
            margin_l: 20,
            margin_r: 20,
            margin_v: 20,
            ..Self::zeroed()
        }
    }
}

/// One `Dialogue:` event. `text` is the raw tail of the line (override tags
/// included), with trailing `\r`/tab/space stripped but leading whitespace
/// after the last comma preserved — exactly what libass hands the renderer.
#[derive(Debug, Clone, PartialEq)]
pub(crate) struct Event {
    /// Sequential index for full-script parses (like `parse_memory`).
    pub read_order: i32,
    pub layer: i32,
    pub start_ms: i64,
    /// `End - Start`; may be negative when the script says so (such an
    /// event simply never displays).
    pub duration_ms: i64,
    /// Index into [`Script::styles`], resolved at parse time; 0 (the
    /// built-in default style) when the format has no Style field.
    pub style: usize,
    pub name: String,
    pub margin_l: i32,
    pub margin_r: i32,
    pub margin_v: i32,
    pub effect: String,
    pub text: String,
}

impl Event {
    /// `ass_alloc_event`: calloc semantics.
    pub(super) fn zeroed() -> Self {
        Self {
            read_order: 0,
            layer: 0,
            start_ms: 0,
            duration_ms: 0,
            style: 0,
            name: String::new(),
            margin_l: 0,
            margin_r: 0,
            margin_v: 0,
            effect: String::new(),
            text: String::new(),
        }
    }
}

/// A `[Fonts]` attachment, already uudecoded (`decode_font`).
#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct EmbeddedFont {
    pub filename: String,
    pub data: Vec<u8>,
}

/// A fully parsed script (libass `ASS_Track`). Header fields keep libass's
/// zero defaults when absent (`PlayResX/Y` 0 means "unset"; resolution
/// fallbacks are the renderer's job, not the parser's).
#[derive(Debug, Clone, PartialEq)]
pub(crate) struct Script {
    pub track_type: TrackType,
    pub play_res_x: i32,
    pub play_res_y: i32,
    pub layout_res_x: i32,
    pub layout_res_y: i32,
    pub timer: f64,
    pub wrap_style: i32,
    pub scaled_border_and_shadow: bool,
    pub kerning: bool,
    /// First two characters of the `Language:` header, when present.
    pub language: Option<String>,
    pub ycbcr_matrix: YcbcrMatrix,
    /// `styles[0]` is always the built-in default style; script styles
    /// follow in file order.
    pub styles: Vec<Style>,
    /// Index of the style lookups fall back to: the last script style
    /// named exactly `Default`, or 0 (the built-in) when there is none.
    pub default_style: usize,
    pub events: Vec<Event>,
    pub fonts: Vec<EmbeddedFont>,
}

impl Script {
    /// `ass_new_track`: zeroed header plus the built-in default style.
    pub(super) fn new() -> Self {
        Self {
            track_type: TrackType::Unknown,
            play_res_x: 0,
            play_res_y: 0,
            layout_res_x: 0,
            layout_res_y: 0,
            timer: 0.0,
            wrap_style: 0,
            scaled_border_and_shadow: false,
            kerning: false,
            language: None,
            ycbcr_matrix: YcbcrMatrix::Default,
            styles: vec![Style::builtin_default()],
            default_style: 0,
            events: Vec::new(),
            fonts: Vec::new(),
        }
    }

    /// `ass_lookup_style`: leading `*`s are stripped, `default` is
    /// case-normalized, later definitions shadow earlier ones (backwards
    /// search, exact match), and unknown names fall back to the default
    /// style with a warning.
    pub(crate) fn lookup_style(&self, name: &str) -> usize {
        let name = name.trim_start_matches('*');
        let name = if name.eq_ignore_ascii_case("Default") {
            "Default"
        } else {
            name
        };
        for (index, style) in self.styles.iter().enumerate().rev() {
            if style.name == name {
                return index;
            }
        }
        log::warn!(
            "ass parser: no style named '{name}' found, using '{}'",
            self.styles[self.default_style].name
        );
        self.default_style
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn builtin_default_matches_set_default_style() {
        let style = Style::builtin_default();
        assert_eq!(style.name, "Default");
        assert_eq!(style.font_name, "Arial");
        assert_eq!(style.font_size, 18.0);
        assert_eq!(style.primary_colour.packed(), 0xffff_ff00);
        assert_eq!(style.secondary_colour.packed(), 0x00ff_ff00);
        assert_eq!(style.outline_colour.packed(), 0x0000_0000);
        assert_eq!(style.back_colour.packed(), 0x0000_0080);
        assert_eq!(style.bold, 200);
        assert_eq!((style.scale_x, style.scale_y), (1.0, 1.0));
        assert_eq!(style.border_style, 1);
        assert_eq!((style.outline, style.shadow), (2.0, 3.0));
        assert_eq!(style.alignment, 2);
        assert_eq!(
            (style.margin_l, style.margin_r, style.margin_v),
            (20, 20, 20)
        );
        assert_eq!((style.italic, style.underline, style.strike_out), (0, 0, 0));
        assert_eq!((style.encoding, style.justify, style.blur), (0, 0, 0.0));
    }

    #[test]
    fn lookup_style_matches_ass_lookup_style() {
        let mut script = Script::new();
        script.styles.push(Style {
            name: "Sign".to_string(),
            ..Style::zeroed()
        });
        script.styles.push(Style {
            name: "Default".to_string(),
            ..Style::zeroed()
        });
        script.default_style = 2;
        script.styles.push(Style {
            name: "Sign".to_string(),
            font_name: "Later".to_string(),
            ..Style::zeroed()
        });

        // Later duplicate wins (backwards search).
        assert_eq!(script.lookup_style("Sign"), 3);
        // '*' prefixes are junk.
        assert_eq!(script.lookup_style("**Sign"), 3);
        // Only "Default" is case-normalized...
        assert_eq!(script.lookup_style("DEFAULT"), 2);
        // ...other lookups are exact.
        assert_eq!(script.lookup_style("sign"), script.default_style);
        // Unknown names fall back to the default style.
        assert_eq!(script.lookup_style("Missing"), 2);
    }
}
