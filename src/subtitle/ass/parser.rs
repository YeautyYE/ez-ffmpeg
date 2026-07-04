//! Line- and section-level parsing (libass 0.17.1 `ass.c`: `process_text`,
//! `process_line`, the section processors and `parse_memory`), plus the
//! public [`parse`] entry point.

use super::fields::{
    format_line_compare, parse_event_tail, process_style_line, Cursor, ASS_EVENT_FORMAT,
    ASS_STYLE_FORMAT, SSA_EVENT_FORMAT, SSA_STYLE_FORMAT,
};
use super::types::{EmbeddedFont, Script, TrackType};
use super::value::{
    parse_bool, parse_f64, parse_i32_saturating, parse_int, parse_ycbcr_matrix, ASS_SPACES,
};

// Script Info headers seen so far (libass `ScriptInfo` flags): duplicates
// warn, and two compatibility rules depend on the exact set.
const SINFO_PLAYRESX: u32 = 1 << 0;
const SINFO_PLAYRESY: u32 = 1 << 1;
const SINFO_TIMER: u32 = 1 << 2;
const SINFO_WRAPSTYLE: u32 = 1 << 3;
const SINFO_SCALEDBORDER: u32 = 1 << 4;
const SINFO_COLOURMATRIX: u32 = 1 << 5;
const SINFO_KERNING: u32 = 1 << 6;
const SINFO_SCRIPTTYPE: u32 = 1 << 7;
const SINFO_LANGUAGE: u32 = 1 << 8;
const SINFO_LAYOUTRESX: u32 = 1 << 9;
const SINFO_LAYOUTRESY: u32 = 1 << 10;
const GENBY_FFMPEG: u32 = 1 << 11;

/// The one way a full-script parse fails, mirroring `parse_memory`: nothing
/// ever identified the input as SSA or ASS.
#[derive(Debug, PartialEq, Eq)]
pub(crate) struct NotAScript;

impl std::fmt::Display for NotAScript {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str("not an SSA/ASS script: no [V4/V4+ Styles] section or ScriptType header")
    }
}

impl std::error::Error for NotAScript {}

/// Parses a complete ASS/SSA script (the `ass_read_memory` path, UTF-8
/// input). Charset conversion is the caller's job, exactly as it is for
/// libass with a null codepage.
pub(crate) fn parse(content: &str) -> Result<Script, NotAScript> {
    let mut parser = ScriptParser::new();
    parser.process_data(content);
    parser.finish()
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum Section {
    None,
    Info,
    Styles,
    Events,
    Fonts,
}

/// Incremental script parser: the full-script path uses [`parse`]; the
/// decoder path (FFmpeg lavcodec output) creates one, feeds
/// [`Self::process_codec_private`] and [`Self::process_chunk`], then takes
/// the script with [`Self::into_script`].
pub(crate) struct ScriptParser {
    script: Script,
    section: Section,
    style_format: Option<String>,
    event_format: Option<String>,
    header_flags: u32,
    fontname: Option<String>,
    fontdata: Vec<u8>,
    /// `ass_new_track` enables ReadOrder duplicate elimination by default.
    seen_read_orders: std::collections::HashSet<i32>,
    /// Mirrors libass's lazily created read-order bitmap: it is built on the
    /// first `ass_process_chunk` call that arrives when events already
    /// exist. Before that, duplicate detection scans stored (successfully
    /// parsed) events only, so a malformed chunk does not reserve its
    /// ReadOrder; after that, the check is test-and-set and reserves the
    /// ReadOrder even when the rest of the chunk fails to parse.
    read_order_bitmap_active: bool,
}

fn starts_ci(s: &str, prefix: &str) -> bool {
    s.as_bytes()
        .get(..prefix.len())
        .is_some_and(|head| head.eq_ignore_ascii_case(prefix.as_bytes()))
}

fn preview(line: &str) -> String {
    line.chars().take(30).collect()
}

impl ScriptParser {
    pub(crate) fn new() -> Self {
        Self {
            script: Script::new(),
            section: Section::None,
            style_format: None,
            event_format: None,
            header_flags: 0,
            fontname: None,
            fontdata: Vec::new(),
            seen_read_orders: std::collections::HashSet::new(),
            read_order_bitmap_active: false,
        }
    }

    /// `ass_process_data` / `process_text`: parses a chunk of header/script
    /// text. Stops at the first NUL byte (libass parses a NUL-terminated
    /// copy), splits on CR/LF (any mix), skips blank lines, and strips
    /// UTF-8 BOMs found at line starts.
    pub(crate) fn process_data(&mut self, content: &str) {
        const LINE_BREAKS: &[char] = &['\r', '\n'];
        let mut rest = content.split('\0').next().unwrap_or("");
        loop {
            loop {
                if let Some(tail) = rest.strip_prefix(LINE_BREAKS) {
                    rest = tail;
                } else if let Some(tail) = rest.strip_prefix('\u{feff}') {
                    rest = tail;
                } else {
                    break;
                }
            }
            if rest.is_empty() {
                break;
            }
            let line_end = rest.find(LINE_BREAKS).unwrap_or(rest.len());
            let (line, tail) = rest.split_at(line_end);
            rest = tail;
            self.process_line(line);
        }
        // There is no explicit end-of-font marker in SSA/ASS.
        self.flush_font();
    }

    /// `ass_process_codec_private`: header text (Script Info + styles), then
    /// the event-format fallback for ancient mkvtoolnix files. force_style
    /// application happens in the caller (it owns the override list).
    pub(crate) fn process_codec_private(&mut self, header: &str) {
        self.process_data(header);
        if self.event_format.is_none() {
            log::debug!("ass parser: no event format found, using fallback");
            self.event_format = Some(match self.script.track_type {
                TrackType::Ssa => SSA_EVENT_FORMAT.to_string(),
                _ => ASS_EVENT_FORMAT.to_string(),
            });
        }
    }

    /// `ass_process_chunk`: one embedded event with explicit timing
    /// (`ReadOrder, Layer, <format tail from Style on>`). Duplicate
    /// ReadOrders are dropped, like libass's default `check_readorder`.
    pub(crate) fn process_chunk(&mut self, body: &str, start_ms: i64, duration_ms: i64) {
        let Some(format) = self.event_format.clone() else {
            log::warn!("ass parser: event format header missing");
            return;
        };
        let body = body.split('\0').next().unwrap_or("");

        if !self.read_order_bitmap_active && !self.script.events.is_empty() {
            self.read_order_bitmap_active = true;
            for event in &self.script.events {
                self.seen_read_orders.insert(event.read_order);
            }
        }

        let mut values = Cursor::new(body);
        let Some(read_order_token) = values.next_token() else {
            return;
        };
        // libass uses atoi here (saturating i32).
        let read_order = parse_i32_saturating(read_order_token, 10);
        if self.read_order_bitmap_active {
            if !self.seen_read_orders.insert(read_order) {
                return;
            }
        } else if self
            .script
            .events
            .iter()
            .any(|event| event.read_order == read_order)
        {
            return;
        }
        let Some(layer_token) = values.next_token() else {
            return;
        };
        let layer = parse_int(layer_token);

        // The remaining fields follow the event format with the first three
        // entries (Layer, Start, End) skipped — n_ignored = 3 in libass.
        if let Some(mut event) = parse_event_tail(&self.script, &format, values.remainder(), 3) {
            event.read_order = read_order;
            event.layer = layer;
            event.start_ms = start_ms;
            event.duration_ms = duration_ms;
            self.script.events.push(event);
        }
    }

    /// `process_line`: known section headers switch state (prefix match,
    /// case-insensitive); every other line — including unknown section
    /// headers — is fed to the CURRENT section's processor, a libass quirk
    /// preserved deliberately.
    fn process_line(&mut self, line: &str) {
        let line = line.trim_start_matches(ASS_SPACES);
        if starts_ci(line, "[Script Info]") {
            self.section = Section::Info;
        } else if starts_ci(line, "[V4 Styles]") {
            self.section = Section::Styles;
            self.script.track_type = TrackType::Ssa;
        } else if starts_ci(line, "[V4+ Styles]") {
            self.section = Section::Styles;
            self.script.track_type = TrackType::Ass;
        } else if starts_ci(line, "[Events]") {
            self.section = Section::Events;
        } else if starts_ci(line, "[Fonts]") {
            self.section = Section::Fonts;
        } else {
            match self.section {
                Section::Info => self.process_info_line(line),
                Section::Styles => self.process_styles_line(line),
                Section::Events => self.process_events_line(line),
                Section::Fonts => self.process_fonts_line(line),
                Section::None => {}
            }
        }
    }

    fn note_header(&mut self, flag: u32, name: &str) {
        if self.header_flags & flag != 0 {
            log::warn!(
                "ass parser: duplicate Script Info header '{name}'; previous value overwritten"
            );
        } else {
            self.header_flags |= flag;
        }
    }

    /// `process_info_line`: prefixes are case-SENSITIVE here (unlike section
    /// headers and field names); values tolerate leading whitespace because
    /// the scalar parsers skip it.
    fn process_info_line(&mut self, line: &str) {
        if let Some(value) = line.strip_prefix("PlayResX:") {
            self.note_header(SINFO_PLAYRESX, "PlayResX");
            self.script.play_res_x = parse_int(value);
        } else if let Some(value) = line.strip_prefix("PlayResY:") {
            self.note_header(SINFO_PLAYRESY, "PlayResY");
            self.script.play_res_y = parse_int(value);
        } else if let Some(value) = line.strip_prefix("LayoutResX:") {
            self.note_header(SINFO_LAYOUTRESX, "LayoutResX");
            self.script.layout_res_x = parse_int(value);
        } else if let Some(value) = line.strip_prefix("LayoutResY:") {
            self.note_header(SINFO_LAYOUTRESY, "LayoutResY");
            self.script.layout_res_y = parse_int(value);
        } else if let Some(value) = line.strip_prefix("Timer:") {
            self.note_header(SINFO_TIMER, "Timer");
            self.script.timer = parse_f64(value);
        } else if let Some(value) = line.strip_prefix("WrapStyle:") {
            self.note_header(SINFO_WRAPSTYLE, "WrapStyle");
            self.script.wrap_style = parse_int(value);
        } else if let Some(value) = line.strip_prefix("ScaledBorderAndShadow:") {
            self.note_header(SINFO_SCALEDBORDER, "ScaledBorderAndShadow");
            self.script.scaled_border_and_shadow = parse_bool(value);
        } else if let Some(value) = line.strip_prefix("Kerning:") {
            self.note_header(SINFO_KERNING, "Kerning");
            self.script.kerning = parse_bool(value);
        } else if let Some(value) = line.strip_prefix("YCbCr Matrix:") {
            self.note_header(SINFO_COLOURMATRIX, "YCbCr Matrix");
            self.script.ycbcr_matrix = parse_ycbcr_matrix(value);
        } else if let Some(value) = line.strip_prefix("Language:") {
            self.note_header(SINFO_LANGUAGE, "Language");
            const C_SPACES: &[char] = &[' ', '\t', '\n', '\x0b', '\x0c', '\r'];
            let value = value.trim_start_matches(C_SPACES);
            // libass strndup(p, 2) takes two BYTES; taking two characters is
            // equivalent for the ISO-639-1 codes this is meant for and keeps
            // the value valid UTF-8.
            self.script.language = Some(value.chars().take(2).collect());
        } else if let Some(value) = line.strip_prefix("ScriptType:") {
            self.note_header(SINFO_SCRIPTTYPE, "ScriptType");
            self.parse_script_type(value);
        } else if let Some(rest) = line.strip_prefix("; Script generated by ") {
            if rest.starts_with("FFmpeg/Lavc") {
                self.header_flags |= GENBY_FFMPEG;
            }
        }
    }

    /// `parse_script_type`: VSFilter-compatible — no leading 'v' required,
    /// the value is read from the last non-space backwards ("4.00" marks
    /// SSA, a trailing '+' marks ASS).
    fn parse_script_type(&mut self, value: &str) {
        let trimmed = value.trim_end_matches(ASS_SPACES);
        if trimmed.len() < 4 {
            return;
        }
        let (body, track_type) = match trimmed.strip_suffix('+') {
            Some(body) => (body, TrackType::Ass),
            None => (trimmed, TrackType::Ssa),
        };
        let bytes = body.as_bytes();
        if bytes.len() >= 4 && &bytes[bytes.len() - 4..] == b"4.00" {
            self.script.track_type = track_type;
        }
    }

    /// `custom_format_line_compatibility`: scripts with custom Format lines
    /// and no explicit ScaledBorderAndShadow header default it to yes,
    /// because libass historically did.
    fn custom_format_compat(&mut self, format: &str, standard: &str) {
        if self.header_flags & SINFO_SCALEDBORDER == 0 && !format_line_compare(format, standard) {
            log::info!(
                "ass parser: track has custom format line(s); \
                 'ScaledBorderAndShadow' will default to 'yes'"
            );
            self.script.scaled_border_and_shadow = true;
        }
    }

    /// `detect_legacy_conv_subs`: pre-2020 FFmpeg SRT→ASS conversions did
    /// not write ScaledBorderAndShadow but expected "yes"; they are
    /// recognized by their exact header set and single Default style.
    fn detect_legacy_conv_subs(&self) -> bool {
        self.header_flags == (SINFO_SCRIPTTYPE | SINFO_PLAYRESX | SINFO_PLAYRESY | GENBY_FFMPEG)
            && self.script.styles.len() == 2
            && self.script.styles[1]
                .name
                .as_bytes()
                .starts_with(b"Default")
    }

    fn process_styles_line(&mut self, line: &str) {
        if let Some(value) = line.strip_prefix("Format:") {
            let format = value.trim_start_matches(ASS_SPACES);
            self.style_format = Some(format.to_string());
            let standard = match self.script.track_type {
                TrackType::Ass => ASS_STYLE_FORMAT,
                _ => SSA_STYLE_FORMAT,
            };
            self.custom_format_compat(format, standard);
        } else if let Some(value) = line.strip_prefix("Style:") {
            let body = value.trim_start_matches(ASS_SPACES);
            let format = self
                .style_format
                .get_or_insert_with(|| match self.script.track_type {
                    // No style format header: probably an ancient script.
                    TrackType::Ssa => SSA_STYLE_FORMAT.to_string(),
                    _ => ASS_STYLE_FORMAT.to_string(),
                })
                .clone();
            process_style_line(&mut self.script, &format, body);
        }
    }

    fn process_events_line(&mut self, line: &str) {
        if let Some(value) = line.strip_prefix("Format:") {
            let format = value.trim_start_matches(ASS_SPACES);
            self.event_format = Some(format.to_string());
            let standard = match self.script.track_type {
                TrackType::Ass => ASS_EVENT_FORMAT,
                _ => SSA_EVENT_FORMAT,
            };
            self.custom_format_compat(format, standard);
            if self.detect_legacy_conv_subs() {
                log::info!("ass parser: track treated as legacy ffmpeg sub");
                self.script.scaled_border_and_shadow = true;
            }
        } else if let Some(value) = line.strip_prefix("Dialogue:") {
            let body = value.trim_start_matches(ASS_SPACES);
            let format = self
                .event_format
                .get_or_insert_with(|| {
                    log::debug!("ass parser: no event format found, using fallback");
                    match self.script.track_type {
                        TrackType::Ssa => SSA_EVENT_FORMAT.to_string(),
                        _ => ASS_EVENT_FORMAT.to_string(),
                    }
                })
                .clone();
            if let Some(event) = parse_event_tail(&self.script, &format, body, 0) {
                self.script.events.push(event);
            }
        } else {
            log::debug!("ass parser: not understood: '{}'", preview(line));
        }
    }

    /// `process_fonts_line`: a `fontname:` header starts (and flushes any
    /// previous) attachment; every other line is raw uuencoded data.
    fn process_fonts_line(&mut self, line: &str) {
        if let Some(value) = line.strip_prefix("fontname:") {
            self.flush_font();
            self.fontname = Some(value.trim_start_matches(ASS_SPACES).to_string());
            return;
        }
        if self.fontname.is_none() {
            log::debug!("ass parser: not understood: '{}'", preview(line));
            return;
        }
        self.fontdata.extend_from_slice(line.as_bytes());
    }

    /// `decode_font`: uudecode-style unpacking (4 chars → 3 bytes, offset
    /// 33, 6 bits per char); a data length of 4k+1 is invalid and discards
    /// the whole attachment.
    fn flush_font(&mut self) {
        let Some(filename) = self.fontname.take() else {
            return;
        };
        let data = std::mem::take(&mut self.fontdata);
        if data.len() % 4 == 1 {
            log::warn!("ass parser: bad encoded data size for font '{filename}'");
            return;
        }
        let mut decoded = Vec::with_capacity(data.len() / 4 * 3 + 2);
        for chunk in data.chunks(4) {
            let mut value: u32 = 0;
            for (i, &c) in chunk.iter().enumerate() {
                value |= u32::from(c.wrapping_sub(33) & 63) << (6 * (3 - i));
            }
            decoded.push((value >> 16) as u8);
            if chunk.len() >= 3 {
                decoded.push((value >> 8) as u8);
            }
            if chunk.len() >= 4 {
                decoded.push(value as u8);
            }
        }
        self.script.fonts.push(EmbeddedFont {
            filename,
            data: decoded,
        });
    }

    /// `parse_memory` tail: number events, reject scripts whose type never
    /// became known.
    fn finish(mut self) -> Result<Script, NotAScript> {
        for (index, event) in self.script.events.iter_mut().enumerate() {
            event.read_order = index as i32;
        }
        if self.script.track_type == TrackType::Unknown {
            return Err(NotAScript);
        }
        Ok(self.script)
    }

    /// Takes the script for the incremental (decoder) path: events keep the
    /// ReadOrder from their chunks and the type check does not apply (the
    /// decoder's header may legitimately omit ScriptType — libass tracks
    /// created by `ass_new_track` are always usable).
    pub(crate) fn into_script(self) -> Script {
        self.script
    }
}

// Re-exported here so the module surface groups "parse + adjust" together.
pub(crate) use super::fields::apply_force_style;

#[cfg(test)]
mod tests {
    use super::super::value::{Color, YcbcrMatrix, VALIGN_CENTER, VALIGN_TOP};
    use super::*;
    use crate::subtitle::test_util;

    #[test]
    fn parses_the_shared_minimal_script() {
        let script = parse(&test_util::minimal_ass(test_util::HELLO_EVENT)).expect("parse");
        assert_eq!(script.track_type, TrackType::Ass);
        assert_eq!((script.play_res_x, script.play_res_y), (640, 360));

        // Built-in default at 0, the script's own Default after it.
        assert_eq!(script.styles.len(), 2);
        assert_eq!(script.default_style, 1);
        let style = &script.styles[1];
        assert_eq!(style.name, "Default");
        assert_eq!(style.font_name, "DejaVu Sans");
        assert_eq!(style.font_size, 48.0);
        assert_eq!(
            style.primary_colour,
            Color {
                r: 255,
                g: 255,
                b: 255,
                t: 0
            }
        );
        assert_eq!((style.scale_x, style.scale_y), (1.0, 1.0));
        assert_eq!(style.border_style, 1);
        assert_eq!((style.outline, style.shadow), (2.0, 0.0));
        assert_eq!(style.alignment, 2); // numpad 2 == legacy 2
        assert_eq!(
            (style.margin_l, style.margin_r, style.margin_v),
            (10, 10, 10)
        );
        assert_eq!(style.encoding, 1);

        assert_eq!(script.events.len(), 1);
        let event = &script.events[0];
        assert_eq!(event.text, "Hello subtitle");
        assert_eq!((event.start_ms, event.duration_ms), (0, 5_000));
        assert_eq!(event.style, 1);
        assert_eq!(event.read_order, 0);
    }

    #[test]
    fn respects_reordered_format_lines() {
        let script = parse(
            "[V4+ Styles]\n\
             Format: Fontsize, Name, Alignment\n\
             Style: 64,Big,8\n\
             [Events]\n\
             Format: Text, Start, End\n\
             Dialogue: ignored tail\n\
             ",
        )
        .expect("parse");
        let style = &script.styles[1];
        assert_eq!((style.name.as_str(), style.font_size), ("Big", 64.0));
        assert_eq!(style.alignment, 2 | VALIGN_TOP); // numpad 8
                                                     // Text first: the whole tail is the text, no times parsed.
        let event = &script.events[0];
        assert_eq!(event.text, "ignored tail");
        assert_eq!((event.start_ms, event.duration_ms), (0, 0));
    }

    #[test]
    fn short_style_line_keeps_zeroed_defaults() {
        let script = parse(
            "[V4+ Styles]\n\
             Style: **Starred\n\
             ",
        )
        .expect("parse");
        let style = &script.styles[1];
        assert_eq!(style.name, "Starred"); // leading '*'s stripped
        assert_eq!(style.font_name, "Arial"); // unparsed fields fall back
        assert_eq!((style.scale_x, style.scale_y), (1.0, 1.0)); // 100/100
        assert_eq!(style.font_size, 0.0);
        assert_eq!(style.alignment, 0);
        assert_eq!(script.default_style, 0); // "Starred" != "Default"
    }

    #[test]
    fn ssa_styles_get_vsfilter_compatibility_fixups() {
        let header = "[Script Info]\nScriptType: v4.00\n\n[V4 Styles]\n";
        let format = "Format: Name, PrimaryColour, SecondaryColour, TertiaryColour, \
                      BackColour, Bold, Alignment, AlphaLevel\n";
        let parse_one = |style_line: &str| {
            let script = parse(&format!("{header}{format}{style_line}\n")).expect("parse ssa");
            script.styles[1].clone()
        };

        let style = parse_one("Style: A,&HFF&,&H00FF00&,&H123456&,&H0000FF&,-1,2,100");
        assert_eq!(script_type_of(header), TrackType::Ssa);
        // BackColour (&H0000FF& = red) overwrites OutlineColour; the SSA
        // TertiaryColour field is never read.
        let rgb = |c: Color| (c.r, c.g, c.b);
        assert_eq!(rgb(style.outline_colour), (255, 0, 0));
        assert_eq!(rgb(style.outline_colour), rgb(style.back_colour));
        // AlphaLevel becomes the front alpha; back alpha is forced to 0x80.
        assert_eq!(style.primary_colour.t, 100);
        assert_eq!(style.secondary_colour.t, 100);
        assert_eq!(style.outline_colour.t, 100);
        assert_eq!(style.back_colour.t, 0x80);
        // Bold -1 normalizes to 1.
        assert_eq!(style.bold, 1);
        // SSA alignment 2 stays 2 (legacy encoding already).
        assert_eq!(style.alignment, 2);

        // VSFilter remaps: SSA 8 → 3, SSA 4 → 11.
        assert_eq!(parse_one("Style: B,,,,,0,8,0").alignment, 3);
        assert_eq!(parse_one("Style: C,,,,,0,4,0").alignment, 11);
    }

    fn script_type_of(content: &str) -> TrackType {
        parse(content).map(|s| s.track_type).unwrap_or_default()
    }

    #[test]
    fn ass_alignment_is_numpad_converted() {
        let script = parse(
            "[V4+ Styles]\n\
             Format: Name, Alignment\n\
             Style: Center,5\n\
             Style: TopLeft,7\n\
             ",
        )
        .expect("parse");
        assert_eq!(script.styles[1].alignment, 2 | VALIGN_CENTER);
        assert_eq!(script.styles[2].alignment, 1 | VALIGN_TOP);
    }

    #[test]
    fn event_tail_preserves_text_verbatim() {
        let script = parse(
            "[V4+ Styles]\n\
             Style: Default\n\
             [Events]\n\
             Format: Layer, Start, End, Style, Actor, MarginL, MarginR, MarginV, Effect, Text\n\
             Dialogue: 0,0:00:01.00,0:00:02.00,Default,Speaker,0,0,0,,  {\\b1}Hi, there\\N \t \n\
             Comment: 0,0:00:01.00,0:00:02.00,Default,,0,0,0,,ignored\n\
             Dialogue: 0,0:00:01.00\n\
             Dialogue: 1,0:00:02.00,0:00:01.00,Missing,,0,0,0,,\n\
             ",
        )
        .expect("parse");
        assert_eq!(script.events.len(), 2, "comment + short dialogue dropped");

        let event = &script.events[0];
        // Leading spaces after the last comma survive; tags and inner commas
        // are text; trailing spaces/tabs are stripped.
        assert_eq!(event.text, "  {\\b1}Hi, there\\N");
        assert_eq!(event.name, "Speaker"); // Actor aliases to Name
        assert_eq!((event.start_ms, event.duration_ms), (1_000, 1_000));

        let event = &script.events[1];
        assert_eq!(event.text, ""); // empty text events are kept
        assert_eq!(event.style, script.default_style); // unknown style falls back
        assert_eq!(event.duration_ms, -1_000); // End < Start is preserved
        assert_eq!(event.read_order, 1);
    }

    #[test]
    fn dialogue_without_event_format_uses_fallback() {
        let script = parse(
            "[Script Info]\n\
             ScriptType: v4.00+\n\
             [Events]\n\
             Dialogue: 0,0:00:00.00,0:00:01.00,Default,,0,0,0,,text\n\
             ",
        )
        .expect("parse");
        assert_eq!(script.events.len(), 1);
        assert_eq!(script.events[0].text, "text");
        assert_eq!(script.events[0].duration_ms, 1_000);
    }

    #[test]
    fn info_headers_parse_and_duplicates_overwrite() {
        let script = parse(
            "[Script Info]\n\
             ScriptType: v4.00+\n\
             PlayResX: 1280\n\
             PlayResX: 1920\n\
             PlayResY: 1080\n\
             LayoutResX: 640\n\
             LayoutResY: 360\n\
             Timer: 100.0000\n\
             WrapStyle: 2\n\
             ScaledBorderAndShadow: yes\n\
             Kerning: yes\n\
             YCbCr Matrix: TV.709\n\
             Language: eng\n\
             playresx: 1\n\
             ",
        )
        .expect("parse");
        assert_eq!(script.play_res_x, 1920, "last duplicate wins");
        assert_eq!(script.play_res_y, 1080);
        assert_eq!((script.layout_res_x, script.layout_res_y), (640, 360));
        assert_eq!(script.timer, 100.0);
        assert_eq!(script.wrap_style, 2);
        assert!(script.scaled_border_and_shadow);
        assert!(script.kerning);
        assert_eq!(script.ycbcr_matrix, YcbcrMatrix::Bt709Tv);
        assert_eq!(script.language.as_deref(), Some("en"));
        assert_eq!(script.play_res_x, 1920, "info prefixes are case-sensitive");
    }

    #[test]
    fn script_type_detection_is_vsfilter_compatible() {
        let of = |value: &str| script_type_of(&format!("[Script Info]\nScriptType:{value}\n"));
        assert_eq!(of(" v4.00+"), TrackType::Ass);
        assert_eq!(of(" v4.00"), TrackType::Ssa);
        assert_eq!(of(" V4.00+  "), TrackType::Ass); // trailing spaces trimmed
        assert_eq!(of("anything4.00"), TrackType::Ssa); // suffix check only
        assert_eq!(of(" v4.1"), TrackType::Unknown);
        assert_eq!(of(" 4.0+"), TrackType::Unknown); // too short after '+'
    }

    #[test]
    fn rejects_input_that_never_reveals_a_type() {
        assert_eq!(parse("definitely not an ass script"), Err(NotAScript));
        assert_eq!(
            parse("[Script Info]\nTitle: no type here\n[Events]\n"),
            Err(NotAScript)
        );
    }

    #[test]
    fn custom_format_lines_default_scaled_border_to_yes() {
        // Custom style format (extra field), no explicit SBAS header.
        let custom = "[V4+ Styles]\nFormat: Name, Fontsize, RelativeTo\n";
        assert!(parse(custom).expect("parse").scaled_border_and_shadow);

        // An explicit header seen FIRST pins the value.
        let pinned = format!("[Script Info]\nScaledBorderAndShadow: no\n{custom}");
        assert!(!parse(&pinned).expect("parse").scaled_border_and_shadow);

        // The standard format (even via the Actor alias in events) is not
        // "custom".
        let standard = "[V4+ Styles]\n[Events]\nFormat: Layer, Start, End, Style, Actor, \
                        MarginL, MarginR, MarginV, Effect, Text\n";
        assert!(!parse(standard).expect("parse").scaled_border_and_shadow);
    }

    #[test]
    fn detects_legacy_ffmpeg_converted_subtitles() {
        let legacy = "[Script Info]\n\
                      ; Script generated by FFmpeg/Lavc57.107.100\n\
                      ScriptType: v4.00+\n\
                      PlayResX: 384\n\
                      PlayResY: 288\n\
                      \n\
                      [V4+ Styles]\n\
                      Format: Name, Fontname, Fontsize, PrimaryColour, SecondaryColour, \
                      OutlineColour, BackColour, Bold, Italic, Underline, StrikeOut, ScaleX, \
                      ScaleY, Spacing, Angle, BorderStyle, Outline, Shadow, Alignment, MarginL, \
                      MarginR, MarginV, Encoding\n\
                      Style: Default,Arial,16,&Hffffff,&Hffffff,&H0,&H0,0,0,0,0,100,100,0,0,1,1,0,2,10,10,10,0\n\
                      \n\
                      [Events]\n\
                      Format: Layer, Start, End, Style, Name, MarginL, MarginR, MarginV, Effect, Text\n\
                      Dialogue: 0,0:00:00.00,0:00:01.00,Default,,0,0,0,,legacy\n";
        assert!(parse(legacy).expect("parse").scaled_border_and_shadow);

        // Any extra tracked header breaks the exact-set detection.
        let with_wrapstyle = legacy.replace("PlayResY: 288\n", "PlayResY: 288\nWrapStyle: 0\n");
        assert!(
            !parse(&with_wrapstyle)
                .expect("parse")
                .scaled_border_and_shadow
        );
    }

    #[test]
    fn handles_bom_crlf_blank_lines_and_nul_truncation() {
        let script = parse(
            "\u{feff}[Script Info]\r\n\
             ScriptType: v4.00+\r\n\
             \r\n\
             \u{feff}PlayResX: 640\r\n\
             [Events]\r\n\
             Dialogue: 0,0:00:00.00,0:00:01.00,Default,,0,0,0,,kept\r\n\
             \0Dialogue: 0,0:00:01.00,0:00:02.00,Default,,0,0,0,,after nul\r\n",
        )
        .expect("parse");
        assert_eq!(script.play_res_x, 640);
        assert_eq!(script.events.len(), 1, "everything after NUL is invisible");
        assert_eq!(script.events[0].text, "kept");
    }

    #[test]
    fn unknown_section_headers_bleed_into_the_current_section() {
        // libass has no notion of unknown sections: "[Aegisub Project
        // Garbage]" does not switch state, so following lines still parse
        // as Script Info. Preserved bug-for-bug.
        let script = parse(
            "[Script Info]\n\
             ScriptType: v4.00+\n\
             PlayResX: 640\n\
             [Aegisub Project Garbage]\n\
             PlayResX: 999\n\
             ",
        )
        .expect("parse");
        assert_eq!(script.play_res_x, 999);
    }

    #[test]
    fn fonts_section_uudecodes_attachments() {
        let script = parse(
            "[Script Info]\n\
             ScriptType: v4.00+\n\
             [Fonts]\n\
             fontname: sample_bold.ttf\n\
             !!!!\n\
             0B\n\
             fontname: dropped.ttf\n\
             !!!!!\n\
             fontname: tail.ttf\n\
             0000\n\
             ",
        )
        .expect("parse");
        assert_eq!(script.fonts.len(), 2, "4k+1 data length discards the font");

        // "!!!!" decodes to three zero bytes; "0B" is ('0'-33)=15, ('B'-33)=33
        // → (15<<18 | 33<<12) >> 16 = 62.
        assert_eq!(script.fonts[0].filename, "sample_bold.ttf");
        assert_eq!(script.fonts[0].data, vec![0, 0, 0, 62]);

        // "0000": 15 in each 6-bit slot = 3,994,575 → bytes 60, 243, 207;
        // flushed by end-of-input, not by another fontname line.
        assert_eq!(script.fonts[1].filename, "tail.ttf");
        assert_eq!(script.fonts[1].data, vec![60, 243, 207]);
    }

    #[test]
    fn incremental_feed_matches_the_decoder_contract() {
        // The lavcodec path: codec private (header only), then timed chunks
        // in `ReadOrder, Layer, Style, Name, MarginL, MarginR, MarginV,
        // Effect, Text` form (FFmpeg's ff_ass_subtitle_header output shape).
        let mut parser = ScriptParser::new();
        parser.process_codec_private(
            "[Script Info]\n\
             ScriptType: v4.00+\n\
             PlayResX: 384\n\
             PlayResY: 288\n\
             [V4+ Styles]\n\
             Format: Name, Fontname, Fontsize\n\
             Style: Default,Arial,16\n\
             [Events]\n\
             Format: Layer, Start, End, Style, Name, MarginL, MarginR, MarginV, Effect, Text\n",
        );
        parser.process_chunk("0,0,Default,,0,0,0,,First line", 1_000, 2_000);
        parser.process_chunk("1,0,Default,,0,0,0,,Second, with comma", 4_000, 1_500);
        // Duplicate ReadOrder: dropped (Matroska resend semantics).
        parser.process_chunk("0,0,Default,,0,0,0,,Duplicate", 9_000, 1_000);

        let script = parser.into_script();
        assert_eq!(script.play_res_x, 384);
        assert_eq!(script.events.len(), 2);
        let first = &script.events[0];
        assert_eq!(first.text, "First line");
        assert_eq!((first.start_ms, first.duration_ms), (1_000, 2_000));
        assert_eq!(first.read_order, 0);
        assert_eq!(first.style, 1);
        let second = &script.events[1];
        assert_eq!(second.text, "Second, with comma");
        assert_eq!((second.start_ms, second.duration_ms), (4_000, 1_500));
        assert_eq!(second.read_order, 1);
    }

    #[test]
    fn codec_private_without_event_format_gets_fallback() {
        let mut parser = ScriptParser::new();
        parser.process_codec_private("[Script Info]\nScriptType: v4.00+\n");
        parser.process_chunk("0,0,Default,,0,0,0,,text", 0, 1_000);
        let script = parser.into_script();
        assert_eq!(script.events.len(), 1);
        assert_eq!(script.events[0].text, "text");
    }

    #[test]
    fn malformed_chunk_reserves_read_order_only_after_first_event() {
        // libass builds its read-order bitmap lazily: on the first chunk
        // call that arrives with events already stored. Before that, a
        // malformed chunk (parse failure rolls the event back) leaves no
        // trace, so a later valid chunk may reuse its ReadOrder. After the
        // bitmap exists, the duplicate check is test-and-set and reserves
        // the ReadOrder even when the tail fails to parse.
        let header = "[Events]\n\
             Format: Layer, Start, End, Style, Name, MarginL, MarginR, MarginV, Effect, Text\n";

        // Empty track: malformed `5` (no Layer, no tail) is discarded
        // without reserving ReadOrder 5 — the retry renders.
        let mut parser = ScriptParser::new();
        parser.process_codec_private(header);
        parser.process_chunk("5", 0, 1_000);
        parser.process_chunk("5,0,Default,,0,0,0,,Visible", 0, 1_000);
        let script = parser.into_script();
        assert_eq!(script.events.len(), 1);
        assert_eq!(script.events[0].text, "Visible");

        // After one stored event the bitmap is active: a malformed chunk
        // now reserves its ReadOrder and the retry is dropped.
        let mut parser = ScriptParser::new();
        parser.process_codec_private(header);
        parser.process_chunk("1,0,Default,,0,0,0,,First", 0, 1_000);
        parser.process_chunk("7", 0, 1_000);
        parser.process_chunk("7,0,Default,,0,0,0,,Late", 0, 1_000);
        let script = parser.into_script();
        assert_eq!(script.events.len(), 1);
        assert_eq!(script.events[0].text, "First");
    }

    #[test]
    fn chunk_layer_is_a_wrapping_int_header() {
        // ReadOrder uses atoi (saturating); Layer uses parse_int_header
        // (wrapping) — both fed before the shared tail.
        let mut parser = ScriptParser::new();
        parser.process_codec_private(
            "[Events]\n\
             Format: Layer, Start, End, Style, Name, MarginL, MarginR, MarginV, Effect, Text\n",
        );
        parser.process_chunk("7,4294967298,Default,,0,0,0,,wrapped", 100, 200);
        let script = parser.into_script();
        assert_eq!(script.events.len(), 1);
        assert_eq!(script.events[0].read_order, 7);
        assert_eq!(script.events[0].layer, 2); // 2^32 + 2 wraps to 2
    }

    #[test]
    fn force_style_applies_raw_values() {
        let mut script = parse(&test_util::minimal_ass(test_util::HELLO_EVENT)).expect("parse");
        apply_force_style(
            &mut script,
            &[
                "FontSize=96",
                "default.PrimaryColour=&HFF&", // style scope is case-insensitive
                "PlayResX=1920",               // script-header override
                "Bold=700",                    // raw: no 0/1 squashing
                "ScaleX=200",                  // raw: NOT divided by 100
                "Blur=2.5",
                "Justify=1",
                "AlphaLevel=300", // clamped to 255, front AND back
                "NoEqualsSignHere",
            ],
        );
        assert_eq!(script.play_res_x, 1920);
        for style in &script.styles {
            assert_eq!(style.font_size, 96.0);
            assert_eq!(style.bold, 700);
            assert_eq!(style.scale_x, 200.0);
            assert_eq!(style.blur, 2.5);
            assert_eq!(style.justify, 1);
            assert_eq!(style.primary_colour.t, 255);
            assert_eq!(style.back_colour.t, 255);
        }
        // The scoped override only hit the style actually named Default —
        // including the built-in default style at index 0.
        assert_eq!(script.styles[0].primary_colour.r, 255);
        assert_eq!(script.styles[1].primary_colour.r, 255);
        assert_eq!(script.styles[1].primary_colour.b, 0);
    }
}
