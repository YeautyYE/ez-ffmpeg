//! Text shaping for the pure-Rust renderer: Unicode bidi run splitting
//! (unicode-bidi) and glyph shaping (rustybuzz), with the simple
//! left-to-right fallback used when `TextShaping::Simple` is selected.

use super::fonts::LoadedFace;

/// One shaped glyph, positioned relative to the run origin in font units
/// (scaled to pixels by the caller, which knows the em size).
#[derive(Debug, Clone, Copy, PartialEq)]
pub(crate) struct ShapedGlyph {
    pub glyph_id: u16,
    /// Byte offset of the source character in the run's text.
    pub cluster: u32,
    pub x_advance: i32,
    pub x_offset: i32,
    pub y_offset: i32,
}

/// Splits `text` into bidi runs in VISUAL order, returning `(range, rtl)`
/// per run. Pure-LTR text yields a single run without allocating a bidi
/// context — the common case for subtitles.
pub(crate) fn bidi_runs(text: &str) -> Vec<(std::ops::Range<usize>, bool)> {
    let has_rtl = text.chars().any(|c| {
        matches!(
            unicode_bidi::bidi_class(c),
            unicode_bidi::BidiClass::R
                | unicode_bidi::BidiClass::AL
                | unicode_bidi::BidiClass::AN
                | unicode_bidi::BidiClass::RLE
                | unicode_bidi::BidiClass::RLO
                | unicode_bidi::BidiClass::RLI
        )
    });
    if !has_rtl {
        return vec![(0..text.len(), false)];
    }
    let info = unicode_bidi::BidiInfo::new(text, None);
    let Some(paragraph) = info.paragraphs.first() else {
        return vec![(0..text.len(), false)];
    };
    let (levels, runs) = info.visual_runs(paragraph, paragraph.range.clone());
    runs.into_iter()
        .map(|range| {
            let rtl = levels[range.start].is_rtl();
            (range, rtl)
        })
        .collect()
}

/// Shapes one run with rustybuzz (complex path: ligatures, marks, script
/// shaping — the equivalent of libass's HarfBuzz shaper).
pub(crate) fn shape_complex(face: &LoadedFace, text: &str, rtl: bool) -> Vec<ShapedGlyph> {
    let mut buffer = rustybuzz::UnicodeBuffer::new();
    buffer.push_str(text);
    buffer.set_direction(if rtl {
        rustybuzz::Direction::RightToLeft
    } else {
        rustybuzz::Direction::LeftToRight
    });
    let output = rustybuzz::shape(face.shaper(), &[], buffer);
    output
        .glyph_infos()
        .iter()
        .zip(output.glyph_positions())
        .map(|(info, pos)| ShapedGlyph {
            glyph_id: info.glyph_id as u16,
            cluster: info.cluster,
            x_advance: pos.x_advance,
            x_offset: pos.x_offset,
            y_offset: pos.y_offset,
        })
        .collect()
}

/// Simple shaper (`TextShaping::Simple` / rustybuzz failure fallback):
/// cmap lookup + horizontal advances, no ligatures, no reordering.
pub(crate) fn shape_simple(face: &LoadedFace, text: &str) -> Vec<ShapedGlyph> {
    let face = face.face();
    text.char_indices()
        .map(|(offset, ch)| {
            let glyph_id = face.glyph_index(ch).unwrap_or(ttf_parser::GlyphId(0));
            let x_advance = face.glyph_hor_advance(glyph_id).map_or(0, i32::from);
            ShapedGlyph {
                glyph_id: glyph_id.0,
                cluster: offset as u32,
                x_advance,
                x_offset: 0,
                y_offset: 0,
            }
        })
        .collect()
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::subtitle::render::fonts::{FaceRequest, FontStore};
    use crate::subtitle::test_util;

    fn test_face() -> Option<LoadedFace> {
        let path = test_util::test_font()?;
        let mut store = FontStore::new(false);
        store.load_default_font_file(std::path::Path::new(path));
        let face_ref = store.select("whatever", FaceRequest::from_ass(0, 0))?;
        store.load(&face_ref)
    }

    #[test]
    fn ltr_text_is_one_run() {
        assert_eq!(bidi_runs("Hello world"), vec![(0..11, false)]);
    }

    #[test]
    fn rtl_text_splits_into_visual_runs() {
        // "abc " + Hebrew alef-bet + " def"
        let text = "abc \u{05d0}\u{05d1} def";
        let runs = bidi_runs(text);
        assert!(runs.len() >= 2, "expected mixed-direction runs: {runs:?}");
        assert!(runs.iter().any(|(_, rtl)| *rtl), "one run must be RTL");
        // Every byte of the text is covered exactly once.
        let mut covered: Vec<std::ops::Range<usize>> =
            runs.iter().map(|(r, _)| r.clone()).collect();
        covered.sort_by_key(|r| r.start);
        let total: usize = covered.iter().map(|r| r.len()).sum();
        assert_eq!(total, text.len());
    }

    #[test]
    fn complex_and_simple_shapers_agree_on_plain_ascii() {
        let Some(face) = test_face() else {
            eprintln!("skipping: no known test font present on this machine");
            return;
        };
        let complex = shape_complex(&face, "Hi", false);
        let simple = shape_simple(&face, "Hi");
        assert_eq!(complex.len(), 2);
        assert_eq!(simple.len(), 2);
        // Same glyphs and advances for text with no shaping rules.
        assert_eq!(complex[0].glyph_id, simple[0].glyph_id);
        assert_eq!(complex[0].x_advance, simple[0].x_advance);
        assert!(complex[0].x_advance > 0);
    }

    #[test]
    fn missing_glyphs_map_to_notdef() {
        let Some(face) = test_face() else {
            eprintln!("skipping: no known test font present on this machine");
            return;
        };
        // U+FFFF is unassigned in every font.
        let shaped = shape_simple(&face, "\u{ffff}");
        assert_eq!(shaped[0].glyph_id, 0);
    }
}
