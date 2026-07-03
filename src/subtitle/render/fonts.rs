//! Font discovery, matching, and face access for the pure-Rust renderer.
//!
//! Mirrors the font semantics the filter exposed on top of libass:
//! an optional system database (`FontProvider::Autodetect`), an extra
//! fonts directory, one pinned default font file, in-memory fonts
//! (container attachments and `[Fonts]` sections), and a default family.
//! Style matching scores family/weight/italic like fontconfig's simple
//! match, with the libass fallback chain (requested family, then the
//! default family, then the pinned file, then any loaded face).

use std::sync::Arc;

/// One loadable face: shared bytes plus the face index inside the file.
#[derive(Clone)]
pub(crate) struct FaceRef {
    pub data: Arc<Vec<u8>>,
    pub index: u32,
}

/// A parsed face bundled with the shared bytes that back it.
///
/// Self-referential by construction: `data` keeps the bytes alive for
/// exactly as long as the parsed face that borrows them. The face is
/// stored in its rustybuzz form (which derefs to `ttf_parser::Face`) so
/// the OpenType shaping tables are parsed once per face, not once per
/// shaped text run.
pub(crate) struct LoadedFace {
    _data: Arc<Vec<u8>>,
    // SAFETY invariant: `face` borrows from `_data`'s heap allocation,
    // which is stable (Arc) and dropped after `face` (field order).
    face: rustybuzz::Face<'static>,
}

impl LoadedFace {
    fn parse(face_ref: &FaceRef) -> Option<Self> {
        let data = Arc::clone(&face_ref.data);
        // SAFETY: the 'static lifetime is a private lie — the bytes live in
        // an Arc held by the same struct and are never mutated; the face is
        // only exposed re-borrowed to the struct's own lifetime.
        let bytes: &'static [u8] = unsafe { std::slice::from_raw_parts(data.as_ptr(), data.len()) };
        let face = ttf_parser::Face::parse(bytes, face_ref.index).ok()?;
        Some(Self {
            _data: data,
            face: rustybuzz::Face::from_face(face),
        })
    }

    pub(crate) fn face(&self) -> &ttf_parser::Face<'_> {
        &self.face
    }

    pub(crate) fn shaper(&self) -> &rustybuzz::Face<'_> {
        &self.face
    }
}

/// Weight/italic request derived from the ASS style state
/// (`ass_update_font` semantics).
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub(crate) struct FaceRequest {
    /// 400 normal / 700 bold / exact numeric weight (ASS `\b` >= 100).
    pub weight: u16,
    pub italic: bool,
}

impl FaceRequest {
    /// libass `ass_update_font`: bold 1/-1 = 700, <= 0 = 400, > 1 = exact;
    /// italic 1 = italic, everything else = upright.
    pub(crate) fn from_ass(bold: i32, italic: i32) -> Self {
        let weight = match bold {
            1 | -1 => 700,
            b if b <= 0 => 400,
            b => b.clamp(100, 1000) as u16,
        };
        Self {
            weight,
            italic: italic == 1,
        }
    }
}

struct StoredFace {
    face_ref: FaceRef,
    families: Vec<String>,
    weight: u16,
    italic: bool,
}

/// All font sources merged into one queryable store.
pub(crate) struct FontStore {
    faces: Vec<StoredFace>,
    /// Index of the face loaded from `default_font_file`, if any.
    pinned_default: Option<usize>,
    default_family: Option<String>,
    system: Option<fontdb::Database>,
}

impl FontStore {
    pub(crate) fn new(use_system_fonts: bool) -> Self {
        let system = use_system_fonts.then(|| {
            let mut db = fontdb::Database::new();
            db.load_system_fonts();
            db
        });
        Self {
            faces: Vec::new(),
            pinned_default: None,
            default_family: None,
            system,
        }
    }

    pub(crate) fn set_default_family(&mut self, family: Option<String>) {
        self.default_family = family;
    }

    /// Registers an in-memory font (container attachment or `[Fonts]`
    /// entry). Invalid font data is skipped with a log line, like libass.
    pub(crate) fn add_memory_font(&mut self, name: &str, data: Vec<u8>) {
        let data = Arc::new(data);
        let count = ttf_parser::fonts_in_collection(&data).unwrap_or(1);
        let mut any = false;
        for index in 0..count {
            if self.push_face(Arc::clone(&data), index) {
                any = true;
            }
        }
        if !any {
            log::warn!("subtitle fonts: attachment '{name}' contains no parsable font face");
        }
    }

    /// Loads every font file directly inside `dir` (libass `fontsdir`:
    /// non-recursive scan, unreadable entries skipped).
    pub(crate) fn load_fonts_dir(&mut self, dir: &std::path::Path) {
        let Ok(entries) = std::fs::read_dir(dir) else {
            log::warn!("subtitle fonts: cannot read fonts dir {}", dir.display());
            return;
        };
        for entry in entries.flatten() {
            let path = entry.path();
            if path.is_file() {
                self.load_font_file(&path);
            }
        }
    }

    /// Loads the pinned default font file; returns false when unusable.
    pub(crate) fn load_default_font_file(&mut self, path: &std::path::Path) -> bool {
        let before = self.faces.len();
        self.load_font_file(path);
        if self.faces.len() > before {
            self.pinned_default = Some(before);
            true
        } else {
            false
        }
    }

    fn load_font_file(&mut self, path: &std::path::Path) {
        match std::fs::read(path) {
            Ok(data) => {
                let data = Arc::new(data);
                let count = ttf_parser::fonts_in_collection(&data).unwrap_or(1);
                let mut any = false;
                for index in 0..count {
                    if self.push_face(Arc::clone(&data), index) {
                        any = true;
                    }
                }
                if !any {
                    log::debug!(
                        "subtitle fonts: {} is not a parsable font, skipped",
                        path.display()
                    );
                }
            }
            Err(err) => {
                log::warn!("subtitle fonts: cannot read {}: {err}", path.display());
            }
        }
    }

    fn push_face(&mut self, data: Arc<Vec<u8>>, index: u32) -> bool {
        let Ok(face) = ttf_parser::Face::parse(&data, index) else {
            return false;
        };
        let mut families = Vec::new();
        for name in face.names() {
            let is_family = name.name_id == ttf_parser::name_id::FAMILY
                || name.name_id == ttf_parser::name_id::TYPOGRAPHIC_FAMILY
                || name.name_id == ttf_parser::name_id::FULL_NAME;
            if is_family {
                if let Some(value) = name.to_string() {
                    if !families.iter().any(|f| f == &value) {
                        families.push(value);
                    }
                }
            }
        }
        let weight = face.weight().to_number();
        let italic = face.is_italic() || face.is_oblique();
        self.faces.push(StoredFace {
            face_ref: FaceRef { data, index },
            families,
            weight,
            italic,
        });
        true
    }

    /// Number of loaded (non-system) faces; used to detect whether any
    /// explicit font source is available at all.
    pub(crate) fn loaded_faces(&self) -> usize {
        self.faces.len()
    }

    /// Resolves `family` + style to a face, walking the libass fallback
    /// chain: requested family, default family, pinned default file, any
    /// loaded face, system database. `None` means no font exists anywhere
    /// (the event cannot be rendered; the caller logs one warning).
    pub(crate) fn select(&self, family: &str, request: FaceRequest) -> Option<FaceRef> {
        if let Some(found) = self.match_loaded(family, request) {
            return Some(found);
        }
        if let Some(default_family) = self.default_family.as_deref() {
            if !default_family.eq_ignore_ascii_case(family) {
                if let Some(found) = self.match_loaded(default_family, request) {
                    return Some(found);
                }
            }
        }
        if let Some(found) = self.match_system(family, request) {
            return Some(found);
        }
        if let Some(default_family) = self.default_family.as_deref() {
            if let Some(found) = self.match_system(default_family, request) {
                return Some(found);
            }
        }
        if let Some(pinned) = self.pinned_default {
            return Some(self.faces[pinned].face_ref.clone());
        }
        if let Some(stored) = self.faces.first() {
            return Some(stored.face_ref.clone());
        }
        // Last resort: any sans-serif the system knows.
        self.match_system("sans-serif", request)
    }

    fn match_loaded(&self, family: &str, request: FaceRequest) -> Option<FaceRef> {
        let family = family.trim();
        let mut best: Option<(u32, &StoredFace)> = None;
        for stored in &self.faces {
            let family_match = stored
                .families
                .iter()
                .any(|f| f.eq_ignore_ascii_case(family));
            if !family_match {
                continue;
            }
            let score = style_distance(stored.weight, stored.italic, request);
            if best.as_ref().is_none_or(|(s, _)| score < *s) {
                best = Some((score, stored));
            }
        }
        best.map(|(_, stored)| stored.face_ref.clone())
    }

    fn match_system(&self, family: &str, request: FaceRequest) -> Option<FaceRef> {
        let db = self.system.as_ref()?;
        let family = family.trim();
        let query_family = match family.to_ascii_lowercase().as_str() {
            "sans-serif" | "" => fontdb::Family::SansSerif,
            "serif" => fontdb::Family::Serif,
            "monospace" => fontdb::Family::Monospace,
            _ => fontdb::Family::Name(family),
        };
        let query = fontdb::Query {
            families: &[query_family, fontdb::Family::SansSerif],
            weight: fontdb::Weight(request.weight),
            stretch: fontdb::Stretch::Normal,
            style: if request.italic {
                fontdb::Style::Italic
            } else {
                fontdb::Style::Normal
            },
        };
        let id = db.query(&query)?;
        let (source, index) = db.face_source(id)?;
        match source {
            fontdb::Source::Binary(bin) => Some(FaceRef {
                data: Arc::new(bin.as_ref().as_ref().to_vec()),
                index,
            }),
            fontdb::Source::File(path) => {
                let data = std::fs::read(&path).ok()?;
                Some(FaceRef {
                    data: Arc::new(data),
                    index,
                })
            }
            fontdb::Source::SharedFile(_, bin) => Some(FaceRef {
                data: Arc::new(bin.as_ref().as_ref().to_vec()),
                index,
            }),
        }
    }

    /// Parses a face for shaping/metrics; `None` when the bytes are bad.
    pub(crate) fn load(&self, face_ref: &FaceRef) -> Option<LoadedFace> {
        LoadedFace::parse(face_ref)
    }
}

/// Smaller is better: weight distance dominates, italic mismatch adds a
/// fixed penalty (fontconfig-like).
fn style_distance(weight: u16, italic: bool, request: FaceRequest) -> u32 {
    let weight_gap = (i32::from(weight) - i32::from(request.weight)).unsigned_abs();
    let italic_gap = if italic != request.italic { 2000 } else { 0 };
    weight_gap + italic_gap
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::subtitle::test_util;

    fn store_with_test_font() -> Option<(FontStore, String)> {
        let path = test_util::test_font()?;
        let mut store = FontStore::new(false);
        assert!(store.load_default_font_file(std::path::Path::new(path)));
        let face_ref = store.faces[0].face_ref.clone();
        let loaded = store.load(&face_ref).expect("parse test font");
        let family = store.faces[0].families.first().cloned().unwrap_or_default();
        assert!(loaded.face().number_of_glyphs() > 0);
        Some((store, family))
    }

    #[test]
    fn pinned_font_resolves_any_family() {
        let Some((store, family)) = store_with_test_font() else {
            eprintln!("skipping: no known test font present on this machine");
            return;
        };
        let request = FaceRequest::from_ass(0, 0);
        // The exact family matches...
        assert!(store.match_loaded(&family, request).is_some());
        // ...and a bogus family still lands on the pinned default.
        assert!(store.select("No Such Family Anywhere", request).is_some());
    }

    #[test]
    fn face_request_maps_ass_bold_italic() {
        assert_eq!(
            FaceRequest::from_ass(0, 0),
            FaceRequest {
                weight: 400,
                italic: false
            }
        );
        assert_eq!(
            FaceRequest::from_ass(1, 0),
            FaceRequest {
                weight: 700,
                italic: false
            }
        );
        assert_eq!(
            FaceRequest::from_ass(-1, 1),
            FaceRequest {
                weight: 700,
                italic: true
            }
        );
        assert_eq!(
            FaceRequest::from_ass(200, 0),
            FaceRequest {
                weight: 200,
                italic: false
            }
        );
        assert_eq!(
            FaceRequest::from_ass(700, 2),
            FaceRequest {
                weight: 700,
                italic: false
            }
        );
    }

    #[test]
    fn memory_font_registers_and_matches() {
        let Some(path) = test_util::test_font() else {
            eprintln!("skipping: no known test font present on this machine");
            return;
        };
        let data = std::fs::read(path).expect("read test font");
        let mut store = FontStore::new(false);
        store.add_memory_font("embedded.ttf", data);
        assert!(store.loaded_faces() > 0);
        assert!(store
            .select("DejaVu Sans", FaceRequest::from_ass(0, 0))
            .is_some());
    }

    #[test]
    fn empty_store_without_system_fonts_returns_none() {
        let store = FontStore::new(false);
        assert!(store.select("Arial", FaceRequest::from_ass(0, 0)).is_none());
    }
}
