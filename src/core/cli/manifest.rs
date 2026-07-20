//! The compatibility manifest: the single source of truth for what the
//! CLI-compat layer accepts, what it verifies, and what it documents.
//!
//! Three tables live here:
//! - the option accept surface (re-exported from [`super::table`]),
//! - the VERIFIED SHAPES — command shapes backed by a passing semantic
//!   golden (`tests/cli_goldens.rs`); ONLY these may execute in-process,
//! - the generated support table embedded in the module docs (a unit test
//!   asserts the docs stay in sync).
//!
//! `verified` status is earned by a golden, never hand-asserted: every entry
//! in [`VERIFIED_SHAPES`] names its golden test. Shapes that parse but match
//! no entry are emit-only ("unverified scaffolding").

use super::ir::CliIr;
#[cfg(test)]
use super::table::{Arity, ScopeRule, OPTION_TABLE};

/// Manifest revision. Bump on ANY change to the accept surface, the verified
/// shapes, or a rejection reason. Emitted code headers carry this value.
pub(crate) const MANIFEST_REVISION: u32 = 1;

/// The CLI dialect this parser implements: the option grammar was written
/// against the FFmpeg 7.1 command-line documentation and fftools sources.
pub(crate) const DIALECT: &str = "ffmpeg 7.1 command line";

/// One verified runtime profile: an FFmpeg release line with a passing
/// version-matched semantic suite. Micro versions vary per point release and
/// are deliberately not pinned.
pub(crate) struct RuntimeProfile {
    pub(crate) name: &'static str,
    /// libavcodec (major, minor).
    pub(crate) avcodec: (u32, u32),
    /// libavformat (major, minor).
    pub(crate) avformat: (u32, u32),
}

/// FFmpeg 7.1 ships libavcodec 61.19 / libavformat 61.7; FFmpeg 8.1 ships
/// libavcodec 62.28 / libavformat 62.12 (their `version.h` /
/// `version_major.h`).
pub(crate) const VERIFIED_PROFILES: &[RuntimeProfile] = &[
    RuntimeProfile {
        name: "FFmpeg 7.1",
        avcodec: (61, 19),
        avformat: (61, 7),
    },
    RuntimeProfile {
        name: "FFmpeg 8.1",
        avcodec: (62, 28),
        avformat: (62, 12),
    },
];

/// One golden-backed shape: a fixed scope-qualified option set (the
/// fingerprint), pinned values where the golden pinned them, and the output
/// extension the golden produced. Parameter variation inside a shape is
/// values/paths only.
pub(crate) struct VerifiedShape {
    pub(crate) id: &'static str,
    pub(crate) summary: &'static str,
    /// Sorted scope-qualified option keys, exactly as
    /// [`CliIr::fingerprint`] produces them.
    pub(crate) fingerprint: &'static [&'static str],
    /// Golden test that earns the `verified` status.
    pub(crate) golden: &'static str,
    /// Extra pinned-value predicate beyond the fingerprint.
    pub(crate) pins: fn(&CliIr) -> bool,
    /// Output extension the golden pinned (muxer identity is part of the
    /// shape).
    pub(crate) output_ext: &'static str,
}

fn pins_v1(ir: &CliIr) -> bool {
    ir.output.video_codec.as_deref() == Some("libx264")
        && ir.output.audio_codec.as_deref() == Some("aac")
}

fn pins_v2(ir: &CliIr) -> bool {
    pins_v1(ir) && ir.input.duration.is_none()
}

fn pins_v3(ir: &CliIr) -> bool {
    ir.output.audio_codec.as_deref() == Some("aac")
}

fn pins_v4(ir: &CliIr) -> bool {
    ir.output.video_codec.as_deref() == Some("mjpeg") && ir.output.frames_v == Some(1)
}

fn pins_v6(ir: &CliIr) -> bool {
    pins_v1(ir)
        && ir.output.format.as_deref() == Some("hls")
        && ir.output.hls_playlist_type.as_deref() == Some("vod")
        && ir.output.hls_list_size.as_deref() == Some("0")
        && ir.output.hls_segment_filename.is_some()
}

/// The R6 six: transcode, re-encoded clip, audio extract, thumbnail, scaled
/// transcode, VOD HLS.
pub(crate) const VERIFIED_SHAPES: &[VerifiedShape] = &[
    VerifiedShape {
        id: "V1",
        summary: "H.264/AAC transcode (crf + preset)",
        fingerprint: &["out:-c:a", "out:-c:v", "out:-crf", "out:-preset"],
        golden: "golden_v1_transcode",
        pins: pins_v1,
        output_ext: "mp4",
    },
    VerifiedShape {
        id: "V2",
        summary: "re-encoded clip (input -ss, output -t)",
        fingerprint: &["in:-ss", "out:-c:a", "out:-c:v", "out:-crf", "out:-t"],
        golden: "golden_v2_clip",
        pins: pins_v2,
        output_ext: "mp4",
    },
    VerifiedShape {
        id: "V3",
        summary: "audio extract (-vn, AAC)",
        fingerprint: &["out:-b:a", "out:-c:a", "out:-vn"],
        golden: "golden_v3_audio_extract",
        pins: pins_v3,
        output_ext: "m4a",
    },
    VerifiedShape {
        id: "V4",
        summary: "single-frame thumbnail (input -ss, -an, mjpeg)",
        fingerprint: &["in:-ss", "out:-an", "out:-c:v", "out:-frames:v"],
        golden: "golden_v4_thumbnail",
        pins: pins_v4,
        output_ext: "jpg",
    },
    VerifiedShape {
        id: "V5",
        summary: "scaled H.264/AAC transcode (-vf scale)",
        fingerprint: &["out:-c:a", "out:-c:v", "out:-crf", "out:-preset", "out:-vf"],
        golden: "golden_v5_scale",
        pins: pins_v1,
        output_ext: "mp4",
    },
    VerifiedShape {
        id: "V6",
        summary: "single-rendition VOD HLS",
        fingerprint: &[
            "out:-c:a",
            "out:-c:v",
            "out:-crf",
            "out:-f",
            "out:-hls_list_size",
            "out:-hls_playlist_type",
            "out:-hls_segment_filename",
            "out:-hls_time",
        ],
        golden: "golden_v6_hls",
        pins: pins_v6,
        output_ext: "m3u8",
    },
];

/// Verification verdict for a parsed command.
#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) enum ShapeStatus {
    /// Option set, pinned values and output extension match a golden-backed
    /// shape: execution is allowed.
    Verified(&'static str),
    /// Parses cleanly but no golden covers it: emit-only.
    Unverified,
}

pub(crate) fn classify(ir: &CliIr) -> ShapeStatus {
    let fingerprint = ir.fingerprint();
    for shape in VERIFIED_SHAPES {
        if fingerprint == shape.fingerprint
            && (shape.pins)(ir)
            && output_extension(&ir.output.url)
                .is_some_and(|e| e.eq_ignore_ascii_case(shape.output_ext))
        {
            return ShapeStatus::Verified(shape.id);
        }
    }
    ShapeStatus::Unverified
}

fn output_extension(url: &str) -> Option<&str> {
    let name = url.rsplit(['/', '\\']).next().unwrap_or(url);
    let (stem, ext) = name.rsplit_once('.')?;
    if stem.is_empty() {
        return None;
    }
    Some(ext)
}

/// Looks up a verified shape by id (emit uses this to stamp the shape's
/// summary and backing golden into generated headers).
pub(crate) fn shape(id: &str) -> Option<&'static VerifiedShape> {
    VERIFIED_SHAPES.iter().find(|shape| shape.id == id)
}

/// Renders the support table embedded in the module documentation. A unit
/// test asserts `mod.rs` contains exactly this text, so the docs can never
/// drift from the manifest.
#[cfg(test)]
pub(crate) fn support_table_markdown() -> String {
    let mut table = String::from("| option | scope | notes |\n|---|---|---|\n");
    for spec in OPTION_TABLE {
        let scope = match spec.scope {
            ScopeRule::Global => "global",
            ScopeRule::OutputOnly => "output",
            ScopeRule::InputOrOutput => "input or output (position-scoped)",
        };
        let notes = if spec.noop {
            "accepted, no in-process effect"
        } else if spec.arity == Arity::Flag {
            "flag"
        } else {
            "takes a value"
        };
        table.push_str(&format!("| `{}` | {} | {} |\n", spec.name, scope, notes));
    }
    table
}

#[cfg(test)]
mod tests {
    use super::super::parse::parse;
    use super::super::tokenize::tokenize;
    use super::*;

    fn ir_of(cmd: &str) -> CliIr {
        parse(&tokenize(cmd).unwrap()).unwrap_or_else(|e| panic!("{cmd} should parse: {e}"))
    }

    #[test]
    fn fingerprints_are_sorted_as_declared() {
        // The shape tables must stay byte-identical to what
        // CliIr::fingerprint produces, or classification silently misses.
        for shape in VERIFIED_SHAPES {
            let mut sorted = shape.fingerprint.to_vec();
            sorted.sort_unstable();
            assert_eq!(
                shape.fingerprint,
                &sorted[..],
                "shape {} is not sorted",
                shape.id
            );
        }
    }

    #[test]
    fn each_verified_shape_matches_its_command() {
        for (cmd, id) in [
            ("ffmpeg -i in.mkv -c:v libx264 -crf 23 -preset fast -c:a aac -y out.mp4", "V1"),
            ("ffmpeg -ss 10 -i in.mp4 -t 20 -c:v libx264 -crf 23 -c:a aac -y clip.mp4", "V2"),
            ("ffmpeg -i in.mp4 -vn -c:a aac -b:a 192k -y out.m4a", "V3"),
            ("ffmpeg -ss 5 -i in.mp4 -an -c:v mjpeg -frames:v 1 -y thumb.jpg", "V4"),
            (
                "ffmpeg -i in.mp4 -vf scale=1280:-2 -c:v libx264 -crf 23 -preset fast -c:a aac -y scaled.mp4",
                "V5",
            ),
            (
                "ffmpeg -i in.mp4 -c:v libx264 -crf 23 -c:a aac -f hls -hls_time 6 -hls_playlist_type vod -hls_list_size 0 -hls_segment_filename 'seg_%03d.ts' -y out.m3u8",
                "V6",
            ),
        ] {
            assert_eq!(
                classify(&ir_of(cmd)),
                ShapeStatus::Verified(id),
                "command should classify as {id}: {cmd}"
            );
        }
    }

    #[test]
    fn value_variation_keeps_the_shape() {
        let cmd = "ffmpeg -i '素材/其他 输入.mkv' -c:v libx264 -crf 18 -preset veryslow -c:a aac -y '输出/成品 v2.mp4'";
        assert_eq!(classify(&ir_of(cmd)), ShapeStatus::Verified("V1"));
    }

    #[test]
    fn option_set_variation_is_unverified() {
        // V1 minus -preset parses but is NOT the golden-backed shape.
        let cmd = "ffmpeg -i in.mkv -c:v libx264 -crf 23 -c:a aac -y out.mp4";
        assert_eq!(classify(&ir_of(cmd)), ShapeStatus::Unverified);
        // V1 plus -b:a likewise.
        let cmd =
            "ffmpeg -i in.mkv -c:v libx264 -crf 23 -preset fast -c:a aac -b:a 192k -y out.mp4";
        assert_eq!(classify(&ir_of(cmd)), ShapeStatus::Unverified);
    }

    #[test]
    fn pinned_codec_variation_is_unverified() {
        let cmd = "ffmpeg -i in.mkv -c:v mpeg4 -c:a aac -y out.mp4";
        assert_eq!(classify(&ir_of(cmd)), ShapeStatus::Unverified);
    }

    #[test]
    fn container_variation_is_unverified() {
        // The golden pinned .mp4; .mkv changes the muxer, hence the shape.
        let cmd = "ffmpeg -i in.mkv -c:v libx264 -crf 23 -preset fast -c:a aac -y out.mkv";
        assert_eq!(classify(&ir_of(cmd)), ShapeStatus::Unverified);
    }

    #[test]
    fn noop_globals_do_not_change_the_shape() {
        let cmd = "ffmpeg -hide_banner -loglevel error -i in.mkv -c:v libx264 -crf 23 -preset fast -c:a aac -y out.mp4";
        assert_eq!(classify(&ir_of(cmd)), ShapeStatus::Verified("V1"));
    }

    #[test]
    fn support_table_lists_every_option() {
        let table = support_table_markdown();
        for spec in OPTION_TABLE {
            assert!(
                table.contains(&format!("`{}`", spec.name)),
                "support table is missing {}",
                spec.name
            );
        }
    }

    #[test]
    fn every_verified_shape_names_a_real_golden_test() {
        // `verified` status is EARNED by a golden, never hand-asserted: each
        // manifest entry must point at an existing test fn in
        // tests/cli_goldens.rs (E.4's status model, mechanically enforced).
        let goldens = include_str!("../../../tests/cli_goldens.rs");
        for shape in VERIFIED_SHAPES {
            let needle = format!("fn {}(", shape.golden);
            assert!(
                goldens.contains(&needle),
                "shape {} claims golden `{}` but tests/cli_goldens.rs has no such test",
                shape.id,
                shape.golden
            );
        }
    }

    #[test]
    fn module_docs_embed_the_generated_support_table() {
        // The docs table in mod.rs is generated from the manifest; this
        // pins the two together (E.4: manifest is the single source).
        let source = include_str!("mod.rs");
        for line in support_table_markdown().lines() {
            let doc_line = format!("//! {line}");
            assert!(
                source.contains(&doc_line),
                "mod.rs docs are missing the manifest row: {line}"
            );
        }
    }
}
