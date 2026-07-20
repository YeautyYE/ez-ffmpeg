//! The compatibility manifest: the single source of truth for what the
//! CLI-compat layer accepts, what may run, what may only be emitted, and
//! what the documentation says about all of it.
//!
//! Classification is three-tier and EXPLICIT (E.4's status model):
//! - [`VERIFIED_SHAPES`]: golden-backed shapes; the only tier that may
//!   execute in-process. Each entry carries its canonical argv — the golden
//!   tests iterate THESE entries (path substitution only), so a shape cannot
//!   claim verified status without its golden actually consuming it.
//! - [`UNVERIFIED_SHAPES`]: enumerated emit-only entries. The code
//!   generators label their output "unverified scaffolding"; execution is
//!   refused. A parseable command whose fingerprint is NOT enumerated here
//!   does not silently become scaffolding —
//! - unmatched: typed rejection ([`super::CliError::UnmatchedShape`]). No
//!   silent classes exist.
//!
//! The module documentation's support tables are generated from these tables
//! and pinned by an exact-equality test — containment is not enough, the
//! docs must BE the manifest's rendering.

use super::ir::CliIr;
#[cfg(test)]
use super::table::{Arity, Repeat, ScopeRule, OPTION_TABLE};

/// Manifest revision. Bump on ANY change to the accept surface, the shape
/// tables, or a rejection reason. Emitted code headers carry this value.
pub(crate) const MANIFEST_REVISION: u32 = 2;

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

/// FFmpeg 7.1 ships libavcodec 61.19 / libavformat 61.7 (its `version.h` /
/// `version_major.h`).
///
/// FFmpeg 8.1 (libavcodec 62.28 / libavformat 62.12) is NOT listed: a
/// profile earns its row only when a version-matched golden lane has
/// actually executed the semantic suite against that line's CLI, and no 8.1
/// lane has run yet. Until then, runtime execution on a linked 8.x build
/// fails closed as an unverified profile.
pub(crate) const VERIFIED_PROFILES: &[RuntimeProfile] = &[RuntimeProfile {
    name: "FFmpeg 7.1",
    avcodec: (61, 19),
    avformat: (61, 7),
}];

/// One golden-backed shape: a fixed scope-qualified option set (the
/// fingerprint), pinned values where the golden pinned them, the output
/// extension the golden produced, and the canonical argv the golden tests
/// and the compile-pinned emitted examples both consume. Parameter
/// variation inside a shape is values/paths only.
pub(crate) struct VerifiedShape {
    pub(crate) id: &'static str,
    pub(crate) summary: &'static str,
    /// Sorted scope-qualified option keys, exactly as
    /// [`CliIr::fingerprint`] produces them.
    pub(crate) fingerprint: &'static [&'static str],
    /// Golden test that earns the `verified` status (`golden_tests`).
    pub(crate) golden: &'static str,
    /// Extra pinned-value predicate beyond the fingerprint.
    pub(crate) pins: fn(&CliIr) -> bool,
    /// Output extension the golden pinned (muxer identity is part of the
    /// shape).
    pub(crate) output_ext: &'static str,
    /// The canonical command (program name stripped, relative paths). The
    /// golden runner substitutes paths; the checked-in emitted example is
    /// exactly `emit(canonical_argv)`.
    pub(crate) canonical_argv: &'static [&'static str],
    /// Basename of the compile-pinned emitted example under `examples/`.
    pub(crate) emitted_example: &'static str,
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
        canonical_argv: &[
            "-i", "in.mkv", "-c:v", "libx264", "-crf", "23", "-preset", "fast", "-c:a", "aac",
            "-y", "out.mp4",
        ],
        emitted_example: "cli_emitted_transcode",
    },
    VerifiedShape {
        id: "V2",
        summary: "re-encoded clip (input -ss, output -t)",
        fingerprint: &["in:-ss", "out:-c:a", "out:-c:v", "out:-crf", "out:-t"],
        golden: "golden_v2_clip",
        pins: pins_v2,
        output_ext: "mp4",
        canonical_argv: &[
            "-ss", "10", "-i", "in.mp4", "-t", "20", "-c:v", "libx264", "-crf", "23", "-c:a",
            "aac", "-y", "clip.mp4",
        ],
        emitted_example: "cli_emitted_clip",
    },
    VerifiedShape {
        id: "V3",
        summary: "audio extract (-vn, AAC)",
        fingerprint: &["out:-b:a", "out:-c:a", "out:-vn"],
        golden: "golden_v3_audio_extract",
        pins: pins_v3,
        output_ext: "m4a",
        canonical_argv: &["-i", "in.mp4", "-vn", "-c:a", "aac", "-b:a", "192k", "-y", "out.m4a"],
        emitted_example: "cli_emitted_audio_extract",
    },
    VerifiedShape {
        id: "V4",
        summary: "single-frame thumbnail (input -ss, -an, mjpeg)",
        fingerprint: &["in:-ss", "out:-an", "out:-c:v", "out:-frames:v"],
        golden: "golden_v4_thumbnail",
        pins: pins_v4,
        output_ext: "jpg",
        canonical_argv: &[
            "-ss", "5", "-i", "in.mp4", "-an", "-c:v", "mjpeg", "-frames:v", "1", "-y",
            "thumb.jpg",
        ],
        emitted_example: "cli_emitted_thumbnail",
    },
    VerifiedShape {
        id: "V5",
        summary: "scaled H.264/AAC transcode (-vf scale)",
        fingerprint: &["out:-c:a", "out:-c:v", "out:-crf", "out:-preset", "out:-vf"],
        golden: "golden_v5_scale",
        pins: pins_v1,
        output_ext: "mp4",
        canonical_argv: &[
            "-i", "in.mp4", "-vf", "scale=1280:-2", "-c:v", "libx264", "-crf", "23", "-preset",
            "fast", "-c:a", "aac", "-y", "scaled.mp4",
        ],
        emitted_example: "cli_emitted_scale",
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
        canonical_argv: &[
            "-i", "in.mp4", "-c:v", "libx264", "-crf", "23", "-c:a", "aac", "-f", "hls",
            "-hls_time", "6", "-hls_playlist_type", "vod", "-hls_list_size", "0",
            "-hls_segment_filename", "seg_%03d.ts", "-y", "out.m3u8",
        ],
        emitted_example: "cli_emitted_hls",
    },
];

/// One enumerated emit-only entry: parses cleanly, no golden covers it, code
/// generation is allowed WITH the scaffolding banner. The fingerprint is the
/// entry's identity; values and paths vary freely inside it.
pub(crate) struct UnverifiedShape {
    pub(crate) id: &'static str,
    pub(crate) summary: &'static str,
    pub(crate) fingerprint: &'static [&'static str],
}

/// The documented emit-only surface. An entry here is a deliberate record —
/// "this shape is worth scaffolding, not yet worth verifying" — sourced from
/// the classification corpus. Anything outside both tables is rejected.
pub(crate) const UNVERIFIED_SHAPES: &[UnverifiedShape] = &[
    UnverifiedShape {
        id: "U1",
        summary: "input-side trim (-ss + -t before -i)",
        fingerprint: &["in:-ss", "in:-t"],
    },
    UnverifiedShape {
        id: "U2",
        summary: "faststart remux (explicit per-media copy)",
        fingerprint: &["out:-c:a copy", "out:-c:v copy", "out:-movflags"],
    },
    UnverifiedShape {
        id: "U3",
        summary: "scale with audio copy",
        fingerprint: &["out:-c:a copy", "out:-vf"],
    },
    UnverifiedShape {
        id: "U4",
        summary: "thumbnail recipe shape (-vf + -frames:v)",
        fingerprint: &["in:-ss", "out:-frames:v", "out:-vf"],
    },
    UnverifiedShape {
        id: "U5",
        summary: "audio extract via stream copy",
        fingerprint: &["out:-c:a copy", "out:-vn"],
    },
    UnverifiedShape {
        id: "U6",
        summary: "PCM / resampled audio extract",
        fingerprint: &["out:-ac", "out:-ar", "out:-c:a"],
    },
    UnverifiedShape {
        id: "U7",
        summary: "partial HLS option set",
        fingerprint: &[
            "out:-c:a",
            "out:-c:v",
            "out:-f",
            "out:-hls_playlist_type",
            "out:-hls_time",
        ],
    },
    UnverifiedShape {
        id: "U8",
        summary: "mapped transcode (basic index maps)",
        fingerprint: &["out:-c:a", "out:-c:v", "out:-map"],
    },
    UnverifiedShape {
        id: "U9",
        summary: "mapped audio copy",
        fingerprint: &["out:-c:a copy", "out:-map"],
    },
    UnverifiedShape {
        id: "U10",
        summary: "cross-scope trim pair (input -t, output -to)",
        fingerprint: &["in:-t", "out:-to"],
    },
    UnverifiedShape {
        id: "U11",
        summary: "output-side seek transcode",
        fingerprint: &[
            "out:-c:a",
            "out:-c:v",
            "out:-crf",
            "out:-preset",
            "out:-ss",
            "out:-t",
        ],
    },
    UnverifiedShape {
        id: "U12",
        summary: "bitrate-driven transcode",
        fingerprint: &["out:-b:a", "out:-b:v", "out:-c:a", "out:-c:v"],
    },
    UnverifiedShape {
        id: "U13",
        summary: "pixel-format transcode",
        fingerprint: &["out:-c:v", "out:-pix_fmt"],
    },
    UnverifiedShape {
        id: "U14",
        summary: "explicit input format (e.g. lavfi)",
        fingerprint: &["in:-f", "out:-c:v"],
    },
    UnverifiedShape {
        id: "U15",
        summary: "container-defaults remux",
        fingerprint: &[],
    },
    UnverifiedShape {
        id: "U16",
        summary: "video-codec-only transcode",
        fingerprint: &["out:-c:v"],
    },
    UnverifiedShape {
        id: "U17",
        summary: "transcode without preset",
        fingerprint: &["out:-c:a", "out:-c:v", "out:-crf"],
    },
    UnverifiedShape {
        id: "U18",
        summary: "transcode with audio bitrate",
        fingerprint: &["out:-b:a", "out:-c:a", "out:-c:v", "out:-crf", "out:-preset"],
    },
    UnverifiedShape {
        id: "U19",
        summary: "audio+video codec selection only",
        fingerprint: &["out:-c:a", "out:-c:v"],
    },
    UnverifiedShape {
        id: "U20",
        summary: "transcode shape with unpinned values/container",
        fingerprint: &["out:-c:a", "out:-c:v", "out:-crf", "out:-preset"],
    },
    UnverifiedShape {
        id: "U21",
        summary: "clip shape with unpinned values/container",
        fingerprint: &["in:-ss", "out:-c:a", "out:-c:v", "out:-crf", "out:-t"],
    },
    UnverifiedShape {
        id: "U22",
        summary: "audio-extract shape with unpinned values/container",
        fingerprint: &["out:-b:a", "out:-c:a", "out:-vn"],
    },
    UnverifiedShape {
        id: "U23",
        summary: "thumbnail shape with unpinned values/container",
        fingerprint: &["in:-ss", "out:-an", "out:-c:v", "out:-frames:v"],
    },
    UnverifiedShape {
        id: "U24",
        summary: "scaled-transcode shape with unpinned values/container",
        fingerprint: &["out:-c:a", "out:-c:v", "out:-crf", "out:-preset", "out:-vf"],
    },
    UnverifiedShape {
        id: "U25",
        summary: "VOD HLS shape with unpinned values/container",
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
    },
];

/// Classification verdict for a parsed command.
#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) enum ShapeStatus {
    /// Option set, pinned values and output extension match a golden-backed
    /// shape: execution is allowed.
    Verified(&'static str),
    /// Matches an enumerated emit-only entry (by id): scaffolding
    /// generation is allowed, execution is refused.
    Unverified(&'static str),
    /// Parses, but matches neither table: rejected outright.
    Unmatched,
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
    for entry in UNVERIFIED_SHAPES {
        if fingerprint == entry.fingerprint {
            return ShapeStatus::Unverified(entry.id);
        }
    }
    ShapeStatus::Unmatched
}

fn output_extension(url: &str) -> Option<&str> {
    let name = url.rsplit(['/', '\\']).next().unwrap_or(url);
    let (stem, ext) = name.rsplit_once('.')?;
    if stem.is_empty() {
        return None;
    }
    Some(ext)
}

/// Looks up a verified shape by id (emit stamps the shape's summary and
/// backing golden into generated headers; the golden runner iterates the
/// table directly).
pub(crate) fn shape(id: &str) -> Option<&'static VerifiedShape> {
    VERIFIED_SHAPES.iter().find(|shape| shape.id == id)
}

/// Looks up an unverified entry by id (emit cites it in the scaffolding
/// banner).
pub(crate) fn unverified_entry(id: &str) -> Option<&'static UnverifiedShape> {
    UNVERIFIED_SHAPES.iter().find(|entry| entry.id == id)
}

/// Renders the generated documentation block embedded in the module docs
/// between the `manifest:begin` / `manifest:end` markers. An exact-equality
/// test pins the docs to this rendering — the docs ARE the manifest.
#[cfg(test)]
pub(crate) fn manifest_docs_markdown() -> String {
    let mut out = String::new();
    out.push_str(&format!(
        "Manifest revision {MANIFEST_REVISION}; dialect: {DIALECT}.\n\n"
    ));
    out.push_str("| option | scope | repeat | notes | maps to |\n|---|---|---|---|---|\n");
    for spec in OPTION_TABLE {
        let scope = match spec.scope {
            ScopeRule::Global => "global",
            ScopeRule::OutputOnly => "output",
            ScopeRule::InputOrOutput => "input or output (position-scoped)",
        };
        let repeat = match spec.repeat {
            Repeat::Once => "once",
            Repeat::Accumulate => "accumulates",
            Repeat::Free => "repeatable",
        };
        let notes = if spec.noop {
            "accepted, no in-process effect"
        } else if spec.arity == Arity::Flag {
            "flag"
        } else {
            "takes a value"
        };
        out.push_str(&format!(
            "| `{}` | {} | {} | {} | {} |\n",
            spec.name, scope, repeat, notes, spec.sink
        ));
    }
    out.push_str(
        "\nVerified shapes (may execute; each is backed by a semantic golden and a\ncompile-pinned emitted example):\n\n",
    );
    out.push_str("| id | shape | container |\n|---|---|---|\n");
    for shape in VERIFIED_SHAPES {
        out.push_str(&format!(
            "| {} | {} | `.{}` |\n",
            shape.id, shape.summary, shape.output_ext
        ));
    }
    out.push_str(
        "\nUnverified entries (emit-only: code generation with a scaffolding banner,\nexecution refused):\n\n",
    );
    out.push_str("| id | shape |\n|---|---|\n");
    for entry in UNVERIFIED_SHAPES {
        out.push_str(&format!("| {} | {} |\n", entry.id, entry.summary));
    }
    out.push_str(
        "\nEverything else — including parseable option sets outside both tables — is\nrejected with a typed diagnostic.\n",
    );
    out
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
        for (id, fingerprint) in VERIFIED_SHAPES
            .iter()
            .map(|s| (s.id, s.fingerprint))
            .chain(UNVERIFIED_SHAPES.iter().map(|s| (s.id, s.fingerprint)))
        {
            let mut sorted = fingerprint.to_vec();
            sorted.sort_unstable();
            assert_eq!(fingerprint, &sorted[..], "shape {id} is not sorted");
        }
    }

    #[test]
    fn shape_ids_are_unique() {
        let mut ids: Vec<&str> = VERIFIED_SHAPES
            .iter()
            .map(|s| s.id)
            .chain(UNVERIFIED_SHAPES.iter().map(|s| s.id))
            .collect();
        let total = ids.len();
        ids.sort_unstable();
        ids.dedup();
        assert_eq!(ids.len(), total, "duplicate shape ids in the manifest");
    }

    #[test]
    fn canonical_argv_is_the_typed_golden_linkage() {
        // Every verified shape's OWN canonical command must classify as that
        // shape — the golden runner and the emitted examples consume exactly
        // this argv, so a drifted fingerprint, pin or extension breaks here
        // instead of silently unverifying the golden.
        for shape in VERIFIED_SHAPES {
            let args: Vec<String> = shape.canonical_argv.iter().map(|s| s.to_string()).collect();
            let ir = parse(&args)
                .unwrap_or_else(|e| panic!("canonical argv of {} must parse: {e}", shape.id));
            assert_eq!(
                classify(&ir),
                ShapeStatus::Verified(shape.id),
                "canonical argv of {} must classify as itself",
                shape.id
            );
        }
    }

    #[test]
    fn value_variation_keeps_the_shape() {
        let cmd = "ffmpeg -i '素材/其他 输入.mkv' -c:v libx264 -crf 18 -preset veryslow -c:a aac -y '输出/成品 v2.mp4'";
        assert_eq!(classify(&ir_of(cmd)), ShapeStatus::Verified("V1"));
    }

    #[test]
    fn option_set_variation_is_an_enumerated_unverified_entry() {
        // V1 minus -preset parses but is NOT the golden-backed shape; it is
        // the documented U17 entry.
        let cmd = "ffmpeg -i in.mkv -c:v libx264 -crf 23 -c:a aac -y out.mp4";
        assert_eq!(classify(&ir_of(cmd)), ShapeStatus::Unverified("U17"));
        // V1 plus -b:a is the documented U18 entry.
        let cmd =
            "ffmpeg -i in.mkv -c:v libx264 -crf 23 -preset fast -c:a aac -b:a 192k -y out.mp4";
        assert_eq!(classify(&ir_of(cmd)), ShapeStatus::Unverified("U18"));
    }

    #[test]
    fn pinned_codec_variation_is_unverified() {
        let cmd = "ffmpeg -i in.mkv -c:v mpeg4 -c:a aac -y out.mp4";
        assert_eq!(classify(&ir_of(cmd)), ShapeStatus::Unverified("U19"));
    }

    #[test]
    fn container_variation_falls_to_the_unpinned_twin() {
        // The golden pinned .mp4; .mkv changes the muxer, hence the verified
        // shape no longer applies — the unpinned twin U20 catches it.
        let cmd = "ffmpeg -i in.mkv -c:v libx264 -crf 23 -preset fast -c:a aac -y out.mkv";
        assert_eq!(classify(&ir_of(cmd)), ShapeStatus::Unverified("U20"));
    }

    #[test]
    fn unenumerated_parses_are_unmatched() {
        for cmd in [
            "ffmpeg -i in.mp4 -ac 1 -y out.mp4",
            "ffmpeg -i in.mp4 -f mp4 -y out2.mp4",
            "ffmpeg -i in.mp4 -pix_fmt yuv420p -ar 48000 -y out.mp4",
        ] {
            assert_eq!(
                classify(&ir_of(cmd)),
                ShapeStatus::Unmatched,
                "{cmd} must not silently become scaffolding"
            );
        }
    }

    #[test]
    fn noop_globals_do_not_change_the_shape() {
        let cmd = "ffmpeg -hide_banner -loglevel error -i in.mkv -c:v libx264 -crf 23 -preset fast -c:a aac -y out.mp4";
        assert_eq!(classify(&ir_of(cmd)), ShapeStatus::Verified("V1"));
    }

    #[test]
    fn module_docs_equal_the_generated_manifest_block() {
        // Exact equality, not containment: the docs between the markers must
        // BE the manifest rendering, byte for byte.
        let source = include_str!("mod.rs");
        let begin = "//! <!-- manifest:begin -->\n";
        let end = "//! <!-- manifest:end -->";
        let start = source
            .find(begin)
            .expect("mod.rs docs are missing the manifest:begin marker")
            + begin.len();
        let stop = source
            .find(end)
            .expect("mod.rs docs are missing the manifest:end marker");
        let embedded: String = source[start..stop]
            .lines()
            .map(|line| {
                let line = line.strip_prefix("//!").unwrap_or(line);
                let line = line.strip_prefix(" ").unwrap_or(line);
                format!("{line}\n")
            })
            .collect();
        assert_eq!(
            embedded,
            manifest_docs_markdown(),
            "mod.rs docs drifted from the generated manifest block; regenerate the section \
             between the markers"
        );
    }
}
