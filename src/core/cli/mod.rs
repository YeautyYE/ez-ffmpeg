//! CLI-compat layer (`cli` feature): run or translate a strict, documented
//! subset of ffmpeg command lines in-process.
//!
//! Two capabilities share one typed intermediate representation:
//!
//! - **Run**: [`from_cli_args`] / [`from_cli`] parse a command and build a
//!   ready-to-start [`FfmpegContext`] on the crate's native pipeline.
//! - **Translate**: [`emit_rust_code_from_args`] / [`emit_rust_code`] turn
//!   the same command into a complete, compile-ready builder program to copy
//!   into your codebase.
//!
//! ```no_run
//! use ez_ffmpeg::cli::from_cli_args;
//!
//! # fn main() -> Result<(), Box<dyn std::error::Error>> {
//! from_cli_args(&["-i", "in.mkv", "-c:v", "libx264", "-crf", "23",
//!                 "-preset", "fast", "-c:a", "aac", "-y", "out.mp4"])?
//!     .start()?
//!     .wait()?;
//! # Ok(())
//! # }
//! ```
//!
//! # The contract: classify everything, approximate nothing
//!
//! **Broad CLI compatibility is a non-goal.** The ffmpeg CLI is ~14k lines
//! of option machinery whose semantics move with every release; chasing it
//! wholesale produces the "runs, but subtly different" failures that destroy
//! trust. This layer does the opposite:
//!
//! - every argv token must classify against the versioned compatibility
//!   manifest, or the ENTIRE command is rejected with a token-anchored,
//!   typed diagnostic ([`CliError`]) — nothing is dropped or guessed;
//! - commands whose exact shape is backed by a semantic golden test (stream
//!   identity, codecs, dimensions, durations, playlist topology compared
//!   against the real ffmpeg CLI) are **verified**: they may execute;
//! - commands that parse and match an ENUMERATED unverified manifest entry
//!   are **emit-only**: the code generators label their output "unverified
//!   scaffolding" and [`from_cli_args`] refuses to run them;
//! - commands that parse but match neither manifest table are **rejected
//!   outright** ([`CliError::UnmatchedShape`]) — run and emit alike; no
//!   silent scaffolding class exists;
//! - execution additionally requires a verified runtime profile of the
//!   linked FFmpeg (currently 7.1 only; 8.1 joins once its version-matched
//!   golden lane passes) — anything else fails with the typed
//!   [`CliError::UnverifiedRuntimeProfile`] before any I/O.
//!
//! CLI-initiated pipelines also run with strict AVOption handling: an option
//! no component consumed fails the run (fftools `check_avoptions` parity)
//! instead of the default builder path's warning.
//!
//! # The manifest (generated; revision-pinned by an exact-equality test)
//!
//! <!-- manifest:begin -->
//! Manifest revision 3; dialect: ffmpeg 7.1 command line.
//!
//! | option | scope | selector | repeat | notes | maps to |
//! |---|---|---|---|---|---|
//! | `-y` | global | run | repeatable | flag | mandatory overwrite gate |
//! | `-hide_banner` | global | no-op | repeatable | accepted, no in-process effect | none (documented no-op) |
//! | `-nostdin` | global | no-op | repeatable | accepted, no in-process effect | none (documented no-op) |
//! | `-stats` | global | no-op | repeatable | accepted, no in-process effect | none (documented no-op) |
//! | `-nostats` | global | no-op | repeatable | accepted, no in-process effect | none (documented no-op) |
//! | `-loglevel` | global | no-op | repeatable | accepted, no in-process effect | none (documented no-op) |
//! | `-v` | global | no-op | repeatable | accepted, no in-process effect | none (documented no-op) |
//! | `-ss` | input or output (position-scoped) | container | once | takes a value | Input::set_start_time_us / Output::set_start_time_us |
//! | `-t` | input or output (position-scoped) | container | once | takes a value | Input::set_recording_time_us / Output::set_recording_time_us |
//! | `-to` | input or output (position-scoped) | container | once | takes a value | Input::set_stop_time_us / Output::set_stop_time_us |
//! | `-f` | input or output (position-scoped) | container | once | takes a value | Input::set_format / Output::set_format |
//! | `-vn` | output | video | repeatable | flag | Output::disable_video |
//! | `-an` | output | audio | repeatable | flag | Output::disable_audio |
//! | `-c:v` | output | video | once | takes a value | Output::set_video_codec |
//! | `-c:a` | output | audio | once | takes a value | Output::set_audio_codec |
//! | `-b:v` | output | video | once | takes a value | Output::set_video_bitrate |
//! | `-b:a` | output | audio | once | takes a value | Output::set_audio_bitrate |
//! | `-crf` | output | video | once | takes a value | Output::set_video_codec_opt("crf", …), libx264 only |
//! | `-preset` | output | video | once | takes a value | Output::set_video_codec_opt("preset", …), libx264 only |
//! | `-pix_fmt` | output | video | once | takes a value | Output::set_pix_fmt |
//! | `-ar` | output | audio | once | takes a value | Output::set_audio_sample_rate |
//! | `-ac` | output | audio | once | takes a value | Output::set_audio_channels |
//! | `-frames:v` | output | video | once | takes a value | Output::set_max_video_frames(1) |
//! | `-vf` | output | video | once | takes a value | Output::set_video_filter |
//! | `-map` | output | stream map | accumulates | takes a value | Output::add_stream_map / add_stream_map_with_copy |
//! | `-movflags` | output | container | once | takes a value | Output::set_format_opt("movflags", "+faststart") |
//! | `-hls_time` | output | container | once | takes a value | Output::set_format_opt("hls_time", …) |
//! | `-hls_playlist_type` | output | container | once | takes a value | Output::set_format_opt("hls_playlist_type", "vod") |
//! | `-hls_list_size` | output | container | once | takes a value | Output::set_format_opt("hls_list_size", "0") |
//! | `-hls_segment_filename` | output | container | once | takes a value | Output::set_format_opt("hls_segment_filename", …) |
//!
//! Verified shapes (may execute; each is backed by a semantic golden and a
//! compile-pinned emitted example):
//!
//! | id | shape | container |
//! |---|---|---|
//! | V1 | H.264/AAC transcode (crf + preset) | `.mp4` |
//! | V2 | re-encoded clip (input -ss, output -t) | `.mp4` |
//! | V3 | audio extract (-vn, AAC) | `.m4a` |
//! | V4 | single-frame thumbnail (input -ss, -an, mjpeg) | `.jpg` |
//! | V5 | scaled H.264/AAC transcode (-vf scale) | `.mp4` |
//! | V6 | single-rendition VOD HLS | `.m3u8` |
//!
//! Unverified entries (emit-only: code generation with a scaffolding banner,
//! execution refused):
//!
//! | id | shape |
//! |---|---|
//! | U1 | input-side trim (-ss + -t before -i) |
//! | U2 | faststart remux (explicit per-media copy) |
//! | U3 | scale with audio copy |
//! | U4 | thumbnail recipe shape (-vf + -frames:v) |
//! | U5 | audio extract via stream copy |
//! | U6 | PCM / resampled audio extract |
//! | U7 | partial HLS option set |
//! | U8 | mapped transcode (basic index maps) |
//! | U9 | mapped audio copy |
//! | U10 | cross-scope trim pair (input -t, output -to) |
//! | U11 | output-side seek transcode |
//! | U12 | bitrate-driven transcode |
//! | U13 | pixel-format transcode |
//! | U14 | explicit input format (e.g. lavfi) |
//! | U15 | container-defaults remux |
//! | U16 | video-codec-only transcode |
//! | U17 | transcode without preset |
//! | U18 | transcode with audio bitrate |
//! | U19 | audio+video codec selection only |
//! | U20 | transcode shape with unpinned values/container |
//! | U21 | clip shape with unpinned values/container |
//! | U22 | audio-extract shape with unpinned values/container |
//! | U23 | thumbnail shape with unpinned values/container |
//! | U24 | scaled-transcode shape with unpinned values/container |
//! | U25 | VOD HLS shape with unpinned values/container |
//!
//! Everything else — including parseable option sets outside both tables — is
//! rejected with a typed diagnostic.
//! <!-- manifest:end -->
//!
//! # String form
//!
//! [`from_cli`] / [`emit_rust_code`] accept one string and apply POSIX word
//! splitting ONLY: single/double quotes, backslash escapes,
//! backslash-newline continuation, plus caret-newline continuation as an
//! explicitly named cmd.exe compatibility extension. No variables, globs,
//! tilde, pipes, redirects, comments or command lists — those tokens are
//! rejected, never emulated (unquoted `*`, `?` and `[` included: quote or
//! escape a literal). Windows `cmd.exe` quoting is NOT reproduced:
//! commands relying on `CommandLineToArgvW` backslash rules must use the
//! argv form.

mod emit;
mod error;
mod ir;
mod lower;
mod manifest;
mod parse;
mod table;
mod tokenize;

#[cfg(all(test, not(docsrs)))]
mod corpus_tests;
#[cfg(all(test, not(docsrs)))]
mod golden_tests;
#[cfg(all(test, not(docsrs)))]
mod strict_tests;

pub use error::{CliError, CliScope};

use crate::core::context::ffmpeg_context::FfmpegContext;
use manifest::{ShapeStatus, VERIFIED_PROFILES};

/// Builds a ready-to-start [`FfmpegContext`] from ffmpeg-style argv tokens.
///
/// This is the primary form: argv has zero quoting ambiguity (the same shape
/// `ffmpeg.wasm` chose). A leading `ffmpeg` / `ffmpeg.exe` token is
/// tolerated and stripped.
///
/// The command must classify completely against the compatibility manifest
/// AND match a verified (golden-backed) shape, and the linked FFmpeg must be
/// a verified runtime profile — otherwise a typed [`CliError`] is returned
/// before any I/O. See the [module docs](self) for the exact surface.
pub fn from_cli_args<S: AsRef<str>>(args: &[S]) -> Result<FfmpegContext, CliError> {
    let args: Vec<String> = args.iter().map(|s| s.as_ref().to_string()).collect();
    let args = tokenize::strip_program_token(args);
    run_from_tokens(&args)
}

/// Single-string convenience wrapper over [`from_cli_args`].
///
/// Applies the documented POSIX word-splitting contract (see the
/// [module docs](self)); everything after tokenization is identical to the
/// argv form.
pub fn from_cli(command: &str) -> Result<FfmpegContext, CliError> {
    let args = tokenize::tokenize(command)?;
    run_from_tokens(&args)
}

/// Translates ffmpeg-style argv tokens into a complete Rust program using
/// the ez-ffmpeg builder API.
///
/// Emission works for verified shapes and for the manifest's enumerated
/// unverified entries — including shapes that are
/// not verified for execution, whose output is prominently labeled
/// "unverified scaffolding". The generated code and [`from_cli_args`]
/// consume the same lowered plan, so what you read is what would run.
pub fn emit_rust_code_from_args<S: AsRef<str>>(args: &[S]) -> Result<String, CliError> {
    let args: Vec<String> = args.iter().map(|s| s.as_ref().to_string()).collect();
    let args = tokenize::strip_program_token(args);
    emit_from_tokens(&args)
}

/// Single-string convenience wrapper over [`emit_rust_code_from_args`],
/// using the same POSIX word-splitting contract as [`from_cli`].
pub fn emit_rust_code(command: &str) -> Result<String, CliError> {
    let args = tokenize::tokenize(command)?;
    emit_from_tokens(&args)
}

fn run_from_tokens(args: &[String]) -> Result<FfmpegContext, CliError> {
    let ir = parse::parse(args)?;
    match manifest::classify(&ir) {
        ShapeStatus::Verified(_) => {}
        ShapeStatus::Unverified(_) => {
            return Err(CliError::NotVerified {
                parsed_options: ir.fingerprint().iter().map(|s| s.to_string()).collect(),
            });
        }
        ShapeStatus::Unmatched => {
            return Err(CliError::UnmatchedShape {
                parsed_options: ir.fingerprint().iter().map(|s| s.to_string()).collect(),
            });
        }
    }
    check_runtime_profile()?;
    // The hard simple-filter prerequisite (exactly one video stream under a
    // -vf command) is enforced INSIDE context binding, on the demuxer
    // instances the pipeline executes with — one opening, no TOCTOU window,
    // no second remote fetch. The lowering arms it; here the typed core
    // error is translated to the public diagnostic.
    lower::lower(&ir).into_context().map_err(|err| match err {
        crate::error::Error::AmbiguousVideoSource { video_streams } => {
            CliError::AmbiguousFilterSource { video_streams }
        }
        other => CliError::Build(other),
    })
}

fn emit_from_tokens(args: &[String]) -> Result<String, CliError> {
    let ir = parse::parse(args)?;
    let status = manifest::classify(&ir);
    if status == ShapeStatus::Unmatched {
        // No silent scaffolding class: unenumerated shapes get nothing.
        return Err(CliError::UnmatchedShape {
            parsed_options: ir.fingerprint().iter().map(|s| s.to_string()).collect(),
        });
    }
    Ok(emit::emit(&lower::lower(&ir), args, &status))
}

/// Runtime-profile gate: in-process execution is allowed only on linked
/// FFmpeg builds whose libavcodec/libavformat major.minor pairs match a
/// verified profile. Purely a version check — it runs before any I/O.
/// Whether the LINKED libavcodec/libavformat pair matches a verified runtime
/// profile. Tests use this to stay honest on non-verified lanes: on a linked
/// 8.x build the correct expectation is the typed `UnverifiedRuntimeProfile`
/// failure, not runtime success.
pub(crate) fn linked_profile_verified() -> bool {
    let avcodec = unsafe { ffmpeg_sys_next::avcodec_version() };
    let avformat = unsafe { ffmpeg_sys_next::avformat_version() };
    let pair = |v: u32| (v >> 16, (v >> 8) & 0xff);
    let (ac_major, ac_minor) = pair(avcodec);
    let (af_major, af_minor) = pair(avformat);
    VERIFIED_PROFILES.iter().any(|profile| {
        (ac_major, ac_minor) == profile.avcodec && (af_major, af_minor) == profile.avformat
    })
}

fn check_runtime_profile() -> Result<(), CliError> {
    let avcodec = unsafe { ffmpeg_sys_next::avcodec_version() };
    let avformat = unsafe { ffmpeg_sys_next::avformat_version() };
    let pair = |v: u32| (v >> 16, (v >> 8) & 0xff);
    let (ac_major, ac_minor) = pair(avcodec);
    let (af_major, af_minor) = pair(avformat);

    if linked_profile_verified() {
        return Ok(());
    }
    Err(CliError::UnverifiedRuntimeProfile {
        linked_avcodec: format!("{ac_major}.{ac_minor}"),
        linked_avformat: format!("{af_major}.{af_minor}"),
        verified: VERIFIED_PROFILES
            .iter()
            .map(|profile| {
                format!(
                    "{} (avcodec {}.{}, avformat {}.{})",
                    profile.name,
                    profile.avcodec.0,
                    profile.avcodec.1,
                    profile.avformat.0,
                    profile.avformat.1
                )
            })
            .collect::<Vec<_>>()
            .join(", "),
    })
}

#[cfg(test)]
mod facade_tests {
    use super::*;

    #[test]
    fn runtime_profile_gate_matches_the_linked_build() {
        // Profile-aware: on a verified line (7.1) the gate passes; on any
        // other linked line the typed failure IS the correct behavior, and
        // asserting it keeps non-verified CI lanes green and honest.
        match check_runtime_profile() {
            Ok(()) => assert!(
                linked_profile_verified(),
                "gate passed on a non-verified linked profile"
            ),
            Err(CliError::UnverifiedRuntimeProfile { .. }) => assert!(
                !linked_profile_verified(),
                "gate rejected a verified linked profile"
            ),
            Err(other) => panic!("unexpected gate error: {other}"),
        }
    }

    /// Fail-closed pin with LITERAL version pairs. `VERIFIED_PROFILES` is
    /// deliberately not consulted: the test above derives its expectation
    /// from the same table the gate reads, so a table edit that silently
    /// widened the gate (say, adding an unproven 8.x row) would satisfy
    /// both sides. FFmpeg 7.1 is the only verified line — libavcodec 61.19
    /// / libavformat 61.7 — and those numbers are hardcoded here: on any
    /// other linked pair, `from_cli_args` on a verified-shape command must
    /// return the typed profile refusal before any I/O.
    #[test]
    fn non_71_linked_pair_fails_closed_by_literal_version() {
        let pair = |v: u32| (v >> 16, (v >> 8) & 0xff);
        let linked_is_71 = pair(unsafe { ffmpeg_sys_next::avcodec_version() }) == (61, 19)
            && pair(unsafe { ffmpeg_sys_next::avformat_version() }) == (61, 7);
        let args = [
            "-i",
            "no_such_fixture.mkv",
            "-c:v",
            "libx264",
            "-crf",
            "23",
            "-preset",
            "fast",
            "-c:a",
            "aac",
            "-y",
            "out.mp4",
        ];
        match from_cli_args(&args) {
            Err(CliError::UnverifiedRuntimeProfile { .. }) => assert!(
                !linked_is_71,
                "profile refusal on the literal 7.1 pair (61.19/61.7)"
            ),
            Ok(_) => assert!(
                linked_is_71,
                "a non-7.1 linked pair must fail closed with UnverifiedRuntimeProfile"
            ),
            Err(other) => assert!(
                linked_is_71,
                "a non-7.1 linked pair must fail closed with UnverifiedRuntimeProfile, got: {other}"
            ),
        }
    }

    #[test]
    fn unverified_shape_is_refused_at_run_but_emitted() {
        let args = ["-i", "in.mp4", "-c:v", "mpeg4", "-y", "out.avi"];
        let err = from_cli_args(&args).map(|_| ()).unwrap_err();
        assert!(
            matches!(&err, CliError::NotVerified { parsed_options }
                if parsed_options.iter().any(|o| o == "out:-c:v")),
            "unexpected error: {err}"
        );
        let code = emit_rust_code_from_args(&args).unwrap();
        assert!(code.contains("UNVERIFIED SCAFFOLDING"));
    }

    #[test]
    fn argv_form_tolerates_a_program_token() {
        let err = from_cli_args(&["ffmpeg", "-i", "in.mp4", "-c:v", "mpeg4", "-y", "o.avi"])
            .map(|_| ())
            .unwrap_err();
        // Stripped "ffmpeg" leaves a parseable (unverified) command, proving
        // the token was not mistaken for an output path.
        assert!(
            matches!(err, CliError::NotVerified { .. }),
            "unexpected error: {err}"
        );
    }

    #[test]
    fn run_and_emit_reject_identically() {
        let args = ["-i", "in.mp4", "-fps_mode", "cfr", "-y", "out.mp4"];
        let run_err = from_cli_args(&args).map(|_| ()).unwrap_err();
        let emit_err = emit_rust_code_from_args(&args).unwrap_err();
        assert_eq!(run_err.to_string(), emit_err.to_string());
    }

    #[test]
    fn loglevel_grammar_is_wired_to_the_facade() {
        // End-to-end pin of the -loglevel value-grammar wiring: the
        // flag-prefix grammar must be reachable through from_cli itself,
        // anchored at the VALUE token — disconnecting -loglevel from its
        // rule would fail here even if the rule's own unit tests stayed
        // green.
        let err = from_cli(
            "ffmpeg -hide_banner -loglevel banana+error -i in.mkv -c:v libx264 -crf 23 -preset fast -c:a aac -y out.mp4",
        )
        .map(|_| ())
        .unwrap_err();
        match &err {
            CliError::UnsupportedValue {
                option,
                value,
                index,
                ..
            } => {
                assert_eq!(option, "-loglevel");
                assert_eq!(value, "banana+error");
                // tokens: 0=-hide_banner 1=-loglevel 2=banana+error …
                assert_eq!(*index, 2, "the VALUE token is the anchor");
            }
            other => panic!("expected UnsupportedValue for banana+error, got {other}"),
        }
    }

    #[test]
    fn every_rejection_carries_the_cta() {
        for args in [
            &["-i", "in.mp4", "-fps_mode", "cfr", "-y", "out.mp4"][..],
            &["-i", "in.mp4", "out.mp4"][..],
            &["-i", "in.mp4", "-c:v", "mpeg4", "-y", "out.avi"][..],
        ] {
            let err = from_cli_args(args).map(|_| ()).unwrap_err();
            assert!(
                err.to_string().contains("open an issue"),
                "rejection lost its CTA: {err}"
            );
        }
    }
}
