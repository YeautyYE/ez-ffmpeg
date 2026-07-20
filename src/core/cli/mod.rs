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
//! - commands that parse but match no verified shape are **emit-only**: the
//!   code generators label their output "unverified scaffolding" and
//!   [`from_cli_args`] refuses to run them;
//! - execution requires a verified runtime profile of the linked FFmpeg
//!   (currently 7.1 and 8.1); anything else fails before any I/O.
//!
//! CLI-initiated pipelines also run with strict AVOption handling: an option
//! no component consumed fails the run (fftools `check_avoptions` parity)
//! instead of the default builder path's warning.
//!
//! # Accepted options (generated from the manifest, revision 1)
//!
//! | option | scope | notes |
//! |---|---|---|
//! | `-y` | global | flag |
//! | `-hide_banner` | global | accepted, no in-process effect |
//! | `-nostdin` | global | accepted, no in-process effect |
//! | `-stats` | global | accepted, no in-process effect |
//! | `-nostats` | global | accepted, no in-process effect |
//! | `-loglevel` | global | accepted, no in-process effect |
//! | `-v` | global | accepted, no in-process effect |
//! | `-ss` | input or output (position-scoped) | takes a value |
//! | `-t` | input or output (position-scoped) | takes a value |
//! | `-to` | input or output (position-scoped) | takes a value |
//! | `-f` | input or output (position-scoped) | takes a value |
//! | `-vn` | output | flag |
//! | `-an` | output | flag |
//! | `-c:v` | output | takes a value |
//! | `-c:a` | output | takes a value |
//! | `-b:v` | output | takes a value |
//! | `-b:a` | output | takes a value |
//! | `-crf` | output | takes a value |
//! | `-preset` | output | takes a value |
//! | `-pix_fmt` | output | takes a value |
//! | `-ar` | output | takes a value |
//! | `-ac` | output | takes a value |
//! | `-frames:v` | output | takes a value |
//! | `-vf` | output | takes a value |
//! | `-map` | output | takes a value |
//! | `-movflags` | output | takes a value |
//! | `-hls_time` | output | takes a value |
//! | `-hls_playlist_type` | output | takes a value |
//! | `-hls_list_size` | output | takes a value |
//! | `-hls_segment_filename` | output | takes a value |
//!
//! Structural limits (Round 1): single input, single output, canonical
//! `[options] -i INPUT [options] OUTPUT` order, mandatory `-y`, decimal
//! seconds for every time value, `-vf` limited to one simple `scale=…`,
//! `-map` limited to basic index form in filterless commands, HLS limited to
//! the exact single-rendition VOD option set. Excluded spellings are
//! rejected with a reason and, where one exists, the supported alternative.
//!
//! # String form
//!
//! [`from_cli`] / [`emit_rust_code`] accept one string and apply POSIX word
//! splitting ONLY: single/double quotes, backslash escapes,
//! backslash-newline continuation, plus caret-newline continuation as an
//! explicitly named cmd.exe compatibility extension. No variables, globs,
//! tilde, pipes, redirects, comments or command lists — those tokens are
//! rejected, never emulated. Windows `cmd.exe` quoting is NOT reproduced:
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
/// Emission works for every command that parses — including shapes that are
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
        ShapeStatus::Unverified => {
            return Err(CliError::NotVerified {
                parsed_options: ir.fingerprint().iter().map(|s| s.to_string()).collect(),
            });
        }
    }
    check_runtime_profile()?;
    lower::lower(&ir).into_context().map_err(CliError::Build)
}

fn emit_from_tokens(args: &[String]) -> Result<String, CliError> {
    let ir = parse::parse(args)?;
    let status = manifest::classify(&ir);
    Ok(emit::emit(&lower::lower(&ir), args, &status))
}

/// Runtime-profile gate: in-process execution is allowed only on linked
/// FFmpeg builds whose libavcodec/libavformat major.minor pairs match a
/// verified profile. Purely a version check — it runs before any I/O.
fn check_runtime_profile() -> Result<(), CliError> {
    let avcodec = unsafe { ffmpeg_sys_next::avcodec_version() };
    let avformat = unsafe { ffmpeg_sys_next::avformat_version() };
    let pair = |v: u32| (v >> 16, (v >> 8) & 0xff);
    let (ac_major, ac_minor) = pair(avcodec);
    let (af_major, af_minor) = pair(avformat);

    let verified = VERIFIED_PROFILES.iter().any(|profile| {
        (ac_major, ac_minor) == profile.avcodec && (af_major, af_minor) == profile.avformat
    });
    if verified {
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
    fn runtime_profile_gate_accepts_the_linked_build() {
        // The dev environment links FFmpeg 7.1 or 8.1; both are verified.
        check_runtime_profile().expect("linked build should be a verified profile");
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
