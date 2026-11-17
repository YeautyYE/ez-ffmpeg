//! Emit injection hardening: user-controlled argv text must never break out
//! of the generated program's comments or string literals.
//!
//! The regression case: a VERIFIED V6 command whose `-hls_segment_filename`
//! value smuggles a quoted newline followed by `compile_error!(...)`. An
//! emitter that copies raw argv into `//` comments lets the payload escape
//! the comment, and the generated program fails to compile. Every piece of
//! user text placed in comments is therefore control-escaped, and the
//! command must produce a program that type-checks.
//!
//! Each emission is pinned byte-for-byte (with the crate-version stamp masked)
//! to `examples/cli_emit_hardening_<case>/main.rs`. Cargo compiles those
//! programs as ordinary example targets, for instance with
//! `cargo build --features cli --examples`, so a payload that turns into
//! live code fails the example build.

#![cfg(feature = "cli")]

/// Replaces the header's crate-version stamp with a placeholder so the
/// emission pins survive version bumps without repinning the examples.
/// Deliberately strict: the marker must appear exactly once and the
/// stamp must LOOK like a version (leading digit, then the semver
/// charset), so the mask can never swallow non-version drift.
/// This mirrors the helper in `src/core/cli/emit.rs` and must keep the same strictness.
fn mask_crate_version(code: &str) -> String {
    const MARKER: &str = "crate: ez-ffmpeg ";
    assert_eq!(
        code.matches(MARKER).count(),
        1,
        "expected exactly one crate-version stamp:\n{code}"
    );
    let (head, rest) = code.split_once(MARKER).unwrap();
    let stamp_len = rest
        .bytes()
        .take_while(|b| b.is_ascii_alphanumeric() || matches!(b, b'.' | b'-' | b'+'))
        .count();
    // The stamp is an ASCII prefix, so `stamp_len` is a char boundary.
    let (stamp, tail) = rest.split_at(stamp_len);
    assert!(
        stamp.as_bytes().first().is_some_and(u8::is_ascii_digit),
        "crate-version stamp is not a version: {rest:?}"
    );
    format!("{head}{MARKER}<crate version>{tail}")
}

/// The pin plus cargo compiling the example is this case's type-check.
/// This test compiles nothing itself because nested builds can replace the
/// running test executable and resolve a different feature set.
fn assert_pinned(code: &str, pinned: &str, example: &str) {
    // Windows autocrlf checkouts use CRLF; normalize only the pinned source.
    let pinned = pinned.replace("\r\n", "\n");
    assert_eq!(
        mask_crate_version(code),
        mask_crate_version(&pinned),
        "examples/{example}/main.rs drifted from the emitter and must be regenerated \
         from the emitter output; cargo compiling that example is this case's type-check"
    );
}

/// A benign program must pin and compile too, so a hostile-case failure is
/// attributable to its payload, not the pin mechanism or the example build.
#[test]
fn benign_emission_is_pinned() {
    let code = ez_ffmpeg::cli::emit_rust_code(
        "ffmpeg -i in.mkv -c:v libx264 -crf 23 -preset fast -c:a aac -y out.mp4",
    )
    .unwrap();
    assert_pinned(
        &code,
        include_str!("../examples/cli_emit_hardening_benign/main.rs"),
        "cli_emit_hardening_benign",
    );
}

#[test]
fn quoted_newline_payload_cannot_escape_generated_comments() {
    // The payload: a quoted newline + compile_error! in a VERIFIED
    // V6 command's segment filename.
    let hostile = "ffmpeg -i in.mp4 -c:v libx264 -crf 23 -c:a aac -f hls -hls_time 6 \
                   -hls_playlist_type vod -hls_list_size 0 -hls_segment_filename \
                   'seg\ncompile_error!(\"boom\")_%03d.ts' -y out.m3u8";
    let code = ez_ffmpeg::cli::emit_rust_code(hostile).unwrap();

    // Structural: no generated LINE may start with the payload — the raw
    // newline must have been escape-rendered inside its comment/literal.
    for line in code.lines() {
        let trimmed = line.trim_start();
        assert!(
            !trimmed.starts_with("compile_error!"),
            "payload escaped onto its own line:\n{code}"
        );
    }
    // And the pinned example still type-checks (the literal keeps the payload as
    // DATA inside a string).
    assert_pinned(
        &code,
        include_str!("../examples/cli_emit_hardening_quoted_newline/main.rs"),
        "cli_emit_hardening_quoted_newline",
    );
}

#[test]
fn hostile_noop_values_and_paths_stay_in_comments() {
    // Payloads through a -loglevel value are rejected by its grammar, so
    // aim at paths (header command comment) and the unverified banner path.
    let hostile = "ffmpeg -i 'in\ncompile_error!(\"x\").mp4' -c:v mpeg4 -y 'out\n].avi'";
    let code = ez_ffmpeg::cli::emit_rust_code(hostile).unwrap();
    for line in code.lines() {
        let trimmed = line.trim_start();
        assert!(
            !trimmed.starts_with("compile_error!"),
            "payload escaped onto its own line:\n{code}"
        );
    }
    assert_pinned(
        &code,
        include_str!("../examples/cli_emit_hardening_paths/main.rs"),
        "cli_emit_hardening_paths",
    );
}

/// `*/` in a token must stay data. It is inert in `//` line comments and in
/// string literals; if the emitter ever moved user text into a `/* */` block
/// comment, this payload would terminate it early and the trailing
/// `compile_error!` would become live code — compiling the pinned example is the trap.
#[test]
fn block_comment_terminator_token_stays_data() {
    let args = [
        "-i",
        "in.mp4",
        "-c:v",
        "mpeg4",
        "-y",
        "out*/compile_error!(\"boom\")/*.mp4",
    ];
    let code = ez_ffmpeg::cli::emit_rust_code_from_args(&args).unwrap();
    for line in code.lines() {
        assert!(
            !line.trim_start().starts_with("compile_error!"),
            "payload escaped onto its own line:\n{code}"
        );
    }
    assert_pinned(
        &code,
        include_str!("../examples/cli_emit_hardening_block_comment/main.rs"),
        "cli_emit_hardening_block_comment",
    );
}

/// A quoted newline followed by `//!` aims at Rust's inner doc comments:
/// mid-file, a line starting `//!` is a hard compile error (E0753) even
/// though it looks like "just another comment". The escaped newline must
/// keep it from ever reaching line-start.
#[test]
fn inner_doc_comment_payload_cannot_reach_line_start() {
    let args = ["-i", "in\n//! boom.mp4", "-c:v", "mpeg4", "-y", "out.avi"];
    let code = ez_ffmpeg::cli::emit_rust_code_from_args(&args).unwrap();
    for line in code.lines() {
        assert!(
            !line.trim_start().starts_with("//!"),
            "inner doc comment reached line start:\n{code}"
        );
    }
    assert_pinned(
        &code,
        include_str!("../examples/cli_emit_hardening_inner_doc/main.rs"),
        "cli_emit_hardening_inner_doc",
    );
}

/// An embedded NUL is a control character and must be escape-rendered
/// everywhere — no raw 0x00 byte may survive into generated source.
#[test]
fn nul_byte_token_is_escape_rendered() {
    let args = ["-i", "in\0nul.mp4", "-c:v", "mpeg4", "-y", "out.avi"];
    let code = ez_ffmpeg::cli::emit_rust_code_from_args(&args).unwrap();
    assert!(
        !code.contains('\0'),
        "raw NUL leaked into generated source:\n{code:?}"
    );
    assert_pinned(
        &code,
        include_str!("../examples/cli_emit_hardening_nul/main.rs"),
        "cli_emit_hardening_nul",
    );
}

/// U+2028/U+2029 are line terminators in JavaScript-family tooling and some
/// editors, but NOT Unicode controls — a controls-only escape passes them
/// raw into `//` comments, where tooling that breaks lines at them would
/// see the smuggled `//!` at line start. No raw separator may survive.
#[test]
fn unicode_line_separators_are_escape_rendered() {
    let args = [
        "-i",
        "in\u{2028}//! zl.mp4",
        "-c:v",
        "mpeg4",
        "-y",
        "out\u{2029}//! zp.avi",
    ];
    let code = ez_ffmpeg::cli::emit_rust_code_from_args(&args).unwrap();
    assert!(
        !code.contains('\u{2028}') && !code.contains('\u{2029}'),
        "raw U+2028/U+2029 leaked into generated source:\n{code:?}"
    );
    assert_pinned(
        &code,
        include_str!("../examples/cli_emit_hardening_line_separators/main.rs"),
        "cli_emit_hardening_line_separators",
    );
}
