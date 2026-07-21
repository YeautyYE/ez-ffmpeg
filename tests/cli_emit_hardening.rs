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
//! Type-checking is real: the generated source is compiled with
//! `rustc --emit=metadata` against the newest `libez_ffmpeg` rlib in this
//! test binary's own deps directory — the single authority, required to
//! accept; no other coexisting rlib is consulted (no linking, so no
//! native-library setup is needed).

#![cfg(feature = "cli")]

use std::process::Command;

fn scratch_dir() -> std::path::PathBuf {
    let dir = std::env::temp_dir().join(format!("ez_ffmpeg_emit_hardening_{}", std::process::id()));
    std::fs::create_dir_all(&dir).unwrap();
    dir
}

/// Locates this build's deps directory (where the test binary itself lives)
/// and every `libez_ffmpeg-*.rlib` inside it, newest modification first.
fn deps_and_rlibs() -> (std::path::PathBuf, Vec<std::path::PathBuf>) {
    let exe = std::env::current_exe().unwrap();
    let deps = exe.parent().unwrap().to_path_buf();
    let mut rlibs: Vec<(std::time::SystemTime, std::path::PathBuf)> = std::fs::read_dir(&deps)
        .unwrap()
        .filter_map(|entry| {
            let entry = entry.ok()?;
            let name = entry.file_name().to_string_lossy().into_owned();
            if name.starts_with("libez_ffmpeg-") && name.ends_with(".rlib") {
                Some((entry.metadata().ok()?.modified().ok()?, entry.path()))
            } else {
                None
            }
        })
        .collect();
    rlibs.sort_by(|a, b| b.0.cmp(&a.0));
    assert!(
        !rlibs.is_empty(),
        "no libez_ffmpeg rlib in {deps:?}; integration tests build the lib as a dependency"
    );
    (deps, rlibs.into_iter().map(|(_, path)| path).collect())
}

/// Type-checks a generated program against the NEWEST `libez_ffmpeg` rlib —
/// and ONLY that one. Multiple rlibs coexist in deps (feature variants,
/// artifacts surviving toolchain updates), and any walk that keeps trying
/// candidates until one accepts is a false-pass channel: a program the
/// current crate rejects could still find one stale artifact that accepts
/// it. So the newest rlib must accept outright; every failure — a genuine
/// type error, but also an unusable artifact (metadata incompatibility
/// after a toolchain change) — fails the test, and the message lists the
/// coexisting rlibs so a stale-toolchain failure is recognizable and
/// fixable (`cargo clean`) instead of silently routed around.
fn assert_type_checks(code: &str, name: &str) {
    let dir = scratch_dir();
    let source = dir.join(format!("{name}.rs"));
    std::fs::write(&source, code).unwrap();
    let (deps, rlibs) = deps_and_rlibs();
    let newest = &rlibs[0];
    let out = Command::new("rustc")
        .arg("--edition")
        .arg("2021")
        .arg("--crate-type")
        .arg("bin")
        .arg("--emit=metadata")
        .arg("--extern")
        .arg(format!("ez_ffmpeg={}", newest.display()))
        .arg("-L")
        .arg(format!("dependency={}", deps.display()))
        .arg("--out-dir")
        .arg(&dir)
        .arg(&source)
        .output()
        .expect("failed to spawn rustc");
    if out.status.success() {
        return;
    }
    let stderr = String::from_utf8_lossy(&out.stderr);
    let others: Vec<String> = rlibs[1..]
        .iter()
        .map(|rlib| rlib.display().to_string())
        .collect();
    let others = if others.is_empty() {
        "none".to_string()
    } else {
        others.join(", ")
    };
    panic!(
        "generated program {name} failed to type-check against the newest rlib {}:\n{stderr}\n\
         older coexisting rlibs (deliberately not consulted — an accept from a stale artifact \
         proves nothing; if the failure above is a metadata incompatibility from a toolchain \
         change, run `cargo clean`): {others}",
        newest.display()
    );
}

/// Sanity: the harness itself must be able to compile a benign emission —
/// otherwise a broken rustc invocation would masquerade as an injection.
#[test]
fn harness_compiles_a_benign_emission() {
    let code = ez_ffmpeg::cli::emit_rust_code(
        "ffmpeg -i in.mkv -c:v libx264 -crf 23 -preset fast -c:a aac -y out.mp4",
    )
    .unwrap();
    assert_type_checks(&code, "benign_v1");
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
    // And the program still type-checks (the literal keeps the payload as
    // DATA inside a string).
    assert_type_checks(&code, "hostile_v6");
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
    assert_type_checks(&code, "hostile_paths");
}

/// `*/` in a token must stay data. It is inert in `//` line comments and in
/// string literals; if the emitter ever moved user text into a `/* */` block
/// comment, this payload would terminate it early and the trailing
/// `compile_error!` would become live code — the type-check is the trap.
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
    assert_type_checks(&code, "hostile_block_comment");
}

/// A quoted newline followed by `//!` aims at rustc's inner doc comments:
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
    assert_type_checks(&code, "hostile_inner_doc");
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
    assert_type_checks(&code, "hostile_nul");
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
    assert_type_checks(&code, "hostile_line_separators");
}
