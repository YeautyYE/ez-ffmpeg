//! Emit injection hardening: user-controlled argv text must never break out
//! of the generated program's comments or string literals.
//!
//! The review probe: a VERIFIED V6 command whose `-hls_segment_filename`
//! value smuggles a quoted newline followed by `compile_error!(...)`. Before
//! the fix the emitter copied raw argv into `//` comments, so the payload
//! escaped the comment and the generated program failed to compile. Now
//! every piece of user text placed in comments is control-escaped, and the
//! probe must produce a program that type-checks.
//!
//! Type-checking is real: the generated source is compiled with
//! `rustc --emit=metadata` against this build's own `libez_ffmpeg` rlib (no
//! linking, so no native-library setup is needed).

#![cfg(feature = "cli")]

use std::process::Command;

fn scratch_dir() -> std::path::PathBuf {
    let dir = std::env::temp_dir().join(format!("ez_ffmpeg_emit_hardening_{}", std::process::id()));
    std::fs::create_dir_all(&dir).unwrap();
    dir
}

/// Locates this build's deps directory (where the test binary itself lives)
/// and the newest `libez_ffmpeg-*.rlib` inside it.
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

/// Type-checks a generated program against the crate. Tries each candidate
/// rlib (feature variants may coexist in deps) and passes when any accepts.
fn assert_type_checks(code: &str, name: &str) {
    let dir = scratch_dir();
    let source = dir.join(format!("{name}.rs"));
    std::fs::write(&source, code).unwrap();
    let (deps, rlibs) = deps_and_rlibs();
    let mut failures = Vec::new();
    for rlib in &rlibs {
        let out = Command::new("rustc")
            .arg("--edition")
            .arg("2021")
            .arg("--crate-type")
            .arg("bin")
            .arg("--emit=metadata")
            .arg("--extern")
            .arg(format!("ez_ffmpeg={}", rlib.display()))
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
        failures.push(format!(
            "{}:\n{}",
            rlib.display(),
            String::from_utf8_lossy(&out.stderr)
        ));
    }
    panic!("generated program {name} failed to type-check against every candidate rlib:\n{}",
        failures.join("\n---\n"));
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
    // The review's probe: a quoted newline + compile_error! in a VERIFIED
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
