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
//! `rustc --emit=metadata` against the exact `libez_ffmpeg` rlib this test
//! binary was linked with — the single authority, required to accept; no
//! other coexisting rlib is consulted (no linking, so no native-library
//! setup is needed). That rlib is identified by asking cargo, and accepted
//! only once cargo's answer names this running binary as its own test
//! executable; see [`linked_rlib`].

#![cfg(feature = "cli")]

use std::path::{Path, PathBuf};
use std::process::Command;

fn scratch_dir() -> std::path::PathBuf {
    let dir = std::env::temp_dir().join(format!("ez_ffmpeg_emit_hardening_{}", std::process::id()));
    std::fs::create_dir_all(&dir).unwrap();
    dir
}

/// Name of the lib target this test type-checks against (the `ez-ffmpeg`
/// package builds a lib named `ez_ffmpeg`), as cargo reports it in the
/// `name` field of a `compiler-artifact` message.
const LIB_DEP_NAME: &str = "ez_ffmpeg";

/// Every feature of this crate, paired with whether THIS test binary carries
/// it. An integration test is compiled with the package's feature set, so
/// `cfg!` reproduces that set exactly — and [`linked_rlib`]'s identity check
/// turns any mismatch into a named failure instead of a silent fallback onto
/// a differently-featured rlib.
fn active_features() -> Vec<&'static str> {
    [
        ("async", cfg!(feature = "async")),
        ("cli", cfg!(feature = "cli")),
        ("flv", cfg!(feature = "flv")),
        ("http-input", cfg!(feature = "http-input")),
        ("opengl", cfg!(feature = "opengl")),
        ("rtmp", cfg!(feature = "rtmp")),
        ("static", cfg!(feature = "static")),
        ("subtitle", cfg!(feature = "subtitle")),
        ("wgpu", cfg!(feature = "wgpu")),
    ]
    .into_iter()
    .filter(|(_, enabled)| *enabled)
    .map(|(name, _)| name)
    .collect()
}

/// Read the JSON string that starts at the opening quote of `s`, returning it
/// together with the remainder of the line.
///
/// Backslash escapes are decoded because they genuinely occur: cargo emits
/// Windows paths as `"C:\\path\\to\\lib.rlib"`, and taking those bytes
/// verbatim would hand rustc a path with doubled separators. `\uXXXX` does
/// not appear in the path and name fields read here, so it is rejected rather
/// than half-decoded — these helpers only ever return a value they fully
/// understood.
fn read_json_string(s: &str) -> Option<(String, &str)> {
    let rest = s.strip_prefix('"')?;
    let mut out = String::new();
    let mut chars = rest.char_indices();
    while let Some((idx, ch)) = chars.next() {
        match ch {
            '"' => return Some((out, &rest[idx + 1..])),
            '\\' => {
                let (_, escaped) = chars.next()?;
                out.push(match escaped {
                    '"' | '\\' | '/' => escaped,
                    'n' => '\n',
                    'r' => '\r',
                    't' => '\t',
                    'b' => '\u{8}',
                    'f' => '\u{c}',
                    _ => return None,
                });
            }
            _ => out.push(ch),
        }
    }
    None
}

/// The JSON string value following `key` on one artifact line, or `None` when
/// that value is not a string — cargo writes `null` for a unit that produced
/// no executable.
fn json_string_after(line: &str, key: &str) -> Option<String> {
    let (_, rest) = line.split_once(key)?;
    read_json_string(rest).map(|(value, _)| value)
}

/// The JSON string array following `key` on one artifact line. An absent or
/// malformed array yields an empty vector; callers fail on the emptiness, so
/// the resulting message names what was missing rather than a parse position.
fn json_string_array_after(line: &str, key: &str) -> Vec<String> {
    let Some(mut rest) = line
        .split_once(key)
        .and_then(|(_, rest)| rest.strip_prefix('['))
    else {
        return Vec::new();
    };
    let mut out = Vec::new();
    loop {
        rest = rest.trim_start();
        let Some((value, tail)) = read_json_string(rest) else {
            break;
        };
        out.push(value);
        rest = tail.trim_start();
        match rest.strip_prefix(',') {
            Some(tail) => rest = tail,
            None => break,
        }
    }
    out
}

/// The `libez_ffmpeg` rlib THIS test binary links, plus every directory rustc
/// needs on its `-L dependency=` search path.
///
/// Both come from cargo's `--message-format=json` artifact stream, never from
/// cargo's on-disk layout. That layout is not a stable interface and it moved:
/// cargo 1.100 builds each unit under
/// `target/<profile>/build/<pkg>/<unit-hash>/{out,fingerprint}/`, where earlier
/// versions used `target/<profile>/deps/` with records under
/// `target/<profile>/.fingerprint/<pkg>-<unit-hash>/`. Every fixed-depth walk
/// up from this binary broke at once, and dependency rlibs stopped sharing one
/// directory, so a single `-L dependency=target/<profile>/deps` no longer
/// resolves them either.
///
/// What makes the answer trustworthy is an identity check, not the absence of
/// alternatives: the artifact set is accepted only if a test executable cargo
/// reports IS the binary now running. A feature set that does not match the
/// one this binary was built with makes cargo describe a different executable,
/// which fails the test naming both paths — it cannot quietly type-check the
/// generated program against a sibling rlib built with other features. There
/// is deliberately no fallback: an unidentifiable rlib fails, and guessing one
/// would reopen the false-pass hole.
fn linked_rlib() -> (Vec<PathBuf>, PathBuf) {
    let exe = std::env::current_exe().expect("cannot resolve the running test executable");
    let features = active_features().join(",");
    let mut cargo = Command::new(env!("CARGO"));
    cargo.args([
        "test",
        "--test",
        env!("CARGO_CRATE_NAME"),
        "--no-run",
        "--message-format=json",
    ]);
    if !features.is_empty() {
        cargo.args(["--features", features.as_str()]);
    }
    let out = cargo
        .output()
        .expect("failed to spawn cargo to identify the linked rlib");
    assert!(
        out.status.success(),
        "cargo could not re-resolve this test target with features [{features}]:\n{}",
        String::from_utf8_lossy(&out.stderr)
    );
    let stdout = String::from_utf8_lossy(&out.stdout);
    let artifacts: Vec<&str> = stdout
        .lines()
        .filter(|line| line.contains(r#""reason":"compiler-artifact""#))
        .collect();

    // Identity gate. Canonicalize both sides: `current_exe` resolves symlinks
    // on some platforms while cargo reports the path it wrote, and a spurious
    // mismatch here would read as a feature-set bug.
    let canonical = |path: &Path| path.canonicalize().unwrap_or_else(|_| path.to_path_buf());
    let want = canonical(&exe);
    let reported: Vec<PathBuf> = artifacts
        .iter()
        .filter(|line| line.contains(r#""kind":["test"]"#))
        .filter_map(|line| json_string_after(line, r#""executable":"#))
        .map(PathBuf::from)
        .collect();
    assert!(
        reported.iter().any(|path| canonical(path) == want),
        "cargo built test executables {reported:?} for features [{features}], none of which is \
         this running binary {}; the rlib this binary links therefore cannot be identified, and \
         accepting another unit's rlib would type-check the generated program against a crate \
         built with different features",
        exe.display()
    );

    // The directory of EVERY artifact in this unit graph goes on the search
    // path, not just the rlibs: loading `ez_ffmpeg`'s metadata also resolves
    // its proc-macro dependencies, which are dynamic libraries. Under the old
    // layout one `-L dependency=target/<profile>/deps` covered both kinds
    // because every unit landed in that one directory; now each unit has its
    // own, and omitting the proc-macro directories makes rustc fail with
    // `E0463: can't find crate for ez_ffmpeg` — the root crate, not the
    // dependency it could not resolve, which is a confusing way to learn this.
    let mut lib_dirs: Vec<PathBuf> = Vec::new();
    let mut rlib: Option<PathBuf> = None;
    for line in &artifacts {
        let is_lib_target = line.contains(r#""kind":["lib"]"#)
            && line.contains(&format!(r#""name":"{LIB_DEP_NAME}""#));
        for filename in json_string_array_after(line, r#""filenames":"#) {
            let path = PathBuf::from(filename);
            if let Some(dir) = path.parent() {
                if !lib_dirs.iter().any(|known| known == dir) {
                    lib_dirs.push(dir.to_path_buf());
                }
            }
            if is_lib_target && path.extension().is_some_and(|ext| ext == "rlib") {
                assert!(
                    rlib.is_none(),
                    "cargo reported more than one `{LIB_DEP_NAME}` lib rlib ({:?} and {path:?}); \
                     exactly one unit can be the crate this binary links",
                    rlib
                );
                rlib = Some(path);
            }
        }
    }
    let rlib = rlib.unwrap_or_else(|| {
        panic!(
            "cargo reported no `{LIB_DEP_NAME}` lib rlib for features [{features}]; without it \
             the generated program cannot be type-checked against the linked crate"
        )
    });
    assert!(
        rlib.exists(),
        "cargo reported the linked rlib at {rlib:?}, which does not exist"
    );
    (lib_dirs, rlib)
}

/// Type-checks a generated program against the rlib this test binary was
/// LINKED with — and ONLY that one. Multiple rlibs coexist in deps (feature
/// variants, artifacts surviving interrupted builds), and any walk that
/// keeps trying candidates until one accepts is a false-pass channel: a
/// program the current crate rejects could still find one stale artifact
/// that accepts it. So the linked rlib must accept outright; every failure
/// — a genuine type error, but also an unusable artifact (metadata
/// incompatibility between the `rustc` on PATH and the one cargo invoked) —
/// fails the test, and the message lists the coexisting rlibs so a
/// stale-artifact puzzle is recognizable and fixable (`cargo clean`)
/// instead of silently routed around.
fn assert_type_checks(code: &str, name: &str) {
    let dir = scratch_dir();
    let source = dir.join(format!("{name}.rs"));
    std::fs::write(&source, code).unwrap();
    let (lib_dirs, rlib) = linked_rlib();
    let mut rustc = Command::new("rustc");
    rustc
        .arg("--edition")
        .arg("2021")
        .arg("--crate-type")
        .arg("bin")
        .arg("--emit=metadata")
        .arg("--extern")
        .arg(format!("ez_ffmpeg={}", rlib.display()));
    // One `-L` per directory holding an rlib of this unit graph. Cargo 1.100
    // gives every unit its own directory, so the dependency search path is a
    // set rather than the single `deps/` directory it used to be.
    for lib_dir in &lib_dirs {
        rustc.arg("-L").arg(format!("dependency={}", lib_dir.display()));
    }
    let out = rustc
        .arg("--out-dir")
        .arg(&dir)
        .arg(&source)
        .output()
        .expect("failed to spawn rustc");
    if out.status.success() {
        return;
    }
    let stderr = String::from_utf8_lossy(&out.stderr);
    panic!(
        "generated program {name} failed to type-check against the linked rlib {}:\n{stderr}\n\
         no other rlib is consulted — an accept from an artifact this test binary does not link \
         proves nothing. If the failure above is a metadata incompatibility, the `rustc` on PATH \
         differs from the one cargo invoked. `can't find crate for {LIB_DEP_NAME}` itself means \
         rustc could not resolve one of that crate's own dependencies: the search path below is \
         every directory cargo reported for this unit graph ({} entries), so a gap there is a \
         reporting gap, not a stale artifact.\n{lib_dirs:#?}",
        rlib.display(),
        lib_dirs.len()
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
