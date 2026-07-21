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
//! binary was linked with, resolved through cargo's fingerprint records —
//! the single authority, required to accept; no other coexisting rlib is
//! consulted (no linking, so no native-library setup is needed).

#![cfg(feature = "cli")]

use std::path::{Path, PathBuf};
use std::process::Command;

fn scratch_dir() -> std::path::PathBuf {
    let dir = std::env::temp_dir().join(format!("ez_ffmpeg_emit_hardening_{}", std::process::id()));
    std::fs::create_dir_all(&dir).unwrap();
    dir
}

/// Name of the lib dependency as cargo records it in fingerprint data (the
/// `ez-ffmpeg` package builds a lib target named `ez_ffmpeg`).
const LIB_DEP_NAME: &str = "ez_ffmpeg";

/// Pulls the `ez_ffmpeg` dependency's fingerprint hash out of a unit
/// fingerprint JSON. Entries in the `deps` array have the shape
/// `[<pkg-id-hash>, "<dep name>", <bool>, <fingerprint-hash>]`, and the one
/// named `ez_ffmpeg` is the lib this unit was built against. The scan is
/// textual to keep the test dependency-free; any shape drift fails loudly
/// instead of guessing.
fn lib_dep_fingerprint(json: &str, source: &Path) -> u64 {
    let marker = format!("\"{LIB_DEP_NAME}\",");
    let mut hashes: Vec<u64> = json
        .match_indices(&marker)
        .filter_map(|(idx, _)| {
            let entry = &json[idx + marker.len()..];
            let entry = &entry[..entry.find(']')?];
            entry.rsplit(',').next()?.trim().parse::<u64>().ok()
        })
        .collect();
    hashes.sort_unstable();
    hashes.dedup();
    match hashes[..] {
        [fp] => fp,
        [] => panic!(
            "no `{LIB_DEP_NAME}` dep entry with a fingerprint hash in {}; cargo's fingerprint \
             JSON may have changed shape — the rlib this test binary links cannot be identified",
            source.display()
        ),
        _ => panic!(
            "several distinct `{LIB_DEP_NAME}` dep fingerprints in {}: {hashes:?}; refusing to \
             pick one",
            source.display()
        ),
    }
}

/// Resolves the deps directory, the exact `libez_ffmpeg-<hash>.rlib` this
/// test binary was linked against, and every other coexisting rlib (those
/// feed diagnostics only — they are never consulted).
///
/// Identity, not recency: several `libez_ffmpeg-*.rlib` builds coexist in
/// deps/ (feature variants, artifacts surviving interrupted builds), and any
/// heuristic pick — newest mtime included — can hand the type-check an rlib
/// this binary does not link, silently validating emitted code against the
/// wrong crate build. Cargo records the real link, so follow its records:
///
///   1. the test executable's file name ends in this test unit's hash
///      (`cli_emit_hardening-<unit-hash>`);
///   2. `.fingerprint/<pkg>-<unit-hash>/test-integration-test-<name>.json`
///      is this unit's build record, whose `deps` array carries the
///      fingerprint hash of the `ez_ffmpeg` lib it was compiled against;
///   3. the lib unit directory whose `lib-ez_ffmpeg` file holds that hash
///      (cargo writes it as the hex of its little-endian bytes) names the
///      rlib: `libez_ffmpeg-<lib-unit-hash>.rlib`.
///
/// Any step that cannot be completed fails the test naming the searched
/// path. There is deliberately no fallback: a broken chain means the linked
/// rlib cannot be identified, and guessing would reopen the false-pass hole.
fn linked_rlib() -> (PathBuf, PathBuf, Vec<PathBuf>) {
    let exe = std::env::current_exe().expect("cannot resolve the running test executable");
    let deps = exe
        .parent()
        .unwrap_or_else(|| panic!("test executable {} has no parent directory", exe.display()))
        .to_path_buf();
    let stem = exe
        .file_stem()
        .and_then(|stem| stem.to_str())
        .unwrap_or_else(|| panic!("test executable {} has no UTF-8 file stem", exe.display()));
    let (target_name, unit_hash) = stem.rsplit_once('-').unwrap_or_else(|| {
        panic!("test executable name {stem:?} carries no `-<unit-hash>` suffix; run under `cargo test`")
    });
    assert!(
        unit_hash.len() == 16 && unit_hash.bytes().all(|b| b.is_ascii_hexdigit()),
        "test executable suffix {unit_hash:?} is not a 16-digit unit hash ({})",
        exe.display()
    );
    let fingerprint_root = deps
        .parent()
        .unwrap_or_else(|| panic!("deps directory {} has no parent", deps.display()))
        .join(".fingerprint");
    let unit_json = fingerprint_root
        .join(format!("{}-{unit_hash}", env!("CARGO_PKG_NAME")))
        .join(format!("test-integration-test-{target_name}.json"));
    let json = std::fs::read_to_string(&unit_json).unwrap_or_else(|err| {
        panic!(
            "cannot read this test unit's fingerprint {} ({err}); without that record the rlib \
             this binary links cannot be identified",
            unit_json.display()
        )
    });
    let needle: String = lib_dep_fingerprint(&json, &unit_json)
        .to_le_bytes()
        .iter()
        .map(|byte| format!("{byte:02x}"))
        .collect();
    let entries = std::fs::read_dir(&fingerprint_root).unwrap_or_else(|err| {
        panic!("cannot read fingerprint root {} ({err})", fingerprint_root.display())
    });
    let mut lib_hashes: Vec<String> = entries
        .filter_map(|entry| {
            let entry = entry.ok()?;
            let name = entry.file_name().to_str()?.to_owned();
            let hash = name.strip_prefix(concat!(env!("CARGO_PKG_NAME"), "-"))?.to_owned();
            let recorded =
                std::fs::read_to_string(entry.path().join(format!("lib-{LIB_DEP_NAME}"))).ok()?;
            (recorded.trim() == needle).then_some(hash)
        })
        .collect();
    lib_hashes.sort_unstable();
    lib_hashes.dedup();
    let lib_hash = match &lib_hashes[..] {
        [hash] => hash,
        [] => panic!(
            "no lib unit under {} holds the dep fingerprint {needle} recorded in {}; the rlib \
             this test binary links cannot be identified (a fresh `cargo test` rewrites both \
             records consistently)",
            fingerprint_root.display(),
            unit_json.display()
        ),
        several => panic!(
            "several lib units under {} hold the dep fingerprint {needle}: {several:?}; \
             refusing to pick one",
            fingerprint_root.display()
        ),
    };
    let rlib = deps.join(format!("lib{LIB_DEP_NAME}-{lib_hash}.rlib"));
    assert!(
        rlib.is_file(),
        "fingerprints name {} as the linked rlib, but it does not exist",
        rlib.display()
    );
    let others: Vec<PathBuf> = std::fs::read_dir(&deps)
        .unwrap_or_else(|err| panic!("cannot read deps directory {} ({err})", deps.display()))
        .filter_map(|entry| {
            let path = entry.ok()?.path();
            let name = path.file_name()?.to_str()?;
            (name.starts_with("libez_ffmpeg-") && name.ends_with(".rlib") && path != rlib)
                .then_some(path)
        })
        .collect();
    (deps, rlib, others)
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
    let (deps, rlib, others) = linked_rlib();
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
    let stderr = String::from_utf8_lossy(&out.stderr);
    let others: Vec<String> = others.iter().map(|path| path.display().to_string()).collect();
    let others = if others.is_empty() {
        "none".to_string()
    } else {
        others.join(", ")
    };
    panic!(
        "generated program {name} failed to type-check against the linked rlib {}:\n{stderr}\n\
         coexisting rlibs (deliberately not consulted — an accept from an artifact this test \
         binary does not link proves nothing; if the failure above is a metadata \
         incompatibility, run `cargo clean`): {others}",
        rlib.display()
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
