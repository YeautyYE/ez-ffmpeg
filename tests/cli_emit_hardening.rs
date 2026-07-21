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

/// Byte cursor over the `deps` array of a unit fingerprint JSON. Every
/// method either consumes exactly the expected token or panics naming the
/// record and the byte offset; there is no recovery state, so a malformed
/// record can never yield a value.
struct DepsScan<'a> {
    bytes: &'a [u8],
    pos: usize,
    source: &'a Path,
}

impl<'a> DepsScan<'a> {
    fn fail(&self, what: &str) -> ! {
        panic!(
            "malformed `deps` array in {} at byte {}: {what}; cargo's fingerprint JSON may \
             have changed shape — refusing to decode the record by guesswork",
            self.source.display(),
            self.pos
        )
    }

    fn peek(&self) -> u8 {
        match self.bytes.get(self.pos) {
            Some(byte) => *byte,
            None => self.fail("record ends inside the array"),
        }
    }

    fn expect(&mut self, byte: u8) {
        let found = self.peek();
        if found != byte {
            self.fail(&format!("expected {:?}, found {:?}", byte as char, found as char));
        }
        self.pos += 1;
    }

    /// Consumes `byte` if it is next; reports whether it did.
    fn eat(&mut self, byte: u8) -> bool {
        let hit = self.bytes.get(self.pos) == Some(&byte);
        self.pos += usize::from(hit);
        hit
    }

    fn u64_field(&mut self) -> u64 {
        let start = self.pos;
        while self.bytes.get(self.pos).is_some_and(u8::is_ascii_digit) {
            self.pos += 1;
        }
        if self.pos == start {
            self.fail("expected an unsigned integer field");
        }
        let digits = std::str::from_utf8(&self.bytes[start..self.pos])
            .expect("ASCII digits are valid UTF-8");
        digits.parse::<u64>().unwrap_or_else(|err| {
            self.fail(&format!("integer field {digits:?} is not a u64 ({err})"))
        })
    }

    /// A string field. Escape sequences are a hard failure, not decoded:
    /// dep names cargo records never need them, so one appearing means the
    /// record does not have the shape this scanner understands.
    fn str_field(&mut self) -> &'a str {
        self.expect(b'"');
        let start = self.pos;
        loop {
            match self.peek() {
                b'"' => break,
                b'\\' => self.fail("string field holds an escape sequence"),
                _ => self.pos += 1,
            }
        }
        // The delimiting quotes are ASCII bytes, so the enclosed byte range
        // of this (already valid UTF-8) record is valid UTF-8 itself.
        let text = std::str::from_utf8(&self.bytes[start..self.pos])
            .expect("bytes between ASCII quote delimiters of a str are valid UTF-8");
        self.pos += 1;
        text
    }

    fn bool_field(&mut self) {
        if self.bytes[self.pos..].starts_with(b"true") {
            self.pos += 4;
        } else if self.bytes[self.pos..].starts_with(b"false") {
            self.pos += 5;
        } else {
            self.fail("expected `true` or `false`");
        }
    }
}

/// Pulls the `ez_ffmpeg` dependency's fingerprint hash out of a unit
/// fingerprint JSON. Cargo writes the record as minified JSON whose sole
/// top-level `deps` key holds an array of four-element entries
/// `[<pkg-id-hash>, "<dep name>", <public flag>, <fingerprint-hash>]`, and
/// the entry named `ez_ffmpeg` is the lib this unit was built against.
/// The decoding is dependency-free, so instead of a JSON parser there is a
/// scanner that anchors on the single `"deps":[` occurrence and accepts
/// precisely the recorded entry shape. Dep-name text appearing anywhere
/// else in the record (another field could embed `"ez_ffmpeg",77`) is
/// never consulted, and every deviation — a missing or duplicated
/// `"deps":[` key, an entry with the wrong element count or types, an
/// unterminated string, a non-u64 number — is a hard failure naming the
/// record, never a guess.
fn lib_dep_fingerprint(json: &str, source: &Path) -> u64 {
    const DEPS_KEY: &str = "\"deps\":[";
    let mut occurrences = json.match_indices(DEPS_KEY).map(|(idx, _)| idx);
    let anchor = occurrences.next().unwrap_or_else(|| {
        panic!(
            "no `\"deps\":[` key in {}; cargo's fingerprint JSON may have changed shape — \
             the rlib this test binary links cannot be identified",
            source.display()
        )
    });
    if let Some(imposter) = occurrences.next() {
        panic!(
            "`\"deps\":[` occurs at byte {anchor} and again at byte {imposter} of {}; \
             refusing to guess which one is the top-level deps array",
            source.display()
        );
    }
    let mut scan = DepsScan { bytes: json.as_bytes(), pos: anchor + DEPS_KEY.len(), source };
    let mut hashes: Vec<u64> = Vec::new();
    if !scan.eat(b']') {
        loop {
            scan.expect(b'[');
            scan.u64_field(); // package-id hash: shape-checked, value unused
            scan.expect(b',');
            let name = scan.str_field();
            scan.expect(b',');
            scan.bool_field();
            scan.expect(b',');
            let fingerprint = scan.u64_field();
            scan.expect(b']');
            if name == LIB_DEP_NAME {
                hashes.push(fingerprint);
            }
            if scan.eat(b']') {
                break;
            }
            scan.expect(b',');
        }
    }
    hashes.sort_unstable();
    hashes.dedup();
    match hashes[..] {
        [fp] => fp,
        [] => panic!(
            "no `{LIB_DEP_NAME}` dep entry in the `deps` array of {}; the rlib this test \
             binary links cannot be identified",
            source.display()
        ),
        _ => panic!(
            "several distinct `{LIB_DEP_NAME}` dep fingerprints in {}: {hashes:?}; refusing to \
             pick one",
            source.display()
        ),
    }
}

/// A unit directory with no `lib-ez_ffmpeg` record is normally structural —
/// a unit of this package that builds no lib (this test's own unit, a
/// build-script unit) — and safe to skip. But cargo writes the artifact
/// before the fingerprint record, so an interrupted build can leave
/// `deps/libez_ffmpeg-<hash>.rlib` behind with the record missing, and such
/// an orphan could be the very build this binary links: skipping its unit
/// would let a stale sibling claim the unique match. Unit directory and
/// artifact share the hash (`.fingerprint/<pkg>-<hash>` names
/// `libez_ffmpeg-<hash>.rlib` — the same correspondence the selected
/// unit's rlib path is built from after the scan), so the skip is allowed
/// only once that artifact is proven absent.
fn assert_no_orphan_rlib(deps: &Path, record: &Path, unit_hash: &str) {
    let artifact = deps.join(format!("lib{LIB_DEP_NAME}-{unit_hash}.rlib"));
    match artifact.symlink_metadata() {
        Ok(_) => panic!(
            "unit record {} does not exist, yet the unit's artifact {} does; an interrupted \
             build can leave an rlib whose fingerprint was never written, and the linked \
             rlib cannot be claimed unique while such a candidate exists (run `cargo clean`)",
            record.display(),
            artifact.display()
        ),
        Err(err) if err.kind() == std::io::ErrorKind::NotFound => {}
        Err(err) => panic!(
            "unit record {} does not exist and the probe for its artifact {} failed ({err}); \
             refusing to skip a unit whose artifact state cannot be read",
            record.display(),
            artifact.display()
        ),
    }
}

/// Resolves the deps directory, the exact `libez_ffmpeg-<hash>.rlib` this
/// test binary was linked against, and a listing of every other coexisting
/// rlib (the listing feeds diagnostics only — those rlibs are never
/// consulted, and a deps-directory entry that cannot be read, or a deps
/// directory that cannot be listed at all, is annotated in it rather than
/// dropped).
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
/// path — including every record touched along the way: an unreadable
/// directory entry, a non-UTF-8 file name, or an unreadable or malformed
/// `lib-ez_ffmpeg` record aborts the scan instead of being skipped, and a
/// unit directory with no such record is skipped only once its same-hash
/// rlib artifact is proven absent (cargo writes the artifact before the
/// record, so an interrupted build strands an rlib no record accounts
/// for), because step 3 may claim a unique match only if every candidate
/// that could be the linked lib was actually read. There is deliberately
/// no fallback: a broken chain means the linked rlib cannot be identified,
/// and guessing would reopen the false-pass hole.
fn linked_rlib() -> (PathBuf, PathBuf, Vec<String>) {
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
    // Uniqueness below is a claim about ALL candidate records, so every
    // record this walk touches must be decoded: an unreadable entry, a
    // non-UTF-8 name, or an unreadable/malformed record file could hide the
    // very unit that was linked, letting a stale sibling pass as "unique".
    // Only two outcomes skip a directory — a foreign package prefix, or the
    // record file not existing (a unit of this package that is not the lib,
    // e.g. this test's own unit) once the unit is also proven to have left
    // no orphan rlib artifact behind.
    let mut lib_hashes: Vec<String> = Vec::new();
    for entry in entries {
        let entry = entry.unwrap_or_else(|err| {
            panic!(
                "unreadable directory entry under fingerprint root {} ({err}); the linked \
                 rlib cannot be claimed unique without reading every unit record",
                fingerprint_root.display()
            )
        });
        let name = entry.file_name();
        let name = name.to_str().unwrap_or_else(|| {
            panic!(
                "non-UTF-8 file name {name:?} under fingerprint root {}; the linked rlib \
                 cannot be claimed unique without decoding every unit record",
                fingerprint_root.display()
            )
        });
        let Some(hash) = name.strip_prefix(concat!(env!("CARGO_PKG_NAME"), "-")) else {
            continue;
        };
        let record = entry.path().join(format!("lib-{LIB_DEP_NAME}"));
        let recorded = match std::fs::read_to_string(&record) {
            Ok(text) => text,
            Err(err) if err.kind() == std::io::ErrorKind::NotFound => {
                assert_no_orphan_rlib(&deps, &record, hash);
                continue;
            }
            Err(err) => panic!(
                "cannot read lib unit record {} ({err}); the linked rlib cannot be claimed \
                 unique while any candidate record is unreadable",
                record.display()
            ),
        };
        let recorded = recorded.trim();
        assert!(
            recorded.len() == 16 && recorded.bytes().all(|b| b.is_ascii_hexdigit()),
            "malformed lib unit record {}: {recorded:?} is not the 16-hex-digit fingerprint \
             hash cargo writes; the linked rlib cannot be claimed unique while any candidate \
             record is undecodable",
            record.display()
        );
        // The equality is over lowercase hex on both sides: the needle by
        // construction (`{:02x}`), the record by normalization here. The
        // validation above admits either case, and two records holding one
        // fingerprint in two cases must collide into the several-units
        // panic below — a case difference must never manufacture a unique
        // match.
        let recorded = recorded.to_ascii_lowercase();
        if recorded == needle {
            lib_hashes.push(hash.to_owned());
        }
    }
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
    // Diagnostics only: list the coexisting rlibs so a stale-artifact puzzle
    // is recognizable. Entries this walk cannot decode are annotated in the
    // listing instead of silently dropped — hiding them would understate how
    // crowded deps/ is exactly when the state is corrupt — and a deps
    // directory that cannot be listed at all becomes a single annotated
    // line: this listing exists to explain a primary failure, so it must
    // never panic and preempt that failure's own message.
    let others: Vec<String> = match std::fs::read_dir(&deps) {
        Ok(entries) => entries
            .filter_map(|entry| match entry {
                Ok(entry) => {
                    let path = entry.path();
                    match path.file_name().and_then(|name| name.to_str()) {
                        Some(name) => (name.starts_with("libez_ffmpeg-")
                            && name.ends_with(".rlib")
                            && path != rlib)
                            .then(|| path.display().to_string()),
                        None => Some(format!("{} (non-UTF-8 file name)", path.display())),
                    }
                }
                Err(err) => Some(format!("<unreadable entry in {} ({err})>", deps.display())),
            })
            .collect(),
        Err(err) => vec![format!("<cannot list deps dir {}: {err}>", deps.display())],
    };
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
