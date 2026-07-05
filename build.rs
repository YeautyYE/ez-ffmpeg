fn main() {
    // docs.rs builds documentation in a network-blocked sandbox and may lack a
    // matching FFmpeg dev environment, so the crate compiles its FFI call sites
    // as stubs there (see the `#[cfg(docsrs)]` / `#[cfg(not(docsrs))]` gates
    // throughout `src/`). docs.rs sets `DOCS_RS=1` in the build environment;
    // mirror that into a `docsrs` cfg so those stubs are selected only on docs.rs.
    //
    // `docsrs` is deliberately NOT a Cargo feature. A Cargo feature is enabled by
    // `--all-features`, which would compile the stubs into an ordinary build — and
    // those stubs skip the real FFmpeg calls, leaving a binary that segfaults the
    // moment it opens a file. Keying off `DOCS_RS` instead means a normal
    // `cargo build`/`test`/`run` (with or without `--all-features`) always compiles
    // the real FFmpeg path; only docs.rs ever sees the stubs. (The stubs guard
    // rustdoc's type-check, not runtime — docs.rs never executes this code.)
    println!("cargo:rerun-if-env-changed=DOCS_RS");
    if std::env::var_os("DOCS_RS").is_some() {
        println!("cargo:rustc-cfg=docsrs");
    }
    // `docsrs` is a well-known cfg name, but declare it explicitly so the
    // `unexpected_cfgs` lint stays quiet across toolchains.
    println!("cargo:rustc-check-cfg=cfg(docsrs)");
}
