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

    // ffmpeg-sys-next (`links = "ffmpeg"`) publishes one `cargo:KEY=VALUE`
    // metadata entry per probed FFmpeg version / API macro, and Cargo hands
    // those to direct dependents as `DEP_FFMPEG_*` environment variables.
    // Re-emit the true ones as plain cfgs (`ffmpeg_8_0`, `ffmpeg_7_1`, ...)
    // so sources can gate on the FFmpeg actually linked — the same mechanism
    // ffmpeg-next's build.rs uses for its version features. Keys with an
    // empty value mark versions/macros the linked FFmpeg does NOT have; they
    // still get a check-cfg declaration so e.g. `#[cfg(ffmpeg_8_0)]` lints
    // clean when building against FFmpeg 7.x.
    for (name, value) in std::env::vars() {
        if let Some(key) = name.strip_prefix("DEP_FFMPEG_") {
            let key = key.to_lowercase();
            // Metadata keys are snake_case idents today; skip anything that
            // would not form a valid cfg name rather than break the build.
            if key.is_empty()
                || key.starts_with(|c: char| c.is_ascii_digit())
                || !key.chars().all(|c| c.is_ascii_alphanumeric() || c == '_')
            {
                continue;
            }
            println!("cargo:rustc-check-cfg=cfg({key})");
            if value == "true" {
                println!("cargo:rustc-cfg={key}");
            }
        }
    }
    // docs.rs builds see no DEP_FFMPEG_* variables at all; keep the cfgs the
    // sources reference declared there too.
    println!("cargo:rustc-check-cfg=cfg(ffmpeg_8_0)");
}
