# Installation & Build Guide

ez-ffmpeg links against the FFmpeg system libraries. This guide covers
providing those libraries on each platform, static linking on Windows,
building FFmpeg from source through Cargo, what common tasks require from the
linked FFmpeg build, troubleshooting for the most-reported build errors, and
release-profile advice.

Version requirements:

- **Rust:** 1.80.0 or higher for the default feature set. The optional `wgpu`
  GPU-filter feature pulls in the wgpu 26 dependency stack, which currently
  requires Rust 1.85+.
- **FFmpeg:** 7.1 through 8.x (one build links either major; the bindings
  gate on the installed version).

## Platform prerequisites

### macOS

```bash
brew install ffmpeg
```

### Windows

```bash
# For dynamic linking
vcpkg install ffmpeg

# For static linking (requires 'static' feature)
vcpkg install ffmpeg:x64-windows-static-md

# Set VCPKG_ROOT environment variable
```

Set `VCPKG_ROOT` in your shell before building. Calling `std::env::set_var`
inside `build.rs` does not work: `ffmpeg-sys-next`'s build script runs as a
separate process and never sees those variables.

### Linux

ez-ffmpeg links FFmpeg **7.1–8.x**, which is newer than what many
distributions package (Ubuntu 24.04 ships FFmpeg 6.x, for example). Install
the development libraries from a source that provides FFmpeg 7.1+ or 8 — or
build FFmpeg through Cargo (see
[Building FFmpeg from source](#building-ffmpeg-from-source-linuxmacos)):

```bash
# Debian/Ubuntu (needs an apt source that provides FFmpeg 7.1+):
sudo apt install pkg-config clang \
    libavcodec-dev libavformat-dev libavfilter-dev libavdevice-dev \
    libavutil-dev libswscale-dev libswresample-dev
pkg-config --modversion libavcodec   # needs >= 61.13 (FFmpeg 7.1); 62.x => FFmpeg 8.x
```

`pkg-config` and `clang` (for bindgen) are required regardless of how FFmpeg
is provided.

## Static linking on Windows

Static linking fails with `unresolved external symbol` errors?

`ffmpeg-sys-next`'s vcpkg path emits only a handful of Windows system
libraries (see
[rust-ffmpeg-sys#28](https://github.com/zmwangx/rust-ffmpeg-sys/issues/28)),
so the final link of your application can fail with many
`unresolved external symbol` errors (`BCrypt*`, `MF*`, DirectShow, ...).
If it does, declare the missing libraries in **your own project's**
`build.rs`:

```rust
// build.rs of your application
fn main() {
    if std::env::var("CARGO_CFG_TARGET_OS").as_deref() == Ok("windows") {
        for lib in [
            "user32", "kernel32", "gdi32", "shell32", "ole32", "oleaut32",
            "uuid", "advapi32", "bcrypt", "ws2_32", "winmm", "crypt32",
            "secur32", "strmiids", "mfplat", "mfuuid", "mf", "mfreadwrite",
            "dxgi", "d3d11", "quartz", "comdlg32", "winspool", "version",
            "setupapi", "shlwapi", "ncrypt", "vfw32",
        ] {
            println!("cargo:rustc-link-lib={}", lib);
        }
    }
}
```

Notes:

- The exact list depends on the vcpkg FFmpeg port version and its enabled
  features; the list above is the union of libraries users reported in
  [#16](https://github.com/YeautyYE/ez-ffmpeg/issues/16) in June 2025 and
  July 2026 — your exact set may differ.
- Dynamic linking (`vcpkg install ffmpeg`) is not affected.

## Building FFmpeg from source (Linux/macOS)

No system FFmpeg? `ffmpeg-next` — a dependency of ez-ffmpeg — can compile a
minimal FFmpeg from source during `cargo build`. Add it as a direct
dependency with its `build` feature; Cargo feature unification applies it to
the copy ez-ffmpeg uses:

```toml
[dependencies]
ez-ffmpeg = "*"
# Keep the FFmpeg major in sync with the one ez-ffmpeg depends on:
ffmpeg-next = { version = "8.1", features = ["build"] }
```

What to expect:

- **Platforms:** Linux and macOS. **Not** supported on Windows — vcpkg
  (above) remains the Windows path.
- **Build prerequisites:** `git`, network access, a C toolchain, `make`,
  `nasm`/`yasm`, `clang`, and `pkg-config`. The first build compiles FFmpeg
  (typically 10–20 minutes) and adds roughly 1 GB to `target/` (the artifacts
  keep debug symbols).
- **Reproducibility:** the build clones FFmpeg's `release/8.1` **moving
  branch** at build time, so two clean builds of the same `Cargo.lock` can
  compile different FFmpeg commits. `--locked` and `cargo vendor` do not
  cover the nested clone, and offline builds are not supported. This is fine
  for local decode/analysis and CI; for production, prefer a system FFmpeg
  you provision and pin yourself.
- **Portability:** the upstream build compiles with `-march=native`, so the
  resulting binaries are tied to the building machine's CPU — do not
  redistribute them.
- **Capabilities:** the result is a *minimal* FFmpeg (`--disable-autodetect`,
  no external libraries): all native decoders (H.264, HEVC, AV1, VP9, AAC,
  MP3, …), native encoders such as AAC/MJPEG/GIF, the muxers/demuxers that
  need no external dependency (dependency-gated ones — e.g. the WHIP muxer —
  are omitted), the detection filters behind the analysis API
  (black/silence/scene/loudness), and file/pipe I/O — but **no libx264**
  (HLS ladders must select another encoder via `.video_codec(...)`),
  **no PNG/WebP encoders** (write thumbnails as `.jpg`), and **no
  https/TLS**. See the capability matrix below.

To add GPL components, combine the documented `build-*` features — for
example H.264 encoding via a **system-installed** libx264 (the feature links
it, it does not compile it):

```toml
ffmpeg-next = { version = "8.1", features = ["build", "build-license-gpl", "build-lib-x264"] }
```

Binaries produced this way are subject to the GPL.

## FFmpeg capability matrix

What common ez-ffmpeg tasks require from the linked FFmpeg build:

| Task | Requirement in the linked FFmpeg |
|---|---|
| Decode H.264 / HEVC / AV1 / VP9 / AAC / MP3 | Native decoders — any standard build |
| Encode AAC audio | Native encoder — any standard build |
| JPEG thumbnails, GIF export | Native encoders — any standard build |
| Black / silence / scene / loudness detection | Built-in filters — any standard build |
| Subtitle burn-in (`subtitle` feature) | Nothing extra — rendered by a pure-Rust engine, no `--enable-libass` |
| RTMP server (`rtmp` feature) | Nothing extra — in-process server |
| H.264 encode (`HlsLadder` default `libx264`) | `--enable-gpl --enable-libx264` (vcpkg: the `x264` feature); otherwise pick another encoder via `.video_codec(...)` / `Output::set_video_codec` |
| PNG thumbnails | A PNG encoder (zlib; present in full builds, absent from the minimal source build) |
| WebP thumbnails | `--enable-libwebp` |
| `https://` inputs | A TLS backend (`--enable-openssl` / `--enable-gnutls`; present in full builds, absent from the minimal source build) |
| Crop detection | The `cropdetect` filter — a GPL build (`--enable-gpl`) |
| Hardware acceleration (NVENC / QSV / AMF / VideoToolbox / VAAPI) | The matching build flags **and** the runtime drivers/SDK |

When a required encoder is missing, ez-ffmpeg fails with an error naming it
(for example `encoder 'libx264' is not available in the linked FFmpeg
build`). `ez_ffmpeg::codec::get_encoders()` / `get_decoders()` list what your
linked build actually provides.

## Troubleshooting

<details>
<summary><code>encoder '…' is not available in the linked FFmpeg build</code></summary>

The FFmpeg your binary linked against does not include that encoder. List
what is actually available with `ez_ffmpeg::codec::get_encoders()`, then
either link an FFmpeg build that enables the encoder (see the capability
matrix above) or select an available one (`Output::set_video_codec` /
`set_audio_codec`, or `.video_codec(...)` on recipes). On Windows, a vcpkg
*feature list* is not proof the DLLs you link at runtime provide it —
inspect with `get_encoders()`, and note that hardware encoders
(`h264_nvenc`, `qsv`, `amf`) additionally require the vendor runtime/driver.
(Reported in [#35](https://github.com/YeautyYE/ez-ffmpeg/issues/35).)

</details>

<details>
<summary><code>unresolved import ffmpeg_sys_next::AVCodecConfig</code> and similar missing-type build errors</summary>

The FFmpeg headers found at build time are older than 7.1 (`AVCodecConfig`
arrived in FFmpeg 7.1). Distribution and vcpkg ports can lag — check with
`pkg-config --modversion libavcodec` (the API needs libavcodec >= 61.13;
released FFmpeg 7.1 reports 61.19.x, and 62.x = 8.x) or your
vcpkg port version, and upgrade to FFmpeg 7.1–8.x.
(Reported in [#18](https://github.com/YeautyYE/ez-ffmpeg/issues/18).)

</details>

<details>
<summary>ARM/aarch64: <code>cannot find type __va_list_tag_aarch64</code></summary>

A `va_list` portability bug in older ez-ffmpeg releases, fixed in current
versions. Update the `ez-ffmpeg` dependency; if a current release still
fails on your target triple, open an issue including the triple and FFmpeg
version.
(Reported in [#33](https://github.com/YeautyYE/ez-ffmpeg/issues/33).)

</details>

<details>
<summary>Windows static linking fails only when the project has both <code>main.rs</code> and <code>lib.rs</code></summary>

Projects with both a binary and a library target in the same package have
hit unresolved FFmpeg symbols in Windows static builds. If you see this, use
a single-target layout: move the code into `main.rs`, or split the library
into its own package that the binary depends on. See
[Static linking on Windows](#static-linking-on-windows) for the accompanying
system-library list.
(Reported in [#20](https://github.com/YeautyYE/ez-ffmpeg/issues/20).)

</details>

## Performance build

ez-ffmpeg is a library; release-profile choices are controlled by the final
application or workspace root, not by this crate when used as a dependency.
For production binaries, start with:

```toml
[profile.release]
lto = "thin"
codegen-units = 1
```

For machine-specific deployments, benchmark with `target-cpu=native` (do NOT
use it for redistributed generic binaries):

```bash
RUSTFLAGS="-C target-cpu=native" cargo build --release
```

These Rust settings optimize ez-ffmpeg and your application code. Dynamically
linked FFmpeg libraries are built separately; their performance depends on
how your FFmpeg package was compiled. Allocator choice is workload-dependent
— for RTMP fan-out, many concurrent connections, or allocation-heavy
callback pipelines, measure jemalloc or mimalloc against the system allocator
before adopting one globally.
