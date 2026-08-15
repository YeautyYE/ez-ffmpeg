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
  no external libraries): native decoders that FFmpeg compiles without extra
  libraries (typically H.264, HEVC, VP9, AAC, MP3 — **not** AV1/`dav1d`
  unless that library is present), native encoders such as AAC/MJPEG/GIF, the muxers/demuxers that
  need no external dependency (dependency-gated ones — e.g. the WHIP muxer —
  are omitted), the detection filters behind the analysis API
  (black/silence/scene/loudness), and file/pipe I/O — but **no libx264**
  (the `HlsLadder` default fails with a named encoder-selection error;
  pick another encoder via `.video_codec(...)` or opt in to host-dependent
  selection with `.video_codec_auto()`),
  **no PNG/WebP encoders** (write thumbnails as `.jpg`), and **no
  https/TLS**. See the [capability and licensing matrix](#ffmpeg-capability-and-licensing-matrix) below.

To add GPL components, combine the documented `build-*` features — for
example H.264 encoding via a **system-installed** libx264 (the feature links
it, it does not compile it):

```toml
ffmpeg-next = { version = "8.1", features = ["build", "build-license-gpl", "build-lib-x264"] }
```

Binaries produced this way are subject to the GPL.

## FFmpeg capability and licensing matrix

`ez-ffmpeg` links the FFmpeg libraries selected by your application. Crate
features do not add codecs or filters to that FFmpeg build.

The **tested LGPL minimum** is a CI profile: FFmpeg 8.1.2 shared libraries
configured with `--disable-gpl --disable-nonfree --disable-version3
--disable-autodetect`. It contains FFmpeg's native LGPL components and no
optional external libraries. Probe a linked build with
`ez_ffmpeg::capabilities` (`is_encoder_available`, `is_decoder_available`,
`is_filter_available`, `is_muxer_available`, `is_input_protocol_available`,
`is_output_protocol_available`). A `true` result means the component is
**registered**, not that a hardware session will open or a network endpoint
will answer.

The `ffmpeg-next/build` source path above is a different, static, host-CPU
build. It is not this shared `n8.1.2` contract profile.

This is a technical compatibility matrix, not legal advice. FFmpeg's own
LGPL checklist recommends building without `--enable-gpl` and
`--enable-nonfree`, using dynamic linking, supplying corresponding source
and build information, preserving notices, and reviewing EULA restrictions.
External-library licenses and codec patents require separate review:
https://ffmpeg.org/legal.html

### Tested LGPL minimum

The contract job hard-asserts this profile and runs the media paths named
below. Rows that only mention a muxer/decoder name are registry probes
unless the What-to-do column says a suite actually writes or decodes that
format.

| Task | Required component | What failure looks like | What to do |
|---|---|---|---|
| Decode H.264, HEVC, VP9, AAC, or MP3 | FFmpeg native decoders | A named decoder-open error | Confirm the decoder with `capabilities::is_decoder_available`. This profile does not enable `dav1d` or any other external AV1 decoder |
| Encode AAC, MJPEG, GIF, or MPEG-4 Part 2 | `aac`, `mjpeg`, `gif`, or `mpeg4` encoder | `EncoderUnavailable { name }` | Select one of these native encoders explicitly |
| Read/write MP4, MOV, Matroska, MPEG-TS, HLS, FLV, or GIF | Native demuxers/muxers | A named format/muxer error | MP4 (mpeg4), GIF, M4A, and HLS-fMP4 are written in this profile. MOV/Matroska/MPEG-TS/FLV are registry-probed; check `capabilities::is_muxer_available` |
| Export decoded video/audio frames | Native decoder + swscale/swresample | Decoder or conversion setup fails | Check the input decoder; no GPL component is required |
| Burn ASS/SRT subtitles with the `subtitle` feature | The crate's Rust subtitle renderer | Font discovery/rendering error | Provide a usable font; FFmpeg `subtitles`/libass is not required. This profile runs `subtitle_chain` transcode plus renderer unit tests; a full frame-pipeline burn-in also needs a font the host can open |
| Black, silence, scene, and EBU R128 analysis | `blackdetect`, `silencedetect`, `scdet`, `ebur128` | `FFmpeg filter '<name>' is not available` | Check the filter with `capabilities::is_filter_available` |
| Crop analysis (`VideoDetector::Crop`) | The crate's Rust crop detector | `InvalidRecipeArg` if options are out of range; `AnalysisFrame` if a decoded frame is a hardware surface, an unsupported pixel format, or interlaced | Native Rust; no FFmpeg `cropdetect` filter and no `--enable-gpl` are required. Hardware frames need an explicit `hwdownload` before the frame pipeline. Interlaced frames fail closed (deinterlace first). `skip_initial` can step over known-bad leading frames before that validation |
| Embedded RTMP server and FLV parsing | `rtmp` / `flv` crate features | A Rust networking/protocol error | This profile asserts FFmpeg `rtmp` output-protocol registration and the crate features. Full RTMP loopback / FLV E2E is on GPL CI lanes, not this profile. Publishing may still need a suitable H.264 source or encoder |
| MP4 output with no video codec selected | `movenc` default | The output video codec is `mpeg4`, not H.264 | Select the intended encoder explicitly; do not rely on a container default |

### Available while staying LGPL

These need an extra LGPL/permissive encoder, hardware backend, or library.
They are not in the tested minimum profile.

| Task | Optional component or replacement | What failure looks like | What to do |
|---|---|---|---|
| `HlsLadder` without libx264 | A non-GPL encoder the target devices accept | A named HLS encoder-selection error on the historical default | Call `.video_codec_auto()` for host-dependent selection (`h264_videotoolbox` → `h264_nvenc` → `h264_qsv` → `libopenh264`). Pinning one of those names with `.video_codec(...)` uses the same HLS-safe option set; other explicit names do not. Validate player compatibility. Auto-admitted `h264_qsv` is **not** on the packet-sink whitelist |
| H.264 software encoding | FFmpeg `libopenh264` wrapper | Named H.264 encoder is absent | Build FFmpeg with libopenh264 or use a platform hardware encoder; review patent obligations separately. OpenH264's BSD-2-Clause code license is not Cisco's binary patent coverage |
| H.264 hardware encoding | VideoToolbox, NVENC, QSV, AMF, or VAAPI | Encoder is absent, or opening it fails at runtime | Enable the backend and install the required driver/SDK; registry presence does not prove device availability |
| Strict packet-sink video | `libx264`, `h264_nvenc`, `h264_videotoolbox`, or `libopenh264` | `EncoderNotWhitelisted`, `BFramesUnsupported` (VideoToolbox explicit B-frames only), or `EncoderUnavailable` | Use an admitted encoder that this FFmpeg build actually provides. VideoToolbox explicit `bf` / `max_b_frames` must be integer 0 (or unset). Unset `libx264` still uses FFmpeg's default B-frames; set `bf=0` for `dts == pts`. AAC-only sinks work in the minimum profile |
| HDR→SDR on FFmpeg 8.x | Built-in `scale` with `intent=perceptual` (after zscale/libplacebo if those are present) | A named routing error before the first output frame | Probe filter options, not the FFmpeg version string; do not treat a gray file as success |
| HDR→SDR on FFmpeg 7.1 without zimg/libplacebo | No correct built-in PQ/HLG→SDR chain (zimg is zlib; libplacebo is LGPL-2.1) | A named routing error; no output file | Use FFmpeg 8.x `scale` intent, enable zimg or libplacebo, or skip the conversion. Do not use 7.1 `tonemap` on non-linear input. Enabling GPL does not unlock this path |
| PNG thumbnails | zlib-backed PNG encoder | PNG encoder is unavailable | Rebuild FFmpeg with zlib |
| WebP thumbnails | libwebp | WebP encoder is unavailable | Rebuild FFmpeg with `--enable-libwebp` |
| HTTPS input | OpenSSL, GnuTLS, Schannel, or the crate's opt-in `http-input` feature | The `https` input protocol is unavailable | Check `capabilities::is_input_protocol_available("https")`; enable a TLS backend, provide bytes through custom I/O, or use the explicit `HttpInput` API when the `http-input` feature is enabled. Application reconnect is off by default; seekable resume and short-206 continuation require an ETag or Last-Modified unless `require_validator` is set false. A 206 with unknown instance-length (`bytes start-end/*`) is not treated as a complete body. Seeking to a known size is EOF, not a failed Range request. |
| SRT transport | libsrt | The `srt` output protocol is unavailable | Rebuild FFmpeg with `--enable-libsrt` |
| WHIP output | FFmpeg 8.x + DTLS backend + compatible codecs | WHIP muxer/protocol or encoder is unavailable | Enable a supported DTLS backend and select compatible audio/video encoders |
| AV1 encoding with rav1e | librav1e | `librav1e` encoder is unavailable | Enable librav1e; its code is BSD-2-Clause and it carries the Alliance for Open Media Patent License |

OpenSSL 3.x does not automatically make a GPL-enabled FFmpeg build nonfree:
that combination requires FFmpeg's version-3 licensing mode
(`--enable-version3`). Always inspect the exact configure result and every
external library in the build.

### GPL required by the current implementation

These requests still need a GPL-enabled FFmpeg (or an explicit GPL encoder).
There is no silent fallback.

| Task | GPL component | What failure looks like | What to do |
|---|---|---|---|
| `HlsLadder` historical default (`libx264`) | libx264 | A named HLS encoder-selection error | Use `.video_codec_auto()` / `.video_codec(...)`, or knowingly use an FFmpeg build configured with `--enable-gpl --enable-libx264` |
| Explicit `libx264` encoding | libx264 | `EncoderUnavailable { name: "libx264" }` | Use another encoder, or knowingly enable GPL libx264 |
| Explicit `libx265` encoding | libx265 | `EncoderUnavailable { name: "libx265" }` | Use another encoder, or knowingly use a GPL-enabled FFmpeg build |
| Other GPL-only FFmpeg filters/libraries | The component named by FFmpeg's `LICENSE.md` | A named component is unavailable | Consult the FFmpeg license file for the exact linked version before enabling it |

`ez_ffmpeg::codec::get_encoders()` / `get_decoders()` list what your linked
build actually provides. There is no `is_lgpl_build()` helper: missing
`libx264` does not prove the rest of the build is LGPL.

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
