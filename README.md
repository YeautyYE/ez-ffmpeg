<p align="center">
  <img src="https://raw.githubusercontent.com/YeautyYE/ez-ffmpeg/main/logo.jpg" alt="Logo" width="300">
</p>

<div align="center">

[![Crates.io](https://img.shields.io/crates/v/ez-ffmpeg.svg)](https://crates.io/crates/ez-ffmpeg)
[![Documentation](https://img.shields.io/badge/docs.rs-ez--ffmpeg-blue)](https://docs.rs/ez-ffmpeg)
[![License: MIT/Apache-2.0/MPL-2.0](https://img.shields.io/badge/License-MIT%2FApache--2.0%2FMPL--2.0-brightgreen.svg)](https://github.com/YeautyYE/ez-ffmpeg/blob/main/LICENSE-APACHE)
[![Rust](https://img.shields.io/badge/Rust-%3E=1.80.0-orange)](https://www.rust-lang.org/)
[![FFmpeg](https://img.shields.io/badge/FFmpeg-7.0--8.x-blue)](https://ffmpeg.org)

</div>


## Overview

**`ez-ffmpeg`** provides a **safe and ergonomic Rust interface for FFmpeg integration**, offering a familiar API that closely follows FFmpeg’s original logic and parameter structures.

This library:
- Exposes a safe public API; the internal FFmpeg FFI layer uses audited `unsafe` code
- Keeps the execution logic and parameter conventions as close to FFmpeg as possible
- Provides an intuitive and user-friendly API for media processing
- Supports custom Rust filters and flexible input/output handling
- Offers optional GPU-accelerated custom filters (wgpu) and a high-performance embedded RTMP server
- Ships one-shot recipes (thumbnails/sprite sheets, animated GIF, HLS ABR ladders) and a detection/measurement API (black/silence/scene/crop/EBU R128 loudness) that returns typed Rust results instead of only FFmpeg logs

By abstracting the complexity of the raw C API, `ez-ffmpeg` simplifies configuring media pipelines, performing transcoding and filtering, and inspecting media streams.

The transcoding pipeline is ported from the FFmpeg CLI sources (`fftools/ffmpeg`, FFmpeg 7.x): the demux/decode/filter/encode/mux stages keep the fftools function names and semantics, and code comments cite the corresponding C file and line (line numbers refer to the FFmpeg `n7.1` tag). FFmpeg developers can navigate the codebase by grepping for the names they already know (`ts_fixup`, `video_sync_process`, `enc_open`, `mux_fixup_ts`, ...).

Bitstream filters (`-bsf:v/-bsf:a/-bsf:s`) are supported via `Output::set_video_bsf` / `set_audio_bsf` / `set_subtitle_bsf` (single filter or comma-separated chain, e.g. `h264_mp4toannexb`).

Not every CLI feature is implemented. Notable gaps (unsupported paths fail with explicit errors): progress/stats reporting (`-progress`), sub2video, `-fix_sub_duration`, and two-pass encoding.

## Version Requirements

- **Rust:** Version 1.80.0 or higher.
- **FFmpeg:** Version 7.0 through 8.x (one build links either major; the bindings gate on the installed version). 

## Documentation

More information about this crate can be found in the [crate documentation](https://docs.rs/ez-ffmpeg).

## Quick Start

### Installation Prerequisites

#### macOS
```bash
brew install ffmpeg
```

#### Windows
```bash
# For dynamic linking
vcpkg install ffmpeg

# For static linking (requires 'static' feature)
vcpkg install ffmpeg:x64-windows-static-md

# Set VCPKG_ROOT environment variable
```

<details>
<summary>Static linking fails with <code>unresolved external symbol</code> errors? (click to expand)</summary>

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
- Set `VCPKG_ROOT` in your shell before building. Calling `std::env::set_var`
  inside `build.rs` does not work: `ffmpeg-sys-next`'s build script runs as a
  separate process and never sees those variables.
- The exact list depends on the vcpkg FFmpeg port version and its enabled
  features; the list above is the union of libraries users reported in
  [#16](https://github.com/YeautyYE/ez-ffmpeg/issues/16) in June 2025 and
  July 2026 — your exact set may differ.
- Dynamic linking (`vcpkg install ffmpeg`) is not affected.

</details>

### Adding the Dependency

Add **ez-ffmpeg** to your project by including it in your `Cargo.toml`:

```toml
[dependencies]
ez-ffmpeg = "*"
```

### Basic Usage

Below is a basic example to get you started. Create or update your `main.rs` with the following code:

```rust
use ez_ffmpeg::FfmpegContext;
use ez_ffmpeg::FfmpegScheduler;

fn main() -> Result<(), Box<dyn std::error::Error>> {
    // 1. Build the FFmpeg context
    let context = FfmpegContext::builder()
        .input("input.mp4")
        .filter_desc("hue=s=0") // Example filter: desaturate (optional)
        .output("output.mov")
        .build()?;

    // 2. Run it via FfmpegScheduler (synchronous mode)
    let result = FfmpegScheduler::new(context)
        .start()?
        .wait();
    result?; // Propagate any errors that occur
    Ok(())
}
```
More examples can be found [here][examples].

[examples]: https://github.com/YeautyYE/ez-ffmpeg/tree/master/examples

## Streaming protocol outputs (WHIP / SRT)

Whether a streaming output works depends on the FFmpeg build your process
links against, not on ez-ffmpeg itself. The `capabilities` module probes the
linked build up front, so unsupported setups can fail with a clear error
instead of a mid-pipeline failure:

```rust
use ez_ffmpeg::capabilities;

// Muxers (output formats) and I/O protocols are separate namespaces.
let has_whip_muxer = capabilities::is_muxer_available("whip");
let has_srt_protocol = capabilities::is_output_protocol_available("srt");
```

A `true` result only means the component is compiled into the linked FFmpeg;
encoders, TLS backends, endpoint compatibility, and network reachability are
separate concerns.

### WHIP output (experimental, FFmpeg 8+)

**Status:** FFmpeg 8 ships an upstream `whip` muxer that publishes WebRTC
streams to WHIP endpoints (Twitch/IVS, Cloudflare Stream, LiveKit, MediaMTX,
...). The muxer is marked **experimental** upstream and has a known upstream
FIXME on Opus timestamp handling, and ez-ffmpeg's own CI cannot exercise it
(its FFmpeg builds carry no DTLS backend) — treat this section as status and
instructions, not as a verified recipe.

Requirements, all imposed by the upstream muxer:

- **FFmpeg 8.0 or newer, built with a DTLS-capable TLS backend.** FFmpeg 8.0
  supports OpenSSL or Schannel for DTLS; FFmpeg 8.1 accepts any of OpenSSL,
  GnuTLS, Schannel, or mbedTLS. Without one of these, the `whip` muxer is not
  compiled in — `capabilities::is_muxer_available("whip")` returns `false`.
- **Video: H.264, Baseline or Constrained Baseline profile only, with
  B-frames disabled.** The muxer supports only those H.264 profiles, and
  real-time WebRTC playout does not reorder frames. `libx264` defaults to the
  High profile, so you must set the profile explicitly (see the example). The
  muxer reads the H.264 profile/level from global headers; ez-ffmpeg raises
  the encoder's global-header flag automatically whenever a muxer requires
  it, so that part needs no configuration.
- **Audio: Opus at 48 kHz stereo** — the muxer enforces this combination.

Discover the encoders your build offers with `ez_ffmpeg::codec::get_encoders()`.
H.264 encoders are commonly `libx264` (GPL — mind your licensing),
`libopenh264`, or hardware encoders such as `h264_nvenc` /
`h264_videotoolbox`; Opus is commonly `libopus`.

```rust
use ez_ffmpeg::{capabilities, FfmpegContext, FfmpegScheduler, Input, Output};

fn main() -> Result<(), Box<dyn std::error::Error>> {
    if !capabilities::is_muxer_available("whip") {
        return Err("this FFmpeg build has no 'whip' muxer \
                    (WHIP needs FFmpeg >= 8.0 with a DTLS backend)"
            .into());
    }

    // File inputs must be paced to real time for live publishing.
    let input = Input::from("input.mp4").set_readrate(1.0);

    let output = Output::from("https://example.com/whip/endpoint")
        .set_format("whip") // required: never auto-guessed from the URL
        .set_format_opt("authorization", "<token>") // raw token; FFmpeg itself adds "Bearer "
        .set_video_codec("libx264") // pick from codec::get_encoders()
        .set_video_codec_opt("profile", "baseline") // WHIP: Baseline profile only
        .set_video_codec_opt("bf", "0") // WHIP: no B-frames
        .set_audio_codec("libopus")
        .set_audio_sample_rate(48000) // the muxer requires 48 kHz stereo Opus
        .set_audio_channels(2);

    FfmpegScheduler::new(FfmpegContext::builder().input(input).output(output).build()?)
        .start()?
        .wait()?;
    Ok(())
}
```

### SRT output

SRT output needs two independent components in the linked FFmpeg build: the
`srt` **protocol** (built with `--enable-libsrt`) for transport, and a
container muxer for the payload — MPEG-TS below. Probe both:

```rust
use ez_ffmpeg::capabilities;

let srt_ready = capabilities::is_output_protocol_available("srt")
    && capabilities::is_muxer_available("mpegts");
```

> **Never test SRT streaming support with `is_muxer_available("srt")`.** That
> name matches the SubRip **subtitle** muxer, which exists in practically
> every FFmpeg build, so the probe returns `true` whether or not the SRT
> transport is present — it tells you nothing about SRT streaming support.

```rust
use ez_ffmpeg::{capabilities, FfmpegContext, FfmpegScheduler, Output};

fn main() -> Result<(), Box<dyn std::error::Error>> {
    if !(capabilities::is_output_protocol_available("srt")
        && capabilities::is_muxer_available("mpegts"))
    {
        return Err("this FFmpeg build lacks the srt protocol \
                    (--enable-libsrt) or the mpegts muxer"
            .into());
    }

    let output = Output::from(
        // ALL protocol options live in the URL query; latency is in MICROSECONDS.
        "srt://127.0.0.1:9000?mode=caller&transtype=live&latency=120000&payload_size=1316",
    )
    .set_format("mpegts");

    FfmpegScheduler::new(FfmpegContext::builder().input("input.mp4").output(output).build()?)
        .start()?
        .wait()?;
    Ok(())
}
```

Three warnings worth reading twice:

- **Protocol options go in the URL query — only.** On the output side,
  ez-ffmpeg opens the network connection without an options dictionary, so
  `Output::set_format_opt` feeds the MPEG-TS **muxer**, never the SRT
  protocol. A `passphrase` set that way only draws an "option not recognized"
  warning from the muxer and never reaches the transport, so the stream goes
  out **unencrypted** with no hard error. Encryption parameters (`passphrase`,
  `pbkeylen`) must go in the URL query; percent-encode the passphrase if it
  contains characters reserved in URLs.
- **`latency` is in microseconds, not milliseconds.** `latency=120000` means
  120 ms. libsrt truncates the value to whole milliseconds by integer
  division, so a value of `120` collapses to 0 ms — an unusable budget.
- **Redact stream URLs in logs.** With SRT, credentials (`passphrase`) are
  part of the URL itself, so any log line printing the URL leaks them.

On the **input** side, `srt://` is a live network protocol with no seek
support, and — unlike the output side — `Input::set_format_opt` options do
reach the `avformat_open_input` call.

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
linked FFmpeg libraries are built separately; their performance depends on how
your FFmpeg package was compiled. Allocator choice is workload-dependent — for
RTMP fan-out, many concurrent connections, or allocation-heavy callback
pipelines, measure jemalloc or mimalloc against the system allocator before
adopting one globally.

## Features

**ez-ffmpeg** offers several optional features that can be enabled in your `Cargo.toml` as needed:

- **wgpu:** GPU-accelerated custom video filters written in WGSL, running headless over Vulkan/Metal/DX12/GL — YUV↔RGB conversion on the GPU with the correct color matrix, GPU work overlapped with CPU work while preserving output order, and experimental zero-copy hardware-frame input (Linux/Vulkan). Ships a built-in effect catalog (`wgpu_filter::effects`) with typed parameters and live updates — color grading, beautify, sharpen/blur/pixelate, geometric transforms, lens/motion looks (soul, sway, wave, swirl, magnifier, fisheye) and green-screen chroma keying — no WGSL required. Successor to the deprecated `opengl` feature.
- **opengl:** *(deprecated, superseded by `wgpu`)* GPU-accelerated OpenGL filters. Requires a display connection and converts colors on the CPU; kept functional for existing users — see the `opengl` module docs for migration.
- **rtmp:** High-performance embedded RTMP server with native epoll/kqueue, O(1) GOP sharing, and 10,000+ concurrent connections on Linux/macOS (8,000 on Windows). In-process ingest with no TCP between FFmpeg and server.
- **subtitle:** Native ASS/SRT subtitle burn-in rendered by a pure-Rust engine inside the frame pipeline — independent of FFmpeg build flags (no `--enable-libass` needed, no system libass), with in-memory script input and explicit font-file control.
- **flv:** Provides support for FLV container parsing and handling.
- **async:** Adds asynchronous functionality (allowing you to `.await` operations).
- **static:** Enables static linking for FFmpeg libraries (via `ffmpeg-next/static`).

## License

ez-ffmpeg is licensed under your choice of the MIT, Apache-2.0, or MPL-2.0 licenses. You may select the license that best fits your needs.
**Important:** While ez-ffmpeg is freely usable, FFmpeg has its own licensing terms. Ensure that your use of its components complies with FFmpeg's license.