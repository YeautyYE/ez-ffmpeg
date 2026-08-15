<p align="center">
  <img src="https://raw.githubusercontent.com/YeautyYE/ez-ffmpeg/main/logo.jpg" alt="Logo" width="300">
</p>

<div align="center">

[![Crates.io](https://img.shields.io/crates/v/ez-ffmpeg.svg)](https://crates.io/crates/ez-ffmpeg)
[![Documentation](https://img.shields.io/badge/docs.rs-ez--ffmpeg-blue)](https://docs.rs/ez-ffmpeg)
[![License: MIT/Apache-2.0/MPL-2.0](https://img.shields.io/badge/License-MIT%2FApache--2.0%2FMPL--2.0-brightgreen.svg)](https://github.com/YeautyYE/ez-ffmpeg/blob/main/LICENSE-APACHE)
[![Rust](https://img.shields.io/badge/Rust-%3E=1.80.0-orange)](https://www.rust-lang.org/)
[![FFmpeg](https://img.shields.io/badge/FFmpeg-7.1--8.x-blue)](https://ffmpeg.org)
[![CI](https://github.com/YeautyYE/ez-ffmpeg/actions/workflows/ci.yml/badge.svg?branch=main&event=push)](https://github.com/YeautyYE/ez-ffmpeg/actions/workflows/ci.yml?query=branch%3Amain+event%3Apush)

</div>


## Overview

**`ez-ffmpeg`** provides a **safe and ergonomic Rust interface for FFmpeg integration**, offering a familiar API that closely follows FFmpeg’s original logic and parameter structures.

This library:
- Exposes a safe public API; the internal FFmpeg FFI layer uses audited `unsafe` code
- Keeps the execution logic and parameter conventions as close to FFmpeg as possible
- Provides an intuitive and user-friendly API for media processing
- Supports custom Rust filters and flexible input/output handling
- Offers optional GPU-accelerated custom filters (wgpu) and a high-performance embedded RTMP server
- Ships one-shot recipes (thumbnails, GIF, HLS), typed detection/measurement APIs (black/silence/scene/loudness), and experimental frame/sample/packet export and WHIP/SRT streaming outputs (experimental APIs may change between minor releases) — see the [crate documentation](https://docs.rs/ez-ffmpeg) for details
- Documents an [HDR-to-SDR tone-mapping cookbook](https://docs.rs/ez-ffmpeg/latest/ez_ffmpeg/recipes/) (PQ/HLG detection, zscale/libplacebo chains that avoid the washed-out look, and FFmpeg 8 `scale` with `intent=perceptual` as the last fallback when neither is available) with a runnable `examples/hdr_to_sdr`

By abstracting the complexity of the raw C API, `ez-ffmpeg` simplifies configuring media pipelines, performing transcoding and filtering, and inspecting media streams.

The transcoding pipeline is ported from FFmpeg's own `fftools/ffmpeg` sources — same stage semantics, same function names. Migrating a CLI command? See the [CLI-to-API mapping](https://docs.rs/ez-ffmpeg/latest/ez_ffmpeg/#cli-to-api-mapping).

## Version Requirements

- **Rust:** Version 1.80.0 or higher. The optional `wgpu` feature requires Rust 1.85+.
- **FFmpeg:** Version 7.1 through 8.x (one build links either major).

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

#### Linux
```bash
# The libav*-dev packages are FFmpeg's development libraries.
# Needs FFmpeg 7.1+ (Ubuntu 24.04 ships 6.x — see docs/INSTALL.md).
sudo apt install pkg-config clang libavcodec-dev libavformat-dev \
    libavfilter-dev libavdevice-dev libavutil-dev libswscale-dev libswresample-dev
```

Static linking, building FFmpeg from source, and troubleshooting: see [docs/INSTALL.md](https://github.com/YeautyYE/ez-ffmpeg/blob/main/docs/INSTALL.md). Homebrew, apt, and vcpkg FFmpeg builds are often configured with `--enable-gpl`; inspect `ffmpeg -buildconf` / `avutil_license()` before shipping a closed-source binary. Closed-source integrators should review the [FFmpeg capability and licensing matrix](https://github.com/YeautyYE/ez-ffmpeg/blob/main/docs/INSTALL.md#ffmpeg-capability-and-licensing-matrix) before selecting an FFmpeg build.

### Adding the Dependency

Add **ez-ffmpeg** to your project by including it in your `Cargo.toml`:

```toml
[dependencies]
ez-ffmpeg = "0.17"
```

### Basic Usage

Below is a basic example to get you started. Create or update your `main.rs` with the following code:

It runs the equivalent of `ffmpeg -i input.mp4 -vf "hue=s=0" output.mov`.

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

Prefer to start from the command itself? With the `cli` feature, `cli::from_cli` parses a supported `ffmpeg` command into the same pipeline — `from_cli("ffmpeg -i input.mp4 -vf \"hue=s=0\" output.mov")?.start()?.wait()?` — and `cli::emit_rust_code` translates a command into builder code. Unsupported flags fail with explicit errors.

More examples can be found [here][examples].

[examples]: https://github.com/YeautyYE/ez-ffmpeg/tree/main/examples

## Features

**ez-ffmpeg** offers several optional features that can be enabled in your `Cargo.toml` as needed:

- **wgpu:** GPU-accelerated custom video filters (WGSL shaders, headless-capable).
- **rtmp:** Embedded RTMP server with native epoll/kqueue and in-process ingest.
- **flv:** Provides support for FLV container parsing and handling.
- **subtitle:** Native ASS/SRT subtitle burn-in rendered by a pure-Rust engine (no libass needed).
- **async:** Adds asynchronous functionality (allowing you to `.await` operations).
- **cli:** Strict ffmpeg command-line compatibility subset (run or translate supported commands).
- **http-input:** Single-resource HTTP(S) input via rustls (`HttpInput`). Opt-in; `Input::from("https://…")` still uses FFmpeg's own protocols.
- **static:** Enables static linking for FFmpeg libraries (via `ffmpeg-next/static`).
- **opengl:** *(deprecated, superseded by `wgpu`)* GPU-accelerated OpenGL filters.

## License

ez-ffmpeg is licensed under your choice of the MIT, Apache-2.0, or MPL-2.0 licenses. You may select the license that best fits your needs.
**Important:** While ez-ffmpeg is freely usable, FFmpeg has its own licensing terms. Ensure that your use of its components complies with FFmpeg's license.