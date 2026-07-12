// In test builds, libtest-generated code references test items through the
// deprecated `opengl` module path, which a file-level allow cannot cover.
#![cfg_attr(test, allow(deprecated))]
// Safety-hygiene lint (clippy-only; does not affect normal builds). Every
// public `unsafe fn` must document its contract with a `# Safety` section.
// Broader gates (`clippy::undocumented_unsafe_blocks`, `unsafe_op_in_unsafe_fn`)
// are deferred until the pre-existing unsafe-doc/import backlog is paid down.
#![warn(clippy::missing_safety_doc)]

//! # ez-ffmpeg
//!
//! **ez-ffmpeg** provides a safe and ergonomic Rust interface for [FFmpeg](https://ffmpeg.org)
//! integration. By abstracting away much of the raw C API complexity,
//! It abstracts the complexity of the raw C API, allowing you to configure media pipelines,
//! perform transcoding and filtering, and inspect streams with ease.
//!
//! ## Crate Layout
//!
//! - **`core`**: The foundational module that contains the main building blocks for configuring
//!   and running FFmpeg pipelines. This includes:
//!   - `Input` / `Output`: Descriptors for where media data comes from and goes to (files, URLs,
//!     custom I/O callbacks, etc.).
//!   - `FilterComplex` and [`FrameFilter`](filter::frame_filter::FrameFilter): Mechanisms for applying FFmpeg filter graphs or
//!     custom transformations.
//!   - `container_info`: Utilities to extract information about the container, such as duration and format details.
//!   - `stream_info`: Utilities to query media metadata (duration, codecs, etc.).
//!   - `hwaccel`: Helpers for enumerating and configuring hardware-accelerated video codecs
//!     (CUDA, VAAPI, VideoToolbox, etc.).
//!   - `codec`: Tools to list and inspect available encoders/decoders.
//!   - `device`: Utilities to discover system cameras, microphones, and other input devices.
//!   - `filter`: Query FFmpeg's built-in filters and infrastructure for building custom frame-processing filters.
//!   - `context`: Houses [`FfmpegContext`] for assembling an FFmpeg job.
//!   - `scheduler`: Provides [`FfmpegScheduler`] which manages the lifecycle of that job.
//!
//! - **`wgpu_filter`** (feature `"wgpu"`): GPU-accelerated frame filters via wgpu
//!   (Vulkan/Metal/DX12/GL). Provide a WGSL fragment shader and apply effects with
//!   correct color handling, headless operation, and GPU/CPU overlap.
//!
//! - **`opengl`** (feature `"opengl"`, deprecated): The former OpenGL filter path,
//!   superseded by `wgpu_filter`. Kept for backward compatibility; it requires a
//!   display connection and will be removed in a future major release.
//!
//! - **`rtmp`** (feature `"rtmp"`): Embedded RTMP server `EmbedRtmpServer` built for production streaming,
//!   using native epoll/kqueue/WSAPoll via libc FFI (edge-triggered on Linux/macOS, level-triggered on Windows),
//!   zero-copy GOP fanout with `Arc<[FrameData]>`, and tiered backpressure (1/2/4MB) on a 2-thread model;
//!   10,000+ conns on Linux/macOS (8,000 on Windows) with in-process ingest (no TCP between FFmpeg and server).
//!
//! - **`flv`** (feature `"flv"`): Provides data structures and helpers for handling FLV
//!   containers, useful if you’re working with RTMP or other FLV-based workflows.
//!
//! - **`subtitle`** (feature `"subtitle"`): Burns ASS/SRT subtitles onto video frames inside
//!   the frame pipeline with a pure-Rust renderer — independent of whether the linked FFmpeg
//!   was built with `--enable-libass`. Accepts subtitle files or in-memory scripts and
//!   explicit font files.
//!
//! ## Basic Usage
//!
//! For a simple pipeline, you typically do the following:
//!
//! 1. Build a [`FfmpegContext`] by specifying at least one [input](Input)
//!    and one [output](Output). Optionally, add filter descriptions
//!    (`filter_desc`) or attach [`FrameFilter`](filter::frame_filter::FrameFilter) pipelines at either the input (post-decode)
//!    or the output (pre-encode) stage.
//! 2. Create an [`FfmpegScheduler`] from that context, then call `start()` and `wait()` (or `.await`
//!    if you enable the `"async"` feature) to run the job.
//!
//! ```rust,ignore
//! use ez_ffmpeg::FfmpegContext;
//! use ez_ffmpeg::FfmpegScheduler;
//!
//! fn main() -> Result<(), Box<dyn std::error::Error>> {
//!     // 1. Build the FFmpeg context
//!     let context = FfmpegContext::builder()
//!         .input("input.mp4")
//!         .filter_desc("hue=s=0") // Example filter: desaturate
//!         .output("output.mov")
//!         .build()?;
//!
//!     // 2. Run it via FfmpegScheduler (sync mode)
//!     let result = FfmpegScheduler::new(context)
//!         .start()?
//!         .wait();
//!     result?; // If any error occurred, propagate it
//!     Ok(())
//! }
//! ```
//!
//! ## Feature Flags
//!
//! **`ez-ffmpeg`** uses Cargo features to provide optional functionality. By default, no optional
//! features are enabled, allowing you to keep dependencies minimal. You can enable features as needed
//! in your `Cargo.toml`:
//!
//! ```toml
//! [dependencies.ez-ffmpeg]
//! version = "*"
//! features = ["wgpu", "rtmp", "flv", "async"]
//! ```
//!
//! ### Core Features
//!
//! - **`wgpu`**: Enables wgpu-based GPU filters (WGSL shaders, headless-capable).
//! - **`opengl`** (deprecated): Enables the former OpenGL-based filters; superseded by `wgpu`.
//! - **`rtmp`**: Embedded RTMP server tuned for scale (10,000+ conns on Linux/macOS, 8,000 on Windows),
//!   native epoll/kqueue/WSAPoll IO (edge-triggered on Linux/macOS), zero-copy GOP, and in-process ingest
//!   that avoids TCP between FFmpeg and server.
//! - **`flv`**: Adds FLV container parsing and handling.
//! - **`subtitle`**: Native ASS/SRT subtitle burn-in rendered in pure Rust — no system
//!   libraries beyond FFmpeg itself (see the `subtitle` module docs).
//! - **`async`**: Adds asynchronous functionality: [`FfmpegScheduler`] additionally implements
//!   `Future`, so a running scheduler can be `.await`ed as a non-blocking alternative to the
//!   always-available synchronous `wait()`.
//! - **`static`**: Uses static linking for FFmpeg libraries (via `ffmpeg-next/static`).
//!
//! ## Relationship to the FFmpeg CLI
//!
//! The transcoding pipeline (demux -> decode -> filter -> encode -> mux) is
//! ported from the FFmpeg CLI sources, `fftools/ffmpeg` of **FFmpeg 7.x**:
//! function names, timestamp handling and scheduling semantics follow that
//! release, and code comments cite the corresponding fftools file and line
//! (line numbers refer to the FFmpeg `n7.1` tag).
//! If you know `ffmpeg_demux.c` or `ffmpeg_filter.c`, grepping this crate
//! for the same function names (`ts_fixup`, `video_sync_process`,
//! `enc_open`, `mux_fixup_ts`, ...) lands in the equivalent Rust.
//!
//! Bitstream filters (`-bsf:v/-bsf:a/-bsf:s`) are supported through
//! [`Output::set_video_bsf`](crate::core::context::output::Output::set_video_bsf)
//! and its audio/subtitle siblings (single filter or comma-separated chain).
//!
//! Not every CLI feature is implemented. Notable gaps: progress/stats
//! reporting (`-progress`), sub2video (rendering bitmap subtitles into
//! video), `-fix_sub_duration`, and two-pass encoding. Unsupported paths
//! fail with explicit errors rather than approximations.
//!
//! ## Logging
//!
//! FFmpeg's own diagnostics (av_log) are redirected into the Rust `log`
//! facade under the [`FFMPEG_LOG_TARGET`] target. Without a logger installed
//! (env_logger, tracing-log, ...) all FFmpeg messages are silently dropped —
//! including decoder errors that explain a failing job. Use
//! [`set_ffmpeg_log_level`] to bound the forwarded verbosity and
//! `Input::set_log_level_offset` to shift it per input.
//!
//! ## License Notice
//!
//! ez-ffmpeg is licensed under your choice of MIT, Apache-2.0, or MPL-2.0
//! (matching the `license` field in Cargo.toml).
//!
//! **Note:** FFmpeg itself is subject to its own licensing terms. When enabling features that incorporate FFmpeg components,
//! please ensure that your usage complies with FFmpeg's license.

pub mod core;
pub mod error;
pub mod util;

/// Internal RAII wrappers concentrating raw FFmpeg FFI pointers (Rung-2 boundary).
pub(crate) mod raw;

pub use self::core::analysis;
pub use self::core::codec;
pub use self::core::container_info;
pub use self::core::context::ffmpeg_context::FfmpegContext;
pub use self::core::context::input::Input;
pub use self::core::context::output::Output;
pub use self::core::device;
pub use self::core::filter;
pub use self::core::hwaccel;
pub use self::core::packet_scanner;
pub use self::core::recipes;
pub use self::core::scheduler::ffmpeg_scheduler::FfmpegScheduler;
pub use self::core::stream_info;
pub use self::core::{set_ffmpeg_log_level, FfmpegLogLevel, FFMPEG_LOG_TARGET};

// ez-ffmpeg is a thin FFmpeg wrapper, so FFmpeg's core types appear in the public
// API by design (e.g. `StreamInfo` carries an `AVCodecID`, an audio stream an
// `AVChannelOrder`, `Output::set_audio_sample_fmt` takes an `AVSampleFormat`, and a
// filter's info carries `filter::Flags`). They are re-exported here so downstream
// code can name them via `ez_ffmpeg::` without a direct `ffmpeg-next` /
// `ffmpeg-sys-next` dependency. Feature-specific ecosystem types (`bytes` for `flv`,
// `bytemuck`/`glow` for the GPU features) are intentionally left exposed to callers
// already working in those ecosystems.
pub use ffmpeg_next::filter::Flags as FilterFlags;
pub use ffmpeg_next::Frame;
pub use ffmpeg_sys_next::AVChannelOrder;
pub use ffmpeg_sys_next::AVCodecID;
pub use ffmpeg_sys_next::AVHWDeviceType;
pub use ffmpeg_sys_next::AVMediaType;
pub use ffmpeg_sys_next::AVRational;
pub use ffmpeg_sys_next::AVSampleFormat;

#[cfg(feature = "opengl")]
#[deprecated(
    since = "0.11.0",
    note = "the OpenGL filter path is superseded by `wgpu_filter` (feature \"wgpu\"): it needs a \
            display connection and converts colors on the CPU; see the module docs for migration"
)]
pub mod opengl;
#[cfg(feature = "opengl")]
use surfman::declare_surfman;
#[cfg(feature = "opengl")]
declare_surfman!();

#[cfg(feature = "wgpu")]
pub mod wgpu_filter;

#[cfg(feature = "rtmp")]
pub mod rtmp;

#[cfg(feature = "flv")]
pub mod flv;

#[cfg(feature = "subtitle")]
pub mod subtitle;
