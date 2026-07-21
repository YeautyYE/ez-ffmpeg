//! Frame & sample export for AI/CV — decode a video into packed RGB frames
//! (and audio into f32 PCM) in one pass, correctly.
//!
//! # Experimental (0.14)
//!
//! This module is **experimental**. Its API shape may be reshaped in 0.15;
//! within the 0.14.x line, patch releases will not break it. Correctness
//! defects (deadlocks, over-delivery, dropped errors, wrong strides, wrong
//! color) are **not** waived by this banner — they are release blockers.
//!
//! # What it does
//!
//! [`FrameExtractor`] runs a single decode → (optional resize) → RGB conversion
//! pass over one input and hands you owned, tightly packed [`VideoFrame`]s:
//!
//! ```no_run
//! use ez_ffmpeg::frame_export::{FrameExtractor, PixelLayout, Sampling};
//!
//! # fn main() -> Result<(), ez_ffmpeg::error::Error> {
//! // One frame per second, 224x224 RGB, for a CV pipeline.
//! for frame in FrameExtractor::new("input.mp4")
//!     .sampling(Sampling::EverySec(1.0))
//!     .width(224)
//!     .height(224)
//!     .pixel(PixelLayout::Rgb24)
//!     .frames()?
//! {
//!     let frame = frame?;
//!     // frame.as_bytes(): width*height*3 tightly packed bytes, top-down.
//!     let _ = (frame.width(), frame.height(), frame.pts_us());
//! }
//! # Ok(())
//! # }
//! ```
//!
//! # Color correctness (the wedge)
//!
//! [`ColorPolicy::Tagged`] (the default) honors the frame's own colorspace tags
//! when converting YUV → RGB, so BT.709 (HD) content is not silently decoded as
//! BT.601. [`ColorPolicy::TaggedOrResolutionGuess`] additionally fills in
//! UNTAGGED frames with a per-frame resolution guess (height ≥ 720 → BT.709,
//! else BT.601) without ever overriding real tags. [`ColorPolicy::Force`] pins
//! a specific matrix/range for all frames.
//!
//! # Conversion precision & throughput
//!
//! The conversion stage (any resize plus the pixel-format conversion, one
//! swscale pass) runs the FFmpeg CLI's default swscale configuration
//! ([`ConversionPrecision::Standard`]): an all-frames export runs in the same
//! throughput class as an `ffmpeg -vf "scale=..,format=.."` run with default
//! flags — on typical sources the decode, not the conversion, is the
//! bottleneck — and produces the same bytes as that CLI run when both link
//! the same libswscale build (asserted end-to-end by a golden test).
//! [`FrameExtractor::conversion_precision`] opts into
//! [`ConversionPrecision::High`] (accurate rounding + full chroma
//! interpolation) when the last bit of precision matters more than
//! throughput, at a several-fold conversion cost. The precision tier never
//! changes color interpretation — matrix and range handling are identical in
//! both tiers.
//!
//! Migrating from v0.14.0: that release ran the conversion with the
//! High-equivalent flags (`accurate_rnd+full_chroma_int`) unconditionally —
//! there was no precision knob — so the `Standard` default produces
//! different bytes than v0.14.0 on chroma edges and rounded values. Select
//! [`ConversionPrecision::High`] to reproduce v0.14.0 output (byte-identical
//! when linked against the same libswscale build); `Standard` instead
//! matches the ffmpeg CLI's default conversion path.
//!
//! # Audio (PCM) export
//!
//! [`SampleExtractor`] is the audio sibling: it decodes one input's audio into
//! owned, interleaved `f32` PCM — the buffer layout whisper-rs / candle / ort
//! consume. Defaults preserve the source rate and channel layout (models that
//! expect a fixed shape need explicit normalization):
//! [`SampleExtractor::sample_rate`] and [`SampleExtractor::channels`] opt into
//! resample / channel conversion, and [`SampleExtractor::for_whisper`] presets
//! the 16 kHz mono that speech models expect.
//!
//! ```no_run
//! use ez_ffmpeg::frame_export::SampleExtractor;
//!
//! # fn main() -> Result<(), ez_ffmpeg::error::Error> {
//! // 16 kHz mono f32, ready to hand to an ASR model.
//! let pcm: Vec<f32> = SampleExtractor::for_whisper("input.mp4").collect_samples()?;
//! # let _ = pcm;
//! # Ok(())
//! # }
//! ```
//!
//! # Threading & teardown
//!
//! A run drives the normal scheduler (demux → decode → input frame pipeline →
//! filtergraph → null output) with the export sink mounted on the output frame
//! pipeline. Every run carries a dedicated input-pipeline thread (the
//! per-frame HDR guard / color stamp, plus the `UniformN` sampler when used)
//! between the decoder and the filtergraph, connected by a small bounded
//! channel hop. The returned [`FrameIter`] is `Send` (consume it on a worker
//! thread) and fused (exactly one terminal error, then `None` forever).
//! Dropping it early aborts the run cleanly — teardown drops the receiver
//! before aborting the scheduler, which may block until in-flight FFmpeg calls
//! return.
//!
//! # Non-goals (v1)
//!
//! Random access by index/timestamp, planar / >8-bit output, HDR tone mapping
//! (HDR input is a typed error — declared HDR fails at open time from stream
//! parameters, and decoded frames are re-checked at runtime, so a mid-stream
//! splice to HDR surfaces the same typed error instead of wrong colors; on
//! inputs with multiple video streams, set `video_stream_index` to keep that
//! runtime guard on the exported stream), GPU-frame export without download,
//! and Python bindings are out of scope.

mod error;
mod frame;
mod guard;
mod iter;
mod options;
mod resolve;
mod sampler;
mod selector;
mod sink;
mod video;

#[cfg(test)]
mod color_goldens;

pub use error::FrameExportError;
pub use frame::VideoFrame;
pub use iter::FrameIter;
pub use options::{ColorPolicy, ConversionPrecision, PixelLayout, Sampling, YuvMatrix, YuvRange};
pub use video::FrameExtractor;

// --- Audio (PCM) sample export ---
mod audio;
mod audio_iter;
mod audio_options;
mod audio_resolve;
mod audio_sink;
mod chunk;

pub use audio::SampleExtractor;
pub use audio_iter::SampleIter;
pub use audio_options::Channels;
pub use chunk::AudioChunk;
