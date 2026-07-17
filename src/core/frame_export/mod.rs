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
//! BT.601. [`ColorPolicy::Force`] pins a specific matrix/range for all frames.
//!
//! # Audio (PCM) export
//!
//! [`SampleExtractor`] is the audio sibling: it decodes one input's audio into
//! owned, interleaved `f32` PCM — the shape whisper-rs / candle / ort consume.
//! Defaults preserve the source rate and layout; [`SampleExtractor::sample_rate`]
//! and [`SampleExtractor::channels`] opt into resample / downmix, and
//! [`SampleExtractor::for_whisper`] presets 16 kHz mono.
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
//! A run drives the normal scheduler (demux → decode → filtergraph → null
//! output) with the export sink mounted on the output frame pipeline. The
//! returned [`FrameIter`] is `Send` (consume it on a worker thread) and fused
//! (exactly one terminal error, then `None` forever). Dropping it early aborts
//! the run cleanly — teardown drops the receiver before aborting the scheduler,
//! which may block until in-flight FFmpeg calls return.
//!
//! # Non-goals (v1)
//!
//! Random access by index/timestamp, planar / >8-bit output, HDR tone mapping
//! (HDR input is a typed error), GPU-frame export without download, and Python
//! bindings are out of scope.

mod error;
mod frame;
mod iter;
mod options;
mod resolve;
mod sink;
mod video;

pub use error::FrameExportError;
pub use frame::VideoFrame;
pub use iter::FrameIter;
pub use options::{ColorPolicy, PixelLayout, Sampling, YuvMatrix, YuvRange};
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
