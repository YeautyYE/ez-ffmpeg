//! Typed errors for frame export.
//!
//! These surface either from `FrameExtractor::frames()` / `SampleExtractor::samples()`
//! (option validation and stream/duration/HDR resolution, before any worker
//! thread starts) or as the iterator's single terminal error (runtime failures).
//! They reach the public [`crate::error::Error`] through the
//! [`crate::error::Error::FrameExport`] variant.

use thiserror::Error;

/// Something wrong with a frame-export request or run.
#[derive(Error, Debug)]
#[non_exhaustive]
pub enum FrameExportError {
    /// The input has no video stream to export from.
    #[error("input has no video stream")]
    NoVideoStream,

    /// An explicit `video_stream_index` was out of range.
    #[error("video stream index {index} out of bounds: input has {count} stream(s)")]
    StreamIndexOutOfBounds {
        /// The requested (out-of-range) index.
        index: usize,
        /// The number of streams the input actually has.
        count: usize,
    },

    /// An explicit `video_stream_index` referred to a non-video stream.
    #[error("stream {index} is not a video stream")]
    NotAVideoStream {
        /// The requested index.
        index: usize,
    },

    /// `UniformN` needs a duration, and none could be resolved.
    #[error("UniformN needs a resolvable duration; use duration_hint_us() or EverySec instead")]
    UnknownDuration,

    /// The input is HDR (BT.2020 / PQ / HLG). Tone mapping is a documented
    /// non-goal of this module.
    #[error(
        "HDR input (BT.2020 / PQ / HLG) requires tone mapping, which frame export does not do yet"
    )]
    HdrRequiresToneMapping,

    /// `UniformN(n)` was requested but the video stream produced no frames.
    #[error("UniformN({n}) on a video stream that produced no frames")]
    EmptyVideoStream {
        /// The requested exact frame count.
        n: u32,
    },

    /// An option value was invalid (zero count, zero size, non-finite seconds, …).
    #[error("invalid frame-export option: {0}")]
    InvalidOption(String),

    /// The input has no audio stream to export from.
    #[error("input has no audio stream")]
    NoAudioStream,

    /// An explicit `audio_stream_index` was out of range.
    #[error("audio stream index {index} out of bounds: input has {count} stream(s)")]
    AudioStreamIndexOutOfBounds {
        /// The requested (out-of-range) index.
        index: usize,
        /// The number of streams the input actually has.
        count: usize,
    },

    /// An explicit `audio_stream_index` referred to a non-audio stream.
    #[error("stream {index} is not an audio stream")]
    NotAnAudioStream {
        /// The requested index.
        index: usize,
    },
}
