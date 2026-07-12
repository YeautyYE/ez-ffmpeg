//! Typed error for building and configuring [`WgpuFrameFilter`].
//!
//! [`WgpuFrameFilter`]: crate::wgpu_filter::WgpuFrameFilter

/// Errors from [`WgpuFrameFilterBuilder`] and the filter's configuration
/// surface (mirrors `SubtitleError` for the subtitle feature; converts
/// into [`crate::error::Error::WgpuFilter`]).
///
/// Runtime failures (device loss, oversized frames, unsupported formats)
/// surface through the pipeline as
/// [`FrameFilterError`](crate::filter::frame_filter::FrameFilterError)
/// instead — this type covers what can go wrong before a frame flows.
///
/// [`WgpuFrameFilterBuilder`]: crate::wgpu_filter::WgpuFrameFilterBuilder
#[derive(thiserror::Error, Debug)]
#[non_exhaustive]
pub enum WgpuFilterError {
    /// A builder option is invalid: missing shader, a `shader_yuv_wgsl`
    /// body violating its contract, a zero output dimension, a params
    /// struct with a bad size, or `frames_in_flight` out of range.
    #[error("invalid wgpu filter option: {0}")]
    InvalidOption(String),

    /// [`params_handle`](crate::wgpu_filter::WgpuFrameFilter::params_handle)
    /// was called with a type whose size does not match the value given to
    /// [`params`](crate::wgpu_filter::WgpuFrameFilterBuilder::params).
    #[error("params type mismatch: {0}")]
    ParamsTypeMismatch(String),
}
