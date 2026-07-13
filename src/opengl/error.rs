//! Typed error for constructing [`OpenGLFrameFilter`].
//!
//! [`OpenGLFrameFilter`]: crate::opengl::opengl_frame_filter::OpenGLFrameFilter

/// Errors from the [`OpenGLFrameFilter`] constructors (mirrors
/// `WgpuFilterError` for the wgpu feature; converts into
/// [`crate::error::Error::OpenGLFilter`]).
///
/// Runtime failures (shader compilation and linking, GL object creation,
/// frame upload and readback) surface through the pipeline as
/// [`FrameFilterError`](crate::filter::frame_filter::FrameFilterError)
/// instead — this type covers what can go wrong before a frame flows.
///
/// [`OpenGLFrameFilter`]: crate::opengl::opengl_frame_filter::OpenGLFrameFilter
#[derive(thiserror::Error, Debug)]
#[non_exhaustive]
pub enum OpenGLFilterError {
    /// A constructor option is invalid: the fragment shader passed to
    /// `new_simple` is missing the `in vec2 TexCoord;` input that the
    /// default vertex shader feeds.
    #[error("invalid OpenGL filter option: {0}")]
    InvalidOption(String),

    /// Surfman could not provide a GL device or context: no display
    /// connection, no usable adapter, or the requested GL version is
    /// unsupported.
    #[error("OpenGL context creation failed: {0}")]
    ContextCreation(String),
}
