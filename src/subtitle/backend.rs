//! Renderer backend boundary.
//!
//! The frame filter consumes rendered subtitle overlays through
//! [`SubtitleRenderer`], keeping the rendering engine swappable and the
//! filter renderer-agnostic: everything downstream of this trait (color
//! conversion, pixel-format layout, blend kernels, timing) never sees the
//! engine. [`crate::subtitle::render::PureRenderer`] is the production
//! implementation.

use super::blend::OverlayImage;

/// One rendering session for one loaded subtitle track.
///
/// Configuration happens on the builder thread; [`Self::render_frame`] runs
/// on the pipeline pump thread after the whole value has been moved there.
/// Implementors are `Send` and never shared (`Sync` is deliberately not
/// required).
pub(crate) trait SubtitleRenderer: Send {
    /// Geometry of the frames that will be rendered onto.
    fn set_frame_size(&mut self, width: i32, height: i32);

    /// Geometry the subtitles were authored against (FFmpeg `original_size`
    /// storage-size semantics).
    fn set_storage_size(&mut self, width: i32, height: i32);

    /// Pixel-aspect compensation used together with the storage size.
    fn set_pixel_aspect(&mut self, par: f64);

    /// Renders the track at `now_ms`.
    ///
    /// The returned overlays are valid until the next `render_frame` call —
    /// they may borrow renderer-owned memory, which the `'_` borrow on
    /// `self` enforces at compile time. Implementations return only
    /// non-degenerate nodes (positive size, stride >= width, bitmap of at
    /// least `stride * (h - 1) + w` bytes).
    fn render_frame(&mut self, now_ms: i64) -> Vec<OverlayImage<'_>>;

    /// Deterministic teardown on the pump thread; `Drop` remains the
    /// idempotent backstop for early-abort paths.
    fn teardown(&mut self);
}
