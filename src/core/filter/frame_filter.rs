use crate::core::filter::frame_filter_context::FrameFilterContext;
use ffmpeg_next::Frame;
use ffmpeg_sys_next::AVMediaType;

/// Declares whether a filter's [`request_frame`](FrameFilter::request_frame) can
/// produce frames on its own, so the pipeline knows whether it must poll it.
///
/// The default is [`MayProduce`](RequestFrameMode::MayProduce), which preserves
/// the historical behavior (every filter is polled). Filters that only ever
/// transform their input — passthroughs, metadata taps — should return
/// [`Never`](RequestFrameMode::Never): a pipeline whose filters are all `Never`
/// blocks on its input instead of waking ~1000×/sec to poll no-op filters
/// (PERF-8).
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum RequestFrameMode {
    /// `request_frame` never yields a frame during normal operation; the
    /// pipeline never polls this filter.
    Never,
    /// `request_frame` may yield frames independently of input — a generator
    /// source, or a filter releasing delayed / asynchronous output (e.g. the GPU
    /// pipeline). The pipeline polls this filter.
    MayProduce,
}

/// Error type returned by [`FrameFilter`] methods.
///
/// A boxed error trait object, so an implementation can propagate any error
/// with `?` or `.into()` (e.g. `return Err("bad config".into())`) instead of
/// being forced to construct this crate's private error enum. The pipeline
/// wraps it into [`Error::FrameFilterInit`](crate::error::Error::FrameFilterInit)
/// / `FrameFilterProcess` / `FrameFilterRequest`, preserving the source.
pub type FrameFilterError = Box<dyn std::error::Error + Send + Sync + 'static>;

pub trait FrameFilter: Send {
    /// Returns the media type this filter operates on.
    ///
    /// This is used to determine whether the filter is compatible with a specific media type
    /// (e.g., video, audio, etc.). Each filter should define the media type it supports.
    fn media_type(&self) -> AVMediaType;

    /// Initializes the filter.
    ///
    /// This method is called once when the filter is added to the pipeline and prepares
    /// the filter for processing. The `ctx` provides access to the `FrameFilterContext`,
    /// which includes the filter's name and its associated pipeline. The pipeline allows
    /// filters to set or retrieve attributes, enabling the sharing of information across
    /// filters dynamically.
    ///
    /// # Parameters
    /// - `ctx`: The context that provides metadata and dynamic modification capabilities.
    ///
    /// # Returns
    /// - `Ok(())` if initialization succeeds.
    /// - `Err(e)` (any [`FrameFilterError`]) if initialization fails.
    fn init(&mut self, ctx: &mut FrameFilterContext) -> Result<(), FrameFilterError> {
        log::debug!("Initializing filter:{}", ctx.name());
        Ok(())
    }

    /// Processes a single frame through the filter.
    ///
    /// This method applies the filter's logic to a given frame and optionally produces
    /// a new frame. The `ctx` provides access to the filter's metadata and allows dynamic
    /// pipeline modifications if needed. The pipeline allows filters to set or retrieve
    /// attributes, enabling the sharing of information across filters during processing.
    ///
    /// # Parameters
    /// - `frame`: The input frame to be processed.
    /// - `ctx`: The context that provides metadata and dynamic modification capabilities.
    ///
    /// # Returns
    /// - `Ok(Some(frame))` if the filter produces a new frame.
    /// - `Ok(None)` if no frame is produced.
    /// - `Err(e)` (any [`FrameFilterError`]) if processing fails.
    ///
    /// # Default
    /// The provided default returns `Ok(None)`, which the pipeline treats as
    /// "drop this frame and stop the chain" — so a filter that does NOT override
    /// `filter_frame` silently discards every frame. A pass-through filter must
    /// return `Ok(Some(frame))`; only a filter that genuinely consumes frames
    /// (e.g. a terminal sink) should rely on the dropping default. To forward an
    /// end-of-stream flush marker untouched, probe it with
    /// [`frame_is_eof_marker`](crate::util::ffmpeg_utils::frame_is_eof_marker).
    ///
    /// # In-place mutation
    /// The frame's data buffers are usually REFCOUNTED and shared (the
    /// decoder's frame pool, other consumers of the same source): mutating
    /// them in place corrupts data someone else still reads. Before any
    /// in-place edit, call
    /// [`make_frame_writable`](crate::util::ffmpeg_utils::make_frame_writable)
    /// (cheap when already exclusive, copies when shared) or probe with
    /// [`frame_is_writable`](crate::util::ffmpeg_utils::frame_is_writable).
    /// Building a fresh output frame (`av_frame_get_buffer` +
    /// `av_frame_copy_props`) needs neither.
    ///
    /// # End of stream
    /// At end of stream the pipeline flushes the chain as an ordered
    /// cascade: each filter receives a props-only marker (a valid frame
    /// with no data buffers) as its flush cue and is then drained dry via
    /// [`request_frame`], in chain order — so a filter sees its cue only
    /// AFTER every filter before it has fully drained through it, and no
    /// real frame follows a filter's own cue. Only then does end of stream
    /// propagate downstream. A filter that holds frames back must release
    /// ALL remaining output when its cue arrives:
    /// - a producing filter (GPU/asynchronous) should resolve its pending
    ///   work — blocking if necessary — so the following [`request_frame`]
    ///   drain can pull everything out before returning `None`;
    /// - a non-producing filter ([`RequestFrameMode::Never`]) that delays
    ///   frames may instead return its held frame from `filter_frame`,
    ///   consuming the marker; passing an unneeded marker through is
    ///   equally fine (the pipeline recycles it).
    ///
    /// A filter can still see more than one props-only frame per stream —
    /// e.g. a source-side EOF-timestamp marker traversing the already
    /// drained chain after the flush. Treat every marker as a flush point,
    /// never as a terminal signal or an error. As a safety net against
    /// runaway generators, the end-of-stream drain forwards at most 1024
    /// frames per filter; a filter that hits that cap has the rest of its
    /// backlog discarded and stops being polled (and sees no further
    /// markers) until the next real input frame arrives.
    ///
    /// [`request_frame`]: FrameFilter::request_frame
    fn filter_frame(
        &mut self,
        _frame: Frame,
        _ctx: &mut FrameFilterContext,
    ) -> Result<Option<Frame>, FrameFilterError> {
        Ok(None)
    }

    /// Requests a frame from the filter.
    ///
    /// This method is used to pull frames from the filter when needed. For example,
    /// some filters might generate frames independently of input frames. The context
    /// provides access to the pipeline, allowing filters to set or retrieve attributes
    /// dynamically during frame requests.
    ///
    /// # Parameters
    /// - `ctx`: The context that provides metadata and dynamic modification capabilities.
    ///
    /// # Returns
    /// - `Ok(Some(frame))` if the filter produces a frame.
    /// - `Ok(None)` if no frame is produced.
    /// - `Err(e)` (any [`FrameFilterError`]) if the request fails.
    fn request_frame(
        &mut self,
        _ctx: &mut FrameFilterContext,
    ) -> Result<Option<Frame>, FrameFilterError> {
        Ok(None)
    }

    /// Declares whether [`request_frame`](FrameFilter::request_frame) can produce
    /// frames autonomously. Returning [`RequestFrameMode::Never`] lets the
    /// pipeline stop polling this filter (PERF-8). The default preserves the
    /// historical always-polled behavior for third-party generator filters.
    fn request_frame_mode(&self) -> RequestFrameMode {
        RequestFrameMode::MayProduce
    }

    /// Cleans up the filter.
    ///
    /// This method is called when the filter is removed from the pipeline or when
    /// the pipeline is terminated. It allows the filter to release resources or perform
    /// any necessary cleanup. The context provides access to the pipeline, allowing
    /// filters to set or retrieve attributes for final updates or cleanup of shared state.
    ///
    /// # Parameters
    /// - `ctx`: The context that provides metadata and dynamic modification capabilities.
    fn uninit(&mut self, ctx: &mut FrameFilterContext) {
        log::debug!("Uninitialized filter:{}", ctx.name());
    }
}

pub struct NoopFilter {
    media_type: AVMediaType,
}

impl NoopFilter {
    pub fn new(media_type: AVMediaType) -> Self {
        Self { media_type }
    }
}

impl FrameFilter for NoopFilter {
    fn media_type(&self) -> AVMediaType {
        self.media_type
    }

    fn filter_frame(
        &mut self,
        frame: Frame,
        _ctx: &mut FrameFilterContext,
    ) -> Result<Option<Frame>, FrameFilterError> {
        Ok(Some(frame))
    }

    fn request_frame_mode(&self) -> RequestFrameMode {
        RequestFrameMode::Never
    }
}
