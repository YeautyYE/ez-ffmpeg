use crate::core::filter::frame_filter::{FrameFilter, FrameFilterError, RequestFrameMode};
use crate::core::filter::frame_filter_context::FrameFilterContext;
use crate::filter::frame_pipeline_builder::FramePipelineBuilder;
use ffmpeg_sys_next::AVMediaType;
use std::any::Any;
use std::collections::HashMap;

/// Inert replacement for an arbitrary payload caught at a user filter hook.
/// The pipeline worker still unwinds so `ThreadDoneGuard` publishes
/// `Error::WorkerPanicked`, but the detached thread never owns user drop code.
#[derive(Debug)]
struct FrameFilterCallbackPanicked;

/// Inert replacement emitted after normal pipeline teardown observed one or
/// more panicking user destructors. Every user value has already been reclaimed
/// under its own catch before this payload is resumed.
#[derive(Debug)]
struct FramePipelineTeardownPanicked;

fn invoke_filter_callback<T>(callback: impl FnOnce() -> T) -> T {
    match std::panic::catch_unwind(std::panic::AssertUnwindSafe(callback)) {
        Ok(value) => value,
        Err(payload) => {
            crate::core::packet_sink::dispose_panic_payload(payload);
            std::panic::resume_unwind(Box::new(FrameFilterCallbackPanicked));
        }
    }
}

fn drop_user_value_contained<T>(value: T) -> bool {
    match std::panic::catch_unwind(std::panic::AssertUnwindSafe(move || drop(value))) {
        Ok(()) => false,
        Err(payload) => {
            crate::core::packet_sink::dispose_panic_payload(payload);
            true
        }
    }
}

/// Internally, we store each filter along with its name in a holder.
pub(crate) struct FilterHolder {
    name: String,
    filter: Box<dyn FrameFilter>,
}

/// A pipeline that processes frames by passing them through all filters in order.
/// It also stores an attribute map that filters can access/modify via `FrameFilterContext`.
pub struct FramePipeline {
    pub(crate) media_type: AVMediaType,
    pub(crate) stream_index: Option<usize>,

    pub(crate) filters: Vec<FilterHolder>,

    // Shared data among all filters
    attribute_map: HashMap<String, Box<dyn Any + Send>>,
}

impl FramePipeline {
    /// Creates a new pipeline for a given media type.
    /// All filters must match this type.
    pub fn new(media_type: AVMediaType, stream_index: Option<usize>) -> Self {
        Self {
            media_type,
            stream_index,
            filters: Vec::new(),
            attribute_map: HashMap::new(),
        }
    }

    /// Adds a filter to the pipeline. No dynamic removal is provided in this simplified approach.
    ///
    /// # Panics
    /// Panics if the filter's media type differs from the pipeline's.
    pub fn add_filter(&mut self, name: impl Into<String>, filter: Box<dyn FrameFilter>) {
        assert_eq!(self.media_type, filter.media_type());
        self.filters.push(FilterHolder {
            name: name.into(),
            filter,
        });
    }

    /// Allows external code to directly set an attribute. (Optional convenience)
    pub fn set_attribute<T: 'static + std::marker::Send>(
        &mut self,
        key: impl Into<String>,
        value: T,
    ) {
        self.attribute_map.insert(key.into(), Box::new(value));
    }

    /// Allows external code to retrieve an attribute by key.
    pub fn get_attribute<T: 'static>(&self, key: &str) -> Option<&T> {
        self.attribute_map
            .get(key)
            .and_then(|v| v.downcast_ref::<T>())
    }

    /// Initializes all filters in order.
    pub(crate) fn init_filters(&mut self) -> Result<(), FrameFilterError> {
        for holder in &mut self.filters {
            let mut ctx = FrameFilterContext::new(&holder.name, &mut self.attribute_map);
            invoke_filter_callback(|| holder.filter.init(&mut ctx))?;
        }
        Ok(())
    }

    /// Calls `uninit` on all filters (in the same order).
    /// (You can reverse the order if needed, but typically it's not strict.)
    pub(crate) fn uninit_filters(&mut self) {
        for holder in &mut self.filters {
            let mut ctx = FrameFilterContext::new(&holder.name, &mut self.attribute_map);
            invoke_filter_callback(|| holder.filter.uninit(&mut ctx));
        }
    }

    /// Pushes a frame through each filter in order. If any filter returns `None`,
    /// the frame is dropped. Otherwise, the final `Some(frame)` is returned.
    // The scheduler loop now routes through `run_filters_skipping` (the EOF
    // marker traversal must bypass flush-capped filters); this plain form
    // remains for the wgpu feature's tests and the attribute-map unit test —
    // all `#[cfg(test)]` code, so every non-test build allows the dead code.
    #[cfg_attr(not(test), allow(dead_code))]
    pub(crate) fn run_filters(
        &mut self,
        frame: ffmpeg_next::Frame,
    ) -> Result<Option<ffmpeg_next::Frame>, FrameFilterError> {
        self.run_filters_skipping(&[], frame)
    }

    /// `run_filters`, minus the filters whose index is marked in `skip`
    /// (indices past `skip`'s length are not skipped). The end-of-stream
    /// marker traversal uses this for filters whose flush drain hit the
    /// per-filter cap: such a filter already consumed its cue and its
    /// remaining backlog is discarded by contract — handing it the source
    /// marker would let it convert the marker into yet another real frame
    /// for filters that already consumed their own cue.
    pub(crate) fn run_filters_skipping(
        &mut self,
        skip: &[bool],
        mut frame: ffmpeg_next::Frame,
    ) -> Result<Option<ffmpeg_next::Frame>, FrameFilterError> {
        for (i, holder) in self.filters.iter_mut().enumerate() {
            if skip.get(i).copied().unwrap_or(false) {
                continue;
            }
            let mut ctx = FrameFilterContext::new(&holder.name, &mut self.attribute_map);
            match invoke_filter_callback(|| holder.filter.filter_frame(frame, &mut ctx))? {
                Some(f) => {
                    frame = f;
                }
                None => {
                    return Ok(None);
                }
            }
        }
        Ok(Some(frame))
    }

    // Used by the wgpu feature's tests; the pipeline loop now iterates
    // request_frame_indices() instead (PERF-8), so it is unused in a default build.
    #[allow(dead_code)]
    pub(crate) fn filter_len(&self) -> usize {
        self.filters.len()
    }

    /// Indices of filters whose `request_frame` may produce frames and so must
    /// be polled by the pipeline loop. Filters declaring
    /// [`RequestFrameMode::Never`] are omitted, letting an all-`Never` pipeline
    /// block on its input instead of polling no-op filters (PERF-8).
    pub(crate) fn request_frame_indices(&self) -> Vec<usize> {
        self.filters
            .iter()
            .enumerate()
            .filter(|(_, h)| {
                invoke_filter_callback(|| h.filter.request_frame_mode()) != RequestFrameMode::Never
            })
            .map(|(i, _)| i)
            .collect()
    }

    pub(crate) fn request_frame(
        &mut self,
        index: usize,
    ) -> Result<Option<ffmpeg_next::Frame>, FrameFilterError> {
        assert!(index < self.filters.len());
        let holder = &mut self.filters[index];
        let mut ctx = FrameFilterContext::new(&holder.name, &mut self.attribute_map);
        invoke_filter_callback(|| holder.filter.request_frame(&mut ctx))
    }

    /// Whether the filter at `index` currently reports deliverable (or
    /// in-flight) `request_frame` output — see
    /// [`FrameFilter::request_frame_pending`]. The pipeline loop derives its
    /// wait interval from this: the ~1ms poll cadence only while some polled
    /// filter reports pending output, the long idle interval otherwise.
    pub(crate) fn request_frame_pending_at(&self, index: usize) -> bool {
        assert!(index < self.filters.len());
        invoke_filter_callback(|| self.filters[index].filter.request_frame_pending())
    }

    /// Runs `filter_frame` on the single filter at `index`, returning ITS
    /// output without pushing it further down the chain. The end-of-stream
    /// flush uses this to hand each filter its cue exactly once — routing a
    /// released real frame onward is the caller's decision, and a passed-back
    /// marker must not cue the filters behind it out of order.
    pub(crate) fn run_filter_at(
        &mut self,
        index: usize,
        frame: ffmpeg_next::Frame,
    ) -> Result<Option<ffmpeg_next::Frame>, FrameFilterError> {
        assert!(index < self.filters.len());
        let holder = &mut self.filters[index];
        let mut ctx = FrameFilterContext::new(&holder.name, &mut self.attribute_map);
        invoke_filter_callback(|| holder.filter.filter_frame(frame, &mut ctx))
    }

    /// Passes the given `frame` through the filters starting at `start_index`.
    ///
    /// For example, if `start_index` is 2, we will call `filter_frame` on the 2nd filter,
    /// then the 3rd, and so on, up to the last filter in the pipeline. If any filter
    /// returns `None`, the frame is discarded and no further filters are called.
    ///
    /// # Parameters
    /// - `start_index`: The zero-based index of the filter from which to begin processing.
    /// - `frame`: The FFmpeg `Frame` to be processed.
    ///
    /// # Returns
    /// - `Ok(Some(frame))` if the frame is successfully processed by all remaining filters.
    /// - `Ok(None)` if any filter discards the frame by returning `None`.
    /// - `Err(e)` (a boxed [`FrameFilterError`]) if an error occurs in any filter.
    pub(crate) fn run_filters_from(
        &mut self,
        start_index: usize,
        mut frame: ffmpeg_next::Frame,
    ) -> Result<Option<ffmpeg_next::Frame>, FrameFilterError> {
        // If start_index is out of bounds, we can either return an error
        // or treat it as "no filters to run." Here we choose to check bounds explicitly.
        if start_index >= self.filters.len() {
            // No filters to run, so the frame passes through unchanged.
            return Ok(Some(frame));
        }

        // Iterate from `start_index` to the end of `self.filters`.
        for i in start_index..self.filters.len() {
            let holder = &mut self.filters[i];

            // Build a temporary context, giving the filter its name and the attribute map.
            let mut ctx = FrameFilterContext::new(&holder.name, &mut self.attribute_map);

            // Call `filter_frame` on the filter. If `None`, discard the frame and stop.
            match invoke_filter_callback(|| holder.filter.filter_frame(frame, &mut ctx))? {
                Some(f) => {
                    frame = f; // Continue to the next filter
                }
                None => {
                    // The filter has dropped this frame
                    return Ok(None);
                }
            }
        }

        // If we reach here, all remaining filters have produced Some(frame).
        Ok(Some(frame))
    }
}

impl Drop for FramePipeline {
    fn drop(&mut self) {
        let mut teardown_panicked = false;

        // A Vec/HashMap aggregate drop would destroy siblings during the first
        // unwind. Reclaim each erased user value independently so one hostile
        // destructor cannot make a later sibling run mid-unwind.
        for holder in self.filters.drain(..) {
            let FilterHolder { name, filter } = holder;
            drop(name);
            teardown_panicked |= drop_user_value_contained(filter);
        }
        for (key, value) in self.attribute_map.drain() {
            drop(key);
            teardown_panicked |= drop_user_value_contained(value);
        }

        // During a callback unwind, preserve the already-normalized callback
        // panic. On an otherwise normal exit, keep the existing contract that
        // a user teardown panic fails the worker as `WorkerPanicked`.
        if teardown_panicked && !std::thread::panicking() {
            std::panic::resume_unwind(Box::new(FramePipelineTeardownPanicked));
        }
    }
}

impl From<FramePipelineBuilder> for FramePipeline {
    fn from(pipeline: FramePipelineBuilder) -> Self {
        pipeline.build()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::core::filter::frame_filter::NoopFilter;
    use crate::core::scheduler::ffmpeg_scheduler::STATUS_RUN;
    use crate::util::thread_synchronizer::{ThreadDoneGuard, ThreadSynchronizer};
    use ffmpeg_next::Frame;
    use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};
    use std::sync::{Arc, Mutex};

    // Keeps the default request_frame_mode (MayProduce): a generator source.
    struct GeneratorFilter;
    impl FrameFilter for GeneratorFilter {
        fn media_type(&self) -> AVMediaType {
            AVMediaType::AVMEDIA_TYPE_VIDEO
        }
    }

    // PERF-8: a pipeline of only passthrough (Never) filters must report no
    // indices to poll, so the loop can block on input instead of spinning; a
    // producing filter must still be polled.
    #[test]
    fn request_frame_indices_skips_never_filters() {
        let media = AVMediaType::AVMEDIA_TYPE_VIDEO;

        let mut all_passthrough = FramePipeline::new(media, Some(0));
        all_passthrough.add_filter("noop0", Box::new(NoopFilter::new(media)));
        all_passthrough.add_filter("noop1", Box::new(NoopFilter::new(media)));
        assert!(
            all_passthrough.request_frame_indices().is_empty(),
            "an all-passthrough pipeline must not be polled"
        );

        let mut with_generator = FramePipeline::new(media, Some(0));
        with_generator.add_filter("noop", Box::new(NoopFilter::new(media)));
        with_generator.add_filter("gen", Box::new(GeneratorFilter));
        assert_eq!(
            with_generator.request_frame_indices(),
            vec![1],
            "only the producing filter (index 1) must be polled"
        );
    }

    // A filter that writes a shared attribute, and one that reads it back — proving
    // the &mut FrameFilterContext plumbing reaches user code, so an attribute set by
    // one filter is visible to a later filter in the same run.
    struct SetAttrFilter {
        media: AVMediaType,
    }
    impl FrameFilter for SetAttrFilter {
        fn media_type(&self) -> AVMediaType {
            self.media
        }
        fn filter_frame(
            &mut self,
            frame: Frame,
            ctx: &mut FrameFilterContext,
        ) -> Result<Option<Frame>, FrameFilterError> {
            ctx.set_attribute("shared_counter", 42i32);
            Ok(Some(frame))
        }
        fn request_frame_mode(&self) -> RequestFrameMode {
            RequestFrameMode::Never
        }
    }

    struct GetAttrFilter {
        media: AVMediaType,
        seen: Arc<Mutex<Option<i32>>>,
    }
    impl FrameFilter for GetAttrFilter {
        fn media_type(&self) -> AVMediaType {
            self.media
        }
        fn filter_frame(
            &mut self,
            frame: Frame,
            ctx: &mut FrameFilterContext,
        ) -> Result<Option<Frame>, FrameFilterError> {
            *self.seen.lock().unwrap() = ctx.get_attribute::<i32>("shared_counter").copied();
            Ok(Some(frame))
        }
        fn request_frame_mode(&self) -> RequestFrameMode {
            RequestFrameMode::Never
        }
    }

    // Before this change the FrameFilter hooks took `&FrameFilterContext`, so
    // `set_attribute` (which needs `&mut self`) was uncallable and the attribute API
    // was dead. With `&mut FrameFilterContext`, a value one filter writes must reach a
    // later filter through the shared pipeline map.
    #[test]
    fn ctx_attribute_written_by_one_filter_is_read_by_a_later_one() {
        let media = AVMediaType::AVMEDIA_TYPE_VIDEO;
        let seen = Arc::new(Mutex::new(None));

        let mut pipeline = FramePipeline::new(media, Some(0));
        pipeline.add_filter("setter", Box::new(SetAttrFilter { media }));
        pipeline.add_filter(
            "getter",
            Box::new(GetAttrFilter {
                media,
                seen: seen.clone(),
            }),
        );

        // SAFETY: `Frame::empty()` allocates a valid but buffer-less frame; the two
        // passthrough filters only forward it and never read its planes, so the absent
        // data buffers cause no undefined behavior here.
        let frame = unsafe { Frame::empty() };
        let out = pipeline
            .run_filters(frame)
            .expect("run_filters should succeed");
        assert!(out.is_some(), "both passthrough filters forward the frame");
        assert_eq!(
            *seen.lock().unwrap(),
            Some(42),
            "the getter must read the attribute the setter wrote via &mut ctx"
        );
    }

    #[derive(Clone, Copy)]
    enum PanickingHook {
        Init,
        Uninit,
        FilterFrame,
        RequestFrame,
        RequestFrameMode,
        RequestFramePending,
    }

    struct PayloadDropBomb(Arc<AtomicBool>);

    impl Drop for PayloadDropBomb {
        fn drop(&mut self) {
            self.0.store(true, Ordering::Release);
            panic!("test payload destructor panic");
        }
    }

    struct HookPanicFilter {
        hook: PanickingHook,
        payload_dropped: Arc<AtomicBool>,
    }

    impl HookPanicFilter {
        fn explode(&self) -> ! {
            std::panic::panic_any(PayloadDropBomb(self.payload_dropped.clone()));
        }
    }

    impl FrameFilter for HookPanicFilter {
        fn media_type(&self) -> AVMediaType {
            AVMediaType::AVMEDIA_TYPE_VIDEO
        }

        fn init(&mut self, _ctx: &mut FrameFilterContext) -> Result<(), FrameFilterError> {
            if matches!(self.hook, PanickingHook::Init) {
                self.explode();
            }
            Ok(())
        }

        fn filter_frame(
            &mut self,
            frame: Frame,
            _ctx: &mut FrameFilterContext,
        ) -> Result<Option<Frame>, FrameFilterError> {
            if matches!(self.hook, PanickingHook::FilterFrame) {
                self.explode();
            }
            Ok(Some(frame))
        }

        fn request_frame(
            &mut self,
            _ctx: &mut FrameFilterContext,
        ) -> Result<Option<Frame>, FrameFilterError> {
            if matches!(self.hook, PanickingHook::RequestFrame) {
                self.explode();
            }
            Ok(None)
        }

        fn request_frame_mode(&self) -> RequestFrameMode {
            if matches!(self.hook, PanickingHook::RequestFrameMode) {
                self.explode();
            }
            RequestFrameMode::MayProduce
        }

        fn request_frame_pending(&self) -> bool {
            if matches!(self.hook, PanickingHook::RequestFramePending) {
                self.explode();
            }
            true
        }

        fn uninit(&mut self, _ctx: &mut FrameFilterContext) {
            if matches!(self.hook, PanickingHook::Uninit) {
                self.explode();
            }
        }
    }

    fn hook_pipeline(hook: PanickingHook) -> (FramePipeline, Arc<AtomicBool>) {
        let payload_dropped = Arc::new(AtomicBool::new(false));
        let mut pipeline = FramePipeline::new(AVMediaType::AVMEDIA_TYPE_VIDEO, Some(0));
        pipeline.add_filter(
            "panic",
            Box::new(HookPanicFilter {
                hook,
                payload_dropped: payload_dropped.clone(),
            }),
        );
        (pipeline, payload_dropped)
    }

    fn assert_normalized_callback_panic(
        outcome: std::thread::Result<()>,
        payload_dropped: &AtomicBool,
    ) {
        let payload = outcome.expect_err("the selected filter hook must panic");
        assert!(
            payload.is::<FrameFilterCallbackPanicked>(),
            "the worker must receive only the inert callback-panic payload"
        );
        assert!(
            payload_dropped.load(Ordering::Acquire),
            "the original hostile payload must be disposed before normalization"
        );
    }

    #[test]
    fn every_worker_hook_normalizes_and_disposes_arbitrary_panic_payloads() {
        for hook in [
            PanickingHook::Init,
            PanickingHook::Uninit,
            PanickingHook::FilterFrame,
            PanickingHook::RequestFrame,
            PanickingHook::RequestFrameMode,
            PanickingHook::RequestFramePending,
        ] {
            let (mut pipeline, payload_dropped) = hook_pipeline(hook);
            let outcome = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| match hook {
                PanickingHook::Init => {
                    let _ = pipeline.init_filters();
                }
                PanickingHook::Uninit => pipeline.uninit_filters(),
                PanickingHook::FilterFrame => {
                    // SAFETY: the filter does not inspect the frame before panicking.
                    let _ = pipeline.run_filter_at(0, unsafe { Frame::empty() });
                }
                PanickingHook::RequestFrame => {
                    let _ = pipeline.request_frame(0);
                }
                PanickingHook::RequestFrameMode => {
                    let _ = pipeline.request_frame_indices();
                }
                PanickingHook::RequestFramePending => {
                    let _ = pipeline.request_frame_pending_at(0);
                }
            }));
            assert_normalized_callback_panic(outcome, &payload_dropped);
        }
    }

    struct DropBomb(Arc<AtomicBool>);

    impl Drop for DropBomb {
        fn drop(&mut self) {
            self.0.store(true, Ordering::Release);
            panic!("test user-state destructor panic");
        }
    }

    struct FilterDropBomb {
        _bomb: DropBomb,
    }

    impl FrameFilter for FilterDropBomb {
        fn media_type(&self) -> AVMediaType {
            AVMediaType::AVMEDIA_TYPE_VIDEO
        }
    }

    fn pipeline_with_drop_bombs(flags: &[Arc<AtomicBool>]) -> FramePipeline {
        let mut pipeline = FramePipeline::new(AVMediaType::AVMEDIA_TYPE_VIDEO, Some(0));
        pipeline.add_filter(
            "drop-filter-0",
            Box::new(FilterDropBomb {
                _bomb: DropBomb(flags[0].clone()),
            }),
        );
        pipeline.add_filter(
            "drop-filter-1",
            Box::new(FilterDropBomb {
                _bomb: DropBomb(flags[1].clone()),
            }),
        );
        pipeline.set_attribute("drop-attribute-0", DropBomb(flags[2].clone()));
        pipeline.set_attribute("drop-attribute-1", DropBomb(flags[3].clone()));
        pipeline
    }

    fn drop_flags() -> Vec<Arc<AtomicBool>> {
        (0..4).map(|_| Arc::new(AtomicBool::new(false))).collect()
    }

    fn assert_all_dropped(flags: &[Arc<AtomicBool>]) {
        assert!(
            flags.iter().all(|flag| flag.load(Ordering::Acquire)),
            "every filter box and attribute value must be reclaimed independently"
        );
    }

    #[test]
    fn normal_pipeline_teardown_disposes_siblings_then_propagates_one_inert_panic() {
        let flags = drop_flags();
        let pipeline = pipeline_with_drop_bombs(&flags);
        let outcome = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| drop(pipeline)));
        let payload = outcome.expect_err("panicking user teardown must remain observable");
        assert!(payload.is::<FramePipelineTeardownPanicked>());
        assert_all_dropped(&flags);
    }

    #[test]
    fn callback_unwind_contains_every_sibling_destructor_panic() {
        let flags = drop_flags();
        let (mut pipeline, payload_dropped) = hook_pipeline(PanickingHook::Init);
        let mut bombs = pipeline_with_drop_bombs(&flags);
        pipeline.filters.append(&mut bombs.filters);
        pipeline.attribute_map.extend(bombs.attribute_map.drain());
        drop(bombs);

        let outcome = std::panic::catch_unwind(std::panic::AssertUnwindSafe(move || {
            let _ = pipeline.init_filters();
        }));
        assert_normalized_callback_panic(outcome, &payload_dropped);
        assert_all_dropped(&flags);
    }

    fn run_as_tracked_worker(
        operation: impl FnOnce() + Send + 'static,
    ) -> crate::error::Result<()> {
        let sync = ThreadSynchronizer::new();
        let status = Arc::new(AtomicUsize::new(STATUS_RUN));
        let result = Arc::new(Mutex::new(None));
        sync.thread_start();
        let guard = ThreadDoneGuard::adopt(sync.clone(), status.clone(), result.clone());
        let worker = std::thread::spawn(move || {
            let _done = guard.activate();
            operation();
        });
        assert!(worker.join().is_err(), "the tracked worker must unwind");
        sync.wait_for_all_threads();
        let published = result
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .take()
            .expect("the worker panic must publish a scheduler result");
        published
    }

    #[test]
    fn normalized_callback_and_teardown_panics_still_publish_worker_panicked() {
        let (mut callback_pipeline, _) = hook_pipeline(PanickingHook::Init);
        let callback_result = run_as_tracked_worker(move || {
            let _ = callback_pipeline.init_filters();
        });
        assert!(matches!(
            callback_result,
            Err(crate::error::Error::WorkerPanicked(_))
        ));

        let flags = drop_flags();
        let teardown_pipeline = pipeline_with_drop_bombs(&flags);
        let teardown_result = run_as_tracked_worker(move || drop(teardown_pipeline));
        assert!(matches!(
            teardown_result,
            Err(crate::error::Error::WorkerPanicked(_))
        ));
        assert_all_dropped(&flags);
    }
}
