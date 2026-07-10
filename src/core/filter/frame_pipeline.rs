use crate::core::filter::frame_filter::{FrameFilter, FrameFilterError, RequestFrameMode};
use crate::core::filter::frame_filter_context::FrameFilterContext;
use crate::filter::frame_pipeline_builder::FramePipelineBuilder;
use ffmpeg_sys_next::AVMediaType;
use std::any::Any;
use std::collections::HashMap;

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
            let ctx = FrameFilterContext::new(&holder.name, &mut self.attribute_map);
            holder.filter.init(&ctx)?;
        }
        Ok(())
    }

    /// Calls `uninit` on all filters (in the same order).
    /// (You can reverse the order if needed, but typically it's not strict.)
    pub(crate) fn uninit_filters(&mut self) {
        for holder in &mut self.filters {
            let ctx = FrameFilterContext::new(&holder.name, &mut self.attribute_map);
            holder.filter.uninit(&ctx);
        }
    }

    /// Pushes a frame through each filter in order. If any filter returns `None`,
    /// the frame is dropped. Otherwise, the final `Some(frame)` is returned.
    pub(crate) fn run_filters(
        &mut self,
        mut frame: ffmpeg_next::Frame,
    ) -> Result<Option<ffmpeg_next::Frame>, FrameFilterError> {
        for holder in &mut self.filters {
            let ctx = FrameFilterContext::new(&holder.name, &mut self.attribute_map);
            match holder.filter.filter_frame(frame, &ctx)? {
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
            .filter(|(_, h)| h.filter.request_frame_mode() != RequestFrameMode::Never)
            .map(|(i, _)| i)
            .collect()
    }

    pub(crate) fn request_frame(
        &mut self,
        index: usize,
    ) -> Result<Option<ffmpeg_next::Frame>, FrameFilterError> {
        assert!(index < self.filters.len());
        let holder = &mut self.filters[index];
        let ctx = FrameFilterContext::new(&holder.name, &mut self.attribute_map);
        holder.filter.request_frame(&ctx)
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
        let ctx = FrameFilterContext::new(&holder.name, &mut self.attribute_map);
        holder.filter.filter_frame(frame, &ctx)
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
            let ctx = FrameFilterContext::new(&holder.name, &mut self.attribute_map);

            // Call `filter_frame` on the filter. If `None`, discard the frame and stop.
            match holder.filter.filter_frame(frame, &ctx)? {
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

impl From<FramePipelineBuilder> for FramePipeline {
    fn from(pipeline: FramePipelineBuilder) -> Self {
        pipeline.build()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::core::filter::frame_filter::NoopFilter;

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
}
