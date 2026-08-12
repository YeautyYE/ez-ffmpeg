use std::ffi::CStr;
use std::ptr::null_mut;

/// The [`FrameFilter`](frame_filter::FrameFilter) trait for writing custom
/// Rust frame-processing stages, plus its polling and error contracts.
pub mod frame_filter;
pub mod frame_filter_context;
/// [`FramePipeline`](frame_pipeline::FramePipeline): an ordered chain of
/// frame filters applied to one stream, post-decode or pre-encode.
pub mod frame_pipeline;
/// [`FramePipelineBuilder`](frame_pipeline_builder::FramePipelineBuilder) for
/// assembling a [`FramePipeline`](frame_pipeline::FramePipeline) from named
/// filters.
pub mod frame_pipeline_builder;

/// Retrieves a list of all filters recognized by FFmpeg.
///
/// This function iterates through all available filters in the FFmpeg filter system and collects their
/// names, descriptions, and associated flags into a vector of `FilterInfo` structs.
///
/// # Example
///
/// ```rust,ignore
/// let filters = get_filters();
/// for filter in filters {
///     println!("Filter: {} - {}", filter.name, filter.description);
/// }
/// ```
///
/// # Returns
/// A vector of `FilterInfo` structs representing all available filters.
pub fn get_filters() -> Vec<FilterInfo> {
    let mut filter_infos = Vec::new();

    let mut opaque = null_mut();
    loop {
        // SAFETY: av_filter_iterate keeps its iteration state in `opaque`,
        // which starts as null and is only ever passed back to the same
        // function. Every non-null return points at an entry of libavfilter's
        // static filter registry, valid for the life of the process. `name`
        // is always a NUL-terminated string in that same static data, while
        // `description` may be null (FFmpeg strips it via
        // NULL_IF_CONFIG_SMALL in CONFIG_SMALL builds), so it is checked
        // before being dereferenced.
        unsafe {
            let filter = ffmpeg_sys_next::av_filter_iterate(&mut opaque);
            if filter.is_null() {
                break;
            }

            let name = CStr::from_ptr((*filter).name).to_str().unwrap_or("unknown");
            let description = if (*filter).description.is_null() {
                ""
            } else {
                CStr::from_ptr((*filter).description).to_str().unwrap_or("")
            };
            let flags = ffmpeg_next::filter::Flags::from_bits_truncate((*filter).flags);

            filter_infos.push(FilterInfo {
                name: name.to_string(),
                description: description.to_string(),
                flags,
            });
        }
    }

    filter_infos
}

/// Represents metadata about a specific filter recognized by FFmpeg.
///
/// This struct consolidates information from the FFmpeg filter system into a single, user-friendly format.
/// It can be used to inspect properties of filters available in your FFmpeg build.
///
/// # Fields
/// * `name` - The name of the filter (e.g., `"scale"`, `"crop"`).
/// * `description` - A brief description of the filter's functionality.
#[derive(Clone, Debug)]
pub struct FilterInfo {
    /// The name of the filter.
    pub name: String,
    /// A brief description of the filter's functionality.
    pub description: String,
    /// The flags associated with the filter, indicating its capabilities and properties.
    pub flags: ffmpeg_next::filter::Flags,
}
