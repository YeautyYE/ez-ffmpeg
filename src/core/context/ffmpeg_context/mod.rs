use crate::core::context::demuxer::{CopyMuxHandle, Demuxer};
use crate::core::context::ffmpeg_context_builder::FfmpegContextBuilder;
use crate::core::context::filter_complex::FilterComplex;
use crate::core::context::filter_graph::FilterGraph;
use crate::core::context::input::Input;
use crate::core::context::input_filter::{InputFilter, IFILTER_FLAG_AUTOROTATE};
use crate::core::context::muxer::Muxer;
use crate::core::context::output::{Output, StreamMap};
use crate::core::context::output_filter::{
    OutputFilter, OFILTER_FLAG_AUDIO_24BIT, OFILTER_FLAG_AUTOSCALE, OFILTER_FLAG_DISABLE_CONVERT,
};
use crate::core::context::{frame_alloc, CodecContext};
use crate::core::metadata::StreamSpecifier;
use crate::core::scheduler::ffmpeg_scheduler;
use crate::core::scheduler::ffmpeg_scheduler::{FfmpegScheduler, Initialization};
#[cfg(not(docsrs))]
use crate::core::scheduler::filter_task::graph_opts_apply;
use crate::core::scheduler::input_controller::SchNode;
use crate::error::Error::{
    FileSameAsInput, FilterDescUtf8, FilterNameUtf8, FilterZeroInputs, FilterZeroOutputs,
    FrameFilterStreamTypeNoMatched, FrameFilterTypeNoMatched, ParseInteger,
};
use crate::error::FilterGraphParseError::{
    InvalidFileIndexInFg, InvalidFilterSpecifier, OutputUnconnected,
};
use crate::error::{
    AllocOutputContextError, FilterGraphOperationError, FilterGraphParseError, FindStreamError,
    OpenInputError, OpenOutputError,
};
use crate::error::{Error, Result};
use crate::filter::frame_pipeline::FramePipeline;
use crate::hwaccel::{hw_device_for_filter, init_filter_hw_device};
use crate::util::ffmpeg_utils::{hashmap_to_avdictionary, DictGuard};
#[cfg(not(docsrs))]
use ffmpeg_sys_next::AVChannelOrder::AV_CHANNEL_ORDER_UNSPEC;
#[cfg(not(docsrs))]
use ffmpeg_sys_next::AVCodecConfig::*;
use ffmpeg_sys_next::AVCodecID::{AV_CODEC_ID_AC3, AV_CODEC_ID_MP3, AV_CODEC_ID_NONE};
use ffmpeg_sys_next::AVColorRange::AVCOL_RANGE_UNSPECIFIED;
use ffmpeg_sys_next::AVColorSpace::AVCOL_SPC_UNSPECIFIED;
use ffmpeg_sys_next::AVMediaType::{
    AVMEDIA_TYPE_ATTACHMENT, AVMEDIA_TYPE_AUDIO, AVMEDIA_TYPE_DATA, AVMEDIA_TYPE_SUBTITLE,
    AVMEDIA_TYPE_VIDEO,
};
use ffmpeg_sys_next::AVPixelFormat::AV_PIX_FMT_NONE;
use ffmpeg_sys_next::AVSampleFormat::AV_SAMPLE_FMT_NONE;
use ffmpeg_sys_next::{
    av_add_q, av_channel_layout_default, av_codec_get_id, av_codec_get_tag2, av_freep,
    av_get_exact_bits_per_sample, av_get_pix_fmt, av_guess_codec, av_guess_format,
    av_guess_frame_rate, av_inv_q, av_malloc, av_rescale_q, av_seek_frame, avcodec_alloc_context3,
    avcodec_descriptor_get, avcodec_descriptor_get_by_name, avcodec_find_encoder,
    avcodec_find_encoder_by_name, avcodec_get_name, avcodec_parameters_from_context,
    avcodec_parameters_to_context, avfilter_pad_get_name, avfilter_pad_get_type,
    avformat_alloc_context, avformat_alloc_output_context2, avformat_close_input,
    avformat_find_stream_info, avformat_flush, avformat_free_context, avformat_open_input,
    avio_alloc_context, AVCodec, AVCodecID, AVColorRange, AVColorSpace, AVFilterContext,
    AVFilterInOut, AVFilterPad, AVFormatContext, AVMediaType, AVOutputFormat, AVPixelFormat,
    AVRational, AVSampleFormat, AVStream, AVERROR_ENCODER_NOT_FOUND, AVFMT_FLAG_CUSTOM_IO,
    AVFMT_GLOBALHEADER, AVFMT_NOBINSEARCH, AVFMT_NOFILE, AVFMT_NOGENSEARCH, AVFMT_NOSTREAMS,
    AVSEEK_FLAG_BACKWARD, AV_CODEC_PROP_BITMAP_SUB, AV_CODEC_PROP_TEXT_SUB, AV_TIME_BASE,
};
#[cfg(not(docsrs))]
use ffmpeg_sys_next::{
    av_buffer_ref, av_channel_layout_copy, av_packet_side_data_new, avcodec_get_supported_config,
    avfilter_graph_segment_apply, avfilter_graph_segment_create_filters,
    avfilter_graph_segment_free, avfilter_graph_segment_parse, AVChannelLayout,
    AVFILTER_FLAG_HWDEVICE,
};
#[cfg(not(docsrs))]
use ffmpeg_sys_next::{
    avformat_query_codec, AVSTREAM_EVENT_FLAG_NEW_PACKETS, AV_DISPOSITION_ATTACHED_PIC,
    AV_DISPOSITION_DEFAULT,
};
use log::{debug, error, info, warn};
use std::collections::HashMap;
use std::ffi::{c_uint, c_void, CStr, CString};
use std::ptr::{null, null_mut};
use std::sync::Arc;

mod fg_bind;
mod open_input;
mod open_output;
mod opt_util;

/// Log target held stable across the module split so target-based log
/// filtering and routing keep observing `ez_ffmpeg::core::context::ffmpeg_context`.
const LOG_TARGET: &str = module_path!();

use fg_bind::{fg_bind_inputs, init_filter_graphs};
use open_input::open_input_files;
use open_output::open_output_files;
use opt_util::outputs_bind;

pub(super) use open_input::InputOpaque;
pub(super) use open_output::OutputOpaque;

// Re-exported only so the `#[cfg(test)] mod tests` below can name these
// relocated items via the crate-absolute `ffmpeg_context::…` path it already used.
#[cfg(test)]
use fg_bind::{bind_fg_inputs_by_fg, fg_complex_bind_input};
#[cfg(test)]
use open_input::{read_packet_wrapper, seek_input_packet_wrapper};
#[cfg(test)]
use open_output::write_packet_wrapper;

pub struct FfmpegContext {
    pub(crate) independent_readrate: bool,
    pub(crate) demuxs: Vec<Demuxer>,
    pub(crate) filter_graphs: Vec<FilterGraph>,
    pub(crate) muxs: Vec<Muxer>,
    // Created at build time so the AVIO interrupt callbacks installed on the
    // input/output contexts observe the same atomic the scheduler drives.
    pub(crate) scheduler_status: Arc<std::sync::atomic::AtomicUsize>,
    pub(crate) interrupt_state: Arc<crate::core::context::InterruptState>,
}

// SAFETY: FfmpegContext can be sent to another thread because all its fields
// are either Send or wrapped in thread-safe containers. The raw FFmpeg pointers
// are only accessed from the thread that owns the FfmpegContext.
// Note: FfmpegContext is NOT Sync because it contains non-Sync fields like
// Box<dyn FrameFilter> which only implements Send.
unsafe impl Send for FfmpegContext {}

impl FfmpegContext {
    /// Creates a new [`FfmpegContextBuilder`] which allows you to configure
    /// and construct an [`FfmpegContext`] with custom inputs, outputs, filters,
    /// and other parameters.
    ///
    /// # Examples
    /// ```rust,ignore
    /// let context = FfmpegContext::builder()
    ///     .input("input.mp4")
    ///     .output("output.mp4")
    ///     .build()
    ///     .unwrap();
    /// ```
    pub fn builder() -> FfmpegContextBuilder {
        FfmpegContextBuilder::new()
    }

    /// Consumes this [`FfmpegContext`] and starts an FFmpeg job, returning
    /// an [`FfmpegScheduler<ffmpeg_scheduler::Running>`] for further management.
    ///
    /// Internally, this method creates an [`FfmpegScheduler`] from the context
    /// and immediately calls [`FfmpegScheduler::start()`].
    ///
    /// # Returns
    /// - `Ok(FfmpegScheduler<Running>)` if the scheduling process started successfully.
    /// - `Err(...)` if there was an error initializing or starting FFmpeg.
    ///
    /// # Example
    /// ```rust,ignore
    /// let context = FfmpegContext::builder()
    ///     .input("input.mp4")
    ///     .output("output.mp4")
    ///     .build()
    ///     .unwrap();
    ///
    /// // Start the FFmpeg job and get a scheduler to manage it
    /// let scheduler = context.start().expect("Failed to start Ffmpeg job");
    ///
    /// // Optionally, wait for it to finish
    /// let result = scheduler.wait();
    /// assert!(result.is_ok());
    /// ```
    pub fn start(self) -> Result<FfmpegScheduler<ffmpeg_scheduler::Running>> {
        let ffmpeg_scheduler = FfmpegScheduler::new(self);
        ffmpeg_scheduler.start()
    }

    #[allow(dead_code)]
    pub(crate) fn new(
        inputs: Vec<Input>,
        filter_complexs: Vec<FilterComplex>,
        outputs: Vec<Output>,
    ) -> Result<FfmpegContext> {
        Self::new_with_options(false, inputs, filter_complexs, outputs, false, Vec::new())
    }

    pub(crate) fn new_with_options(
        mut independent_readrate: bool,
        mut inputs: Vec<Input>,
        mut filter_complexs: Vec<FilterComplex>,
        mut outputs: Vec<Output>,
        copy_ts: bool,
        deferred_filter_descs: Vec<
            crate::core::context::ffmpeg_context_builder::DeferredFilterDesc,
        >,
    ) -> Result<FfmpegContext> {
        check_duplicate_inputs_outputs(&inputs, &outputs)?;

        crate::core::initialize_ffmpeg();

        // The status atomic exists before any context is opened so every
        // input/output AVFormatContext can carry an interrupt callback bound
        // to it (fftools installs decode_interrupt_cb the same way).
        let scheduler_status = Arc::new(std::sync::atomic::AtomicUsize::new(
            crate::core::scheduler::ffmpeg_scheduler::STATUS_INIT,
        ));
        let interrupt_state = Arc::new(crate::core::context::InterruptState::new(
            scheduler_status.clone(),
        ));

        let mut demuxs = open_input_files(&mut inputs, copy_ts, &interrupt_state)?;

        if demuxs.len() <= 1 {
            independent_readrate = false;
        }

        // Resolve deferred filter descriptions against the just-opened demuxers
        // (stream selection, durations, codec parameters) and append them BEFORE
        // the emptiness check below, so the resulting graph is initialized. These
        // closures must not consume corrected timing state
        // (start_time_effective / ts_offset) — that is populated later by
        // correct_input_start_times.
        for deferred in deferred_filter_descs {
            filter_complexs.push(deferred(&demuxs)?);
        }

        let mut filter_graphs = if !filter_complexs.is_empty() {
            let mut filter_graphs = init_filter_graphs(filter_complexs)?;
            fg_bind_inputs(&mut filter_graphs, &mut demuxs)?;
            filter_graphs
        } else {
            Vec::new()
        };

        let mut muxs = open_output_files(&mut outputs, copy_ts, &interrupt_state)?;

        outputs_bind(&mut muxs, &mut filter_graphs, &mut demuxs)?;

        // Propagate input recording_time to mux as a convenience feature.
        // This allows users to set recording_time on Input and have it work
        // correctly for stream-copy scenarios (where the mux-side check in
        // streamcopy_rescale needs recording_time). Only propagate when all
        // mapped streams come from the same input file to avoid incorrect
        // truncation in multi-input scenarios.
        for mux in muxs.iter_mut() {
            if mux.recording_time_us.is_none() {
                let mapping = mux.stream_input_mapping();
                if !mapping.is_empty() {
                    let first_input = mapping[0].1 .0;
                    let all_same_input = mapping.iter().all(|(_, (idx, _))| *idx == first_input);
                    if all_same_input {
                        if let Some(demux) = demuxs.get(first_input) {
                            if let Some(recording_time) = demux.recording_time_us {
                                mux.recording_time_us = Some(recording_time);
                            }
                        }
                    }
                }
            }
        }

        correct_input_start_times(&mut demuxs, copy_ts);

        check_output_streams(&muxs)?;

        check_fg_bindings(&filter_graphs)?;

        check_frame_filter_pipeline(&muxs, &demuxs)?;

        Ok(Self {
            independent_readrate,
            demuxs,
            filter_graphs,
            muxs,
            scheduler_status,
            interrupt_state,
        })
    }
}

const START_AT_ZERO: bool = false;

fn correct_input_start_times(demuxs: &mut Vec<Demuxer>, copy_ts: bool) {
    for (i, demux) in demuxs.iter_mut().enumerate() {
        unsafe {
            let is = demux.in_fmt_ctx_ptr();

            demux.start_time_effective = (*is).start_time;
            if (*is).start_time == ffmpeg_sys_next::AV_NOPTS_VALUE
                || (*(*is).iformat).flags & ffmpeg_sys_next::AVFMT_TS_DISCONT == 0
            {
                continue;
            }

            let mut new_start_time = i64::MAX;
            let stream_count = (*is).nb_streams;
            for j in 0..stream_count {
                let st = *(*is).streams.add(j as usize);
                if (*st).discard == ffmpeg_sys_next::AVDiscard::AVDISCARD_ALL
                    || (*st).start_time == ffmpeg_sys_next::AV_NOPTS_VALUE
                {
                    continue;
                }
                new_start_time = std::cmp::min(
                    new_start_time,
                    av_rescale_q(
                        (*st).start_time,
                        (*st).time_base,
                        ffmpeg_sys_next::AV_TIME_BASE_Q,
                    ),
                );
            }
            let diff = new_start_time - (*is).start_time;
            if diff != 0 {
                debug!("Correcting start time of Input #{i} by {diff}us.");
                demux.start_time_effective = new_start_time;
                if copy_ts && START_AT_ZERO {
                    demux.ts_offset = -new_start_time;
                } else if !copy_ts {
                    let abs_start_seek = (*is).start_time + demux.start_time_us.unwrap_or(0);
                    demux.ts_offset = if abs_start_seek > new_start_time {
                        -abs_start_seek
                    } else {
                        -new_start_time
                    };
                } else if copy_ts {
                    demux.ts_offset = 0;
                }

                // demux.ts_offset += demux.input_ts_offset;
            }
        }
    }
}

fn check_pipeline<T>(
    frame_pipelines: Option<&Vec<FramePipeline>>,
    streams: &[T],
    tag: &str,
    get_stream_index: impl Fn(&T) -> usize,
    get_codec_type: impl Fn(&T) -> &AVMediaType,
) -> Result<()> {
    let tag_cap = {
        let mut chars = tag.chars();
        match chars.next() {
            None => String::new(),
            Some(first) => first.to_uppercase().collect::<String>() + chars.as_str(),
        }
    };

    frame_pipelines
        .into_iter()
        .flat_map(|pipelines| pipelines.iter())
        .try_for_each(|pipeline| {
            if let Some(idx) = pipeline.stream_index {
                streams
                    .iter()
                    .any(|s| {
                        get_stream_index(s) == idx && get_codec_type(s) == &pipeline.media_type
                    })
                    .then_some(())
                    .ok_or_else(|| {
                        Into::<crate::error::Error>::into(FrameFilterStreamTypeNoMatched(
                            tag_cap.clone(),
                            idx,
                            format!("{:?}", pipeline.media_type),
                        ))
                    })
            } else {
                streams
                    .iter()
                    .any(|s| get_codec_type(s) == &pipeline.media_type)
                    .then_some(())
                    .ok_or_else(|| {
                        FrameFilterTypeNoMatched(tag.into(), format!("{:?}", pipeline.media_type))
                    })
            }
        })?;
    Ok(())
}

fn check_frame_filter_pipeline(muxs: &[Muxer], demuxs: &[Demuxer]) -> Result<()> {
    muxs.iter().try_for_each(|mux| {
        check_pipeline(
            mux.frame_pipelines.as_ref(),
            mux.get_streams(),
            "output",
            |s| s.stream_index,
            |s| &s.codec_type,
        )
    })?;
    demuxs.iter().try_for_each(|demux| {
        check_pipeline(
            demux.frame_pipelines.as_ref(),
            demux.get_streams(),
            "input",
            |s| s.stream_index,
            |s| &s.codec_type,
        )
    })?;
    Ok(())
}

fn check_fg_bindings(filter_graphs: &Vec<FilterGraph>) -> Result<()> {
    // check that all outputs were bound
    for filter_graph in filter_graphs {
        for (i, output_filter) in filter_graph.outputs.iter().enumerate() {
            if !output_filter.has_dst() {
                let linklabel = if output_filter.linklabel.is_empty() {
                    "unlabeled".to_string()
                } else {
                    output_filter.linklabel.clone()
                };
                return Err(OutputUnconnected(output_filter.name.clone(), i, linklabel).into());
            }
        }
    }
    Ok(())
}

impl From<FfmpegContext> for FfmpegScheduler<Initialization> {
    fn from(val: FfmpegContext) -> Self {
        FfmpegScheduler::new(val)
    }
}

fn check_output_streams(muxs: &Vec<Muxer>) -> Result<()> {
    for mux in muxs {
        unsafe {
            let oformat = (*mux.out_fmt_ctx_ptr()).oformat;
            if !mux.has_src() && (*oformat).flags & AVFMT_NOSTREAMS == 0 {
                warn!("Output file does not contain any stream");
                return Err(OpenOutputError::NotContainStream.into());
            }
        }
    }
    Ok(())
}

fn check_duplicate_inputs_outputs(inputs: &[Input], outputs: &[Output]) -> Result<()> {
    for output in outputs {
        if let Some(output_url) = &output.url {
            for input in inputs {
                if let Some(input_url) = &input.url {
                    if input_url == output_url {
                        return Err(FileSameAsInput(input_url.clone()));
                    }
                }
            }
        }
    }
    Ok(())
}

/// Similar to strtol() in C
/// FFmpeg reference: ffmpeg_opt.c:512 - strtol(arg, &endptr, 0)
/// Used for parsing file indices and other integers in stream specifiers
fn strtol(input: &str) -> Result<(i64, &str)> {
    let mut chars = input.chars().peekable();
    let mut negative = false;

    if let Some(&ch) = chars.peek() {
        if ch == '-' {
            negative = true;
            chars.next();
        } else if !ch.is_ascii_digit() {
            return Err(ParseInteger);
        }
    }

    let number_start = input.len() - chars.clone().collect::<String>().len();

    let number_str: String = chars
        .by_ref()
        .take_while(|ch| ch.is_ascii_digit())
        .collect();

    if number_str.is_empty() {
        return Err(ParseInteger);
    }

    let number: i64 = number_str.parse().map_err(|_| ParseInteger)?;

    let remainder_index = number_start + number_str.len();
    let remainder = &input[remainder_index..];

    if negative {
        Ok((-number, remainder))
    } else {
        Ok((number, remainder))
    }
}

fn convert_options(
    opts: Option<HashMap<String, String>>,
) -> Result<Option<HashMap<CString, CString>>> {
    if opts.is_none() {
        return Ok(None);
    }

    let converted = opts.map(|map| {
        map.into_iter()
            .map(|(k, v)| Ok((CString::new(k)?, CString::new(v)?)))
            .collect::<Result<HashMap<CString, CString>, _>>() // Collect into a HashMap
    });

    converted.transpose() // Convert `Result<Option<T>>` into `Option<Result<T>>`
}

#[cfg(test)]
mod tests {
    use std::ffi::{CStr, CString};
    use std::ptr::null_mut;

    use crate::core::context::ffmpeg_context::{strtol, FfmpegContext, Output};
    use ffmpeg_sys_next::avfilter_graph_parse_ptr;

    use crate::core::context::ffmpeg_context::{bind_fg_inputs_by_fg, fg_complex_bind_input};
    use crate::core::context::ffmpeg_context::{
        read_packet_wrapper, seek_input_packet_wrapper, write_packet_wrapper, InputOpaque,
        OutputOpaque,
    };
    use crate::core::context::filter_graph::FilterGraph;
    use crate::core::context::input_filter::InputFilter;
    use crate::core::context::null_frame;
    use crate::core::context::output_filter::OutputFilter;
    use ffmpeg_sys_next::AVMediaType::AVMEDIA_TYPE_VIDEO;

    fn test_input(linklabel: &str, name: &str) -> InputFilter {
        InputFilter::new(
            linklabel.to_string(),
            AVMEDIA_TYPE_VIDEO,
            name.to_string(),
            null_frame(),
        )
    }

    fn test_graph(inputs: Vec<InputFilter>, outputs: Vec<OutputFilter>) -> FilterGraph {
        FilterGraph::new("null".to_string(), inputs, outputs, None, None)
    }

    // The REAL builder must leave a cross-graph-bound pad as a `None` hole in the
    // consumer node's pad-indexed scheduler-input list, with the demuxer-bound
    // pad in its own slot — not a shifted/dense list. FC0 outputs `[mid]` (the
    // consumer's pad 0, cross-graph); `[1:v]` is a demuxer stream (pad 1). This
    // pins the structure the runtime relies on (a shifted list would mis-route
    // demuxer choke-balancing).
    #[test]
    fn builder_leaves_cross_graph_pad_as_a_hole_in_scheduler_inputs() {
        use crate::core::context::input::Input;
        use crate::core::scheduler::input_controller::SchNode;

        let out = std::env::temp_dir().join(format!(
            "ez_ffmpeg_xgraph_struct_{}.mp4",
            std::process::id()
        ));
        let ctx = FfmpegContext::builder()
            .input(Input::from("color=c=red:s=64x64:r=15:d=0.2").set_format("lavfi"))
            .input(Input::from("color=c=blue:s=64x64:r=15:d=0.2").set_format("lavfi"))
            .filter_desc("[0:v]hue=s=0[mid]")
            .filter_desc("[mid][1:v]overlay[vout]")
            .output(
                Output::from(out.to_str().unwrap())
                    .add_stream_map("vout")
                    .set_video_codec("mpeg4"),
            )
            .build()
            .expect("cross-graph build");

        let consumer = ctx
            .filter_graphs
            .iter()
            .find(|fg| fg.graph_desc.contains("overlay"))
            .expect("consumer graph present");
        let SchNode::Filter { inputs, .. } = consumer.node.as_ref() else {
            panic!("consumer node must be a Filter");
        };
        assert_eq!(inputs.len(), 2, "one scheduler-input slot per filter pad");
        assert!(inputs[0].is_none(), "pad 0 ([mid]) is a cross-graph hole");
        assert!(inputs[1].is_some(), "pad 1 ([1:v]) binds a demuxer");
    }

    #[test]
    fn cross_graph_binding_uses_input_pad_index_and_marks_bound() {
        // Producer (graph 0): one output labeled "mid".
        // Consumer (graph 1): pad 0 labeled "mid", pad 1 unlabeled.
        // The consumer's GRAPH index (1) differs from the matching PAD
        // index (0) on purpose: routing and finished_flag_list indexing
        // work per input pad, not per graph.
        let producer = test_graph(
            vec![test_input("", "in0")],
            vec![OutputFilter::new(
                "mid".to_string(),
                AVMEDIA_TYPE_VIDEO,
                "out0".to_string(),
            )],
        );
        let consumer = test_graph(
            vec![test_input("mid", "in0"), test_input("", "in1")],
            vec![OutputFilter::new(
                String::new(),
                AVMEDIA_TYPE_VIDEO,
                "out0".to_string(),
            )],
        );
        let mut graphs = vec![producer, consumer];

        bind_fg_inputs_by_fg(&mut graphs).unwrap();

        assert!(
            graphs[0].outputs[0].has_dst(),
            "producer output must be connected to the consumer"
        );
        assert_eq!(
            graphs[0].outputs[0].fg_input_index, 0,
            "fg_input_index must be the consumer's input PAD index, not its graph index"
        );
        assert_eq!(
            graphs[0].outputs[0].finished_flag_list.len(),
            2,
            "the producer must hold the consumer's per-pad finished flags"
        );
        assert!(
            graphs[1].inputs[0].bound,
            "the cross-connected pad must be marked bound"
        );
        assert!(
            !graphs[1].inputs[1].bound,
            "unrelated pads must stay unbound"
        );
    }

    #[test]
    fn complex_bind_skips_already_bound_labeled_input() {
        let mut consumer = test_graph(
            vec![test_input("mid", "in0")],
            vec![OutputFilter::new(
                String::new(),
                AVMEDIA_TYPE_VIDEO,
                "out0".to_string(),
            )],
        );
        consumer.inputs[0].bound = true;

        // No demuxers exist: if the pad were (re-)bound to an input stream
        // this would fail with "stream not found".
        let result = fg_complex_bind_input(&mut consumer, 0, &mut Vec::new());
        assert!(
            result.is_ok(),
            "a pad already bound to another graph must not be re-bound: {result:?}"
        );
    }

    #[test]
    fn complex_bind_skips_bound_reserved_in_label() {
        // The reserved label "in" takes the auto-bind branch, which must
        // also respect an existing cross-graph binding.
        let mut consumer = test_graph(
            vec![test_input("in", "in0")],
            vec![OutputFilter::new(
                String::new(),
                AVMEDIA_TYPE_VIDEO,
                "out0".to_string(),
            )],
        );
        consumer.inputs[0].bound = true;

        let result = fg_complex_bind_input(&mut consumer, 0, &mut Vec::new());
        assert!(
            result.is_ok(),
            "a bound pad labeled 'in' must not fall through to stream auto-binding: {result:?}"
        );
    }

    #[test]
    fn test_filter() {
        let desc_cstr = CString::new("[1:v][2:v]concat=n=2:v=1:a=0[vout]").unwrap();
        // let desc_cstr = CString::new("fps=15").unwrap();

        unsafe {
            let graph = crate::raw::FilterGraph::alloc().unwrap();
            let mut inputs = crate::raw::FilterInOut::empty();
            let mut outputs = crate::raw::FilterInOut::empty();

            let ret = avfilter_graph_parse_ptr(
                graph.as_ptr(),
                desc_cstr.as_ptr(),
                inputs.as_out_ptr(),
                outputs.as_out_ptr(),
                null_mut(),
            );
            if ret < 0 {
                println!("err ret:{}", crate::util::ffmpeg_utils::av_err2str(ret));
                return;
            }

            println!("inputs.is_null:{}", inputs.as_ptr().is_null());
            println!("outputs.is_null:{}", outputs.as_ptr().is_null());

            let mut cur = inputs.as_ptr();
            while !cur.is_null() {
                let input_name = CStr::from_ptr((*cur).name);
                println!("Input name: {}", input_name.to_str().unwrap());
                cur = (*cur).next;
            }

            let output_name = CStr::from_ptr((*outputs.as_ptr()).name);
            println!("Output name: {}", output_name.to_str().unwrap());

            let filter_ctx = (*outputs.as_ptr()).filter_ctx;
            println!("filter_ctx.is_null:{}", filter_ctx.is_null());
        }
    }

    #[test]
    fn fallback_frame_carries_real_stream_parameters() {
        // Regression: the probe dec_ctx used to stay at codec defaults, so
        // the fallback frame bound to each filtergraph input had format=-1
        // and the EOF-before-first-frame path could never configure a graph.
        let ctx = FfmpegContext::new(
            vec!["test.mp4".into()],
            vec!["hue=s=0".into()],
            vec!["output_fallback_probe.mp4".to_string().into()],
        )
        .unwrap();
        let fallback = unsafe { &*ctx.filter_graphs[0].inputs[0].opts.fallback.as_ptr() };
        assert!(
            fallback.format >= 0,
            "fallback must carry the stream's real format, got {}",
            fallback.format
        );
        assert!(
            fallback.width > 0 && fallback.height > 0,
            "fallback must carry the stream's dimensions, got {}x{}",
            fallback.width,
            fallback.height
        );
        assert!(
            fallback.time_base.num > 0 && fallback.time_base.den > 0,
            "fallback must carry the stream's packet time base, got {}/{}",
            fallback.time_base.num,
            fallback.time_base.den
        );
    }

    #[test]
    fn test_new() {
        let _ = env_logger::builder()
            .filter_level(log::LevelFilter::Debug)
            .is_test(true)
            .try_init();
        let _ffmpeg_context = FfmpegContext::new(
            vec!["test.mp4".to_string().into()],
            vec!["hue=s=0".to_string().into()],
            vec!["output.mp4".to_string().into()],
        )
        .unwrap();
        let _ffmpeg_context = FfmpegContext::new(
            vec!["test.mp4".into()],
            vec!["[0:v]hue=s=0".into()],
            vec!["output.mp4".to_string().into()],
        )
        .unwrap();
        let _ffmpeg_context = FfmpegContext::new(
            vec!["test.mp4".into()],
            vec!["hue=s=0[my-out]".into()],
            vec![Output::from("output.mp4").add_stream_map("my-out")],
        )
        .unwrap();
        let result = FfmpegContext::new(
            vec!["test.mp4".into()],
            vec!["hue=s=0".into()],
            vec![Output::from("output.mp4").add_stream_map("0:v?")],
        );
        assert!(result.is_err());
        let result = FfmpegContext::new(
            vec!["test.mp4".into()],
            vec!["hue=s=0".into()],
            vec![Output::from("output.mp4").add_stream_map_with_copy("1:v?")],
        );
        assert!(result.is_err());
        let result = FfmpegContext::new(
            vec!["test.mp4".into()],
            vec!["hue=s=0[fg-out]".into()],
            vec![
                Output::from("output.mp4").add_stream_map("my-out?"),
                Output::from("output.mp4").add_stream_map("fg-out"),
            ],
        );
        assert!(result.is_err());
        // ignore filter
        let result = FfmpegContext::new(
            vec!["test.mp4".into()],
            vec!["hue=s=0".into()],
            vec![Output::from("output.mp4").add_stream_map_with_copy("1:v")],
        );
        assert!(result.is_err());
        let result = FfmpegContext::new(
            vec!["test.mp4".into()],
            vec!["hue=s=0[fg-out]".into()],
            vec![Output::from("output.mp4").add_stream_map("fg-out?")],
        );
        assert!(result.is_err());
    }

    #[test]
    fn test_builder() {
        let _ = env_logger::builder()
            .filter_level(log::LevelFilter::Debug)
            .is_test(true)
            .try_init();

        let _context1 = FfmpegContext::builder()
            .input("test.mp4")
            .filter_desc("hue=s=0")
            .output("output.mp4")
            .build()
            .unwrap();

        let _context2 = FfmpegContext::builder()
            .inputs(vec!["test.mp4"])
            .filter_descs(vec!["hue=s=0"])
            .outputs(vec!["output.mp4"])
            .build()
            .unwrap();
    }

    #[test]
    fn test_strtol() {
        let input = "-123---abc";
        let result = strtol(input);
        assert_eq!(result.unwrap(), (-123, "---abc"));

        let input = "123---abc";
        let result = strtol(input);
        assert_eq!(result.unwrap(), (123, "---abc"));

        let input = "-123aa";
        let result = strtol(input);
        assert_eq!(result.unwrap(), (-123, "aa"));

        let input = "-aa";
        let result = strtol(input);
        assert!(result.is_err());

        let input = "abc";
        let result = strtol(input);
        assert!(result.is_err())
    }

    // ---- custom-IO wrapper hardening (deterministic, no FFmpeg involved) ----
    //
    // The wrappers are plain extern "C" fns; driving them directly with a
    // fabricated opaque pins their exact contract: oversized read lengths are
    // clamped to EIO, a panicking closure is contained AND poisons the
    // context, and a poisoned context never re-enters user code.

    use std::sync::atomic::{AtomicUsize, Ordering as AtomicOrdering};
    use std::sync::Arc;

    fn eio() -> libc::c_int {
        ffmpeg_sys_next::AVERROR(ffmpeg_sys_next::EIO)
    }

    #[test]
    fn read_wrapper_clamps_oversized_length_and_allows_exact_fit() {
        let opaque = Box::into_raw(Box::new(InputOpaque {
            read: Box::new(|buf| buf.len() as i32 + 64),
            seek: None,
            poisoned: false,
        }));
        let mut buf = [0u8; 32];
        let ret = unsafe {
            read_packet_wrapper(
                opaque as *mut libc::c_void,
                buf.as_mut_ptr(),
                buf.len() as libc::c_int,
            )
        };
        assert_eq!(ret, eio(), "a forged over-length must clamp to EIO");
        unsafe {
            // Not poisoned by a bogus length: an exact-fit read stays legal.
            (*opaque).read = Box::new(|buf| buf.len() as i32);
            let ret = read_packet_wrapper(
                opaque as *mut libc::c_void,
                buf.as_mut_ptr(),
                buf.len() as libc::c_int,
            );
            assert_eq!(
                ret,
                buf.len() as libc::c_int,
                "ret == buf_size is within bounds and must pass through"
            );
            drop(Box::from_raw(opaque));
        }
    }

    #[test]
    fn read_wrapper_contains_panic_and_poisons() {
        let calls = Arc::new(AtomicUsize::new(0));
        let probe = Arc::clone(&calls);
        let opaque = Box::into_raw(Box::new(InputOpaque {
            read: Box::new(move |_buf| {
                probe.fetch_add(1, AtomicOrdering::SeqCst);
                panic!("test-injected read panic");
            }),
            seek: None,
            poisoned: false,
        }));
        let mut buf = [0u8; 32];
        for _ in 0..3 {
            let ret = unsafe {
                read_packet_wrapper(
                    opaque as *mut libc::c_void,
                    buf.as_mut_ptr(),
                    buf.len() as libc::c_int,
                )
            };
            assert_eq!(ret, eio());
        }
        assert_eq!(
            calls.load(AtomicOrdering::SeqCst),
            1,
            "the panicking closure must never be re-entered once poisoned"
        );
        unsafe { drop(Box::from_raw(opaque)) };
    }

    #[test]
    fn write_wrapper_contains_panic_and_poisons() {
        let calls = Arc::new(AtomicUsize::new(0));
        let probe = Arc::clone(&calls);
        let opaque = Box::into_raw(Box::new(OutputOpaque {
            write: Box::new(move |_buf| {
                probe.fetch_add(1, AtomicOrdering::SeqCst);
                panic!("test-injected write panic");
            }),
            seek: None,
            poisoned: false,
        }));
        let buf = [0u8; 32];
        for _ in 0..3 {
            let ret = unsafe {
                write_packet_wrapper(
                    opaque as *mut libc::c_void,
                    buf.as_ptr(),
                    buf.len() as libc::c_int,
                )
            };
            assert_eq!(ret, eio());
        }
        assert_eq!(
            calls.load(AtomicOrdering::SeqCst),
            1,
            "the panicking closure must never be re-entered once poisoned"
        );
        unsafe { drop(Box::from_raw(opaque)) };
    }

    #[test]
    fn seek_panic_poisons_the_whole_input_context() {
        let reads = Arc::new(AtomicUsize::new(0));
        let read_probe = Arc::clone(&reads);
        let opaque = Box::into_raw(Box::new(InputOpaque {
            read: Box::new(move |buf| {
                read_probe.fetch_add(1, AtomicOrdering::SeqCst);
                buf.len() as i32
            }),
            seek: Some(Box::new(|_offset, _whence| {
                panic!("test-injected seek panic")
            })),
            poisoned: false,
        }));
        let mut buf = [0u8; 32];
        unsafe {
            // Healthy read before the panic.
            let ret = read_packet_wrapper(
                opaque as *mut libc::c_void,
                buf.as_mut_ptr(),
                buf.len() as libc::c_int,
            );
            assert_eq!(ret, buf.len() as libc::c_int);

            // The panicking seek is contained as EIO (not ESPIPE, which
            // would let FFmpeg fall back to non-seeking modes and mask it).
            let ret = seek_input_packet_wrapper(opaque as *mut libc::c_void, 0, 0);
            assert_eq!(ret, eio() as i64);

            // Cross-callback poison: the read closure must not run again.
            let ret = read_packet_wrapper(
                opaque as *mut libc::c_void,
                buf.as_mut_ptr(),
                buf.len() as libc::c_int,
            );
            assert_eq!(ret, eio());
            drop(Box::from_raw(opaque));
        }
        assert_eq!(
            reads.load(AtomicOrdering::SeqCst),
            1,
            "a seek panic must poison reads on the same context"
        );
    }

    /// H12: a short-writing sink (io::Write-style) must not lose the
    /// remainder — the wrapper resubmits until the whole buffer went out.
    #[test]
    fn write_wrapper_resubmits_short_writes() {
        let seen = Arc::new(std::sync::Mutex::new(Vec::new()));
        let probe = Arc::clone(&seen);
        let opaque = Box::into_raw(Box::new(OutputOpaque {
            write: Box::new(move |buf| {
                // Write at most 10 bytes per call, recording each chunk.
                let n = buf.len().min(10);
                probe.lock().unwrap().extend_from_slice(&buf[..n]);
                n as i32
            }),
            seek: None,
            poisoned: false,
        }));
        let data: Vec<u8> = (0..64u8).collect();
        let ret = unsafe {
            write_packet_wrapper(
                opaque as *mut libc::c_void,
                data.as_ptr(),
                data.len() as libc::c_int,
            )
        };
        assert_eq!(
            ret,
            data.len() as libc::c_int,
            "the whole buffer must report written"
        );
        assert_eq!(
            *seen.lock().unwrap(),
            data,
            "no byte may be lost or reordered"
        );
        unsafe { drop(Box::from_raw(opaque)) };
    }

    /// H12: zero progress and over-claimed lengths are I/O faults, not
    /// silent success.
    #[test]
    fn write_wrapper_rejects_zero_progress_and_over_claims() {
        let opaque = Box::into_raw(Box::new(OutputOpaque {
            write: Box::new(|_buf| 0),
            seek: None,
            poisoned: false,
        }));
        let data = [7u8; 16];
        let ret = unsafe { write_packet_wrapper(opaque as *mut libc::c_void, data.as_ptr(), 16) };
        assert_eq!(
            ret,
            eio(),
            "a zero-progress sink must fail, not spin or succeed"
        );
        unsafe {
            (*opaque).write = Box::new(|buf| buf.len() as i32 + 4);
            let ret = write_packet_wrapper(opaque as *mut libc::c_void, data.as_ptr(), 16);
            assert_eq!(ret, eio(), "an over-claimed write length must fail");
            drop(Box::from_raw(opaque));
        }
    }

    /// A negative error from the sink passes through unchanged.
    #[test]
    fn write_wrapper_passes_sink_errors_through() {
        let opaque = Box::into_raw(Box::new(OutputOpaque {
            write: Box::new(|_buf| ffmpeg_sys_next::AVERROR(ffmpeg_sys_next::ENOSPC)),
            seek: None,
            poisoned: false,
        }));
        let data = [7u8; 8];
        let ret = unsafe { write_packet_wrapper(opaque as *mut libc::c_void, data.as_ptr(), 8) };
        assert_eq!(ret, ffmpeg_sys_next::AVERROR(ffmpeg_sys_next::ENOSPC));
        unsafe { drop(Box::from_raw(opaque)) };
    }

    #[test]
    fn absent_seek_callback_stays_espipe() {
        let opaque = Box::into_raw(Box::new(InputOpaque {
            read: Box::new(|buf| buf.len() as i32),
            seek: None,
            poisoned: false,
        }));
        let ret = unsafe { seek_input_packet_wrapper(opaque as *mut libc::c_void, 0, 0) };
        assert_eq!(
            ret,
            ffmpeg_sys_next::AVERROR(ffmpeg_sys_next::ESPIPE) as i64,
            "no seek callback means genuinely unseekable, not an I/O fault"
        );
        unsafe { drop(Box::from_raw(opaque)) };
    }
}
