use crate::core::context::filter_graph::FilterGraph;
use crate::core::context::input_filter::{InputFilterOptions, IFILTER_FLAG_AUTOROTATE};
use crate::core::context::obj_pool::ObjPool;
use crate::core::context::output::VSyncMethod;
use crate::core::context::output::VSyncMethod::{VsyncCfr, VsyncVscfr};
use crate::core::context::output_filter::OutputFilterOptions;
use crate::core::context::{null_frame, FrameBox, FrameData, SideDataList};
use crate::core::display::get_rotation;
use crate::core::scheduler::ffmpeg_scheduler::{
    frame_is_null, is_stopping, set_scheduler_error, wait_until_not_paused,
};
use crate::core::scheduler::input_controller::{InputController, SchNode};
use crate::error::{Error, FilterGraphError, FilterGraphOperationError, FilterGraphParseError};
use crate::hwaccel::{hw_device_for_filter, HWDevice};
use crate::util::ffmpeg_utils::av_err2str;
use crate::util::ffmpeg_utils::av_rescale_q_rnd;
use crate::util::thread_synchronizer::{ThreadDoneGuard, ThreadSynchronizer};
use crossbeam_channel::{RecvTimeoutError, Sender};
use ffmpeg_next::Frame;
#[cfg(not(docsrs))]
use ffmpeg_sys_next::AVChannelOrder::AV_CHANNEL_ORDER_UNSPEC;
use ffmpeg_sys_next::AVColorRange::AVCOL_RANGE_UNSPECIFIED;
use ffmpeg_sys_next::AVColorSpace::AVCOL_SPC_UNSPECIFIED;
use ffmpeg_sys_next::AVFrameSideDataType::AV_FRAME_DATA_DISPLAYMATRIX;
#[cfg(ffmpeg_8_0)]
use ffmpeg_sys_next::AVFrameSideDataType::AV_FRAME_DATA_DOWNMIX_INFO;
use ffmpeg_sys_next::AVMediaType::{AVMEDIA_TYPE_AUDIO, AVMEDIA_TYPE_SUBTITLE, AVMEDIA_TYPE_VIDEO};
use ffmpeg_sys_next::AVOptionType::AV_OPT_TYPE_BINARY;
use ffmpeg_sys_next::AVPixelFormat::AV_PIX_FMT_NONE;
use ffmpeg_sys_next::AVRounding::{AV_ROUND_NEAR_INF, AV_ROUND_PASS_MINMAX};
use ffmpeg_sys_next::AVSampleFormat::AV_SAMPLE_FMT_NONE;
use ffmpeg_sys_next::{
    av_bprint_chars, av_bprint_finalize, av_bprint_init, av_bprintf, av_buffer_ref,
    av_buffer_unref, av_buffersink_get_frame_flags, av_buffersink_get_frame_rate,
    av_buffersink_get_h, av_buffersink_get_sample_aspect_ratio, av_buffersink_get_sample_rate,
    av_buffersink_get_time_base, av_buffersink_get_w, av_buffersrc_add_frame,
    av_buffersrc_add_frame_flags, av_buffersrc_close, av_buffersrc_get_nb_failed_requests,
    av_buffersrc_parameters_alloc, av_buffersrc_parameters_set, av_color_range_name,
    av_color_space_name, av_dict_free, av_frame_alloc, av_frame_free, av_frame_get_side_data,
    av_frame_ref, av_frame_remove_side_data, av_freep, av_get_pix_fmt_name, av_get_sample_fmt_name,
    av_inv_q, av_log2, av_malloc, av_opt_find, av_opt_set, av_opt_set_bin, av_opt_set_int, av_q2d,
    av_rescale_q, av_strdup, avfilter_get_by_name, avfilter_graph_config,
    avfilter_graph_create_filter, avfilter_graph_request_oldest, avfilter_link,
    avfilter_pad_get_type, avio_close, avio_closep, avio_open, avio_open2, avio_read,
    avio_read_to_bprint, avio_size, AVBPrint, AVBufferRef, AVColorRange, AVColorSpace,
    AVFilterContext, AVFilterGraph, AVFilterInOut, AVFrame, AVMediaType, AVPixelFormat, AVRational,
    AVSampleFormat, AVERROR, AVERROR_BUG, AVERROR_EOF, AVERROR_OPTION_NOT_FOUND, AVIO_FLAG_READ,
    AV_BPRINT_SIZE_AUTOMATIC, AV_BUFFERSINK_FLAG_NO_REQUEST, AV_BUFFERSRC_FLAG_PUSH,
    AV_NOPTS_VALUE, AV_OPT_SEARCH_CHILDREN, AV_PIX_FMT_FLAG_HWACCEL, AV_TIME_BASE_Q, EAGAIN,
    EINVAL, EIO, ENOMEM,
};
#[cfg(not(docsrs))]
use ffmpeg_sys_next::{
    av_buffer_replace, av_buffersink_get_ch_layout, av_buffersink_get_color_range,
    av_buffersink_get_colorspace, av_buffersink_get_format, av_channel_layout_check,
    av_channel_layout_compare, av_channel_layout_copy, av_channel_layout_describe_bprint,
    av_channel_layout_uninit, av_dict_iterate, avfilter_graph_segment_apply,
    avfilter_graph_segment_create_filters, avfilter_graph_segment_free,
    avfilter_graph_segment_parse, AVChannelLayout, AVChannelLayout__bindgen_ty_1, AVChannelOrder,
    AVFilterGraphSegment, AVFILTER_FLAG_HWDEVICE, AVFILTER_FLAG_METADATA_ONLY, AV_FRAME_FLAG_KEY,
};
#[cfg(ffmpeg_8_0)]
use ffmpeg_sys_next::{
    av_buffersink_get_side_data, av_frame_side_data_desc,
    AVSideDataProps::AV_SIDE_DATA_PROP_GLOBAL, AV_FRAME_SIDE_DATA_FLAG_REPLACE,
};
use log::{debug, error, info, trace, warn};
use std::collections::VecDeque;
use std::ffi::{c_char, c_void, CStr, CString};
use std::ptr::{null, null_mut};
use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};
use std::sync::{Arc, Mutex};
use std::time::Duration;

pub(crate) fn filter_graph_init(
    fg_index: usize,
    filter_graph: &mut FilterGraph,
    frame_pool: ObjPool<Frame>,
    input_controller: Arc<InputController>,
    filter_node: Arc<SchNode>,
    scheduler_status: Arc<AtomicUsize>,
    thread_sync: ThreadSynchronizer,
    scheduler_result: Arc<Mutex<Option<crate::error::Result<()>>>>,
) -> crate::error::Result<()> {
    // The graph's filter hw device is registered at context-build time now
    // (init_filter_graph, fftools 0f5592cfc737 ordering) so that the probe
    // parse already sees it; configure_filtergraph below resolves it from the
    // global registry via hw_device_for_filter().
    let (src, finished_flag_list) = filter_graph.take_src();

    let input_len = filter_graph.inputs.len();
    let output_len = filter_graph.outputs.len();
    let graph_desc = filter_graph.graph_desc.clone();
    // NEW-SC-03: snapshot the graph-level sws/swr opts before spawning the
    // filter thread; they are moved into the closure and installed on the
    // FilterGraphParameter, then applied on every (re)configure.
    let graph_sws_opts = filter_graph.sws_opts.clone();
    let graph_swr_opts = filter_graph.swr_opts.clone();

    let mut ifps = Vec::with_capacity(input_len);
    for i in 0..input_len {
        let opts = std::mem::replace(
            &mut filter_graph.inputs[i].opts,
            InputFilterOptions::empty(),
        );

        let input_filter_parameter = InputFilterParameter::new(
            i,
            filter_graph.inputs[i].name.clone(),
            filter_graph.inputs[i].media_type,
            opts,
        );
        ifps.push(input_filter_parameter);
    }

    let mut ofps = Vec::with_capacity(output_len);
    for i in 0..output_len {
        let output_filter_parameter = OutputFilterParameter::new(
            filter_graph.outputs[i].media_type,
            filter_graph.outputs[i].opts.clone(),
            filter_graph.outputs[i].take_dst(),
            filter_graph.outputs[i].fg_input_index,
            filter_graph.outputs[i].finished_flag_list.clone(),
        );
        ofps.push(output_filter_parameter);
    }

    // Slot claimed before spawn; the guard releases it on any exit path.
    thread_sync.thread_start();
    let thread_done_guard = ThreadDoneGuard::adopt(thread_sync.clone(), scheduler_status.clone());

    let result = std::thread::Builder::new()
        .name(format!("filtergraph{fg_index}"))
        .spawn(move || {
            let _thread_done = thread_done_guard;
            let mut graph: Option<crate::raw::FilterGraph> = None;
            let mut fgp = FilterGraphParameter::default();
            fgp.sws_opts = graph_sws_opts;
            fgp.swr_opts = graph_swr_opts;
            let node = filter_node.as_ref();
            let SchNode::Filter {
                inputs: _,
                best_input,
            } = node
            else {
                unreachable!()
            };

            loop {
                // update scheduling to account for desired input stream, if it changed
                //
                // this check needs no locking because only the filtering thread
                // updates this value
                if fgp.next_in != best_input.load(Ordering::Acquire) {
                    best_input.store(fgp.next_in, Ordering::Release);
                    input_controller.update_locked(&scheduler_status);
                }

                let result = src.recv_timeout(Duration::from_millis(100));

                if is_stopping(wait_until_not_paused(&scheduler_status)) {
                    info!("Filtergraph receiver end command, finishing.");
                    break;
                }

                if let Err(e) = result {
                    if e == RecvTimeoutError::Disconnected {
                        debug!("Filtergraph all src is finished.");
                        break;
                    }
                    continue;
                }

                let frame_box = result.unwrap();
                let input_index = frame_box.frame_data.fg_input_index;

                if input_index < finished_flag_list.len() {
                    if finished_flag_list[input_index].load(Ordering::Acquire) {
                        continue;
                    }
                } else {
                    unreachable!()
                }

                unsafe {
                    if ifps[input_index].media_type == AVMEDIA_TYPE_SUBTITLE {
                        // sub2video (rendering subtitles as filtergraph video
                        // input) is not implemented; binding rejects it up
                        // front, so a frame here is a wiring bug.
                        error!(
                            "subtitle frames cannot feed filtergraph {fg_index}: \
                             sub2video is not supported"
                        );
                        frame_pool.release(frame_box.frame);
                    } else if !frame_is_null(&frame_box.frame)
                        && !(*frame_box.frame.as_ptr()).buf[0].is_null()
                    {
                        if let Err(e) = fg_send_frame(
                            fg_index,
                            &mut graph,
                            &mut fgp,
                            &graph_desc,
                            frame_box,
                            &mut ifps,
                            &mut ofps,
                            input_index,
                            &frame_pool,
                        ) {
                            match e {
                                Error::FilterGraph(
                                    FilterGraphOperationError::BufferSourceAddFrameError(
                                        FilterGraphError::EOF,
                                    ),
                                ) => {
                                    debug!("Input {} no longer accepts new data", input_index);
                                    filter_receive_finish(&finished_flag_list, input_index);
                                }
                                e => {
                                    error!("filter_graph send_frame error: {e}");
                                    set_scheduler_error(&scheduler_status, &scheduler_result, e);
                                    break;
                                }
                            }
                        }
                    } else if let Err(e) = fg_send_eof(
                        fg_index,
                        &mut graph,
                        &mut fgp,
                        &graph_desc,
                        frame_box,
                        &mut ifps,
                        &mut ofps,
                        input_index,
                        &frame_pool,
                    ) {
                        match e {
                            Error::FilterGraph(
                                FilterGraphOperationError::BufferSourceAddFrameError(
                                    FilterGraphError::EOF,
                                ),
                            ) => {
                                debug!("Input {} no longer accepts new data", input_index);
                                filter_receive_finish(&finished_flag_list, input_index);
                            }
                            e => {
                                error!("filter_graph send_eof error: {e}");
                                set_scheduler_error(&scheduler_status, &scheduler_result, e);
                                break;
                            }
                        }
                    }
                }

                if is_stopping(wait_until_not_paused(&scheduler_status)) {
                    info!("Filtergraph receiver end command, finishing.");
                    break;
                }

                unsafe {
                    let graph_ptr = match graph.as_ref() {
                        Some(g) => g.as_ptr(),
                        None => null_mut(),
                    };
                    let ret = fg_read_frames(graph_ptr, &mut fgp, &ifps, &mut ofps, &frame_pool);
                    if ret == AVERROR_EOF {
                        trace!("Filtergraph All consumers returned EOF");
                        break;
                    }
                    if ret < 0 {
                        set_scheduler_error(
                            &scheduler_status,
                            &scheduler_result,
                            Error::FilterGraph(FilterGraphOperationError::ProcessFramesError(
                                FilterGraphError::from(ret),
                            )),
                        );
                        cleanup_filtergraph(&mut graph, &mut ifps, &mut ofps);
                        break;
                    }
                }
            }

            for ofp in &mut ofps {
                let ret = unsafe { fg_output_frame(&mut fgp, ofp, null_frame(), &frame_pool) };
                if ret < 0 {
                    set_scheduler_error(
                        &scheduler_status,
                        &scheduler_result,
                        Error::FilterGraph(FilterGraphOperationError::SendFramesError(
                            FilterGraphError::from(ret),
                        )),
                    );
                    break;
                }
            }

            cleanup_filtergraph(&mut graph, &mut ifps, &mut ofps);
            free_input_filter_resources(&mut ifps);
            debug!("FilterGraph finished.");
        });
    if let Err(e) = result {
        error!("FilterGraph thread exited with error: {e}");
        return Err(FilterGraphOperationError::ThreadExited.into());
    }

    Ok(())
}

fn filter_receive_finish(finished_flag_list: &Arc<[AtomicBool]>, input_index: usize) {
    if input_index < finished_flag_list.len() {
        if !finished_flag_list[input_index].load(Ordering::Acquire) {
            finished_flag_list[input_index].store(true, Ordering::Release);
        }
    } else {
        unreachable!()
    }
}

unsafe fn ifilter_has_all_input_formats(ifps: &Vec<InputFilterParameter>) -> bool {
    for ifp in ifps {
        if ifp.format < 0 {
            return false;
        }
    }
    true
}

struct InputFilterParameter {
    input_filter_index: usize,
    name: String,
    media_type: AVMediaType,
    opts: InputFilterOptions,
    hw_frames_ctx: *mut AVBufferRef,
    time_base: AVRational,
    format: i32,
    width: i32,
    height: i32,
    sample_aspect_ratio: AVRational,
    color_space: AVColorSpace,
    color_range: AVColorRange,
    sample_rate: i32,
    #[cfg(not(docsrs))]
    ch_layout: AVChannelLayout,
    displaymatrix_present: bool,
    displaymatrix_applied: bool,
    displaymatrix: [i32; 9],
    // Global side data collected from input frames for the buffersrc
    // parameters (fftools InputFilterPriv.side_data). Only populated (and
    // read) on FFmpeg 8+; stays empty on 7.x — hence dead there by design.
    #[cfg_attr(not(ffmpeg_8_0), allow(dead_code))]
    side_data: SideDataList,
    // Most recent downmix info seen on input frames; a change forces a graph
    // reinit because aresample consumes it at init time (fftools
    // 4f9afbb1b2; FFmpeg 8+ only).
    #[cfg(ffmpeg_8_0)]
    downmixinfo_present: bool,
    #[cfg(ffmpeg_8_0)]
    downmixinfo: ffmpeg_sys_next::AVDownmixInfo,
    filter: *mut AVFilterContext,

    frame_queue: VecDeque<FrameBox>,

    eof: bool,
}

impl InputFilterParameter {
    fn new(
        input_filter_index: usize,
        name: String,
        media_type: AVMediaType,
        opts: InputFilterOptions,
    ) -> Self {
        Self {
            input_filter_index,
            name,
            media_type,
            opts,
            hw_frames_ctx: null_mut(),
            time_base: AVRational { num: 0, den: 1 },
            format: -1,
            width: 0,
            height: 0,
            sample_aspect_ratio: AVRational { num: 0, den: 1 },
            color_space: AVColorSpace::AVCOL_SPC_UNSPECIFIED,
            color_range: AVColorRange::AVCOL_RANGE_UNSPECIFIED,
            sample_rate: 0,
            #[cfg(not(docsrs))]
            ch_layout: AVChannelLayout {
                order: AVChannelOrder::AV_CHANNEL_ORDER_UNSPEC,
                nb_channels: 0,
                u: AVChannelLayout__bindgen_ty_1 { mask: 0 },
                opaque: null_mut(),
            },
            displaymatrix_present: false,
            displaymatrix_applied: false,
            displaymatrix: [0; 9],
            side_data: SideDataList::new(),
            #[cfg(ffmpeg_8_0)]
            downmixinfo_present: false,
            // SAFETY: plain-old-data struct; all-zeroes is the fftools zero
            // init (type 0 = AV_DOWNMIX_TYPE_UNKNOWN).
            #[cfg(ffmpeg_8_0)]
            downmixinfo: unsafe { std::mem::zeroed() },
            filter: null_mut(),
            frame_queue: Default::default(),
            eof: false,
        }
    }
}

// SAFETY: InputFilterParameter can be sent to another thread. The raw FFmpeg pointers
// (filter, hw_frames_ctx) are only accessed from the filter task's owning thread.
unsafe impl Send for InputFilterParameter {}

#[derive(Default)]
struct FilterGraphParameter {
    // index of the next input to request from the scheduler
    next_in: usize,
    nb_outputs_done: usize,
    is_meta: bool,

    // NEW-SC-03: graph-level sws/swr option strings for the auto-inserted
    // scale/aresample filters (from an explicit FilterComplex). Set once when
    // the filter thread starts and read on every (re)configure to fill
    // AVFilterGraph.scale_sws_opts / aresample_swr_opts. `None` for implicit
    // per-output graphs, which resolve their value from the bound outputs.
    sws_opts: Option<String>,
    swr_opts: Option<String>,
}

struct OutputFilterParameter {
    media_type: AVMediaType,
    opts: OutputFilterOptions,
    dst: Option<Sender<FrameBox>>,

    fg_input_index: usize,
    finished_flag_list: Arc<[AtomicBool]>,

    filter: *mut AVFilterContext,
    name: String,
    fpsconv_context: FPSConvContext,
    eof: bool,

    // Whether THIS output has emitted at least one frame. Tracked per-output
    // rather than graph-wide: FFmpeg keeps a single thread-level
    // FilterGraphThread.got_frame, but ez drives multiple outputs from one
    // filtergraph, so gating each output's init-dummy, EOF flush, and
    // side-data snapshot on a shared flag mis-fires when a sibling output
    // produced frames while this one produced none. Owned by the filter
    // thread, like `eof`.
    got_frame: bool,

    format: AVPixelFormat,
    width: i32,
    height: i32,
    color_space: AVColorSpace,
    color_range: AVColorRange,
    sample_aspect_ratio: AVRational,
    sample_rate: i32,

    // Per-configuration snapshot of the side data the sink negotiated,
    // shared into FrameData for the frame that opens the encoder (fftools
    // OutputFilterPriv.side_data). Only populated on FFmpeg 8+.
    side_data: Option<Arc<SideDataList>>,

    tb_out: AVRational,
    // Once an output frame chose the timebase, a filtergraph reconfig must
    // not change it: downstream timestamps are already scaled to it
    // (ffmpeg_filter.c OutputFilterPriv.tb_out_locked).
    tb_out_locked: bool,

    next_pts: i64,
}

impl OutputFilterParameter {
    fn new(
        media_type: AVMediaType,
        opts: OutputFilterOptions,
        dst: Option<Sender<FrameBox>>,
        fg_input_index: usize,
        finished_flag_list: Arc<[AtomicBool]>,
    ) -> Self {
        let mut fpsconv_context = FPSConvContext::default();
        fpsconv_context.framerate = opts.framerate;
        fpsconv_context.framerate_max = opts.framerate_max;
        Self {
            media_type,
            dst,
            fg_input_index,
            finished_flag_list,
            filter: null_mut(),
            name: "".to_string(),
            opts,
            fpsconv_context,
            eof: false,
            got_frame: false,
            format: AVPixelFormat::AV_PIX_FMT_NONE,
            width: 0,
            height: 0,
            color_space: AVColorSpace::AVCOL_SPC_RGB,
            color_range: AVColorRange::AVCOL_RANGE_UNSPECIFIED,
            sample_aspect_ratio: AVRational { num: 0, den: 0 },
            sample_rate: 0,
            side_data: None,
            tb_out: AVRational { num: 0, den: 0 },
            tb_out_locked: false,
            next_pts: 0,
        }
    }
}

// SAFETY: OutputFilterParameter can be sent to another thread. The raw AVFilterContext
// pointer is only accessed from the filter task's owning thread.
unsafe impl Send for OutputFilterParameter {}

#[cfg(docsrs)]
unsafe fn fg_send_eof(
    fg_index: usize,
    graph: &mut Option<crate::raw::FilterGraph>,
    fgp: &mut FilterGraphParameter,
    graph_desc: &str,
    mut frame_box: FrameBox,
    ifps: &mut Vec<InputFilterParameter>,
    ofps: &mut Vec<OutputFilterParameter>,
    input_filter_index: usize,
    frame_pool: &ObjPool<Frame>,
) -> crate::error::Result<()> {
    Ok(())
}

#[cfg(not(docsrs))]
unsafe fn fg_send_eof(
    fg_index: usize,
    graph: &mut Option<crate::raw::FilterGraph>,
    fgp: &mut FilterGraphParameter,
    graph_desc: &str,
    mut frame_box: FrameBox,
    ifps: &mut Vec<InputFilterParameter>,
    ofps: &mut Vec<OutputFilterParameter>,
    input_filter_index: usize,
    frame_pool: &ObjPool<Frame>,
) -> crate::error::Result<()> {
    let ifp = &ifps[input_filter_index];
    if ifp.eof {
        frame_pool.release(frame_box.frame);
        return Ok(());
    }

    let ifp = &mut ifps[input_filter_index];
    ifp.eof = true;

    let frame = frame_box.frame.as_mut_ptr();
    if !ifp.filter.is_null() {
        let pts = av_rescale_q_rnd(
            (*frame).pts,
            (*frame).time_base,
            ifp.time_base,
            AV_ROUND_NEAR_INF as u32 | AV_ROUND_PASS_MINMAX as u32,
        );
        let ret = av_buffersrc_close(ifp.filter, pts, AV_BUFFERSRC_FLAG_PUSH as u32);
        frame_pool.release(frame_box.frame);
        if ret < 0 {
            return Err(Error::FilterGraph(
                FilterGraphOperationError::BufferSourceCloseError(FilterGraphError::from(ret)),
            ));
        }
    } else {
        if ifp.format < 0 {
            // the filtergraph was never configured, use the fallback parameters

            let ifp = &mut ifps[input_filter_index];
            ifp.format = (*ifp.opts.fallback.as_ptr()).format;
            ifp.sample_rate = (*ifp.opts.fallback.as_ptr()).sample_rate;
            ifp.width = (*ifp.opts.fallback.as_ptr()).width;
            ifp.height = (*ifp.opts.fallback.as_ptr()).height;
            ifp.sample_aspect_ratio = (*ifp.opts.fallback.as_ptr()).sample_aspect_ratio;
            ifp.color_space = (*ifp.opts.fallback.as_ptr()).colorspace;
            ifp.color_range = (*ifp.opts.fallback.as_ptr()).color_range;
            ifp.time_base = (*ifp.opts.fallback.as_ptr()).time_base;

            let ret = av_channel_layout_copy(
                &mut ifp.ch_layout,
                &(*ifp.opts.fallback.as_ptr()).ch_layout,
            );
            if ret < 0 {
                frame_pool.release(frame_box.frame);
                return Err(Error::FilterGraph(
                    FilterGraphOperationError::ChannelLayoutCopyError(FilterGraphError::from(ret)),
                ));
            }

            if ifilter_has_all_input_formats(ifps) {
                if let Err(e) = configure_filtergraph(fg_index, graph, fgp, graph_desc, ifps, ofps)
                {
                    error!("Error initializing filters! {e}");
                    frame_pool.release(frame_box.frame);
                    return Err(e);
                }
            }
        }

        let ifp = &ifps[input_filter_index];
        if ifp.format < 0 {
            // Hard error, matching ffmpeg_filter.c:2746: a stream
            // that reached EOF without ever configuring its input leaves
            // the graph silent — fail loudly instead.
            error!(
                "Cannot determine format of input {} after EOF",
                ifp.opts.name
            );
            frame_pool.release(frame_box.frame);
            return Err(Error::FilterGraph(FilterGraphOperationError::InvalidData));
        }
        frame_pool.release(frame_box.frame);
    }

    Ok(())
}

const VIDEO_CHANGED: i32 = 1 << 0;
const AUDIO_CHANGED: i32 = 1 << 1;
const MATRIX_CHANGED: i32 = 1 << 2;
// Bit 3 belongs to DOWNMIX_CHANGED so the numbering stays aligned with the
// fftools n8.1 ReinitReason enum (which moved HWACCEL to 1 << 4 as well).
#[cfg(ffmpeg_8_0)]
const DOWNMIX_CHANGED: i32 = 1 << 3;
const HWACCEL_CHANGED: i32 = 1 << 4;
#[cfg(docsrs)]
unsafe fn fg_send_frame(
    fg_index: usize,
    graph: &mut Option<crate::raw::FilterGraph>,
    fgp: &mut FilterGraphParameter,
    graph_desc: &str,
    mut frame_box: FrameBox,
    ifps: &mut Vec<InputFilterParameter>,
    ofps: &mut Vec<OutputFilterParameter>,
    input_filter_index: usize,
    frame_pool: &ObjPool<Frame>,
) -> crate::error::Result<()> {
    Ok(())
}

#[cfg(not(docsrs))]
unsafe fn fg_send_frame(
    fg_index: usize,
    graph: &mut Option<crate::raw::FilterGraph>,
    fgp: &mut FilterGraphParameter,
    graph_desc: &str,
    mut frame_box: FrameBox,
    ifps: &mut Vec<InputFilterParameter>,
    ofps: &mut Vec<OutputFilterParameter>,
    input_filter_index: usize,
    frame_pool: &ObjPool<Frame>,
) -> crate::error::Result<()> {
    let frame = frame_box.frame.as_mut_ptr();
    let mut need_reinit = 0;

    let ifp = &ifps[input_filter_index];
    let media_type = ifp.media_type;
    // determine if the parameters for this input changed
    // Check audio or video parameters
    match media_type {
        AVMEDIA_TYPE_AUDIO => {
            if ifp.format != (*frame).format
                || ifp.sample_rate != (*frame).sample_rate
                || unsafe { av_channel_layout_compare(&ifp.ch_layout, &(*frame).ch_layout) } != 0
            {
                need_reinit |= AUDIO_CHANGED;
            }
        }
        AVMEDIA_TYPE_VIDEO => {
            if ifp.format != (*frame).format
                || ifp.width != (*frame).width
                || ifp.height != (*frame).height
                || ifp.color_space != (*frame).colorspace
                || ifp.color_range != (*frame).color_range
            {
                need_reinit |= VIDEO_CHANGED;
            }
        }
        _ => {}
    }

    // Check display matrix
    let sd = av_frame_get_side_data(frame, AV_FRAME_DATA_DISPLAYMATRIX);
    if !sd.is_null() {
        if !ifp.displaymatrix_present
            || std::slice::from_raw_parts((*sd).data, size_of_val(&ifp.displaymatrix))
                != std::slice::from_raw_parts(
                    ifp.displaymatrix.as_ptr() as *const u8,
                    size_of_val(&ifp.displaymatrix),
                )
        {
            need_reinit |= MATRIX_CHANGED;
        }
    } else if ifp.displaymatrix_present {
        need_reinit |= MATRIX_CHANGED;
    }

    // Check downmix info (FFmpeg 8+, fftools 4f9afbb1b2): aresample consumes
    // it at graph init, so a change between frames requires a reinit.
    #[cfg(ffmpeg_8_0)]
    {
        let sd = av_frame_get_side_data(frame, AV_FRAME_DATA_DOWNMIX_INFO);
        if !sd.is_null() {
            if !ifp.downmixinfo_present
                || std::slice::from_raw_parts((*sd).data, size_of_val(&ifp.downmixinfo))
                    != std::slice::from_raw_parts(
                        &ifp.downmixinfo as *const _ as *const u8,
                        size_of_val(&ifp.downmixinfo),
                    )
            {
                need_reinit |= DOWNMIX_CHANGED;
            }
        } else if ifp.downmixinfo_present {
            need_reinit |= DOWNMIX_CHANGED;
        }
    }

    // Always allow reinit
    /*if !graph.is_null() && !(ifp.opts.flags & IFILTER_FLAG_REINIT){
        need_reinit = 0;
    }*/

    if (ifp.hw_frames_ctx.is_null() != (*frame).hw_frames_ctx.is_null())
        || (!ifp.hw_frames_ctx.is_null()
            && (*ifp.hw_frames_ctx).data != (*(*frame).hw_frames_ctx).data)
    {
        need_reinit |= HWACCEL_CHANGED;
    }

    // Reinitialize if needed
    if need_reinit != 0 || graph.is_none() {
        ifilter_parameters_from_frame(&mut ifps[input_filter_index], frame, media_type)?;

        if !ifilter_has_all_input_formats(ifps) {
            let _ = &mut ifps[input_filter_index].frame_queue.push_back(frame_box);
            return Ok(());
        }

        if let Some(g) = graph.as_ref() {
            let ret = fg_read_frames(g.as_ptr(), fgp, ifps, ofps, frame_pool);
            if ret < 0 {
                return Err(Error::FilterGraph(
                    FilterGraphOperationError::ProcessFramesError(FilterGraphError::from(ret)),
                ));
            }

            let mut reason: AVBPrint = std::mem::zeroed();
            av_bprint_init(&mut reason, 0, AV_BPRINT_SIZE_AUTOMATIC as u32);

            if need_reinit & AUDIO_CHANGED != 0 {
                let fmt_str = CString::new("audio parameters changed to %d Hz, ").unwrap();
                let sample_format_name =
                    match crate::util::format_convert::sample_fmt_from_raw((*frame).format) {
                        Some(fmt) => av_get_sample_fmt_name(fmt),
                        None => null(),
                    };
                av_bprintf(&mut reason, fmt_str.as_ptr(), (*frame).sample_rate);
                av_channel_layout_describe_bprint(&(*frame).ch_layout, &mut reason);
                let comma_str = CString::new(", %s, ").unwrap();
                av_bprintf(
                    &mut reason,
                    comma_str.as_ptr(),
                    unknown_if_null(sample_format_name),
                );
            }

            if need_reinit & VIDEO_CHANGED != 0 {
                let pixel_format_name =
                    match crate::util::format_convert::pix_fmt_from_raw((*frame).format) {
                        Some(fmt) => av_get_pix_fmt_name(fmt),
                        None => null(),
                    };
                let color_space_name = av_color_space_name((*frame).colorspace);
                let color_range_name = av_color_range_name((*frame).color_range);

                let fmt_str =
                    CString::new("video parameters changed to %s(%s, %s), %dx%d, ").unwrap();
                av_bprintf(
                    &mut reason,
                    fmt_str.as_ptr(),
                    unknown_if_null(pixel_format_name),
                    unknown_if_null(color_range_name),
                    unknown_if_null(color_space_name),
                    (*frame).width,
                    (*frame).height,
                );
            }

            if need_reinit & MATRIX_CHANGED != 0 {
                let matrix_changed_str = CString::new("display matrix changed, ").unwrap();
                av_bprintf(&mut reason, matrix_changed_str.as_ptr());
            }

            #[cfg(ffmpeg_8_0)]
            if need_reinit & DOWNMIX_CHANGED != 0 {
                let downmix_changed_str = CString::new("downmix metadata changed, ").unwrap();
                av_bprintf(&mut reason, downmix_changed_str.as_ptr());
            }

            if need_reinit & HWACCEL_CHANGED != 0 {
                let hwaccel_changed_str = CString::new("hwaccel changed, ").unwrap();
                av_bprintf(&mut reason, hwaccel_changed_str.as_ptr());
            }

            // Remove the last comma if necessary
            if reason.len > 1 {
                let len = reason.len as usize;
                let reason_ptr = reason.str_ as *mut u8;
                *reason_ptr.add(len - 2) = 0; // Set the last comma to null terminator
            }

            let reason_str = CStr::from_ptr(reason.str_)
                .to_str()
                .unwrap_or("Unknown reason")
                .to_string();
            info!("Reconfiguring filter because graph {}", reason_str);
        }

        if let Err(e) = configure_filtergraph(fg_index, graph, fgp, graph_desc, ifps, ofps) {
            error!("Error reinitializing filters! {}", e);
            frame_pool.release(frame_box.frame);
            return Err(e);
        }
    }

    let ifp = &ifps[input_filter_index];

    (*frame).pts = av_rescale_q((*frame).pts, (*frame).time_base, ifp.time_base);
    (*frame).duration = av_rescale_q((*frame).duration, (*frame).time_base, ifp.time_base);
    (*frame).time_base = ifp.time_base;

    if ifp.displaymatrix_applied {
        av_frame_remove_side_data(frame, AV_FRAME_DATA_DISPLAYMATRIX);
    }

    let ret = av_buffersrc_add_frame_flags(ifp.filter, frame, AV_BUFFERSRC_FLAG_PUSH as u32 as i32);
    frame_pool.release(frame_box.frame);
    if ret < 0 {
        if ret != AVERROR_EOF {
            error!("Error while filtering: {}", av_err2str(ret));
        }
        return Err(Error::FilterGraph(
            FilterGraphOperationError::BufferSourceAddFrameError(FilterGraphError::from(ret)),
        ));
    }
    Ok(())
}

/// A view of an option string that treats both `None` and `""` as "unset".
#[cfg(not(docsrs))]
fn non_empty_opt(opt: &Option<String>) -> Option<&str> {
    match opt {
        Some(s) if !s.is_empty() => Some(s.as_str()),
        _ => None,
    }
}

/// Resolve the single graph-level value for one auto-conversion option
/// (sws or swr) from the graph-level override and the per-output requests.
///
/// FFmpeg's `scale_sws_opts` / `aresample_swr_opts` are graph-level, so a graph
/// can carry exactly one value:
///   * an explicit graph-level value (from a `FilterComplex`) always wins;
///   * otherwise the unique non-empty value among the bound outputs is used;
///   * two bound outputs asking for DIFFERENT non-empty values is a hard
///     `InvalidArgument` — we must not silently pick one.
#[cfg(not(docsrs))]
fn resolve_auto_conv_opt<'a>(
    graph_level: Option<&'a str>,
    per_output: impl Iterator<Item = Option<&'a str>>,
) -> crate::error::Result<Option<&'a str>> {
    if let Some(g) = graph_level {
        return Ok(Some(g));
    }
    let mut chosen: Option<&'a str> = None;
    for v in per_output.flatten() {
        match chosen {
            None => chosen = Some(v),
            Some(prev) if prev != v => {
                return Err(Error::FilterGraph(FilterGraphOperationError::ParseError(
                    FilterGraphParseError::InvalidArgument,
                )));
            }
            Some(_) => {}
        }
    }
    Ok(chosen)
}

/// `av_strdup` a resolved option string for storage on the graph. A NUL byte in
/// the string maps to `InvalidArgument`; allocation failure to `OutOfMemory`.
/// The returned pointer is owned by FFmpeg once written onto the graph.
#[cfg(not(docsrs))]
unsafe fn av_strdup_opt(s: &str) -> crate::error::Result<*mut c_char> {
    let c = CString::new(s).map_err(|_| {
        Error::FilterGraph(FilterGraphOperationError::ParseError(
            FilterGraphParseError::InvalidArgument,
        ))
    })?;
    let dup = av_strdup(c.as_ptr());
    if dup.is_null() {
        return Err(Error::FilterGraph(FilterGraphOperationError::ParseError(
            FilterGraphParseError::OutOfMemory,
        )));
    }
    Ok(dup)
}

/// NEW-SC-03: install the graph-level sws/swr option strings onto a freshly
/// allocated `AVFilterGraph` so libavfilter's **auto-inserted** `scale` /
/// `aresample` filters pick them up. Explicit `scale=`/`aresample=` filters the
/// user wrote keep their own arguments — only the auto-inserted ones read these
/// graph fields.
///
/// FFmpeg takes ownership of the `av_strdup`-ed strings and frees them in
/// `avfilter_graph_free`, matching fftools. Both option strings are resolved
/// first (either may raise a conflict error) before any allocation, so the only
/// FFI state we leave behind on error is the graph itself, which the caller
/// frees via `cleanup_filtergraph`.
///
/// # Safety
/// `graph_ptr` must point at a live `AVFilterGraph` whose `scale_sws_opts` /
/// `aresample_swr_opts` are still NULL (a just-allocated graph), so overwriting
/// them leaks nothing.
#[cfg(not(docsrs))]
unsafe fn apply_auto_conv_opts(
    graph_ptr: *mut AVFilterGraph,
    fgp: &FilterGraphParameter,
    ofps: &[OutputFilterParameter],
) -> crate::error::Result<()> {
    let sws = resolve_auto_conv_opt(
        non_empty_opt(&fgp.sws_opts),
        ofps.iter().map(|o| non_empty_opt(&o.opts.sws_opts)),
    )?;
    let swr = resolve_auto_conv_opt(
        non_empty_opt(&fgp.swr_opts),
        ofps.iter().map(|o| non_empty_opt(&o.opts.swr_opts)),
    )?;

    if let Some(sws) = sws {
        (*graph_ptr).scale_sws_opts = av_strdup_opt(sws)?;
    }
    if let Some(swr) = swr {
        (*graph_ptr).aresample_swr_opts = av_strdup_opt(swr)?;
    }
    Ok(())
}

#[cfg(docsrs)]
unsafe fn configure_filtergraph(
    fg_index: usize,
    graph: &mut Option<crate::raw::FilterGraph>,
    fgp: &mut FilterGraphParameter,
    graph_desc: &str,
    ifps: &mut Vec<InputFilterParameter>,
    ofps: &mut Vec<OutputFilterParameter>,
) -> crate::error::Result<()> {
    Ok(())
}

#[cfg(not(docsrs))]
unsafe fn configure_filtergraph(
    fg_index: usize,
    graph: &mut Option<crate::raw::FilterGraph>,
    fgp: &mut FilterGraphParameter,
    graph_desc: &str,
    ifps: &mut Vec<InputFilterParameter>,
    ofps: &mut Vec<OutputFilterParameter>,
) -> crate::error::Result<()> {
    cleanup_filtergraph(graph, ifps, ofps);
    *graph = Some(crate::raw::FilterGraph::alloc().ok_or(Error::FilterGraph(
        FilterGraphOperationError::ParseError(FilterGraphParseError::OutOfMemory),
    ))?);
    // Raw pointer into the just-allocated graph. It stays valid until a
    // `cleanup_filtergraph` on an error path drops the owner — and every such
    // path returns immediately, so `graph_ptr` is never read after the drop.
    let graph_ptr = graph.as_ref().unwrap().as_ptr();
    (*graph_ptr).nb_threads = 0;

    // NEW-SC-03: apply the resolved graph-level sws/swr option strings to the
    // auto-inserted scale/aresample filters BEFORE parse/config. On a resolution
    // conflict or OOM, free the just-allocated graph first (same as every other
    // error path in this function), then surface the error.
    if let Err(e) = apply_auto_conv_opts(graph_ptr, fgp, ofps) {
        cleanup_filtergraph(graph, ifps, ofps);
        return Err(e);
    }

    let hw_device = hw_device_for_filter();

    // The AVFilterInOut lists are FFmpeg-allocated out-params the caller must
    // free; owning them in `raw::FilterInOut` frees each exactly once on every
    // path (including every error return below and an unwind), so there is no
    // manual `avfilter_inout_free` and no `?`/early-return leak. The lists are
    // only walked here (never node-consumed by linking), so holding them until
    // scope end — rather than freeing mid-function — is equivalent.
    let mut inputs = crate::raw::FilterInOut::empty();
    let mut outputs = crate::raw::FilterInOut::empty();
    let mut ret = graph_parse(
        graph_ptr,
        graph_desc,
        inputs.as_out_ptr(),
        outputs.as_out_ptr(),
        hw_device,
    );
    if ret < 0 {
        cleanup_filtergraph(graph, ifps, ofps);
        return Err(Error::FilterGraph(FilterGraphOperationError::ParseError(
            FilterGraphParseError::from(ret),
        )));
    }

    let mut cur = inputs.as_ptr();
    let mut i = 0;
    loop {
        if cur.is_null() {
            break;
        }

        let ifp = ifps.get_mut(i);
        if ifp.is_none() {
            error!("input[{i}] can't find matched InputFilterParameter");
            break;
        }

        let ret = configure_input_filter(fg_index, graph_ptr, ifp.unwrap(), cur);
        if ret < 0 {
            cleanup_filtergraph(graph, ifps, ofps);
            return Err(Error::FilterGraph(FilterGraphOperationError::ParseError(
                FilterGraphParseError::from(ret),
            )));
        }

        cur = (*cur).next;
        i += 1;
    }

    cur = outputs.as_ptr();
    i = 0;
    loop {
        if cur.is_null() {
            break;
        }

        let ofp = ofps.get_mut(i);
        if ofp.is_none() {
            error!("output[{i}] can't find matched OutputFilterParameter");
            break;
        }

        let ret = configure_output_filter(graph_ptr, ofp.unwrap(), cur);
        if ret < 0 {
            cleanup_filtergraph(graph, ifps, ofps);
            return Err(Error::FilterGraph(FilterGraphOperationError::ParseError(
                FilterGraphParseError::from(ret),
            )));
        }

        cur = (*cur).next;
        i += 1;
    }

    //TODO disable_conversions

    ret = avfilter_graph_config(graph_ptr, null_mut());
    if ret < 0 {
        cleanup_filtergraph(graph, ifps, ofps);
        return Err(Error::FilterGraph(FilterGraphOperationError::ParseError(
            FilterGraphParseError::from(ret),
        )));
    }

    fgp.is_meta = graph_is_meta(graph_ptr);

    /* limit the lists of allowed formats to the ones selected, to
     * make sure they stay the same if the filtergraph is reconfigured later */
    for ofp in &mut *ofps {
        // `ofp.format` is AVPixelFormat-typed but carries a sample format for
        // audio sinks; both are contiguous #[repr(i32)] enums and every valid
        // sample-format value is < AV_PIX_FMT_NB, so a range-checked pixel-fmt
        // conversion round-trips either case without the UB risk of a bare
        // transmute on an out-of-range int. (Rung 2 retypes this field to i32.)
        ofp.format =
            crate::util::format_convert::pix_fmt_from_raw(av_buffersink_get_format(ofp.filter))
                .unwrap_or(AV_PIX_FMT_NONE);
        ofp.width = av_buffersink_get_w(ofp.filter);
        ofp.height = av_buffersink_get_h(ofp.filter);
        ofp.color_space = av_buffersink_get_colorspace(ofp.filter);
        ofp.color_range = av_buffersink_get_color_range(ofp.filter);

        // Tentative value only: once frames chose a timebase it is locked
        // and a graph reconfig must not drift it (ffmpeg_filter.c:1973-1975).
        if !ofp.tb_out_locked {
            ofp.tb_out = av_buffersink_get_time_base(ofp.filter);
        }

        ofp.sample_aspect_ratio = av_buffersink_get_sample_aspect_ratio(ofp.filter);

        ofp.sample_rate = av_buffersink_get_sample_rate(ofp.filter);

        av_channel_layout_uninit(&mut ofp.opts.ch_layout);
        ret = av_buffersink_get_ch_layout(ofp.filter, &mut ofp.opts.ch_layout);
        if ret < 0 {
            cleanup_filtergraph(graph, ifps, ofps);
            return Err(Error::FilterGraph(FilterGraphOperationError::ParseError(
                FilterGraphParseError::from(ret),
            )));
        }

        // FFmpeg 8+: merge the side data this sink negotiated over the
        // previous configuration's entries (REPLACE keeps entries older
        // configurations produced, matching n8.1 configure_filtergraph),
        // then freeze the snapshot the encoder side reads via FrameData.
        #[cfg(ffmpeg_8_0)]
        {
            let mut merged = SideDataList::new();
            // Arc-clone the previous snapshot first: iterating it through
            // `ofp` would keep `ofps` borrowed across the error paths, which
            // hand `ofps` to cleanup_filtergraph again (E0499).
            let prev = ofp.side_data.clone();
            if let Some(prev) = prev {
                for sd in prev.iter() {
                    let err = merged.push_clone(sd, 0);
                    if err < 0 {
                        cleanup_filtergraph(graph, ifps, ofps);
                        return Err(Error::FilterGraph(
                            FilterGraphOperationError::FrameSideDataCloneError(
                                FilterGraphError::from(err),
                            ),
                        ));
                    }
                }
            }
            let mut nb_sd = 0;
            let sd_arr = av_buffersink_get_side_data(ofp.filter, &mut nb_sd);
            for j in 0..nb_sd {
                let err = merged.push_clone(
                    *sd_arr.offset(j as isize),
                    AV_FRAME_SIDE_DATA_FLAG_REPLACE as u32,
                );
                if err < 0 {
                    cleanup_filtergraph(graph, ifps, ofps);
                    return Err(Error::FilterGraph(
                        FilterGraphOperationError::FrameSideDataCloneError(FilterGraphError::from(
                            err,
                        )),
                    ));
                }
            }
            ofp.side_data = Some(Arc::new(merged));
        }
    }

    for ifp in &mut *ifps {
        loop {
            let option = ifp.frame_queue.pop_front();
            if option.is_none() {
                break;
            }
            let tmp_frame_box = option.unwrap();
            if ifp.media_type == AVMEDIA_TYPE_SUBTITLE {
                // sub2video is not supported: drop the queued frame instead
                // of silently keeping a dead branch.
                error!(
                    "subtitle frames cannot feed a filtergraph input: \
                     sub2video is not supported"
                );
            } else {
                let mut tmp_frame = unsafe { av_frame_alloc() };
                if tmp_frame.is_null() {
                    cleanup_filtergraph(graph, ifps, ofps);
                    return Err(Error::FilterGraph(
                        FilterGraphOperationError::BufferSourceAddFrameError(
                            FilterGraphError::OutOfMemory,
                        ),
                    ));
                }
                ret = av_frame_ref(tmp_frame, tmp_frame_box.frame.as_ptr());
                if ret < 0 {
                    av_frame_free(&mut tmp_frame);
                    cleanup_filtergraph(graph, ifps, ofps);
                    return Err(Error::FilterGraph(
                        FilterGraphOperationError::BufferSourceAddFrameError(
                            FilterGraphError::from(ret),
                        ),
                    ));
                }
                // Frames queued before the first configuration still carry
                // their display matrix; autorotate already consumed it, so
                // strip it like the direct send path does. Upstream fixed
                // this on the 8.x line only (01f63ef0b4), hence the gate:
                // 7.x stays bit-identical with fftools n7.1.
                #[cfg(ffmpeg_8_0)]
                if ifp.media_type == AVMEDIA_TYPE_VIDEO && ifp.displaymatrix_applied {
                    av_frame_remove_side_data(tmp_frame, AV_FRAME_DATA_DISPLAYMATRIX);
                }
                ret = av_buffersrc_add_frame(ifp.filter, tmp_frame);
                // add_frame moves the data references out of tmp_frame but
                // never owns the shell: free it on success and failure alike
                // (the success path leaked one AVFrame per replayed frame).
                av_frame_free(&mut tmp_frame);
                if ret < 0 {
                    // Fail immediately: continuing the replay would silently
                    // drop this frame and a later success overwrote `ret`.
                    cleanup_filtergraph(graph, ifps, ofps);
                    return Err(Error::FilterGraph(
                        FilterGraphOperationError::BufferSourceAddFrameError(
                            FilterGraphError::from(ret),
                        ),
                    ));
                }
            }
        }
    }

    let mut have_input_eof = false;
    /* send the EOFs for the finished inputs */
    for ifp in &mut *ifps {
        if ifp.eof {
            ret = av_buffersrc_add_frame(ifp.filter, null_mut());
            if ret < 0 {
                cleanup_filtergraph(graph, ifps, ofps);
                return Err(Error::FilterGraph(
                    FilterGraphOperationError::BufferSourceAddFrameError(FilterGraphError::from(
                        ret,
                    )),
                ));
            }
            have_input_eof = true;
        }
    }

    if have_input_eof {
        // make sure the EOF propagates to the end of the graph
        ret = avfilter_graph_request_oldest(graph_ptr);
        if ret < 0 && ret != AVERROR(EAGAIN) && ret != AVERROR_EOF {
            cleanup_filtergraph(graph, ifps, ofps);
            return Err(Error::FilterGraph(
                FilterGraphOperationError::RequestOldestError(FilterGraphError::from(ret)),
            ));
        }
    }

    Ok(())
}

#[cfg(not(docsrs))]
unsafe fn graph_is_meta(graph: *mut AVFilterGraph) -> bool {
    for i in 0..(*graph).nb_filters {
        unsafe {
            let filter_context = *(*graph).filters.add(i as usize);
            let filter = (*filter_context).filter;

            if !((*filter).flags & AVFILTER_FLAG_METADATA_ONLY != 0
                || (*filter_context).nb_outputs == 0
                || filter_is_buffersrc(filter_context))
            {
                return false;
            }
        }
    }
    true
}

fn filter_is_buffersrc(f: *mut AVFilterContext) -> bool {
    unsafe {
        (*f).nb_inputs == 0
            && (CStr::from_ptr((*(*f).filter).name) == c"buffer"
                || CStr::from_ptr((*(*f).filter).name) == c"abuffer")
    }
}

unsafe fn configure_output_filter(
    graph: *mut AVFilterGraph,
    ofp: &mut OutputFilterParameter,
    output: *mut AVFilterInOut,
) -> i32 {
    match ofp.media_type {
        AVMEDIA_TYPE_VIDEO => configure_output_video_filter(graph, ofp, output),
        AVMEDIA_TYPE_AUDIO => configure_output_audio_filter(graph, ofp, output),
        _ => {
            error!("Unexpected media type {:?}", ofp.media_type);
            0
        }
    }
}

#[cfg(docsrs)]
unsafe fn configure_output_audio_filter(
    graph: *mut AVFilterGraph,
    ofp: &mut OutputFilterParameter,
    output: *mut AVFilterInOut,
) -> i32 {
    0
}

#[cfg(not(docsrs))]
unsafe fn configure_output_audio_filter(
    graph: *mut AVFilterGraph,
    ofp: &mut OutputFilterParameter,
    output: *mut AVFilterInOut,
) -> i32 {
    let mut pad_idx = (*output).pad_idx;
    let mut last_filter = (*output).filter_ctx;

    let result = CString::new(format!("out_{}", ofp.opts.name));
    if result.is_err() {
        return AVERROR(ENOMEM);
    }
    let name = result.unwrap();

    let abuffer_str = std::ffi::CString::new("abuffersink").unwrap();
    let abuffer_filter = avfilter_get_by_name(abuffer_str.as_ptr());

    let mut ret = avfilter_graph_create_filter(
        &mut ofp.filter,
        abuffer_filter,
        name.as_ptr(),
        null(),
        null_mut(),
        graph,
    );
    if ret < 0 {
        return ret;
    }

    // fftools n7.1 set "all_channel_counts" on the freshly created abuffersink
    // here. Do not re-add it when re-porting: FFmpeg 8 rejects setting a
    // non-runtime option after avfilter_graph_create_filter() has initialized
    // the filter (EINVAL, see ez-ffmpeg issue #37), FFmpeg 9 (lavfi 12)
    // removes the option entirely, and it was a no-op all along — fftools
    // n8.0 dropped the call.

    let mut bprint = AVBPrint {
        str_: null_mut(),
        len: 0,
        size: 0,
        size_max: 0,
        reserved_internal_buffer: [0; 1],
        reserved_padding: [0; 1000],
    };
    av_bprint_init(&mut bprint, 0, u32::MAX);

    choose_sample_fmts(
        &mut bprint,
        ofp.opts.audio_format,
        ofp.opts.audio_formats.clone(),
    );
    choose_sample_rates(
        &mut bprint,
        ofp.opts.sample_rate,
        ofp.opts.sample_rates.clone(),
    );
    choose_channel_layouts(&mut bprint, ofp.opts.ch_layout, ofp.opts.ch_layouts.clone());

    if bprint.len >= bprint.size {
        av_bprint_finalize(&mut bprint, null_mut());
        return AVERROR(ENOMEM);
    }

    if bprint.len > 0 {
        let mut filter = null_mut();

        let result = CString::new(format!("format_out_{}", ofp.name));
        if result.is_err() {
            av_bprint_finalize(&mut bprint, null_mut());
            return AVERROR(ENOMEM);
        }
        let name = result.unwrap();

        let format_out_str = CString::new("aformat").unwrap();
        let format_out_filter = avfilter_get_by_name(format_out_str.as_ptr());
        let mut ret = avfilter_graph_create_filter(
            &mut filter,
            format_out_filter,
            name.as_ptr(),
            bprint.str_,
            null_mut(),
            graph,
        );
        if ret < 0 {
            av_bprint_finalize(&mut bprint, null_mut());
            return ret;
        }

        ret = avfilter_link(last_filter, pad_idx as u32, filter, 0);
        if ret < 0 {
            av_bprint_finalize(&mut bprint, null_mut());
            return ret;
        }

        last_filter = filter;
        pad_idx = 0;
    }

    //TODO auto apad

    let name = format!("trim_out_{}", ofp.name);
    ret = insert_trim(
        ofp.opts.trim_start_us,
        ofp.opts.trim_duration_us,
        &mut last_filter,
        &mut pad_idx,
        &name,
    );
    if ret < 0 {
        return ret;
    }

    ret = avfilter_link(last_filter, pad_idx as u32, ofp.filter, 0);
    if ret < 0 {
        av_bprint_finalize(&mut bprint, null_mut());
        return ret;
    }

    0
}

fn insert_trim(
    start_time: Option<i64>,
    duration: Option<i64>,
    last_filter: *mut *mut AVFilterContext,
    pad_idx: &mut i32,
    filter_name: &str,
) -> i32 {
    if start_time.is_none() && duration.is_none() {
        return 0;
    }
    unsafe {
        let graph = (**last_filter).graph;
        let type_ = avfilter_pad_get_type((**last_filter).output_pads, *pad_idx);
        let name = if type_ == AVMEDIA_TYPE_VIDEO {
            "trim".to_string()
        } else {
            "atrim".to_string()
        };
        let mut ret = 0;

        let name_cstring = CString::new(name.clone()).unwrap();
        let trim = avfilter_get_by_name(name_cstring.as_ptr());
        if trim.is_null() {
            error!("{name} filter not present, cannot limit recording time.");
            return ffmpeg_sys_next::AVERROR_FILTER_NOT_FOUND;
        }

        let Ok(filter_name_cstring) = CString::new(filter_name) else {
            return AVERROR(EINVAL);
        };
        let ctx =
            ffmpeg_sys_next::avfilter_graph_alloc_filter(graph, trim, filter_name_cstring.as_ptr());
        if ctx.is_null() {
            return AVERROR(ENOMEM);
        }

        if let Some(duration) = duration {
            let durationi_cstring = CString::new("durationi").unwrap();

            ret = av_opt_set_int(
                ctx as *mut _,
                durationi_cstring.as_ptr(),
                duration,
                AV_OPT_SEARCH_CHILDREN,
            );
        }

        if ret >= 0 {
            if let Some(start_time) = start_time {
                let starti_cstring = CString::new("starti").unwrap();
                ret = av_opt_set_int(
                    ctx as *mut _,
                    starti_cstring.as_ptr(),
                    start_time,
                    AV_OPT_SEARCH_CHILDREN,
                );
            }
        }
        if ret < 0 {
            error!("Error configuring the {name} filter");
            return ret;
        }

        ret = ffmpeg_sys_next::avfilter_init_str(ctx, null_mut());
        if ret < 0 {
            return ret;
        }

        ret = avfilter_link(*last_filter, *pad_idx as u32, ctx, 0);
        if ret < 0 {
            return ret;
        }

        *last_filter = ctx;
        *pad_idx = 0;
    }

    0
}

/// Tears down the current graph so it can be (re)configured.
///
/// Mirrors fftools' cleanup_filtergraph: only the filter pointers and the
/// graph itself are released. `ifp.hw_frames_ctx` must survive — it is an
/// input PARAMETER (set from the incoming frame by
/// `ifilter_parameters_from_frame` right before reconfiguration) that the
/// next `configure_filtergraph` hands to buffersrc. Unrefing it here made
/// every hw-frame filter chain fail with "Setting BufferSourceContext.pix_fmt
/// to a HW format requires hw_frames_ctx to be non-NULL". Its lifetime ends
/// in `free_input_filter_resources` when the filter task exits.
fn cleanup_filtergraph(
    graph: &mut Option<crate::raw::FilterGraph>,
    ifps: &mut Vec<InputFilterParameter>,
    ofps: &mut Vec<OutputFilterParameter>,
) {
    for input_filter_parameter in ifps {
        input_filter_parameter.filter = null_mut();
    }
    for output_filter_parameter in ofps {
        output_filter_parameter.filter = null_mut();
    }
    // Drop the owned graph (if any), freeing it via avfilter_graph_free. The
    // filter back-refs nulled above point into the graph, so they are cleared
    // before this drop, never after — a naive drop that skipped the nulling
    // would leave every ifp/ofp.filter dangling into the freed graph.
    let _ = graph.take();
}

/// Final release of per-input resources owned by the filter task, called once
/// when the task thread ends (counterpart of fftools' ifilter_free).
fn free_input_filter_resources(ifps: &mut Vec<InputFilterParameter>) {
    for input_filter_parameter in ifps {
        if !input_filter_parameter.hw_frames_ctx.is_null() {
            unsafe { av_buffer_unref(&mut input_filter_parameter.hw_frames_ctx) };
        }
        // Custom channel layouts allocated by av_channel_layout_copy. The
        // field itself is gated the same way (docs.rs builds stub it out).
        #[cfg(not(docsrs))]
        unsafe {
            ffmpeg_sys_next::av_channel_layout_uninit(&mut input_filter_parameter.ch_layout)
        };
    }
}

unsafe fn fg_read_frames(
    graph: *mut AVFilterGraph,
    fgp: &mut FilterGraphParameter,
    ifps: &Vec<InputFilterParameter>,
    ofps: &mut Vec<OutputFilterParameter>,
    frame_pool: &ObjPool<Frame>,
) -> i32 {
    // graph not configured, just select the input to request
    if graph.is_null() {
        for ifp in ifps {
            if ifp.format < 0 && !ifp.eof {
                fgp.next_in = ifp.input_filter_index;
                return 0;
            }
        }
        return AVERROR_BUG;
    }

    loop {
        if fgp.nb_outputs_done >= ofps.len() {
            break;
        }

        let ret = avfilter_graph_request_oldest(graph);
        if ret == AVERROR(EAGAIN) {
            fgp.next_in = choose_input(ifps);
            break;
        } else if ret < 0 {
            if ret == AVERROR_EOF {
                trace!("Filtergraph returned EOF, finishing");
            } else {
                error!(
                    "Error requesting a frame from the filtergraph: {}",
                    av_err2str(ret)
                );
            }
            return ret;
        }

        //TODO rate-control

        for ofp in &mut *ofps {
            loop {
                let ret = fg_output_step(fgp, ofp, frame_pool);
                if ret < 0 {
                    return ret;
                }
                if ret != 0 {
                    break;
                }
            }
        }
    }

    if fgp.nb_outputs_done == ofps.len() {
        AVERROR_EOF
    } else {
        0
    }
}

fn choose_input(ifps: &Vec<InputFilterParameter>) -> usize {
    let mut nb_requests_max = -1;
    let mut best_input: i32 = -1;

    for ifp in ifps {
        if ifp.eof {
            continue;
        }
        let nb_requests = unsafe { av_buffersrc_get_nb_failed_requests(ifp.filter) } as i32;
        if nb_requests > nb_requests_max {
            nb_requests_max = nb_requests;
            best_input = ifp.input_filter_index as i32;
        }
    }

    assert!(best_input >= 0);

    best_input as usize
}

#[cfg(docsrs)]
unsafe fn fg_output_step(
    fgp: &mut FilterGraphParameter,
    ofp: &mut OutputFilterParameter,
    frame_pool: &ObjPool<Frame>,
) -> i32 {
    0
}

#[cfg(not(docsrs))]
unsafe fn fg_output_step(
    fgp: &mut FilterGraphParameter,
    ofp: &mut OutputFilterParameter,
    frame_pool: &ObjPool<Frame>,
) -> i32 {
    let Ok(mut frame) = frame_pool.get() else {
        return AVERROR(ffmpeg_sys_next::ENOMEM);
    };
    let mut ret = av_buffersink_get_frame_flags(
        ofp.filter,
        frame.as_mut_ptr(),
        AV_BUFFERSINK_FLAG_NO_REQUEST,
    );

    if ret == AVERROR_EOF && !ofp.eof {
        frame_pool.release(frame);
        ret = fg_output_frame(fgp, ofp, null_frame(), frame_pool);
        return if ret < 0 { ret } else { 1 };
    } else if ret == AVERROR(EAGAIN) || ret == AVERROR_EOF {
        frame_pool.release(frame);
        return 1;
    } else if ret < 0 {
        frame_pool.release(frame);
        warn!(
            "Error in retrieving a frame from the filtergraph: {}",
            av_err2str(ret)
        );
        return ret;
    }

    if ofp.eof {
        frame_pool.release(frame);
        return 0;
    }

    // Choose the output timebase the first time we get a frame, then lock
    // it: re-choosing after a reconfig would shift all downstream
    // timestamps (ffmpeg_filter.c:2527-2528; locked at :2190).
    if !ofp.tb_out_locked {
        choose_out_timebase(ofp, &frame);
        ofp.tb_out_locked = true;
    }
    (*frame.as_mut_ptr()).time_base = av_buffersink_get_time_base(ofp.filter);

    /*if !fgp.is_meta {

    }*/

    if ofp.media_type == AVMEDIA_TYPE_VIDEO && (*frame.as_ptr()).duration == 0 {
        let fr = av_buffersink_get_frame_rate(ofp.filter);
        if fr.num > 0 && fr.den > 0 {
            (*frame.as_mut_ptr()).duration =
                av_rescale_q(1, av_inv_q(fr), (*frame.as_ptr()).time_base);
        }
    }

    ret = fg_output_frame(fgp, ofp, frame, frame_pool);
    if ret < 0 {
        return ret;
    }

    0
}

unsafe fn choose_out_timebase(ofp: &mut OutputFilterParameter, frame: &Frame) {
    let mut tb = AVRational { num: 0, den: 0 };
    if ofp.media_type == AVMEDIA_TYPE_AUDIO {
        tb = AVRational {
            num: 1,
            den: (*frame.as_ptr()).sample_rate,
        };
        ofp.tb_out = tb;
        return;
    }

    let mut fr = ofp.fpsconv_context.framerate;
    if fr.num == 0 {
        let fr_sink = av_buffersink_get_frame_rate(ofp.filter);
        if fr_sink.num > 0 && fr_sink.den > 0 {
            fr = fr_sink;
        }
    }

    if let Some(vsync_method) = ofp.opts.vsync_method {
        if vsync_method == VsyncCfr || vsync_method == VsyncVscfr {
            if fr.num == 0 && ofp.fpsconv_context.framerate_max.num == 0 {
                fr = AVRational { num: 25, den: 1 };
                warn!(
                    "No information \
                about the input framerate is available. Falling \
                back to a default value of 25fps. Use the `framerate` option \
                if you want a different framerate."
                );
            }

            // Cap only when the rate exceeds the bound or is invalid:
            // `fr.den != 0` here was a mistranslation of fftools `!fr.den`
            // and made the cap unconditional (ffmpeg_filter.c:2165-2168).
            if ofp.fpsconv_context.framerate_max.num != 0
                && (av_q2d(fr) > av_q2d(ofp.fpsconv_context.framerate_max) || fr.den == 0)
            {
                fr = ofp.fpsconv_context.framerate_max;
            }
        }
    }

    if !(tb.num > 0 && tb.den > 0) {
        tb = av_inv_q(fr);
    }
    if !(tb.num > 0 && tb.den > 0) {
        tb = (*frame.as_ptr()).time_base;
    }

    ofp.fpsconv_context.framerate = fr;

    ofp.tb_out = tb;
}

#[cfg(docsrs)]
unsafe fn fg_output_frame(
    fgp: &mut FilterGraphParameter,
    ofp: &mut OutputFilterParameter,
    mut frame: Frame,
    frame_pool: &ObjPool<Frame>,
) -> i32 {
    0
}

#[cfg(not(docsrs))]
unsafe fn fg_output_frame(
    fgp: &mut FilterGraphParameter,
    ofp: &mut OutputFilterParameter,
    mut frame: Frame,
    frame_pool: &ObjPool<Frame>,
) -> i32 {
    if !ofp.finished_flag_list.is_empty()
        && ofp.fg_input_index < ofp.finished_flag_list.len()
        && ofp.finished_flag_list[ofp.fg_input_index].load(Ordering::Acquire)
    {
        ofp.eof = true;
        fgp.nb_outputs_done += 1;
        return 0;
    }

    let mut nb_frames = if !frame_is_null(&frame) { 1 } else { 0 };
    let mut nb_frames_prev = 0;

    if ofp.media_type == AVMEDIA_TYPE_VIDEO && (!frame_is_null(&frame) || ofp.got_frame) {
        unsafe { video_sync_process(ofp, frame.as_mut_ptr(), &mut nb_frames, &mut nb_frames_prev) };
    }

    let frame_prev = &ofp.fpsconv_context.last_frame;
    for i in 0..nb_frames {
        let frame_out = if ofp.media_type == AVMEDIA_TYPE_VIDEO {
            let frame_in = if i < nb_frames_prev && !(*frame_prev.as_ptr()).buf[0].is_null() {
                frame_prev
            } else {
                &frame
            };

            if frame_is_null(frame_in) {
                break;
            }

            let Ok(mut out) = frame_pool.get() else {
                return AVERROR(ffmpeg_sys_next::ENOMEM);
            };
            let ret = av_frame_ref(out.as_mut_ptr(), frame_in.as_ptr());
            if ret < 0 {
                return ret;
            }

            (*out.as_mut_ptr()).pts = ofp.next_pts;

            if ofp.fpsconv_context.dropped_keyframe {
                (*out.as_mut_ptr()).flags |= AV_FRAME_FLAG_KEY;
                ofp.fpsconv_context.dropped_keyframe = false;
            }

            out
        } else {
            // let frame = frame;
            let Ok(mut out) = frame_pool.get() else {
                return AVERROR(ffmpeg_sys_next::ENOMEM);
            };
            (*frame.as_mut_ptr()).pts = if (*frame.as_ptr()).pts == AV_NOPTS_VALUE {
                ofp.next_pts
            } else {
                av_rescale_q(
                    (*frame.as_ptr()).pts,
                    (*frame.as_ptr()).time_base,
                    ofp.tb_out,
                ) - av_rescale_q(ofp.opts.ts_offset.unwrap_or(0), AV_TIME_BASE_Q, ofp.tb_out)
            };
            (*frame.as_mut_ptr()).time_base = ofp.tb_out;
            (*frame.as_mut_ptr()).duration = av_rescale_q(
                (*frame.as_ptr()).nb_samples as i64,
                AVRational {
                    num: 1,
                    den: (*frame.as_ptr()).sample_rate,
                },
                ofp.tb_out,
            );

            ofp.next_pts = (*frame.as_ptr()).pts + (*frame.as_ptr()).duration;

            let ret = av_frame_ref(out.as_mut_ptr(), frame.as_ptr());
            if ret < 0 {
                return ret;
            }
            out
        };

        // send the frame to consumers
        if let Some(dst) = ofp.dst.as_ref() {
            let framerate = if ofp.opts.framerate.den == 0 {
                None
            } else {
                Some(ofp.opts.framerate)
            };

            let frame_box = FrameBox {
                frame: frame_out,
                frame_data: FrameData {
                    framerate,
                    bits_per_raw_sample: 0,
                    input_stream_width: 0,
                    input_stream_height: 0,
                    subtitle_header: None,
                    fg_input_index: ofp.fg_input_index,
                    // Only the frame that opens the encoder needs the graph's
                    // side data snapshot (fftools fg_output_step).
                    side_data: if ofp.got_frame {
                        None
                    } else {
                        ofp.side_data.clone()
                    },
                },
            };

            if let Err(_) = dst.send(frame_box) {
                if !ofp.eof {
                    ofp.eof = true;
                    fgp.nb_outputs_done += 1;
                }
                return 0;
            }
        }

        if ofp.media_type == AVMEDIA_TYPE_VIDEO {
            ofp.fpsconv_context.frame_number += 1;
            ofp.next_pts += 1;

            if i == nb_frames_prev && !frame_is_null(&frame) {
                (*frame.as_mut_ptr()).flags &= !AV_FRAME_FLAG_KEY;
            }
        }

        ofp.got_frame = true;
    }

    let frame_is_null = frame_is_null(&frame);
    if !frame_is_null {
        if ofp.media_type == AVMEDIA_TYPE_VIDEO {
            let frame_prev = std::mem::replace(&mut ofp.fpsconv_context.last_frame, frame);
            frame_pool.release(frame_prev);
        } else {
            // Audio (and any non-video) sink frame: fg_output_frame already
            // copied the buffers into a separate pooled shell via av_frame_ref
            // and sent that downstream, so this original sink shell can return
            // to the pool instead of Drop-freeing it every output frame
            // (alloc-05). release() only unrefs this shell; the refcounted
            // buffers now held by the sent frame are untouched.
            frame_pool.release(frame);
        }
    }

    if frame_is_null {
        return close_output(ofp, frame_pool);
    }

    0
}

#[cfg(docsrs)]
unsafe fn close_output(ofp: &mut OutputFilterParameter, frame_pool: &ObjPool<Frame>) -> i32 {
    0
}

#[cfg(not(docsrs))]
unsafe fn close_output(ofp: &mut OutputFilterParameter, frame_pool: &ObjPool<Frame>) -> i32 {
    // we are finished and no frames were ever seen at this output,
    // at least initialize the encoder with a dummy frame

    if !ofp.got_frame {
        let Ok(mut frame) = frame_pool.get() else {
            return AVERROR(ffmpeg_sys_next::ENOMEM);
        };

        (*frame.as_mut_ptr()).time_base = ofp.tb_out;
        // `ofp.format` is a typed enum; widening to the raw `c_int` field is a
        // plain `as` cast, never a transmute.
        (*frame.as_mut_ptr()).format = ofp.format as i32;

        (*frame.as_mut_ptr()).width = ofp.width;
        (*frame.as_mut_ptr()).height = ofp.height;
        (*frame.as_mut_ptr()).sample_aspect_ratio = ofp.sample_aspect_ratio;

        (*frame.as_mut_ptr()).sample_rate = ofp.sample_rate;

        if ofp.opts.ch_layout.nb_channels != 0 {
            // Copy from the output filter's negotiated layout; copying the
            // frame's own (empty) layout onto itself left the dummy audio
            // frame without channels (ffmpeg_filter.c close_output copies
            // from ofp->ch_layout).
            let ret =
                av_channel_layout_copy(&mut (*frame.as_mut_ptr()).ch_layout, &ofp.opts.ch_layout);
            if ret < 0 {
                return ret;
            }
        }

        warn!("No filtered frames for output stream, trying to initialize anyway.");

        if let Some(dst) = ofp.dst.clone() {
            let framerate = if ofp.opts.framerate.den == 0 {
                None
            } else {
                Some(ofp.opts.framerate)
            };

            let frame_box = FrameBox {
                frame,
                frame_data: FrameData {
                    framerate,
                    bits_per_raw_sample: 0,
                    input_stream_width: 0,
                    input_stream_height: 0,
                    subtitle_header: None,
                    fg_input_index: ofp.fg_input_index,
                    // The dummy init frame carries the snapshot too
                    // (n8.1 close_output).
                    side_data: ofp.side_data.clone(),
                },
            };

            if let Err(_) = dst.send(frame_box) {
                return AVERROR(ffmpeg_sys_next::EOF);
            }
        }
    }

    ofp.eof = true;

    if let Some(dst) = ofp.dst.clone() {
        let frame_box = FrameBox {
            frame: null_frame(),
            frame_data: FrameData {
                framerate: None,
                bits_per_raw_sample: 0,
                input_stream_width: 0,
                input_stream_height: 0,
                subtitle_header: None,
                fg_input_index: ofp.fg_input_index,
                side_data: None,
            },
        };

        if let Err(_) = dst.send(frame_box) {
            return AVERROR(ffmpeg_sys_next::EOF);
        }
    }

    0
}

#[cfg(docsrs)]
unsafe fn video_sync_process(
    ofp: &mut OutputFilterParameter,
    frame: *mut AVFrame,
    nb_frames: &mut i64,
    nb_frames_prev: &mut i64,
) {
}

#[cfg(not(docsrs))]
unsafe fn video_sync_process(
    ofp: &mut OutputFilterParameter,
    frame: *mut AVFrame,
    nb_frames: &mut i64,
    nb_frames_prev: &mut i64,
) {
    let Some(vsync_method) = ofp.opts.vsync_method else {
        error!("No vsync method on video sync!!!");
        return;
    };

    let fps = &mut ofp.fpsconv_context;

    if frame.is_null() {
        *nb_frames_prev = mid_pred(
            fps.frames_prev_hist[0],
            fps.frames_prev_hist[1],
            fps.frames_prev_hist[2],
        );
        *nb_frames = *nb_frames_prev;

        if *nb_frames == 0 && fps.last_dropped > 0 {
            fps.last_dropped += 1;
        }

        // Handle finish cleanup here
        fps.frames_prev_hist.copy_within(0..2, 1);
        fps.frames_prev_hist[0] = *nb_frames_prev;

        if *nb_frames_prev == 0 && fps.last_dropped > 0 {
            info!(
                "Dropping frame {} at ts {}",
                fps.frame_number,
                (*fps.last_frame.as_ptr()).pts
            );
        }

        // Same duplication cap as the live path below: the flush replays the
        // history median, which must not emit absurd counts either
        // (ffmpeg_filter.c:2340-2344 runs for flush and live alike).
        if *nb_frames > 3_240_000 {
            error!("{} frame duplication too large, skipping", *nb_frames - 1);
            *nb_frames = 0;
            return;
        }

        fps.last_dropped = (nb_frames == nb_frames_prev && !frame.is_null()) as i32;

        if fps.last_dropped != 0 && (*frame).flags & AV_FRAME_FLAG_KEY != 0 {
            fps.dropped_keyframe = true;
        }

        return;
    }

    let mut duration = (*frame).duration as f64 * av_q2d((*frame).time_base) / av_q2d(ofp.tb_out);
    let mut sync_ipts =
        adjust_frame_pts_to_encoder_tb(frame, ofp.tb_out, ofp.opts.ts_offset.unwrap_or(0));
    /* delta0 is the "drift" between the input frame and
     * where it would fall in the output. */
    let mut delta0 = sync_ipts - ofp.next_pts as f64;
    let mut delta = delta0 + duration;

    // tracks the number of times the PREVIOUS frame should be duplicated,
    // mostly for variable framerate (VFR)
    *nb_frames_prev = 0;
    /* by default, we output a single frame */
    *nb_frames = 1;

    if delta0 < 0.0 && delta > 0.0 && vsync_method != VSyncMethod::VsyncPassthrough {
        if delta0 < -0.6 {
            debug!("Past duration {:.2} too large", -delta0);
        } else {
            debug!("Clipping frame in rate conversion by {:.6}", -delta0);
        }
        sync_ipts = ofp.next_pts as f64;
        duration += delta0;
        delta0 = 0.0;
    }

    match vsync_method {
        // VSCFR is CFR that keeps the first frame's original timestamp: its
        // prologue suppresses the initial padding, then FALLS THROUGH into
        // the CFR drop/duplicate logic (ffmpeg_filter.c:2288-2295).
        VSyncMethod::VsyncVscfr | VSyncMethod::VsyncCfr => {
            if vsync_method == VSyncMethod::VsyncVscfr && fps.frame_number == 0 && delta0 >= 0.5 {
                debug!("Not duplicating {} initial frames", delta0.round() as i32);
                delta = duration;
                delta0 = 0.0;
                ofp.next_pts = sync_ipts.round() as i64;
            }
            if delta < -1.1 {
                *nb_frames = 0;
            } else if delta > 1.1 {
                *nb_frames = delta.round() as i64;
                if delta0 > 1.1 {
                    *nb_frames_prev = (delta0 - 0.6).round() as i64;
                }
            }
            (*frame).duration = 1;
        }
        VSyncMethod::VsyncVfr => {
            if delta <= -0.6 {
                *nb_frames = 0;
            } else if delta > 0.6 {
                ofp.next_pts = sync_ipts.round() as i64;
            }
            (*frame).duration = duration.round() as i64;
        }
        VSyncMethod::VsyncPassthrough => {
            ofp.next_pts = sync_ipts.round() as i64;
            (*frame).duration = duration.round() as i64;
        }
        _ => {}
    }

    // Handle finish cleanup here
    fps.frames_prev_hist.copy_within(0..2, 1);
    fps.frames_prev_hist[0] = *nb_frames_prev;

    if *nb_frames_prev == 0 && fps.last_dropped > 0 {
        info!(
            "Dropping frame {} at ts {}",
            fps.frame_number,
            (*fps.last_frame.as_ptr()).pts
        );
    }

    // dts_error_threshold (3600*30) * 30: duplication this large means
    // broken timestamps; skip the frame instead of emitting millions of
    // duplicates (ffmpeg_filter.c:2340-2344, after the history update).
    if *nb_frames > 3_240_000 {
        error!("{} frame duplication too large, skipping", *nb_frames - 1);
        *nb_frames = 0;
        return;
    }

    fps.last_dropped = (nb_frames == nb_frames_prev) as i32;

    if fps.last_dropped != 0 && (*frame).flags & AV_FRAME_FLAG_KEY != 0 {
        fps.dropped_keyframe = true;
    }
}

#[cfg(docsrs)]
unsafe fn adjust_frame_pts_to_encoder_tb(
    frame: *mut AVFrame,
    tb_dst: AVRational,
    start_time: i64,
) -> f64 {
    0.0
}

#[cfg(not(docsrs))]
unsafe fn adjust_frame_pts_to_encoder_tb(
    frame: *mut AVFrame,
    tb_dst: AVRational,
    start_time: i64,
) -> f64 {
    let mut float_pts = AV_NOPTS_VALUE as f64;
    let mut tb = tb_dst;
    let filter_tb = (*frame).time_base;
    let extra_bits = av_clip(29 - av_log2(tb.den as u32), 0, 16);

    if (*frame).pts == AV_NOPTS_VALUE {
        return float_pts;
    }

    tb.den <<= extra_bits;
    float_pts = av_rescale_q((*frame).pts, filter_tb, tb) as f64
        - av_rescale_q(start_time, AV_TIME_BASE_Q, tb) as f64;
    float_pts /= (1 << extra_bits) as f64;

    if float_pts != float_pts.round() {
        float_pts += float_pts.signum() * 1.0 / (1 << 17) as f64;
    }

    (*frame).pts = av_rescale_q((*frame).pts, filter_tb, tb_dst)
        - av_rescale_q(start_time, AV_TIME_BASE_Q, tb_dst);
    (*frame).time_base = tb_dst;

    float_pts
}

fn av_clip(x: i32, min_val: i32, max_val: i32) -> i32 {
    std::cmp::max(min_val, std::cmp::min(x, max_val))
}

fn mid_pred(a: i64, b: i64, c: i64) -> i64 {
    if a > b {
        if b > c {
            b // a > b > c
        } else if a > c {
            c // a > c > b
        } else {
            a // c > a > b
        }
    } else if a > c {
        a // b > a > c
    } else if b > c {
        c // b > c > a
    } else {
        b // c > b > a
    }
}

/// Returns a NUL-terminated C string pointer safe to pass to a C `%s` arg: the
/// input pointer when non-null, else a static `"unknown"`. The previous version
/// returned a Rust `&str` whose `.as_ptr()` was NOT NUL-terminated, so feeding
/// it to `%s` overran into adjacent memory.
fn unknown_if_null(ptr: *const c_char) -> *const c_char {
    if ptr.is_null() {
        b"unknown\0".as_ptr() as *const c_char
    } else {
        ptr
    }
}

struct FPSConvContext {
    last_frame: Frame,
    frame_number: usize,

    /* history of nb_frames_prev, i.e. the number of times the
     * previous frame was duplicated by vsync code in recent
     * do_video_out() calls */
    frames_prev_hist: [i64; 3],

    #[allow(dead_code)]
    dup_warning: u64,

    last_dropped: i32,
    dropped_keyframe: bool,

    framerate: AVRational,
    // -fpsmax upper bound (ffmpeg_mux_init.c ms->max_frame_rate)
    framerate_max: AVRational,
}

impl Default for FPSConvContext {
    fn default() -> Self {
        Self {
            last_frame: unsafe { Frame::empty() },
            frame_number: 0,
            frames_prev_hist: [0; 3],
            dup_warning: 0,
            last_dropped: 0,
            dropped_keyframe: false,
            framerate: AVRational { num: 0, den: 0 },
            framerate_max: AVRational { num: 0, den: 0 },
        }
    }
}

#[cfg(docsrs)]
fn ifilter_parameters_from_frame(
    ifp: &mut InputFilterParameter,
    frame: *const AVFrame,
    media_type: AVMediaType,
) -> crate::error::Result<()> {
    Ok(())
}

#[cfg(not(docsrs))]
fn ifilter_parameters_from_frame(
    ifp: &mut InputFilterParameter,
    frame: *const AVFrame,
    media_type: AVMediaType,
) -> crate::error::Result<()> {
    unsafe {
        // Replace hw_frames_ctx
        let ret = av_buffer_replace(&mut ifp.hw_frames_ctx, (*frame).hw_frames_ctx);
        if ret < 0 {
            error!("Replace hw_frames_ctx error: {}", av_err2str(ret));
            return Err(Error::FilterGraph(
                FilterGraphOperationError::BufferReplaceoseError(FilterGraphError::from(ret)),
            ));
        }

        // Determine time_base
        ifp.time_base = if media_type == AVMEDIA_TYPE_AUDIO {
            AVRational {
                num: 1,
                den: (*frame).sample_rate,
            }
        } else {
            (*frame).time_base
        };

        // Set format
        ifp.format = (*frame).format;

        // Set video-specific parameters
        ifp.width = (*frame).width;
        ifp.height = (*frame).height;
        ifp.sample_aspect_ratio = (*frame).sample_aspect_ratio;
        ifp.color_space = (*frame).colorspace;
        ifp.color_range = (*frame).color_range;

        // Set audio-specific parameters
        ifp.sample_rate = (*frame).sample_rate;
        let ret = av_channel_layout_copy(&mut ifp.ch_layout, &(*frame).ch_layout);
        if ret < 0 {
            error!("layout_copy error: {}", av_err2str(ret));
            return Err(Error::FilterGraph(
                FilterGraphOperationError::ChannelLayoutCopyError(FilterGraphError::from(ret)),
            ));
        }

        // FFmpeg 8+: collect global side data for the buffersrc parameters.
        // The display matrix is excluded — autorotate consumes it (and strips
        // it from frames), so forwarding it too would rotate twice (fftools
        // ifilter_parameters_from_frame, n8.1 / 7b18beb477).
        #[cfg(ffmpeg_8_0)]
        {
            ifp.side_data.clear();
            for i in 0..(*frame).nb_side_data {
                let sd = *(*frame).side_data.offset(i as isize);
                let desc = av_frame_side_data_desc((*sd).type_);
                if (*desc).props & AV_SIDE_DATA_PROP_GLOBAL as u32 == 0
                    || (*sd).type_ == AV_FRAME_DATA_DISPLAYMATRIX
                {
                    continue;
                }
                let ret = ifp.side_data.push_clone(sd, 0);
                if ret < 0 {
                    error!("Clone side data error: {}", av_err2str(ret));
                    return Err(Error::FilterGraph(
                        FilterGraphOperationError::FrameSideDataCloneError(FilterGraphError::from(
                            ret,
                        )),
                    ));
                }
            }
        }

        // Handle display matrix side data
        let sd = av_frame_get_side_data(frame, AV_FRAME_DATA_DISPLAYMATRIX);
        if !sd.is_null() {
            std::ptr::copy_nonoverlapping(
                (*sd).data as *const i32,
                ifp.displaymatrix.as_mut_ptr(),
                9,
            );
            ifp.displaymatrix_present = true;
        } else {
            ifp.displaymatrix_present = false;
        }

        // Copy downmix-related side data to the input filter parameters so
        // it reaches the filter chain even though it is not "global":
        // filters like aresample need it during init, not when remixing a
        // frame (fftools ifilter_parameters_from_frame, n8.1).
        #[cfg(ffmpeg_8_0)]
        {
            let sd = av_frame_get_side_data(frame, AV_FRAME_DATA_DOWNMIX_INFO);
            if !sd.is_null() {
                let ret = ifp.side_data.push_clone(sd, 0);
                if ret < 0 {
                    error!("Clone downmix side data error: {}", av_err2str(ret));
                    return Err(Error::FilterGraph(
                        FilterGraphOperationError::FrameSideDataCloneError(FilterGraphError::from(
                            ret,
                        )),
                    ));
                }
                std::ptr::copy_nonoverlapping(
                    (*sd).data as *const ffmpeg_sys_next::AVDownmixInfo,
                    &mut ifp.downmixinfo,
                    1,
                );
                ifp.downmixinfo_present = true;
            } else {
                ifp.downmixinfo_present = false;
            }
        }

        Ok(())
    }
}

fn choose_sample_fmts(
    bprint: &mut AVBPrint,
    format: AVSampleFormat,
    formats: Option<Vec<AVSampleFormat>>,
) {
    if format == AV_SAMPLE_FMT_NONE && (formats.is_none() || formats.as_ref().unwrap().is_empty()) {
        return;
    }

    unsafe {
        {
            let sample_fmts = CString::new("sample_fmts=").unwrap();
            av_bprintf(bprint, sample_fmts.as_ptr());
        }

        if format != AV_SAMPLE_FMT_NONE {
            let format_name = av_get_sample_fmt_name(format);
            if !format_name.is_null() {
                let fmt_str = CString::new("%s").unwrap();
                av_bprintf(bprint, fmt_str.as_ptr(), format_name);
            }
        } else if let Some(formats) = formats {
            for fmt in formats {
                let format_name = av_get_sample_fmt_name(fmt);
                if !format_name.is_null() {
                    let pipe_str = CString::new("%s|").unwrap();
                    av_bprintf(bprint, pipe_str.as_ptr(), format_name);
                }
            }
            if bprint.len > 0 {
                let last_char_ptr = bprint.str_.add(bprint.len as usize - 1);
                *last_char_ptr = 0; // Remove last '|'
                bprint.len -= 1;
            }
        }

        av_bprint_chars(bprint, b':' as c_char, 1);
    }
}

#[cfg(docsrs)]
fn choose_sample_rates(bprint: &mut AVBPrint, rate: i32, rates: Option<Vec<i32>>) {}

#[cfg(not(docsrs))]
fn choose_sample_rates(bprint: &mut AVBPrint, rate: i32, rates: Option<Vec<i32>>) {
    if rate == 0 && (rates.is_none() || rates.as_ref().unwrap().is_empty()) {
        return;
    }

    unsafe {
        {
            let sample_rates_str = CString::new("sample_rates=").unwrap();
            av_bprintf(bprint, sample_rates_str.as_ptr());
        }

        if rate != 0 {
            let fmt_specifier = CString::new("%d").unwrap();
            av_bprintf(bprint, fmt_specifier.as_ptr(), rate);
        } else if let Some(rates) = rates {
            for r in rates {
                let pipe_specifier = CString::new("%d|").unwrap();
                av_bprintf(bprint, pipe_specifier.as_ptr(), r);
            }
            if bprint.len > 0 {
                let last_char_ptr = bprint.str_.add(bprint.len as usize - 1);
                *last_char_ptr = 0; // Remove last '|'
                bprint.len -= 1;
            }
        }

        av_bprint_chars(bprint, b':' as c_char, 1);
    }
}

#[cfg(not(docsrs))]
fn choose_channel_layouts(
    bprint: &mut AVBPrint,
    layout: AVChannelLayout,
    layouts: Option<Vec<AVChannelLayout>>,
) {
    unsafe {
        let ret = av_channel_layout_check(&layout as *const AVChannelLayout);
        if ret != 0 {
            let channel_layouts_str = CString::new("channel_layouts=").unwrap();
            av_bprintf(bprint, channel_layouts_str.as_ptr());
            av_channel_layout_describe_bprint(&layout, bprint);
        } else if let Some(layouts) = layouts {
            if layouts.is_empty() {
                return;
            }

            let channel_layouts_str = CString::new("channel_layouts=").unwrap();
            av_bprintf(bprint, channel_layouts_str.as_ptr());

            for l in layouts {
                if l.nb_channels == 0 {
                    break;
                }

                av_channel_layout_describe_bprint(&l, bprint);
                if !bprint.str_.is_null() {
                    let pipe_str = CString::new("|").unwrap();
                    av_bprintf(bprint, pipe_str.as_ptr());
                }
            }
            if bprint.len > 0 {
                let last_char_ptr = bprint.str_.add(bprint.len as usize - 1);
                *last_char_ptr = 0; // Remove last '|'
                bprint.len -= 1;
            }
        } else {
            return;
        }

        av_bprint_chars(bprint, b':' as c_char, 1);
    }
}

unsafe fn configure_output_video_filter(
    graph: *mut AVFilterGraph,
    ofp: &mut OutputFilterParameter,
    output: *mut AVFilterInOut,
) -> i32 {
    let mut pad_idx = (*output).pad_idx;

    let mut last_filter = (*output).filter_ctx;

    let result = CString::new(format!("out_{}", ofp.opts.name));
    if result.is_err() {
        return AVERROR(ENOMEM);
    }
    let name = result.unwrap();

    let buffer_str = std::ffi::CString::new("buffersink").unwrap();
    let buffer_filter = avfilter_get_by_name(buffer_str.as_ptr());

    let mut ret = avfilter_graph_create_filter(
        &mut ofp.filter,
        buffer_filter,
        name.as_ptr(),
        null(),
        null_mut(),
        graph,
    );
    if ret < 0 {
        return ret;
    }

    let mut bprint = AVBPrint {
        str_: null_mut(),
        len: 0,
        size: 0,
        size_max: 0,
        reserved_internal_buffer: [0; 1],
        reserved_padding: [0; 1000],
    };
    av_bprint_init(&mut bprint, 0, u32::MAX);

    // Use user-specified format (via -pix_fmt) if set, otherwise use encoder-supported formats
    choose_pix_fmts(&mut bprint, ofp.opts.format, ofp.opts.formats.clone());
    choose_color_spaces(
        &mut bprint,
        AVCOL_SPC_UNSPECIFIED,
        ofp.opts.color_spaces.clone().unwrap_or_default(),
    );
    choose_color_ranges(
        &mut bprint,
        AVCOL_RANGE_UNSPECIFIED,
        ofp.opts.color_ranges.clone().unwrap_or_default(),
    );

    if bprint.len >= bprint.size {
        av_bprint_finalize(&mut bprint, null_mut());
        return AVERROR(ENOMEM);
    }

    if bprint.len > 0 {
        let mut filter = null_mut();

        let format_str = CString::new("format").unwrap();
        let format_filter = avfilter_get_by_name(format_str.as_ptr());

        let mut ret = avfilter_graph_create_filter(
            &mut filter,
            format_filter,
            format_str.as_ptr(),
            bprint.str_,
            null_mut(),
            graph,
        );
        if ret < 0 {
            av_bprint_finalize(&mut bprint, null_mut());
            return ret;
        }

        ret = avfilter_link(last_filter, pad_idx as u32, filter, 0);
        if ret < 0 {
            av_bprint_finalize(&mut bprint, null_mut());
            return ret;
        }

        last_filter = filter;
        pad_idx = 0;
    }

    let name = format!("trim_out_{}", ofp.name);
    ret = insert_trim(
        ofp.opts.trim_start_us,
        ofp.opts.trim_duration_us,
        &mut last_filter,
        &mut pad_idx,
        &name,
    );
    if ret < 0 {
        return ret;
    }

    ret = avfilter_link(last_filter, pad_idx as u32, ofp.filter, 0);
    if ret < 0 {
        av_bprint_finalize(&mut bprint, null_mut());
        return ret;
    }
    0
}

fn choose_pix_fmts(
    bprint: &mut AVBPrint,
    format: AVPixelFormat,
    formats: Option<Vec<AVPixelFormat>>,
) {
    if format == AV_PIX_FMT_NONE && (formats.is_none() || formats.as_ref().unwrap().is_empty()) {
        return;
    }

    unsafe {
        {
            let pix_fmts_str = CString::new("pix_fmts=").unwrap();
            av_bprintf(bprint, pix_fmts_str.as_ptr());
        }

        if format != AV_PIX_FMT_NONE {
            let format_name = av_get_pix_fmt_name(format);
            if !format_name.is_null() {
                let fmt_specifier = CString::new("%s").unwrap();
                av_bprintf(bprint, fmt_specifier.as_ptr(), format_name);
            }
        } else if let Some(formats) = formats {
            for fmt in formats {
                let format_name = av_get_pix_fmt_name(fmt);
                if !format_name.is_null() {
                    let fmt_pipe = CString::new("%s|").unwrap();
                    av_bprintf(bprint, fmt_pipe.as_ptr(), format_name);
                }
            }
            if bprint.len > 0 {
                let last_char_ptr = bprint.str_.add(bprint.len as usize - 1);
                *last_char_ptr = 0; // Remove last '|'
                bprint.len -= 1;
            }
        }

        av_bprint_chars(bprint, b':' as c_char, 1);
    }
}

fn choose_color_spaces(
    bprint: &mut AVBPrint,
    color_space: AVColorSpace,
    color_spaces: Vec<AVColorSpace>,
) {
    if color_space == AVCOL_SPC_UNSPECIFIED && color_spaces.is_empty() {
        return;
    }

    unsafe {
        {
            let color_spaces_str = CString::new("color_spaces=").unwrap();
            av_bprintf(bprint, color_spaces_str.as_ptr());
        }

        if color_space != AVCOL_SPC_UNSPECIFIED {
            let color_space_name = av_color_space_name(color_space);
            if !color_space_name.is_null() {
                let fmt_str = CString::new("%s").unwrap();
                av_bprintf(bprint, fmt_str.as_ptr(), color_space_name);
            }
        } else {
            for cs in color_spaces {
                let color_space_name = av_color_space_name(cs);
                if !color_space_name.is_null() {
                    let fmt_pipe = CString::new("%s|").unwrap();
                    av_bprintf(bprint, fmt_pipe.as_ptr(), color_space_name);
                }
            }
            if bprint.len > 0 {
                let last_char_ptr = bprint.str_.add(bprint.len as usize - 1);
                *last_char_ptr = 0; // Remove last '|'
                bprint.len -= 1;
            }
        }

        av_bprint_chars(bprint, b':' as c_char, 1);
    }
}

fn choose_color_ranges(
    bprint: &mut AVBPrint,
    color_range: AVColorRange,
    color_ranges: Vec<AVColorRange>,
) {
    if color_range == AVCOL_RANGE_UNSPECIFIED && color_ranges.is_empty() {
        return;
    }

    unsafe {
        {
            let color_ranges_str = CString::new("color_ranges=").unwrap();
            av_bprintf(bprint, color_ranges_str.as_ptr());
        }

        if color_range != AVCOL_RANGE_UNSPECIFIED {
            let color_range_name = av_color_range_name(color_range);
            if !color_range_name.is_null() {
                let fmt_str = CString::new("%s").unwrap();
                av_bprintf(bprint, fmt_str.as_ptr(), color_range_name);
            }
        } else {
            for cr in color_ranges {
                let color_range_name = av_color_range_name(cr);
                if !color_range_name.is_null() {
                    let fmt_pipe = CString::new("%s|").unwrap();
                    av_bprintf(bprint, fmt_pipe.as_ptr(), color_range_name);
                }
            }
            if bprint.len > 0 {
                let last_char_ptr = bprint.str_.add(bprint.len as usize - 1);
                *last_char_ptr = 0; // Remove last '|'
                bprint.len -= 1;
            }
        }

        av_bprint_chars(bprint, b':' as c_char, 1);
    }
}

unsafe fn configure_input_filter(
    fg_index: usize,
    graph: *mut AVFilterGraph,
    ifp: &mut InputFilterParameter,
    input: *mut AVFilterInOut,
) -> i32 {
    match ifp.media_type {
        AVMEDIA_TYPE_VIDEO => configure_input_video_filter(fg_index, graph, ifp, input),
        AVMEDIA_TYPE_AUDIO => configure_input_audio_filter(fg_index, graph, ifp, input),
        _ => {
            error!("Unexpected media type {:?}", ifp.media_type);
            0
        }
    }
}

#[cfg(docsrs)]
unsafe fn configure_input_audio_filter(
    fg_index: usize,
    graph: *mut AVFilterGraph,
    ifp: &mut InputFilterParameter,
    input: *mut AVFilterInOut,
) -> i32 {
    0
}

#[cfg(not(docsrs))]
unsafe fn configure_input_audio_filter(
    fg_index: usize,
    graph: *mut AVFilterGraph,
    ifp: &mut InputFilterParameter,
    input: *mut AVFilterInOut,
) -> i32 {
    let mut pad_idx = 0;
    let abuffer_str = std::ffi::CString::new("abuffer").unwrap();
    let abuffer_filter = avfilter_get_by_name(abuffer_str.as_ptr());

    let mut args = AVBPrint {
        str_: null_mut(),
        len: 0,
        size: 0,
        size_max: 0,
        reserved_internal_buffer: [0; 1],
        reserved_padding: [0; 1000],
    };

    av_bprint_init(&mut args, 0, AV_BPRINT_SIZE_AUTOMATIC as u32);
    // Reject an invalid/unknown sample format instead of feeding a null into the
    // C "%s" args string below — mirrors configure_input_video_filter's
    // `pix_fmt_desc_from_raw(...) else return AVERROR(EINVAL)`.
    let Some(sample_fmt) = crate::util::format_convert::sample_fmt_from_raw(ifp.format) else {
        return AVERROR(ffmpeg_sys_next::EINVAL);
    };
    let sample_fmt_name = av_get_sample_fmt_name(sample_fmt);

    {
        let fmt_str = CString::new("time_base=%d/%d:sample_rate=%d:sample_fmt=%s").unwrap();
        av_bprintf(
            &mut args,
            fmt_str.as_ptr(),
            ifp.time_base.num,
            ifp.time_base.den,
            ifp.sample_rate,
            sample_fmt_name,
        );
    }

    if av_channel_layout_check(&ifp.ch_layout) != 0
        && ifp.ch_layout.order != AV_CHANNEL_ORDER_UNSPEC
    {
        let channel_layout_str = CString::new(":channel_layout=").unwrap();
        av_bprintf(&mut args, channel_layout_str.as_ptr());
        av_channel_layout_describe_bprint(&ifp.ch_layout, &mut args);
    } else {
        let channels_fmt = CString::new(":channels=%d").unwrap();
        av_bprintf(&mut args, channels_fmt.as_ptr(), ifp.ch_layout.nb_channels);
    }

    let name = format!("graph_{fg_index}_in_{}", ifp.opts.name);
    let name = CString::new(name.as_str()).expect("CString::new failed");

    let mut ret = avfilter_graph_create_filter(
        &mut ifp.filter,
        abuffer_filter,
        name.as_ptr(),
        args.str_,
        null_mut(),
        graph,
    );
    if ret < 0 {
        return ret;
    }

    // FFmpeg 8+: the args string cannot carry side data, so hand the
    // collected global entries to abuffer through the parameters API —
    // buffersrc deep-copies them (n8.1 configure_input_audio_filter,
    // e61b9d4094).
    #[cfg(ffmpeg_8_0)]
    {
        let mut par = av_buffersrc_parameters_alloc();
        if par.is_null() {
            return AVERROR(ENOMEM);
        }
        (*par).side_data = ifp.side_data.as_mut_ptr();
        (*par).nb_side_data = ifp.side_data.len();
        let ret = av_buffersrc_parameters_set(ifp.filter, par);
        av_freep(&mut par as *mut _ as *mut c_void);
        if ret < 0 {
            return ret;
        }
    }

    let mut last_filter = ifp.filter;

    let name = format!("trim_in_{}", ifp.name);
    ret = insert_trim(
        ifp.opts.trim_start_us,
        ifp.opts.trim_end_us,
        &mut last_filter,
        &mut pad_idx,
        &name,
    );
    if ret < 0 {
        return ret;
    }

    ret = avfilter_link(last_filter, 0, (*input).filter_ctx, (*input).pad_idx as u32);
    if ret < 0 {
        return ret;
    }
    0
}

unsafe fn configure_input_video_filter(
    fg_index: usize,
    graph: *mut AVFilterGraph,
    ifp: &mut InputFilterParameter,
    input: *mut AVFilterInOut,
) -> i32 {
    let mut pad_idx = 0;

    let buffer_str = std::ffi::CString::new("buffer").unwrap();
    let buffer_filter = avfilter_get_by_name(buffer_str.as_ptr());

    let mut par = av_buffersrc_parameters_alloc();
    if par.is_null() {
        return AVERROR(ENOMEM);
    }

    let mut sar = ifp.sample_aspect_ratio;
    if sar.den == 0 {
        sar = AVRational { num: 0, den: 1 };
    }

    let result = CString::new(format!(
        "graph {fg_index} input from stream {}",
        ifp.opts.name
    ));
    if result.is_err() {
        av_freep(&mut par as *mut _ as *mut c_void);
        return AVERROR(ENOMEM);
    }
    let name = result.unwrap();

    // Checked descriptor lookup BEFORE the buffer filter is initialized:
    // av_buffersrc_parameters_set copies any non-NONE format through, and
    // FFmpeg 8's buffersrc init dereferences the format descriptor without a
    // NULL check, so an out-of-range value must be rejected here — matching
    // the old args path, whose AVOption parsing rejected it at filter
    // creation.
    let Some(desc) = crate::util::format_convert::pix_fmt_desc_from_raw(ifp.format) else {
        av_freep(&mut par as *mut _ as *mut c_void);
        return AVERROR(ffmpeg_sys_next::EINVAL);
    };

    // All parameters — hw_frames_ctx included — must reach the buffer filter
    // BEFORE it is initialized: FFmpeg 8 validates a HW pix_fmt against
    // hw_frames_ctx already in buffersrc's init (EINVAL when it is still
    // NULL), whereas avfilter_graph_create_filter() initializes right away.
    // So alloc -> av_buffersrc_parameters_set -> avfilter_init_dict, as
    // fftools does since n8.0 (ffmpeg_filter.c, commit 53c71777e193). This
    // ordering behaves identically on FFmpeg 7.x.
    ifp.filter = ffmpeg_sys_next::avfilter_graph_alloc_filter(graph, buffer_filter, name.as_ptr());
    if ifp.filter.is_null() {
        av_freep(&mut par as *mut _ as *mut c_void);
        return AVERROR(ENOMEM);
    }

    (*par).format = ifp.format;
    (*par).time_base = ifp.time_base;
    (*par).frame_rate = ifp.opts.framerate;
    (*par).width = ifp.width;
    (*par).height = ifp.height;
    (*par).sample_aspect_ratio = sar;
    (*par).color_space = ifp.color_space;
    (*par).color_range = ifp.color_range;
    (*par).hw_frames_ctx = ifp.hw_frames_ctx;
    // FFmpeg 8+ propagates global side data through the graph; buffersrc
    // deep-copies the entries, the list keeps ownership (n8.1
    // configure_input_video_filter).
    #[cfg(ffmpeg_8_0)]
    {
        (*par).side_data = ifp.side_data.as_mut_ptr();
        (*par).nb_side_data = ifp.side_data.len();
    }

    let mut ret = av_buffersrc_parameters_set(ifp.filter, par);
    av_freep(&mut par as *mut _ as *mut c_void);
    if ret < 0 {
        return ret;
    }

    ret = ffmpeg_sys_next::avfilter_init_dict(ifp.filter, null_mut());
    if ret < 0 {
        return ret;
    }

    let mut last_filter = ifp.filter;

    /*if (ifp.opts.flags & IFILTER_FLAG_CROP) {
        char crop_buf[64];
        snprintf(crop_buf, sizeof(crop_buf), "w=iw-%u-%u:h=ih-%u-%u:x=%u:y=%u",
        ifp.opts.crop_left, ifp.opts.crop_right,
        ifp.opts.crop_top, ifp.opts.crop_bottom,
        ifp.opts.crop_left, ifp.opts.crop_top);
        ret = insert_filter(&last_filter, &pad_idx, "crop", crop_buf);
        if ret < 0
        return ret;
    }*/

    ifp.displaymatrix_applied = false;
    if ifp.opts.flags & IFILTER_FLAG_AUTOROTATE != 0
        && (*desc).flags & AV_PIX_FMT_FLAG_HWACCEL as u64 == 0
    {
        let displaymatrix = ifp.displaymatrix;
        let theta = get_rotation(&displaymatrix);

        if (theta - 90.0).abs() < 1.0 {
            let args = if displaymatrix[3] > 0 {
                "cclock_flip"
            } else {
                "clock"
            };
            ret = insert_filter(&mut last_filter, &mut pad_idx, "transpose", Some(args));
        } else if (theta - 180.0).abs() < 1.0 {
            if displaymatrix[0] < 0 {
                ret = insert_filter(&mut last_filter, &mut pad_idx, "hflip", None);
                if ret < 0 {
                    return ret;
                }
            }
            if displaymatrix[4] < 0 {
                ret = insert_filter(&mut last_filter, &mut pad_idx, "vflip", None);
            }
        } else if (theta - 270.0).abs() < 1.0 {
            let args = if displaymatrix[3] < 0 {
                "clock_flip"
            } else {
                "cclock"
            };
            ret = insert_filter(&mut last_filter, &mut pad_idx, "transpose", Some(args));
        } else if theta.abs() > 1.0 {
            let rotate_buf = format!("{:.6}*PI/180", theta);
            ret = insert_filter(&mut last_filter, &mut pad_idx, "rotate", Some(&rotate_buf));
        } else if theta.abs() < 1.0 && displaymatrix[4] < 0 {
            ret = insert_filter(&mut last_filter, &mut pad_idx, "vflip", None);
        }

        if ret < 0 {
            return ret;
        }

        ifp.displaymatrix_applied = true;
    }

    let name = format!("trim_in_{}", ifp.name);
    ret = insert_trim(
        ifp.opts.trim_start_us,
        ifp.opts.trim_end_us,
        &mut last_filter,
        &mut pad_idx,
        &name,
    );
    if ret < 0 {
        return ret;
    }

    ret = avfilter_link(last_filter, 0, (*input).filter_ctx, (*input).pad_idx as u32);
    if ret < 0 {
        return ret;
    }
    0
}

fn insert_filter(
    last_filter: &mut *mut AVFilterContext,
    pad_idx: &mut i32,
    filter_name: &str,
    args: Option<&str>,
) -> i32 {
    let graph = unsafe { (*(*last_filter)).graph };
    let Ok(filter_name_cstr) = CString::new(filter_name) else {
        return AVERROR(EINVAL);
    };
    let filter = unsafe { avfilter_get_by_name(filter_name_cstr.as_ptr()) };
    if filter.is_null() {
        return AVERROR_BUG;
    }

    let mut ctx = std::ptr::null_mut();
    let args_cstr = match args.map(CString::new).transpose() {
        Ok(args_cstr) => args_cstr,
        Err(_) => return AVERROR(EINVAL),
    };
    let args_ptr = args_cstr
        .as_ref()
        .map_or(std::ptr::null(), |cstr| cstr.as_ptr());

    let ret = unsafe {
        avfilter_graph_create_filter(
            &mut ctx,
            filter,
            filter_name_cstr.as_ptr(),
            args_ptr,
            std::ptr::null_mut(),
            graph,
        )
    };
    if ret < 0 {
        return ret;
    }

    let ret = unsafe { avfilter_link(*last_filter, *pad_idx as u32, ctx, 0) };
    if ret < 0 {
        return ret;
    }

    *last_filter = ctx;
    *pad_idx = 0;
    0
}

#[cfg(docsrs)]
unsafe fn graph_parse(
    graph: *mut AVFilterGraph,
    graph_desc: &str,
    inputs: *mut *mut AVFilterInOut,
    outputs: *mut *mut AVFilterInOut,
    hw_device: Option<HWDevice>,
) -> i32 {
    0
}

#[cfg(not(docsrs))]
unsafe fn graph_parse(
    graph: *mut AVFilterGraph,
    graph_desc: &str,
    inputs: *mut *mut AVFilterInOut,
    outputs: *mut *mut AVFilterInOut,
    hw_device: Option<HWDevice>,
) -> i32 {
    let Ok(desc) = CString::new(graph_desc) else {
        error!("Filter graph description contains an interior NUL byte");
        return AVERROR(EINVAL);
    };
    let mut seg = null_mut();
    *inputs = null_mut();
    *outputs = null_mut();
    let mut ret = avfilter_graph_segment_parse(graph, desc.as_ptr(), 0, &mut seg);
    if ret < 0 {
        return ret;
    }

    ret = avfilter_graph_segment_create_filters(seg, 0);
    if ret < 0 {
        avfilter_graph_segment_free(&mut seg);
        return ret;
    }

    if let Some(hw_device) = hw_device {
        for i in 0..(*graph).nb_filters {
            let f = *(*graph).filters.add(i as usize);

            if (*(*f).filter).flags & AVFILTER_FLAG_HWDEVICE == 0 {
                continue;
            }
            (*f).hw_device_ctx = av_buffer_ref(hw_device.device_ref);
            if (*f).hw_device_ctx.is_null() {
                avfilter_graph_segment_free(&mut seg);
                return AVERROR(ENOMEM);
            }
        }
    }

    ret = graph_opts_apply(seg);
    if ret < 0 {
        avfilter_graph_segment_free(&mut seg);
        return ret;
    }

    ret = avfilter_graph_segment_apply(seg, 0, inputs, outputs);
    avfilter_graph_segment_free(&mut seg);

    ret
}

#[cfg(not(docsrs))]
pub(crate) unsafe fn graph_opts_apply(seg: *mut AVFilterGraphSegment) -> i32 {
    for i in 0..(*seg).nb_chains {
        let ch = *(*seg).chains.add(i);

        for j in 0..(*ch).nb_filters {
            let p = *(*ch).filters.add(j);
            let mut e = null();

            assert!(!(*p).filter.is_null());

            loop {
                e = av_dict_iterate((*p).opts, e);
                if e.is_null() {
                    break;
                }

                let ret = filter_opt_apply((*p).filter, (*e).key, (*e).value);
                if ret < 0 {
                    return ret;
                }
            }

            av_dict_free(&mut (*p).opts);
        }
    }

    0
}

unsafe fn filter_opt_apply(f: *mut AVFilterContext, mut key: *mut c_char, val: *mut c_char) -> i32 {
    let mut ret = av_opt_set(f as *mut libc::c_void, key, val, AV_OPT_SEARCH_CHILDREN);
    if ret >= 0 {
        return 0;
    }

    let o = if ret == AVERROR_OPTION_NOT_FOUND && *key == b'/' as c_char {
        av_opt_find(
            f as *mut libc::c_void,
            key.add(1),
            null(),
            0,
            AV_OPT_SEARCH_CHILDREN,
        )
    } else {
        null()
    };
    if o.is_null() {
        error!(
            "Error applying option '{}' to filter '{}': {}",
            CStr::from_ptr(key).to_str().unwrap_or("[unknow key]"),
            CStr::from_ptr((*(*f).filter).name)
                .to_str()
                .unwrap_or("[unknow filter name]"),
            av_err2str(ret)
        );
        return ret;
    }

    key = key.add(1);

    if (*o).type_ == AV_OPT_TYPE_BINARY {
        let result = read_binary(val);
        if let Err(e) = result {
            error!(
                "Error loading value for option '{}' from file {}",
                CStr::from_ptr(key).to_str().unwrap_or("[unknow key]"),
                CStr::from_ptr(val).to_str().unwrap_or("[unknow val]")
            );
            return e;
        }
        let (mut data, len) = result.unwrap();

        ret = av_opt_set_bin(
            f as *mut libc::c_void,
            key,
            data as *mut u8,
            len as i32,
            AV_OPT_SEARCH_CHILDREN,
        );
        av_freep(&mut data as *mut _ as *mut c_void);
    } else {
        let mut data = file_read(val);
        if data.is_null() {
            error!(
                "Error loading value for option '{}' from file {}",
                CStr::from_ptr(key).to_str().unwrap_or("[unknow key]"),
                CStr::from_ptr(val).to_str().unwrap_or("[unknow val]")
            );
            return AVERROR(EIO);
        }

        ret = av_opt_set(f as *mut libc::c_void, key, data, AV_OPT_SEARCH_CHILDREN);
        av_freep(&mut data as *mut _ as *mut c_void);
    }
    if ret < 0 {
        error!(
            "Error applying option '{}' to filter '{}': {}",
            CStr::from_ptr(key).to_str().unwrap_or("[unknow key]"),
            CStr::from_ptr((*(*f).filter).name)
                .to_str()
                .unwrap_or("[unknow filter name]"),
            av_err2str(ret)
        );
        return ret;
    }

    0
}

// Port of fftools read_file_to_string (cmdutils.c): load a filter option
// value from a file for the `/opt=path` syntax.
unsafe fn file_read(filename: *mut c_char) -> *mut c_char {
    let mut pb = null_mut();
    let mut ret = avio_open(&mut pb, filename, AVIO_FLAG_READ);
    // The AVBPrint lives on the stack like the C original; passing a null
    // pointer to av_bprint_init would write through NULL.
    let mut bprint: AVBPrint = std::mem::zeroed();
    let mut str = null_mut();

    if ret < 0 {
        error!(
            "Error opening file {}.",
            CStr::from_ptr(filename)
                .to_str()
                .unwrap_or("[unknow filename]")
        );
        return null_mut();
    }

    av_bprint_init(&mut bprint, 0, u32::MAX);
    ret = avio_read_to_bprint(pb, &mut bprint, usize::MAX);
    avio_closep(&mut pb);
    if ret < 0 {
        av_bprint_finalize(&mut bprint, null_mut());
        return null_mut();
    }
    ret = av_bprint_finalize(&mut bprint, &mut str);
    if ret < 0 {
        return null_mut();
    }
    str
}

unsafe fn read_binary(path: *mut c_char) -> crate::error::Result<(*mut c_void, i64), i32> {
    let mut io = null_mut();

    let ret = avio_open2(&mut io, path, AVIO_FLAG_READ, null(), null_mut());
    if ret < 0 {
        error!(
            "Cannot open file '{}': {}",
            CStr::from_ptr(path).to_str().unwrap_or("[unknow path]"),
            av_err2str(ret)
        );
        return Err(ret);
    }

    let fsize = avio_size(io);
    // fftools read_binary (ffmpeg_filter.c, n7.1) rejects negative sizes
    // and sizes over INT_MAX in one combined check with AVERROR(EIO): the
    // avio_read below takes an i32 length, so a larger file would silently
    // truncate both the read and the resulting option value.
    if !(0..=i64::from(i32::MAX)).contains(&fsize) {
        error!(
            "Cannot obtain size of file '{}'",
            CStr::from_ptr(path).to_str().unwrap_or("[unknow path]")
        );
        avio_close(io);
        return Err(AVERROR(EIO));
    }

    let mut data = av_malloc(fsize as usize);
    if data.is_null() {
        avio_close(io);
        return Err(AVERROR(ENOMEM));
    }

    let read_size = avio_read(io, data as *mut libc::c_uchar, fsize as i32);
    if read_size != fsize as i32 {
        error!(
            "Error reading file '{}'. read_size:{read_size}",
            CStr::from_ptr(path).to_str().unwrap_or("[unknow path]")
        );
        // av_freep takes the address of the pointer; passing the buffer
        // itself freed whatever its first bytes happened to contain
        // (ffmpeg_filter.c:470, where data is already a uint8_t**).
        av_freep(&mut data as *mut _ as *mut c_void);
        avio_close(io);
        return Err(if read_size < 0 {
            read_size
        } else {
            AVERROR(EIO)
        });
    }

    avio_close(io);

    Ok((data, fsize))
}

// NEW-SC-03: unit tests for the graph-level sws/swr option resolver. These pin
// the precedence and conflict contract (the P3-hard part) without needing an
// encoder — the integration coverage lives in tests/sws_swr_tuning.rs.
#[cfg(all(test, not(docsrs)))]
mod auto_conv_opts_tests {
    use super::*;

    fn is_invalid_argument(err: &Error) -> bool {
        matches!(
            err,
            Error::FilterGraph(FilterGraphOperationError::ParseError(
                FilterGraphParseError::InvalidArgument
            ))
        )
    }

    #[test]
    fn non_empty_opt_treats_none_and_empty_as_unset() {
        assert_eq!(non_empty_opt(&None), None);
        assert_eq!(non_empty_opt(&Some(String::new())), None);
        assert_eq!(
            non_empty_opt(&Some("flags=lanczos".to_string())),
            Some("flags=lanczos")
        );
    }

    #[test]
    fn graph_level_value_wins_over_conflicting_outputs() {
        // A graph-level value short-circuits: even outputs that disagree with
        // each other never produce a conflict once the graph itself decided.
        let per_output = vec![Some("flags=bilinear"), Some("flags=bicubic")];
        let resolved = resolve_auto_conv_opt(Some("flags=lanczos"), per_output.into_iter())
            .expect("graph-level value must resolve without conflict");
        assert_eq!(resolved, Some("flags=lanczos"));
    }

    #[test]
    fn unique_non_empty_output_value_is_chosen() {
        // Only some outputs set a value; identical repeats are not a conflict.
        let per_output = vec![None, Some("resampler=soxr"), None, Some("resampler=soxr")];
        let resolved = resolve_auto_conv_opt(None, per_output.into_iter())
            .expect("a single distinct output value must resolve");
        assert_eq!(resolved, Some("resampler=soxr"));
    }

    #[test]
    fn conflicting_output_values_error_with_invalid_argument() {
        let per_output = vec![Some("flags=bilinear"), Some("flags=bicubic")];
        let err = resolve_auto_conv_opt(None, per_output.into_iter())
            .expect_err("two different output values must be rejected");
        assert!(
            is_invalid_argument(&err),
            "expected InvalidArgument, got {err:?}"
        );
    }

    #[test]
    fn nothing_set_resolves_to_none() {
        let per_output: Vec<Option<&str>> = vec![None, None];
        let resolved = resolve_auto_conv_opt(None, per_output.into_iter())
            .expect("no values must resolve to None (default behavior)");
        assert_eq!(resolved, None);
    }
}

// Regression tests for the per-output `got_frame` gate. `got_frame` used to be
// graph-level, so in a multi-output complex graph a sibling output that emitted
// frames would flip the shared flag and suppress THIS output's init dummy —
// starving its encoder. The flag now lives on OutputFilterParameter, so each
// output's dummy is gated on whether IT produced a frame.
//
// `close_output` is exercised directly: no filtergraph, media, or GPU needed.
//   got_frame == false -> a non-null init dummy, then the null EOF marker.
//   got_frame == true  -> only the null EOF marker (no dummy).
#[cfg(all(test, not(docsrs)))]
mod per_output_got_frame_tests {
    use super::*;
    use crate::core::scheduler::ffmpeg_scheduler::unref_frame;

    // Local mirror of ffmpeg_scheduler::new_frame (which is module-private):
    // allocate a bare AVFrame shell for the pool.
    fn test_new_frame() -> crate::error::Result<Frame> {
        let f = unsafe { av_frame_alloc() };
        assert!(!f.is_null(), "av_frame_alloc failed in test");
        Ok(unsafe { Frame::wrap(f) })
    }

    fn make_video_ofp(dst: Sender<FrameBox>) -> OutputFilterParameter {
        OutputFilterParameter::new(
            AVMEDIA_TYPE_VIDEO,
            OutputFilterOptions::new(),
            Some(dst),
            0,
            Arc::from(Vec::<AtomicBool>::new()),
        )
    }

    #[test]
    fn empty_output_emits_its_own_init_dummy() {
        let frame_pool =
            ObjPool::new(1, test_new_frame, unref_frame, frame_is_null).expect("frame pool");
        let (tx, rx) = crossbeam_channel::unbounded::<FrameBox>();
        let mut ofp = make_video_ofp(tx);
        // This output produced nothing; a sibling output may have, but that must
        // no longer matter now the flag is per-output.
        ofp.got_frame = false;

        let ret = unsafe { close_output(&mut ofp, &frame_pool) };
        assert_eq!(ret, 0);

        // First: the non-null init dummy that opens this output's encoder.
        let dummy = rx.try_recv().expect("expected an init dummy frame");
        assert!(
            !frame_is_null(&dummy.frame),
            "init dummy must be a real frame"
        );
        // Then: the null EOF marker.
        let marker = rx.try_recv().expect("expected the EOF marker");
        assert!(
            frame_is_null(&marker.frame),
            "EOF marker must be the null frame"
        );
        assert!(ofp.eof, "close_output must mark the output EOF");
    }

    #[test]
    fn output_with_frames_skips_the_init_dummy() {
        let frame_pool =
            ObjPool::new(1, test_new_frame, unref_frame, frame_is_null).expect("frame pool");
        let (tx, rx) = crossbeam_channel::unbounded::<FrameBox>();
        let mut ofp = make_video_ofp(tx);
        // This output already emitted a frame -> no dummy needed.
        ofp.got_frame = true;

        let ret = unsafe { close_output(&mut ofp, &frame_pool) };
        assert_eq!(ret, 0);

        // Only the null EOF marker is sent; no dummy precedes it.
        let marker = rx.try_recv().expect("expected the EOF marker");
        assert!(
            frame_is_null(&marker.frame),
            "only the null EOF marker is expected"
        );
        assert!(rx.try_recv().is_err(), "no init dummy must be emitted");
        assert!(ofp.eof, "close_output must mark the output EOF");
    }
}
