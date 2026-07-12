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

mod config;
mod runtime;

/// Log target held stable across the module split (`config`/`runtime` submodules)
/// so target-based log filtering and routing keep observing the pre-split
/// `ez_ffmpeg::core::scheduler::filter_task` rather than per-submodule paths.
const LOG_TARGET: &str = module_path!();

use config::cleanup_filtergraph;
use runtime::{fg_output_frame, fg_read_frames, fg_send_eof, fg_send_frame};

#[cfg(not(docsrs))]
pub(crate) use config::graph_opts_apply;

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
    let thread_done_guard = ThreadDoneGuard::adopt(
        thread_sync.clone(),
        scheduler_status.clone(),
        scheduler_result.clone(),
    );

    let result = std::thread::Builder::new()
        .name(format!("filtergraph{fg_index}"))
        .spawn(move || {
            let _thread_done = thread_done_guard;
            // the frame-owning resources below are `move`-closure CAPTURES,
            // but `_thread_done` (which releases the thread slot, zeroing the sync
            // counter that stop()/wait()/the async Future gate on) is a body local.
            // Rust drops body locals BEFORE captures, so without this rebind the
            // guard would release the slot BEFORE these dropped: a buffered frame
            // carrying a blocking (or panicking) AVBufferRef free callback — an
            // `ifps[i].frame_queue` pre-config backlog frame, an `ofps` held frame,
            // an unconsumed `src` frame, or a pooled frame — would then run its
            // teardown AFTER the caller already observed completion, violating
            // counter==0 => teardown complete (the same teardown-drop-order class,
            // in this worker).
            // Rebinding them as body locals declared AFTER the guard makes them drop
            // BEFORE it on EVERY exit path (break, error, normal completion).
            let mut ifps = ifps;
            let mut ofps = ofps;
            let src = src;
            let frame_pool = frame_pool;
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

/// Per-graph-input best-effort retained-byte cap for the pre-config buffering
/// queue, paired with the frame-COUNT cap. This 256 MiB cap keeps a single stalled
/// input's NORMAL frames to ~82 1080p / ~20 4K frames while leaving a real
/// startup-skew window. It is best-effort, not exact (see `frame_retained_bytes`):
/// an adversarial FrameFilter can retain memory the estimate misses, and the
/// frame-COUNT cap bounds only the queue LENGTH, not per-frame memory — so this is
/// NOT a strict memory bound against self-crafted frames. It does bound the
/// ORIGINAL threat (unbalanced input timing flooding with normal decoder frames)
/// well. For small frames the count cap trips first. Multiple stalled pads stack,
/// and there is no fixed upper bound on the input-pad count, so the aggregate
/// pressure grows with the user-configured graph width (this cap is per graph input).
const PRE_CONFIG_QUEUE_MAX_BYTES: usize = 256 * 1024 * 1024;

/// Conservative fixed cost charged per side-data entry, on top of its payload.
/// FFmpeg allows unlimited duplicate side-data of the same type (flags=0), and a
/// zero-length entry still allocates an `AVFrameSideData` struct, an
/// `AVBufferRef` + backing `AVBuffer`, a pointer slot in the frame's `side_data`
/// table, and allocator overhead (~176 B/entry measured). Without charging it, a
/// `FrameFilter` could attach millions of empty side-data entries per frame and pin
/// substantial memory while the payload-only count reads ~0. Set above the measured
/// per-entry footprint so a flood trips the byte threshold.
const SIDE_DATA_ENTRY_OVERHEAD: usize = 256;

/// A best-effort ESTIMATE of the heap a frame pins while it sits in the pre-config
/// queue — NOT an exact ceiling. Sums the allocations that dominate a normal
/// decoder frame, as two components that are DISTINCT allocations and therefore
/// ADD:
/// - plane bytes: the image/audio `AVBufferRef`s (`buf` + `extended_buf`). For a
///   hardware frame the tiny opaque `buf[0]` under-represents the real footprint,
///   so a conservative software-surface estimate (from the hw-frames context's
///   sw_format/width/height) replaces it as a floor.
/// - extra bytes: attached side data (`AVBufferRef` payloads, e.g. a large
///   ICC/SEI blob) with a fixed per-entry structural cost, the user-settable
///   `opaque_ref`, and the frame-owned `AVDictionary` metadata (the frame's
///   `metadata` plus every side-data `metadata`). The libav-internal
///   `private_ref` is deliberately excluded (not user-reachable; a RefStruct
///   handle rather than an `AVBufferRef` on FFmpeg 8).
/// The HW surface floor and the extras combine additively, never max'd: a 3 MiB HW
/// surface carrying a 4 MiB side-data blob pins 7 MiB.
///
/// Deliberately NOT a precise byte ceiling, and NOT a memory bound against an
/// adversarial `FrameFilter`. A filter can craft frames whose real footprint this
/// estimate misses by an UNBOUNDED amount — most sharply the un-shrunk
/// `side_data`/`extended_data` table CAPACITY (a filter can grow the table to N
/// entries then remove all but one: the queued frame reports `nb_side_data == 1`
/// while the table still holds N slots), plus per-plane `AVBuffer` structs on
/// many-channel audio and allocator rounding on huge dictionaries. The
/// per-media-type frame-COUNT cap (256 video / 4096 audio) bounds only the queue
/// LENGTH (cardinality), NOT the per-frame memory — so self-inflicted pathological
/// frames are NOT strictly memory-bounded. That is a known limitation outside the
/// original threat model (unbalanced input TIMING flooding the queue with NORMAL
/// decoder frames, which this estimate bounds well). `FrameBox.frame_data` is
/// intentionally NOT counted (small bounded pre-config metadata). `null` counts 0.
///
/// # Safety
/// `frame` must be null or a valid `AVFrame` pointer whose buffer table is live.
unsafe fn frame_retained_bytes(frame: *const AVFrame) -> usize {
    if frame.is_null() {
        return 0;
    }
    // Plane component: image/audio buffers. A HW frame replaces this with a
    // software-surface floor below.
    let mut plane_bytes = 0usize;
    for &buf in (*frame).buf.iter() {
        if !buf.is_null() {
            plane_bytes = plane_bytes.saturating_add((*buf).size);
        }
    }
    let nb_extended = (*frame).nb_extended_buf;
    if nb_extended > 0 && !(*frame).extended_buf.is_null() {
        for i in 0..nb_extended as isize {
            let buf = *(*frame).extended_buf.offset(i);
            if !buf.is_null() {
                plane_bytes = plane_bytes.saturating_add((*buf).size);
            }
        }
    }

    // Extra component: distinct allocations that add on top of the planes/surface.
    let mut extra_bytes = 0usize;
    // Side-data buffers (a large ICC profile / SEI / user-data blob), each
    // side-data metadata dictionary, and a fixed per-entry structural cost so a
    // flood of zero-length entries cannot slip under the byte cap.
    let nb_side = (*frame).nb_side_data;
    if nb_side > 0 && !(*frame).side_data.is_null() {
        for i in 0..nb_side as isize {
            let sd = *(*frame).side_data.offset(i);
            if !sd.is_null() {
                extra_bytes = extra_bytes.saturating_add(SIDE_DATA_ENTRY_OVERHEAD);
                if !(*sd).buf.is_null() {
                    extra_bytes = extra_bytes.saturating_add((*(*sd).buf).size);
                }
                extra_bytes = extra_bytes.saturating_add(av_dict_retained_bytes((*sd).metadata));
            }
        }
    }
    // Other owned refs. `opaque_ref` is user-settable, so a FrameFilter can pin a
    // large buffer there and it belongs in the estimate. `private_ref` is
    // libav-INTERNAL — the headers forbid external code from inspecting it, and on
    // FFmpeg 8 its type is an internal RefStruct handle, not an `AVBufferRef` (so
    // treating it as one is both a type error and undefined behavior). It is not
    // user-reachable, hence outside the threat model and deliberately not counted.
    let opaque_ref = (*frame).opaque_ref;
    if !opaque_ref.is_null() {
        extra_bytes = extra_bytes.saturating_add((*opaque_ref).size);
    }
    // Frame-level metadata dictionary.
    extra_bytes = extra_bytes.saturating_add(av_dict_retained_bytes((*frame).metadata));

    // Hardware surface floor: buf[0] is a small handle whose release callback
    // owns the GPU surface, so charge at least a conservative software-surface
    // estimate for the PLANE component — a queue of tiny HW handles must not slip
    // under the byte cap and exhaust the device pool first.
    let mut hw_estimate = 0usize;
    if !(*frame).hw_frames_ctx.is_null() {
        let hwfc = (*(*frame).hw_frames_ctx).data as *const ffmpeg_sys_next::AVHWFramesContext;
        if !hwfc.is_null() {
            let sw_estimate = ffmpeg_sys_next::av_image_get_buffer_size(
                (*hwfc).sw_format,
                (*hwfc).width,
                (*hwfc).height,
                1,
            );
            if sw_estimate > 0 {
                hw_estimate = sw_estimate as usize;
            }
        }
    }
    combine_retained_bytes(plane_bytes, hw_estimate, extra_bytes)
}

/// Combine the plane/HW-surface component with the additive extras. The surface
/// floor MAXes the plane bytes (buf[0] under-represents a HW surface, so take the
/// larger of the two for the plane component); the extras (side data, refs,
/// metadata dicts) are DISTINCT allocations and are ADDED. Pure arithmetic, split
/// out so the max-vs-add boundary is unit-testable without a live HW frame.
fn combine_retained_bytes(
    plane_bytes: usize,
    hw_surface_estimate: usize,
    extra_bytes: usize,
) -> usize {
    plane_bytes
        .max(hw_surface_estimate)
        .saturating_add(extra_bytes)
}

/// Retained bytes of an `AVDictionary` — the summed key/value string lengths plus
/// a per-entry struct overhead. A frame's `metadata` dict and each side-data
/// `metadata` dict are frame-owned heap allocations a custom `FrameFilter` can
/// grow arbitrarily, so they count toward the pre-config queue's byte cap.
///
/// # Safety
/// `dict` must be null or a valid `AVDictionary` pointer.
unsafe fn av_dict_retained_bytes(dict: *const ffmpeg_sys_next::AVDictionary) -> usize {
    if dict.is_null() {
        return 0;
    }
    let mut total = 0usize;
    let mut prev: *const ffmpeg_sys_next::AVDictionaryEntry = std::ptr::null();
    loop {
        // Empty key + AV_DICT_IGNORE_SUFFIX iterates every entry.
        let entry = ffmpeg_sys_next::av_dict_get(
            dict,
            c"".as_ptr(),
            prev,
            ffmpeg_sys_next::AV_DICT_IGNORE_SUFFIX,
        );
        if entry.is_null() {
            break;
        }
        let key_len = if (*entry).key.is_null() {
            0
        } else {
            std::ffi::CStr::from_ptr((*entry).key).to_bytes().len()
        };
        let val_len = if (*entry).value.is_null() {
            0
        } else {
            std::ffi::CStr::from_ptr((*entry).value).to_bytes().len()
        };
        total = total
            .saturating_add(key_len)
            .saturating_add(val_len)
            .saturating_add(2) // the two NUL terminators
            .saturating_add(std::mem::size_of::<ffmpeg_sys_next::AVDictionaryEntry>());
        prev = entry as *const _;
    }
    total
}

/// Whether admitting a frame of `incoming_bytes` would push the pre-config
/// buffering queue past EITHER cap: the per-media-type frame COUNT (`frame_cap`)
/// or the estimated retained-BYTE threshold (`PRE_CONFIG_QUEUE_MAX_BYTES`). Split out so the
/// dual-cap logic is unit-testable without a live filtergraph.
fn pre_config_queue_full(
    queued_frames: usize,
    frame_cap: usize,
    queued_bytes: usize,
    incoming_bytes: usize,
) -> bool {
    queued_frames >= frame_cap
        || queued_bytes.saturating_add(incoming_bytes) > PRE_CONFIG_QUEUE_MAX_BYTES
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
    /// Best-effort retained-memory estimate across `frame_queue` — the sum of
    /// `frame_retained_bytes` over the queued frames (plane/side-data payloads plus
    /// the per-entry structural, dictionary, `opaque_ref` and HW-surface
    /// components), NOT the frame count. Kept in sync on every push/pop so the
    /// pre-config overflow guard can pressure on MEMORY, not just frame count — a
    /// 256-frame cap is ~800 MiB at 1080p and ~3.2 GiB at 4K, so the count cap alone
    /// would let normal frames grow large. It is not an exact memory bound (see
    /// `frame_retained_bytes`).
    frame_queue_bytes: usize,

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
            frame_queue_bytes: 0,
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

// the pre-config buffering queue must weigh MEMORY (best-effort), not
// just frame count. A 256-frame cap is ~800 MiB at 1080p and ~3.2 GiB at 4K, so a
// companion estimated-retained-byte threshold is required. These exercise the byte
// measurement and the dual-cap predicate directly (no live filtergraph).
#[cfg(all(test, not(docsrs)))]
mod pre_config_queue_cap_tests {
    use super::*;

    fn video_frame_with_buffer(w: i32, h: i32) -> Frame {
        unsafe {
            let f = av_frame_alloc();
            assert!(!f.is_null(), "av_frame_alloc failed in test");
            (*f).format = ffmpeg_sys_next::AVPixelFormat::AV_PIX_FMT_YUV420P as i32;
            (*f).width = w;
            (*f).height = h;
            let ret = ffmpeg_sys_next::av_frame_get_buffer(f, 0);
            assert_eq!(ret, 0, "av_frame_get_buffer failed in test");
            Frame::wrap(f)
        }
    }

    #[test]
    fn frame_retained_bytes_sums_buffer_allocations() {
        let frame = video_frame_with_buffer(1920, 1080);
        let bytes = unsafe { frame_retained_bytes(frame.as_ptr()) };
        // YUV420P 1080p is ~3.1 MiB; at least the luma plane, well under 8 MiB.
        assert!(
            bytes >= (1920 * 1080) as usize,
            "expected >= luma-plane bytes, got {bytes}"
        );
        assert!(
            bytes < 8 * 1024 * 1024,
            "1080p retained bytes unexpectedly large: {bytes}"
        );
        // A bare frame with no ref-counted buffers retains nothing; null is 0.
        let bare = unsafe { Frame::wrap(av_frame_alloc()) };
        assert_eq!(unsafe { frame_retained_bytes(bare.as_ptr()) }, 0);
        assert_eq!(unsafe { frame_retained_bytes(std::ptr::null()) }, 0);
    }

    // a large side-data blob owned by the frame must be charged too — the
    // image planes alone under-count the memory the queue pins.
    #[test]
    fn frame_retained_bytes_includes_side_data() {
        let frame = video_frame_with_buffer(320, 240);
        let base = unsafe { frame_retained_bytes(frame.as_ptr()) };
        let side_bytes = 4 * 1024 * 1024usize;
        unsafe {
            let sd = ffmpeg_sys_next::av_frame_new_side_data(
                frame.as_ptr() as *mut _,
                ffmpeg_sys_next::AVFrameSideDataType::AV_FRAME_DATA_SEI_UNREGISTERED,
                side_bytes as _,
            );
            assert!(!sd.is_null(), "av_frame_new_side_data failed in test");
        }
        let with_side = unsafe { frame_retained_bytes(frame.as_ptr()) };
        assert!(
            with_side >= base + side_bytes,
            "attached side data ({side_bytes} B) must be charged: base={base}, with_side={with_side}"
        );
    }

    // a HW surface and the extras it carries are DISTINCT allocations, so
    // the byte charge must ADD them, never max them together. Exercised on the
    // pure combiner because a real hw_frames_ctx needs a device absent in CI.
    #[test]
    fn hw_surface_floor_adds_to_extras_and_never_maxes_them() {
        let mib = 1024 * 1024;
        // 3 MiB HW surface (tiny plane handle) + 4 MiB side data = 7 MiB, not 4.
        assert_eq!(
            combine_retained_bytes(4 * 1024, 3 * mib, 4 * mib),
            3 * mib + 4 * mib,
            "HW surface floor and extras must add, not max"
        );
        // SW frame: no HW estimate, plane + extra.
        assert_eq!(combine_retained_bytes(2 * mib, 0, 1024), 2 * mib + 1024);
        // Large plane, no HW estimate, no extra: the plane stands alone.
        assert_eq!(combine_retained_bytes(2 * mib, 0, 0), 2 * mib);
        // Tiny plane, large surface floor, no extra: the floor wins the max.
        assert_eq!(combine_retained_bytes(4 * 1024, 3 * mib, 0), 3 * mib);
    }

    // a frame-owned AVDictionary (metadata a custom FrameFilter can stuff)
    // is heap the queue pins and must be charged — the image planes miss it.
    #[test]
    fn frame_retained_bytes_includes_metadata_dictionary() {
        let frame = video_frame_with_buffer(320, 240);
        let base = unsafe { frame_retained_bytes(frame.as_ptr()) };
        let blob = 512 * 1024usize;
        let value = std::ffi::CString::new(vec![b'x'; blob]).unwrap();
        unsafe {
            let ret = ffmpeg_sys_next::av_dict_set(
                &mut (*(frame.as_ptr() as *mut AVFrame)).metadata,
                c"blob".as_ptr(),
                value.as_ptr(),
                0,
            );
            assert!(ret >= 0, "av_dict_set failed in test");
        }
        let with_meta = unsafe { frame_retained_bytes(frame.as_ptr()) };
        assert!(
            with_meta >= base + blob,
            "frame metadata dict ({blob} B) must be charged: base={base}, with_meta={with_meta}"
        );
    }

    // side-data metadata dicts are frame-owned too. A tiny side-data
    // buffer with a big metadata dict must still be charged for the dict.
    #[test]
    fn frame_retained_bytes_includes_side_data_metadata() {
        let frame = video_frame_with_buffer(320, 240);
        let blob = 256 * 1024usize;
        let value = std::ffi::CString::new(vec![b'y'; blob]).unwrap();
        let (base, with_meta) = unsafe {
            let sd = ffmpeg_sys_next::av_frame_new_side_data(
                frame.as_ptr() as *mut _,
                ffmpeg_sys_next::AVFrameSideDataType::AV_FRAME_DATA_SEI_UNREGISTERED,
                8,
            );
            assert!(!sd.is_null(), "av_frame_new_side_data failed in test");
            let base = frame_retained_bytes(frame.as_ptr());
            let ret = ffmpeg_sys_next::av_dict_set(
                &mut (*sd).metadata,
                c"blob".as_ptr(),
                value.as_ptr(),
                0,
            );
            assert!(ret >= 0, "av_dict_set failed in test");
            (base, frame_retained_bytes(frame.as_ptr()))
        };
        assert!(
            with_meta >= base + blob,
            "side-data metadata ({blob} B) must be charged: base={base}, with_meta={with_meta}"
        );
    }

    // FFmpeg's av_frame_new_side_data appends without limit; a flood of
    // ZERO-length entries pins real per-entry struct/ref memory the payload-only
    // count misses. Each entry must be charged the structural overhead so the
    // estimate crosses the byte threshold rather than slipping under it at ~0 bytes.
    #[test]
    fn frame_retained_bytes_charges_empty_side_data_entries() {
        let frame = video_frame_with_buffer(64, 64);
        let base = unsafe { frame_retained_bytes(frame.as_ptr()) };
        let n = 1000usize;
        unsafe {
            for _ in 0..n {
                let sd = ffmpeg_sys_next::av_frame_new_side_data(
                    frame.as_ptr() as *mut _,
                    ffmpeg_sys_next::AVFrameSideDataType::AV_FRAME_DATA_SEI_UNREGISTERED,
                    0, // zero-length payload — only the structural overhead remains
                );
                assert!(!sd.is_null(), "av_frame_new_side_data(0) failed in test");
            }
        }
        let with_entries = unsafe { frame_retained_bytes(frame.as_ptr()) };
        assert!(
            with_entries >= base + n * SIDE_DATA_ENTRY_OVERHEAD,
            "each of {n} empty side-data entries must be charged the structural \
             overhead: base={base}, with={with_entries}"
        );
    }

    #[test]
    fn byte_cap_trips_before_the_frame_cap_for_large_frames() {
        // A handful of 4K-sized frames (~12 MiB each) blow the 256 MiB byte
        // threshold long before the 256-frame count cap (which alone would allow
        // ~3.2 GiB).
        let four_k = 12 * 1024 * 1024usize;
        let queued_bytes = 250 * 1024 * 1024; // just under the threshold
        assert!(
            pre_config_queue_full(20, 256, queued_bytes, four_k),
            "the byte threshold must trip while the frame count (20) is far under the cap"
        );
        // The same low frame count with negligible bytes trips neither cap.
        assert!(
            !pre_config_queue_full(20, 256, 1024, 1024),
            "20 small frames must not trip either cap"
        );
    }

    #[test]
    fn frame_cap_trips_for_many_small_frames() {
        // Tiny frames stay far under the byte threshold but must still be bounded
        // by the frame-count cap.
        assert!(
            pre_config_queue_full(4096, 4096, 1024, 128),
            "the frame-count cap must trip for many tiny frames"
        );
        assert!(
            !pre_config_queue_full(4095, 4096, 1024, 128),
            "one below the frame cap with negligible bytes must not trip"
        );
    }
}
