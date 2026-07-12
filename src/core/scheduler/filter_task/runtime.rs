use super::config::configure_filtergraph;
use super::*;

unsafe fn ifilter_has_all_input_formats(ifps: &Vec<InputFilterParameter>) -> bool {
    for ifp in ifps {
        if ifp.format < 0 {
            return false;
        }
    }
    true
}

#[cfg(docsrs)]
pub(super) unsafe fn fg_send_eof(
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
pub(super) unsafe fn fg_send_eof(
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
        // A NULL EOF frame reaches here from a cross-graph producer's EOF marker,
        // and from a healthy pipeline whose cue frame was consumed or could not
        // be allocated (only the final NULL sentinel arrives). Signal EOF with
        // av_buffersrc_add_frame_flags(NULL), which closes the source at its own
        // accumulated frame-end time (mirrors the reconfiguration EOF path). A
        // fixed AV_NOPTS_VALUE would drop the tail timing, and dereferencing the
        // null frame for a pts would crash. A decoder EOF marker instead carries an
        // estimated frame-end timestamp (last_frame_pts + duration_est) when a last
        // pts is known, else AV_NOPTS_VALUE, so close at that pts.
        let ret = if frame.is_null() {
            av_buffersrc_add_frame_flags(ifp.filter, null_mut(), AV_BUFFERSRC_FLAG_PUSH as i32)
        } else {
            let pts = av_rescale_q_rnd(
                (*frame).pts,
                (*frame).time_base,
                ifp.time_base,
                AV_ROUND_NEAR_INF as u32 | AV_ROUND_PASS_MINMAX as u32,
            );
            av_buffersrc_close(ifp.filter, pts, AV_BUFFERSRC_FLAG_PUSH as u32)
        };
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
                    error!(target: LOG_TARGET, "Error initializing filters! {e}");
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
            error!(target: LOG_TARGET,
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
pub(super) unsafe fn fg_send_frame(
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
pub(super) unsafe fn fg_send_frame(
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
            // The graph cannot be configured until EVERY input pad has seen a
            // frame; until then this pad's frames park here. A fast pad paired
            // with an input that never produces (a data-only stream mapped by
            // mistake, a live source that never starts) used to grow this
            // queue without bound — decoded 1080p video is ~3 MiB a frame, so
            // that is an OOM in minutes. Cap it per media type (video frames
            // are large, audio frames tiny) and fail the graph with a
            // diagnosable error instead of silently dropping frames (dropping
            // would corrupt overlay/xfade semantics). Belt-and-braces: the
            // frame-COUNT cap (256 video / 4096 audio) bounds the queue LENGTH
            // (cardinality only, NOT per-frame memory), and the best-effort
            // retained-BYTE cap (PRE_CONFIG_QUEUE_MAX_BYTES) keeps NORMAL frames far
            // tighter than the count cap would (256 video frames is ~800 MiB at
            // 1080p and ~3.2 GiB at 4K). The byte figure is an estimate, not exact
            // and not a strict memory bound against self-crafted frames (see
            // `frame_retained_bytes`); the count cap guarantees only a finite queue
            // length.
            // Whichever trips first
            // fails the graph with a diagnosable error carrying both figures. A
            // user-configurable limit is future builder-API work. fftools has
            // the same queue but the CLI's lockstep demuxing keeps it shallow;
            // this crate's independently-paced inputs make the overflow reachable.
            let frame_bytes = unsafe { frame_retained_bytes(frame) };
            let ifp = &mut ifps[input_filter_index];
            let cap = match ifp.media_type {
                AVMediaType::AVMEDIA_TYPE_VIDEO => 256,
                _ => 4096,
            };
            if pre_config_queue_full(
                ifp.frame_queue.len(),
                cap,
                ifp.frame_queue_bytes,
                frame_bytes,
            ) {
                return Err(Error::FilterGraph(
                    FilterGraphOperationError::PreConfigQueueOverflow(
                        ifp.name.clone(),
                        ifp.frame_queue.len(),
                        // Projected total: the already-queued bytes plus the
                        // incoming frame that tipped the cap.
                        ifp.frame_queue_bytes.saturating_add(frame_bytes),
                    ),
                ));
            }
            ifp.frame_queue.push_back(frame_box);
            ifp.frame_queue_bytes = ifp.frame_queue_bytes.saturating_add(frame_bytes);
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
            info!(target: LOG_TARGET, "Reconfiguring filter because graph {}", reason_str);
        }

        if let Err(e) = configure_filtergraph(fg_index, graph, fgp, graph_desc, ifps, ofps) {
            error!(target: LOG_TARGET, "Error reinitializing filters! {}", e);
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
            error!(target: LOG_TARGET, "Error while filtering: {}", av_err2str(ret));
        }
        return Err(Error::FilterGraph(
            FilterGraphOperationError::BufferSourceAddFrameError(FilterGraphError::from(ret)),
        ));
    }
    Ok(())
}

pub(super) unsafe fn fg_read_frames(
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
                trace!(target: LOG_TARGET, "Filtergraph returned EOF, finishing");
            } else {
                error!(target: LOG_TARGET,
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
        warn!(target: LOG_TARGET,
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
                warn!(target: LOG_TARGET,
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
pub(super) unsafe fn fg_output_frame(
    fgp: &mut FilterGraphParameter,
    ofp: &mut OutputFilterParameter,
    mut frame: Frame,
    frame_pool: &ObjPool<Frame>,
) -> i32 {
    0
}

#[cfg(not(docsrs))]
pub(super) unsafe fn fg_output_frame(
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

        warn!(target: LOG_TARGET, "No filtered frames for output stream, trying to initialize anyway.");

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
        error!(target: LOG_TARGET, "No vsync method on video sync!!!");
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
            info!(target: LOG_TARGET,
                "Dropping frame {} at ts {}",
                fps.frame_number,
                (*fps.last_frame.as_ptr()).pts
            );
        }

        // Same duplication cap as the live path below: the flush replays the
        // history median, which must not emit absurd counts either
        // (ffmpeg_filter.c:2340-2344 runs for flush and live alike).
        if *nb_frames > 3_240_000 {
            error!(target: LOG_TARGET, "{} frame duplication too large, skipping", *nb_frames - 1);
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
            debug!(target: LOG_TARGET, "Past duration {:.2} too large", -delta0);
        } else {
            debug!(target: LOG_TARGET, "Clipping frame in rate conversion by {:.6}", -delta0);
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
                debug!(target: LOG_TARGET, "Not duplicating {} initial frames", delta0.round() as i32);
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
        info!(target: LOG_TARGET,
            "Dropping frame {} at ts {}",
            fps.frame_number,
            (*fps.last_frame.as_ptr()).pts
        );
    }

    // dts_error_threshold (3600*30) * 30: duplication this large means
    // broken timestamps; skip the frame instead of emitting millions of
    // duplicates (ffmpeg_filter.c:2340-2344, after the history update).
    if *nb_frames > 3_240_000 {
        error!(target: LOG_TARGET, "{} frame duplication too large, skipping", *nb_frames - 1);
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
            error!(target: LOG_TARGET, "Replace hw_frames_ctx error: {}", av_err2str(ret));
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
            error!(target: LOG_TARGET, "layout_copy error: {}", av_err2str(ret));
            return Err(Error::FilterGraph(
                FilterGraphOperationError::ChannelLayoutCopyError(FilterGraphError::from(ret)),
            ));
        }

        // FFmpeg 8+: collect global side data for the buffersrc parameters.
        // The display matrix is excluded — autorotate consumes it (and strips
        // it from frames), so forwarding it too would rotate twice (fftools
        // ifilter_parameters_from_frame, n8.1 / 7b18beb477).
        // bound ONLY the clone's OWN deep-copied metadata, not the
        // incoming frame. push_clone shares the payload buffer (refcounted) but
        // av_dict_copy deep-copies each entry's metadata dict, so a FrameFilter
        // attaching a pathologically large metadata dict on a global OR downmix
        // side-data would otherwise force an unbounded copy here. `cloned_meta_bytes`
        // accumulates across BOTH clone sites (global loop + downmix below) and is
        // capped at the estimated-byte threshold (`PRE_CONFIG_QUEUE_MAX_BYTES`); the
        // ifp.side_data snapshot is
        // non-accumulating (cleared each reinit). The frame's OWN queue admission
        // (count + byte cap) is handled separately by the caller — charging the
        // full frame here would wrongly reject a legit large frame that carries a
        // tiny global side-data on the non-queue (single-input / already-configured)
        // path. The stricter queue-admission-BEFORE-clone ordering needs splitting
        // this reinit, which is FFmpeg-8-only code that neither the local build nor
        // CI can compile (both link FFmpeg 7) — deferred to an FFmpeg-8 CI lane.
        #[cfg(ffmpeg_8_0)]
        let mut cloned_meta_bytes = 0usize;
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
                cloned_meta_bytes =
                    cloned_meta_bytes.saturating_add(av_dict_retained_bytes((*sd).metadata));
                if cloned_meta_bytes > PRE_CONFIG_QUEUE_MAX_BYTES {
                    return Err(Error::FilterGraph(
                        FilterGraphOperationError::OversizedSideDataClone(
                            ifp.name.clone(),
                            cloned_meta_bytes,
                        ),
                    ));
                }
                let ret = ifp.side_data.push_clone(sd, 0);
                if ret < 0 {
                    error!(target: LOG_TARGET, "Clone side data error: {}", av_err2str(ret));
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
                // Same clone-only bound as the global loop (the downmix side-data
                // is fixed-size but its metadata dict is not); `cloned_meta_bytes`
                // carries over the global clones already made this reinit.
                cloned_meta_bytes =
                    cloned_meta_bytes.saturating_add(av_dict_retained_bytes((*sd).metadata));
                if cloned_meta_bytes > PRE_CONFIG_QUEUE_MAX_BYTES {
                    return Err(Error::FilterGraph(
                        FilterGraphOperationError::OversizedSideDataClone(
                            ifp.name.clone(),
                            cloned_meta_bytes,
                        ),
                    ));
                }
                let ret = ifp.side_data.push_clone(sd, 0);
                if ret < 0 {
                    error!(target: LOG_TARGET, "Clone downmix side data error: {}", av_err2str(ret));
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
