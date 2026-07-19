use super::*;

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
pub(super) unsafe fn configure_filtergraph(
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
pub(super) unsafe fn configure_filtergraph(
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
            error!(target: LOG_TARGET, "input[{i}] can't find matched InputFilterParameter");
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
            error!(target: LOG_TARGET, "output[{i}] can't find matched OutputFilterParameter");
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

    // A probe-fabricated pad (a deferred-init filter whose real parse
    // produced fewer pads than the build-time probe assumed) leaves its
    // parameter slot unconfigured: the walks above pair parsed pads with
    // slots and simply run out of parsed pads. Everything downstream —
    // queued-frame replay, EOF replay, buffersink reads — dereferences the
    // filter context unconditionally, so reject the mismatch here, at the
    // single choke point, instead of segfaulting later.
    for idx in 0..ifps.len() {
        if ifps[idx].filter.is_null() {
            error!(target: LOG_TARGET,
                "input[{idx}] of filter graph '{graph_desc}' was never configured: \
                 the graph's real input pads differ from the build-time assumption");
            cleanup_filtergraph(graph, ifps, ofps);
            return Err(Error::FilterGraph(FilterGraphOperationError::ParseError(
                FilterGraphParseError::InvalidArgument,
            )));
        }
    }
    for idx in 0..ofps.len() {
        if ofps[idx].filter.is_null() {
            error!(target: LOG_TARGET,
                "output[{idx}] of filter graph '{graph_desc}' was never configured: \
                 the graph's real output pads differ from the build-time assumption");
            cleanup_filtergraph(graph, ifps, ofps);
            return Err(Error::FilterGraph(FilterGraphOperationError::ParseError(
                FilterGraphParseError::InvalidArgument,
            )));
        }
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
            // Keep the retained-byte counter in sync with the queue as it drains.
            ifp.frame_queue_bytes = ifp
                .frame_queue_bytes
                .saturating_sub(unsafe { frame_retained_bytes(tmp_frame_box.frame.as_ptr()) });
            if ifp.media_type == AVMEDIA_TYPE_SUBTITLE {
                // sub2video is not supported: drop the queued frame instead
                // of silently keeping a dead branch.
                error!(target: LOG_TARGET,
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
            error!(target: LOG_TARGET, "Unexpected media type {:?}", ofp.media_type);
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
            error!(target: LOG_TARGET, "{name} filter not present, cannot limit recording time.");
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
            error!(target: LOG_TARGET, "Error configuring the {name} filter");
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
pub(super) fn cleanup_filtergraph(
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
            error!(target: LOG_TARGET, "Unexpected media type {:?}", ifp.media_type);
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
        && desc.flags & AV_PIX_FMT_FLAG_HWACCEL as u64 == 0
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
        error!(target: LOG_TARGET, "Filter graph description contains an interior NUL byte");
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
            (*f).hw_device_ctx = av_buffer_ref(hw_device.device_ref());
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
        error!(target: LOG_TARGET,
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
            error!(target: LOG_TARGET,
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
            error!(target: LOG_TARGET,
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
        error!(target: LOG_TARGET,
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
        error!(target: LOG_TARGET,
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
        error!(target: LOG_TARGET,
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
        error!(target: LOG_TARGET,
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
        error!(target: LOG_TARGET,
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
