use super::*;

pub(super) fn fg_bind_inputs(
    filter_graphs: &mut Vec<FilterGraph>,
    demuxs: &mut Vec<Demuxer>,
) -> Result<()> {
    if filter_graphs.is_empty() {
        return Ok(());
    }
    bind_fg_inputs_by_fg(filter_graphs)?;

    for filter_graph in filter_graphs.iter_mut() {
        for i in 0..filter_graph.inputs.len() {
            fg_complex_bind_input(filter_graph, i, demuxs)?;
        }
    }

    Ok(())
}

struct FilterLabel {
    linklabel: String,
    media_type: AVMediaType,
}

pub(super) fn bind_fg_inputs_by_fg(filter_graphs: &mut Vec<FilterGraph>) -> Result<()> {
    let fg_labels = filter_graphs
        .iter()
        .map(|filter_graph| {
            let inputs = filter_graph
                .inputs
                .iter()
                .map(|input| FilterLabel {
                    linklabel: input.linklabel.clone(),
                    media_type: input.media_type,
                })
                .collect::<Vec<_>>();
            let outputs = filter_graph
                .outputs
                .iter()
                .map(|output| FilterLabel {
                    linklabel: output.linklabel.clone(),
                    media_type: output.media_type,
                })
                .collect::<Vec<_>>();
            (inputs, outputs)
        })
        .collect::<Vec<_>>();

    for (i, (inputs, _outputs)) in fg_labels.iter().enumerate() {
        for (input_pad_idx, input_filter_label) in inputs.iter().enumerate() {
            if input_filter_label.linklabel.is_empty() {
                continue;
            }

            'outer: for (j, (_inputs, outputs)) in fg_labels.iter().enumerate() {
                if i == j {
                    continue;
                }

                for (output_idx, output_filter_label) in outputs.iter().enumerate() {
                    if output_filter_label.linklabel != input_filter_label.linklabel {
                        continue;
                    }
                    if output_filter_label.media_type != input_filter_label.media_type {
                        warn!(target: LOG_TARGET,
                            "Tried to connect {:?} output to {:?} input",
                            output_filter_label.media_type, input_filter_label.media_type
                        );
                        return Err(FilterGraphParseError::InvalidArgument.into());
                    }

                    {
                        let filter_graph = &filter_graphs[j];
                        let output_filter = &filter_graph.outputs[output_idx];
                        if output_filter.has_dst() {
                            continue;
                        }
                    }

                    let (sender, finished_flag_list) = {
                        let filter_graph = &mut filter_graphs[i];
                        filter_graph.get_src_sender()
                    };

                    {
                        let filter_graph = &mut filter_graphs[j];
                        filter_graph.outputs[output_idx].set_dst(sender);
                        // The consumer routes frames and indexes its per-pad
                        // finished_flag_list by INPUT PAD index, not by the
                        // consumer's graph index.
                        filter_graph.outputs[output_idx].fg_input_index = input_pad_idx;
                        filter_graph.outputs[output_idx].finished_flag_list = finished_flag_list;
                    }
                    // Mark the pad so fg_complex_bind_input does not bind it
                    // to a demuxer stream on top of this connection.
                    filter_graphs[i].inputs[input_pad_idx].bound = true;

                    break 'outer;
                }
            }
        }
    }
    Ok(())
}

pub(super) fn fg_complex_bind_input(
    filter_graph: &mut FilterGraph,
    input_filter_index: usize,
    demuxs: &mut Vec<Demuxer>,
) -> Result<()> {
    // A pad already connected to another filtergraph's output must not also
    // be bound to a demuxer stream (covers labeled pads including the
    // reserved "in" label, which otherwise takes the auto-bind branch).
    if filter_graph.inputs[input_filter_index].bound {
        return Ok(());
    }

    let graph_desc = &filter_graph.graph_desc;
    let input_filter = &mut filter_graph.inputs[input_filter_index];
    let (demux_idx, stream_idx) = if !input_filter.linklabel.is_empty()
        && input_filter.linklabel != "in"
    {
        let (demux_idx, stream_idx) = fg_find_input_idx_by_linklabel(
            &input_filter.linklabel,
            input_filter.media_type,
            demuxs,
            graph_desc,
        )?;

        info!(target: LOG_TARGET,
            "Binding filter input with label '{}' to input stream {stream_idx}:{demux_idx}",
            input_filter.linklabel
        );
        (demux_idx, stream_idx)
    } else {
        let mut demux_idx = -1i32;
        let mut stream_idx = 0;
        for (d_idx, demux) in demuxs.iter().enumerate() {
            for (st_idx, intput_stream) in demux.get_streams().iter().enumerate() {
                if intput_stream.is_used() {
                    continue;
                }
                if intput_stream.codec_type == input_filter.media_type {
                    demux_idx = d_idx as i32;
                    stream_idx = st_idx;
                    break;
                }
            }
            if demux_idx >= 0 {
                break;
            }
        }

        if demux_idx < 0 {
            warn!(target: LOG_TARGET,
                "Cannot find a matching stream for unlabeled input pad {}",
                input_filter.name
            );
            return Err(FilterGraphParseError::InvalidArgument.into());
        }

        debug!(target: LOG_TARGET, "FilterGraph binding unlabeled input {input_filter_index} to input stream {stream_idx}:{demux_idx}");

        (demux_idx as usize, stream_idx)
    };

    let demux = &mut demuxs[demux_idx];

    ifilter_bind_ist(filter_graph, input_filter_index, stream_idx, demux)
}

#[cfg(docsrs)]
pub(super) fn ifilter_bind_ist(
    filter_graph: &mut FilterGraph,
    input_index: usize,
    stream_idx: usize,
    demux: &mut Demuxer,
) -> Result<()> {
    Ok(())
}

#[cfg(not(docsrs))]
pub(super) fn ifilter_bind_ist(
    filter_graph: &mut FilterGraph,
    input_index: usize,
    stream_idx: usize,
    demux: &mut Demuxer,
) -> Result<()> {
    unsafe {
        let input_filter = &mut filter_graph.inputs[input_index];
        let ist = *(*demux.in_fmt_ctx_ptr()).streams.add(stream_idx);
        let par = (*ist).codecpar;
        if (*par).codec_type == AVMEDIA_TYPE_VIDEO {
            // A user-forced input framerate feeds the filtergraph directly;
            // only guess from the container when none was forced
            // (ffmpeg_demux.c ist_filter_add: ist->framerate ?: guess).
            let framerate = if demux.framerate.num > 0 && demux.framerate.den > 0 {
                demux.framerate
            } else {
                av_guess_frame_rate(demux.in_fmt_ctx_ptr(), ist, null_mut())
            };
            input_filter.opts.framerate = framerate;
        } else if (*par).codec_type == AVMEDIA_TYPE_SUBTITLE {
            input_filter.opts.sub2video_width = (*par).width;
            input_filter.opts.sub2video_height = (*par).height;

            if input_filter.opts.sub2video_width <= 0 || input_filter.opts.sub2video_height <= 0 {
                let nb_streams = (*demux.in_fmt_ctx_ptr()).nb_streams;
                for j in 0..nb_streams {
                    let par1 = (**(*demux.in_fmt_ctx_ptr()).streams.add(j as usize)).codecpar;
                    if (*par1).codec_type == AVMEDIA_TYPE_VIDEO {
                        input_filter.opts.sub2video_width =
                            std::cmp::max(input_filter.opts.sub2video_width, (*par1).width);
                        input_filter.opts.sub2video_height =
                            std::cmp::max(input_filter.opts.sub2video_height, (*par1).height);
                    }
                }
            }

            if input_filter.opts.sub2video_width <= 0 || input_filter.opts.sub2video_height <= 0 {
                input_filter.opts.sub2video_width =
                    std::cmp::max(input_filter.opts.sub2video_width, 720);
                input_filter.opts.sub2video_height =
                    std::cmp::max(input_filter.opts.sub2video_height, 576);
            }

            demux.get_stream_mut(stream_idx).have_sub2video = true;
        }

        let dec_ctx = {
            let input_stream = demux.get_stream_mut(stream_idx);
            avcodec_alloc_context3(input_stream.codec.as_ptr())
        };
        if dec_ctx.is_null() {
            return Err(FilterGraphParseError::OutOfMemory.into());
        }
        let _codec_ctx = CodecContext::new(dec_ctx);

        // A freshly allocated context only carries codec defaults (format
        // -1, 0x0, timebase 0/1), which made the fallback below useless: the
        // EOF-before-first-frame path could never configure the graph
        // ("Cannot determine format of input ... after EOF"). fftools fills
        // the fallback from the opened decoder (dec_open's param_out); at
        // bind time no decoder is open, so the stream's codecpar — the same
        // values a decoder would start from — is the faithful source.
        let ret = avcodec_parameters_to_context(dec_ctx, par);
        if ret < 0 {
            return Err(FilterGraphParseError::from(ret).into());
        }
        (*dec_ctx).pkt_timebase = (*ist).time_base;

        let fallback = input_filter.opts.fallback.as_mut_ptr();
        if (*dec_ctx).codec_type == AVMEDIA_TYPE_AUDIO {
            (*fallback).format = (*dec_ctx).sample_fmt as i32;
            (*fallback).sample_rate = (*dec_ctx).sample_rate;

            let ret = av_channel_layout_copy(&mut (*fallback).ch_layout, &(*dec_ctx).ch_layout);
            if ret < 0 {
                return Err(FilterGraphParseError::from(ret).into());
            }
        } else if (*dec_ctx).codec_type == AVMEDIA_TYPE_VIDEO {
            (*fallback).format = (*dec_ctx).pix_fmt as i32;
            (*fallback).width = (*dec_ctx).width;
            (*fallback).height = (*dec_ctx).height;
            (*fallback).sample_aspect_ratio = (*dec_ctx).sample_aspect_ratio;
            (*fallback).colorspace = (*dec_ctx).colorspace;
            (*fallback).color_range = (*dec_ctx).color_range;
        }
        (*fallback).time_base = (*dec_ctx).pkt_timebase;

        // Set autorotate flag based on demuxer configuration
        // FFmpeg source: ffmpeg_demux.c:1137, ffmpeg_filter.c:1744-1778 (FFmpeg 7.x)
        if demux.autorotate {
            input_filter.opts.flags |= IFILTER_FLAG_AUTOROTATE;
        }

        let tsoffset = if demux.copy_ts {
            let mut tsoffset = if demux.start_time_us.is_some() {
                demux.start_time_us.unwrap()
            } else {
                0
            };
            if (*demux.in_fmt_ctx_ptr()).start_time != ffmpeg_sys_next::AV_NOPTS_VALUE {
                tsoffset += (*demux.in_fmt_ctx_ptr()).start_time
            }
            tsoffset
        } else {
            0
        };
        if demux.start_time_us.is_some() {
            input_filter.opts.trim_start_us = Some(tsoffset);
        }
        input_filter.opts.trim_end_us = demux.recording_time_us;

        let (sender, finished_flag_list) = filter_graph.get_src_sender();
        {
            let input_stream = demux.get_stream_mut(stream_idx);
            input_stream.add_fg_dst(sender, input_index, finished_flag_list);
        };

        let node = Arc::make_mut(&mut filter_graph.node);
        let SchNode::Filter { inputs, .. } = node else {
            unreachable!()
        };
        // Assign into the pad-indexed slot (pre-sized to the pad count in
        // FilterGraph::new, so `input_index` is always in range — the function
        // already indexed `filter_graph.inputs[input_index]` above). A
        // `Vec::insert` here used to panic when a cross-graph-bound pad earlier
        // in the list was skipped, leaving `input_index` past the list length;
        // pads that stay cross-graph-bound remain `None` holes. A length
        // mismatch here would be an internal invariant break, so surface it as a
        // bug rather than silently leaving a hole.
        let Some(slot) = inputs.get_mut(input_index) else {
            return Err(Error::Bug);
        };
        *slot = Some(demux.node.clone());

        demux.connect_stream(stream_idx);
        Ok(())
    }
}

/// Find input stream index by filter graph linklabel
/// FFmpeg reference: ffmpeg_filter.c - fg_create logic for parsing filter input specifiers
/// Uses StreamSpecifier for complete stream specifier parsing
fn fg_find_input_idx_by_linklabel(
    linklabel: &str,
    filter_media_type: AVMediaType,
    demuxs: &mut Vec<Demuxer>,
    desc: &str,
) -> Result<(usize, usize)> {
    // Remove brackets if present
    let new_linklabel = if linklabel.starts_with("[") && linklabel.ends_with("]") {
        if linklabel.len() <= 2 {
            warn!(target: LOG_TARGET, "Filter linklabel is empty");
            return Err(InvalidFilterSpecifier(desc.to_string()).into());
        } else {
            &linklabel[1..linklabel.len() - 1]
        }
    } else {
        linklabel
    };

    // Parse file index using strtol (FFmpeg reference: ffmpeg_opt.c:512)
    let (file_idx, remainder) =
        strtol(new_linklabel).map_err(|_| FilterGraphParseError::InvalidArgument)?;

    if file_idx < 0 || file_idx as usize >= demuxs.len() {
        return Err(InvalidFileIndexInFg(file_idx as usize, desc.to_string()).into());
    }
    let file_idx = file_idx as usize;

    // Parse stream specifier using StreamSpecifier
    let spec_str = if remainder.is_empty() {
        // No specifier - will match by media type
        ""
    } else if remainder.starts_with(':') {
        &remainder[1..]
    } else {
        remainder
    };

    let stream_spec = if spec_str.is_empty() {
        // No specifier: create one matching the filter's media type
        let mut spec = StreamSpecifier::default();
        spec.media_type = Some(filter_media_type);
        spec
    } else {
        // Parse the specifier
        StreamSpecifier::parse(spec_str).map_err(|e| {
            warn!(target: LOG_TARGET,
                "Invalid stream specifier in filter linklabel '{}': {}",
                linklabel, e
            );
            FilterGraphParseError::InvalidArgument
        })?
    };

    // Find first matching stream
    let demux = &demuxs[file_idx];
    unsafe {
        let fmt_ctx = demux.in_fmt_ctx_ptr();

        let mut subtitle_only_match = false;
        for (idx, _) in demux.get_streams().iter().enumerate() {
            let avstream = *(*fmt_ctx).streams.add(idx);

            if stream_spec.matches(fmt_ctx, avstream) {
                // Additional check: must match filter's media type
                let codec_type = (*avstream).codecpar.as_ref().unwrap().codec_type;
                if codec_type == filter_media_type {
                    return Ok((file_idx, idx));
                }
                if codec_type == AVMEDIA_TYPE_SUBTITLE && filter_media_type == AVMEDIA_TYPE_VIDEO {
                    subtitle_only_match = true;
                }
            }
        }

        if subtitle_only_match {
            // The spec names a subtitle stream feeding a VIDEO pad: that is
            // fftools' sub2video hack, which this crate does not implement.
            // Fail with a specific message instead of "matches no streams".
            error!(target: LOG_TARGET,
                "Stream specifier '{remainder}' in filtergraph description {desc} \
                 matches a subtitle stream, but subtitle streams as filtergraph \
                 inputs (sub2video) are not supported"
            );
            return Err(FilterGraphParseError::InvalidArgument.into());
        }
    }

    // No matching stream found
    warn!(target: LOG_TARGET,
        "Stream specifier '{}' in filtergraph description {} matches no streams.",
        remainder, desc
    );
    Err(FilterGraphParseError::InvalidArgument.into())
}

pub(super) fn init_filter_graphs(filter_complexs: Vec<FilterComplex>) -> Result<Vec<FilterGraph>> {
    let mut filter_graphs = Vec::with_capacity(filter_complexs.len());
    for (i, filter) in filter_complexs.iter().enumerate() {
        let filter_graph = init_filter_graph(
            i,
            &filter.filter_descs,
            filter.hw_device.clone(),
            filter.sws_opts.clone(),
            filter.swr_opts.clone(),
        )?;
        filter_graphs.push(filter_graph);
    }
    Ok(filter_graphs)
}

#[cfg(docsrs)]
pub(super) fn init_filter_graph(
    fg_index: usize,
    filter_desc: &str,
    hw_device: Option<String>,
    sws_opts: Option<String>,
    swr_opts: Option<String>,
) -> Result<FilterGraph> {
    Err(Error::Bug)
}

#[cfg(not(docsrs))]
pub(super) fn init_filter_graph(
    fg_index: usize,
    filter_desc: &str,
    hw_device: Option<String>,
    sws_opts: Option<String>,
    swr_opts: Option<String>,
) -> Result<FilterGraph> {
    let desc_cstr = CString::new(filter_desc)?;

    // fftools 0f5592cfc737: hardware devices must exist before fg_create's
    // probe parse, because some HW filters refuse to initialize without one.
    // ez-ffmpeg's equivalent ordering: register the graph's filter device
    // here at build time (it used to happen at filter-task startup, i.e.
    // after this probe had already run — and failed — for such graphs).
    if let Some(hw_device) = &hw_device {
        let err = init_filter_hw_device(hw_device);
        if err < 0 {
            // Keep the pre-move error shape: this used to surface from the
            // filter task as FilterGraph(ParseError(..)) at scheduler start;
            // only the timing moved to build(), not the variant callers
            // match on.
            return Err(Error::FilterGraph(FilterGraphOperationError::ParseError(
                FilterGraphParseError::from(err),
            )));
        }
    }

    unsafe {
        /* this graph is only used for determining the kinds of inputs
        and outputs we have, and is discarded on exit from this function;
        its filters are created but (except dynamic-pad ones) never
        initialized, so no filter side effects run at build() time */
        // Owned handle: `raw::FilterGraph`'s Drop frees the graph (and every
        // filter context it holds) on every return path below.
        let graph = crate::raw::FilterGraph::alloc().ok_or(FilterGraphParseError::OutOfMemory)?;
        (*graph.as_ptr()).nb_threads = 1;

        let mut seg = null_mut();
        let mut ret = avfilter_graph_segment_parse(graph.as_ptr(), desc_cstr.as_ptr(), 0, &mut seg);
        if ret < 0 {
            return Err(FilterGraphParseError::from(ret).into());
        }

        ret = avfilter_graph_segment_create_filters(seg, 0);
        if ret < 0 {
            avfilter_graph_segment_free(&mut seg);
            return Err(FilterGraphParseError::from(ret).into());
        }

        // Same injection point as the scheduler's graph_parse: the filters
        // exist but are not initialized yet, so a HW-flagged filter can still
        // receive its device — fftools 0f5592cfc737 gave the probe-only parse
        // this ability, and FFmpeg 8 filters may hard-require it in init.
        if let Some(dev) = hw_device_for_filter() {
            for i in 0..(*graph.as_ptr()).nb_filters {
                let f = *(*graph.as_ptr()).filters.add(i as usize);
                if (*(*f).filter).flags & AVFILTER_FLAG_HWDEVICE == 0 {
                    continue;
                }
                (*f).hw_device_ctx = av_buffer_ref(dev.device_ref());
                if (*f).hw_device_ctx.is_null() {
                    avfilter_graph_segment_free(&mut seg);
                    return Err(FilterGraphParseError::OutOfMemory.into());
                }
            }
        }

        ret = graph_opts_apply(seg);
        if ret < 0 {
            avfilter_graph_segment_free(&mut seg);
            return Err(FilterGraphParseError::from(ret).into());
        }

        // NOT avfilter_graph_segment_apply: its init stage would run every
        // filter's side effects (file opens, model/font loads, destination
        // truncation) at build() time, violating the documented build()
        // contract. Only topology-shaping (dynamic-pad) filters are
        // initialized; the open pads are then computed by mirroring the
        // upstream link pass (see fg_probe). Init-time errors of the filters
        // this skips surface from the runtime parse — at the job's first
        // configuration — instead of from build().
        ret = fg_probe::init_topology_filters(seg);
        if ret < 0 {
            avfilter_graph_segment_free(&mut seg);
            return Err(FilterGraphParseError::from(ret).into());
        }

        let topology = fg_probe::probe_open_pads(seg);
        avfilter_graph_segment_free(&mut seg);
        let topology = topology?;

        let mut input_filters = Vec::with_capacity(topology.inputs.len());
        for (filter_index, pad) in topology.inputs.into_iter().enumerate() {
            let fallback = frame_alloc()?;
            let mut filter = InputFilter::new(pad.linklabel, pad.media_type, pad.name, fallback);
            filter.opts.name = format!("fg:{fg_index}:{filter_index}");
            input_filters.push(filter);
        }
        let output_filters = topology
            .outputs
            .into_iter()
            .map(|pad| OutputFilter::new(pad.linklabel, pad.media_type, pad.name))
            .collect::<Vec<_>>();

        // Keep the zero-OUTPUTS check first so a closed zero-in/zero-out graph
        // (e.g. `color=...,nullsink`) keeps returning FilterZeroOutputs as before.
        if output_filters.is_empty() {
            return Err(FilterZeroOutputs);
        }

        // A source-only graph (e.g. `color=...`) has no input pads, so nothing
        // binds it to a demuxer and `unchoke_for_stream` would later index an empty
        // input list. Reject it up front, mirroring the zero-outputs guard (use a
        // lavfi Input for a pure generator instead of a filter_complex).
        if input_filters.is_empty() {
            return Err(FilterZeroInputs);
        }

        let filter_graph = FilterGraph::new(
            filter_desc.to_string(),
            input_filters,
            output_filters,
            sws_opts,
            swr_opts,
        );

        Ok(filter_graph)
    }
}

/// Shape summary of a writer filtergraph description, computed by the same
/// probe `init_filter_graph` runs but reported BEFORE any zero-pad rejection,
/// so the writer build path can validate the full 1-video-in/1-video-out/
/// single-component contract with its own typed errors.
pub(super) struct WriterFilterShape {
    pub(super) input_pads: usize,
    pub(super) video_input_pads: usize,
    pub(super) output_pads: usize,
    pub(super) video_output_pads: usize,
    /// Weakly-connected filter components (see `ProbedTopology`). A
    /// description like `"nullsink;color"` parses to 1 open input + 1 open
    /// output but 2 components: the pushed frames would feed a sink while an
    /// unrelated source feeds the encoder — possibly forever.
    pub(super) components: usize,
}

#[cfg(docsrs)]
pub(super) fn probe_writer_filter_shape(filter_desc: &str) -> Result<WriterFilterShape> {
    let _ = filter_desc;
    Err(Error::Bug)
}

#[cfg(not(docsrs))]
pub(super) fn probe_writer_filter_shape(filter_desc: &str) -> Result<WriterFilterShape> {
    let desc_cstr = CString::new(filter_desc)?;
    unsafe {
        // Same probe preamble as init_filter_graph: parse, create (but never
        // fully init) the filters, apply options, init only topology-shaping
        // filters, then mirror the link pass.
        let graph = crate::raw::FilterGraph::alloc().ok_or(FilterGraphParseError::OutOfMemory)?;
        (*graph.as_ptr()).nb_threads = 1;

        let mut seg = null_mut();
        let mut ret = avfilter_graph_segment_parse(graph.as_ptr(), desc_cstr.as_ptr(), 0, &mut seg);
        if ret < 0 {
            return Err(FilterGraphParseError::from(ret).into());
        }
        ret = avfilter_graph_segment_create_filters(seg, 0);
        if ret < 0 {
            avfilter_graph_segment_free(&mut seg);
            return Err(FilterGraphParseError::from(ret).into());
        }
        ret = graph_opts_apply(seg);
        if ret < 0 {
            avfilter_graph_segment_free(&mut seg);
            return Err(FilterGraphParseError::from(ret).into());
        }
        ret = fg_probe::init_topology_filters(seg);
        if ret < 0 {
            avfilter_graph_segment_free(&mut seg);
            return Err(FilterGraphParseError::from(ret).into());
        }

        let topology = fg_probe::probe_open_pads(seg);
        avfilter_graph_segment_free(&mut seg);
        let topology = topology?;

        let video = |pads: &[fg_probe::ProbedPad]| {
            pads.iter()
                .filter(|p| p.media_type == AVMEDIA_TYPE_VIDEO)
                .count()
        };
        Ok(WriterFilterShape {
            input_pads: topology.inputs.len(),
            video_input_pads: video(&topology.inputs),
            output_pads: topology.outputs.len(),
            video_output_pads: video(&topology.outputs),
            components: topology.filter_components,
        })
    }
}
