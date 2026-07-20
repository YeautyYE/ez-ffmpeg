use super::fg_bind::{ifilter_bind_ist, init_filter_graph, probe_writer_filter_shape};
use super::*;

/// Process metadata for output file
///
/// Mirrors FFmpeg's `copy_meta()` flow (ffmpeg_mux_init.c:2913-2983):
/// 1. Metadata mappings (`copy_metadata`)
/// 2. Chapter auto-copy (`copy_chapters`)
/// 3. Default auto-copy (`copy_metadata_default`)
/// 4. User-specified metadata (`of_add_metadata`)
pub(super) unsafe fn process_metadata(mux: &Muxer, demuxs: &Vec<Demuxer>) -> Result<()> {
    use crate::core::metadata::MetadataType;
    use crate::core::metadata::{
        copy_chapters_from_input, copy_metadata, copy_metadata_default, of_add_metadata,
    };

    // Collect input format contexts for metadata copying
    let input_ctxs: Vec<*const AVFormatContext> = demuxs
        .iter()
        .map(|d| d.in_fmt_ctx_ptr() as *const AVFormatContext)
        .collect();

    let mut metadata_global_manual = false;
    let mut metadata_streams_manual = false;
    let mut metadata_chapters_manual = false;

    // Step 1: Process explicit metadata mappings from user (-map_metadata option)
    // FFmpeg: ffmpeg_mux_init.c:2923-2937
    let mut mark_manual = |meta_type: &MetadataType| -> () {
        match meta_type {
            MetadataType::Global => metadata_global_manual = true,
            MetadataType::Stream(_) => metadata_streams_manual = true,
            MetadataType::Chapter(_) => metadata_chapters_manual = true,
            MetadataType::Program(_) => {}
        }
    };

    for mapping in &mux.metadata_map {
        mark_manual(&mapping.src_type);
        mark_manual(&mapping.dst_type);

        if mapping.input_index >= input_ctxs.len() {
            log::warn!(target: LOG_TARGET,
                "Metadata mapping references non-existent input file index {}",
                mapping.input_index
            );
            continue;
        }

        let input_ctx = input_ctxs[mapping.input_index];
        if let Err(e) = copy_metadata(
            input_ctx,
            mux.out_fmt_ctx_ptr(),
            &mapping.src_type,
            &mapping.dst_type,
        ) {
            log::warn!(target: LOG_TARGET, "Failed to copy metadata from mapping: {}", e);
        }
    }

    // Step 2: Copy chapters from first input with chapters (if auto copy enabled)
    if mux.auto_copy_metadata && !metadata_chapters_manual {
        if let Some(source_demux) = demuxs.iter().find(|d| unsafe {
            !d.in_fmt_ctx_ptr().is_null() && (*d.in_fmt_ctx_ptr()).nb_chapters > 0
        }) {
            if let Err(e) = copy_chapters_from_input(
                source_demux.in_fmt_ctx_ptr(),
                source_demux.ts_offset,
                mux.out_fmt_ctx_ptr(),
                mux.start_time_us,
                mux.recording_time_us,
                true,
            ) {
                log::warn!(target: LOG_TARGET, "Failed to copy chapters: {}", e);
            }
        }
    }

    // Step 3: Apply FFmpeg's automatic metadata copying (if not disabled by user)
    // FFmpeg: ffmpeg_mux_init.c:2962-2983
    let stream_input_mapping = mux.stream_input_mapping();
    let encoding_streams = mux.encoding_streams();

    if mux.auto_copy_metadata {
        if let Err(e) = copy_metadata_default(
            &input_ctxs,
            mux.out_fmt_ctx_ptr(),
            mux.nb_streams,
            &stream_input_mapping,
            &encoding_streams,
            mux.recording_time_us.is_some(),
            mux.auto_copy_metadata,
            metadata_global_manual,
            metadata_streams_manual,
        ) {
            log::warn!(target: LOG_TARGET, "Failed to apply default metadata behavior: {}", e);
        }
    }

    // Step 4: Apply user-specified metadata values (-metadata option)
    // FFmpeg: of_add_metadata runs right after copy_meta (ffmpeg_mux_init.c:3344,3356)
    if let Err(e) = of_add_metadata(
        mux.out_fmt_ctx_ptr(),
        &mux.global_metadata,
        &mux.stream_metadata,
        &mux.chapter_metadata,
        &mux.program_metadata,
    ) {
        log::warn!(target: LOG_TARGET, "Failed to add user metadata: {}", e);
    }

    Ok(())
}

/// Check if linklabel refers to a filter graph output
/// FFmpeg reference: ffmpeg_opt.c:493 - if (arg[0] == '[')
fn is_filter_output_linklabel(linklabel: &str) -> bool {
    linklabel.starts_with('[')
}

/// Parse and expand stream map specifications
/// This mimics FFmpeg's opt_map() behavior: parse once, expand immediately
/// FFmpeg reference: ffmpeg_opt.c:478-596
unsafe fn expand_stream_maps(mux: &mut Muxer, demuxs: &[Demuxer]) -> Result<()> {
    let stream_map_specs = std::mem::take(&mut mux.stream_map_specs);

    for spec in stream_map_specs {
        // FFmpeg reference: opt_map line 488-491 - check for negative map
        let (linklabel, is_negative) = if spec.linklabel.starts_with('-') {
            (&spec.linklabel[1..], true)
        } else {
            (spec.linklabel.as_str(), false)
        };

        // FFmpeg reference: opt_map line 493 - check if this mapping refers to lavfi output
        if is_filter_output_linklabel(linklabel) {
            // FFmpeg reference: opt_map line 494-507 - extract linklabel from brackets
            let pure_linklabel = if linklabel.starts_with('[') {
                // Extract content between '[' and ']'
                if let Some(end_pos) = linklabel.find(']') {
                    &linklabel[1..end_pos]
                } else {
                    warn!(target: LOG_TARGET, "Invalid output link label: {}.", linklabel);
                    return Err(Error::OpenOutput(OpenOutputError::InvalidArgument));
                }
            } else {
                linklabel
            };

            // FFmpeg reference: opt_map line 504 - validate non-empty
            if pure_linklabel.is_empty() {
                warn!(target: LOG_TARGET, "Invalid output link label: {}.", linklabel);
                return Err(Error::OpenOutput(OpenOutputError::InvalidArgument));
            }

            // Store pure linklabel (without brackets) for later matching in map_manual()
            mux.stream_maps.push(StreamMap {
                file_index: 0,   // Not used for filter outputs
                stream_index: 0, // Not used for filter outputs
                linklabel: Some(pure_linklabel.to_string()),
                copy: spec.copy,
                disabled: false,
            });
            continue;
        }

        // FFmpeg reference: opt_map line 512 - parse file index using strtol
        // Try to parse as file stream; if it fails, treat as filter output
        let parse_result = strtol(linklabel);
        if parse_result.is_err() {
            // Failed to parse file index - treat as filter output linklabel
            // This allows bare filter output names like "my-out" (without brackets)
            mux.stream_maps.push(StreamMap {
                file_index: 0,
                stream_index: 0,
                linklabel: Some(linklabel.to_string()),
                copy: spec.copy,
                disabled: false,
            });
            continue;
        }

        let (file_idx, remainder) = parse_result.unwrap();

        // FFmpeg reference: opt_map line 513-517 - validate file index
        if file_idx < 0 || file_idx as usize >= demuxs.len() {
            warn!(target: LOG_TARGET, "Invalid input file index: {}.", file_idx);
            return Err(Error::OpenOutput(OpenOutputError::InvalidArgument));
        }
        let file_idx = file_idx as usize;

        // FFmpeg reference: opt_map line 520 - parse stream specifier
        // FFmpeg reference: opt_map line 533 - handle '?' suffix for allow_unused
        let (spec_str, allow_unused) = if remainder.ends_with('?') {
            (&remainder[..remainder.len() - 1], true)
        } else {
            (remainder, false)
        };

        let stream_spec = if spec_str.is_empty() {
            // Empty specifier matches all streams (FFmpeg behavior)
            StreamSpecifier::default()
        } else {
            // Strip leading ':' and parse stream specifier
            let spec_str = if spec_str.starts_with(':') {
                &spec_str[1..]
            } else {
                spec_str
            };

            StreamSpecifier::parse(spec_str).map_err(|e| {
                warn!(target: LOG_TARGET, "Invalid stream specifier in '{}': {}", linklabel, e);
                Error::OpenOutput(OpenOutputError::InvalidArgument)
            })?
        };

        // FFmpeg reference: opt_map line 543-553 - negative map: disable matching streams
        if is_negative {
            for existing_map in &mut mux.stream_maps {
                // Only process file-based maps (not filter outputs)
                if existing_map.linklabel.is_none() && existing_map.file_index == file_idx {
                    // Check if stream specifier matches
                    let demux = &demuxs[file_idx];
                    let fmt_ctx = demux.in_fmt_ctx_ptr();
                    let avstream = *(*fmt_ctx).streams.add(existing_map.stream_index);

                    if stream_spec.matches(fmt_ctx, avstream) {
                        existing_map.disabled = true;
                    }
                }
            }
            // Negative map doesn't add new entries, just disables existing ones
            continue;
        }

        // FFmpeg reference: opt_map line 555-574 - expand to one StreamMap per matched stream
        let demux = &demuxs[file_idx];
        let fmt_ctx = demux.in_fmt_ctx_ptr();
        let mut matched_count = 0;

        for (stream_idx, _) in demux.get_streams().iter().enumerate() {
            let avstream = *(*fmt_ctx).streams.add(stream_idx);

            // FFmpeg reference: opt_map line 556 - stream_specifier_match
            if stream_spec.matches(fmt_ctx, avstream) {
                mux.stream_maps.push(StreamMap {
                    file_index: file_idx,
                    stream_index: stream_idx,
                    linklabel: None,
                    copy: spec.copy,
                    disabled: false,
                });
                matched_count += 1;
            }
        }

        // FFmpeg reference: opt_map lines 577-590 - error handling for no matches
        if matched_count == 0 {
            if allow_unused {
                // FFmpeg line 579: verbose log for optional mappings
                info!(target: LOG_TARGET, "Stream map '{}' matches no streams; ignoring.", linklabel);
            } else {
                // FFmpeg line 586-587: fatal error with hint about '?' suffix
                warn!(target: LOG_TARGET,
                    "Stream map '{}' matches no streams.\n\
                     To ignore this, add a trailing '?' to the map.",
                    linklabel
                );
                return Err(Error::OpenOutput(OpenOutputError::MatchesNoStreams(
                    linklabel.to_string(),
                )));
            }
        }
    }

    Ok(())
}

pub(super) fn outputs_bind(
    muxs: &mut Vec<Muxer>,
    filter_graphs: &mut Vec<FilterGraph>,
    demuxs: &mut Vec<Demuxer>,
) -> Result<()> {
    // FFmpeg reference: ffmpeg.c calls opt_map during command-line parsing
    // We parse and expand stream maps early, before processing individual streams
    // This must happen AFTER demuxers are opened (need stream info for matching)
    // but BEFORE map_manual() is called (which uses the expanded StreamMap entries)
    unsafe {
        for mux in muxs.iter_mut() {
            if !mux.stream_map_specs.is_empty() {
                expand_stream_maps(mux, demuxs)?;
            }
        }
    }

    // Resolved -vf matrix (each rule enforced at its own assignment point,
    // so no spelling can dodge it and no legal layout is over-rejected):
    //   complex video  -> SimpleAndComplexFilter, raised where an unlabeled
    //                     graph output is ACTUALLY assigned to an output
    //                     that carries a simple filter
    //                     (output_bind_by_unlabeled_filter), or where a
    //                     labeled graph output is mapped to one (map_manual).
    //                     When any output uses set_video_filter, unlabeled
    //                     assignment runs in fftools order — for every
    //                     output, before its manual/automatic mapping
    //                     (ffmpeg_mux_init.c create_streams) — so explicit
    //                     maps cannot reroute the graph past the conflict,
    //                     while an output the graph does NOT land on keeps
    //                     its own simple filter, exactly like FFmpeg.
    //   video copy     -> FilterWithStreamCopy (open_output_file for
    //                     -c:v copy, map_manual for copy maps);
    //   direct encode  -> the filter binds in init_simple_filtergraph,
    //                     which records the consumption;
    //   no video       -> VideoFilterUnused (postcondition below).
    //
    // The fftools-order pass is scoped to jobs that use the per-output
    // filter: with no set_video_filter anywhere, the legacy order (unlabeled
    // binding only for map-less outputs) is preserved bit for bit for
    // existing callers.
    let fftools_unlabeled_order = muxs.iter().any(|mux| mux.video_filter.is_some());

    for (i, mux) in muxs.iter_mut().enumerate() {
        // Initialize auto_disable with muxer's stream disable flags
        // FFmpeg reference: fftools/ffmpeg_mux_init.c:1891-1895
        // auto_disable bitmask: 1 << AVMEDIA_TYPE_* disables that stream type
        let mut auto_disable = 0i32;
        if mux.video_disable {
            auto_disable |= 1 << (AVMEDIA_TYPE_VIDEO as i32);
        }
        if mux.audio_disable {
            auto_disable |= 1 << (AVMEDIA_TYPE_AUDIO as i32);
        }
        if mux.subtitle_disable {
            auto_disable |= 1 << (AVMEDIA_TYPE_SUBTITLE as i32);
        }
        if mux.data_disable {
            auto_disable |= 1 << (AVMEDIA_TYPE_DATA as i32);
        }

        if fftools_unlabeled_order {
            output_bind_by_unlabeled_filter(i, mux, filter_graphs, &mut auto_disable)?;
        }

        if mux.stream_maps.is_empty() {
            if !fftools_unlabeled_order {
                output_bind_by_unlabeled_filter(i, mux, filter_graphs, &mut auto_disable)?;
            }
            /* pick the first stream of each type */
            map_auto_streams(i, mux, demuxs, filter_graphs, auto_disable)?;
        } else {
            for stream_map in mux.stream_maps.clone() {
                map_manual(i, mux, &stream_map, filter_graphs, demuxs)?;
            }
        }

        // Filter-consumption postcondition: a configured -vf that no direct
        // encoded-video stream consumed (audio-only input or maps,
        // disable_video, an optional video map that matched nothing) must
        // not be dropped silently.
        if let Some(video_filter) = &mux.video_filter {
            if !mux.video_filter_bound {
                return Err(OpenOutputError::VideoFilterUnused(video_filter.clone()).into());
            }
        }

        // Create attachment streams (`-attach`) AFTER stream mapping and BEFORE
        // metadata, mirroring FFmpeg's `of_add_attachments`. They take the
        // highest stream indices and never enter the packet path.
        // SAFETY: the output AVFormatContext is live and still owned by `mux`
        // here (the mux worker has not been spawned yet).
        unsafe {
            crate::core::context::attachment::create_attachment_streams(mux)?;
        }

        // Process metadata
        unsafe {
            process_metadata(mux, demuxs)?;
        }
    }

    Ok(())
}

fn map_manual(
    index: usize,
    mux: &mut Muxer,
    stream_map: &StreamMap,
    filter_graphs: &mut Vec<FilterGraph>,
    demuxs: &mut Vec<Demuxer>,
) -> Result<()> {
    // FFmpeg reference: ffmpeg_mux_init.c:1720-1721 - check disabled flag
    if stream_map.disabled {
        return Ok(());
    }

    // FFmpeg reference: ffmpeg_mux_init.c:1723 - check for filter output
    if let Some(linklabel) = &stream_map.linklabel {
        // This is a filter graph output - match by linklabel
        for filter_graph in filter_graphs.iter_mut() {
            for i in 0..filter_graph.outputs.len() {
                let option = {
                    let output_filter = &filter_graph.outputs[i];
                    if output_filter.has_dst()
                        || output_filter.linklabel.is_empty()
                        || &output_filter.linklabel != linklabel
                    {
                        continue;
                    }

                    // -vf + a video stream mapped from a complex graph: the
                    // per-output filter could not be applied anywhere, so
                    // refuse the pair like the CLI (ffmpeg_mux_init.c
                    // ost_get_filters) instead of silently dropping it.
                    if output_filter.media_type == AVMEDIA_TYPE_VIDEO {
                        if let Some(video_filter) = &mux.video_filter {
                            return Err(OpenOutputError::SimpleAndComplexFilter(
                                video_filter.clone(),
                            )
                            .into());
                        }
                    }

                    choose_encoder(mux, output_filter.media_type)?
                };

                match option {
                    None => {
                        // FFmpeg reference: ffmpeg_mux_init.c:1237-1242
                        // Filtering and streamcopy cannot be used together
                        error!(target: LOG_TARGET,
                            "Filtering and streamcopy cannot be used together. \
                             No encoder available for filter output type {:?}.",
                            filter_graph.outputs[i].media_type
                        );
                        return Err(OpenOutputError::InvalidArgument.into());
                    }
                    Some((codec_id, enc)) => {
                        return ofilter_bind_ost(
                            index,
                            mux,
                            filter_graph,
                            i,
                            codec_id,
                            enc,
                            None,
                            false,
                        )
                        .map(|_| ());
                    }
                }
            }
        }

        // FFmpeg reference: ffmpeg_mux_init.c:1740-1742 - filter output not found error
        warn!(target: LOG_TARGET,
            "Output with label '{}' does not exist in any defined filter graph, \
             or was already used elsewhere.",
            linklabel
        );
        return Err(OpenOutputError::InvalidArgument.into());
    }

    // This is an input file stream - use pre-parsed file_index and stream_index
    // These were already validated and expanded in expand_stream_maps()
    let demux_idx = stream_map.file_index;
    let stream_index = stream_map.stream_index;

    let demux = &mut demuxs[demux_idx];

    // Get immutable data first to avoid borrow checker issues
    let demux_node = demux.node.clone();
    let (media_type, input_stream_duration, input_stream_time_base) = {
        let input_stream = demux.get_stream_mut(stream_index);
        (
            input_stream.codec_type,
            input_stream.duration,
            input_stream.time_base,
        )
    };

    // A copy stream map covering a video stream conflicts with a per-output
    // video filter exactly like `-c:v copy` does (that spelling is rejected
    // at build time in open_output_file); map specifiers only expand to
    // concrete streams here, after the inputs are open.
    if stream_map.copy && media_type == AVMEDIA_TYPE_VIDEO {
        if let Some(video_filter) = &mux.video_filter {
            return Err(OpenOutputError::FilterWithStreamCopy(video_filter.clone()).into());
        }
    }

    // FFmpeg reference: fftools/ffmpeg_mux_init.c:1761-1768
    // Check stream disable flags for manual mapping
    // If a stream type is disabled, skip mapping even if explicitly requested
    match media_type {
        AVMEDIA_TYPE_VIDEO if mux.video_disable => {
            info!(target: LOG_TARGET, "Skipping video stream mapping (video_disable=true)");
            return Ok(());
        }
        AVMEDIA_TYPE_AUDIO if mux.audio_disable => {
            info!(target: LOG_TARGET, "Skipping audio stream mapping (audio_disable=true)");
            return Ok(());
        }
        AVMEDIA_TYPE_SUBTITLE if mux.subtitle_disable => {
            info!(target: LOG_TARGET, "Skipping subtitle stream mapping (subtitle_disable=true)");
            return Ok(());
        }
        AVMEDIA_TYPE_DATA if mux.data_disable => {
            info!(target: LOG_TARGET, "Skipping data stream mapping (data_disable=true)");
            return Ok(());
        }
        _ => {}
    }

    info!(target: LOG_TARGET,
        "Binding output stream to input {}:{} ({})",
        demux_idx,
        stream_index,
        match media_type {
            AVMEDIA_TYPE_VIDEO => "video",
            AVMEDIA_TYPE_AUDIO => "audio",
            AVMEDIA_TYPE_SUBTITLE => "subtitle",
            AVMEDIA_TYPE_DATA => "data",
            AVMEDIA_TYPE_ATTACHMENT => "attachment",
            _ => "unknown",
        }
    );

    let option = choose_encoder(mux, media_type)?;

    match option {
        None => {
            // copy
            let (packet_sender, pre_sender, gate, _st, output_stream_index) =
                mux.new_copy_stream(demux_node)?;
            demux.add_packet_dst(
                packet_sender,
                stream_index,
                output_stream_index,
                // Architecture Y': a `-shortest` copy follower carries its mux
                // stream's `source_finished` so the demux can stop producing it
                // once `sq_mux` cascade-finishes it. `None` otherwise.
                if mux.shortest {
                    mux.stream_source_finished(output_stream_index)
                } else {
                    None
                },
                CopyMuxHandle { pre_sender, gate },
            );
            mux.register_stream_source(output_stream_index, demux_idx, stream_index, false);

            unsafe {
                streamcopy_init(
                    mux,
                    *(*demux.in_fmt_ctx_ptr()).streams.add(stream_index),
                    *(*mux.out_fmt_ctx_ptr()).streams.add(output_stream_index),
                    demux.framerate,
                )?;
                rescale_duration(
                    input_stream_duration,
                    input_stream_time_base,
                    *(*mux.out_fmt_ctx_ptr()).streams.add(output_stream_index),
                );
                mux.stream_ready()
            }
        }
        Some((codec_id, enc)) => {
            // connect input_stream to output
            if stream_map.copy {
                // copy
                let (packet_sender, pre_sender, gate, _st, output_stream_index) =
                    mux.new_copy_stream(demux_node)?;
                demux.add_packet_dst(
                    packet_sender,
                    stream_index,
                    output_stream_index,
                    // Architecture Y': a `-shortest` copy follower carries its mux
                    // stream's `source_finished` so the demux can stop producing it
                    // once `sq_mux` cascade-finishes it. `None` otherwise.
                    if mux.shortest {
                        mux.stream_source_finished(output_stream_index)
                    } else {
                        None
                    },
                    CopyMuxHandle { pre_sender, gate },
                );
                mux.register_stream_source(output_stream_index, demux_idx, stream_index, false);

                unsafe {
                    streamcopy_init(
                        mux,
                        *(*demux.in_fmt_ctx_ptr()).streams.add(stream_index),
                        *(*mux.out_fmt_ctx_ptr()).streams.add(output_stream_index),
                        demux.framerate,
                    )?;
                    rescale_duration(
                        input_stream_duration,
                        input_stream_time_base,
                        *(*mux.out_fmt_ctx_ptr()).streams.add(output_stream_index),
                    );
                    mux.stream_ready()
                }
            } else if media_type == AVMEDIA_TYPE_VIDEO || media_type == AVMEDIA_TYPE_AUDIO {
                init_simple_filtergraph(
                    demux,
                    stream_index,
                    codec_id,
                    enc,
                    index,
                    mux,
                    filter_graphs,
                    demux_idx,
                )?;
            } else {
                let (frame_sender, output_stream_index) =
                    mux.add_enc_stream(media_type, enc, demux_node, false)?;
                let input_stream = demux.get_stream_mut(stream_index);
                input_stream.add_dst(frame_sender);
                demux.connect_stream(stream_index);
                mux.register_stream_source(output_stream_index, demux_idx, stream_index, true);

                unsafe {
                    rescale_duration(
                        input_stream_duration,
                        input_stream_time_base,
                        *(*mux.out_fmt_ctx_ptr()).streams.add(output_stream_index),
                    );
                }
            }
        }
    }

    Ok(())
}

#[cfg(not(docsrs))]
fn set_channel_layout(
    ch_layout: &mut AVChannelLayout,
    ch_layouts: &Option<Vec<AVChannelLayout>>,
    layout_requested: &AVChannelLayout,
) -> Result<()> {
    unsafe {
        // Scenario 1: If the requested layout has a specified order (not UNSPEC), copy it directly
        if layout_requested.order != AV_CHANNEL_ORDER_UNSPEC {
            let ret = av_channel_layout_copy(ch_layout, layout_requested);
            if ret < 0 {
                return Err(OpenOutputError::from(ret).into());
            }
            return Ok(());
        }

        // Scenario 2: Requested layout is UNSPEC and no encoder-supported layouts available
        // Use default layout based on channel count
        if ch_layouts.is_none() {
            av_channel_layout_default(ch_layout, layout_requested.nb_channels);
            return Ok(());
        }

        // Scenario 3: Try to match channel count from encoder's supported layouts
        if let Some(layouts) = ch_layouts {
            for layout in layouts {
                if layout.nb_channels == layout_requested.nb_channels {
                    let ret = av_channel_layout_copy(ch_layout, layout);
                    if ret < 0 {
                        return Err(OpenOutputError::from(ret).into());
                    }
                    return Ok(());
                }
            }
        }

        // Scenario 4: No matching channel count found, use default layout
        av_channel_layout_default(ch_layout, layout_requested.nb_channels);
        Ok(())
    }
}

#[cfg(docsrs)]
fn configure_output_filter_opts(
    index: usize,
    mux: &mut Muxer,
    output_filter: &mut OutputFilter,
    codec_id: AVCodecID,
    enc: *const AVCodec,
    output_stream_index: usize,
) -> Result<()> {
    Ok(())
}

#[cfg(not(docsrs))]
fn configure_output_filter_opts(
    index: usize,
    mux: &mut Muxer,
    output_filter: &mut OutputFilter,
    codec_id: AVCodecID,
    enc: *const AVCodec,
    output_stream_index: usize,
) -> Result<()> {
    unsafe {
        output_filter.opts.name = format!("#{index}:{output_stream_index}");
        output_filter.opts.enc = enc;
        output_filter.opts.trim_start_us = mux.start_time_us;
        output_filter.opts.trim_duration_us = mux.recording_time_us;
        output_filter.opts.ts_offset = mux.start_time_us;
        // NEW-SC-03: carry this output's auto-conversion tuning to the output
        // filter opts. filter_task::configure_filtergraph resolves the graph
        // level value (scale_sws_opts / aresample_swr_opts) from these when the
        // graph itself has none.
        output_filter.opts.sws_opts = mux.sws_opts.clone();
        output_filter.opts.swr_opts = mux.swr_opts.clone();

        output_filter.opts.flags = OFILTER_FLAG_DISABLE_CONVERT
            | OFILTER_FLAG_AUTOSCALE
            | if av_get_exact_bits_per_sample(codec_id) == 24 {
                OFILTER_FLAG_AUDIO_24BIT
            } else {
                0
            };

        let enc_ctx = avcodec_alloc_context3(enc);
        if enc_ctx.is_null() {
            return Err(OpenOutputError::OutOfMemory.into());
        }
        let _codec_ctx = CodecContext::new(enc_ctx);

        (*enc_ctx).thread_count = 0;

        if output_filter.media_type == AVMEDIA_TYPE_VIDEO {
            // formats
            let mut formats: *const AVPixelFormat = null();
            let mut ret = avcodec_get_supported_config(
                enc_ctx,
                null(),
                AV_CODEC_CONFIG_PIX_FORMAT,
                0,
                &mut formats as *mut _ as *mut *const libc::c_void,
                null_mut(),
            );
            if ret < 0 {
                return Err(OpenOutputError::from(ret).into());
            }

            let mut current = formats;
            let mut format_list = Vec::new();
            let mut count = 0;
            const MAX_FORMATS: usize = 512;
            while !current.is_null() && *current != AV_PIX_FMT_NONE && count < MAX_FORMATS {
                format_list.push(*current);
                current = current.add(1);
                count += 1;
            }
            if count >= MAX_FORMATS {
                warn!(target: LOG_TARGET, "Reached maximum format limit");
            }
            output_filter.opts.formats = Some(format_list);

            // framerates
            let mut framerates: *const AVRational = null();
            ret = avcodec_get_supported_config(
                enc_ctx,
                null(),
                AV_CODEC_CONFIG_FRAME_RATE,
                0,
                &mut framerates as *mut _ as *mut *const libc::c_void,
                null_mut(),
            );
            if ret < 0 {
                return Err(OpenOutputError::from(ret).into());
            }
            let mut framerate_list = Vec::new();
            let mut current = framerates;
            let mut count = 0;
            const MAX_FRAMERATES: usize = 64;
            while !current.is_null()
                && (*current).num != 0
                && (*current).den != 0
                && count < MAX_FRAMERATES
            {
                framerate_list.push(*current);
                current = current.add(1);
                count += 1;
            }
            if count >= MAX_FRAMERATES {
                warn!(target: LOG_TARGET, "Reached maximum framerate limit");
            }
            output_filter.opts.framerates = Some(framerate_list);

            if let Some(framerate) = mux.framerate {
                output_filter.opts.framerate = framerate;
            }

            // -fpsmax: only one of -r/-fpsmax may be set per stream
            // (ffmpeg_mux_init.c:618-621).
            if let Some(framerate_max) = mux.framerate_max {
                if mux.framerate.is_some() {
                    error!(target: LOG_TARGET, "Only one of framerate and framerate_max can be set for an output");
                    return Err(Error::OpenOutput(OpenOutputError::InvalidArgument));
                }
                output_filter.opts.framerate_max = framerate_max;
            }

            // Set user-requested pixel format (equivalent to -pix_fmt in FFmpeg)
            // FFmpeg reference: fftools/ffmpeg_filter.c - ofilter_bind_ost sets format from OptionsContext
            if let Some(pix_fmt) = mux.pix_fmt {
                output_filter.opts.format = pix_fmt;
            }

            // color_spaces
            let mut color_spaces: *const AVColorSpace = null();
            ret = avcodec_get_supported_config(
                enc_ctx,
                null(),
                AV_CODEC_CONFIG_COLOR_SPACE,
                0,
                &mut color_spaces as *mut _ as *mut *const libc::c_void,
                null_mut(),
            );
            if ret < 0 {
                return Err(OpenOutputError::from(ret).into());
            }
            let mut color_space_list = Vec::new();
            let mut current = color_spaces;
            let mut count = 0;
            const MAX_COLOR_SPACES: usize = 128;
            while !current.is_null()
                && *current != AVCOL_SPC_UNSPECIFIED
                && count < MAX_COLOR_SPACES
            {
                color_space_list.push(*current);
                current = current.add(1);
                count += 1;
            }
            if count >= MAX_COLOR_SPACES {
                warn!(target: LOG_TARGET, "Reached maximum color space limit");
            }
            output_filter.opts.color_spaces = Some(color_space_list);

            //color_ranges
            let mut color_ranges: *const AVColorRange = null();
            ret = avcodec_get_supported_config(
                enc_ctx,
                null(),
                AV_CODEC_CONFIG_COLOR_RANGE,
                0,
                &mut color_ranges as *mut _ as *mut *const libc::c_void,
                null_mut(),
            );
            if ret < 0 {
                return Err(OpenOutputError::from(ret).into());
            }
            let mut color_range_list = Vec::new();
            let mut current = color_ranges;
            let mut count = 0;
            const MAX_COLOR_RANGES: usize = 64;
            while !current.is_null()
                && *current != AVCOL_RANGE_UNSPECIFIED
                && count < MAX_COLOR_RANGES
            {
                color_range_list.push(*current);
                current = current.add(1);
                count += 1;
            }
            if count >= MAX_COLOR_RANGES {
                warn!(target: LOG_TARGET, "Reached maximum color range limit");
            }
            output_filter.opts.color_ranges = Some(color_range_list);

            let stream = &mux.get_streams()[output_stream_index];
            output_filter.opts.vsync_method = stream.vsync_method;
        } else {
            if let Some(sample_fmt) = &mux.audio_sample_fmt {
                output_filter.opts.audio_format = *sample_fmt;
            }
            // audio formats
            let mut audio_formats: *const AVSampleFormat = null();
            let mut ret = avcodec_get_supported_config(
                enc_ctx,
                null(),
                AV_CODEC_CONFIG_SAMPLE_FORMAT,
                0,
                &mut audio_formats as *mut _ as *mut _,
                null_mut(),
            );
            if ret < 0 {
                return Err(OpenOutputError::from(ret).into());
            }

            let mut current = audio_formats;
            let mut audio_format_list = Vec::new();
            let mut count = 0;
            const MAX_AUDIO_FORMATS: usize = 32;
            while !current.is_null() && *current != AV_SAMPLE_FMT_NONE && count < MAX_AUDIO_FORMATS
            {
                audio_format_list.push(*current);
                current = current.add(1);
                count += 1;
            }
            if count >= MAX_AUDIO_FORMATS {
                warn!(target: LOG_TARGET, "Reached maximum audio format limit");
            }
            output_filter.opts.audio_formats = Some(audio_format_list);

            if let Some(audio_sample_rate) = &mux.audio_sample_rate {
                output_filter.opts.sample_rate = *audio_sample_rate;
            }
            // sample_rates
            let mut rates: *const i32 = null();
            ret = avcodec_get_supported_config(
                enc_ctx,
                null(),
                AV_CODEC_CONFIG_SAMPLE_RATE,
                0,
                &mut rates as *mut _ as *mut _,
                null_mut(),
            );
            if ret < 0 {
                return Err(OpenOutputError::from(ret).into());
            }
            let mut rate_list = Vec::new();
            let mut current = rates;
            let mut count = 0;
            const MAX_SAMPLE_RATES: usize = 64;
            while !current.is_null() && *current != 0 && count < MAX_SAMPLE_RATES {
                rate_list.push(*current);
                current = current.add(1);
                count += 1;
            }
            if count >= MAX_SAMPLE_RATES {
                warn!(target: LOG_TARGET, "Reached maximum sample rate limit");
            }
            output_filter.opts.sample_rates = Some(rate_list);

            if let Some(channels) = &mux.audio_channels {
                output_filter.opts.ch_layout.nb_channels = *channels;
            }
            // channel_layouts
            let mut layouts: *const AVChannelLayout = null();
            ret = avcodec_get_supported_config(
                enc_ctx,
                null(),
                AV_CODEC_CONFIG_CHANNEL_LAYOUT,
                0,
                &mut layouts as *mut _ as *mut _,
                null_mut(),
            );
            if ret < 0 {
                return Err(OpenOutputError::from(ret).into());
            }
            let mut layout_list = Vec::new();
            let mut current = layouts;
            let mut count = 0;
            const MAX_CHANNEL_LAYOUTS: usize = 128;
            while !current.is_null()
                && (*current).order != AV_CHANNEL_ORDER_UNSPEC
                && count < MAX_CHANNEL_LAYOUTS
            {
                layout_list.push(*current);
                current = current.add(1);
                count += 1;
            }
            if count >= MAX_CHANNEL_LAYOUTS {
                warn!(target: LOG_TARGET, "Reached maximum channel layout limit");
            }
            output_filter.opts.ch_layouts = Some(layout_list);

            // Call set_channel_layout to resolve UNSPEC layouts to proper defaults
            // Corresponds to FFmpeg ffmpeg_filter.c:879-882
            if output_filter.opts.ch_layout.nb_channels > 0 {
                let layout_requested = output_filter.opts.ch_layout;
                set_channel_layout(
                    &mut output_filter.opts.ch_layout,
                    &output_filter.opts.ch_layouts,
                    &layout_requested,
                )?;
            }
        }
    };
    Ok(())
}

fn map_auto_streams(
    mux_index: usize,
    mux: &mut Muxer,
    demuxs: &mut Vec<Demuxer>,
    filter_graphs: &mut Vec<FilterGraph>,
    auto_disable: i32,
) -> Result<()> {
    unsafe {
        let oformat = (*mux.out_fmt_ctx_ptr()).oformat;
        map_auto_stream(
            mux_index,
            mux,
            demuxs,
            oformat,
            AVMEDIA_TYPE_VIDEO,
            filter_graphs,
            auto_disable,
        )?;
        map_auto_stream(
            mux_index,
            mux,
            demuxs,
            oformat,
            AVMEDIA_TYPE_AUDIO,
            filter_graphs,
            auto_disable,
        )?;
        map_auto_subtitle(mux, demuxs, oformat, auto_disable)?;
        map_auto_data(mux, demuxs, oformat, auto_disable)?;
    }
    Ok(())
}

#[cfg(docsrs)]
unsafe fn map_auto_subtitle(
    mux: &mut Muxer,
    demuxs: &mut Vec<Demuxer>,
    oformat: *const AVOutputFormat,
    auto_disable: i32,
) -> Result<()> {
    Ok(())
}

#[cfg(not(docsrs))]
unsafe fn map_auto_subtitle(
    mux: &mut Muxer,
    demuxs: &mut Vec<Demuxer>,
    oformat: *const AVOutputFormat,
    auto_disable: i32,
) -> Result<()> {
    if auto_disable & (1 << AVMEDIA_TYPE_SUBTITLE as i32) != 0 {
        return Ok(());
    }

    // An explicit user codec (including "copy") must not be gated on the
    // container's default subtitle encoder being available
    // (matches ffmpeg_mux_init.c:1660 `!avcodec_find_encoder(...) && !subtitle_codec_name`).
    let output_codec = avcodec_find_encoder((*oformat).subtitle_codec);
    if output_codec.is_null() && mux.subtitle_codec.is_none() {
        return Ok(());
    }
    let output_descriptor = if output_codec.is_null() {
        null()
    } else {
        avcodec_descriptor_get((*output_codec).id)
    };

    // Scan every subtitle stream of every input and map the first compatible
    // one (matches ffmpeg_mux_init.c:1701-1725, which iterates all input
    // streams and only stops after a successful mapping — an incompatible
    // first track, e.g. PGS before srt, must not end the search).
    for (demux_idx, demux) in demuxs.iter_mut().enumerate() {
        for stream_index in 0..demux.get_streams().len() {
            if demux.get_stream(stream_index).codec_type != AVMEDIA_TYPE_SUBTITLE {
                continue;
            }

            let input_descriptor =
                avcodec_descriptor_get((*demux.get_stream(stream_index).codec_parameters).codec_id);
            let mut input_props = 0;
            if !input_descriptor.is_null() {
                input_props =
                    (*input_descriptor).props & (AV_CODEC_PROP_TEXT_SUB | AV_CODEC_PROP_BITMAP_SUB);
            }
            let mut output_props = 0;
            if !output_descriptor.is_null() {
                output_props = (*output_descriptor).props
                    & (AV_CODEC_PROP_TEXT_SUB | AV_CODEC_PROP_BITMAP_SUB);
            }

            // A user-chosen codec short-circuits the text/bitmap check
            // (matches ffmpeg_mux_init.c:1679 `subtitle_codec_name || ...`).
            let compatible = mux.subtitle_codec.is_some()
                || input_props & output_props != 0
                // Map dvb teletext which has neither property to any output subtitle encoder
                || !input_descriptor.is_null() && !output_descriptor.is_null()
                    && ((*input_descriptor).props == 0 || (*output_descriptor).props == 0);
            if !compatible {
                continue;
            }

            // choose_encoder returns None for "copy": take the streamcopy
            // path like map_auto_stream instead of failing.
            let option = choose_encoder(mux, AVMEDIA_TYPE_SUBTITLE)?;
            if let Some((_codec_id, enc)) = option {
                let (frame_sender, output_stream_index) =
                    mux.add_enc_stream(AVMEDIA_TYPE_SUBTITLE, enc, demux.node.clone(), false)?;
                demux.get_stream_mut(stream_index).add_dst(frame_sender);
                demux.connect_stream(stream_index);
                mux.register_stream_source(output_stream_index, demux_idx, stream_index, true);
                let input_stream = demux.get_stream(stream_index);
                unsafe {
                    rescale_duration(
                        input_stream.duration,
                        input_stream.time_base,
                        *(*mux.out_fmt_ctx_ptr()).streams.add(output_stream_index),
                    );
                }
            } else {
                let input_stream = demux.get_stream(stream_index);
                let input_stream_duration = input_stream.duration;
                let input_stream_time_base = input_stream.time_base;

                let (packet_sender, pre_sender, gate, _st, output_stream_index) =
                    mux.new_copy_stream(demux.node.clone())?;
                demux.add_packet_dst(
                    packet_sender,
                    stream_index,
                    output_stream_index,
                    // Architecture Y': a `-shortest` copy follower carries its mux
                    // stream's `source_finished` so the demux can stop producing it
                    // once `sq_mux` cascade-finishes it. `None` otherwise.
                    if mux.shortest {
                        mux.stream_source_finished(output_stream_index)
                    } else {
                        None
                    },
                    CopyMuxHandle { pre_sender, gate },
                );
                mux.register_stream_source(output_stream_index, demux_idx, stream_index, false);

                unsafe {
                    streamcopy_init(
                        mux,
                        *(*demux.in_fmt_ctx_ptr()).streams.add(stream_index),
                        *(*mux.out_fmt_ctx_ptr()).streams.add(output_stream_index),
                        demux.framerate,
                    )?;
                    rescale_duration(
                        input_stream_duration,
                        input_stream_time_base,
                        *(*mux.out_fmt_ctx_ptr()).streams.add(output_stream_index),
                    );
                    mux.stream_ready()
                }
            }
            return Ok(());
        }
    }

    Ok(())
}

#[cfg(docsrs)]
unsafe fn map_auto_data(
    mux: &mut Muxer,
    demuxs: &mut Vec<Demuxer>,
    oformat: *const AVOutputFormat,
    auto_disable: i32,
) -> Result<()> {
    Ok(())
}

#[cfg(not(docsrs))]
unsafe fn map_auto_data(
    mux: &mut Muxer,
    demuxs: &mut Vec<Demuxer>,
    oformat: *const AVOutputFormat,
    auto_disable: i32,
) -> Result<()> {
    if auto_disable & (1 << AVMEDIA_TYPE_DATA as i32) != 0 {
        return Ok(());
    }

    /* Data only if codec id match */
    let codec_id = av_guess_codec(
        oformat,
        null(),
        (*mux.out_fmt_ctx_ptr()).url,
        null(),
        AVMEDIA_TYPE_DATA,
    );

    if codec_id == AV_CODEC_ID_NONE {
        return Ok(());
    }

    for (demux_idx, demux) in demuxs.iter_mut().enumerate() {
        let option = demux
            .get_streams()
            .iter()
            .enumerate()
            .find_map(|(index, input_stream)| {
                if input_stream.codec_type == AVMEDIA_TYPE_DATA
                    && (*input_stream.codec_parameters).codec_id == codec_id
                {
                    Some(index)
                } else {
                    None
                }
            });

        if option.is_none() {
            continue;
        }

        let stream_index = option.unwrap();
        let option = choose_encoder(mux, AVMEDIA_TYPE_DATA)?;

        if option.is_some() {
            // FFmpeg reference: ffmpeg_mux_init.c:79-89
            // choose_encoder always returns enc=NULL for AVMEDIA_TYPE_DATA
            unreachable!("DATA streams do not have encoders in FFmpeg");
        } else {
            // FFmpeg behavior: DATA streams use stream copy when no encoder is available
            // Reference: fftools/ffmpeg_mux_init.c:79-89 - choose_encoder returns enc=NULL for DATA
            // Reference: fftools/ffmpeg_mux_init.c:1236-1246 - ost_add uses stream copy when enc=NULL
            let input_stream = demux.get_stream(stream_index);
            let input_stream_duration = input_stream.duration;
            let input_stream_time_base = input_stream.time_base;

            let (packet_sender, pre_sender, gate, _st, output_stream_index) =
                mux.new_copy_stream(demux.node.clone())?;
            demux.add_packet_dst(
                packet_sender,
                stream_index,
                output_stream_index,
                // Architecture Y': a `-shortest` copy follower carries its mux
                // stream's `source_finished` so the demux can stop producing it
                // once `sq_mux` cascade-finishes it. `None` otherwise.
                if mux.shortest {
                    mux.stream_source_finished(output_stream_index)
                } else {
                    None
                },
                CopyMuxHandle { pre_sender, gate },
            );
            mux.register_stream_source(output_stream_index, demux_idx, stream_index, false);

            streamcopy_init(
                mux,
                *(*demux.in_fmt_ctx_ptr()).streams.add(stream_index),
                *(*mux.out_fmt_ctx_ptr()).streams.add(output_stream_index),
                demux.framerate,
            )?;
            rescale_duration(
                input_stream_duration,
                input_stream_time_base,
                *(*mux.out_fmt_ctx_ptr()).streams.add(output_stream_index),
            );
            mux.stream_ready()
        }

        break;
    }

    Ok(())
}

#[cfg(docsrs)]
unsafe fn map_auto_stream(
    mux_index: usize,
    mux: &mut Muxer,
    demuxs: &mut Vec<Demuxer>,
    oformat: *const AVOutputFormat,
    media_type: AVMediaType,
    filter_graphs: &mut Vec<FilterGraph>,
    auto_disable: i32,
) -> Result<()> {
    Ok(())
}

#[cfg(not(docsrs))]
unsafe fn map_auto_stream(
    mux_index: usize,
    mux: &mut Muxer,
    demuxs: &mut Vec<Demuxer>,
    oformat: *const AVOutputFormat,
    media_type: AVMediaType,
    filter_graphs: &mut Vec<FilterGraph>,
    auto_disable: i32,
) -> Result<()> {
    if auto_disable & (1 << media_type as i32) != 0 {
        return Ok(());
    }
    if (media_type == AVMEDIA_TYPE_VIDEO
        || media_type == AVMEDIA_TYPE_AUDIO
        || media_type == AVMEDIA_TYPE_DATA)
        && av_guess_codec(
            oformat,
            null(),
            (*mux.out_fmt_ctx_ptr()).url,
            null(),
            media_type,
        ) == AV_CODEC_ID_NONE
    {
        return Ok(());
    }

    // Mirror ffmpeg_mux_init.c map_auto_video/map_auto_audio: score every
    // stream of every input and map the single global best (video: highest
    // resolution; audio: most channels). Other types keep first-found.
    let selected = if media_type == AVMEDIA_TYPE_VIDEO || media_type == AVMEDIA_TYPE_AUDIO {
        unsafe { select_best_stream(demuxs, oformat, media_type) }
    } else {
        demuxs.iter().enumerate().find_map(|(demux_idx, demux)| {
            demux
                .get_streams()
                .iter()
                .position(|input_stream| input_stream.codec_type == media_type)
                .map(|stream_index| (demux_idx, stream_index))
        })
    };
    let Some((demux_idx, stream_index)) = selected else {
        return Ok(());
    };

    let demux = &mut demuxs[demux_idx];
    let input_file_idx = demux_idx;
    let option = choose_encoder(mux, media_type)?;

    if let Some((codec_id, enc)) = option {
        if media_type == AVMEDIA_TYPE_VIDEO || media_type == AVMEDIA_TYPE_AUDIO {
            init_simple_filtergraph(
                demux,
                stream_index,
                codec_id,
                enc,
                mux_index,
                mux,
                filter_graphs,
                input_file_idx,
            )?;
        } else {
            let (frame_sender, output_stream_index) =
                mux.add_enc_stream(media_type, enc, demux.node.clone(), false)?;
            demux.get_stream_mut(stream_index).add_dst(frame_sender);
            demux.connect_stream(stream_index);
            mux.register_stream_source(output_stream_index, input_file_idx, stream_index, true);
            let input_stream = demux.get_stream(stream_index);
            unsafe {
                rescale_duration(
                    input_stream.duration,
                    input_stream.time_base,
                    *(*mux.out_fmt_ctx_ptr()).streams.add(output_stream_index),
                );
            }
        }

        return Ok(());
    }

    // copy: like the encoder branch, the CLI maps exactly one stream per
    // media type.
    let input_stream = demux.get_stream(stream_index);
    let input_stream_duration = input_stream.duration;
    let input_stream_time_base = input_stream.time_base;

    let (packet_sender, pre_sender, gate, _st, output_stream_index) =
        mux.new_copy_stream(demux.node.clone())?;
    demux.add_packet_dst(
        packet_sender,
        stream_index,
        output_stream_index,
        if mux.shortest {
            mux.stream_source_finished(output_stream_index)
        } else {
            None
        },
        CopyMuxHandle { pre_sender, gate },
    );
    mux.register_stream_source(output_stream_index, input_file_idx, stream_index, false);

    unsafe {
        streamcopy_init(
            mux,
            *(*demux.in_fmt_ctx_ptr()).streams.add(stream_index),
            *(*mux.out_fmt_ctx_ptr()).streams.add(output_stream_index),
            demux.framerate,
        )?;
        rescale_duration(
            input_stream_duration,
            input_stream_time_base,
            *(*mux.out_fmt_ctx_ptr()).streams.add(output_stream_index),
        );
        mux.stream_ready()
    }

    Ok(())
}

/// Pick the globally best video/audio stream across all inputs
/// (ffmpeg_mux_init.c map_auto_video:1539 / map_auto_audio:1648): video is
/// scored by resolution, audio by channel count, both raised by
/// NEW_PACKETS/DEFAULT-disposition bonuses; attached pictures only win where
/// the container itself is picture-based (avformat_query_codec == 'APIC').
#[cfg(not(docsrs))]
unsafe fn select_best_stream(
    demuxs: &[Demuxer],
    oformat: *const AVOutputFormat,
    media_type: AVMediaType,
) -> Option<(usize, usize)> {
    const APIC_TAG: i32 =
        (b'A' as i32) | ((b'P' as i32) << 8) | ((b'I' as i32) << 16) | ((b'C' as i32) << 24);
    let qcr = if media_type == AVMEDIA_TYPE_VIDEO {
        avformat_query_codec(oformat, (*oformat).video_codec, 0)
    } else {
        0
    };

    let mut best: Option<(usize, usize)> = None;
    let mut best_score: i64 = 0;

    for (demux_idx, demux) in demuxs.iter().enumerate() {
        let mut file_best: Option<usize> = None;
        let mut file_best_score: i64 = 0;

        for (stream_index, input_stream) in demux.get_streams().iter().enumerate() {
            if input_stream.codec_type != media_type {
                continue;
            }
            // The CLI also skips AV_CODEC_PROP_ENHANCEMENT streams (LCEVC
            // enhancement layers); ffmpeg-sys-next 7.1 has no binding for
            // that flag yet, so the check is deferred to the bindings bump.

            let par = input_stream.codec_parameters;
            let st = input_stream.stream.inner;
            let attached_pic = (*st).disposition & AV_DISPOSITION_ATTACHED_PIC != 0;

            let mut score: i64 = if media_type == AVMEDIA_TYPE_VIDEO {
                (*par).width as i64 * (*par).height as i64
            } else {
                (*par).ch_layout.nb_channels as i64
            };
            if (*st).event_flags & AVSTREAM_EVENT_FLAG_NEW_PACKETS != 0 {
                score += 100_000_000;
            }
            if (*st).disposition & AV_DISPOSITION_DEFAULT != 0 {
                score += 5_000_000;
            }
            if media_type == AVMEDIA_TYPE_VIDEO && qcr != APIC_TAG && attached_pic {
                // Cover art only wins when nothing else is available.
                score = 1;
            }

            if score > file_best_score {
                if media_type == AVMEDIA_TYPE_VIDEO && qcr == APIC_TAG && !attached_pic {
                    // This container carries video only as an attached picture.
                    continue;
                }
                file_best_score = score;
                file_best = Some(stream_index);
            }
        }

        if let Some(stream_index) = file_best {
            let st = demux.get_stream(stream_index).stream.inner;
            let attached_pic = (*st).disposition & AV_DISPOSITION_ATTACHED_PIC != 0;
            // The DEFAULT-disposition bonus ranks streams within one file
            // only; drop it before comparing across files.
            if (media_type != AVMEDIA_TYPE_VIDEO || qcr == APIC_TAG || !attached_pic)
                && (*st).disposition & AV_DISPOSITION_DEFAULT != 0
            {
                file_best_score -= 5_000_000;
            }
            if file_best_score > best_score {
                best_score = file_best_score;
                best = Some((demux_idx, stream_index));
            }
        }
    }

    best
}

/// Media-type label for diagnostics (matches the strings fftools prints).
fn media_type_name(media_type: AVMediaType) -> &'static str {
    match media_type {
        AVMEDIA_TYPE_VIDEO => "video",
        AVMEDIA_TYPE_AUDIO => "audio",
        AVMEDIA_TYPE_SUBTITLE => "subtitle",
        AVMEDIA_TYPE_DATA => "data",
        AVMEDIA_TYPE_ATTACHMENT => "attachment",
        _ => "unknown",
    }
}

/// Shape rules for a per-output simple video filter, checked on the same
/// topology probe the writer path uses (fg_bind): exactly one video input
/// pad, one video output pad, one connected component, and a directed
/// input->output path. Violations are the promised typed errors, including
/// the zero-pad cases init_filter_graph would otherwise report generically
/// (fftools fg_create_simple's contract, plus the topology rules a simple
/// chain implies).
fn validate_simple_video_filter(desc: &str, expected: AVMediaType) -> Result<()> {
    let shape = probe_writer_filter_shape(desc)?;
    if shape.input_pads != 1 || shape.output_pads != 1 {
        return Err(OpenOutputError::SimpleFilterInvalidShape {
            desc: desc.to_string(),
            reason: format!(
                "expected exactly 1 input pad and 1 output pad, found {} and {}",
                shape.input_pads, shape.output_pads
            ),
        }
        .into());
    }
    for pad_type in [shape.input_pad_types[0], shape.output_pad_types[0]] {
        if pad_type != expected {
            return Err(OpenOutputError::SimpleFilterMediaTypeMismatch {
                desc: desc.to_string(),
                found: media_type_name(pad_type).to_string(),
                expected: media_type_name(expected).to_string(),
            }
            .into());
        }
    }
    if shape.components > 1 {
        return Err(OpenOutputError::SimpleFilterInvalidShape {
            desc: desc.to_string(),
            reason: format!(
                "it parses into {} disconnected sub-graphs, so the stream would not flow \
                 through the whole description",
                shape.components
            ),
        }
        .into());
    }
    if !shape.output_reachable {
        return Err(OpenOutputError::SimpleFilterInvalidShape {
            desc: desc.to_string(),
            reason: "no directed path leads from its input pad to its output pad, so the \
                     encoded stream would not contain the filtered frames"
                .to_string(),
        }
        .into());
    }
    Ok(())
}

fn init_simple_filtergraph(
    demux: &mut Demuxer,
    stream_index: usize,
    codec_id: AVCodecID,
    enc: *const AVCodec,
    mux_index: usize,
    mux: &mut Muxer,
    filter_graphs: &mut Vec<FilterGraph>,
    input_file_idx: usize,
) -> Result<()> {
    let codec_type = demux.get_stream(stream_index).codec_type;

    // The implicit per-output graph defaults to a passthrough chain; a
    // per-output video filter (Output::set_video_filter, FFmpeg -vf)
    // replaces the video `null`. Audio has no per-output filter yet.
    if codec_type == AVMEDIA_TYPE_VIDEO {
        if let Some(custom_desc) = mux.video_filter.as_deref() {
            // User-supplied text enters the graph here — validate its
            // topology BEFORE building. Pad counts alone are not enough
            // ("nullsink;color=..." probes as 1 open input + 1 open output
            // while the stream drains into the sink and the encoder consumes
            // the unrelated source forever), so require the full simple-chain
            // shape. The literal "null"/"anull" defaults need no probe.
            validate_simple_video_filter(custom_desc, codec_type)?;
            mux.video_filter_bound = true;
        }
    }
    let filter_desc = if codec_type == AVMEDIA_TYPE_VIDEO {
        mux.video_filter.as_deref().unwrap_or("null")
    } else {
        "anull"
    };
    // Implicit per-output graph: no explicit FilterComplex, so there is no
    // graph-level sws/swr value here. The per-output request (Output::set_sws_opts
    // / set_swr_opts) flows in through the bound OutputFilterOptions instead and
    // is resolved in filter_task::configure_filtergraph.
    let mut filter_graph = init_filter_graph(filter_graphs.len(), filter_desc, None, None, None)?;

    ifilter_bind_ist(&mut filter_graph, 0, stream_index, demux)?;
    // fftools ost->ist rule: fed directly by a single-stream input
    // (ffmpeg_mux_init.c:817-822).
    let single_stream_direct_input = demux.get_streams().len() == 1;
    ofilter_bind_ost(
        mux_index,
        mux,
        &mut filter_graph,
        0,
        codec_id,
        enc,
        Some((input_file_idx, stream_index)),
        single_stream_direct_input,
    )?;

    filter_graphs.push(filter_graph);

    Ok(())
}

unsafe fn rescale_duration(src_duration: i64, src_time_base: AVRational, stream: *mut AVStream) {
    (*stream).duration = av_rescale_q(src_duration, src_time_base, (*stream).time_base);
}

#[cfg(docsrs)]
fn streamcopy_init(
    mux: &mut Muxer,
    input_stream: *mut AVStream,
    output_stream: *mut AVStream,
    input_framerate: AVRational,
) -> Result<()> {
    Ok(())
}

#[cfg(not(docsrs))]
fn streamcopy_init(
    mux: &mut Muxer,
    input_stream: *mut AVStream,
    output_stream: *mut AVStream,
    input_framerate: AVRational,
) -> Result<()> {
    unsafe {
        let codec_ctx = avcodec_alloc_context3(null_mut());
        if codec_ctx.is_null() {
            return Err(OpenOutputError::OutOfMemory.into());
        }
        let _codec_context = CodecContext::new(codec_ctx);

        let mut ret = avcodec_parameters_to_context(codec_ctx, (*input_stream).codecpar);
        if ret < 0 {
            error!(target: LOG_TARGET, "Error setting up codec context options.");
            return Err(OpenOutputError::from(ret).into());
        }

        ret = avcodec_parameters_from_context((*output_stream).codecpar, codec_ctx);
        if ret < 0 {
            error!(target: LOG_TARGET, "Error getting reference codec parameters.");
            return Err(OpenOutputError::from(ret).into());
        }

        // In the CLI this is the user's -tag value, 0 when absent; ez has no
        // per-stream tag option yet. Seeding it from the copied parameters
        // made the check below unreachable, so a source tag the target
        // container cannot represent (e.g. AVI's FMP4 into mp4) was kept and
        // avformat_write_header rejected it.
        let mut codec_tag = 0;
        if codec_tag == 0 {
            let ct = (*(*mux.out_fmt_ctx_ptr()).oformat).codec_tag;
            let mut codec_tag_tmp = 0;
            if ct.is_null()
                || av_codec_get_id(ct, (*(*output_stream).codecpar).codec_tag)
                    == (*(*output_stream).codecpar).codec_id
                || av_codec_get_tag2(
                    ct,
                    (*(*output_stream).codecpar).codec_id,
                    &mut codec_tag_tmp,
                ) == 0
            {
                codec_tag = (*(*output_stream).codecpar).codec_tag;
            }
        }
        (*(*output_stream).codecpar).codec_tag = codec_tag;

        // Match FFmpeg CLI: framerate only applies to video streams.
        // In CLI, ist->framerate is only set for video (ffmpeg_demux.c:1429-1432, case AVMEDIA_TYPE_VIDEO)
        // and ost->frame_rate is only set in new_stream_video() (ffmpeg_mux_init.c:607).
        // Since ez-ffmpeg stores framerate per-file (not per-stream), we need an explicit guard
        // to prevent applying framerate to audio/subtitle streams in streamcopy mode.
        let codec_type = (*(*output_stream).codecpar).codec_type;
        let mut fr = AVRational { num: 0, den: 0 };
        if codec_type == AVMEDIA_TYPE_VIDEO {
            fr = mux.framerate.unwrap_or(AVRational { num: 0, den: 0 });
            if fr.num == 0 {
                fr = input_framerate;
            }
        }

        if fr.num != 0 {
            (*output_stream).avg_frame_rate = fr;
        } else {
            (*output_stream).avg_frame_rate = (*input_stream).avg_frame_rate;
        }

        // copy timebase while removing common factors
        if (*output_stream).time_base.num <= 0 || (*output_stream).time_base.den <= 0 {
            if fr.num != 0 {
                (*output_stream).time_base = av_inv_q(fr);
            } else {
                (*output_stream).time_base =
                    av_add_q((*input_stream).time_base, AVRational { num: 0, den: 1 });
            }
        }

        for i in 0..(*(*input_stream).codecpar).nb_coded_side_data {
            let sd_src = (*(*input_stream).codecpar)
                .coded_side_data
                .offset(i as isize);

            let sd_dst = av_packet_side_data_new(
                &mut (*(*output_stream).codecpar).coded_side_data,
                &mut (*(*output_stream).codecpar).nb_coded_side_data,
                (*sd_src).type_,
                (*sd_src).size,
                0,
            );
            if sd_dst.is_null() {
                return Err(OpenOutputError::OutOfMemory.into());
            }
            std::ptr::copy_nonoverlapping(
                (*sd_src).data as *const u8,
                (*sd_dst).data,
                (*sd_src).size,
            );
        }

        match (*(*output_stream).codecpar).codec_type {
            AVMEDIA_TYPE_AUDIO => {
                if ((*(*output_stream).codecpar).block_align == 1
                    || (*(*output_stream).codecpar).block_align == 1152
                    || (*(*output_stream).codecpar).block_align == 576)
                    && (*(*output_stream).codecpar).codec_id == AV_CODEC_ID_MP3
                {
                    (*(*output_stream).codecpar).block_align = 0;
                }
                if (*(*output_stream).codecpar).codec_id == AV_CODEC_ID_AC3 {
                    (*(*output_stream).codecpar).block_align = 0;
                }
            }
            AVMEDIA_TYPE_VIDEO => {
                let sar = if (*input_stream).sample_aspect_ratio.num != 0 {
                    (*input_stream).sample_aspect_ratio
                } else {
                    (*(*output_stream).codecpar).sample_aspect_ratio
                };
                (*output_stream).sample_aspect_ratio = sar;
                (*(*output_stream).codecpar).sample_aspect_ratio = sar;
                (*output_stream).r_frame_rate = (*input_stream).r_frame_rate;
            }
            _ => {}
        }
    };
    Ok(())
}

fn output_bind_by_unlabeled_filter(
    index: usize,
    mux: &mut Muxer,
    filter_graphs: &mut Vec<FilterGraph>,
    auto_disable: &mut i32,
) -> Result<()> {
    let fg_len = filter_graphs.len();

    for i in 0..fg_len {
        let filter_graph = &mut filter_graphs[i];

        for i in 0..filter_graph.outputs.len() {
            let media_type = filter_graph.outputs[i].media_type;

            // Per-assignment simple/complex conflict: THIS output is about to
            // receive an unlabeled graph output. If it also carries a simple
            // video filter, that is fftools' ost_get_filters error — checked
            // BEFORE the disable-flag skip, because fftools binds unlabeled
            // outputs regardless of -vn and errors on the filter anyway. An
            // output the graph does not land on is free to keep its filter.
            {
                let output_filter = &filter_graph.outputs[i];
                let assignable = (output_filter.linklabel.is_empty()
                    || output_filter.linklabel == "out")
                    && !output_filter.has_dst();
                if assignable && media_type == AVMEDIA_TYPE_VIDEO {
                    if let Some(video_filter) = &mux.video_filter {
                        return Err(OpenOutputError::SimpleAndComplexFilter(
                            video_filter.clone(),
                        )
                        .into());
                    }
                }
            }

            // Check if this stream type is disabled
            if *auto_disable & (1 << media_type as i32) != 0 {
                continue;
            }

            let option = {
                let output_filter = &filter_graph.outputs[i];
                if (!output_filter.linklabel.is_empty() && output_filter.linklabel != "out")
                    || output_filter.has_dst()
                {
                    continue;
                }

                choose_encoder(mux, output_filter.media_type)?
            };

            match option {
                None => {
                    warn!(target: LOG_TARGET,
                        "An unexpected media_type {:?} appears in output_filter",
                        media_type
                    );
                }
                Some((codec_id, enc)) => {
                    *auto_disable |= 1 << media_type as i32;
                    ofilter_bind_ost(index, mux, filter_graph, i, codec_id, enc, None, false)?;
                }
            }
        }
    }

    Ok(())
}

pub(super) fn ofilter_bind_ost(
    index: usize,
    mux: &mut Muxer,
    filter_graph: &mut FilterGraph,
    output_filter_index: usize,
    codec_id: AVCodecID,
    enc: *const AVCodec,
    stream_source: Option<(usize, usize)>,
    single_stream_direct_input: bool,
) -> Result<usize> {
    let output_filter = &mut filter_graph.outputs[output_filter_index];
    let (frame_sender, output_stream_index) = mux.add_enc_stream(
        output_filter.media_type,
        enc,
        filter_graph.node.clone(),
        single_stream_direct_input,
    )?;
    output_filter.set_dst(frame_sender);

    if let Some((file_idx, stream_idx)) = stream_source {
        mux.register_stream_source(output_stream_index, file_idx, stream_idx, true);
    }

    configure_output_filter_opts(
        index,
        mux,
        output_filter,
        codec_id,
        enc,
        output_stream_index,
    )?;
    Ok(output_stream_index)
}

pub(super) fn choose_encoder(
    mux: &Muxer,
    media_type: AVMediaType,
) -> Result<Option<(AVCodecID, *const AVCodec)>> {
    let media_codec = match media_type {
        AVMEDIA_TYPE_VIDEO => mux.video_codec.clone(),
        AVMEDIA_TYPE_AUDIO => mux.audio_codec.clone(),
        AVMEDIA_TYPE_SUBTITLE => mux.subtitle_codec.clone(),
        _ => return Ok(None),
    };

    match media_codec {
        None => {
            let url = CString::new(&*mux.url)?;
            unsafe {
                let codec_id = av_guess_codec(
                    (*mux.out_fmt_ctx_ptr()).oformat,
                    null(),
                    url.as_ptr(),
                    null(),
                    media_type,
                );
                let enc = avcodec_find_encoder(codec_id);
                if enc.is_null() {
                    let format_name = (*(*mux.out_fmt_ctx_ptr()).oformat).name;
                    let format_name = CStr::from_ptr(format_name).to_str();
                    let codec_name = avcodec_get_name(codec_id);
                    let codec_name = CStr::from_ptr(codec_name).to_str();
                    if let (Ok(format_name), Ok(codec_name)) = (format_name, codec_name) {
                        error!(target: LOG_TARGET, "Automatic encoder selection failed Default encoder for format {format_name} (codec {codec_name}) is probably disabled. Please choose an encoder manually.");
                    }
                    // Name the format-guessed codec so the caller knows what to
                    // provision. AV_CODEC_ID_NONE (the format declares no default
                    // codec for this media type) and non-UTF-8 names carry no
                    // actionable name, so they keep the generic errno mapping.
                    return match codec_name {
                        Ok(codec_name) if codec_id != AV_CODEC_ID_NONE => {
                            Err(OpenOutputError::EncoderUnavailable {
                                name: codec_name.to_string(),
                            }
                            .into())
                        }
                        _ => Err(OpenOutputError::from(AVERROR_ENCODER_NOT_FOUND).into()),
                    };
                }

                return Ok(Some((codec_id, enc)));
            }
        }
        Some(media_codec) if media_codec != "copy" => unsafe {
            let media_codec_cstr = CString::new(media_codec.clone())?;

            let mut enc = avcodec_find_encoder_by_name(media_codec_cstr.as_ptr());
            let desc = avcodec_descriptor_get_by_name(media_codec_cstr.as_ptr());

            if enc.is_null() && !desc.is_null() {
                enc = avcodec_find_encoder((*desc).id);
                if !enc.is_null() {
                    let codec_name = (*enc).name;
                    let codec_name = CStr::from_ptr(codec_name).to_str();
                    let desc_name = (*desc).name;
                    let desc_name = CStr::from_ptr(desc_name).to_str();
                    if let (Ok(codec_name), Ok(desc_name)) = (codec_name, desc_name) {
                        debug!(target: LOG_TARGET, "Matched encoder '{codec_name}' for codec '{desc_name}'.");
                    }
                }
            }

            if enc.is_null() {
                error!(target: LOG_TARGET, "Unknown encoder '{media_codec}'");
                // The caller asked for this encoder by name and the linked
                // FFmpeg build does not provide it; report it by name.
                return Err(OpenOutputError::EncoderUnavailable { name: media_codec }.into());
            }

            if (*enc).type_ != media_type {
                error!(target: LOG_TARGET, "Invalid encoder type '{media_codec}'");
                return Err(OpenOutputError::InvalidArgument.into());
            }
            let codec_id = (*enc).id;
            return Ok(Some((codec_id, enc)));
        },
        _ => {}
    };

    Ok(None)
}
