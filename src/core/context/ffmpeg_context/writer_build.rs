//! Dedicated context-construction path for [`VideoWriter`](crate::VideoWriter)
//! jobs: an [`FfmpegContext`] with **zero demuxers** whose single filtergraph
//! is fed by a headless frame source instead of a decoder.
//!
//! The regular constructor (`new_with_options`) binds every filtergraph input
//! pad to a demuxer stream and fails when none exists (`fg_bind.rs`), so the
//! writer cannot reuse it. This path adapts `init_simple_filtergraph`
//! (`opt_util.rs`): build `buffersrc -> [user filter_desc | null] -> buffersink`,
//! bind input pad 0 to the frame source (`ifilter_bind_frame_source`, the
//! frame-source analog of `ifilter_bind_ist`), and bind the output pad to an
//! encoder stream through the ordinary `ofilter_bind_ost`, preserving
//! `disable_video` / streamless-output detection.
//!
//! With zero demuxers the scheduler's demux-keyed machinery is inert by
//! construction: the choke pass is skipped (`balancing_possible` requires more
//! than one demuxer), `SchNode::Filter.inputs[0]` stays the pre-sized `None`
//! hole, and the demux waiter sets are empty — the frame source needs no wake
//! entry because its worker time-boxes both channel directions and polls the
//! scheduler status (see `scheduler::frame_source_task`).

use super::*;
use crate::core::context::frame_source::{FrameSource, FrameSourceParams};
use crate::core::writer::WriterError;
use crossbeam_channel::Sender;

use super::fg_bind::init_filter_graph;
use super::opt_util::{choose_encoder, ofilter_bind_ost, process_metadata};

/// Builds a frame-push context: one frame source, one filtergraph, one output.
/// Returns the context plus the ingress sender the facade writes into.
///
/// `queue_capacity` bounds the ingress channel only; filters, the codec and
/// output I/O buffer beyond it (see the module docs of `core::writer`).
pub(crate) fn build_writer_context(
    params: FrameSourceParams,
    queue_capacity: usize,
    filter_desc: Option<&str>,
    output: Output,
) -> Result<(FfmpegContext, Sender<Vec<u8>>)> {
    crate::core::initialize_ffmpeg();

    // Same bootstrap as new_with_options: the status atomic must exist before
    // the output context opens so its AVIO interrupt callback binds to it.
    let scheduler_status = Arc::new(std::sync::atomic::AtomicUsize::new(
        crate::core::scheduler::ffmpeg_scheduler::STATUS_INIT,
    ));
    let interrupt_state = Arc::new(crate::core::context::InterruptState::new(
        scheduler_status.clone(),
    ));

    let mut outputs = vec![output];
    let mut muxs = open_output_files(&mut outputs, false, &interrupt_state)?;

    // The graph between the pushed frames and the encoder. "null" mirrors
    // init_simple_filtergraph's implicit per-output video graph.
    let desc = filter_desc.unwrap_or("null");
    let mut filter_graph = init_filter_graph(0, desc, None, None, None)?;

    // Exactly one video input pad and one video output pad. init_filter_graph
    // rejects only zero-pad graphs; a multi-input filter_desc would strand pad
    // 1 with no producer — its pre-config buffering would wait forever — and a
    // multi-output graph would leave pad 1 unbound. Audio pads have no source
    // or destination in a video-only writer, so they count as invalid too.
    let input_pads = filter_graph.inputs.len();
    let output_pads = filter_graph.outputs.len();
    let video_input_pads = filter_graph
        .inputs
        .iter()
        .filter(|i| i.media_type == AVMEDIA_TYPE_VIDEO)
        .count();
    let video_output_pads = filter_graph
        .outputs
        .iter()
        .filter(|o| o.media_type == AVMEDIA_TYPE_VIDEO)
        .count();
    if input_pads != 1 || video_input_pads != 1 || output_pads != 1 || video_output_pads != 1 {
        return Err(WriterError::FilterShape {
            input_pads,
            video_input_pads,
            output_pads,
            video_output_pads,
        }
        .into());
    }

    let fg_sender = ifilter_bind_frame_source(&mut filter_graph, &params);

    {
        let mux = &mut muxs[0];
        // ofilter_bind_ost does not consult the disable flags (outputs_bind
        // gates on them before ever calling it), so preserve disable_video
        // here: skip the bind and let the streamless check below reject it.
        if !mux.video_disable {
            match choose_encoder(mux, AVMEDIA_TYPE_VIDEO)? {
                Some((codec_id, enc)) => {
                    // stream_source: none (no input file). single_stream_direct_input:
                    // true — the graph is fed by exactly one source, matching the
                    // one-stream simple graph (init_simple_filtergraph), so CFR
                    // handling and initial-ts preservation behave identically.
                    ofilter_bind_ost(0, mux, &mut filter_graph, 0, codec_id, enc, None, true)?;
                }
                None => {
                    // set_video_codec("copy"): there is no packet stream to copy
                    // from a pushed frame. Same conflict map_manual reports.
                    error!(target: LOG_TARGET,
                        "VideoWriter output requested streamcopy (video codec \
                         'copy'), but pushed frames must be encoded"
                    );
                    return Err(OpenOutputError::InvalidArgument.into());
                }
            }
        }

        // Keep outputs_bind's tail: attachment streams, then metadata. With no
        // demuxers the metadata pass degenerates to the user-specified values.
        unsafe {
            crate::core::context::attachment::create_attachment_streams(mux)?;
            process_metadata(mux, &Vec::new())?;
        }
    }

    // NoVideoDestination without a demuxer: a disable_video()/streamless
    // output never bound the graph output, so the mux got no source
    // (check_output_streams' NotContainStream case) — and an AVFMT_NOSTREAMS/
    // attachment-only output can pass that flag check while the graph output
    // still has no destination (the case A caught with destination_is_empty).
    // Either way the pushed frames have nowhere to go; without this check
    // write() would block forever against a live-but-unconsumed channel.
    if !muxs[0].has_src() || !filter_graph.outputs[0].has_dst() {
        warn!(target: LOG_TARGET, "Writer output consumes no video stream");
        return Err(OpenOutputError::NotContainStream.into());
    }

    check_frame_filter_pipeline(&muxs, &[])?;

    let (ingress_sender, ingress_receiver) = crossbeam_channel::bounded(queue_capacity);
    let frame_source = FrameSource {
        ingress: ingress_receiver,
        fg_sender,
        params,
    };

    Ok((
        FfmpegContext {
            independent_readrate: false,
            demuxs: Vec::new(),
            filter_graphs: vec![filter_graph],
            muxs,
            frame_sources: vec![frame_source],
            scheduler_status,
            interrupt_state,
        },
        ingress_sender,
    ))
}

/// Binds filtergraph input pad 0 to a headless frame source — the frame-source
/// analog of `ifilter_bind_ist` (`fg_bind.rs`), minus everything that needs a
/// demuxer. Returns the cloned filtergraph sender the source will feed.
///
/// Two parameters cannot ride on the frames themselves and must be installed
/// here:
/// - `opts.framerate`: not an `AVFrame` field; `configure_filtergraph` copies
///   it into `AVBufferSrcParameters.frame_rate`.
/// - `opts.fallback`: consulted only by the zero-frame `fg_send_eof` path to
///   configure the graph when EOF arrives before any frame; without it that
///   path fails with "Cannot determine format after EOF".
///
/// The remaining fallback fields (SAR 0/1, colorspace/color_range
/// UNSPECIFIED) keep their `av_frame_alloc` defaults, which is exactly what a
/// pushed frame built from an unref'd pool shell carries.
fn ifilter_bind_frame_source(
    filter_graph: &mut FilterGraph,
    params: &FrameSourceParams,
) -> Sender<crate::core::context::FrameBox> {
    let input_filter = &mut filter_graph.inputs[0];
    input_filter.opts.framerate = AVRational {
        num: params.fps_num,
        den: params.fps_den,
    };
    // SAFETY: `fallback` is the frame allocated for this pad by
    // init_filter_graph; only plain fields are written.
    unsafe {
        let fallback = input_filter.opts.fallback.as_mut_ptr();
        (*fallback).format = params.pix_fmt as i32;
        (*fallback).width = params.width;
        (*fallback).height = params.height;
        (*fallback).time_base = AVRational {
            num: params.fps_den,
            den: params.fps_num,
        };
    }
    // No demuxer stream may be bound on top of this pad; the scheduler-input
    // slot (SchNode::Filter.inputs[0]) stays the pre-sized None hole, which
    // the input controller treats as "no demuxer to unchoke".
    input_filter.bound = true;

    let (sender, _finished_flag_list) = filter_graph.get_src_sender();
    sender
}
