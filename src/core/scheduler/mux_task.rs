use crate::core::context::muxer::{Muxer, StreamBsfChains};
use crate::core::context::obj_pool::ObjPool;
use crate::core::context::{PacketBox, PacketData};
use crate::raw::{BitStreamFilter, FormatContext};
use crate::core::scheduler::ffmpeg_scheduler::{is_stopping, packet_is_null, set_scheduler_error, wait_until_not_paused, STATUS_ABORT, STATUS_END};
use crate::core::scheduler::input_controller::{InputController, SchNode};
use crate::error::Error::Muxing;
use crate::error::{MuxingError, MuxingOperationError, WriteHeaderError};
use crate::util::ffmpeg_utils::{av_err2str, hashmap_to_avdictionary, DictGuard};
use crate::util::thread_synchronizer::ThreadSynchronizer;
use crossbeam_channel::{Receiver, RecvTimeoutError, Sender};
use ffmpeg_next::packet::{Mut, Ref};
use ffmpeg_next::Packet;
use ffmpeg_sys_next::AVMediaType::{AVMEDIA_TYPE_AUDIO, AVMEDIA_TYPE_SUBTITLE, AVMEDIA_TYPE_VIDEO};
use ffmpeg_sys_next::{av_compare_ts, av_get_audio_frame_duration2, av_interleaved_write_frame, av_packet_move_ref, av_packet_rescale_ts, av_rescale_delta, av_rescale_q, av_write_trailer, avcodec_parameters_copy, avformat_write_header, AVFormatContext, AVPacket, AVRational, AVERROR, AVERROR_EOF, AVFMT_NOTIMESTAMPS, AVFMT_TS_NONSTRICT, AV_LOG_DEBUG, AV_LOG_WARNING, AV_NOPTS_VALUE, AV_PKT_FLAG_KEY, AV_TIME_BASE_Q, EAGAIN, ENOMEM};
use std::collections::VecDeque;
use log::{debug, error, info, trace, warn};
use std::collections::HashMap;
use std::ffi::{CStr, CString};
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::{Arc, Mutex};
use std::time::Duration;

pub(crate) fn mux_init(
    mux_idx: usize,
    mux: &mut Muxer,
    packet_pool: ObjPool<Packet>,
    input_controller: Arc<InputController>,
    mux_stream_nodes: Vec<Arc<SchNode>>,
    scheduler_status: Arc<AtomicUsize>,
    thread_sync: ThreadSynchronizer,
    scheduler_result: Arc<Mutex<Option<crate::error::Result<()>>>>,
) -> crate::error::Result<()> {
    // Take sole ownership of the output context out of the Muxer; move it by
    // value through the handoff. The move is the ownership transfer — no
    // null-the-source dance.
    let out_fmt_ctx = mux
        .out_fmt_ctx
        .take()
        .expect("mux_init called without an output context");
    mux_task_start(
        mux_idx,
        out_fmt_ctx,
        mux.take_queue(),
        mux.start_time_us,
        mux.recording_time_us,
        mux.stream_count(),
        mux.format_opts.clone(),
        mux.bsf_chains.clone(),
        mux.take_src_pre_recvs(),
        mux.mux_start_gate(),
        mux.enc_handle_receiver(),
        packet_pool,
        input_controller,
        mux_stream_nodes,
        scheduler_status,
        thread_sync,
        scheduler_result,
    )
}

pub(crate) fn ready_to_init_mux(
    mux_idx: usize,
    mux: &mut Muxer,
    packet_pool: ObjPool<Packet>,
    input_controller: Arc<InputController>,
    scheduler_status: Arc<AtomicUsize>,
    thread_sync: ThreadSynchronizer,
    scheduler_result: Arc<Mutex<Option<crate::error::Result<()>>>>,
) -> crate::error::Result<Option<crossbeam_channel::Sender<i32>>> {
    if !mux.is_ready() {
        let (sender, receiver) = crossbeam_channel::bounded(1);

        // Take sole ownership of the output context out of the Muxer. It is moved
        // into the waiter thread below; if streams never become ready the waiter
        // drops it (freeing once) — the RAII net the old box#1 provided.
        let out_fmt_ctx = mux
            .out_fmt_ctx
            .take()
            .expect("ready_to_init_mux called without an output context");
        let mux_stream_nodes = mux.mux_stream_nodes.clone();
        let queue = mux.take_queue();
        let src_pre_recvs = mux.take_src_pre_recvs();
        let mux_start_gate = mux.mux_start_gate();
        let enc_handle_receiver = mux.enc_handle_receiver();
        let start_time_us = mux.start_time_us;
        let recording_time_us = mux.recording_time_us;
        let stream_count = mux.stream_count();
        let nb_streams_ready = mux.nb_streams_ready.clone();
        let format_opts = mux.format_opts.clone();
        let bsf_chains = mux.bsf_chains.clone();

        let result = std::thread::Builder::new().name(format!("ready-to-init-muxer{mux_idx}")).spawn(move || {
            // Owns the context until streams are ready; drops (frees) if the loop
            // exits early. Handed to mux_task_start on the all-ready branch.
            let mut out_fmt_ctx = Some(out_fmt_ctx);
            loop {
                let result = receiver.recv_timeout(Duration::from_millis(100));

                if is_stopping(wait_until_not_paused(&scheduler_status)) {
                    thread_sync.thread_done_with(|| {
                        scheduler_status.store(STATUS_END, Ordering::Release);
                    });
                    info!("Init muxer receiver end command, finishing.");
                    break;
                }

                if let Err(e) = result {
                    if e == RecvTimeoutError::Disconnected {
                        // Publish the terminal state BEFORE waking waiters
                        // (a woken wait() must observe it; the async waker
                        // is consumed by the wake).
                        thread_sync.thread_done_with(|| {
                            scheduler_status.store(STATUS_END, Ordering::Release);
                        });
                        warn!(
                            "mux init aborted: encoder(s) exited before all {stream_count} streams became ready ({} ready)",
                            nb_streams_ready.load(Ordering::Acquire)
                        );
                        break;
                    }
                    continue;
                }

                let stream_index = result.unwrap();
                debug!("output_stream: {stream_index} is readied");
                let nb_streams_ready = nb_streams_ready.fetch_add(1, Ordering::Release);
                if nb_streams_ready + 1 == stream_count {
                    // Move the context out to the terminal init; a later drop of
                    // the (now `None`) local is a no-op.
                    let out_fmt_ctx = out_fmt_ctx
                        .take()
                        .expect("mux waiter reached all-ready without a context");
                    if let Err(e) = mux_task_start(
                        mux_idx,
                        out_fmt_ctx,
                        queue,
                        start_time_us,
                        recording_time_us,
                        stream_count,
                        format_opts,
                        bsf_chains,
                        src_pre_recvs,
                        mux_start_gate,
                        enc_handle_receiver,
                        packet_pool,
                        input_controller,
                        mux_stream_nodes,
                        scheduler_status,
                        thread_sync,
                        scheduler_result,
                    ) {
                        // mux_task_start already logged the root cause and
                        // recorded it via set_scheduler_error.
                        debug!("Muxer init failed: {e}");
                    }
                    break;
                }
            }
        });
        if let Err(e) = result {
            error!("Mux init thread exited with error: {e}");
            return Err(MuxingOperationError::ThreadExited.into());
        }
        Ok(Some(sender))
    } else {
        Ok(None)
    }
}

fn mux_task_start(mux_idx: usize,
                  out_fmt_ctx: FormatContext,
                  queue: Option<(Sender<PacketBox>, Receiver<PacketBox>)>,
                  start_time_us: Option<i64>,
                  recording_time_us: Option<i64>,
                  stream_count: usize,
                  format_opts: Option<HashMap<CString, CString>>,
                  bsf_chains: StreamBsfChains,
                  src_pre_receivers: Vec<Receiver<PacketBox>>,
                  mux_start_gate: Arc<crate::core::context::MuxStartGate>,
                  enc_handle_receiver: Receiver<std::thread::JoinHandle<()>>,
                  packet_pool: ObjPool<Packet>,
                  input_controller: Arc<InputController>,
                  mux_stream_nodes: Vec<Arc<SchNode>>,
                  scheduler_status: Arc<AtomicUsize>,
                  thread_sync: ThreadSynchronizer,
                  scheduler_result: Arc<Mutex<Option<crate::error::Result<()>>>>,) -> crate::error::Result<()> {

    if queue.is_none() {
        // `out_fmt_ctx` is owned here: this early return drops it, freeing the
        // context. Previously the raw pointer was leaked on this zero-stream path.
        return Ok(());
    }

    let (queue_sender, queue_receiver) = queue.unwrap();

    _mux_init(mux_idx, out_fmt_ctx, queue_receiver, start_time_us, recording_time_us, stream_count, format_opts, bsf_chains, enc_handle_receiver, packet_pool, input_controller, mux_stream_nodes, scheduler_status, thread_sync, scheduler_result)?;

    // Drain the pre-queues and open the gate atomically: an encoder that
    // saw the gate closed cannot park a packet after this drain ran.
    mux_start_gate.start_with(|| {
        // fftools 242ee7b0: flushing the queues stream-by-stream hands the
        // muxer long single-stream runs that can overflow
        // max_interleave_delta and degrade interleaving. Snapshot every
        // queue (the gate locks all senders out for the whole closure) and
        // merge across them by DTS instead.
        let mut queues: Vec<VecDeque<PacketBox>> = src_pre_receivers
            .iter()
            .map(|receiver| {
                let mut queue = VecDeque::new();
                while let Ok(packet_box) = receiver.try_recv() {
                    queue.push_back(packet_box);
                }
                queue
            })
            .collect();

        loop {
            let mut min_stream = None;
            let mut min_ts: Option<(i64, AVRational)> = None;

            // find the queue whose front packet has the earliest dts; a
            // missing timestamp or timebase wins immediately, mirroring the
            // NULL/AV_NOPTS_VALUE short-circuit upstream
            for (i, queue) in queues.iter().enumerate() {
                let Some(front) = queue.front() else { continue };
                // SAFETY: the box owns a live packet parked by an encoder.
                let (dts, tb) = unsafe {
                    let pkt = front.packet.as_ptr();
                    ((*pkt).dts, (*pkt).time_base)
                };
                if dts == AV_NOPTS_VALUE || tb.num <= 0 || tb.den <= 0 {
                    min_stream = Some(i);
                    break;
                }
                match min_ts {
                    // SAFETY: pure arithmetic on validated timebases.
                    Some((min_dts, min_tb))
                        if unsafe { av_compare_ts(min_dts, min_tb, dts, tb) } <= 0 => {}
                    _ => {
                        min_stream = Some(i);
                        min_ts = Some((dts, tb));
                    }
                }
            }

            let Some(i) = min_stream else { break };
            let packet_box = queues[i].pop_front().unwrap();
            let _ = queue_sender.send(packet_box);
        }
    });
    Ok(())
}

fn _mux_init(
    mux_idx: usize,
    out_fmt_ctx: FormatContext,
    pkt_receiver: Receiver<PacketBox>,
    start_time_us: Option<i64>,
    recording_time_us: Option<i64>,
    stream_count: usize,
    format_opts: Option<HashMap<CString, CString>>,
    bsf_chains: StreamBsfChains,
    enc_handle_receiver: Receiver<std::thread::JoinHandle<()>>,
    packet_pool: ObjPool<Packet>,
    input_controller: Arc<InputController>,
    mux_stream_nodes: Vec<Arc<SchNode>>,
    scheduler_status: Arc<AtomicUsize>,
    thread_sync: ThreadSynchronizer,
    scheduler_result: Arc<Mutex<Option<crate::error::Result<()>>>>,
) -> crate::error::Result<()> {
    // `out_fmt_ctx` (a FormatContext) is the sole owner here; it Drops — freeing
    // the context — on the write_header error return below, and is moved into the
    // muxer worker thread on success. `as_ptr()` borrows it for the FFI calls.
    // SAFETY: as_ptr yields the live output context pointer.
    let out_fmt_ctx_ptr = unsafe { out_fmt_ctx.as_ptr() };

    // Guard owns the dict on every path: write_header leaves unrecognized
    // entries behind, which leaked (and were silently swallowed) before.
    let mut opts = DictGuard::new(hashmap_to_avdictionary(&format_opts));

    // Initialize bitstream filters BEFORE writing the header. BSFs such as
    // h264_mp4toannexb / *_metadata rewrite codecpar/extradata inside
    // av_bsf_init, and those changes must reach the muxer header — FFmpeg runs
    // bsf_init in of_stream_init, before mux_check_init's avformat_write_header
    // (ffmpeg_mux.c). This runs in the mux worker, AFTER every stream is ready
    // (codecpar populated by streamcopy_init/enc_open) and BEFORE the pre-mux
    // queue is drained, so no packet is filtered before the header exists.
    // When no output sets a BSF, `stream_bsfs` is all-None and the packet path
    // below is byte-for-byte the pre-BSF path.
    let stream_bsfs = match unsafe {
        init_bitstream_filters(out_fmt_ctx_ptr, &bsf_chains, stream_count)
    } {
        Ok(bsfs) => bsfs,
        Err((name, bsf_ret)) => {
            error!(
                "Could not initialize bitstream filter chain '{name}': {}",
                av_err2str(bsf_ret)
            );
            // Same terminal ordering as the write_header failure below: publish
            // the error before releasing this muxer's thread slot.
            set_scheduler_error(
                &scheduler_status,
                &scheduler_result,
                Muxing(MuxingOperationError::BitstreamFilterInit(
                    name.clone(),
                    MuxingError::from(bsf_ret),
                )),
            );
            thread_sync.thread_done_with(|| {
                scheduler_status.store(STATUS_END, Ordering::Release);
            });
            // `out_fmt_ctx` drops here (frees the context); any partially built
            // `BitStreamFilter`s were already dropped inside the helper.
            return Err(Muxing(MuxingOperationError::BitstreamFilterInit(
                name,
                MuxingError::from(bsf_ret),
            )));
        }
    };

    let ret = unsafe { avformat_write_header(out_fmt_ctx_ptr, opts.as_double_ptr()) };
    if ret < 0 {
        error!("Could not write header (incorrect codec parameters ?): {}", av_err2str(ret));
        // Record the failure BEFORE releasing this muxer's thread slot, so
        // wait() can never observe "all threads done" without the error
        // (set_scheduler_error also publishes STATUS_END, which unwinds the
        // encoders still feeding the pre-mux channels).
        set_scheduler_error(
            &scheduler_status,
            &scheduler_result,
            Muxing(MuxingOperationError::WriteHeader(WriteHeaderError::from(ret))),
        );
        thread_sync.thread_done_with(|| {
            scheduler_status.store(STATUS_END, Ordering::Release);
        });
        // `out_fmt_ctx` drops here, freeing the context.
        return Err(Muxing(MuxingOperationError::WriteHeader(
            WriteHeaderError::from(ret),
        )));
    }

    for key in opts.leftover_keys() {
        warn!("Option '{key}' was not recognized by output {mux_idx}");
    }

    let oformat_flags = unsafe {
        let oformat = (*out_fmt_ctx_ptr).oformat;
        (*oformat).flags
    };

    let format_name = unsafe { CStr::from_ptr((*(*out_fmt_ctx_ptr).oformat).name).to_str().unwrap_or("unknown") };

    let result = std::thread::Builder::new().name(format!("muxer{mux_idx}:{format_name}")).spawn(move || {
        // Move the FormatContext into the worker; it Drops (frees the output
        // context, custom-IO-aware) when this closure ends — the terminal free.
        let out_fmt_ctx = out_fmt_ctx;
        // Per-output-stream BSF chains (None for streams without one). Owned by
        // the worker; each `BitStreamFilter` frees its AVBSFContext/AVPacket on
        // drop when the closure ends.
        let mut stream_bsfs = stream_bsfs;
        // Last packet_data seen per stream, used as the metadata template for
        // BSF flush packets at EOF (they carry no PacketData of their own).
        let mut stream_pkt_templates: Vec<Option<PacketData>> =
            (0..stream_count).map(|_| None).collect();
        let mut stream_started: Vec<bool> = vec![false; stream_count];
        let mut stream_eof: Vec<bool> = vec![false; stream_count];
        let mut st_rescale_delta_last_map = HashMap::new();
        let mut st_last_dts_map = HashMap::new();

        let mut nb_done = 0;

        let mut ret = 0;

        loop {
            let result = pkt_receiver.recv_timeout(Duration::from_millis(100));

            if is_stopping(wait_until_not_paused(&scheduler_status)) {
                info!("Muxer receiver end command, finishing.");
                break;
            }

            if let Err(e) = result {
                if e == RecvTimeoutError::Disconnected {
                    debug!("Encoder thread exit.");
                    break;
                }
                continue;
            }

            let mut packet_box = result.unwrap();
            let pkt = packet_box.packet.as_ptr();
            let packet_data = &packet_box.packet_data;

            // Handle demux EOF signal: stream_index < 0 means a specific stream reached
            // recording_time on the demux side. Use packet_data.output_stream_index
            // to identify which stream finished.
            // Note: differs from CLI where stream_idx < 0 means ALL streams finished
            // (ffmpeg_mux.c:428-431). ez-ffmpeg uses per-stream EOF signaling instead.
            let raw_stream_index = unsafe { (*pkt).stream_index };
            if raw_stream_index < 0 {
                let eof_stream = packet_box.packet_data.output_stream_index;
                if eof_stream >= 0 {
                    let eof_idx = eof_stream as usize;
                    if eof_idx < stream_count && !stream_eof[eof_idx] {
                        // Flush trailing BSF packets before finishing this
                        // stream (no-op when it has no BSF).
                        let fret = unsafe {
                            flush_stream_bsf(
                                &mut st_rescale_delta_last_map,
                                oformat_flags,
                                &mut st_last_dts_map,
                                &out_fmt_ctx,
                                &mut stream_bsfs,
                                &stream_pkt_templates,
                                eof_idx,
                                &packet_pool,
                            )
                        };
                        if fret < 0 {
                            ret = fret;
                            error!("Error flushing bitstream filter at EOF: stream={eof_idx}, ret={fret}");
                            packet_pool.release(packet_box.packet);
                            break;
                        }
                        stream_eof[eof_idx] = true;
                        nb_done += 1;
                        if eof_idx < mux_stream_nodes.len() {
                            let node = mux_stream_nodes[eof_idx].as_ref();
                            let SchNode::MuxStream { src: _, last_dts: _, source_finished } = node else { unreachable!() };
                            source_finished.store(true, Ordering::Release);
                            input_controller.update_locked(&scheduler_status);
                        }
                    }
                }
                packet_pool.release(packet_box.packet);
                if nb_done == stream_count {
                    trace!("All streams finished (demux EOF signal)");
                    break;
                }
                continue;
            }

            let stream_index = raw_stream_index as usize;
            if stream_index >= mux_stream_nodes.len() {
                error!("Invalid stream_index: {} >= {}", stream_index, mux_stream_nodes.len());
                packet_pool.release(packet_box.packet);
                continue;
            }
            let mux_stream_node = &mux_stream_nodes[stream_index];
            unsafe {
                let has_side_data = (*packet_box.packet.as_ptr()).side_data_elems > 0;
                if packet_is_null(&packet_box.packet) || (packet_box.packet.is_empty() && !has_side_data) {
                    let current_status = scheduler_status.load(Ordering::Acquire);
                    if current_status == STATUS_ABORT {
                        debug!("Muxer detected abort from stream {}, exiting without trailer", stream_index);
                        packet_pool.release(packet_box.packet);
                        break;
                    }

                    // Guard: skip if this stream already finished via recording_time EOF
                    if stream_eof[stream_index] {
                        packet_pool.release(packet_box.packet);
                        continue;
                    }

                    // Flush trailing BSF packets before finishing this stream
                    // (no-op when it has no BSF). Already inside `unsafe`.
                    let fret = flush_stream_bsf(
                        &mut st_rescale_delta_last_map,
                        oformat_flags,
                        &mut st_last_dts_map,
                        &out_fmt_ctx,
                        &mut stream_bsfs,
                        &stream_pkt_templates,
                        stream_index,
                        &packet_pool,
                    );
                    if fret < 0 {
                        ret = fret;
                        error!("Error flushing bitstream filter at EOF: stream={stream_index}, ret={fret}");
                        packet_pool.release(packet_box.packet);
                        break;
                    }

                    nb_done += 1;
                    packet_pool.release(packet_box.packet);

                    let mux_stream_node = mux_stream_node.as_ref();
                    let SchNode::MuxStream { src: _, last_dts: _, source_finished } = mux_stream_node else { unreachable!() };
                    source_finished.store(true, Ordering::Release);
                    input_controller.update_locked(&scheduler_status);

                    if nb_done == stream_count {
                        trace!("All streams finished");
                        break;
                    } else {
                        continue;
                    }
                }

                update_last_dts(mux_stream_node, &input_controller, &scheduler_status, pkt);

                // Skip packets for streams that already hit recording_time EOF
                if stream_eof[stream_index] {
                    packet_pool.release(packet_box.packet);
                    continue;
                }

                if !packet_is_null(&packet_box.packet) && packet_data.is_copy {
                    let started = &mut stream_started[stream_index];
                    ret = streamcopy_rescale(
                        packet_box.packet.as_mut_ptr(),
                        packet_data,
                        &start_time_us,
                        &recording_time_us,
                        started,
                    );
                    if ret == AVERROR(EAGAIN) {
                        continue;
                    } else if ret == AVERROR_EOF {
                        // Per-stream EOF: mark this stream as finished, matching CLI's
                        // sch_mux_receive_finish behavior in ffmpeg_mux.c:442.
                        // Flush trailing BSF packets first (no-op without a BSF).
                        let fret = flush_stream_bsf(
                            &mut st_rescale_delta_last_map,
                            oformat_flags,
                            &mut st_last_dts_map,
                            &out_fmt_ctx,
                            &mut stream_bsfs,
                            &stream_pkt_templates,
                            stream_index,
                            &packet_pool,
                        );
                        if fret < 0 {
                            ret = fret;
                            error!("Error flushing bitstream filter at EOF: stream={stream_index}, ret={fret}");
                            packet_pool.release(packet_box.packet);
                            break;
                        }
                        stream_eof[stream_index] = true;
                        packet_pool.release(packet_box.packet);

                        nb_done += 1;
                        let mux_stream_node = mux_stream_node.as_ref();
                        let SchNode::MuxStream { src: _, last_dts: _, source_finished } = mux_stream_node else { unreachable!() };
                        source_finished.store(true, Ordering::Release);
                        input_controller.update_locked(&scheduler_status);

                        if nb_done == stream_count {
                            trace!("All streams finished (recording_time)");
                            break;
                        }
                        continue;
                    }
                }

                // write (bitstream-filter aware). With no BSF for this stream,
                // `mux_filter_and_write_packet` calls exactly the same
                // `write_packet` as before — zero behavior change.
                if !packet_is_null(&packet_box.packet)
                    && (*packet_box.packet.as_ptr()).stream_index >= 0
                {
                    // Snapshot this stream's packet metadata so a later EOF
                    // flush can stamp the BSF's trailing packets correctly.
                    if stream_bsfs[stream_index].is_some() {
                        stream_pkt_templates[stream_index] =
                            Some(packet_box.packet_data.clone());
                    }
                    ret = mux_filter_and_write_packet(
                        &mut st_rescale_delta_last_map,
                        oformat_flags,
                        &mut st_last_dts_map,
                        &out_fmt_ctx,
                        &mut packet_box,
                        stream_bsfs[stream_index].as_mut(),
                        &packet_pool,
                    );
                    packet_pool.release(packet_box.packet);

                    if ret == AVERROR_EOF {
                        trace!("Muxer returned EOF");
                        break;
                    } else if ret < 0 {
                        error!("Error muxing a packet: stream_index={stream_index}, ret={ret}");
                        break;
                    }
                }
            }
        }

        if ret < 0 && ret != AVERROR_EOF {
            set_scheduler_error(
                &scheduler_status,
                &scheduler_result,
                Muxing(MuxingOperationError::InterleavedWriteError(
                    MuxingError::from(ret),
                )),
            );
        }

        // write_trailer
        let final_status = scheduler_status.load(Ordering::Acquire);
        if final_status != STATUS_ABORT {
            unsafe {
                let ret = av_write_trailer(out_fmt_ctx.as_ptr());
                if ret < 0 {
                    error!("Error writing trailer: {}", av_err2str(ret));
                    set_scheduler_error(
                        &scheduler_status,
                        &scheduler_result,
                        Muxing(MuxingOperationError::TrailerWriteError(MuxingError::from(
                            ret,
                        ))),
                    );
                }
            }
        } else {
            debug!("Muxer skipping trailer due to abort");
        }

        debug!("Muxer finished.");

        // Unblock any encoder still sending into the packet queue, then join
        // this muxer's encoders BEFORE the FormatContext drop frees the
        // output streams they write into (FFmpeg joins encoder tasks before
        // muxer cleanup in sch_stop, ffmpeg_sched.c:2535-2604).
        drop(pkt_receiver);
        while let Ok(handle) = enc_handle_receiver.try_recv() {
            let _ = handle.join();
        }

        // Publish the terminal state BEFORE waking waiters (a woken wait()
        // must observe it; the async waker is consumed by the wake).
        thread_sync.thread_done_with(|| {
            scheduler_status.store(STATUS_END, Ordering::Release);
        });
    });
    if let Err(e) = result {
        error!("Muxer thread exited with error: {e}");
        return Err(MuxingOperationError::ThreadExited.into())
    }

    Ok(())
}

unsafe fn update_last_dts(mux_stream_node: &Arc<SchNode>, input_controller: &Arc<InputController>, scheduler_status: &Arc<AtomicUsize>, pkt: *const AVPacket) {
    if (*pkt).dts != AV_NOPTS_VALUE {
        let dts = av_rescale_q((*pkt).dts + (*pkt).duration, (*pkt).time_base, AV_TIME_BASE_Q);
        let node = mux_stream_node.as_ref();
        let SchNode::MuxStream { src: _, last_dts, source_finished: _ } = node else { unreachable!() };
        last_dts.store(dts, Ordering::Release);
        input_controller.update_locked(scheduler_status);
    }
}

unsafe fn streamcopy_rescale(
    pkt: *mut AVPacket,
    packet_data: &PacketData,
    start_time_us: &Option<i64>,
    recording_time_us: &Option<i64>,
    started: &mut bool,
) -> i32 {
    if !packet_data.is_copy {
        return 0;
    }
    let dts = packet_data.dts_est;

    let start_time = start_time_us.unwrap_or(0);

    // recording_time
    if let Some(recording_time_us) = recording_time_us {
        if dts >= recording_time_us + start_time {
            return AVERROR_EOF;
        }
    }

    if !*started && (*pkt).flags & AV_PKT_FLAG_KEY == 0 {
        return AVERROR(EAGAIN);
    }

    // Match FFmpeg CLI: only filter packets before start_time when output start_time is set.
    // CLI default: copy_prior_start=-1 → !(-1)=false → check skipped;
    //              of->start_time=AV_NOPTS_VALUE → check skipped.
    // Without this guard, packets with negative timestamps (between seek keyframe
    // and exact seek point) are incorrectly dropped, causing start_pts mismatch.
    if !*started && start_time_us.is_some() {
        let no_pts = (*pkt).pts == AV_NOPTS_VALUE;
        let not_start = if no_pts {
            dts < start_time
        } else {
            (*pkt).pts < av_rescale_q(start_time, AV_TIME_BASE_Q, (*pkt).time_base)
        };
        if not_start {
            return AVERROR(EAGAIN);
        }
    }

    let ts_offset = av_rescale_q(start_time, AV_TIME_BASE_Q, (*pkt).time_base);

    if (*pkt).pts != AV_NOPTS_VALUE {
        (*pkt).pts -= ts_offset;
    }

    if (*pkt).dts == AV_NOPTS_VALUE {
        (*pkt).dts = av_rescale_q(dts, AV_TIME_BASE_Q, (*pkt).time_base);
    } else if packet_data.codec_type == AVMEDIA_TYPE_AUDIO {
        (*pkt).pts = (*pkt).dts - ts_offset;
    }

    (*pkt).dts -= ts_offset;

    *started = true;
    0
}

unsafe fn write_packet(
    st_rescale_delta_last_map: &mut HashMap<i32, i64>,
    oformat_flags: i32,
    st_last_dts_map: &mut HashMap<i32, i64>,
    out_fmt_ctx: &FormatContext,
    sq_packet_box: &mut PacketBox,
) -> i32 {
    mux_fixup_ts(
        st_rescale_delta_last_map,
        oformat_flags,
        st_last_dts_map,
        sq_packet_box,
        out_fmt_ctx.as_ptr(),
    );

    (*sq_packet_box.packet.as_mut_ptr()).stream_index =
        sq_packet_box.packet_data.output_stream_index;

    av_interleaved_write_frame(out_fmt_ctx.as_ptr(), sq_packet_box.packet.as_mut_ptr())
}

/// Build the per-output-stream BSF list, resolving each stream's chain by its
/// media type (`-bsf:v/-bsf:a/-bsf:s`). Runs BEFORE `avformat_write_header`, so
/// filters that rewrite codecpar/extradata in `av_bsf_init` (h264_mp4toannexb,
/// `*_metadata`) reach the muxer header. Mirrors fftools `bsf_init`
/// (ffmpeg_mux.c): copy the stream's codecpar into `par_in`, seed
/// `time_base_in` from the stream time_base, init, then copy `par_out` back and
/// adopt `time_base_out` (rescaling any preset duration).
///
/// Returns one entry per stream (`None` where the stream has no BSF). On any
/// failure returns `Err((chain_name, averror))`; every `BitStreamFilter`
/// allocated so far is dropped (freed) as the local vec unwinds.
unsafe fn init_bitstream_filters(
    out_fmt_ctx: *mut AVFormatContext,
    bsf_chains: &StreamBsfChains,
    stream_count: usize,
) -> Result<Vec<Option<BitStreamFilter>>, (String, i32)> {
    let mut stream_bsfs: Vec<Option<BitStreamFilter>> =
        (0..stream_count).map(|_| None).collect();

    // No output requested a BSF: leave every slot None so the packet path stays
    // byte-for-byte the pre-BSF path.
    if bsf_chains.is_empty() {
        return Ok(stream_bsfs);
    }

    for i in 0..stream_count {
        let st = *(*out_fmt_ctx).streams.add(i);
        let codec_type = (*(*st).codecpar).codec_type;
        let Some(chain) = bsf_chains.for_media_type(codec_type) else {
            continue;
        };
        let name = || chain.to_string_lossy().into_owned();

        let mut bsf = BitStreamFilter::parse(chain.as_c_str()).map_err(|ret| (name(), ret))?;
        let ctx = bsf.as_ptr();

        let ret = avcodec_parameters_copy((*ctx).par_in, (*st).codecpar);
        if ret < 0 {
            return Err((name(), ret));
        }
        (*ctx).time_base_in = (*st).time_base;

        let ret = bsf.init();
        if ret < 0 {
            return Err((name(), ret));
        }

        let ret = avcodec_parameters_copy((*st).codecpar, (*ctx).par_out);
        if ret < 0 {
            return Err((name(), ret));
        }

        // Adopt the filter's output timebase and rescale any duration that was
        // set against the old one (fftools of_stream_init, ffmpeg_mux.c).
        let old_tb = (*st).time_base;
        let old_duration = (*st).duration;
        (*st).time_base = bsf.time_base_out();
        if old_duration != AV_NOPTS_VALUE && old_tb.num > 0 && old_tb.den > 0 {
            (*st).duration = av_rescale_q(old_duration, old_tb, (*st).time_base);
        }

        stream_bsfs[i] = Some(bsf);
    }

    Ok(stream_bsfs)
}

/// Write one packet, applying this stream's bitstream filter first when
/// present. With `bsf = None` this is exactly the pre-BSF `write_packet` call —
/// the no-BSF path is unchanged. Mirrors fftools `mux_packet_filter`
/// (ffmpeg_mux.c): rescale into the BSF input timebase, send, then drain every
/// output packet to the muxer.
unsafe fn mux_filter_and_write_packet(
    st_rescale_delta_last_map: &mut HashMap<i32, i64>,
    oformat_flags: i32,
    st_last_dts_map: &mut HashMap<i32, i64>,
    out_fmt_ctx: &FormatContext,
    packet_box: &mut PacketBox,
    bsf: Option<&mut BitStreamFilter>,
    packet_pool: &ObjPool<Packet>,
) -> i32 {
    let Some(bsf) = bsf else {
        return write_packet(
            st_rescale_delta_last_map,
            oformat_flags,
            st_last_dts_map,
            out_fmt_ctx,
            packet_box,
        );
    };

    // Empty packets carry no bitstream to filter (in ez they are stream-end
    // markers or side-data-only). Feeding one to av_bsf_send_packet would be
    // read as EOF and prematurely close the filter, so write it directly.
    if packet_box.packet.is_empty() {
        return write_packet(
            st_rescale_delta_last_map,
            oformat_flags,
            st_last_dts_map,
            out_fmt_ctx,
            packet_box,
        );
    }

    let pkt = packet_box.packet.as_mut_ptr();
    // Rescale into the filter's input timebase (fftools ffmpeg_mux.c).
    av_packet_rescale_ts(pkt, (*pkt).time_base, bsf.time_base_in());

    let ret = bsf.send_packet(pkt);
    if ret < 0 {
        return ret;
    }
    // send_packet took ownership of pkt's contents (reset to empty); the caller
    // still releases the now-empty shell to the pool.

    drain_bsf_write(
        st_rescale_delta_last_map,
        oformat_flags,
        st_last_dts_map,
        out_fmt_ctx,
        bsf,
        &packet_box.packet_data,
        packet_pool,
    )
}

/// Drain all currently available output packets from `bsf` into the muxer. Each
/// received packet is moved into a pooled `Packet`, stamped with the filter's
/// output timebase, tagged with `template` metadata, and written via
/// `write_packet`. Returns `0` on `EAGAIN` (input exhausted), `AVERROR_EOF`
/// after a NULL-flush completes, or a negative error.
unsafe fn drain_bsf_write(
    st_rescale_delta_last_map: &mut HashMap<i32, i64>,
    oformat_flags: i32,
    st_last_dts_map: &mut HashMap<i32, i64>,
    out_fmt_ctx: &FormatContext,
    bsf: &mut BitStreamFilter,
    template: &PacketData,
    packet_pool: &ObjPool<Packet>,
) -> i32 {
    loop {
        let ret = bsf.receive_packet();
        if ret == AVERROR(EAGAIN) {
            return 0;
        } else if ret == AVERROR_EOF {
            return AVERROR_EOF;
        } else if ret < 0 {
            return ret;
        }

        // Move the filtered packet into a pooled shell so it flows through the
        // existing write_packet/mux_fixup_ts path; the BSF's own packet is left
        // clean for the next receive.
        let mut out_pkt = match packet_pool.get() {
            Ok(p) => p,
            Err(_) => return AVERROR(ENOMEM),
        };
        av_packet_move_ref(out_pkt.as_mut_ptr(), bsf.pkt_ptr());
        (*out_pkt.as_mut_ptr()).time_base = bsf.time_base_out();

        let mut out_box = PacketBox {
            packet: out_pkt,
            packet_data: template.clone(),
        };
        let wret = write_packet(
            st_rescale_delta_last_map,
            oformat_flags,
            st_last_dts_map,
            out_fmt_ctx,
            &mut out_box,
        );
        packet_pool.release(out_box.packet);
        if wret < 0 {
            return wret;
        }
    }
}

/// Flush a stream's bitstream filter at EOF: send NULL and drain trailing
/// packets before the stream is marked finished. No-op (returns `0`) when the
/// stream has no BSF. Returns `0` on success (including the drain's terminal
/// EOF) or a negative muxing error.
unsafe fn flush_stream_bsf(
    st_rescale_delta_last_map: &mut HashMap<i32, i64>,
    oformat_flags: i32,
    st_last_dts_map: &mut HashMap<i32, i64>,
    out_fmt_ctx: &FormatContext,
    stream_bsfs: &mut [Option<BitStreamFilter>],
    stream_pkt_templates: &[Option<PacketData>],
    stream_index: usize,
    packet_pool: &ObjPool<Packet>,
) -> i32 {
    let Some(bsf) = stream_bsfs[stream_index].as_mut() else {
        return 0;
    };

    // Metadata for the trailing packets: reuse the last real packet's template,
    // or synthesize one from the output stream's codecpar if none was seen.
    let template = match &stream_pkt_templates[stream_index] {
        Some(t) => t.clone(),
        None => {
            let st = *(*out_fmt_ctx.as_ptr()).streams.add(stream_index);
            PacketData {
                dts_est: 0,
                codec_type: (*(*st).codecpar).codec_type,
                output_stream_index: stream_index as i32,
                is_copy: false,
            }
        }
    };

    let ret = bsf.send_packet(std::ptr::null_mut());
    if ret < 0 {
        return ret;
    }
    let dret = drain_bsf_write(
        st_rescale_delta_last_map,
        oformat_flags,
        st_last_dts_map,
        out_fmt_ctx,
        bsf,
        &template,
        packet_pool,
    );
    // A completed flush ends with AVERROR_EOF from the drain; that is success.
    if dret == AVERROR_EOF {
        0
    } else {
        dret
    }
}

unsafe fn mux_fixup_ts(
    st_rescale_delta_last_map: &mut HashMap<i32, i64>,
    oformat_flags: i32,
    st_last_dts_map: &mut HashMap<i32, i64>,
    packet_box: &mut PacketBox,
    out_fmt_ctx: *mut AVFormatContext,
) {
    let pkt = packet_box.packet.as_mut_ptr();
    let packet_data = &packet_box.packet_data;
    let stream_index = packet_data.output_stream_index;

    if packet_data.codec_type == AVMEDIA_TYPE_AUDIO && packet_data.is_copy {
        // Read the muxer's own output stream parameters (ost->st->codecpar
        // in ffmpeg_mux.c): the packet must not carry a pointer into another
        // thread's context.
        let codecpar = (**(*out_fmt_ctx).streams.add(stream_index as usize)).codecpar;
        let mut duration = av_get_audio_frame_duration2(codecpar, (*pkt).size);
        if duration == 0 {
            duration = (*codecpar).frame_size;
        }

        st_rescale_delta_last_map.entry(stream_index).or_insert(0);
        let ts_rescale_delta_last = st_rescale_delta_last_map.get_mut(&stream_index).unwrap();

        (*pkt).dts = av_rescale_delta(
            (*pkt).time_base,
            (*pkt).dts,
            AVRational {
                num: 1,
                den: (*codecpar).sample_rate,
            },
            duration,
            ts_rescale_delta_last,
            (**(*out_fmt_ctx).streams.add(stream_index as usize)).time_base,
        );
        (*pkt).pts = (*pkt).dts;

        (*pkt).duration = av_rescale_q(
            (*pkt).duration,
            (*pkt).time_base,
            (**(*out_fmt_ctx).streams.add(stream_index as usize)).time_base,
        );
    } else {
        av_packet_rescale_ts(
            pkt,
            (*pkt).time_base,
            (**(*out_fmt_ctx).streams.add(stream_index as usize)).time_base,
        );
    }
    (*pkt).time_base = (**(*out_fmt_ctx).streams.add(stream_index as usize)).time_base;

    st_last_dts_map.entry(stream_index).or_insert(AV_NOPTS_VALUE);
    let last_mux_dts = st_last_dts_map.get_mut(&stream_index).unwrap();

    if (oformat_flags & AVFMT_NOTIMESTAMPS) == 0 {
        if (*pkt).dts != AV_NOPTS_VALUE && (*pkt).pts != AV_NOPTS_VALUE && (*pkt).dts > (*pkt).pts {
            warn!(
                "Invalid DTS: {} PTS: {}, replacing by guess",
                (*pkt).dts,
                (*pkt).pts
            );
            (*pkt).pts = (*pkt).pts + (*pkt).dts + *last_mux_dts + 1
                - min3((*pkt).pts, (*pkt).dts, *last_mux_dts + 1)
                - max3((*pkt).pts, (*pkt).dts, *last_mux_dts + 1);
            (*pkt).dts = (*pkt).pts;
        }

        if (packet_data.codec_type == AVMEDIA_TYPE_AUDIO
            || packet_data.codec_type == AVMEDIA_TYPE_VIDEO
            || packet_data.codec_type == AVMEDIA_TYPE_SUBTITLE)
            && (*pkt).dts != AV_NOPTS_VALUE
            && *last_mux_dts != AV_NOPTS_VALUE
        {
            let max = *last_mux_dts + ((oformat_flags & AVFMT_TS_NONSTRICT) == 0) as i64;
            if (*pkt).dts < max {
                let loglevel = if max - (*pkt).dts > 2 || packet_data.codec_type == AVMEDIA_TYPE_VIDEO {
                    AV_LOG_WARNING
                } else {
                    AV_LOG_DEBUG
                };
                if loglevel == AV_LOG_WARNING {
                    warn!(
                        "Non-monotonic DTS; previous: {}, current: {}; ",
                        *last_mux_dts,
                        (*pkt).dts
                    );
                    warn!(
                        "changing to {}. This may result in incorrect timestamps in the output file.",
                        max
                    );
                } else {
                    debug!(
                        "Non-monotonic DTS; previous: {}, current: {}; ",
                        *last_mux_dts,
                        (*pkt).dts
                    );
                    debug!(
                        "changing to {}. This may result in incorrect timestamps in the output file.",
                        max
                    );
                }

                if (*pkt).pts >= (*pkt).dts {
                    (*pkt).pts = std::cmp::max((*pkt).pts, max);
                }
                (*pkt).dts = max;
            }
        }
    }
    *last_mux_dts = (*pkt).dts;
}



fn min3(a: i64, b: i64, c: i64) -> i64 {
    std::cmp::min(a, std::cmp::min(b, c))
}

fn max3(a: i64, b: i64, c: i64) -> i64 {
    std::cmp::max(a, std::cmp::max(b, c))
}
