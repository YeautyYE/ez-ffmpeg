use crate::core::context::muxer::Muxer;
use crate::core::context::obj_pool::ObjPool;
use crate::core::context::pre_mux_queue::PreMuxQueueReceiver;
use crate::core::context::{PacketBox, PacketData};
use crate::raw::FormatContext;
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
use ffmpeg_sys_next::{av_compare_ts, av_get_audio_frame_duration2, av_interleaved_write_frame, av_packet_rescale_ts, av_rescale_delta, av_rescale_q, av_write_trailer, avformat_write_header, AVFormatContext, AVPacket, AVRational, AVERROR, AVERROR_EOF, AVFMT_NOTIMESTAMPS, AVFMT_TS_NONSTRICT, AV_LOG_DEBUG, AV_LOG_WARNING, AV_NOPTS_VALUE, AV_PKT_FLAG_KEY, AV_TIME_BASE_Q, EAGAIN};
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
                  src_pre_receivers: Vec<PreMuxQueueReceiver>,
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

    _mux_init(mux_idx, out_fmt_ctx, queue_receiver, start_time_us, recording_time_us, stream_count, format_opts, enc_handle_receiver, packet_pool, input_controller, mux_stream_nodes, scheduler_status, thread_sync, scheduler_result)?;

    // Drain the pre-queues and open the gate atomically: an encoder that
    // saw the gate closed cannot park a packet after this drain ran.
    mux_start_gate.start_with(|| {
        // fftools 242ee7b0: flushing the queues stream-by-stream hands the
        // muxer long single-stream runs that can overflow
        // max_interleave_delta and degrade interleaving. Snapshot every
        // queue (the gate locks all senders out for the whole closure) and
        // merge across them by DTS instead. drain_all also wakes senders
        // parked on a full queue; they divert to the live queue once the
        // gate flips.
        let mut queues: Vec<VecDeque<PacketBox>> = src_pre_receivers
            .iter()
            .map(|receiver| receiver.drain_all())
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

    let ret = unsafe { avformat_write_header(out_fmt_ctx_ptr, opts.as_double_ptr()) };
    if ret < 0 {
        error!("Could not write header (incorrect codec parameters ?): {}", av_err2str(ret));
        fail_mux_init(
            &scheduler_status,
            &scheduler_result,
            &thread_sync,
            Muxing(MuxingOperationError::WriteHeader(WriteHeaderError::from(ret))),
        );
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

    // Handles for the spawn-failure branch below: the originals move into the
    // worker closure, and a failed spawn drops that closure without ever
    // running it.
    let scheduler_status_spawn = scheduler_status.clone();
    let thread_sync_spawn = thread_sync.clone();
    let scheduler_result_spawn = scheduler_result.clone();

    let result = std::thread::Builder::new().name(format!("muxer{mux_idx}:{format_name}")).spawn(move || {
        // Move the FormatContext into the worker; it Drops (frees the output
        // context, custom-IO-aware) when this closure ends — the terminal free.
        let out_fmt_ctx = out_fmt_ctx;
        let mut stream_started: Vec<bool> = vec![false; stream_count];
        let mut stream_eof: Vec<bool> = vec![false; stream_count];
        // Per-stream timestamp state, indexed by output_stream_index (always a
        // valid mux stream index in [0, stream_count), same invariant the code
        // relies on to index out_fmt_ctx.streams). Flat Vecs instead of HashMaps
        // to drop the per-packet hash lookup on the mux hot path (alloc-06).
        let mut st_rescale_delta_last: Vec<i64> = vec![0; stream_count];
        let mut st_last_dts: Vec<i64> = vec![AV_NOPTS_VALUE; stream_count];

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
                        // The packet was filtered out (before start_time, or a
                        // pre-keyframe streamcopy packet) and is not written;
                        // recycle its pooled shell instead of dropping it, like
                        // the EOF and write paths below (NEW-DP-04).
                        packet_pool.release(packet_box.packet);
                        continue;
                    } else if ret == AVERROR_EOF {
                        // Per-stream EOF: mark this stream as finished, matching CLI's
                        // sch_mux_receive_finish behavior in ffmpeg_mux.c:442
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

                // write
                if !packet_is_null(&packet_box.packet)
                    && (*packet_box.packet.as_ptr()).stream_index >= 0
                {
                    ret = write_packet(
                        &mut st_rescale_delta_last,
                        oformat_flags,
                        &mut st_last_dts,
                        &out_fmt_ctx,
                        &mut packet_box,
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
        // The mux slot was pre-counted at scheduler start and the worker that
        // would release it never ran: record the error and release the slot
        // here, or wait()/stop() hangs forever on the leaked slot. The caller
        // (the ready-to-init thread) only logs this error — it must not
        // release again.
        fail_mux_init(
            &scheduler_status_spawn,
            &scheduler_result_spawn,
            &thread_sync_spawn,
            Muxing(MuxingOperationError::ThreadExited),
        );
        return Err(MuxingOperationError::ThreadExited.into())
    }

    Ok(())
}

/// Records a mux-init failure and releases this muxer's pre-counted thread
/// slot, in that order: the error must be visible before wait() can observe
/// "all threads done" (set_scheduler_error also publishes STATUS_END, which
/// unwinds the encoders still feeding the pre-mux queues).
fn fail_mux_init(
    scheduler_status: &Arc<AtomicUsize>,
    scheduler_result: &Arc<Mutex<Option<crate::error::Result<()>>>>,
    thread_sync: &ThreadSynchronizer,
    error: crate::error::Error,
) {
    set_scheduler_error(scheduler_status, scheduler_result, error);
    thread_sync.thread_done_with(|| {
        scheduler_status.store(STATUS_END, Ordering::Release);
    });
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
    st_rescale_delta_last: &mut [i64],
    oformat_flags: i32,
    st_last_dts: &mut [i64],
    out_fmt_ctx: &FormatContext,
    sq_packet_box: &mut PacketBox,
) -> i32 {
    mux_fixup_ts(
        st_rescale_delta_last,
        oformat_flags,
        st_last_dts,
        sq_packet_box,
        out_fmt_ctx.as_ptr(),
    );

    (*sq_packet_box.packet.as_mut_ptr()).stream_index =
        sq_packet_box.packet_data.output_stream_index;

    av_interleaved_write_frame(out_fmt_ctx.as_ptr(), sq_packet_box.packet.as_mut_ptr())
}

unsafe fn mux_fixup_ts(
    st_rescale_delta_last: &mut [i64],
    oformat_flags: i32,
    st_last_dts: &mut [i64],
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

        let ts_rescale_delta_last = &mut st_rescale_delta_last[stream_index as usize];

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

    let last_mux_dts = &mut st_last_dts[stream_index as usize];

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

#[cfg(test)]
mod tests {
    use super::*;
    use crate::core::scheduler::ffmpeg_scheduler::{is_stopping, STATUS_RUN};
    use std::sync::mpsc;

    /// The delayed-mux failure paths (write_header error, worker spawn
    /// failure) must release the pre-counted mux thread slot AFTER recording
    /// the error — otherwise wait()/stop() hangs forever on the leaked slot
    /// (the thread-slot leak found by review), or wait() returns success
    /// without the error.
    #[test]
    fn fail_mux_init_releases_slot_and_records_error() {
        let thread_sync = ThreadSynchronizer::new();
        let scheduler_status = Arc::new(AtomicUsize::new(STATUS_RUN));
        let scheduler_result: Arc<Mutex<Option<crate::error::Result<()>>>> =
            Arc::new(Mutex::new(None));

        // The slot the scheduler pre-counts for a (possibly delayed) muxer.
        thread_sync.thread_start();

        fail_mux_init(
            &scheduler_status,
            &scheduler_result,
            &thread_sync,
            Muxing(MuxingOperationError::ThreadExited),
        );

        // Slot released: wait_for_all_threads returns instead of hanging.
        let (done_tx, done_rx) = mpsc::channel();
        let sync_clone = thread_sync.clone();
        std::thread::spawn(move || {
            sync_clone.wait_for_all_threads();
            let _ = done_tx.send(());
        });
        assert!(
            done_rx.recv_timeout(Duration::from_secs(5)).is_ok(),
            "mux thread slot leaked: wait_for_all_threads did not return"
        );

        // Error recorded before the slot release, terminal status published.
        assert!(is_stopping(scheduler_status.load(Ordering::Acquire)));
        assert!(matches!(
            &*scheduler_result.lock().unwrap(),
            Some(Err(_))
        ));
    }
}
