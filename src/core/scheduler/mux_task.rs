use crate::core::context::muxer::{Muxer, SqMuxPlan, StreamBsfChains};
use crate::core::context::obj_pool::ObjPool;
use crate::core::context::pre_mux_queue::PreMuxQueueReceiver;
use crate::core::context::{PacketBox, PacketData};
use crate::core::scheduler::ffmpeg_scheduler::{
    is_stopping, packet_is_null, set_scheduler_error, wait_until_not_paused, STATUS_ABORT,
    STATUS_END,
};
use crate::core::scheduler::input_controller::{InputController, SchNode};
use crate::core::scheduler::sync_queue::SyncQueue;
use crate::error::Error::Muxing;
use crate::error::{MuxingError, MuxingOperationError, WriteHeaderError};
use crate::raw::{BitStreamFilter, FormatContext};
use crate::util::ffmpeg_utils::{av_err2str, hashmap_to_avdictionary, DictGuard};
use crate::util::thread_synchronizer::ThreadSynchronizer;
use crossbeam_channel::{Receiver, RecvTimeoutError, Sender};
use ffmpeg_next::packet::{Mut, Ref};
use ffmpeg_next::Packet;
use ffmpeg_sys_next::AVMediaType::{AVMEDIA_TYPE_AUDIO, AVMEDIA_TYPE_SUBTITLE, AVMEDIA_TYPE_VIDEO};
use ffmpeg_sys_next::{
    av_compare_ts, av_get_audio_frame_duration2, av_interleaved_write_frame, av_packet_move_ref,
    av_packet_rescale_ts, av_rescale_delta, av_rescale_q, av_write_trailer,
    avcodec_parameters_copy, avformat_write_header, AVFormatContext, AVPacket, AVRational, AVERROR,
    AVERROR_EOF, AVFMT_NOTIMESTAMPS, AVFMT_TS_NONSTRICT, AV_LOG_DEBUG, AV_LOG_WARNING,
    AV_NOPTS_VALUE, AV_PKT_FLAG_KEY, AV_TIME_BASE_Q, EAGAIN, ENOMEM,
};
use log::{debug, error, info, trace, warn};
use std::collections::HashMap;
use std::collections::VecDeque;
use std::ffi::{CStr, CString};
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::{Arc, Mutex};
use std::time::Duration;

/// A muxer's `-shortest` packet sync queue (mirrors `Muxer.sq_mux`). Owns the
/// single-threaded `SyncQueue<PacketBox>` and the output-stream-index <-> sq-index
/// maps. Built from an [`SqMuxPlan`]; present only when the FFmpeg gate fires.
struct SqMux {
    queue: SyncQueue<PacketBox>,
    /// `output_stream_index -> Some(sq_idx)` for interleaved members, `None` for
    /// attachments (mirrors `ms->sq_idx_mux == -1`).
    sq_idx: Vec<Option<usize>>,
    /// `sq_idx -> output_stream_index` (reverse of `sq_idx`), for mapping a
    /// cascade-finished sq stream back to its output stream.
    ostream: Vec<usize>,
}

/// Build the `sq_mux` from its plan. `add_stream` order is output-stream-index
/// order (the plan is built that way), so `ostream[sq_idx]` is the reverse map.
fn build_sq_mux(plan: SqMuxPlan, stream_count: usize) -> SqMux {
    let mut queue = SyncQueue::<PacketBox>::new(plan.buf_size_us);
    let mut sq_idx = vec![None; stream_count];
    let mut ostream = Vec::with_capacity(plan.streams.len());
    for (output_stream_index, limiting, frames_max) in plan.streams {
        let idx = queue.add_stream(limiting);
        if output_stream_index < stream_count {
            sq_idx[output_stream_index] = Some(idx);
        }
        ostream.push(output_stream_index);
        if let Some(max) = frames_max {
            queue.sq_limit_frames(idx, max);
        }
    }
    SqMux {
        queue,
        sq_idx,
        ostream,
    }
}

/// `frame_end` for a mux packet (`sync_queue.c:126`): `pts + duration` in the
/// packet's own time base, or `None` when the packet carries no pts. Packets
/// have no `frame_samples`, so `nb_samples` is always 0.
///
/// # Safety
/// - `pkt` must be a valid, non-null pointer to an initialized `AVPacket` that
///   stays alive for the call (dereferenced to read `pts`/`duration`/`time_base`).
unsafe fn sq_pkt_end(pkt: *const AVPacket) -> (Option<i64>, AVRational, i32) {
    let pts = (*pkt).pts;
    let end = if pts == AV_NOPTS_VALUE {
        None
    } else {
        Some(pts + (*pkt).duration)
    };
    (end, (*pkt).time_base, 0)
}

pub(crate) fn mux_init(
    mux_idx: usize,
    mux: &mut Muxer,
    packet_pool: ObjPool<Packet>,
    input_controller: Arc<InputController>,
    mux_stream_nodes: Vec<Arc<SchNode>>,
    scheduler_status: Arc<AtomicUsize>,
    thread_sync: ThreadSynchronizer,
    scheduler_result: Arc<Mutex<Option<crate::error::Result<()>>>>,
    mux_done_remaining: Arc<AtomicUsize>,
) -> crate::error::Result<()> {
    // Compute the `-shortest` packet sync-queue plan while the output context is
    // still owned (it reads each stream's media type); `None` unless the gate
    // fires.
    let sq_mux_plan = mux.sq_mux_plan();
    // Take sole ownership of the output context out of the Muxer; move it by
    // value through the handoff. The move is the ownership transfer — no
    // null-the-source dance.
    let out_fmt_ctx = mux
        .out_fmt_ctx
        .take()
        .expect("mux_init called without an output context");
    // This muxer's completion guard: it flows through the handoff and drops on
    // whichever exit path this muxer takes (last one publishes STATUS_END).
    let mux_done = MuxDoneGuard::new(mux_done_remaining, scheduler_status.clone());
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
        sq_mux_plan,
        scheduler_status,
        thread_sync,
        scheduler_result,
        mux_done,
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
    mux_done_remaining: Arc<AtomicUsize>,
) -> crate::error::Result<Option<crossbeam_channel::Sender<i32>>> {
    if !mux.is_ready() {
        let (sender, receiver) = crossbeam_channel::bounded(1);
        // This muxer's completion guard (a ready muxer takes the `mux_init` path
        // and makes its own; the else-branch below drops `mux_done_remaining`
        // without counting). Moved into the waiter thread so it drops on every
        // exit — stop/disconnect before init, or through mux_task_start.
        let mux_done = MuxDoneGuard::new(mux_done_remaining, scheduler_status.clone());

        // Take sole ownership of the output context out of the Muxer. It is moved
        // into the waiter thread below; if streams never become ready the waiter
        // drops it (freeing once) — the RAII net the old box#1 provided.
        // Computed while the output context is still owned (reads stream types).
        let sq_mux_plan = mux.sq_mux_plan();
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
                        sq_mux_plan,
                        scheduler_status,
                        thread_sync,
                        scheduler_result,
                        mux_done,
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

fn mux_task_start(
    mux_idx: usize,
    out_fmt_ctx: FormatContext,
    queue: Option<(Sender<PacketBox>, Receiver<PacketBox>)>,
    start_time_us: Option<i64>,
    recording_time_us: Option<i64>,
    stream_count: usize,
    format_opts: Option<HashMap<CString, CString>>,
    bsf_chains: StreamBsfChains,
    src_pre_receivers: Vec<PreMuxQueueReceiver>,
    mux_start_gate: Arc<crate::core::context::MuxStartGate>,
    enc_handle_receiver: Receiver<std::thread::JoinHandle<()>>,
    packet_pool: ObjPool<Packet>,
    input_controller: Arc<InputController>,
    mux_stream_nodes: Vec<Arc<SchNode>>,
    sq_mux_plan: Option<SqMuxPlan>,
    scheduler_status: Arc<AtomicUsize>,
    thread_sync: ThreadSynchronizer,
    scheduler_result: Arc<Mutex<Option<crate::error::Result<()>>>>,
    mux_done: MuxDoneGuard,
) -> crate::error::Result<()> {
    if queue.is_none() {
        // Zero-stream output (e.g. an AVFMT_NOSTREAMS muxer): no mux worker
        // thread will be spawned to release this muxer's pre-counted thread
        // slot, so release it here — otherwise wait()/stop() hangs forever on
        // the leaked slot (pre-existing; surfaced by the PERF-12 slot-leak
        // review). A streamless output is legitimate for such formats, so this
        // is not an error.
        release_mux_slot(&scheduler_status, &thread_sync);
        // `out_fmt_ctx` is owned here: this early return drops it, freeing the
        // context. `mux_done` also drops here — a streamless output still counts
        // toward "all muxers done", so a mix with a real output cannot strand a
        // choked demuxer. Previously the raw pointer was leaked on this path.
        return Ok(());
    }

    let (queue_sender, queue_receiver) = queue.unwrap();

    _mux_init(
        mux_idx,
        out_fmt_ctx,
        queue_receiver,
        start_time_us,
        recording_time_us,
        stream_count,
        format_opts,
        bsf_chains,
        enc_handle_receiver,
        packet_pool,
        input_controller,
        mux_stream_nodes,
        sq_mux_plan,
        scheduler_status,
        thread_sync,
        scheduler_result,
        mux_done,
    )?;

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
    bsf_chains: StreamBsfChains,
    enc_handle_receiver: Receiver<std::thread::JoinHandle<()>>,
    packet_pool: ObjPool<Packet>,
    input_controller: Arc<InputController>,
    mux_stream_nodes: Vec<Arc<SchNode>>,
    sq_mux_plan: Option<SqMuxPlan>,
    scheduler_status: Arc<AtomicUsize>,
    thread_sync: ThreadSynchronizer,
    scheduler_result: Arc<Mutex<Option<crate::error::Result<()>>>>,
    mux_done: MuxDoneGuard,
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
    // When no output sets a BSF, `stream_bsfs` is an empty vec; the worker then
    // gates all BSF work off `has_bsf` and the packet path below is byte-for-
    // byte the pre-BSF path.
    let stream_bsfs =
        match unsafe { init_bitstream_filters(out_fmt_ctx_ptr, &bsf_chains, stream_count) } {
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
        error!(
            "Could not write header (incorrect codec parameters ?): {}",
            av_err2str(ret)
        );
        fail_mux_init(
            &scheduler_status,
            &scheduler_result,
            &thread_sync,
            Muxing(MuxingOperationError::WriteHeader(WriteHeaderError::from(
                ret,
            ))),
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

    let format_name = unsafe {
        CStr::from_ptr((*(*out_fmt_ctx_ptr).oformat).name)
            .to_str()
            .unwrap_or("unknown")
    };

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
        // This muxer's completion guard rides the worker to the end: it drops
        // AFTER the loop, trailer, and `thread_done_with` below, so the last
        // muxer publishes STATUS_END once the output is fully written — covering
        // the paths (write returning AVERROR_EOF) the balancing pass misses. On
        // a failed spawn the un-run closure is dropped, so it still counts.
        let _mux_done = mux_done;
        // Per-output-stream BSF chains (None for streams without one), or an
        // empty vec when no output set a BSF at all. Owned by the worker; each
        // `BitStreamFilter` frees its AVBSFContext/AVPacket on drop.
        let mut stream_bsfs = stream_bsfs;
        // Loop-invariant gate: when false, the packet path below is byte-for-
        // byte the pre-BSF path (no wrapper call, no template, no flush).
        let has_bsf = !stream_bsfs.is_empty();
        // Last packet_data seen per stream, used as the metadata template for
        // BSF flush packets at EOF (they carry no PacketData of their own).
        // Only allocated when at least one stream has a BSF.
        let mut stream_pkt_templates: Vec<Option<PacketData>> = if has_bsf {
            (0..stream_count).map(|_| None).collect()
        } else {
            Vec::new()
        };
        let mut stream_started: Vec<bool> = vec![false; stream_count];
        let mut stream_eof: Vec<bool> = vec![false; stream_count];
        // Per-stream timestamp state, indexed by output_stream_index (always a
        // valid mux stream index in [0, stream_count), same invariant the code
        // relies on to index out_fmt_ctx.streams). Flat Vecs instead of HashMaps
        // to drop the per-packet hash lookup on the mux hot path (alloc-06).
        let mut st_rescale_delta_last: Vec<i64> = vec![0; stream_count];
        let mut st_last_dts: Vec<i64> = vec![AV_NOPTS_VALUE; stream_count];

        // `-shortest` packet sync queue (owned by this single worker; `None`
        // unless the gate fired). When present, the loop routes every packet
        // through it so copy/subtitle/data followers truncate to the shortest
        // encoded stream, and its cascade publishes `source_finished` to stop a
        // live follower on the demux side (Architecture Y').
        let sq_mux = sq_mux_plan.map(|plan| build_sq_mux(plan, stream_count));

        let mut nb_done = 0;

        // Bundle the stable muxer config and the mutable per-stream state threaded
        // through the sync-queue mux path. `cfg` holds SHARED refs, so the loop
        // below still uses `packet_pool` / `scheduler_status` / `mux_stream_nodes`
        // / `input_controller` / `out_fmt_ctx` directly; `state` holds the
        // exclusive refs, so every direct access to those five arrays goes through
        // `state`.
        let cfg = MuxWriteCfg {
            has_bsf,
            oformat_flags,
            stream_count,
            out_fmt_ctx: &out_fmt_ctx,
            packet_pool: &packet_pool,
            mux_stream_nodes: &mux_stream_nodes,
            input_controller: &input_controller,
            scheduler_status: &scheduler_status,
        };
        let mut state = MuxWriteState {
            stream_pkt_templates: &mut stream_pkt_templates,
            st_rescale_delta_last: &mut st_rescale_delta_last,
            st_last_dts: &mut st_last_dts,
            stream_eof: &mut stream_eof,
            nb_done: &mut nb_done,
        };

        let mut ret = 0;

        if let Some(mut sq) = sq_mux {
            // Reused scratch: released packets to write, and cascade-finished
            // sq-indices, both cleared inside `sq_mux_pump` each call.
            let mut released: Vec<PacketBox> = Vec::new();
            let mut nf: Vec<usize> = Vec::new();
            // finish tb is unused on the null-item (finish) path.
            let fin_tb = AVRational { num: 1, den: 1 };

            // Pre-finish any non-member stream that is inside `stream_count`. The
            // only non-interleaved type is ATTACHMENT, which normally lives
            // OUTSIDE `stream_count` (created via raw `avformat_new_stream`, see
            // context/attachment.rs) — but a mapped attachment-copy (`-map 0:t`)
            // can land inside it. Such a stream is header-only and never streams a
            // packet, so it is done immediately: count it toward `nb_done`, mark
            // it EOF (drop any stray packet), and publish `source_finished`. That
            // last step is essential — otherwise its `last_dts` stays 0 and pins
            // the balancing InputController's `trailing_dts`, choking the real
            // members, and its demux follower scan never retires. In the normal
            // case (no such stream) this loop does nothing.
            for i in 0..stream_count {
                if sq.sq_idx.get(i).copied().flatten().is_none() {
                    if let Err(e) = unsafe {
                        sq_finish_output_stream(
                            i,
                            &cfg,
                            &mut state,
                            &mut stream_bsfs,
                        )
                    } {
                        ret = e;
                    }
                }
            }

            while *state.nb_done < stream_count && ret >= 0 {
                let result = pkt_receiver.recv_timeout(Duration::from_millis(100));

                if is_stopping(wait_until_not_paused(&scheduler_status)) {
                    info!("Muxer receiver end command, finishing.");
                    break;
                }

                let mut packet_box = match result {
                    Ok(pb) => pb,
                    Err(RecvTimeoutError::Disconnected) => {
                        debug!("Encoder thread exit.");
                        break;
                    }
                    Err(RecvTimeoutError::Timeout) => {
                        // Idle tick: fire the sync-queue heartbeat so a live-but-
                        // stalled laggard cannot pin releasable followers forever.
                        match sq_mux_pump(
                            &mut sq, &mut released, &mut nf, &cfg, &mut state, &mut stream_bsfs,
                        ) {
                            Ok(true) => break,
                            Ok(false) => continue,
                            Err(e) => { ret = e; break; }
                        }
                    }
                };

                let pkt = packet_box.packet.as_ptr();
                let raw_stream_index = unsafe { (*pkt).stream_index };

                // Demux EOF signal (recording_time / streamcopy EOF): finish that
                // stream in sq_mux so the cascade truncates its followers.
                if raw_stream_index < 0 {
                    let eof_stream = packet_box.packet_data.output_stream_index;
                    packet_pool.release(packet_box.packet);
                    if eof_stream >= 0 {
                        if let Some(Some(sq_i)) =
                            sq.sq_idx.get(eof_stream as usize).copied()
                        {
                            sq.queue.send(sq_i, None, None, fin_tb, 0);
                        }
                    }
                    match sq_mux_pump(
                        &mut sq, &mut released, &mut nf, &cfg, &mut state, &mut stream_bsfs,
                    ) {
                        Ok(true) => break,
                        Ok(false) => continue,
                        Err(e) => { ret = e; break; }
                    }
                }

                let stream_index = raw_stream_index as usize;
                if stream_index >= mux_stream_nodes.len() {
                    error!("Invalid stream_index: {} >= {}", stream_index, mux_stream_nodes.len());
                    packet_pool.release(packet_box.packet);
                    continue;
                }

                // Encoder EOF marker (null / empty packet): finish this stream in
                // sq_mux, driving the cascade (mirrors the plain loop's marker).
                let is_marker = unsafe {
                    let has_side_data = (*pkt).side_data_elems > 0;
                    packet_is_null(&packet_box.packet)
                        || (packet_box.packet.is_empty() && !has_side_data)
                };
                if is_marker {
                    if scheduler_status.load(Ordering::Acquire) == STATUS_ABORT {
                        debug!("Muxer detected abort from stream {}, exiting without trailer", stream_index);
                        packet_pool.release(packet_box.packet);
                        break;
                    }
                    packet_pool.release(packet_box.packet);
                    if !state.stream_eof[stream_index] {
                        if let Some(Some(sq_i)) = sq.sq_idx.get(stream_index).copied() {
                            sq.queue.send(sq_i, None, None, fin_tb, 0);
                        }
                    }
                    match sq_mux_pump(
                        &mut sq, &mut released, &mut nf, &cfg, &mut state, &mut stream_bsfs,
                    ) {
                        Ok(true) => break,
                        Ok(false) => continue,
                        Err(e) => { ret = e; break; }
                    }
                }

                unsafe {
                    update_last_dts(&mux_stream_nodes[stream_index], &input_controller, &scheduler_status, pkt);
                }

                // Already truncated: drop further packets for this stream.
                if state.stream_eof[stream_index] {
                    packet_pool.release(packet_box.packet);
                    continue;
                }

                // Resolve this stream's sq slot. A non-member (attachment) is not
                // expected on the packet path; write it directly rather than drop.
                let sq_i = match sq.sq_idx.get(stream_index).copied().flatten() {
                    Some(i) => i,
                    None => {
                        let wret = unsafe {
                            mux_write_released(
                                &mut packet_box, &cfg, &mut state, &mut stream_bsfs,
                            )
                        };
                        packet_pool.release(packet_box.packet);
                        if wret == AVERROR_EOF { break; }
                        if wret < 0 { ret = wret; error!("Error muxing a packet: stream_index={stream_index}, ret={wret}"); break; }
                        continue;
                    }
                };

                // Streamcopy timestamp fixup + recording_time, exactly as the
                // plain path — before the packet enters sq_mux.
                if packet_box.packet_data.is_copy {
                    let started = &mut stream_started[stream_index];
                    let rret = unsafe {
                        streamcopy_rescale(
                            packet_box.packet.as_mut_ptr(),
                            &packet_box.packet_data,
                            &start_time_us,
                            &recording_time_us,
                            started,
                        )
                    };
                    if rret == AVERROR(EAGAIN) {
                        packet_pool.release(packet_box.packet);
                        continue;
                    } else if rret == AVERROR_EOF {
                        packet_pool.release(packet_box.packet);
                        sq.queue.send(sq_i, None, None, fin_tb, 0);
                        match sq_mux_pump(
                            &mut sq, &mut released, &mut nf, &cfg, &mut state, &mut stream_bsfs,
                        ) {
                            Ok(true) => break,
                            Ok(false) => continue,
                            Err(e) => { ret = e; break; }
                        }
                    }
                }

                // Feed the data packet; sq_mux holds / reorders / truncates it.
                let (end_ts, tb, nb_samples) =
                    unsafe { sq_pkt_end(packet_box.packet.as_ptr()) };
                sq.queue.send(sq_i, Some(packet_box), end_ts, tb, nb_samples);
                match sq_mux_pump(
                    &mut sq, &mut released, &mut nf, &cfg, &mut state, &mut stream_bsfs,
                ) {
                    Ok(true) => break,
                    Ok(false) => {}
                    Err(e) => { ret = e; break; }
                }
            }
        } else {

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
                    if eof_idx < stream_count && !state.stream_eof[eof_idx] {
                        // Flush trailing BSF packets before finishing this
                        // stream. Skipped entirely when no mux stream has a BSF.
                        if has_bsf {
                            let fret = unsafe {
                                flush_stream_bsf(
                                    &cfg,
                                    &mut state,
                                    &mut stream_bsfs,
                                    eof_idx,
                                )
                            };
                            if fret < 0 {
                                ret = fret;
                                error!("Error flushing bitstream filter at EOF: stream={eof_idx}, ret={fret}");
                                packet_pool.release(packet_box.packet);
                                break;
                            }
                        }
                        state.stream_eof[eof_idx] = true;
                        *state.nb_done += 1;
                        if eof_idx < mux_stream_nodes.len() {
                            let node = mux_stream_nodes[eof_idx].as_ref();
                            let SchNode::MuxStream { src: _, last_dts: _, source_finished } = node else { unreachable!() };
                            source_finished.store(true, Ordering::Release);
                            input_controller.update_locked(&scheduler_status);
                        }
                    }
                }
                packet_pool.release(packet_box.packet);
                if *state.nb_done == stream_count {
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
                    if state.stream_eof[stream_index] {
                        packet_pool.release(packet_box.packet);
                        continue;
                    }

                    // Flush trailing BSF packets before finishing this stream.
                    // Skipped entirely when no mux stream has a BSF. Already
                    // inside `unsafe`.
                    if has_bsf {
                        let fret = flush_stream_bsf(
                            &cfg,
                            &mut state,
                            &mut stream_bsfs,
                            stream_index,
                        );
                        if fret < 0 {
                            ret = fret;
                            error!("Error flushing bitstream filter at EOF: stream={stream_index}, ret={fret}");
                            packet_pool.release(packet_box.packet);
                            break;
                        }
                    }

                    *state.nb_done += 1;
                    packet_pool.release(packet_box.packet);

                    let mux_stream_node = mux_stream_node.as_ref();
                    let SchNode::MuxStream { src: _, last_dts: _, source_finished } = mux_stream_node else { unreachable!() };
                    source_finished.store(true, Ordering::Release);
                    input_controller.update_locked(&scheduler_status);

                    if *state.nb_done == stream_count {
                        trace!("All streams finished");
                        break;
                    } else {
                        continue;
                    }
                }

                update_last_dts(mux_stream_node, &input_controller, &scheduler_status, pkt);

                // Skip packets for streams that already hit recording_time EOF
                if state.stream_eof[stream_index] {
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
                        // sch_mux_receive_finish behavior in ffmpeg_mux.c:442.
                        // Flush trailing BSF packets first. Skipped entirely when
                        // no mux stream has a BSF.
                        if has_bsf {
                            let fret = flush_stream_bsf(
                                &cfg,
                                &mut state,
                                &mut stream_bsfs,
                                stream_index,
                            );
                            if fret < 0 {
                                ret = fret;
                                error!("Error flushing bitstream filter at EOF: stream={stream_index}, ret={fret}");
                                packet_pool.release(packet_box.packet);
                                break;
                            }
                        }
                        state.stream_eof[stream_index] = true;
                        packet_pool.release(packet_box.packet);

                        *state.nb_done += 1;
                        let mux_stream_node = mux_stream_node.as_ref();
                        let SchNode::MuxStream { src: _, last_dts: _, source_finished } = mux_stream_node else { unreachable!() };
                        source_finished.store(true, Ordering::Release);
                        input_controller.update_locked(&scheduler_status);

                        if *state.nb_done == stream_count {
                            trace!("All streams finished (recording_time)");
                            break;
                        }
                        continue;
                    }
                }

                // write. Without any BSF on this mux, take exactly the pre-BSF
                // path (direct `write_packet`, no wrapper, no template).
                if !packet_is_null(&packet_box.packet)
                    && (*packet_box.packet.as_ptr()).stream_index >= 0
                {
                    if has_bsf {
                        // Snapshot this stream's packet metadata so a later EOF
                        // flush can stamp the BSF's trailing packets correctly.
                        if stream_bsfs[stream_index].is_some() {
                            state.stream_pkt_templates[stream_index] =
                                Some(packet_box.packet_data);
                        }
                        ret = mux_filter_and_write_packet(
                            &cfg,
                            &mut state,
                            &mut packet_box,
                            stream_bsfs[stream_index].as_mut(),
                        );
                    } else {
                        ret = write_packet(
                            &cfg,
                            &mut state,
                            &mut packet_box,
                        );
                    }
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
        return Err(MuxingOperationError::ThreadExited.into());
    }

    Ok(())
}

/// Releases a muxer's pre-counted thread slot, publishing STATUS_END if this
/// was the last live thread (mirroring the normal mux-worker exit). Used on
/// every path where the slot was counted at scheduler start but no (further)
/// worker will release it: streamless output, write_header failure, worker
/// spawn failure. Without this the slot leaks and wait()/stop() hangs forever.
fn release_mux_slot(scheduler_status: &Arc<AtomicUsize>, thread_sync: &ThreadSynchronizer) {
    thread_sync.thread_done_with(|| {
        scheduler_status.store(STATUS_END, Ordering::Release);
    });
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
    release_mux_slot(scheduler_status, thread_sync);
}

/// Muxer-completion counter — the ez equivalent of fftools
/// `Scheduler.nb_mux`/`nb_mux_done` (`ffmpeg_sched.c` `mux_done -> sch_wait ->
/// sch_stop`). One guard per muxer, created at scheduler start; whichever muxer
/// finishes LAST — every output having finished, failed, or been streamless —
/// publishes `STATUS_END` directly.
///
/// This closes a termination gap the per-thread counter (`ThreadSynchronizer`)
/// cannot cover on its own: `STATUS_END` was otherwise published only once the
/// LAST worker exited, yet a demuxer choked by the `InputController` is itself a
/// non-exited worker parked in `SchWaiter::wait_with_scheduler_status` waiting
/// for that very status flip. When a muxer exits on a path that never marks all
/// streams `source_finished` (a write returning `AVERROR_EOF`, a streamless
/// output), the balancing pass never unchokes that demuxer and the scheduler
/// hangs forever. Keying the terminal publish on "all muxers done" — a subset of
/// workers that excludes demuxers — breaks the ring from the output side.
///
/// Implemented as an RAII guard rather than a manual call at each exit so the
/// muxer-completion COUNT is exactly-once on every path — normal finish,
/// BSF/header/write failure, worker-spawn failure, streamless output, and
/// unwind (the guard drops on panic): the ownership move into the worker/waiter
/// closures makes the compiler enforce it (a missed or doubled count would
/// silently reintroduce the hang). This governs the mux-done count only; the
/// worker's own per-thread slot (`thread_done_with`) keeps its existing,
/// separate panic behavior.
struct MuxDoneGuard {
    remaining: Arc<AtomicUsize>,
    scheduler_status: Arc<AtomicUsize>,
}

impl MuxDoneGuard {
    fn new(remaining: Arc<AtomicUsize>, scheduler_status: Arc<AtomicUsize>) -> Self {
        Self {
            remaining,
            scheduler_status,
        }
    }
}

impl Drop for MuxDoneGuard {
    fn drop(&mut self) {
        // `fetch_sub` returns the PREVIOUS value; 1 means this guard was the last
        // live muxer. Publishing only stores the atomic (no condvar/waker notify):
        // a choked demuxer observes it within one 100ms poll and exits, and the
        // existing per-thread counter then wakes `wait()` on the true last thread.
        if self.remaining.fetch_sub(1, Ordering::AcqRel) != 1 {
            return;
        }
        // Last muxer done -> publish STATUS_END, but NEVER downgrade a terminal
        // status already in flight: `abort()` owns STATUS_ABORT and
        // `set_scheduler_error` owns STATUS_END; the `is_stopping` guard below
        // refuses to overwrite either.
        let mut current = self.scheduler_status.load(Ordering::Acquire);
        while !is_stopping(current) {
            match self.scheduler_status.compare_exchange_weak(
                current,
                STATUS_END,
                Ordering::AcqRel,
                Ordering::Acquire,
            ) {
                Ok(_) => break,
                Err(actual) => current = actual,
            }
        }
    }
}

/// # Safety
/// - `pkt` must be a valid, non-null pointer to an initialized `AVPacket` that
///   stays alive for the call (dereferenced to read `dts`/`duration`/`time_base`).
unsafe fn update_last_dts(
    mux_stream_node: &Arc<SchNode>,
    input_controller: &Arc<InputController>,
    scheduler_status: &Arc<AtomicUsize>,
    pkt: *const AVPacket,
) {
    if (*pkt).dts != AV_NOPTS_VALUE {
        let dts = av_rescale_q(
            (*pkt).dts + (*pkt).duration,
            (*pkt).time_base,
            AV_TIME_BASE_Q,
        );
        let node = mux_stream_node.as_ref();
        let SchNode::MuxStream {
            src: _,
            last_dts,
            source_finished: _,
        } = node
        else {
            unreachable!()
        };
        last_dts.store(dts, Ordering::Release);
        input_controller.update_locked(scheduler_status);
    }
}

/// # Safety
/// - `pkt` must be a valid, non-null pointer to an initialized `AVPacket`,
///   exclusively borrowed for the call: it is both read and written in place
///   (`pts`/`dts` are mutated, `flags` is read).
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

/// Immutable muxer context shared by every write in the sync-queue mux path.
/// Grouping the stable config that used to be threaded as separate params.
struct MuxWriteCfg<'a> {
    has_bsf: bool,
    oformat_flags: i32,
    stream_count: usize,
    out_fmt_ctx: &'a FormatContext,
    packet_pool: &'a ObjPool<Packet>,
    mux_stream_nodes: &'a [Arc<SchNode>],
    input_controller: &'a Arc<InputController>,
    scheduler_status: &'a Arc<AtomicUsize>,
}

/// Mutable per-stream / progress state threaded through the sync-queue mux path.
struct MuxWriteState<'a> {
    stream_pkt_templates: &'a mut [Option<PacketData>],
    st_rescale_delta_last: &'a mut [i64],
    st_last_dts: &'a mut [i64],
    stream_eof: &'a mut [bool],
    nb_done: &'a mut usize,
}

/// # Safety
/// - `cfg.out_fmt_ctx` must reference the live output context (its pointer is
///   passed to `av_interleaved_write_frame`).
/// - `sq_packet_box.packet` must wrap a live, writable packet.
/// - `sq_packet_box.packet_data.output_stream_index` must be a valid stream index
///   of that context and in bounds for `state`'s per-stream slices: it is used
///   (via `mux_fixup_ts`) to offset `(*out_fmt_ctx).streams` and to index
///   `state.st_rescale_delta_last` / `state.st_last_dts`.
unsafe fn write_packet(
    cfg: &MuxWriteCfg,
    state: &mut MuxWriteState,
    sq_packet_box: &mut PacketBox,
) -> i32 {
    mux_fixup_ts(cfg, state, sq_packet_box);

    (*sq_packet_box.packet.as_mut_ptr()).stream_index =
        sq_packet_box.packet_data.output_stream_index;

    av_interleaved_write_frame(cfg.out_fmt_ctx.as_ptr(), sq_packet_box.packet.as_mut_ptr())
}

/// Write one packet the `sq_mux` released, via the per-stream BSF (or the direct
/// path when the mux has none) — the same write the plain loop performs, just on
/// an `sq_mux`-ordered packet. Snapshots the BSF template exactly like the plain
/// path so an EOF flush stamps trailing packets correctly. Does NOT recycle the
/// pool shell (the caller owns the released `PacketBox`).
///
/// # Safety
/// - `cfg.out_fmt_ctx` must reference the live output context and
///   `packet_box.packet_data.output_stream_index` must be a valid stream index of
///   it: this delegates to `write_packet` / `mux_filter_and_write_packet`, which
///   offset `(*out_fmt_ctx).streams` by that index.
/// - `packet_box.packet` must wrap a live, writable packet.
unsafe fn mux_write_released(
    packet_box: &mut PacketBox,
    cfg: &MuxWriteCfg,
    state: &mut MuxWriteState,
    stream_bsfs: &mut [Option<BitStreamFilter>],
) -> i32 {
    let stream_index = packet_box.packet_data.output_stream_index as usize;
    if cfg.has_bsf {
        if stream_bsfs.get(stream_index).is_some_and(|b| b.is_some()) {
            state.stream_pkt_templates[stream_index] = Some(packet_box.packet_data);
        }
        mux_filter_and_write_packet(cfg, state, packet_box, stream_bsfs[stream_index].as_mut())
    } else {
        write_packet(cfg, state, packet_box)
    }
}

/// Finish one output stream in the mux worker: flush its trailing BSF packets,
/// mark it EOF, count it toward `nb_done`, and publish `source_finished` so the
/// demux stops producing this follower (Architecture Y'). Mirrors the plain
/// loop's per-stream EOF handling; the `sq_mux` cascade is the single place that
/// counts each stream once. Returns `Err(ret)` on a BSF flush error.
///
/// # Safety
/// - `cfg.out_fmt_ctx` must reference the live output context, and the per-stream
///   slices in `state` and `stream_bsfs` must be sized to `cfg.stream_count`: the
///   BSF flush this may call dereferences the context and indexes those slices.
///   (`ost` is itself bounds-checked against `stream_count` inside the function.)
unsafe fn sq_finish_output_stream(
    ost: usize,
    cfg: &MuxWriteCfg,
    state: &mut MuxWriteState,
    stream_bsfs: &mut [Option<BitStreamFilter>],
) -> Result<(), i32> {
    if ost >= cfg.stream_count || state.stream_eof[ost] {
        return Ok(());
    }
    if cfg.has_bsf {
        let fret = flush_stream_bsf(cfg, state, stream_bsfs, ost);
        if fret < 0 {
            return Err(fret);
        }
    }
    state.stream_eof[ost] = true;
    *state.nb_done += 1;
    if ost < cfg.mux_stream_nodes.len() {
        if let SchNode::MuxStream {
            source_finished, ..
        } = cfg.mux_stream_nodes[ost].as_ref()
        {
            source_finished.store(true, Ordering::Release);
        }
    }
    cfg.input_controller.update_locked(cfg.scheduler_status);
    Ok(())
}

/// After feeding `sq_mux`, write every releasable packet to the muxer, THEN apply
/// any cascade-finishes — strictly in that order so a follower's in-bound packets
/// are written before its finish drops the rest. This is the single place that
/// writes `sq_mux` output and counts `nb_done` (each stream once, via the
/// cascade). Returns `Ok(true)` when the muxer should stop (every stream done, or
/// a write EOF), `Ok(false)` to keep going, or `Err(ret)` on a fatal write/BSF
/// error.
fn sq_mux_pump(
    sq: &mut SqMux,
    released: &mut Vec<PacketBox>,
    nf: &mut Vec<usize>,
    cfg: &MuxWriteCfg,
    state: &mut MuxWriteState,
    stream_bsfs: &mut [Option<BitStreamFilter>],
) -> Result<bool, i32> {
    // 1) Write everything releasable BEFORE any finish drops future packets.
    released.clear();
    sq.queue.drain_all_releasable(released);
    for mut pb in released.drain(..) {
        let wret = unsafe { mux_write_released(&mut pb, cfg, state, stream_bsfs) };
        cfg.packet_pool.release(pb.packet);
        if wret == AVERROR_EOF {
            return Ok(true);
        } else if wret < 0 {
            return Err(wret);
        }
    }

    // 2) Apply cascade-finishes (the single `nb_done` authority).
    nf.clear();
    sq.queue.newly_finished(nf);
    for &sq_j in nf.iter() {
        let ost = sq.ostream[sq_j];
        unsafe {
            sq_finish_output_stream(ost, cfg, state, stream_bsfs)?;
        }
    }

    // Termination is over ALL `stream_count` streams, like the plain loop:
    // members are counted here via the cascade, and any non-member (a header-only
    // stream in `stream_count`, e.g. a mapped attachment-copy) is pre-finished at
    // worker start (see the `sq_idx == None` pre-finish loop), so `nb_done` can
    // always reach `stream_count`.
    Ok(*state.nb_done == cfg.stream_count)
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
///
/// # Safety
/// - `out_fmt_ctx` must be a valid, non-null `AVFormatContext` with at least
///   `stream_count` streams (each carrying a valid `codecpar`), alive for the
///   call: it is dereferenced and `streams.add(i)` is read for every `i` in
///   `0..stream_count`.
unsafe fn init_bitstream_filters(
    out_fmt_ctx: *mut AVFormatContext,
    bsf_chains: &StreamBsfChains,
    stream_count: usize,
) -> Result<Vec<Option<BitStreamFilter>>, (String, i32)> {
    // No output requested a BSF: return an EMPTY vec (no allocation). The mux
    // worker keys `has_bsf` off `is_empty()` and then takes byte-for-byte the
    // pre-BSF path — no wrapper, no template, no flush.
    if bsf_chains.is_empty() {
        return Ok(Vec::new());
    }

    let mut stream_bsfs: Vec<Option<BitStreamFilter>> = (0..stream_count).map(|_| None).collect();

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
///
/// # Safety
/// - `packet_box.packet` must wrap a live, writable packet: its raw pointer is
///   dereferenced and handed to the bitstream filter.
/// - `cfg.out_fmt_ctx` must reference the live output context and
///   `packet_box.packet_data.output_stream_index` must be a valid stream index of
///   it — the write it delegates to (`write_packet` / `drain_bsf_write`) offsets
///   `(*out_fmt_ctx).streams` by that index.
unsafe fn mux_filter_and_write_packet(
    cfg: &MuxWriteCfg,
    state: &mut MuxWriteState,
    packet_box: &mut PacketBox,
    bsf: Option<&mut BitStreamFilter>,
) -> i32 {
    let Some(bsf) = bsf else {
        return write_packet(cfg, state, packet_box);
    };

    // Send every real packet to the filter, exactly like fftools
    // mux_packet_filter (ffmpeg_mux.c). av_bsf_send_packet only treats a packet
    // as EOF when it is FFmpeg-empty (data == NULL && side_data_elems == 0,
    // AVPACKET_IS_EMPTY); side-data-only packets are filtered, not dropped. The
    // stream-end markers (truly-empty packets) are already intercepted upstream
    // and never reach this wrapper, so there is no premature-EOF hazard here.
    let pkt = packet_box.packet.as_mut_ptr();
    // Rescale into the filter's input timebase (fftools ffmpeg_mux.c).
    av_packet_rescale_ts(pkt, (*pkt).time_base, bsf.time_base_in());

    let ret = bsf.send_packet(pkt);
    if ret < 0 {
        return ret;
    }
    // send_packet took ownership of pkt's contents (reset to empty); the caller
    // still releases the now-empty shell to the pool.

    match drain_bsf_write(cfg, state, bsf, &packet_box.packet_data) {
        // Normal in-stream drain: input consumed, wait for more.
        BsfDrain::Exhausted => 0,
        // The filter self-EOF'd without a NULL flush (rare): stop the stream
        // just like a muxer EOF on the normal write path.
        BsfDrain::Flushed => AVERROR_EOF,
        // Propagate write/receive errors (incl. a muxer-side AVERROR_EOF).
        BsfDrain::Err(ret) => ret,
    }
}

/// Outcome of draining a bitstream filter's output packets. Distinguishes the
/// filter running out of input (`Exhausted`), a NULL-flush completing
/// (`Flushed`), and a write/receive error (`Err`) — critically keeping a
/// muxer-side `AVERROR_EOF` returned by `write_packet` as an `Err`, not a
/// completed flush, so it terminates muxing like the normal write path.
enum BsfDrain {
    /// `av_bsf_receive_packet` returned `EAGAIN`: input is exhausted, more may
    /// arrive later.
    Exhausted,
    /// `av_bsf_receive_packet` returned `AVERROR_EOF`: a NULL-flush completed.
    Flushed,
    /// A `write_packet`/muxer or `receive_packet` error (negative averror, which
    /// may itself be `AVERROR_EOF` when the *muxer* signals end).
    Err(i32),
}

/// Drain all currently available output packets from `bsf` into the muxer. Each
/// received packet is moved into a pooled `Packet`, stamped with the filter's
/// output timebase, tagged with `template` metadata, and written via
/// `write_packet`.
///
/// # Safety
/// - `bsf` must be an initialized bitstream filter.
/// - `cfg.out_fmt_ctx` must reference the live output context and
///   `template.output_stream_index` must be a valid stream index of it: every
///   drained packet is written via `write_packet`, which offsets
///   `(*out_fmt_ctx).streams` by that index.
unsafe fn drain_bsf_write(
    cfg: &MuxWriteCfg,
    state: &mut MuxWriteState,
    bsf: &mut BitStreamFilter,
    template: &PacketData,
) -> BsfDrain {
    loop {
        let ret = bsf.receive_packet();
        if ret == AVERROR(EAGAIN) {
            return BsfDrain::Exhausted;
        } else if ret == AVERROR_EOF {
            return BsfDrain::Flushed;
        } else if ret < 0 {
            // A receive error skips this packet's remaining BSF output and continues,
            // matching FFmpeg's default (ffmpeg_mux.c logs and continues unless
            // exit_on_error). Only send_packet / write_packet / muxer errors are
            // fatal. Returning Exhausted (not Err) avoids aborting the whole muxer.
            error!("Error receiving a packet from a bitstream filter (skipping): ret={ret}");
            return BsfDrain::Exhausted;
        }

        // Move the filtered packet into a pooled shell so it flows through the
        // existing write_packet/mux_fixup_ts path; the BSF's own packet is left
        // clean for the next receive.
        let mut out_pkt = match cfg.packet_pool.get() {
            Ok(p) => p,
            Err(_) => return BsfDrain::Err(AVERROR(ENOMEM)),
        };
        av_packet_move_ref(out_pkt.as_mut_ptr(), bsf.pkt_ptr());
        (*out_pkt.as_mut_ptr()).time_base = bsf.time_base_out();

        let mut out_box = PacketBox {
            packet: out_pkt,
            packet_data: *template,
        };
        let wret = write_packet(cfg, state, &mut out_box);
        cfg.packet_pool.release(out_box.packet);
        if wret < 0 {
            // Includes a muxer-side AVERROR_EOF — an error to propagate, NOT a
            // completed BSF flush.
            return BsfDrain::Err(wret);
        }
    }
}

/// Flush a stream's bitstream filter at EOF: send NULL and drain trailing
/// packets before the stream is marked finished. No-op (returns `0`) when the
/// stream has no BSF. Returns `0` on success (including the drain's terminal
/// EOF) or a negative muxing error.
///
/// # Safety
/// - `stream_index` must be in bounds for `stream_bsfs` and
///   `state.stream_pkt_templates`, and be a valid stream index of
///   `cfg.out_fmt_ctx`: on the no-template path
///   `(*out_fmt_ctx).streams.add(stream_index)` and its `codecpar` are read.
/// - `cfg.out_fmt_ctx` must reference the live output context.
unsafe fn flush_stream_bsf(
    cfg: &MuxWriteCfg,
    state: &mut MuxWriteState,
    stream_bsfs: &mut [Option<BitStreamFilter>],
    stream_index: usize,
) -> i32 {
    let Some(bsf) = stream_bsfs[stream_index].as_mut() else {
        return 0;
    };

    // Metadata for the trailing packets: reuse the last real packet's template,
    // or synthesize one from the output stream's codecpar if none was seen.
    let template = match &state.stream_pkt_templates[stream_index] {
        Some(t) => *t,
        None => {
            let st = *(*cfg.out_fmt_ctx.as_ptr()).streams.add(stream_index);
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
    match drain_bsf_write(cfg, state, bsf, &template) {
        // The NULL-flush completed (Flushed) or produced no trailing packets
        // (Exhausted): both are success. A write/receive error — including a
        // muxer-side AVERROR_EOF — propagates so the worker terminates.
        BsfDrain::Flushed | BsfDrain::Exhausted => 0,
        BsfDrain::Err(ret) => ret,
    }
}

/// # Safety
/// - `cfg.out_fmt_ctx` must reference the live output context and
///   `packet_box.packet_data.output_stream_index` must be a valid stream index of
///   it: `(*out_fmt_ctx).streams.add(output_stream_index)` is read unchecked.
/// - `packet_box.packet` must wrap a live, writable packet: its
///   `pts`/`dts`/`duration`/`time_base` are mutated in place.
/// - `output_stream_index` must also be in bounds for
///   `state.st_rescale_delta_last` and `state.st_last_dts`.
unsafe fn mux_fixup_ts(cfg: &MuxWriteCfg, state: &mut MuxWriteState, packet_box: &mut PacketBox) {
    let out_fmt_ctx = cfg.out_fmt_ctx.as_ptr();
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

        let ts_rescale_delta_last = &mut state.st_rescale_delta_last[stream_index as usize];

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

    let last_mux_dts = &mut state.st_last_dts[stream_index as usize];

    if (cfg.oformat_flags & AVFMT_NOTIMESTAMPS) == 0 {
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
            let max = *last_mux_dts + ((cfg.oformat_flags & AVFMT_TS_NONSTRICT) == 0) as i64;
            if (*pkt).dts < max {
                let loglevel =
                    if max - (*pkt).dts > 2 || packet_data.codec_type == AVMEDIA_TYPE_VIDEO {
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
    use crate::core::scheduler::ffmpeg_scheduler::{
        is_stopping, STATUS_ABORT, STATUS_END, STATUS_RUN,
    };
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
        assert!(matches!(&*scheduler_result.lock().unwrap(), Some(Err(_))));
    }

    /// The zero-stream (AVFMT_NOSTREAMS) early return must release the
    /// pre-counted slot WITHOUT recording an error — a streamless output is
    /// legitimate, but leaving the slot counted hangs wait()/stop() (the
    /// pre-existing leak surfaced by the slot-leak review).
    #[test]
    fn release_mux_slot_unblocks_wait_without_error() {
        let thread_sync = ThreadSynchronizer::new();
        let scheduler_status = Arc::new(AtomicUsize::new(STATUS_RUN));

        thread_sync.thread_start();
        release_mux_slot(&scheduler_status, &thread_sync);

        let (done_tx, done_rx) = mpsc::channel();
        let sync_clone = thread_sync.clone();
        std::thread::spawn(move || {
            sync_clone.wait_for_all_threads();
            let _ = done_tx.send(());
        });
        assert!(
            done_rx.recv_timeout(Duration::from_secs(5)).is_ok(),
            "zero-stream mux slot leaked: wait_for_all_threads did not return"
        );
        // Last thread released: STATUS_END published, but no error recorded.
        assert!(is_stopping(scheduler_status.load(Ordering::Acquire)));
    }

    // Regression for the premature-STATUS_END truncation (found by the SHIP
    // review): a streamless (AVFMT_NOSTREAMS) output releases its thread slot
    // synchronously via `release_mux_slot`. The scheduler now pre-counts EVERY
    // muxer's slot before any `mux_init`, so an early streamless release cannot
    // drive the thread counter to zero and stop later, still-pending outputs.
    // Here both slots are pre-counted (as the scheduler does): releasing the
    // first must NOT publish a terminal status.
    #[test]
    fn streamless_release_is_not_premature_with_pre_counted_slots() {
        let thread_sync = ThreadSynchronizer::new();
        let status = Arc::new(AtomicUsize::new(STATUS_RUN));

        // Two muxers, both slots claimed up front.
        thread_sync.thread_start();
        thread_sync.thread_start();

        // The first (e.g. streamless) output releases its slot.
        release_mux_slot(&status, &thread_sync);
        assert_eq!(
            status.load(Ordering::Acquire),
            STATUS_RUN,
            "releasing one of two pre-counted mux slots must not publish a terminal status"
        );

        // The second output releases -> now the last slot -> terminal.
        release_mux_slot(&status, &thread_sync);
        assert!(
            is_stopping(status.load(Ordering::Acquire)),
            "the last mux slot release publishes STATUS_END"
        );
    }

    // MuxDoneGuard is the fftools nb_mux/nb_mux_done parity: STATUS_END must be
    // published only when the LAST muxer's guard drops. The RAII move into each
    // muxer's worker/waiter closure is what guarantees every exit path (normal
    // finish, write-AVERROR_EOF break, streamless, header/spawn failure, panic)
    // counts exactly once; this asserts the counting arithmetic itself.
    #[test]
    fn mux_done_guard_publishes_end_only_after_last_muxer() {
        let remaining = Arc::new(AtomicUsize::new(2));
        let status = Arc::new(AtomicUsize::new(STATUS_RUN));

        let g1 = MuxDoneGuard::new(remaining.clone(), status.clone());
        let g2 = MuxDoneGuard::new(remaining.clone(), status.clone());

        drop(g1);
        assert_eq!(
            status.load(Ordering::Acquire),
            STATUS_RUN,
            "one of two muxers finishing must not terminate the scheduler"
        );

        drop(g2);
        assert_eq!(
            status.load(Ordering::Acquire),
            STATUS_END,
            "the last muxer finishing must publish STATUS_END"
        );
    }

    // A completed muxer must never downgrade an abort already in flight
    // (abort() owns STATUS_ABORT, set_scheduler_error owns STATUS_END; the CAS
    // refuses to overwrite either.)
    #[test]
    fn mux_done_guard_never_downgrades_abort() {
        let remaining = Arc::new(AtomicUsize::new(1));
        let status = Arc::new(AtomicUsize::new(STATUS_ABORT));

        drop(MuxDoneGuard::new(remaining, status.clone()));

        assert_eq!(
            status.load(Ordering::Acquire),
            STATUS_ABORT,
            "the last muxer must not overwrite an abort with STATUS_END"
        );
    }

    // Regression for the write-AVERROR_EOF strand the balancing pass misses: a
    // demuxer parked choked in SchWaiter, scheduler still RUNNING, must be
    // released when the LAST muxer finishes — via STATUS_END from the guard,
    // with no update_locked/unchoke. This is the edge (b) the InputController
    // fallback (edge a) cannot reach when a muxer exits without marking every
    // stream source_finished (e.g. a write returning AVERROR_EOF).
    #[test]
    fn last_mux_done_releases_a_choked_demuxer() {
        use crate::util::sch_waiter::SchWaiter;

        let status = Arc::new(AtomicUsize::new(STATUS_RUN));
        let waiter = Arc::new(SchWaiter::new());
        waiter.set(true); // choked with undelivered tail packets

        let (tx, rx) = mpsc::channel();
        let w = Arc::clone(&waiter);
        let st = Arc::clone(&status);
        std::thread::spawn(move || {
            w.wait_with_scheduler_status(&st, false);
            let _ = tx.send(());
        });

        // Parked while the scheduler runs and no muxer has finished.
        std::thread::sleep(Duration::from_millis(150));
        assert!(
            rx.try_recv().is_err(),
            "the demuxer must stay parked until a terminal status is published"
        );

        // The one (last) muxer exits on a path that never unchoked it: the guard
        // publishes STATUS_END directly.
        let remaining = Arc::new(AtomicUsize::new(1));
        drop(MuxDoneGuard::new(remaining, status.clone()));

        rx.recv_timeout(Duration::from_secs(2))
            .expect("the choked demuxer must be released when the last muxer finishes");
    }

    // build_sq_mux wires the plan into the output-index <-> sq-index maps,
    // leaving a None gap for a non-member (attachment) index.
    #[test]
    fn build_sq_mux_maps_members_with_attachment_gap() {
        // Interleaved members at output indices 0, 1, 3; index 2 is a gap
        // (e.g. an attachment), stream_count = 4.
        let plan = SqMuxPlan {
            buf_size_us: 5_000_000,
            streams: vec![(0, true, None), (1, true, Some(7)), (3, true, None)],
        };
        let sq = build_sq_mux(plan, 4);

        // output_stream_index -> sq_idx: members numbered in plan order; gap None.
        assert_eq!(sq.sq_idx, vec![Some(0), Some(1), None, Some(2)]);
        // sq_idx -> output_stream_index (reverse map used by the cascade).
        assert_eq!(sq.ostream, vec![0, 1, 3]);
    }
}
