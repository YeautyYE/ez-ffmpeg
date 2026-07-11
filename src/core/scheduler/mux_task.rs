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
    // Bundle everything whose destruction order is load-bearing into the
    // teardown guard (unblock -> join encoders -> free ctx) right where the
    // ownership leaves the Muxer; every path from here flows through it.
    let (queue_sender, pkt_receiver) = match mux.take_queue() {
        Some((s, r)) => (Some(s), Some(r)),
        None => (None, None),
    };
    let guard = MuxTeardownGuard::new(
        pkt_receiver,
        mux.take_src_pre_recvs(),
        mux.enc_handle_receiver(),
        out_fmt_ctx,
    );
    // Armed for the WHOLE handoff: a panic anywhere between here and the
    // worker prologue (a user log hook inside _mux_init, say) must still
    // release the pre-counted slot or wait()/stop() hangs forever. Every
    // deliberate exit disarms right after its explicit release.
    let slot_guard = MuxSlotGuard::armed(thread_sync.clone(), scheduler_status.clone());
    mux_task_start(
        mux_idx,
        guard,
        slot_guard,
        queue_sender,
        mux.start_time_us,
        mux.recording_time_us,
        mux.stream_count(),
        mux.format_opts.clone(),
        mux.bsf_chains.clone(),
        mux.mux_start_gate(),
        mux.interrupt_state.clone(),
        packet_pool,
        input_controller,
        mux_stream_nodes,
        sq_mux_plan,
        scheduler_status,
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

        // Take sole ownership of the output context out of the Muxer, bundled
        // with the queue receivers and encoder handles into the teardown guard
        // (unblock -> join encoders -> free ctx): every early exit of the
        // waiter below tears down in that order instead of just dropping the
        // context under live encoders. Computed while the output context is
        // still owned (reads stream types).
        let sq_mux_plan = mux.sq_mux_plan();
        let out_fmt_ctx = mux
            .out_fmt_ctx
            .take()
            .expect("ready_to_init_mux called without an output context");
        let mux_stream_nodes = mux.mux_stream_nodes.clone();
        let (queue_sender, pkt_receiver) = match mux.take_queue() {
            Some((s, r)) => (Some(s), Some(r)),
            None => (None, None),
        };
        let guard = MuxTeardownGuard::new(
            pkt_receiver,
            mux.take_src_pre_recvs(),
            mux.enc_handle_receiver(),
            out_fmt_ctx,
        );
        let mux_start_gate = mux.mux_start_gate();
        let interrupt_state = mux.interrupt_state.clone();
        let start_time_us = mux.start_time_us;
        let recording_time_us = mux.recording_time_us;
        let stream_count = mux.stream_count();
        let nb_streams_ready = mux.nb_streams_ready.clone();
        let enc_registered = mux.enc_registered.clone();
        let format_opts = mux.format_opts.clone();
        let bsf_chains = mux.bsf_chains.clone();

        let result = std::thread::Builder::new().name(format!("ready-to-init-muxer{mux_idx}")).spawn(move || {
            // Panic net for the pre-counted thread slot (H4 companion): a
            // waiter panic used to leak the slot AND be swallowed into
            // Ok(()). Declared FIRST so it drops LAST on unwind — after the
            // teardown guard's join/free below. On the all-ready handoff the
            // ARMED guard rides into mux_task_start (no disarm-then-call
            // panic window); the early-exit teardown disarms after its
            // explicit release.
            let mut slot_guard = Some(MuxSlotGuard::armed(
                thread_sync.clone(),
                scheduler_status.clone(),
            ));
            // Owns ctx + receivers until the streams are ready; the ordered
            // teardown (unpark -> join -> free) runs on EVERY early exit via
            // the post-loop block. Handed to mux_task_start on the all-ready
            // branch (leaving `None` here).
            let mut guard = Some(guard);
            // on unwind the explicit wait_enc_registered calls (all-ready /
            // early-exit) are skipped, so `guard`'s Drop would join only the
            // already-queued handles and free the context while start() is still
            // registering encoders — a UAF. Declared AFTER `guard`, this barrier drops
            // BEFORE it on unwind, blocking until registration completes exactly like
            // the normal paths. Panic-free; a no-op once the flag is set.
            let _registration_barrier = MuxRegistrationBarrier {
                enc_registered: enc_registered.clone(),
            };
            // On unwind, records WorkerPanicked and publishes the terminal
            // status BEFORE `guard` (declared before it) joins the encoders —
            // they only exit once a terminal status is visible. No-op on
            // normal exits. Declared AFTER `_registration_barrier` so on unwind it
            // drops FIRST: the terminal status is published, THEN registration is
            // awaited, THEN the guard joins/frees.
            let _panic_status = MuxPanicStatusGuard {
                scheduler_status: scheduler_status.clone(),
                scheduler_result: scheduler_result.clone(),
            };
            // Holds until the start thread finished REGISTERING this muxer's
            // encoders (every enc_init returned, every JoinHandle queued). Any
            // teardown or handoff before that could join an incomplete handle
            // set and free the context under an in-flight encoder. The flag is
            // set on every start()-side path — after the muxer's enc_init loop,
            // or in its failure arm BEFORE fail_start joins this waiter — so
            // this wait is bounded by the registration loop itself.
            let wait_enc_registered = || {
                while !enc_registered.load(Ordering::Acquire) {
                    std::thread::sleep(Duration::from_millis(1));
                }
            };
            let mut queue_sender = queue_sender;
            loop {
                let result = receiver.recv_timeout(Duration::from_millis(100));

                if is_stopping(wait_until_not_paused(&scheduler_status)) {
                    info!("Init muxer receiver end command, finishing.");
                    break;
                }

                if let Err(e) = result {
                    if e == RecvTimeoutError::Disconnected {
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
                    // All-ready implies every encoder produced a packet, which
                    // in practice means registration finished long ago — but
                    // "in practice" is not a proof, and the guard handed over
                    // below can reach fail_mux_init's join within this call.
                    wait_enc_registered();
                    // Move the guard out to the terminal init; the post-loop
                    // teardown then sees `None` and does nothing — the slot is
                    // released by the worker or by fail_mux_init, exactly once.
                    let guard = guard
                        .take()
                        .expect("mux waiter reached all-ready without a context");
                    // Slot ownership transfers WITH THE ARMED GUARD: on
                    // success the worker releases-and-disarms, on failure
                    // mux_task_start's internal paths do — and a panic
                    // anywhere inside still releases via the guard's Drop.
                    let slot_guard = slot_guard
                        .take()
                        .expect("mux waiter reached all-ready without its slot guard");
                    if let Err(e) = mux_task_start(
                        mux_idx,
                        guard,
                        slot_guard,
                        queue_sender.take(),
                        start_time_us,
                        recording_time_us,
                        stream_count,
                        format_opts,
                        bsf_chains,
                        mux_start_gate,
                        interrupt_state,
                        packet_pool,
                        input_controller,
                        mux_stream_nodes,
                        sq_mux_plan,
                        // Clones: the post-loop teardown below still needs the
                        // originals on the all-ready path's break (where the
                        // guard is None and the block is a no-op, but the
                        // borrow checker cannot see that).
                        scheduler_status.clone(),
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
            // Early-exit teardown (stop / disconnect before init): join this
            // muxer's encoders and free the context FIRST, release the
            // pre-counted slot LAST — wait()/stop() must not observe "all
            // threads done" while the context free is still in flight (the
            // free's custom-IO path dereferences the InterruptState a returned
            // stop() lets drop). Another stream's enc_open may be concurrently
            // WRITING (*stream).codecpar when the break fires; the join above
            // is what makes the free safe. thread_done_with still publishes
            // STATUS_END before waking any waiter, preserving the old
            // publish-before-wake contract.
            if let Some(guard) = guard.take() {
                // A stop can fire while the start thread is still running
                // enc_inits for this muxer (an output pipeline publishing an
                // error, a concurrent stop()): joining before registration
                // completed would miss the in-flight handle and free the
                // context under that encoder.
                wait_enc_registered();
                drop(guard);
                // Consuming release: disarm before the (waker-capable) manual
                // release so an unwind here cannot double-release the slot.
                // slot_guard is Some whenever guard was (they transfer together
                // at all-ready); the None arm keeps the old "release anyway"
                // guarantee for the impossible case.
                match slot_guard.take() {
                    Some(slot_guard) => slot_guard.release(),
                    None => thread_sync.thread_done_with(|| {
                        scheduler_status.store(STATUS_END, Ordering::Release);
                    }),
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
    guard: MuxTeardownGuard,
    slot_guard: MuxSlotGuard,
    queue_sender: Option<Sender<PacketBox>>,
    start_time_us: Option<i64>,
    recording_time_us: Option<i64>,
    stream_count: usize,
    format_opts: Option<HashMap<CString, CString>>,
    bsf_chains: StreamBsfChains,
    mux_start_gate: Arc<crate::core::context::MuxStartGate>,
    interrupt_state: Arc<crate::core::context::InterruptState>,
    packet_pool: ObjPool<Packet>,
    input_controller: Arc<InputController>,
    mux_stream_nodes: Vec<Arc<SchNode>>,
    sq_mux_plan: Option<SqMuxPlan>,
    scheduler_status: Arc<AtomicUsize>,
    scheduler_result: Arc<Mutex<Option<crate::error::Result<()>>>>,
    mux_done: MuxDoneGuard,
) -> crate::error::Result<()> {
    let Some(queue_sender) = queue_sender else {
        // Zero-stream output (e.g. an AVFMT_NOSTREAMS muxer): no mux worker
        // thread will be spawned to release this muxer's pre-counted thread
        // slot. Free the context via the guard first (no encoders exist, so
        // the join is empty), THEN release the slot — wait()/stop() must not
        // observe "all threads done" while the free is still in flight. A
        // streamless output is legitimate for such formats, so this is not an
        // error.
        drop(guard);
        // Consuming release: disarm before the (waker-capable) manual release
        // so an unwind cannot double-release the slot.
        slot_guard.release();
        // `mux_done` also drops here — a streamless output still counts
        // toward "all muxers done", so a mix with a real output cannot strand
        // a choked demuxer.
        return Ok(());
    };

    let src_pre_receivers = _mux_init(
        mux_idx,
        guard,
        slot_guard,
        start_time_us,
        recording_time_us,
        stream_count,
        format_opts,
        bsf_chains,
        interrupt_state,
        packet_pool,
        input_controller,
        mux_stream_nodes,
        sq_mux_plan,
        scheduler_status,
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
    guard: MuxTeardownGuard,
    slot_guard: MuxSlotGuard,
    start_time_us: Option<i64>,
    recording_time_us: Option<i64>,
    stream_count: usize,
    format_opts: Option<HashMap<CString, CString>>,
    bsf_chains: StreamBsfChains,
    interrupt_state: Arc<crate::core::context::InterruptState>,
    packet_pool: ObjPool<Packet>,
    input_controller: Arc<InputController>,
    mux_stream_nodes: Vec<Arc<SchNode>>,
    sq_mux_plan: Option<SqMuxPlan>,
    scheduler_status: Arc<AtomicUsize>,
    scheduler_result: Arc<Mutex<Option<crate::error::Result<()>>>>,
    mux_done: MuxDoneGuard,
) -> crate::error::Result<Vec<PreMuxQueueReceiver>> {
    // A panic anywhere below runs BEFORE the guards hand off to the worker at
    // `guard_slot` (the DictGuard alloc, the leftover-option `warn!`, any
    // panicking logger). Rebind the by-value guards into locals in the SAME
    // order the worker frame uses so their reverse-drop order on unwind is
    // load-bearing-correct: _panic_status (record WorkerPanicked + publish the
    // terminal status, so encoders parked on their source recv/sq-drain exit
    // before the join) -> mux_done -> guard (unpark -> join -> free) ->
    // slot_guard (release the pre-counted slot LAST, after the free). Left as
    // PARAMS the drop order would be the reverse of the signature — mux_done
    // (clean publish), then slot_guard (slot release), then guard (free) LAST —
    // releasing the slot before the free (a UAF window wait()/stop() can slip
    // through) and masking the panic behind a clean mux-done. Every path below
    // moves these out (fail_mux_init, the worker handoff slot, the worker
    // closure), so on a NORMAL return they are already gone and _panic_status is
    // a `thread::panicking()` no-op.
    let slot_guard = slot_guard;
    let mut guard = guard;
    let mux_done = mux_done;
    let _panic_status = MuxPanicStatusGuard {
        scheduler_status: scheduler_status.clone(),
        scheduler_result: scheduler_result.clone(),
    };

    // The teardown guard owns the output context; on every failure return it
    // moves into fail_mux_init, whose drop joins this muxer's encoders BEFORE
    // avformat_free_context frees the AVStreams they dereference. On success
    // the pre-mux receivers are handed back to the caller (the gate drain) and
    // the rest of the guard moves into the worker. `as_ptr()` borrows the
    // live context for the FFI calls below.
    // SAFETY: as_ptr yields the live output context pointer.
    let out_fmt_ctx_ptr = unsafe { guard.ctx().as_ptr() };

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
                // Publish the error, join this muxer's encoders, free the
                // context, release the slot — in that order (the delayed-start
                // encoders are live and parked on the pre-mux queues here; the
                // `tests/bsf.rs` nonexistent-BSF case hits this arm every run).
                fail_mux_init(
                    &scheduler_status,
                    &scheduler_result,
                    guard,
                    slot_guard,
                    Muxing(MuxingOperationError::BitstreamFilterInit(
                        name.clone(),
                        MuxingError::from(bsf_ret),
                    )),
                );
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
            guard,
            slot_guard,
            Muxing(MuxingOperationError::WriteHeader(WriteHeaderError::from(
                ret,
            ))),
        );
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
    let scheduler_result_spawn = scheduler_result.clone();

    // Hand the pre-mux receivers back to the caller's gate drain; unpark duty
    // for parked encoders transfers with them. The rest of the guard rides to
    // the worker through a reclaim slot rather than the closure itself: if the
    // spawn FAILS, dropping the closure would run the guard's join while these
    // pre-mux receivers are still open — encoders parked on a full pre-queue
    // would only wake at the 60s backstop. The slot lets the failure branch
    // reclaim the guard and order the teardown correctly.
    // Build the thread name (a panic-capable alloc) BEFORE the guards move into
    // `guard_slot` below: a format!/OOM panic must unwind while `guard` and
    // `slot_guard` are still locals (the established teardown order), not after the
    // handoff where `guard_slot`/`src_pre_receivers` would drop out of order.
    let thread_name = format!("muxer{mux_idx}:{format_name}");
    let src_pre_receivers = guard.take_pre_receivers();
    let guard_slot = Arc::new(Mutex::new((Some(guard), Some(slot_guard))));
    let worker_guard_slot = Arc::clone(&guard_slot);

    let result = std::thread::Builder::new().name(thread_name).spawn(move || {
        // Declaration order is load-bearing for UNWIND (locals drop in reverse
        // order on panic):
        //   pkt_receiver (drops first: unblock senders) -> _panic_status (record
        //   the error BEFORE any terminal status is published) -> mux_done
        //   (publish mux-done terminal) -> guard (join encoders, free ctx) ->
        //   slot_guard (release the thread slot LAST, after the free).
        //
        // Panic-only net for the pre-counted thread slot: the manual
        // `thread_done_with` below is skipped if this worker unwinds, which
        // would leak the slot and hang `wait_for_all_threads`. Handed through
        // the handoff slot (armed since before _mux_init ran, so no panic
        // window exists anywhere along the way); disarmed right after the
        // manual release on the normal path. Taken FIRST so it drops LAST on
        // unwind.
        let slot_guard = worker_guard_slot
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .1
            .take()
            .expect("mux worker started without its slot guard");
        // Ordered-teardown guard (close queue -> join encoders -> free ctx).
        // Dropped explicitly on the normal path below; by unwind on panic.
        let mut guard = worker_guard_slot
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .0
            .take()
            .expect("mux worker started without its teardown guard");
        // This muxer's completion guard. On the NORMAL path it is dropped
        // EXPLICITLY below, BEFORE the encoder join (see that comment): the last
        // muxer must publish STATUS_END before joining so a parked encoder
        // observes is_stopping and exits its recv loop cleanly, rather than being
        // force-fed and erroring on Disconnected. On the early-return /
        // spawn-failure paths (outside this closure) it drops there instead —
        // either way, once. Declared BEFORE `_panic_status` so that on UNWIND it
        // drops AFTER it: the error must be recorded before STATUS_END is
        // published.
        let mux_done = mux_done;
        // Unwind-only: records the panic as the scheduler error and publishes the
        // terminal STATUS_END (its CAS never downgrades an already-published abort) so
        // encoders parked on their sources exit and the guard's join terminates.
        // Declared AFTER `mux_done` (and `guard`) so on unwind it drops BEFORE them —
        // the error is recorded BEFORE `mux_done` publishes STATUS_END, so a status
        // observer never sees END with the error still pending. This matches the proven
        // `_mux_init` order; readers also gate on
        // the thread counter (which every path decrements only after recording its
        // terminal state), so the two together leave no window. No-op unless this
        // worker is panicking.
        let _panic_status = MuxPanicStatusGuard {
            scheduler_status: scheduler_status.clone(),
            scheduler_result: scheduler_result.clone(),
        };
        // Live queue receiver, taken out of the guard for direct recv use.
        // Declared after everything above => drops FIRST on unwind, unblocking
        // encoders parked in send() before the guard's join runs.
        let pkt_receiver = guard
            .take_pkt_receiver()
            .expect("mux worker without a packet queue");
        // Borrow of the guard-owned output context for the FFI calls below;
        // NLL ends this borrow at its last use (the trailer), before the
        // explicit drop(guard) in the teardown block.
        let out_fmt_ctx: &FormatContext = guard.ctx();
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
                    // Local first: a transient filter-out (EAGAIN) is not a
                    // loop outcome. Leaking it into `ret` misrecorded a
                    // healthy stop()/disconnect exit as InterleavedWriteError
                    // AND skipped the trailer-finalize arming (the sq path
                    // already keeps its transient results local).
                    let rret = streamcopy_rescale(
                        packet_box.packet.as_mut_ptr(),
                        packet_data,
                        &start_time_us,
                        &recording_time_us,
                        started,
                    );
                    if rret == AVERROR(EAGAIN) {
                        // The packet was filtered out (before start_time, or a
                        // pre-keyframe streamcopy packet) and is not written;
                        // recycle its pooled shell instead of dropping it, like
                        // the EOF and write paths below (NEW-DP-04).
                        packet_pool.release(packet_box.packet);
                        continue;
                    }
                    ret = rret;
                    if ret == AVERROR_EOF {
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

        // H3: hold the STATUS_END grace open across the trailer AND the
        // output-context free below (the guard lives to closure end), but
        // ONLY when this muxer's write loop ended healthily. A stop() during
        // a faststart moov rewrite used to cut the trailer after 500ms and
        // corrupt the file. After a write ERROR the grace was typically
        // already consumed cutting that write; exempting the trailer then
        // would turn an already-failed output on a dead peer into an
        // unbounded stop() hang (lifecycle's blocked-network test pins this).
        let _finalizing = (ret >= 0 || ret == AVERROR_EOF)
            .then(|| interrupt_state.begin_output_finalize());

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

        // Mark every mux stream finished (idempotent on the normal exit; each
        // stream was already marked as it hit EOF). On an AVERROR_EOF early exit
        // this clears this output's stale `source_finished` so the balancing
        // pass stops steering to a finished output and starving a live sibling
        // (F2).
        for node in &mux_stream_nodes {
            if let SchNode::MuxStream {
                source_finished, ..
            } = node.as_ref()
            {
                source_finished.store(true, Ordering::Release);
            }
        }

        // Publish the mux-done terminal BEFORE the join (F1). For the LAST muxer
        // this stores STATUS_END, so a parked-upstream encoder (in its source
        // recv, filter starved by a choked demuxer — NOT released by
        // `drop(pkt_receiver)`, which only frees encoders blocked in send_to_mux)
        // observes is_stopping and exits CLEANLY from its recv loop: it never
        // sends, so it never records a spurious Disconnected/MuxerFinished error.
        // Dropping HERE, not at closure end after the join, is the fix — the join
        // would otherwise block on that parked encoder before STATUS_END is ever
        // published.
        drop(mux_done);

        // Rebalance so a finished output stops starving a live sibling (F2). This
        // early-returns once stopping, so for the last muxer STATUS_END above has
        // already released everyone; it matters only for a still-running sibling.
        // Known residual: a sibling with its OWN dedicated input keeps the
        // balancing pass from running the fallback, so a demuxer feeding ONLY
        // this finished output can stay choked until the sibling finishes.
        input_controller.update_locked(&scheduler_status);

        // Ordered teardown: join this muxer's encoders, THEN free the output
        // context (the guard's Drop). Freeing before the slot release below
        // also closes the old window where wait()/stop() could return while
        // the context free was still running under a live InterruptState.
        drop(guard);

        // Consuming release: publishes the terminal state before waking waiters
        // (a woken wait() must observe it) and disarms the panic-only net FIRST,
        // so a panicking async waker in thread_done_with cannot leave the guard
        // armed to double-release on unwind.
        slot_guard.release();
    });
    if let Err(e) = result {
        // Run the ordered teardown BEFORE any panic-capable logging. The worker
        // never ran, so its in-thread guards never released; this helper does it
        // in the load-bearing order and is panic-free, so the `error!` below
        // cannot interrupt it mid-sequence (which would drop guard_slot /
        // src_pre_receivers / _panic_status out of order and hang the join).
        fail_mux_worker_spawn(
            &scheduler_status_spawn,
            &scheduler_result_spawn,
            src_pre_receivers,
            &guard_slot,
        );
        error!("Muxer thread exited with error: {e}");
        return Err(MuxingOperationError::ThreadExited.into());
    }

    Ok(src_pre_receivers)
}

/// Releases a muxer's pre-counted thread slot, publishing STATUS_END if this
/// was the last live thread (mirroring the normal mux-worker exit). Used on
/// every path where the slot was counted at scheduler start but no (further)
/// worker will release it: streamless output, write_header failure, worker
/// spawn failure. Without this the slot leaks and wait()/stop() hangs forever.
pub(crate) fn release_mux_slot(
    scheduler_status: &Arc<AtomicUsize>,
    thread_sync: &ThreadSynchronizer,
) {
    thread_sync.thread_done_with(|| {
        scheduler_status.store(STATUS_END, Ordering::Release);
    });
}

/// Ordered teardown for a FAILED mux-worker spawn: the worker never ran, so its
/// in-thread guards never released. This MUST run before any panic-capable op in
/// the spawn-failure branch — after the guards moved into `guard_slot` and the
/// pre-mux receivers were taken out of the guard, a panic (a logger, an OOM)
/// would otherwise drop `guard_slot` (teardown joins encoders) before
/// `src_pre_receivers` closes (encoders still parked → join hangs) and before the
/// waiter's `_panic_status` publishes. The order here is load-bearing: publish
/// the error first (parked encoders observe a terminal status), close the pre-mux
/// queues (unpark encoders parked on them — those receivers live here now, not in
/// the guard), reclaim the guard+slot, teardown (join + free) via the guard, then
/// release the slot LAST. Panic-free (the consuming release contains its waker),
/// so the caller can log AFTER it. Split out so the ordering is unit-testable.
fn fail_mux_worker_spawn(
    scheduler_status: &Arc<AtomicUsize>,
    scheduler_result: &Arc<Mutex<Option<crate::error::Result<()>>>>,
    src_pre_receivers: Vec<PreMuxQueueReceiver>,
    guard_slot: &Arc<Mutex<(Option<MuxTeardownGuard>, Option<MuxSlotGuard>)>>,
) {
    set_scheduler_error(
        scheduler_status,
        scheduler_result,
        Muxing(MuxingOperationError::ThreadExited),
    );
    drop(src_pre_receivers);
    let (guard, slot_guard) = {
        let mut handoff = guard_slot
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        (
            handoff
                .0
                .take()
                .expect("mux worker spawn failed but the closure consumed the guard"),
            handoff
                .1
                .take()
                .expect("mux worker spawn failed but the closure consumed the slot guard"),
        )
    };
    drop(guard);
    slot_guard.release();
}

/// Records a mux-init failure, runs the ordered teardown (unpark -> join ->
/// free the output context), and ONLY THEN releases this muxer's pre-counted
/// thread slot. The order is load-bearing twice over: the error must be
/// published before the encoders wake (so they classify their Disconnected
/// as a graceful stop, and wait() cannot observe "all threads done" without
/// the error), and the slot must stay held until the context free completed
/// (a woken wait()/stop() drops the InterruptState that the free's custom-IO
/// callbacks dereference).
fn fail_mux_init(
    scheduler_status: &Arc<AtomicUsize>,
    scheduler_result: &Arc<Mutex<Option<crate::error::Result<()>>>>,
    guard: MuxTeardownGuard,
    slot_guard: MuxSlotGuard,
    error: crate::error::Error,
) {
    set_scheduler_error(scheduler_status, scheduler_result, error);
    drop(guard);
    // Consuming release: disarm before the (waker-capable) manual release so an
    // unwind cannot double-release the slot. The guard carries its own clones of
    // thread_sync/scheduler_status (armed at the waiter), equivalent to the
    // by-ref pair passed in.
    slot_guard.release();
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

/// Panic-only release net for the mux worker's pre-counted thread slot.
///
/// The mux worker uses a MANUAL `thread_done_with` (not `ThreadDoneGuard`) so it
/// can publish STATUS_END only AFTER writing the trailer and joining its
/// encoders (the ordered terminal publish). That manual call is skipped if the
/// worker unwinds (panics) partway through, leaking the slot — then
/// `wait_for_all_threads` (`RunningGuard::Drop`) hangs forever. This guard
/// releases the slot on unwind; the normal path calls `disarm()` immediately
/// after its manual release so the slot is freed exactly once.
struct MuxSlotGuard {
    armed: bool,
    thread_sync: ThreadSynchronizer,
    scheduler_status: Arc<AtomicUsize>,
}

impl MuxSlotGuard {
    fn armed(thread_sync: ThreadSynchronizer, scheduler_status: Arc<AtomicUsize>) -> Self {
        Self {
            armed: true,
            thread_sync,
            scheduler_status,
        }
    }

    fn disarm(&mut self) {
        self.armed = false;
    }

    /// Consuming manual release: disarm FIRST, then run the same
    /// `thread_done_with` the armed drop would. The disarm-before-act order is
    /// what makes the normal path panic-safe — `thread_done_with` may fire an
    /// async `Waker::wake()`, and if that unwinds, `self` is already disarmed so
    /// its Drop is a no-op and the slot is never released twice (a re-release
    /// would double-decrement the join counter). Replaces every
    /// `thread_done_with(...)/release_mux_slot(...)` + `disarm()` pair.
    fn release(mut self) {
        self.disarm();
        let status = self.scheduler_status.clone();
        self.thread_sync.thread_done_with(move || {
            status.store(STATUS_END, Ordering::Release);
        });
    }
}

impl Drop for MuxSlotGuard {
    fn drop(&mut self) {
        if !self.armed {
            return;
        }
        // Unwind path: release this muxer's thread slot (and publish STATUS_END
        // best-effort, so choked demuxers exit within one poll) exactly as the
        // skipped manual release would have.
        let status = self.scheduler_status.clone();
        self.thread_sync.thread_done_with(move || {
            status.store(STATUS_END, Ordering::Release);
        });
    }
}

/// Panic-only terminal publisher for the mux worker (C2 companion).
///
/// On a worker PANIC in a multi-output job, `MuxDoneGuard` does not publish
/// STATUS_END (other muxers are still live), yet `MuxTeardownGuard`'s unwind
/// drop joins this muxer's encoders — an encoder parked in its source
/// `recv_timeout` (or the `-shortest` drain wait) only exits on a terminal
/// status, so the join would deadlock and the slot release behind it never
/// runs. This guard, declared AFTER both the teardown guard and `mux_done`,
/// drops BEFORE them on unwind — before the join, and before `mux_done`
/// publishes STATUS_END — and itself publishes the terminal status (a
/// panicking muxer ends the whole job — data loss is already certain). It
/// publishes only when actually unwinding (`std::thread::panicking()`), so
/// the normal path needs no disarm and a normally-finishing muxer of a
/// multi-output job never terminates its siblings.
struct MuxPanicStatusGuard {
    scheduler_status: Arc<AtomicUsize>,
    scheduler_result: Arc<Mutex<Option<crate::error::Result<()>>>>,
}

impl Drop for MuxPanicStatusGuard {
    fn drop(&mut self) {
        if !std::thread::panicking() {
            return;
        }
        // Record the panic as the scheduler error BEFORE publishing the
        // terminal status. wait()/poll() gate the RESULT read on the thread
        // counter reaching zero, and the slot-releasing guard drops AFTER
        // this one — so the error is always settled before any reader can observe
        // completion, and a mid-unwind read never reports Ok(()) over a job this
        // panic just truncated (H4). The status flip itself only wakes paused
        // waiters. set_scheduler_result_only leaves the status alone; the CAS
        // below owns publication and must never downgrade an abort.
        crate::core::scheduler::ffmpeg_scheduler::set_scheduler_result_only(
            &self.scheduler_result,
            crate::error::Error::WorkerPanicked(
                std::thread::current().name().unwrap_or("muxer").to_string(),
            ),
        );
        // Same no-downgrade CAS as MuxDoneGuard: never overwrite an abort.
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
        // Paused workers wait on a condvar, not a poll: wake them so they
        // observe the terminal state and unwind promptly.
        crate::core::scheduler::ffmpeg_scheduler::notify_pause_waiters();
    }
}

/// blocks in `Drop` until the scheduler start thread has finished
/// REGISTERING this muxer's encoders (`enc_registered` set). The delayed-start mux
/// waiter's NORMAL exits call `wait_enc_registered` explicitly before handing off /
/// dropping the teardown guard, but an UNWIND (e.g. a panicking log hook in the
/// waiter loop) skips those calls and drops the teardown guard directly — its
/// `try_recv` join then misses an encoder whose handle start() has not queued yet
/// and frees the `AVFormatContext` under that in-flight encoder (a UAF on its
/// `AVStream`s). Declared AFTER the teardown guard in the waiter, this barrier drops
/// BEFORE it on unwind, restoring the "join is sequenced after the whole enc_init
/// loop" invariant on the panic path too. It is panic-free (an atomic load + sleep);
/// the start side publishes `enc_registered` even on its own unwind, so it can never
/// hang. A no-op on the normal paths (they already waited, so the flag is set).
struct MuxRegistrationBarrier {
    enc_registered: Arc<std::sync::atomic::AtomicBool>,
}

impl Drop for MuxRegistrationBarrier {
    fn drop(&mut self) {
        while !self.enc_registered.load(Ordering::Acquire) {
            std::thread::sleep(Duration::from_millis(1));
        }
    }
}

/// Ordered teardown of one muxer's output side (C2): unblock -> join -> free.
///
/// `avformat_free_context` frees every `AVStream` of the output, and this
/// muxer's encoder threads dereference those streams (`enc_open` writes
/// `(*stream).codecpar`, `encode_frame` reads `(*stream)` fields). Every
/// teardown path — init failure, waiter stop/disconnect, worker exit, panic —
/// must therefore join the encoders BEFORE the context drops (fftools joins
/// encoder tasks before muxer cleanup in `sch_stop`). Instead of hand-writing
/// that ordering at each site, this guard owns exactly the resources whose
/// destruction order is load-bearing and encodes the order in `Drop`:
///
///  1. `pkt_receiver` — the live mux queue receiver (crossbeam bounded, sole
///     receiver: `Muxer::take_queue` moves it out). Dropping it disconnects
///     the queue, so an encoder blocked in `send()` wakes with `SendError`.
///  2. `pre_receivers` — the per-stream pre-mux queues. Their `Drop` sets
///     `closed` + `notify_all`, so a sender parked on a full queue wakes and
///     resolves `Disconnected`.
///  3. `enc_handle_receiver` — this muxer's encoder `JoinHandle`s, queued by
///     `enc_init` on the scheduler start thread synchronously after each
///     spawn. Drained with `try_recv` (NOT `recv`: the Muxer keeps a sender
///     clone alive, so the channel never disconnects and `recv` would hang);
///     every handle is provably queued before any drop site can run — each
///     teardown path is sequenced after the whole `enc_init` loop.
///  4. `out_fmt_ctx` — freed LAST, after every encoder above has exited.
///
/// The guard NEVER touches `ThreadSynchronizer` or `scheduler_status`: slot
/// release and status publication stay with their existing owners, which is
/// what lets it compose with `fail_start`/`mux_handed` without double-release.
struct MuxTeardownGuard {
    pkt_receiver: Option<Receiver<PacketBox>>,
    pre_receivers: Vec<PreMuxQueueReceiver>,
    enc_handle_receiver: Receiver<std::thread::JoinHandle<()>>,
    out_fmt_ctx: Option<FormatContext>,
}

impl MuxTeardownGuard {
    fn new(
        pkt_receiver: Option<Receiver<PacketBox>>,
        pre_receivers: Vec<PreMuxQueueReceiver>,
        enc_handle_receiver: Receiver<std::thread::JoinHandle<()>>,
        out_fmt_ctx: FormatContext,
    ) -> Self {
        Self {
            pkt_receiver,
            pre_receivers,
            enc_handle_receiver,
            out_fmt_ctx: Some(out_fmt_ctx),
        }
    }

    /// Borrows the owned output context (alive until Drop frees it).
    fn ctx(&self) -> &FormatContext {
        self.out_fmt_ctx
            .as_ref()
            .expect("mux teardown guard lost its output context")
    }

    /// Worker only: takes the live-queue receiver to recv on directly. The
    /// worker binds it AFTER the guard in declaration order, so on unwind it
    /// still drops FIRST — unblocking senders before the guard's join.
    fn take_pkt_receiver(&mut self) -> Option<Receiver<PacketBox>> {
        self.pkt_receiver.take()
    }

    /// Success path only (the worker spawned and is consuming): hands the
    /// pre-mux receivers to the gate drain; unpark duty transfers with them.
    fn take_pre_receivers(&mut self) -> Vec<PreMuxQueueReceiver> {
        std::mem::take(&mut self.pre_receivers)
    }

    #[cfg(test)]
    fn for_test() -> Self {
        Self {
            pkt_receiver: None,
            pre_receivers: Vec::new(),
            enc_handle_receiver: crossbeam_channel::unbounded().1,
            out_fmt_ctx: None,
        }
    }
}

impl Drop for MuxTeardownGuard {
    fn drop(&mut self) {
        // (a) UNPARK. Close the live queue first (a blocked crossbeam send()
        // returns SendError the moment the sole receiver drops), then every
        // pre-mux queue (their Drop wakes parked senders into Disconnected).
        // Both must precede the join; their mutual order is irrelevant — an
        // encoder is parked on at most one of them at a time.
        drop(self.pkt_receiver.take());
        self.pre_receivers.clear();

        // (b) JOIN every encoder worker of this muxer. try_recv, NOT recv —
        // see the struct docs. Join errors (a panicked encoder) are swallowed
        // like the pre-existing normal-exit join; panic surfacing is a
        // separate concern (H4).
        while let Ok(handle) = self.enc_handle_receiver.try_recv() {
            let _ = handle.join();
        }

        // (c) FREE the output context last (FormatContext::drop ->
        // avformat_free_context, custom-IO-aware). `None` in tests / after a
        // manual take is a no-op.
        drop(self.out_fmt_ctx.take());
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

    /// `MuxRegistrationBarrier::drop` must BLOCK until `enc_registered` is set,
    /// so the mux waiter's UNWIND path cannot let the teardown guard free the output
    /// context before start() finished registering this muxer's encoders. A barrier
    /// that failed to wait would let `drop()` return with the flag still false.
    #[test]
    fn registration_barrier_waits_for_enc_registered() {
        let flag = Arc::new(std::sync::atomic::AtomicBool::new(false));
        let barrier = MuxRegistrationBarrier {
            enc_registered: flag.clone(),
        };
        let setter_flag = flag.clone();
        let setter = std::thread::spawn(move || {
            std::thread::sleep(Duration::from_millis(100));
            setter_flag.store(true, Ordering::Release);
        });
        // drop() spins until the flag is set; it cannot return earlier, so once it
        // returns the flag is provably published.
        drop(barrier);
        assert!(
            flag.load(Ordering::Acquire),
            "the barrier returned before enc_registered was published"
        );
        setter.join().unwrap();
    }

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
            MuxTeardownGuard::for_test(),
            MuxSlotGuard::armed(thread_sync.clone(), scheduler_status.clone()),
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

    /// the mux WORKER (distinct from the demux/filter `ThreadDoneGuard`
    /// path) declares `mux_done` BEFORE `_panic_status`, so on unwind
    /// `_panic_status` (record the panic error) drops before `mux_done` (publish
    /// STATUS_END) and `slot_guard` (release the pre-counted slot) drops LAST. A
    /// reader gated on the thread counter therefore reads the panic error, never a
    /// mid-unwind Ok(()).
    #[test]
    fn mux_worker_unwind_records_error_before_the_slot_drains() {
        let thread_sync = ThreadSynchronizer::new();
        let scheduler_status = Arc::new(AtomicUsize::new(STATUS_RUN));
        let scheduler_result: Arc<Mutex<Option<crate::error::Result<()>>>> =
            Arc::new(Mutex::new(None));
        let mux_done_remaining = Arc::new(AtomicUsize::new(1)); // last muxer => END

        // The scheduler pre-counts the muxer slot before the worker runs.
        thread_sync.thread_start();

        let outcome = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
            // Same declaration order as the worker closure: slot_guard first
            // (drops LAST), then mux_done, then _panic_status (drops FIRST).
            let _slot_guard = MuxSlotGuard::armed(thread_sync.clone(), scheduler_status.clone());
            let _mux_done = MuxDoneGuard::new(mux_done_remaining.clone(), scheduler_status.clone());
            let _panic_status = MuxPanicStatusGuard {
                scheduler_status: scheduler_status.clone(),
                scheduler_result: scheduler_result.clone(),
            };
            panic!("test-injected mux worker panic");
        }));
        assert!(outcome.is_err(), "the injected panic must unwind");

        // The slot is released only after the error is recorded: a reader gated on
        // the counter (wait()/the async Future) reads the error, not Ok(()).
        let (done_tx, done_rx) = mpsc::channel();
        let sync_clone = thread_sync.clone();
        std::thread::spawn(move || {
            sync_clone.wait_for_all_threads();
            let _ = done_tx.send(());
        });
        assert!(
            done_rx.recv_timeout(Duration::from_secs(5)).is_ok(),
            "mux worker slot leaked on the panic path"
        );
        assert!(
            matches!(&*scheduler_result.lock().unwrap(), Some(Err(_))),
            "the worker panic must be recorded as the job error"
        );
        assert!(is_stopping(scheduler_status.load(Ordering::Acquire)));
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

    // BUG A regression: the mux worker releases its pre-counted thread slot via a
    // MANUAL thread_done_with (not ThreadDoneGuard, so it can publish STATUS_END
    // only after the trailer/join). A panic before that call would leak the slot
    // and hang wait_for_all_threads. MuxSlotGuard is the panic-only net: an ARMED
    // drop (the unwind path) must release the slot.
    #[test]
    fn mux_slot_guard_releases_slot_on_armed_drop() {
        let thread_sync = ThreadSynchronizer::new();
        let status = Arc::new(AtomicUsize::new(STATUS_RUN));
        thread_sync.thread_start(); // the pre-counted mux slot

        // Simulate a worker unwinding before its manual release: armed guard drops.
        drop(MuxSlotGuard::armed(thread_sync.clone(), status.clone()));

        // Slot released -> wait_for_all_threads returns instead of hanging.
        let (tx, rx) = mpsc::channel();
        let sync = thread_sync.clone();
        std::thread::spawn(move || {
            sync.wait_for_all_threads();
            let _ = tx.send(());
        });
        assert!(
            rx.recv_timeout(Duration::from_secs(2)).is_ok(),
            "an armed MuxSlotGuard drop must release the slot (else wait() hangs)"
        );
        assert!(
            is_stopping(status.load(Ordering::Acquire)),
            "the armed drop also publishes a terminal status"
        );
    }

    // The normal path disarms the guard right after its manual thread_done_with;
    // a disarmed drop must be a no-op (no double-release, no spurious publish).
    #[test]
    fn mux_slot_guard_disarmed_drop_is_a_noop() {
        let thread_sync = ThreadSynchronizer::new();
        let status = Arc::new(AtomicUsize::new(STATUS_RUN));
        thread_sync.thread_start();

        let mut guard = MuxSlotGuard::armed(thread_sync.clone(), status.clone());
        guard.disarm();
        drop(guard);

        // Disarmed: the slot is still counted (the guard released nothing).
        let (tx, rx) = mpsc::channel();
        let sync = thread_sync.clone();
        std::thread::spawn(move || {
            sync.wait_for_all_threads();
            let _ = tx.send(());
        });
        assert!(
            rx.recv_timeout(Duration::from_millis(200)).is_err(),
            "a disarmed MuxSlotGuard must NOT release the slot (the manual path owns it)"
        );
        assert_eq!(
            status.load(Ordering::Acquire),
            STATUS_RUN,
            "a disarmed guard must not publish a terminal status"
        );
        // Clean up: release the slot so the spawned waiter can finish.
        thread_sync.thread_done_with(|| {});
    }

    // MuxTeardownGuard's drop order is the C2 contract: closing the pre-mux
    // queues must WAKE a sender parked in wait_for_space (unpark), and the
    // encoder handles must be joined — all before the context free (freeing is
    // exercised by the integration/ASAN lanes; a test guard carries no ctx).
    // A parked "encoder" that is only released by the guard proves both: the
    // join can only complete because the unpark happened first.
    #[test]
    fn teardown_guard_unparks_pre_mux_sender_and_joins_encoder() {
        use crate::core::context::pre_mux_queue::{channel, PreMuxQueueConfig, PreQueueTryPush};
        use crate::core::context::PacketData;
        use ffmpeg_sys_next::AVMediaType::AVMEDIA_TYPE_VIDEO;
        use std::sync::atomic::AtomicBool;

        let (pre_sender, pre_receiver) = channel(PreMuxQueueConfig {
            max_packets: 1,
            data_threshold: 1,
        });

        // Fill the queue to capacity so the next admission genuinely PARKS —
        // an empty queue's wait_for_space returns immediately and would let a
        // guard that joins before closing pass this test.
        let first = PacketBox {
            packet: Packet::empty(),
            packet_data: PacketData {
                dts_est: 0,
                codec_type: AVMEDIA_TYPE_VIDEO,
                output_stream_index: 0,
                is_copy: false,
            },
        };
        assert!(
            matches!(pre_sender.try_push(first), PreQueueTryPush::Sent),
            "the first packet must be admitted (queue below max_packets)"
        );

        let parked = Arc::new(AtomicBool::new(false));
        let joined_cleanly = Arc::new(AtomicBool::new(false));
        let parked_probe = Arc::clone(&parked);
        let joined_probe = Arc::clone(&joined_cleanly);

        // The "encoder": parks on the now-full queue with a long timeout; only
        // the guard closing the queue can release it within the test bound.
        // Fullness needs BOTH limits exceeded (FFmpeg parity: packet cap AND
        // byte threshold), so the waited-for size must overshoot the
        // threshold too — the parked empty packet contributes 0 bytes.
        let handle = std::thread::spawn(move || {
            parked_probe.store(true, Ordering::SeqCst);
            pre_sender.wait_for_space(2, Duration::from_secs(30));
            joined_probe.store(true, Ordering::SeqCst);
        });

        // Queue the handle the way enc_init does, keeping a sender clone alive
        // (the real Muxer does too — this is why the guard must try_recv).
        let (handle_tx, handle_rx) = crossbeam_channel::unbounded();
        handle_tx.send(handle).unwrap();

        // Wait until the encoder reached the park call, give it a beat to
        // enter the condvar wait, then prove it is genuinely blocked.
        let deadline = std::time::Instant::now() + Duration::from_secs(5);
        while !parked.load(Ordering::SeqCst) {
            assert!(std::time::Instant::now() < deadline, "encoder never parked");
            std::thread::yield_now();
        }
        std::thread::sleep(Duration::from_millis(100));
        assert!(
            !joined_cleanly.load(Ordering::SeqCst),
            "the encoder was supposed to be parked on the full queue, \
             but it already ran to completion"
        );

        let guard = MuxTeardownGuard {
            pkt_receiver: None,
            pre_receivers: vec![pre_receiver],
            enc_handle_receiver: handle_rx,
            out_fmt_ctx: None,
        };

        let start = std::time::Instant::now();
        drop(guard);
        let elapsed = start.elapsed();

        assert!(
            joined_cleanly.load(Ordering::SeqCst),
            "the guard's join completed without the encoder having run to completion"
        );
        assert!(
            elapsed < Duration::from_secs(5),
            "the guard's drop took {elapsed:?}: the parked sender was not woken by the \
             pre-mux close (join waited out the 30s park timeout instead)"
        );
    }

    // H4 on the mux path: a panicking mux worker/waiter must record
    // WorkerPanicked (result BEFORE status: the result read is gated on the
    // thread counter, and the error is recorded before the slot-releasing guard
    // drops, so a wait()/poll() that observes completion already sees the error)
    // and must never downgrade an abort.
    #[test]
    fn mux_panic_status_guard_records_and_publishes_on_unwind() {
        let status = Arc::new(AtomicUsize::new(STATUS_RUN));
        let result: Arc<Mutex<Option<crate::error::Result<()>>>> = Arc::new(Mutex::new(None));
        let guard = MuxPanicStatusGuard {
            scheduler_status: status.clone(),
            scheduler_result: result.clone(),
        };
        let handle = std::thread::Builder::new()
            .name("panicky-muxer".to_string())
            .spawn(move || {
                let _guard = guard;
                panic!("test-injected mux panic");
            })
            .unwrap();
        let _ = handle.join();

        assert!(is_stopping(status.load(Ordering::Acquire)));
        let recorded = result.lock().unwrap().take();
        match recorded {
            Some(Err(crate::error::Error::WorkerPanicked(name))) => {
                assert_eq!(name, "panicky-muxer");
            }
            other => panic!("expected WorkerPanicked, got {other:?}"),
        }
    }

    // a panic INSIDE _mux_init (before the guards hand off to the worker)
    // must drop the four guards in the SAME order the worker frame uses —
    // _panic_status (publish the terminal status) BEFORE guard (join encoders)
    // BEFORE slot_guard (release the slot). This mirrors the rebind block at the
    // top of `_mux_init`; keep the two in sync. The multi-output case is the
    // sharp one: MuxDoneGuard does NOT publish (siblings still live), so the
    // terminal status a parked encoder needs to exit can only come from
    // _panic_status. If it dropped AFTER guard, the teardown's join would
    // deadlock on that encoder. This binds the guards in _mux_init's declaration
    // order in a panicking thread and asserts the parked encoder is released by
    // the ordered unwind, WorkerPanicked is recorded (not masked by a sibling's
    // MuxerFinished), and the pre-counted slot is released exactly once.
    #[test]
    fn mux_init_internal_panic_publishes_before_join_and_releases_slot_once() {
        use std::sync::atomic::AtomicBool;

        let status = Arc::new(AtomicUsize::new(STATUS_RUN));
        let result: Arc<Mutex<Option<crate::error::Result<()>>>> = Arc::new(Mutex::new(None));
        let thread_sync = ThreadSynchronizer::new();
        thread_sync.thread_start(); // the pre-counted mux slot

        // A parked "encoder" that only exits once a terminal status is visible
        // (its real recv loop exits on is_stopping). The park is BOUNDED so a
        // BROKEN drop order fails the enc_joined assert after the bound rather
        // than hanging the suite; the correct order releases it in ~ms.
        let enc_status = status.clone();
        let enc_joined = Arc::new(AtomicBool::new(false));
        let enc_joined_probe = enc_joined.clone();
        let enc_handle = std::thread::spawn(move || {
            let deadline = std::time::Instant::now() + Duration::from_secs(10);
            while !is_stopping(enc_status.load(Ordering::Acquire)) {
                if std::time::Instant::now() >= deadline {
                    return;
                }
                std::thread::sleep(Duration::from_millis(2));
            }
            enc_joined_probe.store(true, Ordering::SeqCst);
        });
        // Queue the handle the way enc_init does (a sender clone stays alive, so
        // the teardown guard must try_recv).
        let (handle_tx, handle_rx) = crossbeam_channel::unbounded();
        handle_tx.send(enc_handle).unwrap();

        // Two muxers: this one panics, so MuxDoneGuard (2 -> 1) must NOT publish.
        let remaining = Arc::new(AtomicUsize::new(2));

        let status_t = status.clone();
        let result_t = result.clone();
        let sync_t = thread_sync.clone();
        let panicker = std::thread::Builder::new()
            .name("mux-init-panicker".to_string())
            .spawn(move || {
                // _mux_init's declaration order -> reverse-drop is
                // _panic_status -> mux_done -> guard -> slot_guard.
                let slot_guard = MuxSlotGuard::armed(sync_t.clone(), status_t.clone());
                let guard = MuxTeardownGuard {
                    pkt_receiver: None,
                    pre_receivers: Vec::new(),
                    enc_handle_receiver: handle_rx,
                    out_fmt_ctx: None,
                };
                let mux_done = MuxDoneGuard::new(remaining, status_t.clone());
                let _panic_status = MuxPanicStatusGuard {
                    scheduler_status: status_t.clone(),
                    scheduler_result: result_t.clone(),
                };
                // Silence unused warnings; the drop order is the whole point.
                let _ = (&slot_guard, &guard, &mux_done);
                panic!("test-injected panic inside _mux_init");
            })
            .unwrap();
        let _ = panicker.join();

        // The join completed only because _panic_status published the terminal
        // status BEFORE guard dropped — otherwise this encoder would still be
        // parked and the teardown join would have hung the panicker.
        assert!(
            enc_joined.load(Ordering::SeqCst),
            "the ordered unwind must publish the terminal status before the encoder join"
        );

        // WorkerPanicked recorded (not masked by a clean/sibling mux-done).
        assert!(is_stopping(status.load(Ordering::Acquire)));
        match result.lock().unwrap().take() {
            Some(Err(crate::error::Error::WorkerPanicked(name))) => {
                assert_eq!(name, "mux-init-panicker");
            }
            other => panic!("expected WorkerPanicked, got {other:?}"),
        }

        // Slot released exactly once: wait_for_all_threads returns (not leaked),
        // and the counter did not double-decrement (that would underflow and
        // hang below).
        let (tx, rx) = mpsc::channel();
        let sync = thread_sync.clone();
        std::thread::spawn(move || {
            sync.wait_for_all_threads();
            let _ = tx.send(());
        });
        assert!(
            rx.recv_timeout(Duration::from_secs(5)).is_ok(),
            "the mux slot must be released exactly once on the internal-panic unwind"
        );
    }

    // a FAILED mux-worker spawn runs its ordered teardown via
    // `fail_mux_worker_spawn`. The load-bearing order is: close the pre-mux
    // queues (unpark encoders parked on them — those receivers were taken OUT of
    // the guard into `src_pre_receivers`) BEFORE the guard's join. If the join
    // ran first, an encoder parked on a full pre-mux queue could never exit and
    // the teardown would hang. This parks an "encoder" on a full
    // `src_pre_receivers` queue (the guard's own is empty here, matching the real
    // handoff) and asserts the helper unparks + joins + releases without hanging,
    // records the error, and releases the slot exactly once. Running the helper on
    // a side thread makes a WRONG order fail the timeout rather than hang the suite.
    #[test]
    fn mux_worker_spawn_failure_unparks_and_joins_before_releasing_slot() {
        use crate::core::context::pre_mux_queue::{channel, PreMuxQueueConfig, PreQueueTryPush};
        use crate::core::context::PacketData;
        use ffmpeg_sys_next::AVMediaType::AVMEDIA_TYPE_VIDEO;
        use std::sync::atomic::AtomicBool;

        let thread_sync = ThreadSynchronizer::new();
        let status = Arc::new(AtomicUsize::new(STATUS_RUN));
        let result: Arc<Mutex<Option<crate::error::Result<()>>>> = Arc::new(Mutex::new(None));
        thread_sync.thread_start(); // the pre-counted mux slot

        // A pre-mux queue filled to capacity so the encoder genuinely PARKS.
        let (pre_sender, pre_receiver) = channel(PreMuxQueueConfig {
            max_packets: 1,
            data_threshold: 1,
        });
        let first = PacketBox {
            packet: Packet::empty(),
            packet_data: PacketData {
                dts_est: 0,
                codec_type: AVMEDIA_TYPE_VIDEO,
                output_stream_index: 0,
                is_copy: false,
            },
        };
        assert!(matches!(pre_sender.try_push(first), PreQueueTryPush::Sent));

        let joined = Arc::new(AtomicBool::new(false));
        let joined_probe = Arc::clone(&joined);
        // The "encoder": parks on the full queue; only closing it (the drop of
        // src_pre_receivers inside the helper) releases it within the bound.
        let enc = std::thread::spawn(move || {
            pre_sender.wait_for_space(2, Duration::from_secs(30));
            joined_probe.store(true, Ordering::SeqCst);
        });
        let (handle_tx, handle_rx) = crossbeam_channel::unbounded();
        handle_tx.send(enc).unwrap();

        // The pre-mux receiver lives in src_pre_receivers (taken OUT of the guard,
        // as the real handoff does); the guard's own pre_receivers is empty.
        let src_pre_receivers = vec![pre_receiver];
        let guard = MuxTeardownGuard {
            pkt_receiver: None,
            pre_receivers: Vec::new(),
            enc_handle_receiver: handle_rx,
            out_fmt_ctx: None,
        };
        let guard_slot = Arc::new(Mutex::new((
            Some(guard),
            Some(MuxSlotGuard::armed(thread_sync.clone(), status.clone())),
        )));

        let status_c = status.clone();
        let result_c = result.clone();
        let (done_tx, done_rx) = mpsc::channel();
        std::thread::spawn(move || {
            fail_mux_worker_spawn(&status_c, &result_c, src_pre_receivers, &guard_slot);
            let _ = done_tx.send(());
        });
        assert!(
            done_rx.recv_timeout(Duration::from_secs(5)).is_ok(),
            "fail_mux_worker_spawn hung: the pre-mux close did not unpark the encoder before the join"
        );
        assert!(
            joined.load(Ordering::SeqCst),
            "the encoder must be unparked (pre-mux close) before the guard's join"
        );

        // Error recorded and terminal status published.
        assert!(is_stopping(status.load(Ordering::Acquire)));
        assert!(matches!(&*result.lock().unwrap(), Some(Err(_))));

        // Slot released exactly once: wait_for_all_threads returns, not hangs.
        let (tx, rx) = mpsc::channel();
        let sync = thread_sync.clone();
        std::thread::spawn(move || {
            sync.wait_for_all_threads();
            let _ = tx.send(());
        });
        assert!(
            rx.recv_timeout(Duration::from_secs(5)).is_ok(),
            "the pre-counted slot must be released exactly once on spawn failure"
        );
    }

    #[test]
    fn mux_panic_status_guard_never_downgrades_abort() {
        let status = Arc::new(AtomicUsize::new(STATUS_ABORT));
        let result: Arc<Mutex<Option<crate::error::Result<()>>>> = Arc::new(Mutex::new(None));
        let guard = MuxPanicStatusGuard {
            scheduler_status: status.clone(),
            scheduler_result: result.clone(),
        };
        let _ = std::thread::spawn(move || {
            let _guard = guard;
            panic!("test-injected mux panic under abort");
        })
        .join();

        assert_eq!(
            status.load(Ordering::Acquire),
            STATUS_ABORT,
            "the panic publish must not overwrite an abort"
        );
        assert!(
            result.lock().unwrap().is_some(),
            "the panic is still recorded"
        );
    }

    #[test]
    fn mux_panic_status_guard_is_a_noop_without_a_panic() {
        let status = Arc::new(AtomicUsize::new(STATUS_RUN));
        let result: Arc<Mutex<Option<crate::error::Result<()>>>> = Arc::new(Mutex::new(None));
        drop(MuxPanicStatusGuard {
            scheduler_status: status.clone(),
            scheduler_result: result.clone(),
        });
        assert_eq!(status.load(Ordering::Acquire), STATUS_RUN);
        assert!(result.lock().unwrap().is_none());
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
