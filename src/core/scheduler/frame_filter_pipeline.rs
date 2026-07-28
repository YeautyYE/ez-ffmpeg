use crate::core::context::decoder_stream::DecoderStream;
use crate::core::context::encoder_stream::EncoderStream;
use crate::core::context::obj_pool::ObjPool;
use crate::core::context::{FrameBox, FrameData};
use crate::core::scheduler::type_to_symbol;
use crate::error::Error::{
    FrameFilterFrameDuplicateFailed, FrameFilterInit, FrameFilterProcess, FrameFilterRequest,
    FrameFilterStreamTypeNoMatched, FrameFilterThreadExited, FrameFilterTypeNoMatched,
};
use crate::filter::frame_pipeline::FramePipeline;
use crate::util::thread_synchronizer::{ThreadDoneGuard, ThreadSynchronizer};
use crossbeam_channel::{Receiver, RecvTimeoutError, Sender};
use ffmpeg_next::Frame;
use ffmpeg_sys_next::{av_frame_copy_props, av_frame_ref};
use log::{debug, error, info, warn};
use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};
use std::sync::{Arc, Mutex};
use std::thread::sleep;
use std::time::Duration;

/// Downstream destinations of one pipeline: the frame channel, the
/// filtergraph input index it feeds, and that graph's per-input finished
/// flags (empty when the destination is an encoder).
type FrameSenders = Vec<(Sender<FrameBox>, usize, Arc<[AtomicBool]>)>;

pub(crate) fn input_pipeline_init(
    demux_idx: usize,
    pipeline: FramePipeline,
    decoder_streams: &mut Vec<DecoderStream>,
    frame_pool: ObjPool<Frame>,
    scheduler_status: Arc<AtomicUsize>,
    thread_sync: ThreadSynchronizer,
    scheduler_result: Arc<Mutex<Option<crate::error::Result<()>>>>,
) -> crate::error::Result<()> {
    if pipeline.filters.is_empty() {
        warn!("pipeline filters is empty");
        return Ok(());
    }

    // Match type to find index and linklabel.
    let (stream_index, encoder_frame_receiver, pipeline_frame_senders) =
        match_decoder_stream(&pipeline, decoder_streams)?;

    pipeline_init(
        true,
        demux_idx,
        pipeline,
        stream_index,
        encoder_frame_receiver,
        pipeline_frame_senders,
        frame_pool,
        scheduler_status,
        thread_sync,
        scheduler_result,
    )
}
pub(crate) fn output_pipeline_init(
    mux_idx: usize,
    pipeline: FramePipeline,
    encoder_streams: &mut Vec<EncoderStream>,
    frame_pool: ObjPool<Frame>,
    scheduler_status: Arc<AtomicUsize>,
    thread_sync: ThreadSynchronizer,
    scheduler_result: Arc<Mutex<Option<crate::error::Result<()>>>>,
) -> crate::error::Result<()> {
    if pipeline.filters.is_empty() {
        warn!("pipeline filters is empty");
        return Ok(());
    }

    // Match type to find index and linklabel.
    let (stream_index, encoder_frame_receiver, pipeline_frame_sender) =
        match_encoder_stream(&pipeline, encoder_streams)?;

    pipeline_init(
        false,
        mux_idx,
        pipeline,
        stream_index,
        encoder_frame_receiver,
        vec![(pipeline_frame_sender, usize::MAX, Arc::new([]))],
        frame_pool,
        scheduler_status,
        thread_sync,
        scheduler_result,
    )
}

fn match_decoder_stream(
    pipeline: &FramePipeline,
    decoder_streams: &mut Vec<DecoderStream>,
) -> crate::error::Result<(
    usize,
    Receiver<FrameBox>,
    Vec<(Sender<FrameBox>, usize, Arc<[AtomicBool]>)>,
)> {
    let (stream_index, pipeline_frame_receiver, decoder_frame_senders) = match pipeline.stream_index
    {
        Some(stream_index) => {
            match decoder_streams
                .iter_mut()
                .find(|decoder_stream| decoder_stream.stream_index == stream_index)
            {
                None => {
                    return Err(FrameFilterStreamTypeNoMatched(
                        "Input".to_string(),
                        stream_index,
                        format!("{:?}", pipeline.media_type),
                    ))
                }
                Some(decoder_stream) => {
                    let (pipeline_frame_sender, pipeline_frame_receiver) =
                        crossbeam_channel::bounded(8);
                    let decoder_frame_senders = decoder_stream.replace_dsts(
                        pipeline_frame_sender,
                        usize::MAX,
                        Arc::new([]),
                    );

                    (stream_index, pipeline_frame_receiver, decoder_frame_senders)
                }
            }
        }
        None => match decoder_streams
            .iter_mut()
            .find(|decoder_stream| decoder_stream.codec_type == pipeline.media_type)
        {
            None => {
                return Err(FrameFilterTypeNoMatched(
                    "input".to_string(),
                    format!("{:?}", pipeline.media_type),
                ))
            }
            Some(decoder_stream) => {
                let (pipeline_frame_sender, pipeline_frame_receiver) =
                    crossbeam_channel::bounded(8);
                let decoder_frame_senders =
                    decoder_stream.replace_dsts(pipeline_frame_sender, usize::MAX, Arc::new([]));
                (
                    decoder_stream.stream_index,
                    pipeline_frame_receiver,
                    decoder_frame_senders,
                )
            }
        },
    };
    Ok((stream_index, pipeline_frame_receiver, decoder_frame_senders))
}

fn match_encoder_stream(
    pipeline: &FramePipeline,
    encoder_streams: &mut Vec<EncoderStream>,
) -> crate::error::Result<(usize, Receiver<FrameBox>, Sender<FrameBox>)> {
    let (stream_index, encoder_frame_receiver, pipeline_frame_sender) = match pipeline.stream_index
    {
        Some(stream_index) => {
            match encoder_streams
                .iter_mut()
                .find(|encoder_stream| encoder_stream.stream_index == stream_index)
            {
                None => {
                    return Err(FrameFilterStreamTypeNoMatched(
                        "Output".to_string(),
                        stream_index,
                        format!("{:?}", pipeline.media_type),
                    ))
                }
                Some(encoder_stream) => {
                    let (pipeline_frame_sender, pipeline_frame_receiver) =
                        crossbeam_channel::bounded(8);
                    let encoder_frame_receiver =
                        encoder_stream.replace_src(pipeline_frame_receiver);

                    (stream_index, encoder_frame_receiver, pipeline_frame_sender)
                }
            }
        }
        None => match encoder_streams
            .iter_mut()
            .find(|encoder_stream| encoder_stream.codec_type == pipeline.media_type)
        {
            None => {
                return Err(FrameFilterTypeNoMatched(
                    "output".to_string(),
                    format!("{:?}", pipeline.media_type),
                ))
            }
            Some(encoder_stream) => {
                let (pipeline_frame_sender, pipeline_frame_receiver) =
                    crossbeam_channel::bounded(8);
                let encoder_frame_receiver = encoder_stream.replace_src(pipeline_frame_receiver);

                (
                    encoder_stream.stream_index,
                    encoder_frame_receiver,
                    pipeline_frame_sender,
                )
            }
        },
    };
    Ok((stream_index, encoder_frame_receiver, pipeline_frame_sender))
}

fn pipeline_init(
    is_input: bool,
    demux_mux_idx: usize,
    pipeline: FramePipeline,
    stream_index: usize,
    frame_receiver: Receiver<FrameBox>,
    frame_senders: FrameSenders,
    frame_pool: ObjPool<Frame>,
    scheduler_status: Arc<AtomicUsize>,
    thread_sync: ThreadSynchronizer,
    scheduler_result: Arc<Mutex<Option<crate::error::Result<()>>>>,
) -> crate::error::Result<()> {
    let pipeline_name = if is_input {
        "input-frame-pipeline".to_string()
    } else {
        "output-frame-pipeline".to_string()
    };

    // Slot claimed before spawn; the guard releases it on any exit path.
    thread_sync.thread_start();
    let thread_done_guard = ThreadDoneGuard::adopt(
        thread_sync.clone(),
        scheduler_status.clone(),
        scheduler_result.clone(),
    );

    let result = std::thread::Builder::new()
        .name(format!(
            "{pipeline_name}:{}:{stream_index}:{demux_mux_idx}",
            type_to_symbol(pipeline.media_type),
        ))
        .spawn(move || {
            let _thread_done = thread_done_guard;
            // Move every frame-owning CAPTURE into a body local declared AFTER the
            // guard so it drops BEFORE the guard on EVERY exit path. A closure's
            // captures otherwise drop AFTER its body locals (i.e. after the
            // guard), which would release the thread slot BEFORE the user's
            // `FrameFilter`s are torn down: a filter whose `Drop` panics would
            // then go UNRECORDED (job reports Ok over a failure), and a filter
            // whose `Drop` blocks would let the caller free the scheduler context
            // while this thread is still alive. Dropping the pipeline while the
            // guard is still armed records a Drop panic as the job error and keeps
            // the slot counted until the filters are fully gone.
            //
            // `frame_receiver`/`frame_senders`/`frame_pool` are only MOVED
            // into `run_pipeline` on the SUCCESS path; if `frame_filter_init`
            // returns Err (or it or `pipeline_uninit` panics) the early return would
            // otherwise leave them as captures dropping AFTER the guard — so a frame
            // already buffered in `frame_receiver` carrying a blocking `AVBufferRef`
            // free callback (an upstream pipeline can attach one) would run its
            // teardown after the caller observed completion. Rebinding them here
            // makes them drop before the guard on the init-error path too.
            let mut pipeline = pipeline;
            let frame_receiver = frame_receiver;
            let frame_senders = frame_senders;
            let frame_pool = frame_pool;
            if let Err(e) = frame_filter_init(&mut pipeline) {
                pipeline_uninit(&mut pipeline);
                crate::core::scheduler::ffmpeg_scheduler::set_scheduler_error(
                    &scheduler_status,
                    &scheduler_result,
                    e,
                );
                return;
            }

            if let Err(e) = run_pipeline(
                &mut pipeline,
                frame_receiver,
                frame_senders,
                &frame_pool,
                &scheduler_status,
            ) {
                crate::core::scheduler::ffmpeg_scheduler::set_scheduler_error(
                    &scheduler_status,
                    &scheduler_result,
                    e,
                );
            }

            pipeline_uninit(&mut pipeline);
        });

    if let Err(e) = result {
        error!("Pipeline thread exited with error: {e}");
        return Err(FrameFilterThreadExited);
    }

    Ok(())
}

// Wait interval while some polled filter reports pending output: the
// request_frame sweep is the only way that output gets delivered, so it must
// run at a millisecond cadence. (The OS decides what "1ms" means: Windows
// timer granularity and loaded schedulers stretch it — see
// `poll_wait_interval`'s tests for why the cadence is pinned by VALUE.)
const PENDING_POLL_INTERVAL: Duration = Duration::from_millis(1);
// Wait interval when nothing is pending. Long enough to idle cheaply; short
// enough to re-check STATUS_END if a stop ever fails to disconnect the
// source. recv returns immediately when a frame arrives, so active
// throughput is unaffected. This is also the bound on how long a filter that
// misreports "no pending output" can delay its own frames: the sweep still
// runs on every wake.
const IDLE_RECV_INTERVAL: Duration = Duration::from_millis(100);

/// The wait interval for the pipeline loop's next park, re-derived before
/// every park: the millisecond cadence exactly while some un-capped polled
/// filter reports pending output (only the sweep can deliver it), the long
/// safety-net interval otherwise — an EOF-capped filter is excluded because
/// the sweep skips it anyway, and an all-`Never` pipeline (empty
/// `poll_indices`) always parks long. Unit tests pin this choice by exact
/// interval VALUE: which of the two constants the loop parks on is the
/// portable property, while the wall-clock rate a park interval turns into
/// is up to the OS (Windows timer granularity, macOS coalescing under load).
fn poll_wait_interval(
    pipeline: &FramePipeline,
    poll_indices: &[usize],
    eof_capped: &[bool],
) -> Duration {
    if poll_indices
        .iter()
        .any(|&i| !eof_capped[i] && pipeline.request_frame_pending_at(i))
    {
        PENDING_POLL_INTERVAL
    } else {
        IDLE_RECV_INTERVAL
    }
}

fn run_pipeline(
    pipeline: &mut FramePipeline,
    frame_receiver: Receiver<FrameBox>,
    mut frame_senders: FrameSenders,
    frame_pool: &ObjPool<Frame>,
    scheduler_status: &Arc<AtomicUsize>,
) -> crate::error::Result<()> {
    let mut src_finished_flag = false;
    // True while the most recent frame the source delivered was a props-only
    // flush cue (input-side pipelines: the decoder sends one right before the
    // EOF sentinel). The EOF flush then skips synthesizing a duplicate — a
    // filter that finalizes on the cue should see it once per stream.
    let mut cue_since_last_real = false;

    // PERF-8: only filters that can produce frames on their own need the
    // request_frame poll. When none can — the common case of a pipeline built
    // from passthrough/metadata filters — the loop never sweeps request_frame
    // and blocks on the input channel with a long safety timeout instead of
    // waking ~1000x/sec. Filters that DO produce (generators, the GPU pipeline's
    // delayed output) keep the 1ms poll cadence while they report pending
    // output; a polled filter that reports itself idle
    // (`request_frame_pending` = false) lets the loop fall back to the same
    // long timeout until its next input frame arrives.
    let poll_indices = pipeline.request_frame_indices();
    let needs_polling = !poll_indices.is_empty();
    // Filters whose EOF flush drain hit `EOF_FLUSH_FRAME_CAP`. The regular
    // poll sweep must stop pulling from a capped filter: its next output
    // would reach filters that already consumed their flush cue (and, on the
    // null-sentinel path, trail the EOF marker downstream), breaking the
    // ordered-flush "no real frame after your cue" contract. A new real
    // source frame re-arms the chain (stream_loop: the next segment's frames
    // flow and its end-of-stream cues the chain again).
    let mut eof_capped = vec![false; pipeline.filters.len()];

    loop {
        // is_stopping() covers STATUS_ABORT as well as STATUS_END, so an abort
        // (including abort-from-pause) stops this worker like it stops the
        // decoder/encoder/mux workers — an == STATUS_END check would let an
        // aborted pipeline with a producing filter keep running.
        if crate::core::scheduler::ffmpeg_scheduler::is_stopping(
            crate::core::scheduler::ffmpeg_scheduler::wait_until_not_paused(scheduler_status),
        ) {
            info!("Receiver end command, finishing.");
            return Ok(());
        }

        if !src_finished_flag {
            // Re-derived every pass (see `poll_wait_interval` for the rule).
            // That park cannot swallow a wake: a filter only LEAVES the idle
            // state inside a `filter_frame` call on this very thread, and
            // the frame that triggers that call wakes recv immediately — so
            // there is no moment where output is pending while the loop
            // still holds an idle verdict.
            let recv_interval = poll_wait_interval(pipeline, &poll_indices, &eof_capped);
            let result = frame_receiver.recv_timeout(recv_interval);
            match result {
                Err(e) => {
                    if e == RecvTimeoutError::Disconnected {
                        src_finished_flag = true;
                        debug!("Source[decoder/filtergraph] thread exit.");
                        continue;
                    }
                }
                Ok(frame_box) => {
                    // EOF sentinel: dec_done / close_output push a Frame that
                    // wraps a NULL AVFrame as the end-of-stream marker. It must
                    // NEVER reach user FrameFilter code: ffmpeg_next's Frame
                    // accessors (pts/width/is_key/...) are safe fns that
                    // unconditionally deref as_ptr(), so a filter that reads any
                    // property would hit a null deref (UB/SIGSEGV) at every
                    // stream end. Forward it straight downstream — send_frame
                    // already treats the null sentinel as the EOF signal.
                    //
                    // Before the sentinel moves on, flush the chain: an async
                    // filter (the GPU pipeline) resolves its in-flight frames
                    // only on a props-only flush cue, and an output-side
                    // pipeline gets no such marker from the filtergraph when
                    // frames flowed (close_output synthesizes one only for the
                    // never-got-a-frame init case). Skipping the flush would
                    // hand the encoder EOF first and drop the late frames.
                    if crate::core::scheduler::ffmpeg_scheduler::frame_is_null(&frame_box.frame) {
                        if !cue_since_last_real {
                            // Aborted or not, the sentinel below still goes
                            // out: EOF must reach downstream either way.
                            let _ = flush_pipeline_for_eof(
                                pipeline,
                                &mut frame_senders,
                                frame_pool,
                                &poll_indices,
                                &mut eof_capped,
                                scheduler_status,
                            )?;
                        }
                        send_frame(
                            pipeline,
                            &mut frame_senders,
                            frame_pool,
                            Some(frame_box.frame),
                        )?;
                    } else {
                        // A source flush cue is a props-only marker; a real
                        // frame — refcounted OR non-refcounted (buf[0] null but
                        // data present) — is not, so probe both buf and data.
                        let is_cue =
                            crate::util::ffmpeg_utils::frame_is_eof_marker(&frame_box.frame);
                        if is_cue {
                            // A source-delivered flush cue (the decoder's EOF
                            // timestamp marker). Run the ordered flush protocol
                            // FIRST — every filter drains in chain order — and
                            // only then let the source marker itself traverse
                            // the dry chain below: downstream still needs its
                            // props (the EOF timestamp), and on a drained chain
                            // it passes straight through. An aborted flush
                            // (stop / downstream gone) must not push the
                            // marker through user filters either.
                            // Belt-and-braces terminal re-check after the
                            // flush: `completed` must reflect the very last
                            // callback's aftermath before the marker enters
                            // user code again.
                            let completed = flush_pipeline_for_eof(
                                pipeline,
                                &mut frame_senders,
                                frame_pool,
                                &poll_indices,
                                &mut eof_capped,
                                scheduler_status,
                            )?
                                && !crate::core::scheduler::ffmpeg_scheduler::is_stopping(
                                    scheduler_status.load(std::sync::atomic::Ordering::Acquire),
                                );
                            if !completed {
                                frame_pool.release(frame_box.frame);
                                if frame_senders.is_empty() {
                                    debug!("All frame sender finished, finishing.");
                                    return Ok(());
                                }
                                // Stopping: the loop's top-of-iteration status
                                // check exits on the next pass.
                                continue;
                            }
                            cue_since_last_real = true;
                        } else {
                            cue_since_last_real = false;
                            // A real frame means the stream is live again
                            // (stream_loop segment, late source): a filter
                            // capped during an earlier EOF flush may produce
                            // for the new segment, and its next flush will
                            // cue it again.
                            eof_capped.fill(false);
                        }
                        // filter frame. Skipping is live only on the marker
                        // path: a real frame just cleared `eof_capped`, so
                        // this degenerates to the plain chain traversal. A
                        // capped filter must not see the source marker — it
                        // already consumed its cue during the flush, and a
                        // saturating generator would answer the marker with
                        // one more real frame for the already-cued filters
                        // behind it.
                        match pipeline.run_filters_skipping(&eof_capped, frame_box.frame) {
                            Ok(tmp_frame) => {
                                send_frame(pipeline, &mut frame_senders, frame_pool, tmp_frame)?
                            }
                            Err(e) => {
                                error!(
                                    "Pipeline [index:{}] failed, during filter frame. error: {e}",
                                    pipeline.stream_index.unwrap_or(usize::MAX),
                                );
                                return Err(FrameFilterProcess(e));
                            }
                        };
                    }

                    if frame_senders.is_empty() {
                        debug!("All frame sender finished, finishing.");
                        return Ok(());
                    }
                }
            }
        } else if needs_polling {
            // Between drain sweeps after the source disconnected. This state
            // cannot idle at a 1ms cadence: the first sweep that produces
            // nothing exits the loop below (`src_finished_flag &&
            // !produced_frame`), so the sleep is paid only between sweeps
            // that ARE producing. It stays a sleep at all — including for
            // filters reporting no pending output — so a producer that ends
            // each sweep but yields again on the next (violating the idle
            // contract) is paced between sweeps instead of busy-spinning; a
            // producer that never ends a sweep stays inside the sweep itself
            // (bounded there by the per-iteration stop checks) and never
            // reaches this sleep.
            sleep(PENDING_POLL_INTERVAL)
        } else {
            // Source finished and no filter produces autonomously: nothing left
            // to drain. Returning drops frame_senders, signaling EOF downstream.
            debug!("Source finished and no producing filters, finishing.");
            return Ok(());
        }

        // request frame — only from filters that can produce (PERF-8).
        let mut produced_frame = false;
        for &i in &poll_indices {
            // Capped during an EOF flush: everything it produces now would
            // land after the downstream filters' flush cue (they are already
            // cued and drained), violating the ordered-flush contract. Skip
            // until a real source frame clears the mark.
            if eof_capped[i] {
                continue;
            }
            loop {
                // A saturating MayProduce generator (request_frame always
                // returns Some) would otherwise spin here forever: is_stopping
                // and the empty-senders check live only outside this inner
                // loop, so on stop/abort the downstream encoder exits, every
                // send fails, senders empty out — and the loop keeps producing
                // frames into the void at 100% CPU, never releasing the
                // pipeline thread's slot and hanging stop()/wait(). Re-check
                // both each iteration.
                if crate::core::scheduler::ffmpeg_scheduler::is_stopping(
                    scheduler_status.load(std::sync::atomic::Ordering::Acquire),
                ) {
                    return Ok(());
                }
                if frame_senders.is_empty() {
                    return Ok(());
                }
                let result = pipeline.request_frame(i);
                if let Err(e) = result {
                    error!(
                        "Pipeline [index:{}] failed, during request frame.",
                        pipeline.stream_index.unwrap_or(usize::MAX)
                    );
                    return Err(FrameFilterRequest(e));
                }

                let tmp_frame = result.unwrap();
                if tmp_frame.is_none() {
                    break;
                }
                produced_frame = true;

                match pipeline.run_filters_from(i + 1, tmp_frame.unwrap()) {
                    Ok(tmp_frame) => {
                        send_frame(pipeline, &mut frame_senders, frame_pool, tmp_frame)?
                    }
                    Err(e) => {
                        error!(
                            "Pipeline [index:{}] failed, during filter frame. error: {e}",
                            pipeline.stream_index.unwrap_or(usize::MAX)
                        );
                        return Err(FrameFilterProcess(e));
                    }
                };
            }
        }

        if frame_senders.is_empty() {
            debug!("All frame sender finished, finishing.");
            return Ok(());
        }

        // The source (decoder/filtergraph) has disconnected and this pass
        // drained the filters dry: the EOF frame was already forwarded
        // downstream and nothing more will ever be produced. Exit now.
        // Otherwise the thread spins forever and the scheduler's join-all
        // (`ThreadSynchronizer`, added by the pipeline-correctness rework)
        // never completes -- `frame_senders` only shrinks on a *failed* send,
        // which never happens once there is nothing left to send. Returning
        // drops `frame_senders`, disconnecting any still-live destination as a
        // final EOF signal.
        if src_finished_flag && !produced_frame {
            debug!("Source finished and filters drained, finishing.");
            return Ok(());
        }
    }
}

/// Upper bound on frames one EOF flush drain will forward PER FILTER. The
/// flush exists to release the FINITE backlog a filter holds at end of stream
/// (a handful of in-flight GPU readbacks). A filter whose `request_frame`
/// never returns `None` (a saturating generator) must not hold the EOF
/// sentinel hostage: past the cap the filter is marked capped and the poll
/// sweep stops pulling from it, so its remaining backlog is discarded — it
/// can never leak a real frame to filters that already consumed their flush
/// cue, or trail the EOF sentinel downstream. A capped filter is polled
/// again only after a new real source frame re-arms the chain. Documented in
/// [`FrameFilter::filter_frame`]'s "End of stream" section; keep the two in
/// sync.
///
/// [`FrameFilter::filter_frame`]: crate::filter::frame_filter::FrameFilter::filter_frame
const EOF_FLUSH_FRAME_CAP: usize = 1024;

/// Flushes the filter chain right before the EOF sentinel is forwarded.
///
/// Ordered cascade, one stage per filter: stage `k` hands filter `k` a
/// synthesized props-only cue (a fresh pooled shell — the scheduler-wide
/// marker signature) and then drains it dry through `request_frame`. Because
/// stages run in chain order, by the time a filter receives its cue every
/// filter before it has already drained THROUGH it — no real frame arrives
/// after a filter's cue. Each filter gets its cue exactly once: the cue goes
/// to filter `k` alone (`run_filter_at`), a real frame it releases continues
/// down the chain like any frame, and a passed-back marker is recycled so it
/// cannot cue the filters behind `k` early or out of order. The downstream
/// sequence stays exactly "…real frames, EOF".
/// Returns `Ok(true)` when every stage ran to completion, `Ok(false)` when a
/// gate (stopping scheduler / departed downstream) aborted the flush early —
/// the caller must not push more work through the chain in that case.
fn flush_pipeline_for_eof(
    pipeline: &mut FramePipeline,
    frame_senders: &mut FrameSenders,
    frame_pool: &ObjPool<Frame>,
    poll_indices: &[usize],
    eof_capped: &mut [bool],
    scheduler_status: &Arc<AtomicUsize>,
) -> crate::error::Result<bool> {
    // `eof_capped` is sized to the filter count, so this pairs each stage
    // index with its cap mark.
    for (k, capped) in eof_capped.iter_mut().enumerate() {
        // Per-stage gate, BEFORE the cue enters user code: a stage can run
        // arbitrarily expensive filter work (the GPU filter blocks until
        // every in-flight frame completes), which a stopping scheduler must
        // not pay for, and a fully departed downstream could not receive.
        if crate::core::scheduler::ffmpeg_scheduler::is_stopping(
            scheduler_status.load(std::sync::atomic::Ordering::Acquire),
        ) || frame_senders.is_empty()
        {
            return Ok(false);
        }

        let marker = frame_pool.get()?;
        match pipeline.run_filter_at(k, marker) {
            Ok(out) => {
                if !forward_from(
                    pipeline,
                    frame_senders,
                    frame_pool,
                    k + 1,
                    out,
                    scheduler_status,
                )? {
                    return Ok(false);
                }
            }
            Err(e) => {
                error!(
                    "Pipeline [index:{}] failed, during EOF flush cue. error: {e}",
                    pipeline.stream_index.unwrap_or(usize::MAX),
                );
                return Err(FrameFilterProcess(e));
            }
        }

        if !poll_indices.contains(&k) {
            continue;
        }

        // Drain stage k dry. Per-filter cap: one runaway generator must not
        // eat the drain budget of the filters after it.
        let mut flushed = 0usize;
        loop {
            // Mirror the main drain loop's exit conditions: a stopping
            // scheduler or a fully departed downstream must end the flush.
            if crate::core::scheduler::ffmpeg_scheduler::is_stopping(
                scheduler_status.load(std::sync::atomic::Ordering::Acquire),
            ) {
                return Ok(false);
            }
            if frame_senders.is_empty() {
                return Ok(false);
            }
            if flushed >= EOF_FLUSH_FRAME_CAP {
                warn!(
                    "Pipeline [index:{}] EOF flush hit the {EOF_FLUSH_FRAME_CAP}-frame cap \
                     on filter {k}; its remaining output is discarded",
                    pipeline.stream_index.unwrap_or(usize::MAX),
                );
                // Take the filter out of the regular poll sweep too: the
                // filters after it have consumed their cue by the time this
                // flush returns, so anything more it produces would arrive
                // after their cue (or trail the EOF sentinel downstream).
                // A new real source frame clears the mark.
                *capped = true;
                break;
            }
            let result = pipeline.request_frame(k);
            let tmp_frame = match result {
                Ok(tmp_frame) => tmp_frame,
                Err(e) => {
                    error!(
                        "Pipeline [index:{}] failed, during EOF flush request frame.",
                        pipeline.stream_index.unwrap_or(usize::MAX)
                    );
                    return Err(FrameFilterRequest(e));
                }
            };
            let Some(tmp_frame) = tmp_frame else {
                break;
            };
            flushed += 1;
            if !forward_from(
                pipeline,
                frame_senders,
                frame_pool,
                k + 1,
                Some(tmp_frame),
                scheduler_status,
            )? {
                return Ok(false);
            }
        }
    }
    Ok(true)
}

/// Routes one flush-stage output: props-only shells (a marker echoed back)
/// are recycled on the spot — they must not cue later filters early — while
/// a real frame continues through the rest of the chain and downstream.
///
/// The terminal gate is evaluated FIRST and its verdict is returned from
/// every branch — the callback that produced this output may have run for a
/// while, and a stop that became observable during it must abort the flush
/// no matter what the callback returned (frame, marker, or nothing), so no
/// NEW user code starts afterwards. No gate runs between the suffix filters
/// themselves — one chain traversal is the same indivisible unit it is on
/// the streaming path, and aborting mid-chain would strand a frame a filter
/// already owns.
fn forward_from(
    pipeline: &mut FramePipeline,
    frame_senders: &mut FrameSenders,
    frame_pool: &ObjPool<Frame>,
    next_index: usize,
    out: Option<Frame>,
    scheduler_status: &Arc<AtomicUsize>,
) -> crate::error::Result<bool> {
    let proceed = !crate::core::scheduler::ffmpeg_scheduler::is_stopping(
        scheduler_status.load(std::sync::atomic::Ordering::Acquire),
    ) && !frame_senders.is_empty();
    let Some(frame) = out else {
        return Ok(proceed);
    };
    // SAFETY: pointer probe only; a null pointer is never dereferenced.
    if unsafe { frame.as_ptr().is_null() } {
        return Ok(proceed);
    }
    if crate::util::ffmpeg_utils::frame_is_eof_marker(&frame) {
        frame_pool.release(frame);
        return Ok(proceed);
    }
    if !proceed {
        frame_pool.release(frame);
        return Ok(false);
    }
    match pipeline.run_filters_from(next_index, frame) {
        Ok(out) => forward_flushed(pipeline, frame_senders, frame_pool, out).map(|()| true),
        Err(e) => {
            error!(
                "Pipeline [index:{}] failed, during EOF flush filter frame. error: {e}",
                pipeline.stream_index.unwrap_or(usize::MAX)
            );
            Err(FrameFilterProcess(e))
        }
    }
}

/// Forwards one flushed chain output downstream; props-only shells (the
/// synthesized flush marker resurfacing) go back to the pool instead so the
/// consumer never sees a frame that did not exist before the flush.
fn forward_flushed(
    pipeline: &mut FramePipeline,
    frame_senders: &mut FrameSenders,
    frame_pool: &ObjPool<Frame>,
    out: Option<Frame>,
) -> crate::error::Result<()> {
    let Some(frame) = out else {
        return Ok(());
    };
    // SAFETY: pointer probe only; a null pointer is never dereferenced.
    if unsafe { frame.as_ptr().is_null() } {
        // A null shell echoed back; the real EOF sentinel follows separately.
        return Ok(());
    }
    if crate::util::ffmpeg_utils::frame_is_eof_marker(&frame) {
        frame_pool.release(frame);
        return Ok(());
    }
    send_frame(pipeline, frame_senders, frame_pool, Some(frame))
}

fn send_frame(
    pipeline: &mut FramePipeline,
    frame_senders: &mut FrameSenders,
    frame_pool: &ObjPool<Frame>,
    tmp_frame: Option<Frame>,
) -> crate::error::Result<()> {
    if let Some(frame) = tmp_frame {
        let mut frame_box = FrameBox {
            frame,
            frame_data: FrameData {
                framerate: None,
                bits_per_raw_sample: 0,
                input_stream_width: 0,
                input_stream_height: 0,
                subtitle_header: None,
                fg_input_index: usize::MAX,
                side_data: None,
            },
        };

        let mut finished_senders = Vec::new();
        for (i, (sender, fg_input_index, finished_flag_list)) in frame_senders.iter().enumerate() {
            if !finished_flag_list.is_empty()
                && *fg_input_index < finished_flag_list.len()
                && finished_flag_list[*fg_input_index].load(Ordering::Acquire)
            {
                finished_senders.push(i);
                continue;
            }
            if i < frame_senders.len() - 1 {
                let to_send =
                    if crate::core::scheduler::ffmpeg_scheduler::frame_is_null(&frame_box.frame) {
                        // EOF sentinel (null AVFrame pointer): every destination
                        // gets its own null frame, mirroring dec_done. It must
                        // not be dereferenced below.
                        crate::core::context::null_frame()
                    } else {
                        let mut to_send = frame_pool.get()?;

                        // A real frame — refcounted or non-refcounted — is
                        // ref'd: av_frame_ref allocates and copies a
                        // non-refcounted source, so its data is not lost. Only a
                        // props-only marker forwards props alone.
                        if !crate::util::ffmpeg_utils::frame_is_eof_marker(&frame_box.frame) {
                            // SAFETY: non-marker frame is live for the call.
                            let ret = unsafe {
                                av_frame_ref(to_send.as_mut_ptr(), frame_box.frame.as_ptr())
                            };
                            if ret < 0 {
                                return Err(FrameFilterFrameDuplicateFailed);
                            }
                        } else {
                            // SAFETY: frame is live for the call.
                            let ret = unsafe {
                                av_frame_copy_props(to_send.as_mut_ptr(), frame_box.frame.as_ptr())
                            };
                            if ret < 0 {
                                return Err(FrameFilterFrameDuplicateFailed);
                            }
                        }
                        to_send
                    };
                let mut frame_data = frame_box.frame_data.clone();
                frame_data.fg_input_index = *fg_input_index;
                let frame_box = FrameBox {
                    frame: to_send,
                    frame_data,
                };
                if let Err(_) = sender.send(frame_box) {
                    debug!(
                        "Pipeline [index:{}] send frame failed, destination already finished",
                        pipeline.stream_index.unwrap_or(usize::MAX),
                    );
                    finished_senders.push(i);
                    continue;
                }
            } else {
                frame_box.frame_data.fg_input_index = *fg_input_index;
                if let Err(_) = sender.send(frame_box) {
                    debug!(
                        "Pipeline [index:{}] send frame failed, destination already finished",
                        pipeline.stream_index.unwrap_or(usize::MAX)
                    );
                    finished_senders.push(i);
                }
                break;
            }
        }

        // Indices were collected in ascending order: remove from the back so
        // earlier indices stay valid (forward removal shifts the vector and
        // removes the wrong senders).
        for i in finished_senders.into_iter().rev() {
            frame_senders.remove(i);
        }
    }

    Ok(())
}

fn pipeline_uninit(pipeline: &mut FramePipeline) {
    pipeline.uninit_filters()
}

fn frame_filter_init(pipeline: &mut FramePipeline) -> crate::error::Result<()> {
    if let Err(e) = pipeline.init_filters() {
        return Err(FrameFilterInit(e));
    };
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::core::filter::frame_filter::{FrameFilter, FrameFilterError, RequestFrameMode};
    use crate::core::filter::frame_filter_context::FrameFilterContext;
    use crate::core::scheduler::ffmpeg_scheduler::{
        frame_is_null, unref_frame, STATUS_END, STATUS_RUN,
    };
    use ffmpeg_next::Frame;
    use ffmpeg_sys_next::{av_frame_alloc, av_frame_get_buffer, AVMediaType, AVPixelFormat};
    use std::sync::atomic::AtomicBool;
    use std::sync::Arc;

    fn test_new_frame() -> crate::error::Result<Frame> {
        // SAFETY: av_frame_alloc returns an owned empty frame or null; null-check it
        // (extreme OOM) before wrapping so no null frame is dereferenced later. The
        // empty frame is a valid destination for the av_frame_ref clone in send_frame.
        let f = unsafe { av_frame_alloc() };
        assert!(!f.is_null(), "av_frame_alloc must not fail");
        Ok(unsafe { Frame::wrap(f) })
    }

    // send_frame fans one frame to N destinations: every destination EXCEPT the
    // last gets its own av_frame_ref clone, so a single-destination pipeline never
    // exercises that clone path. For a NON-refcounted source (buf[0] null, pixels
    // in data[0] — what a user FrameFilter can emit), av_frame_ref must allocate
    // owned buffers and COPY the data so the clone is not left empty. This pins the
    // two-destination path and the non-refcounted copy.
    #[test]
    fn send_frame_clones_a_non_refcounted_frame_for_a_second_destination() {
        let (tx0, rx0) = crossbeam_channel::unbounded::<FrameBox>();
        let (tx1, rx1) = crossbeam_channel::unbounded::<FrameBox>();
        let no_flags: Arc<[AtomicBool]> = Arc::from(Vec::<AtomicBool>::new());
        let mut senders: FrameSenders =
            vec![(tx0, 0usize, no_flags.clone()), (tx1, 0usize, no_flags)];

        let mut pipeline = FramePipeline::new(AVMediaType::AVMEDIA_TYPE_VIDEO, Some(0));
        let pool = ObjPool::new(2, test_new_frame, unref_frame, frame_is_null).expect("pool");

        // A non-refcounted 4x4 RGBA frame: buf[0] null, real pixels in data[0].
        // `pixels` is declared first so it outlives the frames below (destination 1
        // aliases it; av_frame_free leaves non-owned data[] untouched).
        let mut pixels = vec![0u8; 4 * 4 * 4];
        pixels[0] = 0xAB;
        let pixels_ptr = pixels.as_mut_ptr();
        let src = unsafe {
            let f = av_frame_alloc();
            assert!(!f.is_null(), "av_frame_alloc must not fail");
            (*f).format = AVPixelFormat::AV_PIX_FMT_RGBA as i32;
            (*f).width = 4;
            (*f).height = 4;
            (*f).data[0] = pixels_ptr;
            (*f).linesize[0] = 4 * 4;
            Frame::wrap(f)
        };

        send_frame(&mut pipeline, &mut senders, &pool, Some(src)).expect("send_frame");

        // Destination 0 got the av_frame_ref clone: a non-refcounted source forces
        // av_frame_ref to allocate owned buffers (buf[0] non-null) and copy the data.
        let got0 = rx0.try_recv().expect("destination 0 must receive a frame");
        unsafe {
            let p = got0.frame.as_ptr();
            assert!(!(*p).buf[0].is_null(), "the clone must own its buffers");
            assert!(!(*p).data[0].is_null());
            assert_eq!(
                *(*p).data[0],
                0xAB,
                "the pixel data must be copied, not lost"
            );
        }
        // Destination 1 got the moved original: still the non-refcounted frame, its
        // data[0] aliasing the caller's pixels and with no owned buffer.
        let got1 = rx1.try_recv().expect("destination 1 must receive a frame");
        unsafe {
            let p = got1.frame.as_ptr();
            assert_eq!(
                (*p).data[0],
                pixels_ptr,
                "destination 1 must get the moved original, still aliasing the caller's pixels"
            );
            assert!(
                (*p).buf[0].is_null(),
                "the moved original stays non-refcounted (no owned buffer)"
            );
        }

        // Drop the frames while `pixels` is still alive (destination 1 aliases it).
        drop(got0);
        drop(got1);
        let _ = &pixels;
    }

    // ---- request_frame_pending poll gating ----
    //
    // The hint controls ONLY the loop's wait interval, never whether the
    // sweep runs: an idle hint must collapse the 1ms cadence to the long
    // park, while input frames, stop, and even a misreported hint must all
    // still make progress within one bounded park.

    /// MayProduce probe (default `request_frame_mode`): `pending` is what
    /// `request_frame_pending` reports, `queued` is how many frames
    /// `request_frame` will still yield — a misreporting filter keeps
    /// `queued > 0` while claiming idle. The counters observe the loop from
    /// the test thread. `hint_checks` counts hint evaluations — the loop's
    /// last act before each park, with no status check in between — and
    /// `stop_on_hint_check` publishes a terminal status from INSIDE the
    /// Nth evaluation: on the worker thread itself, after the loop-top
    /// status check has already passed and before the park is entered, so
    /// the stop can only be observed at the check that follows a full
    /// park. No test-thread timing can move the store outside that window.
    struct PendingHintProbe {
        pending: bool,
        queued: usize,
        polls: Arc<AtomicUsize>,
        filtered: Arc<AtomicUsize>,
        hint_checks: Arc<AtomicUsize>,
        /// `(n, status)`: store `STATUS_END` into `status` during the nth
        /// hint evaluation (1-based).
        stop_on_hint_check: Option<(usize, Arc<AtomicUsize>)>,
    }

    impl FrameFilter for PendingHintProbe {
        fn media_type(&self) -> AVMediaType {
            AVMediaType::AVMEDIA_TYPE_VIDEO
        }

        fn filter_frame(
            &mut self,
            frame: Frame,
            _ctx: &mut FrameFilterContext,
        ) -> Result<Option<Frame>, FrameFilterError> {
            self.filtered.fetch_add(1, Ordering::SeqCst);
            Ok(Some(frame))
        }

        fn request_frame(
            &mut self,
            _ctx: &mut FrameFilterContext,
        ) -> Result<Option<Frame>, FrameFilterError> {
            self.polls.fetch_add(1, Ordering::SeqCst);
            if self.queued == 0 {
                return Ok(None);
            }
            self.queued -= 1;
            Ok(Some(test_new_frame().expect("test frame alloc")))
        }

        fn request_frame_pending(&self) -> bool {
            let n = self.hint_checks.fetch_add(1, Ordering::SeqCst) + 1;
            if let Some((fire_on, status)) = &self.stop_on_hint_check {
                if n == *fire_on {
                    status.store(STATUS_END, Ordering::Release);
                }
            }
            self.pending
        }
    }

    /// A real (non-marker) frame: owned data buffers via av_frame_get_buffer,
    /// so buf[0] is non-null and the loop takes the ordinary frame path, and
    /// no external pixel storage needs to outlive the pipeline thread.
    fn test_real_frame() -> Frame {
        unsafe {
            // SAFETY: the frame is null-checked after allocation and the
            // buffer allocation's return code is checked before the frame is
            // handed out; GRAY8 16x16 keeps it a single tiny plane.
            let f = av_frame_alloc();
            assert!(!f.is_null(), "av_frame_alloc must not fail");
            (*f).format = AVPixelFormat::AV_PIX_FMT_GRAY8 as i32;
            (*f).width = 16;
            (*f).height = 16;
            let ret = av_frame_get_buffer(f, 0);
            assert!(ret >= 0, "av_frame_get_buffer failed: {ret}");
            Frame::wrap(f)
        }
    }

    fn test_frame_box(frame: Frame) -> FrameBox {
        FrameBox {
            frame,
            frame_data: FrameData {
                framerate: None,
                bits_per_raw_sample: 0,
                input_stream_width: 0,
                input_stream_height: 0,
                subtitle_header: None,
                fg_input_index: usize::MAX,
                side_data: None,
            },
        }
    }

    struct PollLoopHarness {
        /// `None` once closed (by `finish` or the unwind-safe drop); the
        /// disconnect is the worker's prompt wake.
        src_tx: Option<Sender<FrameBox>>,
        dst_rx: Receiver<FrameBox>,
        status: Arc<AtomicUsize>,
        polls: Arc<AtomicUsize>,
        filtered: Arc<AtomicUsize>,
        /// Bounded join surrogate: carries `run_pipeline`'s result when the
        /// worker thread exits.
        done_rx: Receiver<crate::error::Result<()>>,
        /// Joined on drop, so an assertion failure mid-test still tears the
        /// worker down instead of leaking it past the test.
        worker: Option<std::thread::JoinHandle<()>>,
    }

    impl PollLoopHarness {
        /// Blocks until the worker's first `request_frame` sweep, proving
        /// the loop is running before a timing window opens, and returns
        /// the poll count at that instant as the window's baseline. Without
        /// this, a slow spawn could let a cadence ceiling pass vacuously
        /// (no loop, no polls) or a cadence floor fail spuriously.
        fn await_first_sweep(&self) -> usize {
            let deadline = std::time::Instant::now() + Duration::from_secs(2);
            loop {
                let seen = self.polls.load(Ordering::SeqCst);
                if seen > 0 {
                    return seen;
                }
                assert!(
                    std::time::Instant::now() < deadline,
                    "the pipeline worker never reached its first sweep"
                );
                std::thread::sleep(Duration::from_millis(1));
            }
        }
    }

    impl Drop for PollLoopHarness {
        /// Unwind-safe teardown: publish stop, disconnect the source (the
        /// prompt wake), then JOIN the worker — every exit from a test,
        /// including a failed assertion, leaves no detached pipeline thread
        /// behind. A regression that keeps the worker alive turns into a
        /// hang of the named test rather than a silent leak.
        fn drop(&mut self) {
            self.status.store(STATUS_END, Ordering::Release);
            self.src_tx = None;
            if let Some(worker) = self.worker.take() {
                let _ = worker.join();
            }
        }
    }

    /// Runs `run_pipeline` on a worker thread against a single
    /// `PendingHintProbe`, with the source channel held open by the test.
    fn spawn_poll_loop(pending: bool, queued: usize) -> PollLoopHarness {
        spawn_poll_loop_with(pending, queued, None)
    }

    /// `spawn_poll_loop`, with an optional worker-side stop trigger: when
    /// `stop_on_hint_check` is `Some(n)`, the probe publishes `STATUS_END`
    /// from inside its nth hint evaluation (see `PendingHintProbe`).
    fn spawn_poll_loop_with(
        pending: bool,
        queued: usize,
        stop_on_hint_check: Option<usize>,
    ) -> PollLoopHarness {
        let (src_tx, src_rx) = crossbeam_channel::bounded::<FrameBox>(8);
        let (dst_tx, dst_rx) = crossbeam_channel::unbounded::<FrameBox>();
        let (done_tx, done_rx) = crossbeam_channel::bounded(1);
        let no_flags: Arc<[AtomicBool]> = Arc::from(Vec::<AtomicBool>::new());
        let senders: FrameSenders = vec![(dst_tx, usize::MAX, no_flags)];
        let polls = Arc::new(AtomicUsize::new(0));
        let filtered = Arc::new(AtomicUsize::new(0));
        let status = Arc::new(AtomicUsize::new(STATUS_RUN));

        let mut pipeline = FramePipeline::new(AVMediaType::AVMEDIA_TYPE_VIDEO, Some(0));
        pipeline.add_filter(
            "pending-hint-probe",
            Box::new(PendingHintProbe {
                pending,
                queued,
                polls: polls.clone(),
                filtered: filtered.clone(),
                hint_checks: Arc::new(AtomicUsize::new(0)),
                stop_on_hint_check: stop_on_hint_check.map(|n| (n, status.clone())),
            }),
        );
        let pool = ObjPool::new(2, test_new_frame, unref_frame, frame_is_null).expect("pool");
        let status_for_thread = status.clone();
        let worker = std::thread::spawn(move || {
            let mut pipeline = pipeline;
            let result = run_pipeline(&mut pipeline, src_rx, senders, &pool, &status_for_thread);
            let _ = done_tx.send(result);
        });

        PollLoopHarness {
            src_tx: Some(src_tx),
            dst_rx,
            status,
            polls,
            filtered,
            done_rx,
            worker: Some(worker),
        }
    }

    fn finish(mut h: PollLoopHarness) {
        h.status.store(STATUS_END, Ordering::Release);
        h.src_tx = None;
        h.done_rx
            .recv_timeout(Duration::from_secs(5))
            .expect("pipeline thread must exit after stop")
            .expect("run_pipeline must exit cleanly");
        // Dropping `h` joins the already-exited worker.
    }

    // An idle-hint MayProduce pipeline must park at the long interval (a
    // handful of sweeps in the window; the old fixed 1ms cadence would show
    // hundreds) while an arriving frame still flows through promptly.
    #[test]
    fn idle_pending_hint_parks_the_poll_sweep_without_delaying_input() {
        let h = spawn_poll_loop(false, 0);

        // Idle window, measured from a proven-running loop: the source
        // stays connected but sends nothing. The lower bound proves the
        // parked loop keeps sweeping on every park expiry. The ceiling —
        // 20 sweeps per 400ms, 5x the true ~4-per-window park rate for
        // jitter margin — catches a loop wrongly polling at the
        // millisecond cadence wherever the OS delivers that cadence
        // faster than the ceiling (CI observed ~25-400 per
        // 400ms-equivalent on Linux and Windows; one loaded macOS runner
        // stretched it to ~12, under the ceiling, where the wall clock
        // cannot discriminate). The interval CHOICE itself is pinned
        // platform-independently by
        // `wait_interval_derivation_pins_the_exact_intervals`.
        let baseline = h.await_first_sweep();
        std::thread::sleep(Duration::from_millis(400));
        let idle_polls = h.polls.load(Ordering::SeqCst) - baseline;
        assert!(
            (1..=20).contains(&idle_polls),
            "an idle MayProduce pipeline must park between safety-net sweeps: \
             {idle_polls} request_frame calls in 400ms"
        );

        // The park gates only the sweep cadence, never input delivery: the
        // channel send wakes the parked recv (crossbeam send/recv
        // semantics — the mechanism the loop was already relying on for
        // disconnects). The assertion pins delivery liveness; sub-interval
        // latency is deliberately not timed, to stay robust on a loaded
        // runner.
        h.src_tx
            .as_ref()
            .expect("source still open")
            .send(test_frame_box(test_real_frame()))
            .expect("send to live pipeline");
        let got = h
            .dst_rx
            .recv_timeout(Duration::from_secs(2))
            .expect("a frame sent to a parked pipeline must still be delivered");
        assert_eq!(
            h.filtered.load(Ordering::SeqCst),
            1,
            "the frame must have traversed the filter"
        );
        drop(got);

        finish(h);
    }

    // With the source channel deliberately HELD OPEN (no disconnect wake —
    // the pathological stop path the idle interval is sized for), a parked
    // pipeline must still observe the terminal status within one bounded
    // park; the generous deadline covers scheduler jitter over the 100ms
    // design bound. The stop is published by the WORKER ITSELF, from
    // inside its second hint evaluation — after that iteration's loop-top
    // status check has passed and before its park is entered — so no
    // test-thread timing can slip the store outside the window: the exit
    // necessarily observes it at the check that follows a full park. The
    // second evaluation (not the first) also proves one ordinary
    // park-and-sweep cycle ran beforehand.
    #[test]
    fn parked_idle_pipeline_observes_stop_within_the_bounded_park() {
        let h = spawn_poll_loop_with(false, 0, Some(2));

        h.done_rx
            .recv_timeout(Duration::from_secs(2))
            .expect(
                "a parked pipeline must observe stop within the bounded idle \
                 park interval",
            )
            .expect("run_pipeline must exit cleanly on stop");
    }

    // A filter that misreports "no pending output" while holding frames only
    // request_frame can release: no input ever arrives, so the safety-net
    // sweep that runs on every park expiry is the only progress mechanism —
    // the frames must all surface, never be lost, and the loop must never
    // deadlock. What is pinned is drained-not-lost (the public misreport
    // guarantee); the per-frame one-interval latency is deliberately not
    // timed — a wall-clock assertion there buys no coverage the poll counts
    // in the neighboring tests do not already give, at real flake cost.
    #[test]
    fn false_idle_claim_still_drains_via_the_bounded_park() {
        let h = spawn_poll_loop(false, 3);
        h.await_first_sweep();

        for i in 0..3usize {
            h.dst_rx
                .recv_timeout(Duration::from_secs(5))
                .unwrap_or_else(|_| {
                    panic!("misreported pending output was never drained (frame {i})")
                });
        }

        finish(h);
    }

    // The flip side of the idle park: a filter reporting pending output must
    // keep the millisecond drain cadence — delayed asynchronous results (the
    // GPU pipeline's readbacks) rely on it for prompt delivery. Measured
    // from a proven-running loop. What "1ms" turns into on a wall clock is
    // the OS's call, and CI showed the full spread: ~600 sweeps per 600ms
    // on Linux, 37 under Windows arm64 timer granularity, down to 18 on a
    // loaded macOS runner — so no portable floor can also sit above the
    // idle test's ceiling. This floor therefore discriminates only against
    // the one reachable regression, a pipeline parking at the idle
    // interval despite pending output (~6 sweeps per 600ms; 12 is 2x
    // that); which of the two interval VALUES the loop parks on — the
    // property a rate can only proxy — is pinned platform-independently by
    // `wait_interval_derivation_pins_the_exact_intervals`.
    #[test]
    fn pending_hint_keeps_the_millisecond_drain_cadence() {
        let h = spawn_poll_loop(true, 0);

        let baseline = h.await_first_sweep();
        std::thread::sleep(Duration::from_millis(600));
        let polls = h.polls.load(Ordering::SeqCst) - baseline;
        assert!(
            polls >= 12,
            "a pending-output pipeline is parking at the idle interval: \
             only {polls} request_frame calls in 600ms"
        );

        finish(h);
    }

    /// A `RequestFrameMode::Never` filter for the derivation truth table:
    /// excluded from `poll_indices`, so the wait derivation must never ask
    /// it for pending state — asking IS a derivation bug (an
    /// iterate-all-filters form instead of the polled set), hence the
    /// panic rather than a return value.
    struct NeverProbe;

    impl FrameFilter for NeverProbe {
        fn media_type(&self) -> AVMediaType {
            AVMediaType::AVMEDIA_TYPE_VIDEO
        }

        fn filter_frame(
            &mut self,
            frame: Frame,
            _ctx: &mut FrameFilterContext,
        ) -> Result<Option<Frame>, FrameFilterError> {
            Ok(Some(frame))
        }

        fn request_frame_mode(&self) -> RequestFrameMode {
            RequestFrameMode::Never
        }

        fn request_frame_pending(&self) -> bool {
            panic!("the wait derivation must not query a Never filter's pending state")
        }
    }

    // The interval choice itself, pinned by exact VALUE — the portable half
    // of the cadence contract (the wall-clock tests above own the live-loop
    // half). The rule: the millisecond interval iff some polled filter is
    // BOTH pending and un-capped; anything else — idle, capped, or nothing
    // to poll — parks at the safety-net interval. Pinned as the complete
    // truth table over one to three filters — polled and `Never` mixed —
    // plus the empty set.
    #[test]
    fn wait_interval_derivation_pins_the_exact_intervals() {
        let counters = || {
            (
                Arc::new(AtomicUsize::new(0)),
                Arc::new(AtomicUsize::new(0)),
                Arc::new(AtomicUsize::new(0)),
            )
        };
        // Each probe hands back its hint-query counter so the sweep can
        // assert WHICH filters were consulted, not just the returned
        // interval.
        let probe = |pending: bool| {
            let (polls, filtered, hint_checks) = counters();
            let queries = hint_checks.clone();
            (
                PendingHintProbe {
                    pending,
                    queued: 0,
                    polls,
                    filtered,
                    hint_checks,
                    stop_on_hint_check: None,
                },
                queries,
            )
        };

        // The contract values themselves, as literals: the branch
        // assertions below reuse the constants, so this pin — not they —
        // is what makes an accidental interval edit fail the test.
        assert_eq!(PENDING_POLL_INTERVAL, Duration::from_millis(1));
        assert_eq!(IDLE_RECV_INTERVAL, Duration::from_millis(100));

        // The full truth table, one to three filters, each independently
        // in one of six states: {`Never`, polled-idle, polled-pending} x
        // {un-capped, capped}. `eof_capped` spans ALL filters (mirroring
        // the production vector, which is sized to the filter count) while
        // only `MayProduce` filters enter `poll_indices` — so the table
        // exercises SPARSE poll sets, where reading the cap vector by
        // slot position diverges from the correct filter-index read. The
        // oracle re-derives the expectation from the CONSTRUCTION inputs:
        // the millisecond interval iff some polled filter is both pending
        // and un-capped. Any derivation differing anywhere on this space —
        // cross-filter `any`/`all` mixes, inverted polarity, slot-indexed
        // cap reads, folds that overwrite instead of accumulate — fails
        // on its distinguishing row, and one that queries a `Never`
        // filter's pending state trips that probe's panic. Capped polled
        // filters are additionally asserted to receive ZERO pending
        // queries — the documented exclusion pinned as a side effect,
        // since an operand-order swap in the conjunction returns
        // identical intervals everywhere yet consults capped filters.
        for n in 1..=3u32 {
            for combo in 0..6u32.pow(n) {
                let mut pipeline = FramePipeline::new(AVMediaType::AVMEDIA_TYPE_VIDEO, Some(0));
                let mut eof_capped = Vec::new();
                let mut expected_polled = Vec::new();
                let mut hint_queries = Vec::new();
                let mut some_pending_uncapped = false;
                let mut desc = Vec::new();
                let mut rest = combo;
                for i in 0..n as usize {
                    let d = rest % 6;
                    rest /= 6;
                    let never = d < 2;
                    let pending = d >= 4;
                    let capped = d % 2 == 1;
                    if never {
                        pipeline.add_filter(format!("never{i}"), Box::new(NeverProbe));
                        hint_queries.push(None);
                    } else {
                        let (filter, queries) = probe(pending);
                        pipeline.add_filter(format!("probe{i}"), Box::new(filter));
                        expected_polled.push(i);
                        hint_queries.push(Some(queries));
                    }
                    eof_capped.push(capped);
                    some_pending_uncapped |= !never && pending && !capped;
                    desc.push(match (never, pending, capped) {
                        (true, _, false) => "never",
                        (true, _, true) => "never+capped",
                        (false, false, false) => "idle",
                        (false, false, true) => "idle+capped",
                        (false, true, false) => "pending",
                        (false, true, true) => "pending+capped",
                    });
                }
                let poll_indices = pipeline.request_frame_indices();
                assert_eq!(
                    poll_indices, expected_polled,
                    "only MayProduce filters may be polled: {desc:?}"
                );
                let expected = if some_pending_uncapped {
                    PENDING_POLL_INTERVAL
                } else {
                    IDLE_RECV_INTERVAL
                };
                assert_eq!(
                    poll_wait_interval(&pipeline, &poll_indices, &eof_capped),
                    expected,
                    "filter states {desc:?}"
                );
                for (i, queries) in hint_queries.iter().enumerate() {
                    match queries {
                        Some(queries) if eof_capped[i] => assert_eq!(
                            queries.load(Ordering::SeqCst),
                            0,
                            "a capped filter's pending state must not be queried: \
                             filter {i} in {desc:?}"
                        ),
                        _ => {}
                    }
                }
            }
        }

        // No polled filters at all (all-`Never`) → always the long park.
        let never = FramePipeline::new(AVMediaType::AVMEDIA_TYPE_VIDEO, Some(0));
        let never_indices = never.request_frame_indices();
        assert!(never_indices.is_empty());
        assert_eq!(
            poll_wait_interval(&never, &never_indices, &[]),
            IDLE_RECV_INTERVAL,
            "a pipeline with nothing to poll must park long"
        );
    }
}
