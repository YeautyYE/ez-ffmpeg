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
    // delayed output) keep the 1ms poll cadence.
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
    let recv_interval = if needs_polling {
        Duration::from_millis(1)
    } else {
        // Long enough to idle cheaply; short enough to re-check STATUS_END if a
        // stop ever fails to disconnect the source. recv returns immediately
        // when a frame arrives, so active throughput is unaffected.
        Duration::from_millis(100)
    };

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
            sleep(Duration::from_millis(1))
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
    use crate::core::scheduler::ffmpeg_scheduler::{frame_is_null, unref_frame};
    use ffmpeg_next::Frame;
    use ffmpeg_sys_next::{av_frame_alloc, AVMediaType, AVPixelFormat};
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
}
