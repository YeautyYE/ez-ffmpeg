use crate::core::context::encoder_stream::EncSyncHandle;
use crate::core::context::ffmpeg_context::FfmpegContext;
use crate::core::context::obj_pool::ObjPool;
use crate::core::scheduler::dec_task::dec_init;
use crate::core::scheduler::demux_task::demux_init;
use crate::core::scheduler::enc_task::enc_init;
use crate::core::scheduler::filter_task::filter_graph_init;
use crate::core::scheduler::frame_filter_pipeline::{input_pipeline_init, output_pipeline_init};
use crate::core::scheduler::input_controller::InputController;
use crate::core::scheduler::mux_task::{mux_init, ready_to_init_mux};
use crate::core::scheduler::sync_queue::SyncQueue;
use crate::error::{AllocFrameError, AllocPacketError};
use crate::util::thread_synchronizer::ThreadSynchronizer;
use ffmpeg_next::packet::{Mut, Ref};
use ffmpeg_next::{Frame, Packet};
use ffmpeg_sys_next::AVMediaType::{AVMEDIA_TYPE_AUDIO, AVMEDIA_TYPE_VIDEO};
use ffmpeg_sys_next::{av_frame_alloc, av_frame_unref, av_packet_unref};
use std::marker::PhantomData;
use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};
use std::sync::{Arc, Condvar, Mutex, OnceLock};
use std::time::Duration;

pub struct Initialization;
pub struct Running;
pub struct Paused;
pub struct Ended;

// Internal wrapper for Running state to enable Drop implementation.
//
// This guard ensures that when a FfmpegScheduler<Running> (or Paused) is dropped,
// all worker threads are properly terminated and output files are finalized.
//
// IMPORTANT: Drop only runs when the scheduler goes out of scope normally.
// If the process exits abruptly (e.g., via std::process::exit() or panic in main),
// Drop will NOT run and files may be corrupted.
//
// The guard is passed through state transitions (Running -> Paused -> Running)
// via into_state() to maintain Drop protection across all active states.
struct RunningGuard {
    status: Arc<AtomicUsize>,
    thread_sync: ThreadSynchronizer,
    // Demuxer choke waiters: a stopping scheduler must notify them or a
    // choked demuxer sleeps through the shutdown and joining hangs.
    demux_waiters: Vec<Arc<crate::util::sch_waiter::SchWaiter>>,
    // Keeps the interrupt-callback state alive until every worker thread has
    // exited: FfmpegScheduler's ffmpeg_context field (the other Arc holder)
    // drops BEFORE this guard, while threads may still be inside blocking
    // I/O whose AVIOInterruptCB points into this allocation.
    _interrupt_state: Arc<crate::core::context::InterruptState>,
}

impl Drop for RunningGuard {
    /// Ensures graceful shutdown when scheduler is dropped.
    /// Blocks until all threads complete to prevent data corruption.
    fn drop(&mut self) {
        // Always ensure STATUS_END is set
        if !is_stopping(self.status.load(Ordering::Acquire)) {
            log::debug!("Drop called, setting STATUS_END");
            self.status.store(STATUS_END, Ordering::Release);
        }

        // Wake any paused workers so they observe the terminal state (conc-06).
        notify_pause_waiters();

        // Wake choked demuxers AFTER publishing the terminal state: the
        // waiter re-checks the status and exits (matches FFmpeg sch_stop,
        // which unchokes every demuxer after setting terminate).
        for waiter in &self.demux_waiters {
            waiter.set(false);
        }

        // Always wait for all threads to complete
        // This is safe to call multiple times - if threads are already done (counter==0),
        // wait_for_all_threads() returns immediately without blocking.
        // This ensures cleanup works correctly after both abort() and stop().
        log::debug!("Drop waiting for all threads to finish");
        self.thread_sync.wait_for_all_threads();
    }
}

pub struct FfmpegScheduler<S> {
    ffmpeg_context: FfmpegContext,
    status: Arc<AtomicUsize>,
    thread_sync: ThreadSynchronizer,
    result: Arc<Mutex<Option<crate::error::Result<()>>>>,
    state: PhantomData<S>,
    // Guard for Running state - Some only when in Running state
    _guard: Option<RunningGuard>,
}
// Send is auto-derived: every field is Send (FfmpegContext is Send, the rest
// are Arc/atomic/channel types) and the state markers are unit structs.
//
// Sync is deliberately NOT implemented. FfmpegContext is not Sync (it owns
// Box<dyn FrameFilter> values that are only Send, plus raw FFmpeg pointers);
// a blanket `unsafe impl Sync` here would launder that through shared
// references — the same hole FfmpegContext itself closed in 0.10.0. Nothing
// in the public API needs &FfmpegScheduler from multiple threads: waiting
// consumes self, and the async Future only requires Send.

// fftools: SCH_STATE_UNINIT/STARTED/STOPPED + the `terminate` flag
// (ffmpeg_sched.c SchedulerState; sch_stop enters STOPPED at :2604). ez
// splits terminate into END (graceful) vs ABORT (drop everything) and
// adds PAUSE, which has no CLI counterpart.
pub(crate) const STATUS_INIT: usize = 0;
pub(crate) const STATUS_RUN: usize = 1;
pub(crate) const STATUS_PAUSE: usize = 2;
pub(crate) const STATUS_ABORT: usize = 3;
pub(crate) const STATUS_END: usize = 4;

/// Checks if scheduler is in a stopping state (abort or normal end)
pub(crate) fn is_stopping(status: usize) -> bool {
    status == STATUS_END || status == STATUS_ABORT
}

impl<S: 'static> FfmpegScheduler<S> {
    /// Determines if this scheduler’s **state type** (`S`) matches a specified
    /// type `T`. Primarily used internally to check state transitions at runtime.
    ///
    /// # Returns
    /// - `true` if `S` and `T` are the same type.
    /// - `false` otherwise.
    #[allow(dead_code)]
    fn is_state<T: 'static>(&self) -> bool {
        std::any::TypeId::of::<S>() == std::any::TypeId::of::<T>()
    }

    /// Consumes this scheduler and **transitions** it to a new state type `T`.
    /// Used internally for changing from `Initialization` to `Running`, etc.
    ///
    /// # Returns
    /// A new `FfmpegScheduler<T>` retaining the same internal data,
    /// but typed with the new state `T`.
    fn into_state<T>(self) -> FfmpegScheduler<T> {
        FfmpegScheduler {
            ffmpeg_context: self.ffmpeg_context,
            status: self.status,
            thread_sync: self.thread_sync,
            result: self.result,
            state: Default::default(),
            _guard: self._guard, // Pass guard to maintain Drop protection across state transitions
        }
    }

    /// Internal method to signal the scheduler to stop.
    /// This sets the scheduler's status to "END" and signals all worker threads
    /// to finish up and exit gracefully.
    ///
    /// # Note
    /// This method only sets the signal flag. It does not wait for threads to complete.
    /// Calling code is responsible for deciding whether to wait or not.
    fn signal_stop(&self) {
        self.status.store(STATUS_END, Ordering::Release);
        notify_pause_waiters();
        self.wake_demux_waiters();
    }

    /// Notifies every demuxer choke waiter so it re-checks the (already
    /// published) terminal status. Must run AFTER the status store.
    fn wake_demux_waiters(&self) {
        for demux in &self.ffmpeg_context.demuxs {
            let crate::core::scheduler::input_controller::SchNode::Demux { waiter, .. } =
                demux.node.as_ref()
            else {
                continue;
            };
            waiter.set(false);
        }
    }

    /// Checks whether the FFmpeg job has ended. The job can end because it
    /// completed successfully, encountered an error, or was manually aborted.
    ///
    /// # Returns
    /// - `true` if the FFmpeg job is in the ended state.
    /// - `false` otherwise.
    pub fn is_ended(&self) -> bool {
        is_stopping(self.status.load(Ordering::Acquire))
    }
}

impl FfmpegScheduler<Initialization> {
    /// Creates a new [`FfmpegScheduler`] in the **initialization** state from the given [`FfmpegContext`].
    /// This is the first step to orchestrating an FFmpeg job: you prepare your
    /// inputs, outputs, and filters using [`FfmpegContext`], then pass it here.
    ///
    /// # Example
    /// ```rust,ignore
    /// let context = FfmpegContext::builder()
    ///     .input("input.mp4")
    ///     .output("output.mkv")
    ///     .build()
    ///     .unwrap();
    ///
    /// let scheduler = FfmpegScheduler::new(context);
    /// // At this point, no actual FFmpeg threads are running; call `start()` to begin.
    /// ```
    pub fn new(ffmpeg_context: FfmpegContext) -> FfmpegScheduler<Initialization> {
        // Reuse the context's status atomic: the AVIO interrupt callbacks
        // installed at build time are bound to it, so stop()/abort() can
        // break blocking I/O on the already-opened contexts.
        let status = ffmpeg_context.scheduler_status.clone();
        FfmpegScheduler {
            ffmpeg_context,
            state: Default::default(),
            thread_sync: ThreadSynchronizer::new(),
            status,
            result: Arc::new(Mutex::new(None)),
            _guard: None,
        }
    }

    /// Initializes all FFmpeg components (demuxers, encoders, filters, muxers)
    /// and transitions the scheduler from **Initialization** to **Running**.
    ///
    /// If any part of the setup fails (e.g., an invalid codec, a missing file),
    /// this method returns an error and cleans up resources.
    ///
    /// # Returns
    /// - `Ok(FfmpegScheduler<Running>)` if initialization succeeded.
    /// - `Err(...)` if an error occurred during FFmpeg setup.
    ///
    /// # Example
    /// ```rust,ignore
    /// let scheduler = FfmpegScheduler::new(ffmpeg_context);
    /// let running_scheduler = scheduler.start().expect("Failed to start FFmpeg");
    /// // Now it's in Running state, you can wait or pause/abort, etc.
    /// ```
    pub fn start(mut self) -> crate::error::Result<FfmpegScheduler<Running>> {
        let packet_pool = match ObjPool::new(64, new_packet, unref_packet, packet_is_null) {
            Ok(pool) => pool,
            Err(e) => {
                Self::cleanup(&self.status, &self.ffmpeg_context);
                return Err(e);
            }
        };
        let frame_pool = match ObjPool::new(64, new_frame, unref_frame, frame_is_null) {
            Ok(pool) => pool,
            Err(e) => {
                Self::cleanup(&self.status, &self.ffmpeg_context);
                return Err(e);
            }
        };
        let scheduler_status = self.status.clone();
        scheduler_status.store(STATUS_RUN, Ordering::Release);
        let thread_sync = self.thread_sync.clone();
        let scheduler_result = self.result.clone();

        let demux_nodes = self
            .ffmpeg_context
            .demuxs
            .iter()
            .map(|demux| demux.node.clone())
            .collect::<Vec<_>>();
        let mux_stream_nodes = self
            .ffmpeg_context
            .muxs
            .iter()
            .flat_map(|mux| mux.mux_stream_nodes.clone())
            .collect::<Vec<_>>();
        let input_controller = InputController::new(demux_nodes, mux_stream_nodes);
        let input_controller = Arc::new(input_controller);

        // Muxer-completion counter (fftools `Scheduler.nb_mux`): one guard per
        // muxer; the last to finish publishes STATUS_END, releasing any demuxer
        // still choked by the balancing pass (see MuxDoneGuard in mux_task.rs).
        let mux_done_remaining = Arc::new(AtomicUsize::new(self.ffmpeg_context.muxs.len()));

        // Pre-count EVERY muxer's thread slot before any `mux_init` runs. A
        // streamless (AVFMT_NOSTREAMS) output releases its slot synchronously
        // inside `mux_init` (`release_mux_slot`); counted one-at-a-time, an
        // early streamless output could drive the thread counter to zero and
        // publish a premature STATUS_END before later muxers were even counted,
        // which stops (truncates) those still-pending outputs. Counting all
        // muxers up front keeps the counter > 0 until every muxer — and every
        // later worker — is genuinely done.
        for _ in 0..self.ffmpeg_context.muxs.len() {
            thread_sync.thread_start();
        }

        // Muxer
        for (mux_idx, mux) in self.ffmpeg_context.muxs.iter_mut().enumerate() {
            if mux.is_ready() {
                if let Err(e) = mux_init(
                    mux_idx,
                    mux,
                    packet_pool.clone(),
                    input_controller.clone(),
                    mux.mux_stream_nodes.clone(),
                    scheduler_status.clone(),
                    thread_sync.clone(),
                    scheduler_result.clone(),
                    mux_done_remaining.clone(),
                ) {
                    Self::cleanup(&scheduler_status, &self.ffmpeg_context);
                    return Err(e);
                }
            }
        }

        // Output frame filter pipeline
        let ffmpeg_context = &mut self.ffmpeg_context;
        for (mux_idx, mux) in ffmpeg_context.muxs.iter_mut().enumerate() {
            if let Some(frame_pipelines) = mux.frame_pipelines.take() {
                for frame_pipeline in frame_pipelines {
                    if let Err(e) = output_pipeline_init(
                        mux_idx,
                        frame_pipeline,
                        mux.get_streams_mut(),
                        frame_pool.clone(),
                        scheduler_status.clone(),
                        thread_sync.clone(),
                        scheduler_result.clone(),
                    ) {
                        Self::cleanup(&scheduler_status, ffmpeg_context);
                        return Err(e);
                    }
                }
            }
        }

        // Encoder
        let ffmpeg_context = &mut self.ffmpeg_context;
        for (mux_idx, mux) in &mut ffmpeg_context.muxs.iter_mut().enumerate() {
            let ready_sender = match ready_to_init_mux(
                mux_idx,
                mux,
                packet_pool.clone(),
                input_controller.clone(),
                scheduler_status.clone(),
                thread_sync.clone(),
                scheduler_result.clone(),
                mux_done_remaining.clone(),
            ) {
                Ok(sender) => sender,
                Err(e) => {
                    Self::cleanup(&scheduler_status, ffmpeg_context);
                    return Err(e);
                }
            };

            // -shortest (sq_enc): build a frame-level sync queue over this mux's
            // encoded audio/video streams so none outruns the shortest limiting
            // one. FFmpeg gate (ffmpeg_mux_init.c setup_sync_queues): shortest &&
            // nb_av_enc > 1. Encoded subtitle/data are NOT sq_enc members (they
            // ride sq_mux only); attachments never reach enc_init. Handles are
            // attached before the streams are moved into enc_init below.
            if mux.shortest {
                let buf_us = mux.shortest_buf_duration_us;
                // (position in the encoder-stream Vec, output stream index) for
                // each encoded audio/video stream. The output index resolves this
                // stream's `source_finished` flag below; collected before the
                // mutable borrow that attaches the handles.
                let av: Vec<_> = mux
                    .get_streams()
                    .iter()
                    .enumerate()
                    .filter(|(_, s)| {
                        s.codec_type == AVMEDIA_TYPE_VIDEO || s.codec_type == AVMEDIA_TYPE_AUDIO
                    })
                    .map(|(i, s)| (i, s.stream_index, s.codec_type))
                    .collect();
                if av.len() >= 2 {
                    let mut sq = SyncQueue::new(buf_us);
                    // Each member: (encoder-vec position, its sq_idx, its own
                    // `source_finished`). `add_stream(true)` in `av` order fixes
                    // each stream's sq_idx; the balancer flag (`SchNode::MuxStream`)
                    // lets a finished producer leave the input balancer before
                    // draining (fftools send_to_enc_sq, ffmpeg_sched.c:1929).
                    // A per-stream frame cap (-frames:v/a) is wired into the sync
                    // queue as frames_max so `-shortest` + set_max_{video,audio}_frames
                    // still bounds this stream (fftools sch_sq_add_enc ->
                    // sq_limit_frames). Without it the cap is silently ignored while
                    // sq_enc is active (the enc_task max-frames check only runs on
                    // the non-sync-queue path).
                    let members: Vec<(usize, usize, Option<Arc<AtomicBool>>)> = av
                        .iter()
                        .map(|&(pos, osi, ct)| {
                            let sq_idx = sq.add_stream(true);
                            let max_frames = if ct == AVMEDIA_TYPE_VIDEO {
                                mux.max_video_frames
                            } else {
                                mux.max_audio_frames
                            };
                            if let Some(max_frames) = max_frames {
                                if max_frames >= 0 {
                                    sq.sq_limit_frames(sq_idx, max_frames as u64);
                                }
                            }
                            (pos, sq_idx, mux.stream_source_finished(osi))
                        })
                        .collect();
                    let sq_finished: Arc<[AtomicBool]> =
                        (0..av.len()).map(|_| AtomicBool::new(false)).collect();
                    let queue = Arc::new((Mutex::new(sq), Condvar::new()));
                    let streams = mux.get_streams_mut();
                    for (pos, sq_idx, source_finished) in members {
                        streams[pos].set_sync_queue(EncSyncHandle {
                            queue: queue.clone(),
                            sq_idx,
                            sq_finished: sq_finished.clone(),
                            source_finished,
                        });
                    }
                }
            }

            for enc_stream in mux.take_streams_mut() {
                if let Err(e) = enc_init(
                    mux_idx,
                    enc_stream,
                    ready_sender.clone(),
                    mux.start_time_us,
                    mux.recording_time_us,
                    mux.bits_per_raw_sample,
                    mux.max_video_frames,
                    mux.max_audio_frames,
                    mux.max_subtitle_frames,
                    &mux.video_codec_opts,
                    &mux.audio_codec_opts,
                    &mux.subtitle_codec_opts,
                    mux.oformat_flags,
                    frame_pool.clone(),
                    packet_pool.clone(),
                    scheduler_status.clone(),
                    thread_sync.clone(),
                    mux.enc_handle_sender(),
                    scheduler_result.clone(),
                    input_controller.clone(),
                ) {
                    Self::cleanup(&scheduler_status, ffmpeg_context);
                    return Err(e);
                }
            }
        }

        // Filter graph
        let ffmpeg_context = &mut self.ffmpeg_context;
        for (i, filter_graph) in ffmpeg_context.filter_graphs.iter_mut().enumerate() {
            if let Err(e) = filter_graph_init(
                i,
                filter_graph,
                frame_pool.clone(),
                input_controller.clone(),
                filter_graph.node.clone(),
                scheduler_status.clone(),
                thread_sync.clone(),
                scheduler_result.clone(),
            ) {
                Self::cleanup(&scheduler_status, ffmpeg_context);
                return Err(e);
            }
        }

        // Input frame filter pipeline
        let ffmpeg_context = &mut self.ffmpeg_context;
        for (demux_idx, demux) in ffmpeg_context.demuxs.iter_mut().enumerate() {
            if let Some(frame_pipelines) = demux.frame_pipelines.take() {
                for frame_pipeline in frame_pipelines {
                    if let Err(e) = input_pipeline_init(
                        demux_idx,
                        frame_pipeline,
                        demux.get_streams_mut(),
                        frame_pool.clone(),
                        scheduler_status.clone(),
                        thread_sync.clone(),
                        scheduler_result.clone(),
                    ) {
                        Self::cleanup(&scheduler_status, ffmpeg_context);
                        return Err(e);
                    }
                }
            }
        }

        // Decoder
        for (demux_idx, demux) in &mut ffmpeg_context.demuxs.iter_mut().enumerate() {
            let exit_on_error = demux.exit_on_error;

            for dec_stream in demux.get_streams_mut() {
                if let Err(e) = dec_init(
                    demux_idx,
                    dec_stream,
                    exit_on_error,
                    frame_pool.clone(),
                    packet_pool.clone(),
                    scheduler_status.clone(),
                    thread_sync.clone(),
                    scheduler_result.clone(),
                ) {
                    Self::cleanup(&scheduler_status, ffmpeg_context);
                    return Err(e);
                }
            }
        }

        // Demuxer
        for (demux_idx, demux) in ffmpeg_context.demuxs.iter_mut().enumerate() {
            if let Err(e) = demux_init(
                demux_idx,
                demux,
                ffmpeg_context.independent_readrate,
                packet_pool.clone(),
                demux.node.clone(),
                scheduler_status.clone(),
                thread_sync.clone(),
                scheduler_result.clone(),
            ) {
                Self::cleanup(&scheduler_status, ffmpeg_context);
                return Err(e);
            }
        }

        input_controller.as_ref().update_locked(&scheduler_status);

        // Create Running state with guard for Drop implementation
        let mut running_scheduler = self.into_state::<Running>();
        let demux_waiters = running_scheduler
            .ffmpeg_context
            .demuxs
            .iter()
            .filter_map(|demux| {
                let crate::core::scheduler::input_controller::SchNode::Demux { waiter, .. } =
                    demux.node.as_ref()
                else {
                    return None;
                };
                Some(waiter.clone())
            })
            .collect();
        running_scheduler._guard = Some(RunningGuard {
            status: running_scheduler.status.clone(),
            thread_sync: running_scheduler.thread_sync.clone(),
            demux_waiters,
            _interrupt_state: running_scheduler.ffmpeg_context.interrupt_state.clone(),
        });

        Ok(running_scheduler)
    }

    /// Cleans up Muxers/Demuxers and signals the job to end if an error occurs
    /// during initialization. This is invoked internally when `start()` fails.
    fn cleanup(scheduler_status: &Arc<AtomicUsize>, _ffmpeg_context: &FfmpegContext) {
        // Contexts still owned by Demuxer/Muxer are freed by their Drop when
        // the FfmpegContext goes down with the failed scheduler; contexts
        // already handed to worker threads are freed by those threads
        // (STATUS_END makes them exit).
        scheduler_status.store(STATUS_END, Ordering::Release);
        notify_pause_waiters();
    }
}

impl FfmpegScheduler<Running> {
    /// Pauses a running FFmpeg job, transitioning from `Running` to `Paused`.
    ///
    /// Internally sets the FFmpeg pipeline threads to a paused state. Depending
    /// on your FFmpeg pipeline design, it may take a moment for all threads to
    /// acknowledge this request. If the scheduler is already ended, this does nothing.
    ///
    /// # Returns
    /// A new [`FfmpegScheduler<Paused>`] representing the paused job.
    pub fn pause(self) -> FfmpegScheduler<Paused> {
        // CAS RUN->PAUSE instead of load-check-then-store: a terminal STATUS_END
        // /STATUS_ABORT published concurrently (e.g. the last MuxDoneGuard, or
        // abort()) between a load and a plain store would otherwise be clobbered
        // back to PAUSE, stranding a demuxer whose only release was that terminal
        // status. The CAS only transitions from RUN and never overwrites a
        // stopping (or already-paused) state.
        let _ = self.status.compare_exchange(
            STATUS_RUN,
            STATUS_PAUSE,
            Ordering::AcqRel,
            Ordering::Acquire,
        );
        self.into_state()
    }

    /// Blocks the current thread until the FFmpeg job finishes (success, error, or abort).
    ///
    /// This method is only available in **non-async** builds.
    ///
    /// # Returns
    /// - `Ok(())` if the job completed successfully.
    /// - `Err(...)` if an error was encountered (also logs the error).
    ///
    /// # Notes
    /// - If you enable the `async` feature, this method is replaced by an async `.await`.
    ///   See the `Future` implementation below.
    ///
    /// # Example
    /// ```rust,ignore
    /// // After calling `.start()`:
    /// let result = scheduler.wait();
    /// assert!(result.is_ok());
    /// ```
    pub fn wait(self) -> crate::error::Result<()> {
        if !is_stopping(self.status.load(Ordering::Acquire)) {
            self.thread_sync.wait_for_all_threads();
            self.status.store(STATUS_END, Ordering::Release);
        }

        // A worker that panicked while holding the lock must surface as an
        // error on the caller's thread, not as a second panic.
        let option = self
            .result
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .take();
        match option {
            None => {
                log::info!("FFmpeg task succeeded.");
                Ok(())
            }
            Some(result) => {
                log::error!("FFmpeg task failed.");
                result
            }
        }
    }

    /// **WARNING: Immediately aborts the FFmpeg job without waiting for threads to complete.**
    ///
    /// This method signals all threads to stop and returns **immediately**. It does NOT wait
    /// for threads to finish their work, which means:
    ///
    /// - **Output files WILL BE UNUSABLE** (missing trailer, encoder buffers not flushed)
    /// - **Encoded data in buffers will be lost** (B-frames, delayed frames)
    /// - **Files will NOT be seekable or playable** in most media players
    /// - Only use this when you **do not need the output files at all**
    ///
    /// # When to Use
    ///
    /// - Emergency shutdown scenarios where speed is critical
    /// - Preview/test runs where output is discarded
    /// - User cancellation where output is not needed
    ///
    /// # When NOT to Use
    ///
    /// - **NEVER** when you need valid output files → **Use [`stop()`](Self::stop) instead**
    ///
    /// # Why Files Are Unusable
    ///
    /// `abort()` causes:
    /// 1. Encoder to skip flush -> buffered frames lost
    /// 2. Muxer to skip trailer -> no seeking, no playback in most players
    ///
    /// **The ONLY way to get valid files is to use `stop()` instead of `abort()`.**
    ///
    /// # Comparison
    ///
    /// ```rust,ignore
    /// // WRONG: Files will be unusable
    /// scheduler.abort();
    ///
    /// // CORRECT: Files will be valid and playable
    /// scheduler.stop();
    /// ```
    ///
    /// # Example
    /// ```rust,ignore
    /// let running_scheduler = scheduler.start().unwrap();
    /// // User clicked "Cancel" - don't need output
    /// running_scheduler.abort();
    /// ```
    pub fn abort(self) {
        self.status.store(STATUS_ABORT, Ordering::Release);
        notify_pause_waiters();
        self.wake_demux_waiters();
    }

    /// Gracefully stops the FFmpeg job and waits for all threads to complete.
    ///
    /// This method **blocks** until all processing is finished and ensures **complete data integrity**.
    /// All encoder buffers are flushed, all output files are properly finalized with trailers,
    /// and all threads are cleanly terminated.
    ///
    /// # Guarantees
    ///
    /// - All buffered frames are encoded and written
    /// - Output files contain proper headers and trailers (e.g., MP4 moov atom, MKV Cues)
    /// - Files are seekable and playable in all media players
    /// - No data loss or corruption
    ///
    /// # When to Use
    ///
    /// - **Always use this** when you need valid output files
    /// - Production transcoding workflows
    /// - Before exiting the application
    /// - Any scenario where output quality matters
    ///
    /// # Comparison with abort()
    ///
    /// | Method | Blocks? | Data Integrity | Use Case |
    /// |--------|---------|----------------|----------|
    /// | `stop()` | Yes | Guaranteed | Production, need valid files |
    /// | `abort()` | No | Not guaranteed | Emergency, don't care about files |
    ///
    /// # Example
    /// ```rust,ignore
    /// let running_scheduler = scheduler.start().unwrap();
    /// // ... processing ...
    /// running_scheduler.stop(); // Blocks until complete, files are valid
    /// ```
    pub fn stop(self) {
        self.signal_stop();
        self.thread_sync.wait_for_all_threads();
        log::debug!("stop() completed, all threads finished");
    }
}

#[cfg(feature = "async")]
impl std::future::Future for FfmpegScheduler<Running> {
    type Output = crate::error::Result<()>;

    /// An asynchronous wait for the FFmpeg job. This `Future` **completes**
    /// when the job finishes (either success, error, or abort).
    ///
    /// # Returns
    /// - `Ok(())` if the FFmpeg job completed successfully.
    /// - `Err(...)` if the job was aborted or encountered an error.
    ///
    /// # Example
    /// ```rust,ignore
    /// # // Requires enabling the "async" feature in your Cargo.toml
    /// #[tokio::main]
    /// async fn main() {
    ///     let scheduler = FfmpegScheduler::new(ffmpeg_context).start().unwrap();
    ///     let result = scheduler.await;  // same as scheduler.wait().await
    ///     assert!(result.is_ok());
    /// }
    /// ```
    fn poll(
        self: std::pin::Pin<&mut Self>,
        cx: &mut std::task::Context<'_>,
    ) -> std::task::Poll<Self::Output> {
        fn ready(
            result: &Arc<Mutex<Option<crate::error::Result<()>>>>,
        ) -> std::task::Poll<crate::error::Result<()>> {
            let option = result
                .lock()
                .unwrap_or_else(std::sync::PoisonError::into_inner)
                .take();
            std::task::Poll::Ready(match option {
                None => {
                    log::info!("FFmpeg task succeeded.");
                    Ok(())
                }
                Some(result) => {
                    log::error!("FFmpeg task failed.");
                    result
                }
            })
        }

        let this = self.get_mut();

        if is_stopping(this.status.load(Ordering::Acquire)) {
            return ready(&this.result);
        }

        let thread_sync = this.thread_sync.clone();
        let waker = cx.waker().clone();
        thread_sync.set_waker(waker);

        // Re-check after registering the waker: the last thread may have
        // published the terminal state and consumed a previous waker between
        // the check above and set_waker, in which case no further wake ever
        // comes for the waker just installed.
        if is_stopping(this.status.load(Ordering::Acquire)) {
            return ready(&this.result);
        }

        std::task::Poll::Pending
    }
}

impl FfmpegScheduler<Paused> {
    /// Resumes a paused FFmpeg job, transitioning from `Paused` back to `Running`.
    ///
    /// If the scheduler is in an ended state, this has no effect. Otherwise,
    /// it unpauses the pipeline so it can continue processing.
    ///
    /// # Returns
    /// A new [`FfmpegScheduler<Running>`] representing the resumed job.
    pub fn resume(self) -> FfmpegScheduler<Running> {
        // CAS PAUSE->RUN (see pause()): never resurrect RUN from a terminal
        // STATUS_END/STATUS_ABORT that raced in while paused. Only wake workers
        // when this actually transitioned out of PAUSE.
        if self
            .status
            .compare_exchange(
                STATUS_PAUSE,
                STATUS_RUN,
                Ordering::AcqRel,
                Ordering::Acquire,
            )
            .is_ok()
        {
            // Wake workers parked in wait_until_not_paused so they resume
            // immediately instead of after the safety-net timeout (conc-06).
            notify_pause_waiters();
        }
        self.into_state()
    }

    /// **WARNING: Immediately aborts the paused FFmpeg job without waiting for threads to complete.**
    ///
    /// This method has the **exact same behavior and consequences** as [`FfmpegScheduler<Running>::abort()`]:
    ///
    /// - **Output files WILL BE UNUSABLE** (missing trailer, encoder buffers not flushed)
    /// - **Encoded data in buffers will be lost** (B-frames, delayed frames)
    /// - **Files will NOT be seekable or playable** in most media players
    /// - Only use this when you **do not need the output files at all**
    ///
    /// **The ONLY way to get valid files is to use [`stop()`](FfmpegScheduler::stop) instead of `abort()`.**
    ///
    /// See [`FfmpegScheduler<Running>::abort()`] for complete documentation.
    ///
    /// # Example
    /// ```rust,ignore
    /// let paused_scheduler = running_scheduler.pause();
    /// // User clicked "Cancel" - don't need output
    /// paused_scheduler.abort();
    /// ```
    pub fn abort(self) {
        self.status.store(STATUS_ABORT, Ordering::Release);
        notify_pause_waiters();
        self.wake_demux_waiters();
    }
}

fn new_frame() -> crate::error::Result<Frame> {
    let frame = unsafe { av_frame_alloc() };
    if frame.is_null() {
        return Err(AllocFrameError::OutOfMemory.into());
    }
    Ok(unsafe { Frame::wrap(frame) })
}

fn new_packet() -> crate::error::Result<Packet> {
    let packet = Packet::empty();
    if packet.as_ptr().is_null() {
        return Err(AllocPacketError::OutOfMemory.into());
    }
    Ok(packet)
}

#[inline]
pub(crate) fn unref_frame(frame: &mut Frame) {
    unsafe { av_frame_unref(frame.as_mut_ptr()) };
}

#[inline]
pub(crate) fn unref_packet(packet: &mut Packet) {
    unsafe { av_packet_unref(packet.as_mut_ptr()) };
}

#[inline]
pub(crate) fn frame_is_null(frame: &Frame) -> bool {
    unsafe { frame.as_ptr().is_null() }
}

#[inline]
pub(crate) fn packet_is_null(packet: &Packet) -> bool {
    packet.as_ptr().is_null()
}

/// Process-wide coordination pair for pausing workers (conc-06).
///
/// The mutex guards nothing but the condvar handshake — the real predicate is
/// the per-scheduler `status` atomic. It is shared across schedulers because a
/// transition in one scheduler only ever produces a harmless spurious wakeup in
/// another (the woken worker re-checks its own status and parks again), and
/// pauses are rare, so a global pair avoids threading a per-scheduler gate type
/// through every worker init signature.
fn pause_wait() -> &'static (Mutex<()>, Condvar) {
    static PAUSE_WAIT: OnceLock<(Mutex<()>, Condvar)> = OnceLock::new();
    PAUSE_WAIT.get_or_init(|| (Mutex::new(()), Condvar::new()))
}

/// Block while the scheduler is paused, returning the status once it leaves
/// STATUS_PAUSE (conc-06: replaces a 1ms sleep-poll that burned ~1k wakeups/sec
/// per worker for the whole pause).
///
/// Correctness: the fast path is a single acquire load. On the slow path the
/// predicate is re-checked under the pause mutex before each wait, and
/// `notify_pause_waiters()` takes that same mutex around `notify_all()`, so a
/// transition cannot slip its notify into the window between our check and our
/// wait — there is no lost wakeup. The bounded `wait_timeout` is only a safety
/// net: even if some future transition forgot to notify, a paused worker still
/// re-observes a terminal/resumed status within the timeout instead of hanging.
pub(crate) fn wait_until_not_paused(scheduler_status: &Arc<AtomicUsize>) -> usize {
    // Fast path: not paused — no lock, no condvar.
    let status = scheduler_status.load(Ordering::Acquire);
    if status != STATUS_PAUSE {
        return status;
    }

    let (lock, cond) = pause_wait();
    let mut guard = lock
        .lock()
        .unwrap_or_else(std::sync::PoisonError::into_inner);
    loop {
        let status = scheduler_status.load(Ordering::Acquire);
        if status != STATUS_PAUSE {
            return status;
        }
        let (g, _timeout) = cond
            .wait_timeout(guard, Duration::from_millis(200))
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        guard = g;
    }
}

/// Wake every worker parked in `wait_until_not_paused`. Call this after any
/// status transition out of STATUS_PAUSE (resume / stop / end / abort / error)
/// so paused workers observe the new state immediately. Holding the pause mutex
/// around `notify_all()` closes the lost-wakeup window against the slow-path
/// predicate re-check.
pub(crate) fn notify_pause_waiters() {
    let (lock, cond) = pause_wait();
    let _guard = lock
        .lock()
        .unwrap_or_else(std::sync::PoisonError::into_inner);
    cond.notify_all();
}

pub(crate) fn set_scheduler_error(
    scheduler_status: &Arc<AtomicUsize>,
    scheduler_result: &Arc<Mutex<Option<crate::error::Result<()>>>>,
    error: impl Into<crate::error::Error>,
) {
    // First-error-wins must keep working even after some worker panicked
    // with the lock held; poisoning here would cascade panics through every
    // other worker's error path.
    let mut scheduler_result = scheduler_result
        .lock()
        .unwrap_or_else(std::sync::PoisonError::into_inner);
    if scheduler_result.is_none() {
        scheduler_result.replace(Err(error.into()));
        scheduler_status.store(STATUS_END, Ordering::Release);
        // Wake paused workers so they observe the terminal state and exit,
        // rather than sleeping until the safety-net timeout (conc-06). The error
        // result is already recorded above, so a woken wait() sees it.
        notify_pause_waiters();
    }
}

#[cfg(test)]
mod tests {
    use crate::core::context::ffmpeg_context::FfmpegContext;
    use crate::core::context::input::Input;
    use crate::core::context::output::Output;
    use crate::core::filter::frame_filter::NoopFilter;
    use crate::core::scheduler::ffmpeg_scheduler::{
        FfmpegScheduler, Initialization, Paused, Running, STATUS_INIT, STATUS_PAUSE, STATUS_RUN,
    };
    use crate::filter::frame_pipeline_builder::FramePipelineBuilder;
    use ffmpeg_sys_next::AVMediaType;
    use log::{info, warn};
    use std::sync::atomic::Ordering;
    use std::sync::{Arc, Mutex};
    use std::thread::sleep;
    use std::time::Duration;

    // conc-06: workers parked in wait_until_not_paused must all wake and observe
    // the new status when a transition out of STATUS_PAUSE notifies the gate —
    // no 1ms sleep-poll, no lost wakeup.
    #[test]
    fn pause_gate_wakes_all_waiters_on_transition() {
        use crate::core::scheduler::ffmpeg_scheduler::{
            notify_pause_waiters, wait_until_not_paused,
        };
        use std::sync::atomic::AtomicUsize;
        use std::sync::mpsc;

        let status = Arc::new(AtomicUsize::new(STATUS_PAUSE));
        let (tx, rx) = mpsc::channel();

        let mut handles = Vec::new();
        for _ in 0..4 {
            let status = Arc::clone(&status);
            let tx = tx.clone();
            handles.push(std::thread::spawn(move || {
                tx.send(wait_until_not_paused(&status)).unwrap();
            }));
        }
        drop(tx);

        // Give the workers time to reach the condvar wait, then resume.
        sleep(Duration::from_millis(50));
        // No waiter should have returned while still paused.
        assert!(
            rx.try_recv().is_err(),
            "a paused worker returned before resume"
        );

        status.store(STATUS_RUN, Ordering::Release);
        notify_pause_waiters();

        for _ in 0..4 {
            let observed = rx
                .recv_timeout(Duration::from_secs(5))
                .expect("a paused worker never woke after the transition");
            assert_eq!(observed, STATUS_RUN);
        }
        for h in handles {
            h.join().unwrap();
        }
    }

    #[test]
    fn set_scheduler_error_survives_a_poisoned_result_lock() {
        use crate::core::scheduler::ffmpeg_scheduler::{set_scheduler_error, STATUS_END};
        use std::sync::atomic::AtomicUsize;

        let status = Arc::new(AtomicUsize::new(STATUS_RUN));
        let result: Arc<Mutex<Option<crate::error::Result<()>>>> = Arc::new(Mutex::new(None));

        // Poison the lock the way a worker panicking mid-error would.
        let poisoned = Arc::clone(&result);
        let _ = std::thread::spawn(move || {
            let _guard = poisoned.lock().unwrap();
            panic!("worker panicked while holding the result lock");
        })
        .join();
        assert!(result.is_poisoned(), "test setup must poison the lock");

        // First-error-wins must still record the error instead of panicking.
        set_scheduler_error(&status, &result, crate::error::Error::NotStarted);
        assert_eq!(status.load(Ordering::Acquire), STATUS_END);
        assert!(result
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .is_some());
    }

    #[test]
    fn test_img_to_video() {
        let _ = env_logger::builder()
            .filter_level(log::LevelFilter::Trace)
            .is_test(true)
            .try_init();

        let result = FfmpegContext::builder()
            .input(
                Input::from("logo.jpg")
                    .set_input_opt("loop", "1")
                    .set_recording_time_us(10 * 1000_000),
            )
            .filter_desc("scale=1280:720")
            .output(Output::from("output_img_to_video.mp4"))
            .build()
            .unwrap()
            .start()
            .unwrap()
            .wait();

        assert!(result.is_ok());
    }
    #[test]
    fn test_copy() {
        let _ = env_logger::builder()
            .filter_level(log::LevelFilter::Trace)
            .is_test(true)
            .try_init();

        let result = FfmpegContext::builder()
            .input("test.mp4")
            .output(
                Output::from("output_copy.mp4")
                    .add_stream_map_with_copy("0:v")
                    .add_stream_map_with_copy("0:a"),
            )
            .build()
            .unwrap()
            .start()
            .unwrap()
            .wait();

        assert!(result.is_ok());
    }

    #[test]
    fn test_concat() {
        let _ = env_logger::builder()
            .filter_level(log::LevelFilter::Trace)
            .is_test(true)
            .try_init();

        let result = FfmpegContext::builder()
            .input("test.mp4")
            .input("test.mp4")
            .input("test.mp4")
            .filter_desc("concat=n=3:v=1:a=1")
            .output("output_concat.mp4")
            .build()
            .unwrap()
            .start()
            .unwrap()
            .wait();

        assert!(result.is_ok());

        std::thread::sleep(Duration::from_secs(1));
    }

    // videotoolbox is Apple-only; on other platforms the hwaccel device fails to
    // allocate. Gate to macOS so non-macOS CI isn't a false red.
    #[cfg(target_os = "macos")]
    #[test]
    fn test_to_stdout() {
        let _ = env_logger::builder()
            .filter_level(log::LevelFilter::Trace)
            .is_test(true)
            .try_init();

        let input: Input = "test.mp4".into();
        let output: Output = "-".into();

        let result = FfmpegContext::builder()
            .input(input.set_hwaccel("videotoolbox"))
            .output(output.set_format("null").set_video_codec("h264"))
            .build()
            .unwrap()
            .start()
            .unwrap()
            .wait();

        assert!(result.is_ok());
    }

    // videotoolbox is Apple-only; gate to macOS (see test_to_stdout).
    #[cfg(target_os = "macos")]
    #[test]
    fn test_thumbnail() {
        let _ = env_logger::builder()
            .filter_level(log::LevelFilter::Trace)
            .is_test(true)
            .try_init();

        let mut input: Input = "test.mp4".into();
        let output: Output = "output.jpg".into();

        input.hwaccel = Some("videotoolbox".to_string());

        let result = FfmpegContext::builder()
            .input(input)
            .filter_desc("scale='min(160,iw)':-1")
            .output(output.set_max_video_frames(1))
            .build()
            .unwrap()
            .start()
            .unwrap()
            .wait();

        assert!(result.is_ok());
    }

    #[test]
    fn test_read_write_callback_mp4() {
        let _ = env_logger::builder()
            .filter_level(log::LevelFilter::Trace)
            .is_test(true)
            .try_init();

        use std::fs::File;
        use std::io::{Read, Seek, SeekFrom, Write};

        let input_file = "test.mp4";
        let output_file = "output_rw_callback.mp4";

        let input_file = Arc::new(Mutex::new(
            File::open(input_file).expect("Failed to open input file"),
        ));
        let read_callback: Box<dyn FnMut(&mut [u8]) -> i32 + Send> = {
            let input = Arc::clone(&input_file);
            Box::new(move |buf: &mut [u8]| -> i32 {
                let mut input = input.lock().unwrap();
                match input.read(buf) {
                    Ok(0) => ffmpeg_sys_next::AVERROR_EOF,
                    Ok(bytes_read) => bytes_read as i32,
                    Err(_) => ffmpeg_sys_next::AVERROR(ffmpeg_sys_next::EIO),
                }
            })
        };

        let seek_callback: Box<dyn FnMut(i64, i32) -> i64 + Send> = {
            let input = Arc::clone(&input_file);
            Box::new(move |offset: i64, whence: i32| -> i64 {
                let mut input = input.lock().unwrap();
                // ✅ Handle AVSEEK_SIZE: Return total file size
                if whence == ffmpeg_sys_next::AVSEEK_SIZE {
                    if let Ok(size) = input.metadata().map(|m| m.len() as i64) {
                        info!("FFmpeg requested stream size: {}", size);
                        return size;
                    }
                    return ffmpeg_sys_next::AVERROR(ffmpeg_sys_next::EIO) as i64;
                }
                let seek_result = match whence {
                    ffmpeg_sys_next::SEEK_SET => input.seek(SeekFrom::Start(offset as u64)),
                    ffmpeg_sys_next::SEEK_CUR => input.seek(SeekFrom::Current(offset)),
                    ffmpeg_sys_next::SEEK_END => input.seek(SeekFrom::End(offset)),
                    _ => {
                        warn!("Unsupported seek mode: {whence}");
                        return ffmpeg_sys_next::AVERROR(ffmpeg_sys_next::ESPIPE) as i64;
                    }
                };

                match seek_result {
                    Ok(new_pos) => new_pos as i64,
                    Err(_) => ffmpeg_sys_next::AVERROR(ffmpeg_sys_next::EIO) as i64,
                }
            })
        };
        let mut input: Input = read_callback.into();
        input.seek_callback = Some(seek_callback);

        let output_file = Arc::new(Mutex::new(
            File::create(output_file).expect("Failed to create output file"),
        ));
        let write_callback: Box<dyn FnMut(&[u8]) -> i32 + Send> = {
            let output_file = Arc::clone(&output_file);
            Box::new(move |buf: &[u8]| -> i32 {
                let mut output_file = output_file.lock().unwrap();
                match output_file.write_all(buf) {
                    Ok(_) => buf.len() as i32,
                    Err(_) => ffmpeg_sys_next::AVERROR(ffmpeg_sys_next::EIO),
                }
            })
        };

        let seek_callback: Box<dyn FnMut(i64, i32) -> i64 + Send> = {
            let output_file = Arc::clone(&output_file);
            Box::new(move |offset: i64, whence: i32| -> i64 {
                let mut file = output_file.lock().unwrap();

                if whence == ffmpeg_sys_next::AVSEEK_SIZE {
                    if let Ok(size) = file.metadata().map(|m| m.len() as i64) {
                        println!("FFmpeg requested stream size: {}", size);
                        return size;
                    }
                    return ffmpeg_sys_next::AVERROR(ffmpeg_sys_next::EIO) as i64;
                }

                if (whence & ffmpeg_sys_next::AVSEEK_FLAG_BYTE) != 0 {
                    println!(
                        "FFmpeg requested byte-based seeking. Seeking to byte offset: {}",
                        offset
                    );
                    if let Ok(new_pos) = file.seek(SeekFrom::Start(offset as u64)) {
                        return new_pos as i64;
                    }
                    return ffmpeg_sys_next::AVERROR(ffmpeg_sys_next::EIO) as i64;
                }

                let normalized_whence = whence & !ffmpeg_sys_next::AVSEEK_FORCE;

                match normalized_whence {
                    ffmpeg_sys_next::SEEK_SET => file.seek(SeekFrom::Start(offset as u64)),
                    ffmpeg_sys_next::SEEK_CUR => file.seek(SeekFrom::Current(offset)),
                    ffmpeg_sys_next::SEEK_END => file.seek(SeekFrom::End(offset)),
                    _ => Err(std::io::Error::new(
                        std::io::ErrorKind::InvalidInput,
                        "Unsupported seek mode",
                    )),
                }
                .map_or(
                    ffmpeg_sys_next::AVERROR(ffmpeg_sys_next::EIO) as i64,
                    |pos| pos as i64,
                )
            })
        };
        let mut output: Output = write_callback.into();
        output.seek_callback = Some(seek_callback);
        output.format = Some("mp4".to_string());

        let context = FfmpegContext::builder()
            .input(input)
            .filter_desc("hue=s=0")
            .output(output)
            .build()
            .unwrap();

        let scheduler = FfmpegScheduler::new(context);
        let scheduler = scheduler.start().unwrap();
        let result = scheduler.wait();
        assert!(result.is_ok());
    }

    // Regression: an output frame pipeline must let the scheduler terminate.
    // Before the run_pipeline EOF-exit fix, the pipeline thread never returned
    // after the source disconnected, so the scheduler join-all (`wait()`)
    // deadlocked. This test used to be `#[ignore]`d, which is why the
    // regression shipped unnoticed.
    #[test]
    fn test_pipeline() {
        let _ = env_logger::builder()
            .filter_level(log::LevelFilter::Trace)
            .is_test(true)
            .try_init();

        let out_path = std::env::temp_dir().join("ez_test_pipeline_out.mp4");
        let output: Output = out_path.to_str().unwrap().into();
        let frame_pipeline_builder: FramePipelineBuilder = AVMediaType::AVMEDIA_TYPE_VIDEO.into();
        let frame_pipeline_builder = frame_pipeline_builder.filter(
            "test",
            Box::new(NoopFilter::new(AVMediaType::AVMEDIA_TYPE_VIDEO)),
        );
        let output = output.add_frame_pipeline(frame_pipeline_builder);

        let context = FfmpegContext::builder()
            .input("test.mp4")
            .filter_desc("hue=s=0")
            .output(output)
            .build()
            .unwrap();

        let scheduler = FfmpegScheduler::new(context);
        let scheduler = scheduler.start().unwrap();
        // Watchdog instead of a bare `wait()`: the regression this test
        // guards against is a pipeline thread that never exits, and `wait()`
        // would turn that into a silent test-suite hang (the integration
        // suites use wait_with_watchdog for the same reason). `is_ended()`
        // also turns true on error/abort — in those states the `wait()`
        // below surfaces the stored error as a red panic, so every terminal
        // state fails loudly: success green, error/abort red, deadlock red
        // at the deadline.
        let deadline = std::time::Instant::now() + std::time::Duration::from_secs(60);
        while !scheduler.is_ended() {
            assert!(
                std::time::Instant::now() < deadline,
                "output frame pipeline did not terminate within 60s"
            );
            std::thread::sleep(std::time::Duration::from_millis(20));
        }
        scheduler.wait().unwrap();
        let _ = std::fs::remove_file(&out_path);
    }

    #[test]
    fn test_is_ended() {
        let _ = env_logger::builder()
            .filter_level(log::LevelFilter::Trace)
            .is_test(true)
            .try_init();

        let context = FfmpegContext::builder()
            .input("test.mp4")
            .filter_desc("hue=s=0")
            .output("output_is_ended.mp4")
            .build()
            .unwrap();

        let scheduler = FfmpegScheduler::new(context);
        let scheduler = scheduler.start().unwrap();

        sleep(Duration::from_secs(2));

        assert!(scheduler.is_ended())
    }

    // Exercises videotoolbox decode + h264_videotoolbox encode — Apple-only.
    // Gate to macOS (see test_to_stdout).
    #[cfg(target_os = "macos")]
    #[test]
    fn test_hwaccel() {
        let _ = env_logger::builder()
            .filter_level(log::LevelFilter::Trace)
            .is_test(true)
            .try_init();

        let input: Input = "test.mp4".into();
        let output: Output = "output_hwaccel.mp4".into();

        let result = FfmpegContext::builder()
            .input(input.set_hwaccel("videotoolbox"))
            .filter_desc("hue=s=0")
            .output(output.set_video_codec("h264_videotoolbox"))
            .build()
            .unwrap()
            .start()
            .unwrap()
            .wait();

        assert!(result.is_ok());
    }

    #[cfg(feature = "async")]
    #[tokio::test]
    async fn test_async() {
        let _ = env_logger::builder()
            .filter_level(log::LevelFilter::Trace)
            .is_test(true)
            .try_init();

        let context = FfmpegContext::builder()
            .input("test.mp4")
            .filter_desc("hue=s=0")
            .output("output_async.mp4")
            .build()
            .unwrap();

        let scheduler = FfmpegScheduler::new(context);
        let scheduler = scheduler.start().unwrap();
        let result = scheduler.await;
        assert!(result.is_ok());
    }

    #[test]
    fn test_pause() {
        let _ = env_logger::builder()
            .filter_level(log::LevelFilter::Trace)
            .is_test(true)
            .try_init();

        let context = FfmpegContext::builder()
            .input("test.mp4")
            .filter_desc("hue=s=0")
            .output("output_pause.mp4")
            .build()
            .unwrap();

        let scheduler = FfmpegScheduler::new(context);
        let scheduler = scheduler.start().unwrap();
        let scheduler = scheduler.pause();
        assert!(scheduler.is_state::<Paused>());
        sleep(Duration::from_secs(1));
        let scheduler = scheduler.resume();
        let result = scheduler.wait();
        assert!(result.is_ok());
    }

    #[test]
    fn test_pause_abort() {
        let _ = env_logger::builder()
            .filter_level(log::LevelFilter::Trace)
            .is_test(true)
            .try_init();

        let context = FfmpegContext::builder()
            .input("test.mp4")
            .filter_desc("hue=s=0")
            .output("output_pause_abort.mp4")
            .build()
            .unwrap();

        let scheduler = FfmpegScheduler::new(context);
        let scheduler = scheduler.start().unwrap();
        let scheduler = scheduler.pause();
        assert!(scheduler.is_state::<Paused>());
        sleep(Duration::from_secs(1));
        scheduler.abort();
    }

    #[test]
    fn test_wait() {
        let _ = env_logger::builder()
            .filter_level(log::LevelFilter::Trace)
            .is_test(true)
            .try_init();

        let context = FfmpegContext::builder()
            .input("test.mp4")
            .filter_desc("hue=s=0")
            .output("output_wait.mp4")
            .build()
            .unwrap();

        let result = FfmpegScheduler::new(context).start().unwrap().wait();
        assert!(result.is_ok());
    }

    #[test]
    fn test_status() {
        let _ = env_logger::builder()
            .filter_level(log::LevelFilter::Trace)
            .is_test(true)
            .try_init();

        let context = FfmpegContext::builder()
            .input("test.mp4")
            .filter_desc("hue=s=0")
            .output("output_status.mp4")
            .build()
            .unwrap();

        let scheduler = FfmpegScheduler::new(context);
        assert!(!scheduler.is_state::<Paused>());
        assert!(scheduler.is_state::<Initialization>());
        assert_eq!(scheduler.status.load(Ordering::Acquire), STATUS_INIT);
        let scheduler = scheduler.start().unwrap();
        assert_eq!(scheduler.status.load(Ordering::Acquire), STATUS_RUN);
        assert!(scheduler.is_state::<Running>());
        let scheduler = scheduler.pause();
        assert_eq!(scheduler.status.load(Ordering::Acquire), STATUS_PAUSE);
        assert!(scheduler.is_state::<Paused>());
        let scheduler = scheduler.resume();
        assert_eq!(scheduler.status.load(Ordering::Acquire), STATUS_RUN);
        assert!(scheduler.is_state::<Running>());
        scheduler.abort();

        sleep(Duration::from_secs(1));
    }

    #[test]
    fn test_stop() {
        let _ = env_logger::builder()
            .filter_level(log::LevelFilter::Trace)
            .is_test(true)
            .try_init();

        // A leftover file from a previous run would satisfy the wait loop
        // below before this run's muxer even opens it.
        let _ = std::fs::remove_file("output_stop.mp4");

        let context = FfmpegContext::builder()
            .input("test.mp4")
            .filter_desc("hue=s=0")
            .output("output_stop.mp4")
            .build()
            .unwrap();

        let scheduler = FfmpegScheduler::new(context);
        let scheduler = scheduler.start().unwrap();

        // Let the job process some frames before stopping: wait until the
        // muxer has flushed its first bytes so the stop() lands mid-stream on
        // any machine speed (a fixed sleep raced slow CI runners), bounded so
        // a hung pipeline still fails.
        let deadline = std::time::Instant::now() + Duration::from_secs(30);
        while !std::fs::metadata("output_stop.mp4")
            .map(|m| m.len() > 0)
            .unwrap_or(false)
        {
            assert!(
                std::time::Instant::now() < deadline,
                "pipeline wrote no output within 30s"
            );
            sleep(Duration::from_millis(50));
        }

        // stop() should block until all threads complete
        scheduler.stop();

        // Verify output file exists and has content
        let metadata = std::fs::metadata("output_stop.mp4").unwrap();
        assert!(
            metadata.len() > 0,
            "Output file should have content after stop()"
        );
    }
}
