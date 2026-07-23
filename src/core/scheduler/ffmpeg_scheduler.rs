use crate::core::context::encoder_stream::EncSyncHandle;
use crate::core::context::ffmpeg_context::FfmpegContext;
use crate::core::context::obj_pool::ObjPool;
use crate::core::scheduler::dec_task::dec_init;
use crate::core::scheduler::demux_task::demux_init;
use crate::core::scheduler::enc_task::enc_init;
use crate::core::scheduler::filter_task::filter_graph_init;
use crate::core::scheduler::frame_filter_pipeline::{input_pipeline_init, output_pipeline_init};
use crate::core::scheduler::frame_source_task::frame_source_init;
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

// publishes a muxer's `enc_registered` flag on Drop — including on UNWIND.
// start() sets the flag explicitly on the normal and enc_init-error paths, but a
// PANIC inside an enc_init (e.g. a panicking log hook) mid-registration would skip
// both, leaving the delayed-start mux waiter's registration barrier
// (MuxRegistrationBarrier) blocked forever. This RAII, held across a muxer's
// enc_init loop, republishes the flag as the stack unwinds so that barrier can
// never hang. It drops BEFORE the StartFailGuard (declared earlier in start()),
// so the waiter the guard joins can always pass its barrier.
struct SetEncRegisteredOnDrop(Arc<AtomicBool>);

impl Drop for SetEncRegisteredOnDrop {
    fn drop(&mut self) {
        self.0.store(true, Ordering::Release);
    }
}

/// Test-only observation of the settlement sentinel's release moment. The
/// release is the admission edge for packet sinks parked in the settlement
/// barrier: whatever the scheduler state is at that instant is what an
/// admitted sink's terminal decision (on_end vs a reported failure) can
/// sample. A test thread woken BY the release inherently races the releasing
/// thread's next statements, so these snapshots are taken on the releasing
/// thread itself, inside `release_sentinel`, where they are exact.
///
/// Slots are keyed by the scheduler-status Arc's address: a test arms its
/// own scheduler and concurrent tests' unarmed schedulers pass through
/// untouched. The armed test keeps its status Arc alive from `arm` to
/// `take`, so the address cannot be reused by another scheduler in between.
#[cfg(test)]
pub(crate) mod start_tail_probe {
    use std::sync::Mutex;

    #[derive(Clone, Copy)]
    pub(crate) struct ReleaseSnapshot {
        /// Whether a scheduler result was already recorded when the sentinel
        /// released. The failure path must record BEFORE releasing; the
        /// success tail must release with nothing recorded.
        pub(crate) result_recorded: bool,
        /// Whether the scheduler status was already terminal at the release.
        pub(crate) status_stopping: bool,
        /// Whether the RunningGuard had been installed when the release ran.
        /// On the success tail the release must sit AFTER the installation:
        /// only then is the fallible remainder of start() behind it, and the
        /// failure duty already has a taker for the scheduler's lifetime.
        pub(crate) running_guard_installed: bool,
    }

    struct Slot {
        status_ptr: usize,
        running_guard_installed: bool,
        release: Option<ReleaseSnapshot>,
    }

    static SLOTS: Mutex<Vec<Slot>> = Mutex::new(Vec::new());

    /// Starts observing the scheduler whose status Arc lives at `status_ptr`.
    pub(crate) fn arm(status_ptr: usize) {
        let mut slots = SLOTS.lock().unwrap_or_else(std::sync::PoisonError::into_inner);
        slots.retain(|slot| slot.status_ptr != status_ptr);
        slots.push(Slot {
            status_ptr,
            running_guard_installed: false,
            release: None,
        });
    }

    /// Called by `start()` immediately after installing the RunningGuard.
    pub(crate) fn note_running_guard_installed(status_ptr: usize) {
        let mut slots = SLOTS.lock().unwrap_or_else(std::sync::PoisonError::into_inner);
        if let Some(slot) = slots.iter_mut().find(|slot| slot.status_ptr == status_ptr) {
            slot.running_guard_installed = true;
        }
    }

    /// Called by `release_sentinel` at the instant it commits to releasing.
    /// First release wins — `release_sentinel` is idempotent and only the
    /// releasing call is an admission edge.
    pub(crate) fn record_release(status_ptr: usize, result_recorded: bool, status_stopping: bool) {
        let mut slots = SLOTS.lock().unwrap_or_else(std::sync::PoisonError::into_inner);
        if let Some(slot) = slots.iter_mut().find(|slot| slot.status_ptr == status_ptr) {
            if slot.release.is_none() {
                slot.release = Some(ReleaseSnapshot {
                    result_recorded,
                    status_stopping,
                    running_guard_installed: slot.running_guard_installed,
                });
            }
        }
    }

    /// Stops observing and returns the release-moment snapshot, if the
    /// sentinel was released while armed.
    pub(crate) fn take(status_ptr: usize) -> Option<ReleaseSnapshot> {
        let mut slots = SLOTS.lock().unwrap_or_else(std::sync::PoisonError::into_inner);
        let idx = slots.iter().position(|slot| slot.status_ptr == status_ptr)?;
        slots.remove(idx).release
    }
}

/// Failure cleanup for `start()` once one or more worker threads may already be
/// running, as a guard so it covers BOTH the explicit `Err` returns and a panic
/// unwinding out of any init call (e.g. a user log hook panicking on the
/// "input does not need to be sent" warning after earlier workers spawned).
/// Drop publishes the terminal state, wakes every blocking wait, releases any
/// pre-counted muxer thread slot no worker will free, and JOINS all workers.
///
/// Joining before `start()` drops `self.ffmpeg_context` is what prevents a
/// use-after-free: that context is the sole owner of the `InterruptState` the
/// workers' AVIO interrupt callbacks dereference (via a refcount-free raw
/// pointer). A worker finishing its trailer I/O after the context was freed
/// would otherwise read and write freed memory. The guard is a body local of
/// `start()`, so on unwind it drops before the `self` parameter (locals drop
/// first) — the join always precedes the context teardown. `mux_handed[i]` is
/// true once muxer `i`'s pre-counted slot has been handed to a worker or waiter
/// (which will release it on STATUS_END); the rest are released here so
/// `wait_for_all_threads` cannot block forever on a slot no thread owns.
/// The success path releases the settlement sentinel and disarms the guard
/// back-to-back once the RunningGuard has taken over — nothing fallible in
/// between, so a settlement waiter admitted by that release can never
/// observe a clean job that a later unwind repaints as StartFailed.
struct StartFailGuard {
    armed: bool,
    status: Arc<AtomicUsize>,
    result: Arc<Mutex<Option<crate::error::Result<()>>>>,
    thread_sync: ThreadSynchronizer,
    // Cloned upfront: Drop cannot borrow ffmpeg_context (start() holds it
    // mutably), and the demuxer set is fixed before any worker spawns.
    demux_waiters: Vec<Arc<crate::util::sch_waiter::SchWaiter>>,
    mux_handed: Vec<bool>,
    // Whether the settlement sentinel slot (claimed in start() before the
    // muxer pre-count) is still held. True from construction until
    // release_sentinel() runs — on the success path at start()'s infallible
    // tail, immediately before the disarm; on the failure path inside Drop
    // AFTER the failure is recorded.
    sentinel_held: bool,
}

impl StartFailGuard {
    /// Releases the settlement sentinel slot exactly once, reopening the
    /// packet-sink settlement barrier (`live <= settled`). Like any worker
    /// slot release, it must publish the terminal state when it turns out to
    /// be the last live slot (a job whose muxers all finished inside
    /// `mux_init`, e.g. every output streamless), or a woken `wait()` would
    /// observe a non-terminal status.
    fn release_sentinel(&mut self) {
        if !self.sentinel_held {
            return;
        }
        // Snapshot what a settlement waiter admitted by THIS release could
        // observe, taken here on the releasing thread so ordering tests
        // need not race it. Compiled out of production builds.
        #[cfg(test)]
        start_tail_probe::record_release(
            Arc::as_ptr(&self.status) as usize,
            self.result
                .lock()
                .unwrap_or_else(std::sync::PoisonError::into_inner)
                .is_some(),
            is_stopping(self.status.load(Ordering::Acquire)),
        );
        self.sentinel_held = false;
        let status = self.status.clone();
        self.thread_sync.thread_done_with(|| {
            status.store(STATUS_END, Ordering::Release);
        });
    }
}

impl Drop for StartFailGuard {
    fn drop(&mut self) {
        if !self.armed {
            return;
        }
        // Record the start failure BEFORE publishing the terminal status or
        // releasing any slot (first-error-wins keeps a more specific error a
        // worker recorded earlier). start() returns the actual init error to
        // its caller; this recorded error exists for CONCURRENT observers: a
        // fully-drained packet sink parked in the settlement barrier is
        // admitted by the releases below and samples the scheduler result
        // right after — with nothing recorded, it would deliver `on_end` for
        // a job whose start() failed, and a truncated sink would stay silent
        // instead of reporting the failure.
        set_scheduler_error(&self.status, &self.result, crate::error::Error::StartFailed);
        self.status.store(STATUS_END, Ordering::Release);
        notify_pause_waiters();
        // Wake choked demuxers so they observe the terminal state and exit
        // (mirrors RunningGuard::drop).
        for waiter in &self.demux_waiters {
            waiter.set(false);
        }
        for &handed in &self.mux_handed {
            if !handed {
                crate::core::scheduler::mux_task::release_mux_slot(&self.status, &self.thread_sync);
            }
        }
        // Only after the failure is published: a settlement waiter this
        // release admits must observe the recorded error, never a clean
        // terminal state.
        self.release_sentinel();
        self.thread_sync.wait_for_all_threads();
    }
}

// Internal wrapper for Running state to enable Drop implementation.
//
// When a FfmpegScheduler<Running> (or Paused) is dropped, this guard signals the
// job to terminate and blocks until every tracked worker has released its slot.
// A released slot does not mean the OS thread has fully unwound (a detached
// closure can keep destructing its captures afterward); lifetime safety relies on
// each worker finishing its dangerous resource access before releasing its slot,
// and on this guard keeping `_interrupt_state` alive until then. It does NOT by
// itself guarantee a finalized output: a normal (non-abort) mux teardown only
// ATTEMPTS the trailer, and abort, a spawn failure, a failing trailer, or a
// streamless output can leave an output unfinalized.
//
// IMPORTANT: Drop runs whenever the scheduler is dropped, including while a panic
// unwinds. It is skipped only on non-unwinding termination — `panic = "abort"`,
// std::process::abort(), or std::process::exit() — where workers may be left
// mid-write and files corrupted.
//
// The guard is passed through state transitions (Running -> Paused -> Running)
// via into_state() to maintain Drop protection across all active states.
struct RunningGuard {
    status: Arc<AtomicUsize>,
    thread_sync: ThreadSynchronizer,
    // Demuxer choke waiters: a stopping scheduler must notify them or a
    // choked demuxer sleeps through the shutdown and joining hangs.
    demux_waiters: Vec<Arc<crate::util::sch_waiter::SchWaiter>>,
    // Keeps the interrupt-callback state alive until every tracked worker has
    // released its slot: FfmpegScheduler's ffmpeg_context field (the other Arc holder)
    // drops BEFORE this guard, while threads may still be inside blocking
    // I/O whose AVIOInterruptCB points into this allocation.
    _interrupt_state: Arc<crate::core::context::InterruptState>,
}

impl Drop for RunningGuard {
    /// Signals termination and blocks until every tracked worker releases its
    /// slot, keeping `_interrupt_state` alive until then so a worker still inside
    /// a blocking custom-IO callback never dereferences freed state. Finalization
    /// on a normal teardown is best-effort, not guaranteed here.
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

        // Always wait for all tracked workers to release their slots
        // This is safe to call multiple times - if the counter is already zero,
        // wait_for_all_threads() returns immediately without blocking.
        // This ensures cleanup works correctly after both abort() and stop().
        log::debug!("Drop waiting for tracked workers to release their slots");
        self.thread_sync.wait_for_all_threads();
    }
}

pub struct FfmpegScheduler<S> {
    ffmpeg_context: FfmpegContext,
    status: Arc<AtomicUsize>,
    /// Per-scheduler seqlock pause parity: settled even = running, odd = paused. pause() bumps even->odd BEFORE flipping status and resume() bumps odd->even AFTER its CAS, so the epoch is odd across both transition windows and `status == PAUSE` never coexists with an even epoch. A pre-mux worker charges a wait slice only when this is even and unchanged across it, so paused time never advances the backstop.
    pause_epoch: Arc<AtomicUsize>,
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
            pause_epoch: self.pause_epoch,
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

    /// Test-only: how many threads are currently parked in the settlement
    /// barrier (`wait_peers_settled`). Lets a test prove a terminal
    /// coordinator is genuinely parked while the rest of the job runs,
    /// without exposing the synchronizer itself.
    #[cfg(test)]
    pub(crate) fn parked_settlement_waiters(&self) -> usize {
        self.thread_sync.parked_settlement_waiters()
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
            pause_epoch: Arc::new(AtomicUsize::new(0)),
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
        let pause_epoch = self.pause_epoch.clone();
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

        // Settlement sentinel: one extra tracked slot, claimed before the
        // first registration below and held by this thread until start()
        // can no longer fail — the success path releases it (via
        // `release_sentinel`) at the infallible tail, immediately before
        // the guard disarm; the failure path records the error FIRST — see
        // StartFailGuard::drop. While it is live and never settled, the
        // packet-sink settlement barrier (`live <= settled`,
        // thread_synchronizer.rs) cannot pass, so no fully-drained sink can
        // decide its terminal (on_end vs JobFailed) against a job that can
        // still fail: the terminal decision always samples a job whose
        // start() either committed to success or failed with the error
        // already recorded.
        thread_sync.thread_start();

        // Pre-count EVERY muxer's thread slot before any `mux_init` runs. A
        // streamless (AVFMT_NOSTREAMS) output releases its slot synchronously
        // inside `mux_init` (`release_mux_slot`); counted one-at-a-time, an
        // early streamless output could drive the thread counter to zero and
        // publish a premature STATUS_END before later muxers were even counted,
        // which stops (truncates) those still-pending outputs. Counting all
        // muxers up front keeps the counter > 0 until every muxer — and every
        // later worker — is genuinely done.
        //
        // Muxers are the ONLY workers whose slots need pre-counting. Every
        // other worker (encoder, filter graph, frame pipeline, decoder,
        // demuxer, frame source) claims its slot on THIS thread inside its
        // own init call, strictly before its worker thread spawns, and none
        // of those inits releases a slot synchronously — so the counter never
        // dips as registration proceeds. Their later registration is also
        // invisible to every counter consumer: wait()/stop() cannot run yet
        // (no Running handle exists before start() returns), and a packet
        // sink cannot outrun the registration sequence to a premature on_end,
        // twice over. First, a sink's completion requires a per-stream EOF
        // marker for EVERY stream, and those originate only in producer
        // workers (encoders, or a demuxer's recording-time EOF) — each stage's
        // channel senders stay parked inside FfmpegContext until that stage's
        // init (slot claimed first) moves them into its worker, so neither an
        // EOF marker nor even a disconnect is observable before the whole
        // producing chain, up to the sources spawned by the LAST init loops,
        // has registered. Second, the settlement sentinel above holds the
        // barrier shut for the whole window regardless.
        for _ in 0..self.ffmpeg_context.muxs.len() {
            thread_sync.thread_start();
        }
        // From here on worker threads may be running: any exit from start()
        // other than the disarm at the very end — an explicit `return Err` OR
        // a panic unwinding out of an init call — must publish the terminal
        // state, release the pre-counted slots no worker owns
        // (`mux_handed[i] == false`), and join every spawned worker BEFORE
        // `self.ffmpeg_context` drops. The guard's Drop does exactly that.
        let mut start_fail_guard = StartFailGuard {
            armed: true,
            status: scheduler_status.clone(),
            result: scheduler_result.clone(),
            thread_sync: thread_sync.clone(),
            demux_waiters: self
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
                .collect(),
            mux_handed: vec![false; self.ffmpeg_context.muxs.len()],
            sentinel_held: true,
        };

        // Muxer
        for (mux_idx, mux) in self.ffmpeg_context.muxs.iter_mut().enumerate() {
            if mux.is_ready() {
                // Calling mux_init hands this slot off: on success a worker
                // releases it, on failure fail_mux_init releases it internally.
                start_fail_guard.mux_handed[mux_idx] = true;
                mux_init(
                    mux_idx,
                    mux,
                    packet_pool.clone(),
                    input_controller.clone(),
                    mux.mux_stream_nodes.clone(),
                    scheduler_status.clone(),
                    thread_sync.clone(),
                    scheduler_result.clone(),
                    mux_done_remaining.clone(),
                )?;
            }
        }

        // Output frame filter pipeline
        let ffmpeg_context = &mut self.ffmpeg_context;
        for (mux_idx, mux) in ffmpeg_context.muxs.iter_mut().enumerate() {
            if let Some(frame_pipelines) = mux.frame_pipelines.take() {
                for frame_pipeline in frame_pipelines {
                    output_pipeline_init(
                        mux_idx,
                        frame_pipeline,
                        mux.get_streams_mut(),
                        frame_pool.clone(),
                        scheduler_status.clone(),
                        thread_sync.clone(),
                        scheduler_result.clone(),
                    )?;
                }
            }
        }

        // Encoder
        let ffmpeg_context = &mut self.ffmpeg_context;
        for (mux_idx, mux) in &mut ffmpeg_context.muxs.iter_mut().enumerate() {
            // republish registration-complete on unwind. Armed at the
            // very TOP of this muxer's iteration — BEFORE ready_to_init_mux spawns the
            // delayed-start waiter — so it covers the whole window: waiter handoff, the
            // -shortest sync-queue setup, AND the enc_init loop below. A panic anywhere
            // after the waiter is spawned would otherwise leave the waiter's registration
            // barrier (MuxRegistrationBarrier) blocked forever, retaining the output
            // context and its pre-counted slot. The explicit stores (enc_init error arm;
            // after the loop) still run first on the normal and error paths.
            let _enc_registered_on_unwind = SetEncRegisteredOnDrop(mux.enc_registered.clone());
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
                Ok(sender) => {
                    // A not-ready muxer's slot is now owned by the spawned
                    // waiter thread; a ready muxer was already marked handed in
                    // the loop above (setting it again is idempotent).
                    start_fail_guard.mux_handed[mux_idx] = true;
                    sender
                }
                Err(e) => {
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
                    pause_epoch.clone(),
                    thread_sync.clone(),
                    mux.enc_handle_sender(),
                    scheduler_result.clone(),
                    input_controller.clone(),
                ) {
                    // Registration is over for this muxer (nothing more will
                    // be spawned) — signal it BEFORE the guard joins the
                    // delayed-start waiter, whose teardown waits for the flag
                    // so its encoder join cannot miss an in-flight handle.
                    mux.enc_registered.store(true, Ordering::Release);
                    return Err(e);
                }
            }
            // Every enc_init for this muxer returned: every encoder handle it
            // will ever have is queued. The delayed-start waiter's teardown
            // holds for this flag before joining/freeing.
            mux.enc_registered.store(true, Ordering::Release);
        }

        // Filter graph
        let ffmpeg_context = &mut self.ffmpeg_context;
        for (i, filter_graph) in ffmpeg_context.filter_graphs.iter_mut().enumerate() {
            filter_graph_init(
                i,
                filter_graph,
                frame_pool.clone(),
                input_controller.clone(),
                filter_graph.node.clone(),
                scheduler_status.clone(),
                thread_sync.clone(),
                scheduler_result.clone(),
            )?;
        }

        // Input frame filter pipeline
        for (demux_idx, demux) in ffmpeg_context.demuxs.iter_mut().enumerate() {
            if let Some(frame_pipelines) = demux.frame_pipelines.take() {
                for frame_pipeline in frame_pipelines {
                    input_pipeline_init(
                        demux_idx,
                        frame_pipeline,
                        demux.get_streams_mut(),
                        frame_pool.clone(),
                        scheduler_status.clone(),
                        thread_sync.clone(),
                        scheduler_result.clone(),
                    )?;
                }
            }
        }

        // Decoder
        for (demux_idx, demux) in &mut ffmpeg_context.demuxs.iter_mut().enumerate() {
            let exit_on_error = demux.exit_on_error;

            for dec_stream in demux.get_streams_mut() {
                dec_init(
                    demux_idx,
                    dec_stream,
                    exit_on_error,
                    frame_pool.clone(),
                    packet_pool.clone(),
                    scheduler_status.clone(),
                    thread_sync.clone(),
                    scheduler_result.clone(),
                )?;
            }
        }

        // Demuxer
        for (demux_idx, demux) in ffmpeg_context.demuxs.iter_mut().enumerate() {
            demux_init(
                demux_idx,
                demux,
                ffmpeg_context.independent_readrate,
                packet_pool.clone(),
                demux.node.clone(),
                scheduler_status.clone(),
                pause_epoch.clone(),
                thread_sync.clone(),
                scheduler_result.clone(),
            )?;
        }

        // Frame sources (VideoWriter): spawned LAST, so the worker's entire
        // consumer chain (filter -> encoder -> mux) already exists. Inside the
        // StartFailGuard window: on any later failure (none exists today —
        // this loop is the final init) the pre-claimed slot is joined, and the
        // worker's 100 ms status polls make that join terminate. No
        // demux-keyed wake entry is needed for the same reason.
        for (i, frame_source) in ffmpeg_context.frame_sources.drain(..).enumerate() {
            frame_source_init(
                i,
                frame_source,
                frame_pool.clone(),
                scheduler_status.clone(),
                thread_sync.clone(),
                scheduler_result.clone(),
            )?;
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
        // Test-only: the sentinel release below must find this installation
        // already done; tests pin that ordering through the real start().
        #[cfg(test)]
        start_tail_probe::note_running_guard_installed(
            Arc::as_ptr(&running_scheduler.status) as usize,
        );

        // Registration is complete (every worker this job will ever track
        // claimed its slot above) and the RunningGuard is installed: reopen
        // the packet-sink settlement barrier. A drained sink this release
        // admits samples the job result immediately and may dispatch on_end,
        // so the release must sit where start() can no longer fail. Released
        // before the balancing update and the Running-state construction
        // above, a panic unwinding out of that remainder (say the balancing
        // lock poisoned by a concurrently panicking mux worker) would record
        // StartFailed AFTER that on_end — two contradictory terminals for
        // one job. Nothing fallible may sit between this release and the
        // disarm below.
        start_fail_guard.release_sentinel();

        // Failure duty is now the RunningGuard's: it performs the same
        // terminal-state + join protocol for the scheduler's whole lifetime.
        start_fail_guard.armed = false;

        Ok(running_scheduler)
    }

    /// Cleans up Muxers/Demuxers and signals the job to end if an error occurs
    /// during initialization. This is invoked internally when `start()` fails.
    fn cleanup(scheduler_status: &Arc<AtomicUsize>, _ffmpeg_context: &FfmpegContext) {
        // Early-failure path with no worker threads spawned yet (object-pool
        // allocation): just publish the terminal state. Contexts still owned by
        // Demuxer/Muxer are freed by their Drop when the FfmpegContext goes down
        // with the failed scheduler.
        scheduler_status.store(STATUS_END, Ordering::Release);
        notify_pause_waiters();
    }
}

impl FfmpegScheduler<Running> {
    /// Whether this scheduler's job contains the channel-adapter packet sink
    /// identified by `token` (Arc pointer identity of its `CancellationSlot`).
    /// Backs the [`PacketSinkReceiver::into_events`] pairing check; the
    /// tokens live on the muxers (never taken by the worker handoff), so the
    /// check is race-free against a running job.
    ///
    /// [`PacketSinkReceiver::into_events`]: crate::core::packet_sink::PacketSinkReceiver::into_events
    pub(crate) fn runs_packet_sink(
        &self,
        token: &crate::core::packet_sink::CancellationSlot,
    ) -> bool {
        self.ffmpeg_context.muxs.iter().any(|mux| {
            mux.packet_sink_token
                .as_ref()
                .is_some_and(|t| Arc::ptr_eq(t, token))
        })
    }

    /// Pauses a running FFmpeg job, transitioning from `Running` to `Paused`.
    ///
    /// Internally sets the FFmpeg pipeline threads to a paused state. Depending
    /// on your FFmpeg pipeline design, it may take a moment for all threads to
    /// acknowledge this request. If the scheduler is already ended, this does nothing.
    ///
    /// # Returns
    /// A new [`FfmpegScheduler<Paused>`] representing the paused job.
    pub fn pause(self) -> FfmpegScheduler<Paused> {
        // Seqlock-style pause epoch: even = running, odd = paused. Bump it
        // even->odd to mark entering pause BEFORE flipping status, so any pre-mux
        // wait slice overlapping this pause sees an odd (or moved) epoch and is
        // excluded from the deferred-start backstop — there is never a window where
        // status reads PAUSE while the epoch still reads even. A failed CAS (a
        // terminal state raced in) leaves the epoch odd, which is harmless either
        // way: a stopping worker returns Disconnected via is_stopping before the
        // backstop is ever consulted, so the parity need not be restored.
        self.pause_epoch.fetch_add(1, Ordering::Release);
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
    /// Always available, with or without the `async` feature: the feature is
    /// additive (as Cargo feature unification requires) and only ADDS a
    /// `Future` implementation on `FfmpegScheduler<Running>`, so async code
    /// can `scheduler.await` as a non-blocking alternative to calling this
    /// method. Enabling `async` anywhere in a dependency graph never removes
    /// or changes `wait()` for sync callers.
    ///
    /// # Returns
    /// - `Ok(())` if the job completed successfully.
    /// - `Err(...)` if an error was encountered (also logs the error).
    ///
    /// # Example
    /// ```rust,ignore
    /// // After calling `.start()`:
    /// let result = scheduler.wait();
    /// assert!(result.is_ok());
    /// ```
    pub fn wait(self) -> crate::error::Result<()> {
        // Always drain the thread counter before reading the result — the same
        // discipline as stop(). On a mux panic the terminal status and the error
        // can be published in either order (a normal unwind records the error
        // first via _panic_status; a sibling/stop, or a teardown-destructor panic
        // after mux_done was dropped, can flip the status first), so reading the
        // result on status alone can capture a mid-unwind Ok(()). Every worker
        // releases its thread slot only AFTER recording its terminal state, so
        // counter==0 means the result is fully settled. Storing STATUS_END is
        // still gated so it never clobbers a terminal ABORT.
        self.thread_sync.wait_for_all_threads();
        if !is_stopping(self.status.load(Ordering::Acquire)) {
            self.status.store(STATUS_END, Ordering::Release);
        }

        take_scheduler_result(&self.result)
    }

    /// **WARNING: Hard-aborts the FFmpeg job. A worker that observes the abort at
    /// its cleanup gate skips its flush/trailer, and an in-progress finalize can
    /// be interrupted, so output usability is not guaranteed.**
    ///
    /// This publishes a hard-abort signal, then blocks until every tracked worker
    /// releases its slot (the scheduler's guard waits on drop). A
    /// worker samples the status at its cleanup gate: if it observes
    /// `STATUS_ABORT` there, it skips its encoder flush / muxer trailer. Already
    /// written data is not removed, but a finalize already in flight can be cut
    /// short — `STATUS_ABORT` trips the output interrupt callback immediately,
    /// bypassing the trailer grace window, so an in-progress write may fail or
    /// truncate. It is NOT a non-blocking call, and the wait has no fixed time
    /// bound — a codec, filter, or blocking custom-IO callback that is mid-call
    /// must still return first. Relative to `stop()`:
    ///
    /// - Encoder-buffered frames (B-frames / delayed frames) may be lost
    /// - A muxer left without its trailer may not be seekable/playable — this
    ///   depends on the format (a streamable one like MPEG-TS may still play; an
    ///   MP4 needs its moov atom)
    /// - Data already written to disk is NOT removed: aborting a job whose workers
    ///   had already finished naturally leaves the finalized files intact
    ///
    /// # When to Use
    ///
    /// - Emergency shutdown where you do not need the (possibly partial) output
    /// - Preview/test runs where output is discarded
    /// - User cancellation where output is not needed
    ///
    /// For finalized files, use [`stop()`](Self::stop), or let a finite input
    /// finish naturally and check [`wait()`](Self::wait) returns `Ok` — both let
    /// the normal encoder drain and post-header finalization run (and report a
    /// failure), which `abort()` can short-circuit when it observes the abort at
    /// its cleanup gate.
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

    /// Gracefully stops the FFmpeg job, blocking until every tracked worker has
    /// released its slot, then returns any error a worker recorded.
    ///
    /// # What is finalized
    ///
    /// On the normal (non-abort) mux-worker teardown, an output that has already
    /// written its header has `av_write_trailer` attempted on it — usually
    /// producing a well-formed container (e.g. the MP4 moov atom, MKV Cues),
    /// though a failing trailer write surfaces as `Err` and a worker panic before
    /// the trailer can leave the output without one. A streamless
    /// (`AVFMT_NOSTREAMS`) output writes no header or trailer at all. If `stop()`
    /// is called after `start()` succeeds but BEFORE an output's muxer has
    /// initialized, that output may be left empty or never created while `stop()`
    /// can still return `Ok(())` (a mux-worker spawn failure instead records
    /// `Err`). Whether a finalized file is seekable/playable then depends on the
    /// format and, for custom-IO / URL outputs, the sink — not an unconditional
    /// guarantee.
    ///
    /// # In-flight frames are NOT drained into the output
    ///
    /// `stop()` is a prompt teardown, not a pipeline flush. An opened, unfinished,
    /// non-subtitle encoder attempts a lookahead flush on a graceful stop
    /// (`avcodec_send_frame(NULL)`),
    /// but by then the muxer may already have torn down — the first flushed packet
    /// meeting a disconnected receiver ends the drain — so those packets, together
    /// with anything still queued toward the muxer when `stop()` was called, may
    /// be discarded and the media tail may end earlier than it would after a full
    /// drain. How much is lost depends on queue occupancy, the stream time base,
    /// and filter and encoder delay; there is no fixed media-duration bound.
    ///
    /// To avoid the extra tail loss `stop()` introduces, let a finite input
    /// reach its natural end and confirm [`wait()`](Self::wait) returns `Ok`
    /// rather than calling `stop()` mid-stream. (Natural completion still omits
    /// whatever `-shortest`, frame/time limits, filters, or errors drop.)
    ///
    /// # Comparison with abort()
    ///
    /// | Method | Signal | Blocks | Trailer | In-flight frames |
    /// |--------|--------|----------------|---------|------------------|
    /// | `stop()`  | graceful | Yes | attempted (normal teardown) | tail may be dropped |
    /// | `abort()` | hard     | Yes | not guaranteed              | may be lost |
    ///
    /// Both block until every tracked worker releases its slot (the scheduler's
    /// guard waits on drop), with no fixed time bound. A worker that observes the
    /// abort at its cleanup gate skips its flush/trailer, and an in-progress
    /// finalize may be interrupted, so `abort()`'s output is not guaranteed
    /// usable — but it usually returns faster with nothing left to finalize.
    ///
    /// # Returns
    ///
    /// - `Ok(())` — no worker recorded an error. Only an output that wrote a
    ///   header AND reached the normal mux-worker trailer path completed a
    ///   trailer; a streamless (`AVFMT_NOSTREAMS`) or pre-init output finishes
    ///   without one, and a mux-worker spawn failure records `Err` instead.
    /// - `Err(...)` — the first error any worker recorded, including trailer
    ///   write failures a `()`-returning stop() previously could not report.
    ///
    /// # Blocking caveat
    ///
    /// While an output is finalizing (e.g. a `movflags=+faststart` trailer
    /// rewriting the file), `stop()` blocks without a timeout rather than
    /// cutting the write and corrupting the output. `stop()` consumes the
    /// scheduler, so no handle is left to `abort()` a slow finalize; choosing
    /// `abort()` up front lets a worker skip any finalization it has not reached
    /// at its cleanup gate, though its own wait can still block indefinitely (see
    /// [`abort()`](Self::abort)).
    ///
    /// # Example
    /// ```rust,ignore
    /// let running_scheduler = scheduler.start().unwrap();
    /// // ... processing ...
    /// running_scheduler.stop()?; // Blocks; normal teardown attempts finalization
    /// ```
    pub fn stop(self) -> crate::error::Result<()> {
        self.signal_stop();
        self.thread_sync.wait_for_all_threads();
        log::debug!("stop() completed, all tracked workers released their slots");
        take_scheduler_result(&self.result)
    }
}

/// Takes the recorded scheduler outcome exactly once: `None` means no worker
/// recorded an error — success. A worker that panicked while holding the
/// lock must surface as an error on the caller's thread, not as a second
/// panic, hence the poison recovery. Shared by wait(), stop(), and the async
/// Future.
fn take_scheduler_result(
    result: &Arc<Mutex<Option<crate::error::Result<()>>>>,
) -> crate::error::Result<()> {
    let option = result
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
    ///     // Resolves when the job finishes — the non-blocking counterpart
    ///     // of the always-available synchronous `wait()`.
    ///     let result = scheduler.await;
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
            std::task::Poll::Ready(take_scheduler_result(result))
        }

        let this = self.get_mut();

        // Gate readiness on every tracked worker having released its slot, NOT on the
        // terminal status: on a mux panic the status and the error can be
        // published in either order, so a status-only check can complete the
        // Future with a mid-unwind Ok(()). The counter reaching 0 means every
        // worker released its slot AFTER recording its terminal state; the
        // thread-done waker fires exactly then.
        if this.thread_sync.all_threads_done() {
            return ready(&this.result);
        }

        let thread_sync = this.thread_sync.clone();
        let waker = cx.waker().clone();
        thread_sync.set_waker(waker);

        // Re-check after registering the waker: the last tracked slot may be released
        // (and consumed a previous waker) between the check above and set_waker,
        // in which case no further wake ever comes for the waker just installed.
        if this.thread_sync.all_threads_done() {
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
            // Seqlock parity (see pause()): flip the epoch odd->even AFTER the status
            // CAS, so the transition window is (status RUN, epoch odd) — a pre-mux
            // worker reads that odd baseline as still-paused and conservatively
            // EXCLUDES the slice rather than mischarging it. pause() is the mirror: it
            // bumps even->odd BEFORE its CAS, so its window is odd too. The bump
            // precedes the notify below, so a woken worker already reads an even
            // baseline. A failed CAS is terminal-only and leaves the epoch odd, which
            // is harmless — a stopping worker exits via is_stopping before the
            // backstop is ever consulted.
            self.pause_epoch.fetch_add(1, Ordering::Release);
            // Wake workers parked in wait_until_not_paused so they resume
            // immediately instead of after the safety-net timeout (conc-06).
            notify_pause_waiters();
        }
        self.into_state()
    }

    /// **WARNING: Hard-aborts the paused FFmpeg job. A worker that observes the
    /// abort at its cleanup gate skips its flush/trailer, and an in-progress
    /// finalize can be interrupted, so output usability is not guaranteed.**
    ///
    /// Same behavior and consequences as [`FfmpegScheduler<Running>::abort()`],
    /// including that it blocks until every tracked worker releases its slot (via
    /// the scheduler guard) — it is NOT a non-blocking call. In short: encoder-buffered frames
    /// may be lost and a muxer may be left without its trailer, so the output is
    /// not guaranteed seekable/playable. For finalized files,
    /// [`resume()`](Self::resume) first; then either call
    /// [`stop()`](FfmpegScheduler::stop), or let a finite input finish naturally
    /// and check `wait()` (a paused job cannot advance on its own).
    ///
    /// See [`FfmpegScheduler<Running>::abort()`] for the full contract.
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

/// Records `error` as the scheduler result if none is set, WITHOUT touching
/// the scheduler status — for panic paths whose status publication is owned
/// by a no-downgrade guard (the mux panic guard must never overwrite
/// STATUS_ABORT, which plain set_scheduler_error would).
pub(crate) fn set_scheduler_result_only(
    scheduler_result: &Arc<Mutex<Option<crate::error::Result<()>>>>,
    error: impl Into<crate::error::Error>,
) {
    let mut scheduler_result = scheduler_result
        .lock()
        .unwrap_or_else(std::sync::PoisonError::into_inner);
    if scheduler_result.is_none() {
        scheduler_result.replace(Err(error.into()));
    }
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
        // Publish STATUS_END WITHOUT downgrading an abort: a worker error (or
        // panic) racing abort() used to overwrite STATUS_ABORT, and a muxer
        // observing the downgraded state would flush encoders and write a
        // trailer — the very I/O abort() exists to skip. Same no-downgrade
        // CAS as MuxDoneGuard.
        let mut current = scheduler_status.load(Ordering::Acquire);
        while !is_stopping(current) {
            match scheduler_status.compare_exchange_weak(
                current,
                STATUS_END,
                Ordering::AcqRel,
                Ordering::Acquire,
            ) {
                Ok(_) => break,
                Err(actual) => current = actual,
            }
        }
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

    /// `SetEncRegisteredOnDrop` must publish `enc_registered` on Drop —
    /// including on UNWIND — so a panic inside an `enc_init` mid-registration cannot
    /// leave the mux waiter's `MuxRegistrationBarrier` blocked forever.
    #[test]
    fn set_enc_registered_publishes_on_drop_and_on_unwind() {
        use super::SetEncRegisteredOnDrop;
        use std::sync::atomic::AtomicBool;

        let normal = Arc::new(AtomicBool::new(false));
        {
            let _g = SetEncRegisteredOnDrop(normal.clone());
            assert!(
                !normal.load(Ordering::Acquire),
                "must not publish before drop"
            );
        }
        assert!(
            normal.load(Ordering::Acquire),
            "must publish on normal drop"
        );

        let unwind = Arc::new(AtomicBool::new(false));
        let f = unwind.clone();
        let _ = std::panic::catch_unwind(std::panic::AssertUnwindSafe(move || {
            let _g = SetEncRegisteredOnDrop(f);
            panic!("simulated start() panic mid-registration");
        }));
        assert!(
            unwind.load(Ordering::Acquire),
            "must publish enc_registered on unwind"
        );
    }

    /// The settlement sentinel must hold the packet-sink barrier shut for the
    /// whole registration window, and a failing start() must record the
    /// scheduler error BEFORE the sentinel release admits a parked waiter — a
    /// fully-drained sink sampling the job right after admission must observe
    /// the failure, never a clean terminal state it would report as on_end.
    ///
    /// Two mechanisms make this deterministic rather than schedule-lucky:
    /// the guard drops only once the sink is provably PARKED in the wait
    /// (not merely registered — a delayed sink that parks after the whole
    /// failure path finished would sample the error trivially and prove no
    /// ordering), and the record-before-release order is asserted on the
    /// release-moment snapshot taken by the releasing thread itself, which
    /// no sink-side scheduling can blur.
    #[test]
    fn start_fail_guard_publishes_the_failure_before_admitting_settlement_waiters() {
        use super::{is_stopping, start_tail_probe, StartFailGuard};
        use crate::util::thread_synchronizer::ThreadSynchronizer;
        use std::sync::atomic::{AtomicBool, AtomicUsize};

        let sync = ThreadSynchronizer::new();
        let status = Arc::new(AtomicUsize::new(STATUS_RUN));
        let result: Arc<Mutex<Option<crate::error::Result<()>>>> = Arc::new(Mutex::new(None));
        start_tail_probe::arm(Arc::as_ptr(&status) as usize);

        // start(): the sentinel slot, then a packet-sink worker's slot.
        sync.thread_start();
        sync.thread_start();

        // A locally-complete sink: registers settled, parks at the barrier,
        // and samples the job state right after admission — the terminal
        // decision's exact shape.
        let observed_failure = Arc::new(AtomicBool::new(false));
        let admitted = Arc::new(AtomicBool::new(false));
        let (registered_tx, registered_rx) = std::sync::mpsc::channel();
        let (sink_sync, sink_status, sink_result) = (sync.clone(), status.clone(), result.clone());
        let (observed_w, admitted_w) = (observed_failure.clone(), admitted.clone());
        let sink = std::thread::spawn(move || {
            sink_sync.register_settled();
            // First latch phase: registration observed (see the parked spin
            // below for the second phase).
            registered_tx.send(()).unwrap();
            sink_sync.wait_peers_settled();
            admitted_w.store(true, Ordering::Release);
            let recorded = sink_result
                .lock()
                .unwrap_or_else(std::sync::PoisonError::into_inner);
            let failure_visible = matches!(
                &*recorded,
                Some(Err(crate::error::Error::StartFailed))
            ) && is_stopping(sink_status.load(Ordering::Acquire));
            drop(recorded);
            observed_w.store(failure_visible, Ordering::Release);
            sink_sync.thread_done_with_settled(true, || {});
        });

        // Wait for the registration itself, not wall-clock time. Once it is
        // observed the barrier arithmetic is pinned — live 2 (sentinel +
        // sink) > settled 1 — so no scheduling of the sink thread can pass
        // `wait_peers_settled` while the sentinel slot is live.
        registered_rx
            .recv_timeout(Duration::from_secs(5))
            .expect("the sink must register with the settlement barrier");
        // Second latch phase: the sink is PARKED in the settlement wait, so
        // the admission below can only come from the guard's release edge —
        // a sink that merely arrived late could never distinguish
        // record-then-release from release-then-record.
        let parked_deadline = std::time::Instant::now() + Duration::from_secs(5);
        while sync.parked_settlement_waiters() == 0 {
            assert!(
                std::time::Instant::now() < parked_deadline,
                "the sink never parked in the settlement wait"
            );
            std::thread::sleep(Duration::from_millis(1));
        }
        assert!(
            !admitted.load(Ordering::Acquire),
            "the settlement barrier must hold while the sentinel slot is live"
        );

        // A later init fails: the armed guard drops. Its Drop records the
        // error, publishes the terminal state, releases the sentinel and
        // joins the sink worker.
        drop(StartFailGuard {
            armed: true,
            status: status.clone(),
            result: result.clone(),
            thread_sync: sync.clone(),
            demux_waiters: Vec::new(),
            mux_handed: Vec::new(),
            sentinel_held: true,
        });

        sink.join().unwrap();
        assert!(admitted.load(Ordering::Acquire));
        assert!(
            observed_failure.load(Ordering::Acquire),
            "an admitted settlement waiter must observe the recorded start failure"
        );
        // The release-moment snapshot pins the order on the releasing thread
        // itself: at the instant the sentinel release admitted the parked
        // sink, the failure was already recorded and the terminal state
        // already published — not caught up with later.
        let snapshot = start_tail_probe::take(Arc::as_ptr(&status) as usize)
            .expect("the failing guard's drop must release the sentinel");
        assert!(
            snapshot.result_recorded,
            "the failure must be recorded before the sentinel release admits a waiter"
        );
        assert!(
            snapshot.status_stopping,
            "the terminal status must be published before the sentinel release admits a waiter"
        );
        // First-error-wins left the start failure as the recorded result.
        assert!(matches!(
            &*result.lock().unwrap(),
            Some(Err(crate::error::Error::StartFailed))
        ));
    }

    /// start()'s success tail, driven through the REAL start() of a minimal
    /// copy job (not a synthetic guard, which would stay green however the
    /// production tail were reordered). A drained-sink-shaped waiter parks in
    /// the settlement barrier and the input's reads are gated so the job
    /// cannot finish while the tail is inspected. Pinned:
    ///   1. start() releases the sentinel through `release_sentinel` with the
    ///      RunningGuard ALREADY installed — asserted on the release-moment
    ///      snapshot the releasing thread records, so the release provably
    ///      sits after the installation (and after everything before it:
    ///      the balancing update and the Running-state construction);
    ///   2. at that instant nothing was recorded and the status was not
    ///      terminal — the tail's admission edge shows waiters a clean job;
    ///   3. the release admits no waiter while workers hold slots — after
    ///      start() returns the gated worker still pins the barrier shut;
    ///   4. end-to-end, the waiter is admitted only at full settlement and
    ///      samples the clean terminal (its on_end), which nothing repaints.
    /// NOT pinned: that the remainder between the release and the disarm is
    /// empty of fallible code — an empty window has no runtime observable.
    /// Any code moved between the installation and the release trips
    /// assertion 1; code inserted after the release is out of this test's
    /// reach and must keep the disarm adjacent.
    #[test]
    fn start_tail_releases_the_sentinel_only_after_the_running_guard_is_installed() {
        use super::{is_stopping, start_tail_probe, STATUS_ABORT, STATUS_END};
        use std::fs::File;
        use std::io::{Read, Seek, SeekFrom};
        use std::sync::atomic::{AtomicBool, AtomicUsize};

        // test.mp4 through read/seek callbacks whose post-build reads are
        // held by a gate: while the gate is closed the demux worker cannot
        // reach EOF, so the job provably cannot settle and admit the parked
        // waiter through worker-slot releases. Shrinking the io buffer to
        // 4 KiB and capping probesize at 4 KiB starves build-time probing,
        // but no byte budget can PROVE a post-gate read happens (probing
        // reads and demuxer seeks are unbounded in count) — so the callback
        // counts reads entering and leaving the closed gate, and the test
        // refuses to proceed until one is provably parked there and then.
        // Each wait is bounded so a failing run cannot wedge the worker
        // forever, but a fallback that fires with the gate still closed is
        // recorded and fails the test at the end: every inspection below
        // assumes the gate held its worker throughout.
        let file = Arc::new(Mutex::new(
            File::open("test.mp4").expect("test input must exist"),
        ));
        let gate = Arc::new(AtomicBool::new(false));
        let gate_wait_enters = Arc::new(AtomicUsize::new(0));
        let gate_wait_exits = Arc::new(AtomicUsize::new(0));
        let gate_wait_timed_out = Arc::new(AtomicBool::new(false));
        let (gate_r, file_r) = (gate.clone(), file.clone());
        let (enters_r, exits_r, timed_out_r) = (
            gate_wait_enters.clone(),
            gate_wait_exits.clone(),
            gate_wait_timed_out.clone(),
        );
        let read_callback: Box<dyn FnMut(&mut [u8]) -> i32 + Send> = Box::new(move |buf| {
            if gate_r.load(Ordering::Acquire) {
                enters_r.fetch_add(1, Ordering::AcqRel);
                let deadline = std::time::Instant::now() + Duration::from_secs(10);
                while gate_r.load(Ordering::Acquire) && std::time::Instant::now() < deadline {
                    std::thread::sleep(Duration::from_millis(1));
                }
                if gate_r.load(Ordering::Acquire) {
                    timed_out_r.store(true, Ordering::Release);
                }
                exits_r.fetch_add(1, Ordering::AcqRel);
            }
            let mut file = file_r.lock().unwrap();
            match file.read(buf) {
                Ok(0) => ffmpeg_sys_next::AVERROR_EOF,
                Ok(n) => n as i32,
                Err(_) => ffmpeg_sys_next::AVERROR(ffmpeg_sys_next::EIO),
            }
        });
        let seek_file = file.clone();
        let seek_callback: Box<dyn FnMut(i64, i32) -> i64 + Send> =
            Box::new(move |offset, whence| {
                let mut file = seek_file.lock().unwrap();
                if whence == ffmpeg_sys_next::AVSEEK_SIZE {
                    return match file.metadata() {
                        Ok(meta) => meta.len() as i64,
                        Err(_) => ffmpeg_sys_next::AVERROR(ffmpeg_sys_next::EIO) as i64,
                    };
                }
                let seek = match whence {
                    ffmpeg_sys_next::SEEK_SET => file.seek(SeekFrom::Start(offset as u64)),
                    ffmpeg_sys_next::SEEK_CUR => file.seek(SeekFrom::Current(offset)),
                    ffmpeg_sys_next::SEEK_END => file.seek(SeekFrom::End(offset)),
                    _ => return ffmpeg_sys_next::AVERROR(ffmpeg_sys_next::ESPIPE) as i64,
                };
                match seek {
                    Ok(pos) => pos as i64,
                    Err(_) => ffmpeg_sys_next::AVERROR(ffmpeg_sys_next::EIO) as i64,
                }
            });
        let mut input: Input = read_callback.into();
        input.seek_callback = Some(seek_callback);
        input.io_buffer_size = 4096;
        let input = input.set_format_opt("probesize", "4096");

        // Build probes the input, so the gate closes only afterwards.
        let context = FfmpegContext::builder()
            .input(input)
            .output(
                Output::from("-")
                    .set_format("null")
                    .add_stream_map_with_copy("0:v"),
            )
            .build()
            .expect("a copy job to the null muxer must build");
        gate.store(true, Ordering::Release);

        let scheduler = FfmpegScheduler::new(context);
        let sync = scheduler.thread_sync.clone();
        let status = scheduler.status.clone();
        let result = scheduler.result.clone();
        let status_key = Arc::as_ptr(&status) as usize;
        start_tail_probe::arm(status_key);

        // The drained-sink-shaped waiter: slot claimed BEFORE start() (as a
        // packet-sink worker's slot is claimed before its thread spawns),
        // registered settled, parked in the settlement barrier. On admission
        // it decides its terminal the way the linearization point does — a
        // clean sample (no recorded error, no abort) dispatches on_end.
        //
        // The extra holder slot stands in for the settlement sentinel, which
        // start() has not claimed yet: without it the waiter would be the
        // only live slot (live 1 <= settled 1) and pass the barrier before
        // start() even ran. In production a sink can never observe that
        // state — the sentinel slot is claimed before any registration. The
        // holder is released once start() has returned, when the gated
        // workers hold the barrier instead.
        sync.thread_start();
        sync.thread_start();
        let dispatched_on_end = Arc::new(AtomicBool::new(false));
        let admitted = Arc::new(AtomicBool::new(false));
        let (sink_sync, sink_status, sink_result) = (sync.clone(), status.clone(), result.clone());
        let (dispatched_w, admitted_w) = (dispatched_on_end.clone(), admitted.clone());
        let sink = std::thread::spawn(move || {
            sink_sync.register_settled();
            sink_sync.wait_peers_settled();
            admitted_w.store(true, Ordering::Release);
            let aborted = sink_status.load(Ordering::Acquire) == STATUS_ABORT;
            let clean = sink_result
                .lock()
                .unwrap_or_else(std::sync::PoisonError::into_inner)
                .is_none();
            dispatched_w.store(clean && !aborted, Ordering::Release);
            // The waiter can be the job's last released slot, so its release
            // publishes the terminal state exactly as a worker's would.
            let end_status = sink_status.clone();
            sink_sync.thread_done_with_settled(true, move || {
                end_status.store(STATUS_END, Ordering::Release);
            });
        });
        let parked_deadline = std::time::Instant::now() + Duration::from_secs(5);
        while sync.parked_settlement_waiters() == 0 {
            assert!(
                std::time::Instant::now() < parked_deadline,
                "the settlement waiter never parked"
            );
            std::thread::sleep(Duration::from_millis(1));
        }

        // The holder slot exists only in this test, so a failing start()
        // would deadlock on it: StartFailGuard::drop waits for every slot,
        // and the test thread that would release the holder is inside
        // start(). Two rescuers cover that, each opening the gate BEFORE
        // returning the holder so the failure's own joins drain instead of
        // stalling every gated read to its fallback: a status watcher that
        // reacts the moment the failure guard publishes the terminal status
        // (which it does before its joins), and a 30 s failsafe for a
        // start() that hangs without ever publishing one. The swap makes
        // the release exactly-once whichever side gets there.
        let holder_released = Arc::new(AtomicBool::new(false));
        let release_holder = {
            let (sync, flag) = (sync.clone(), holder_released.clone());
            move || {
                if !flag.swap(true, Ordering::AcqRel) {
                    sync.thread_done_with(|| {});
                }
            }
        };
        let watcher_done = Arc::new(AtomicBool::new(false));
        let start_fail_watcher = {
            let (status, gate, release_holder, done) = (
                status.clone(),
                gate.clone(),
                release_holder.clone(),
                watcher_done.clone(),
            );
            std::thread::spawn(move || {
                while !done.load(Ordering::Acquire) {
                    if is_stopping(status.load(Ordering::Acquire)) {
                        gate.store(false, Ordering::Release);
                        release_holder();
                        return;
                    }
                    std::thread::sleep(Duration::from_millis(2));
                }
            })
        };
        let failsafe_fired = Arc::new(AtomicBool::new(false));
        let (started_tx, started_rx) = std::sync::mpsc::channel::<()>();
        let holder_failsafe = {
            let release_holder = release_holder.clone();
            let gate = gate.clone();
            let fired = failsafe_fired.clone();
            std::thread::spawn(move || {
                if started_rx.recv_timeout(Duration::from_secs(30)).is_err() {
                    fired.store(true, Ordering::Release);
                    gate.store(false, Ordering::Release);
                    release_holder();
                }
            })
        };

        // Reopen the gate on EVERY exit path, in two phases around the
        // fallible start(): the pre-start guard covers a start() that
        // returns an error — the failed expect's unwind opens the gate at
        // once (the joins INSIDE a failing start() are unwedged by the
        // status watcher above, with the read fallback and the failsafe as
        // last resorts) — and is disarmed when the post-start guards take
        // over in the drop order that matters there.
        struct OpenOnExit(Option<Arc<AtomicBool>>);
        impl OpenOnExit {
            fn disarm(&mut self) {
                self.0 = None;
            }
        }
        impl Drop for OpenOnExit {
            fn drop(&mut self) {
                if let Some(gate) = self.0.take() {
                    gate.store(false, Ordering::Release);
                }
            }
        }
        let mut pre_start_gate = OpenOnExit(Some(gate.clone()));
        let running = scheduler.start().expect("the minimal copy job must start");
        watcher_done.store(true, Ordering::Release);
        start_fail_watcher.join().unwrap();
        let _ = started_tx.send(());
        holder_failsafe.join().unwrap();
        // Post-start unwind, in reverse declaration order: the gate opens
        // FIRST (declared last), then the holder goes back, and only then
        // does `running`'s guard join — its workers drain through an open
        // gate with no artificial slot left to wait on.
        struct ReleaseOnExit<F: FnMut()>(F);
        impl<F: FnMut()> Drop for ReleaseOnExit<F> {
            fn drop(&mut self) {
                (self.0)();
            }
        }
        let _holder_guard = ReleaseOnExit(release_holder.clone());
        let _open_gate = OpenOnExit(Some(gate.clone()));
        pre_start_gate.disarm();

        // The handshake the gate's premise rests on: a demux read is parked
        // in the closed gate RIGHT NOW (enters > exits), so EOF was not
        // reachable from build-time buffering alone and the barrier is held
        // by a genuinely gated worker — no byte-budget arithmetic can show
        // that, and a set-once "entered" flag could not either: it stays
        // true after its read burns the fallback and finishes the file.
        // The two loads CAN tear — a fallback exit between them shows
        // fresh enters against stale exits — but that exit records
        // `gate_wait_timed_out` before bumping exits, so the loop fails
        // fast on the flag with the specific diagnosis, and a tear that
        // slips past it is caught by the same flag right below before any
        // inspection runs. Only with a read provably parked has the
        // pre-start holder served its purpose and goes back.
        let handshake_deadline = std::time::Instant::now() + Duration::from_secs(10);
        while gate_wait_enters.load(Ordering::Acquire) <= gate_wait_exits.load(Ordering::Acquire)
        {
            assert!(
                !gate_wait_timed_out.load(Ordering::Acquire),
                "a gated read burned its 10 s fallback while the handshake \
                 was still waiting: the gate is not holding the worker"
            );
            assert!(
                std::time::Instant::now() < handshake_deadline,
                "no read is parked in the closed gate: build-time probing \
                 consumed the input and the gate holds nothing"
            );
            std::thread::sleep(Duration::from_millis(2));
        }
        assert!(
            !gate_wait_timed_out.load(Ordering::Acquire),
            "a gated read burned its 10 s fallback with the gate still closed"
        );
        release_holder();

        // (3) The gated demux worker still holds its slot, so the sentinel
        // release cannot have admitted the waiter.
        assert!(
            !admitted.load(Ordering::Acquire),
            "no settlement waiter may be admitted while workers hold slots"
        );
        // (1) + (2) The release-moment snapshot from the releasing thread.
        let snapshot = start_tail_probe::take(status_key)
            .expect("start()'s success tail must release the sentinel via release_sentinel");
        assert!(
            snapshot.running_guard_installed,
            "the sentinel release must sit after the RunningGuard installation"
        );
        assert!(
            !snapshot.result_recorded && !snapshot.status_stopping,
            "the success tail must release the sentinel against a clean job"
        );
        assert!(
            running._guard.is_some(),
            "start() must return with the RunningGuard installed"
        );

        // (4) Open the gate: the job drains and settles; only then is the
        // waiter admitted, and it samples the clean terminal.
        gate.store(false, Ordering::Release);
        running.wait().expect("the copy job must complete cleanly");
        sink.join().unwrap();
        assert!(admitted.load(Ordering::Acquire));
        assert!(
            dispatched_on_end.load(Ordering::Acquire),
            "the waiter admitted at settlement must observe a clean job"
        );
        assert!(
            result.lock().unwrap().is_none(),
            "no failure may be recorded after the success tail admitted the waiter"
        );
        // Gate health, checked LAST: if any gated read burned its fallback
        // or the failsafe fired, some window above ran against a job the
        // gate was no longer holding and every conclusion is vacuous.
        assert!(
            !gate_wait_timed_out.load(Ordering::Acquire),
            "a gated read burned its 10 s fallback with the gate still closed"
        );
        assert!(
            !failsafe_fired.load(Ordering::Acquire),
            "the 30 s failsafe fired: start() never handed control back \
             within the inspection budget"
        );
    }

    /// Success path: `release_sentinel` releases exactly once (idempotent),
    /// publishes STATUS_END when the sentinel is the last live slot (a job
    /// whose muxers all finished inside `mux_init`), and a disarmed guard
    /// records nothing on drop.
    #[test]
    fn start_sentinel_success_release_publishes_end_and_records_no_error() {
        use super::StartFailGuard;
        use crate::util::thread_synchronizer::ThreadSynchronizer;
        use std::sync::atomic::AtomicUsize;

        let sync = ThreadSynchronizer::new();
        let status = Arc::new(AtomicUsize::new(STATUS_RUN));
        let result: Arc<Mutex<Option<crate::error::Result<()>>>> = Arc::new(Mutex::new(None));

        sync.thread_start();
        let mut guard = StartFailGuard {
            armed: true,
            status: status.clone(),
            result: result.clone(),
            thread_sync: sync.clone(),
            demux_waiters: Vec::new(),
            mux_handed: Vec::new(),
            sentinel_held: true,
        };

        guard.release_sentinel();
        // The sentinel was the last live slot: the release must publish the
        // terminal state before any waiter wakes.
        sync.wait_for_all_threads();
        assert_eq!(status.load(Ordering::Acquire), super::STATUS_END);
        // A second release must not touch the (now zero) counter.
        guard.release_sentinel();
        sync.wait_for_all_threads();

        // The success path disarms; the drop must record nothing.
        guard.armed = false;
        drop(guard);
        assert!(result.lock().unwrap().is_none());
    }

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
                    .set_format_opt("loop", "1")
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
        // Registers/resolves REAL hw devices in the process-global registry:
        // serialize with the hwaccel snapshot tests, which replace that
        // table with sentinel entries for their whole body.
        let _registry = crate::hwaccel::HW_REGISTRY_TEST_LOCK
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
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
        // Registers/resolves REAL hw devices in the process-global registry:
        // serialize with the hwaccel snapshot tests, which replace that
        // table with sentinel entries for their whole body.
        let _registry = crate::hwaccel::HW_REGISTRY_TEST_LOCK
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
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
        // Registers/resolves REAL hw devices in the process-global registry:
        // serialize with the hwaccel snapshot tests, which replace that
        // table with sentinel entries for their whole body.
        let _registry = crate::hwaccel::HW_REGISTRY_TEST_LOCK
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
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
        // Exercise the real pause()/resume() protocol and pin BOTH the status
        // transition and the seqlock pause parity: a running scheduler is even and
        // STATUS_RUN; pause() publishes STATUS_PAUSE and bumps the epoch to the next
        // (odd) value; resume() bumps to the next (even) value.
        let e_run = scheduler.pause_epoch.load(Ordering::Acquire);
        assert_eq!(
            e_run & 1,
            0,
            "a running scheduler's pause epoch must be even"
        );
        let scheduler = scheduler.pause();
        assert_eq!(
            scheduler.status.load(Ordering::Acquire),
            STATUS_PAUSE,
            "pause() must publish STATUS_PAUSE, not merely bump the epoch"
        );
        let e_paused = scheduler.pause_epoch.load(Ordering::Acquire);
        assert_eq!(e_paused, e_run + 1, "pause() must bump the epoch even->odd");
        assert!(scheduler.is_state::<Paused>());
        sleep(Duration::from_secs(1));
        assert_eq!(
            scheduler.status.load(Ordering::Acquire),
            STATUS_PAUSE,
            "the job must stay paused (STATUS_PAUSE) until resume()"
        );
        let scheduler = scheduler.resume();
        let e_resumed = scheduler.pause_epoch.load(Ordering::Acquire);
        assert_eq!(
            e_resumed,
            e_paused + 1,
            "resume() must bump the epoch odd->even"
        );
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

        // stop() should block until all tracked workers release their slots
        scheduler.stop().expect("graceful stop must succeed");

        // Verify output file exists and has content
        let metadata = std::fs::metadata("output_stop.mp4").unwrap();
        assert!(
            metadata.len() > 0,
            "Output file should have content after stop()"
        );
    }
}
