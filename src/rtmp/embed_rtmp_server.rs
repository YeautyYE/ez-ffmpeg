use crate::core::context::output::Output;
use crate::error::Error::{
    RtmpCreateStream, RtmpRegistrationQueueFull, RtmpServerAlreadyStarted, RtmpStreamAlreadyExists,
};
use crate::flv::flv_buffer::FlvBuffer;
use crate::flv::flv_tag::FlvTag;
use crate::rtmp::poller::{waker_pair, WakeHandle, Waker};
use crate::rtmp::reactor::{
    effective_max_connections, EnqueueRefused, IngressBudget, IngressBudgetGuard, PublisherFeed,
    PublisherRegistration, PublisherSource, Reactor, RegistrationHandoff, RegistrationKillSwitch,
    StreamKeyClaim, CHANNEL_HEADROOM, PUBLISHER_CHANNEL_CAPACITY,
    PUBLISHER_INGRESS_HIGH_WATER_BYTES,
};
use bytes::{BufMut, Bytes};
use log::{debug, error, info, warn};
use rml_rtmp::chunk_io::ChunkSerializer;
use rml_rtmp::messages::{MessagePayload, RtmpMessage};
use rml_rtmp::rml_amf0::Amf0Value;
use rml_rtmp::time::RtmpTimestamp;
use std::collections::HashMap;
use std::marker::PhantomData;
use std::net::{Shutdown, TcpListener, TcpStream};
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::{Arc, Mutex, PoisonError};
use std::thread::JoinHandle;

#[derive(Clone)]
pub struct Initialization;
#[derive(Clone)]
pub struct Running;
#[derive(Clone)]
pub struct Ended;

#[derive(Clone)]
pub struct EmbedRtmpServer<S> {
    address: String,
    bound_addr: Option<std::net::SocketAddr>,
    status: Arc<AtomicUsize>,
    // Arc-shared with the reactor. create_* claims a key by wrapping it in a
    // StreamKeyClaim (insert-or-fail, so concurrent creates cannot both win);
    // the claim rides inside the queued registration and releases itself on
    // drop unless the reactor accepts the publisher — from then on removal
    // of the publisher releases the key. DashSet's Clone is a deep copy, so
    // a non-Arc field cloned into the worker thread would split server and
    // reactor onto two disjoint sets and disable the duplicate-key check
    // entirely.
    stream_keys: Arc<dashmap::DashSet<String>>,
    // Registrations for the reactor: key claim + publisher source (raw byte
    // path or media-bypass feed). Lock-shared with the worker rather than a
    // channel, so the create paths' worker-liveness check and their enqueue
    // form one critical section (see RegistrationHandoff).
    registrations: Option<Arc<RegistrationHandoff>>,
    /// Producer-side wakeup for the reactor (PERF-3), set in `start()`.
    wake_handle: Option<WakeHandle>,
    /// Join handles of the two server threads (worker, accept), pushed by
    /// `start()` and shared by every clone of the family. `stop()` drains
    /// this registry and joins them — its settlement barrier — holding the
    /// lock across the joins so a racing `stop()` on a clone blocks on the
    /// lock and also returns only after teardown finished. The server
    /// threads themselves never reach this lock: the only way user code
    /// (which could hold a clone and call `stop()`) runs on them is through
    /// a user-installed global logger, and `settle_server_threads` bails out
    /// on a server thread BEFORE touching the registry (see its reentrancy
    /// notes) — so holding the lock across the joins cannot deadlock.
    threads: Arc<Mutex<Vec<JoinHandle<()>>>>,
    /// The `ThreadId`s of the threads whose handles live in `threads`,
    /// recorded at spawn. `settle_server_threads` probes this list — briefly,
    /// never across a join — to detect the reentrant case above without
    /// going anywhere near the registry lock.
    server_thread_ids: Arc<Mutex<Vec<std::thread::ThreadId>>>,
    /// WHY the family reached its terminal state — `CAUSE_DELIBERATE` for
    /// the user-initiated paths (`stop()`, a handle/guard drop), or
    /// `CAUSE_FATAL` for the failure paths (a contained reactor panic, the
    /// worker dying). First writer wins: the cause is claimed with a
    /// compare-exchange from `CAUSE_NONE` by whichever terminal transition
    /// lands first, so a `stop()` arriving after a crash cannot relabel the
    /// crash as deliberate. In-process publisher write callbacks read it
    /// when their feed send fails, to report a deliberate server stop
    /// calmly instead of as an opaque send error.
    terminal_cause: Arc<AtomicUsize>,
    gop_limit: usize,
    max_connections: Option<usize>,
    state: PhantomData<S>,
}

const STATUS_INIT: usize = 0;
const STATUS_RUN: usize = 1;
const STATUS_END: usize = 2;

/// `terminal_cause` values. Kept separate from the status flag: the status
/// answers "is the family stopped" (three states, compared all over the
/// reactor and accept loops), the cause answers "who stopped it" (write-once,
/// read only by failure classification). Folding the cause into the status
/// would turn every `== STATUS_END` comparison into a range check.
const CAUSE_NONE: usize = 0;
const CAUSE_DELIBERATE: usize = 1;
const CAUSE_FATAL: usize = 2;

/// The one funnel for the server's terminal transition: close the
/// registration intake FIRST, then release-store `STATUS_END`.
///
/// The release-store orders the close ahead of any observer's acquire-load
/// of the status, so a caller that saw `is_stopped() == true` can no longer
/// enqueue a registration and be told `Ok` when no reactor round will
/// consume it (the reactor checks the stop flag before draining the queue).
/// Every path that publishes `STATUS_END` must run through here — a store
/// over a still-open intake reintroduces exactly that lie. Both steps are
/// idempotent, so racing terminal paths (a stop signal against a worker
/// panic, or the accept loop noticing the worker died) may each run this
/// with no ordering between them.
/// Every direct caller of this funnel is a FAILURE path (worker death,
/// reactor panic, start() unwind), so the funnel claims `CAUSE_FATAL` for
/// them; the deliberate paths all run through `signal_stop`, which claims
/// `CAUSE_DELIBERATE` BEFORE delegating here, making this claim a no-op.
/// The compare-exchange makes the cause first-writer-wins under races
/// between a crash and a late `stop()`.
fn close_intake_and_publish_end(
    registrations: &RegistrationHandoff,
    status: &AtomicUsize,
    terminal_cause: &AtomicUsize,
) {
    let _ = terminal_cause.compare_exchange(
        CAUSE_NONE,
        CAUSE_FATAL,
        Ordering::AcqRel,
        Ordering::Acquire,
    );
    registrations.close();
    status.store(STATUS_END, Ordering::Release);
}

/// `stop()`'s settlement barrier: join every server thread in the family's
/// registry, holding the registry lock across the joins so racing `stop()`
/// calls on clones serialize behind it and each returns only once teardown
/// finished. The caller must have signaled the stop first, or the joins
/// would wait on threads with no reason to exit.
///
/// Reentrancy guard — FIRST, before any lock. The server threads do run
/// user code: every `log` macro they execute dispatches to a user-installed
/// global logger, which may hold a server clone and call `stop()`. Such a
/// call must neither block on the registry lock (an outer settlement holds
/// it while joining the very thread the logger runs on — deadlock) nor
/// consume handles (its own would be dropped, detaching a thread a later
/// user-thread `stop()` could still settle). A server thread therefore gets
/// signal-only semantics: return SILENTLY with the registry untouched —
/// silently, because this thread may be inside the logger's own log()
/// frame, above a non-reentrant lock the logger holds, and any log macro
/// here would re-enter the logger and deadlock on it: the exact failure
/// this branch exists to prevent. The id list's lock is held only for this
/// membership probe — never across a join — so the probe itself cannot
/// deadlock.
///
/// The join loop performs no logging and nothing else that can unwind: an
/// unwind mid-drain would detach the remaining handles and poison the
/// registry into a state indistinguishable from settled (empty), turning a
/// later `stop()` into a false barrier. That discipline includes the panic
/// PAYLOADS of joined threads: a payload is an arbitrary `Box<dyn Any>`,
/// and settlement never runs its drop glue at all — payloads are moved
/// aside untouched during the drain and deliberately LEAKED
/// (`mem::forget`) once the lock is released. Running their destructors
/// here, however contained, is unwinnable in principle: a payload whose
/// two field destructors both panic aborts the process inside its own
/// drop glue before any catch regains control, and a destructor is
/// equally free to BLOCK forever on a lock the `stop()` caller holds —
/// either outcome inside a blocking public API is strictly worse than
/// leaking a few bytes per crashed thread, an already-broken, bounded
/// path. (Contrast `dispose_panic_payload` in packet_sink, which runs
/// destructors bounded-chain-contained on paths where a caller is not
/// blocked behind them.) The report afterwards runs under containment,
/// and its own panic payload — a logger's — is forgotten the same way, so
/// nothing can unwind a completed settlement out of `stop()`. Riding over
/// poisoning is safe under that discipline: a poisoned registry still
/// holds exactly the handles no settlement consumed, and the drain below
/// joins them as usual.
fn settle_server_threads(
    server_thread_ids: &Mutex<Vec<std::thread::ThreadId>>,
    threads: &Mutex<Vec<JoinHandle<()>>>,
) {
    let current = std::thread::current().id();
    let is_server_thread = server_thread_ids
        .lock()
        .unwrap_or_else(PoisonError::into_inner)
        .contains(&current);
    if is_server_thread {
        // No logging here — not even a warn. See the reentrancy note above:
        // this frame may sit inside the user logger itself, and dispatching
        // to it again would deadlock on the logger's own lock.
        return;
    }

    let mut panicked: Vec<String> = Vec::new();
    let mut payloads: Vec<Box<dyn std::any::Any + Send>> = Vec::new();
    {
        let mut registry = threads.lock().unwrap_or_else(PoisonError::into_inner);
        for handle in registry.drain(..) {
            // Defensive second line for a handle the id list missed (only
            // reachable from hand-assembled registries in tests): detaching
            // beats self-joining forever. No logging in this loop — see the
            // unwind-safety note above.
            if handle.thread().id() == current {
                continue;
            }
            let name = handle.thread().name().unwrap_or("rtmp-server").to_string();
            if let Err(payload) = handle.join() {
                // Move the payload aside WITHOUT dropping it: its
                // destructor is arbitrary user code that may panic, and no
                // unwind is allowed while the drain and the registry guard
                // are live (see the unwind-safety note above).
                payloads.push(payload);
                panicked.push(name);
            }
        }
    }
    // Leak the retained payloads BEFORE any reporting: no drop glue of an
    // arbitrary payload may run inside a blocking stop() — a two-bomb
    // payload aborts inside its own drop glue before any catch regains
    // control, and a destructor can just as well block on a lock the
    // stop() caller holds. See the unwind-safety note above.
    for payload in payloads {
        std::mem::forget(payload);
    }
    if !panicked.is_empty() {
        // The threads died to uncontained panics (e.g. a user-installed
        // logger panicking outside the reactor's unwind boundary). They are
        // just as terminated — the barrier holds — so report rather than
        // rethrow. The report itself runs contained: settlement is already
        // complete and this is purely informational, so a logger panicking
        // HERE must not unwind a finished settlement out of stop() — and
        // whatever payload that panic carries is forgotten like the join
        // payloads above.
        let report = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
            for name in &panicked {
                error!("Thread[{name}] terminated by an uncontained panic");
            }
        }));
        if let Err(payload) = report {
            std::mem::forget(payload);
        }
    }
}

/// Unwind backstop for `start()`'s window between the lifecycle claim (the
/// `compare_exchange` of `STATUS_INIT` to `STATUS_RUN`) and the successful
/// handoff to the accept thread. Code in that window still logs, and
/// `error!`/`info!` dispatch to a user-installed logger that can itself
/// panic; an unwind escaping there would leave the family stuck at
/// `STATUS_RUN` — every clone reporting a started server that no one can
/// ever stop, and a worker thread that already spawned polling that status
/// forever. Dropped while armed, the guard runs the same terminal sequence
/// as `signal_stop`: the funnel (intake closed first, then `STATUS_END`),
/// then the reactor wake. All steps are idempotent, so overlapping the
/// explicit `signal_stop` on `start()`'s error returns is harmless. The
/// success path disarms the guard once the accept thread owns the listener —
/// from then on terminal transitions belong to `stop()`, the RAII guards
/// and the worker's own fatal paths.
struct StartFailGuard {
    armed: bool,
    registrations: Arc<RegistrationHandoff>,
    status: Arc<AtomicUsize>,
    wake_handle: Option<WakeHandle>,
    threads: Arc<Mutex<Vec<JoinHandle<()>>>>,
    server_thread_ids: Arc<Mutex<Vec<std::thread::ThreadId>>>,
    terminal_cause: Arc<AtomicUsize>,
}

impl Drop for StartFailGuard {
    fn drop(&mut self) {
        if !self.armed {
            return;
        }
        close_intake_and_publish_end(&self.registrations, &self.status, &self.terminal_cause);
        if let Some(wake_handle) = &self.wake_handle {
            wake_handle.wake();
        }
        // Settle whatever did spawn before the unwind: the signal above
        // makes those threads exit promptly, and joining them here keeps
        // the failed start from leaving live threads (and their registry
        // handles) behind for a family no `stop()` can ever reach —
        // `start()` consumed the one lifecycle claim, so no `Running`
        // value will exist to settle them later. Runs on `start()`'s
        // caller thread, never a server thread, so the reentrancy bail-out
        // inside cannot fire.
        settle_server_threads(&self.server_thread_ids, &self.threads);
    }
}

impl<S: 'static> EmbedRtmpServer<S> {
    fn into_state<T>(self) -> EmbedRtmpServer<T> {
        EmbedRtmpServer {
            address: self.address,
            bound_addr: self.bound_addr,
            status: self.status,
            stream_keys: self.stream_keys,
            registrations: self.registrations,
            wake_handle: self.wake_handle,
            threads: self.threads,
            server_thread_ids: self.server_thread_ids,
            terminal_cause: self.terminal_cause,
            gop_limit: self.gop_limit,
            max_connections: self.max_connections,
            state: Default::default(),
        }
    }

    /// Checks whether the RTMP server has been signaled to stop. This returns
    /// `true` once [`stop`](EmbedRtmpServer<Running>::stop) has been called
    /// (or a fatal internal error stopped the server), otherwise `false`.
    ///
    /// Note this reports the *signal*, not thread teardown: the worker threads
    /// observe the flag and exit shortly after (the reactor on its next
    /// wakeup, the accept thread within its ~100ms accept cycle). The one
    /// place both coincide is [`stop`](EmbedRtmpServer<Running>::stop), which
    /// joins the server threads before returning — after a `stop()` call has
    /// returned, teardown is complete as well (except for `stop()`'s
    /// reentrant logger edge, documented there, which is signal-only).
    ///
    /// # Returns
    ///
    /// * `true` if the server has been signaled to stop (and will no longer accept connections).
    /// * `false` if the server is still running.
    pub fn is_stopped(&self) -> bool {
        self.status.load(Ordering::Acquire) == STATUS_END
    }

    /// Signal the server threads to stop without consuming the server.
    ///
    /// Idempotent: re-closing a closed intake, storing `STATUS_END` again and
    /// re-waking an already-woken reactor are all no-ops. The wake matters —
    /// without it the reactor notices the flag only on its next poll timeout,
    /// and a reactor parked in `poll()` would otherwise hold the shutdown for
    /// up to 100ms. When no wake handle exists (waker_pair creation failed at
    /// start), that 100ms poll fallback is exactly the degraded path the
    /// reactor already runs on.
    ///
    /// This exists because `EmbedRtmpServer` cannot implement `Drop` itself:
    /// `into_state()` moves fields out of `self`, which the compiler forbids
    /// for types with a `Drop` impl (E0509). Shared owners that only hold a
    /// reference (e.g. [`StreamHandle`]) stop the server through this instead.
    fn signal_stop(&self) {
        // Claim the terminal cause BEFORE the funnel, first-writer-wins: if
        // a fatal transition (worker death, contained panic) already claimed
        // `CAUSE_FATAL`, this exchange fails and the crash keeps its loud
        // classification — a late deliberate stop() must not relabel it.
        // Ordering: the funnel's release-store of `STATUS_END` orders this
        // claim ahead of any acquire-load of the status, and the reactor
        // tears publishers down only after observing that status, so the
        // cause is in place before any teardown a callback could witness.
        let _ = self.terminal_cause.compare_exchange(
            CAUSE_NONE,
            CAUSE_DELIBERATE,
            Ordering::AcqRel,
            Ordering::Acquire,
        );
        // Close-then-publish through the shared funnel (see
        // `close_intake_and_publish_end` for the ordering argument).
        // Registrations already queued keep their owners — the reactor's
        // remaining rounds, with the worker's kill-switch drain as the
        // backstop.
        match &self.registrations {
            Some(registrations) => {
                close_intake_and_publish_end(registrations, &self.status, &self.terminal_cause)
            }
            // Defensive arm, unreachable today: every caller reaches this
            // with the handoff installed (`stop()`, the RAII guards and
            // `start()`'s failure paths all run after `start()` stored
            // it). Should a signal path for never-started handles ever
            // appear, it must not use this plain store while a started
            // sibling's intake exists — route it through that intake and
            // the funnel instead.
            None => self.status.store(STATUS_END, Ordering::Release),
        }
        if let Some(wake_handle) = &self.wake_handle {
            wake_handle.wake();
        }
    }
}

impl EmbedRtmpServer<Initialization> {
    /// Creates a new RTMP server instance that will listen on the specified address
    /// when [`start`](EmbedRtmpServer<Initialization>::start) is called.
    ///
    /// # Parameters
    ///
    /// * `address` - A string slice representing the address (host:port) to bind the
    ///   RTMP server socket.
    ///
    /// # Returns
    ///
    /// An [`EmbedRtmpServer`] configured to listen on the given address.
    pub fn new(address: impl Into<String>) -> EmbedRtmpServer<Initialization> {
        Self::new_with_gop_limit(address, 1)
    }

    /// Creates a new RTMP server instance that will listen on the specified address,
    /// with a custom GOP limit.
    ///
    /// This method allows specifying the maximum number of GOPs to be cached.
    /// A GOP (Group of Pictures) represents a sequence of video frames (I, P, B frames)
    /// used for efficient video decoding and random access. The GOP limit defines
    /// how many such groups are stored in the cache.
    ///
    /// # Parameters
    ///
    /// * `address` - A string slice representing the address (host:port) to bind the
    ///   RTMP server socket.
    /// * `gop_limit` - The maximum number of GOPs to cache.
    ///
    /// # Returns
    ///
    /// An [`EmbedRtmpServer`] instance configured to listen on the given address and
    /// using the specified GOP limit.
    pub fn new_with_gop_limit(
        address: impl Into<String>,
        gop_limit: usize,
    ) -> EmbedRtmpServer<Initialization> {
        Self {
            address: address.into(),
            bound_addr: None,
            status: Arc::new(AtomicUsize::new(STATUS_INIT)),
            stream_keys: Default::default(),
            registrations: None,
            wake_handle: None,
            threads: Default::default(),
            server_thread_ids: Default::default(),
            terminal_cause: Default::default(),
            gop_limit,
            max_connections: None,
            state: Default::default(),
        }
    }

    /// Sets the maximum number of concurrent connections allowed.
    ///
    /// If not set, the limit is auto-detected based on system file descriptor limits
    /// (default: 10000, capped at 80% of system FD limit).
    ///
    /// # Parameters
    ///
    /// * `max_connections` - Maximum number of concurrent connections
    ///
    /// # Returns
    ///
    /// Self for method chaining.
    pub fn set_max_connections(mut self, max_connections: usize) -> Self {
        self.max_connections = Some(max_connections);
        self
    }

    /// Starts the RTMP server on the configured address, entering a loop that
    /// accepts incoming client connections. This method spawns background threads
    /// to handle the connections and publish events.
    ///
    /// Clones of one server value share a single lifecycle, and that
    /// lifecycle can be started only once: the first `start()` wins, and
    /// every later attempt — a sibling clone while the server runs, or any
    /// clone after it stopped — fails with
    /// [`RtmpServerAlreadyStarted`](crate::error::Error::RtmpServerAlreadyStarted),
    /// refused before any socket is bound: a second start on a fixed port
    /// reports the lifecycle error, not `AddrInUse` from the winner's port.
    /// A `start()` that fails before it claims the lifecycle (e.g. the
    /// bind) leaves it untouched, so a clone may retry; from the claim on,
    /// a failing `start()` stops the lifecycle for good.
    ///
    /// # Returns
    ///
    /// * `Ok(())` if the server successfully starts listening.
    /// * An error variant if the socket could not be bound, this server
    ///   value's lifecycle was already started once, or other I/O errors
    ///   occur.
    pub fn start(mut self) -> crate::error::Result<EmbedRtmpServer<Running>> {
        // Admission, step one: refuse an already-started (or stopped)
        // family before touching the address. Binding first would report a
        // second start() on a fixed port as the winner's port being in use
        // — Error::IO(AddrInUse) — instead of the typed lifecycle error,
        // and right after stop() the outcome would flip between the two
        // depending on how quickly the old accept thread released the
        // listener. This load is advisory only; the compare_exchange below
        // is the authoritative claim.
        if self.status.load(Ordering::Acquire) != STATUS_INIT {
            return Err(RtmpServerAlreadyStarted);
        }

        // Admission, step two: the bind. Failures from here up to the
        // claim below leave the status at STATUS_INIT, so a failed bind
        // stays retryable through a clone.
        let listener = TcpListener::bind(self.address.clone())
            .map_err(|e| <std::io::Error as Into<crate::error::Error>>::into(e))?;

        // Get actual bound address (important for port 0)
        let actual_addr = listener
            .local_addr()
            .map_err(|e| <std::io::Error as Into<crate::error::Error>>::into(e))?;
        self.bound_addr = Some(actual_addr);

        listener
            .set_nonblocking(true)
            .map_err(|e| <std::io::Error as Into<crate::error::Error>>::into(e))?;

        // Calculate effective max and create bounded channel with headroom
        // This prevents unbounded queue growth when reactor is at capacity
        let effective_max = effective_max_connections(self.max_connections);
        let channel_capacity = effective_max.saturating_add(CHANNEL_HEADROOM);
        let (stream_sender, stream_receiver) = crossbeam_channel::bounded(channel_capacity);
        // Publisher registrations ride a lock-shared queue, not a channel:
        // the worker-liveness check and the enqueue must share one critical
        // section, and the worker's exit drain must be terminal (see
        // RegistrationHandoff). The reactor picks registrations up on its
        // normal loop turns (wake or poll timeout), as it did the channel.
        let registrations = Arc::new(RegistrationHandoff::new());
        self.registrations = Some(registrations.clone());

        // PERF-3: create the reactor wakeup pair. The Waker (read side) is moved
        // into the worker thread and registered with the poller; the WakeHandle
        // (write side) is kept here so create_rtmp_input can signal the reactor
        // the instant media is queued.
        // If the wakeup pair cannot be created (e.g. eventfd/loopback exhaustion),
        // degrade gracefully to the POLL_TIMEOUT_MS fallback rather than failing
        // server startup: the low-latency wakeup is an optimization, not a
        // correctness requirement.
        let (waker, wake_handle) = match waker_pair() {
            Ok((waker, handle)) => (Some(waker), Some(handle)),
            Err(e) => {
                warn!("PERF-3: reactor waker unavailable ({e:?}); falling back to the poll-timeout for in-process media latency");
                (None, None)
            }
        };
        self.wake_handle = wake_handle;

        // Admission, step three — the single-start gate for the whole clone
        // family. The status flag (and the key set) is shared by every
        // clone of this server value, but each start() builds its own
        // registration intake around it — so a second started server would
        // leave the family with TWO intakes and one status: stopping either
        // one closes only its own intake while publishing the shared
        // STATUS_END, and the other's create paths would keep returning Ok
        // on handles that report stopped. The CAS makes that state
        // structurally impossible: a racer that slipped past the advisory
        // load above is refused right here, before any thread spawns, and
        // its freshly bound listener drops with the return, releasing the
        // port. This CAS is also the lifecycle's point of no return —
        // failures above it leave the status at STATUS_INIT (retryable
        // through a clone); failures below it are terminal for the family.
        if self
            .status
            .compare_exchange(STATUS_INIT, STATUS_RUN, Ordering::AcqRel, Ordering::Acquire)
            .is_err()
        {
            return Err(RtmpServerAlreadyStarted);
        }

        // The claim is consumed: from here to the disarm at the bottom,
        // every exit — the explicit error returns as much as an unwind out
        // of a log call whose user-installed logger panics — must publish
        // the terminal state, or the family is stuck reporting a running
        // server that no one can ever stop. The guard covers the unwinds;
        // the error paths below also run the funnel explicitly, first
        // thing, so the terminal state is published before they log.
        let mut start_fail_guard = StartFailGuard {
            armed: true,
            registrations: registrations.clone(),
            status: self.status.clone(),
            wake_handle: self.wake_handle.clone(),
            threads: self.threads.clone(),
            server_thread_ids: self.server_thread_ids.clone(),
            terminal_cause: self.terminal_cause.clone(),
        };

        let status = self.status.clone();
        let max_connections = self.max_connections;
        let worker_registrations = registrations.clone();
        let worker_terminal_cause = self.terminal_cause.clone();
        let result = std::thread::Builder::new()
            .name("rtmp-server-worker".to_string())
            .spawn(move || {
                handle_connections(
                    stream_receiver,
                    worker_registrations,
                    self.gop_limit,
                    max_connections,
                    status,
                    worker_terminal_cause,
                    waker,
                )
            });
        match result {
            // The worker's handle joins the family registry: it is what
            // stop()'s settlement barrier joins. Registered before the
            // accept thread spawns, so no exit below can leave a running
            // thread the registry does not know about. Its ThreadId goes
            // into the reentrancy list first — both writes happen before
            // any Running value (and thus any stop()) can exist, so the
            // two locks never race a settlement.
            Ok(handle) => {
                self.server_thread_ids
                    .lock()
                    .unwrap_or_else(PoisonError::into_inner)
                    .push(handle.thread().id());
                self.threads
                    .lock()
                    .unwrap_or_else(PoisonError::into_inner)
                    .push(handle);
            }
            Err(e) => {
                // Nothing has spawned yet: no worker observes STATUS_RUN, and the
                // listener is still owned here (moved into the io closure only
                // below), so it drops on return and releases the port. The
                // family's one start has been consumed by the gate above,
                // though, so the status must not stay at STATUS_RUN — every
                // clone would report a running server forever. Publish the
                // terminal state BEFORE logging, intake closed first as on
                // every terminal path: `error!` can run a user-installed
                // logger that itself panics, and that unwind must not skip
                // the publication.
                self.signal_stop();
                error!("Thread[rtmp-server-worker] exited with error: {e}");
                return Err(crate::error::Error::RtmpThreadExited);
            }
        }

        info!(
            "Embed rtmp server listening for connections on {} (actual: {}, max_connections: {}).",
            &self.address, actual_addr, effective_max
        );

        let status = self.status.clone();
        // The accept loop owns one terminal transition of its own (the
        // worker's connection channel disconnecting below) and needs the
        // intake to run it through the funnel: it takes the handoff Arc
        // still held from the setup above. That transition is a worker
        // death — a fatal cause, which the funnel claims.
        let terminal_cause = self.terminal_cause.clone();
        let result = std::thread::Builder::new()
            .name("rtmp-server-io".to_string())
            .spawn(move || {
                let outcome = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
                    for stream in listener.incoming() {
                        // Check the stop flag on every iteration, not only when the
                        // listener runs dry: under a steady stream of incoming
                        // connections the WouldBlock branch is never taken and
                        // stop() would otherwise never terminate this thread.
                        if status.load(Ordering::Acquire) == STATUS_END {
                            info!("Embed rtmp server stopped.");
                            break;
                        }
                        match stream {
                            Ok(stream) => {
                                // Use try_send to apply backpressure when channel is full
                                match stream_sender.try_send(stream) {
                                    Ok(_) => {
                                        debug!("New rtmp connection accepted.");
                                    }
                                    Err(crossbeam_channel::TrySendError::Full(s)) => {
                                        // Channel full - server at capacity, reject connection immediately
                                        let _ = s.shutdown(Shutdown::Both);
                                        debug!(
                                            "Connection rejected: server at capacity (channel full)"
                                        );
                                    }
                                    Err(crossbeam_channel::TrySendError::Disconnected(_)) => {
                                        error!("Connection channel disconnected");
                                        // The worker died out from under this
                                        // loop. Its own teardown closes the
                                        // intake too, but this thread cannot
                                        // order itself after that: funnel the
                                        // close ahead of the store here as
                                        // well, so STATUS_END is never
                                        // observable over an open intake no
                                        // matter whose store lands first.
                                        close_intake_and_publish_end(
                                            &registrations,
                                            &status,
                                            &terminal_cause,
                                        );
                                        return;
                                    }
                                }
                            }
                            Err(e) => {
                                if e.kind() == std::io::ErrorKind::WouldBlock {
                                    std::thread::sleep(std::time::Duration::from_millis(100));
                                } else if is_fd_exhaustion(&e) {
                                    // Accepting again immediately would fail the same
                                    // way and spin the CPU; back off and let existing
                                    // connections close first.
                                    warn!("Accept failed, file descriptors exhausted: {e}");
                                    std::thread::sleep(std::time::Duration::from_millis(100));
                                } else {
                                    debug!("Rtmp connection error: {:?}", e);
                                }
                            }
                        }
                    }
                }));
                if let Err(payload) = outcome {
                    close_intake_and_publish_end(&registrations, &status, &terminal_cause);
                    let report = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
                        error!("Rtmp accept loop panicked; the server is stopped.");
                    }));
                    if let Err(report_payload) = report {
                        crate::core::packet_sink::dispose_panic_payload(report_payload);
                    }
                    crate::core::packet_sink::dispose_panic_payload(payload);
                }
            });
        match result {
            Ok(handle) => {
                self.server_thread_ids
                    .lock()
                    .unwrap_or_else(PoisonError::into_inner)
                    .push(handle.thread().id());
                self.threads
                    .lock()
                    .unwrap_or_else(PoisonError::into_inner)
                    .push(handle);
            }
            Err(e) => {
                // The worker thread spawned successfully above and is now polling
                // `status` (still STATUS_RUN); without this it would run forever.
                // Signal STATUS_END (and wake the reactor) so it exits, and do it
                // BEFORE logging: `error!` can run a user-installed logger that
                // itself panics, and that unwind must not strand the already
                // running worker. The listener was moved into the failed io
                // closure and drops with it, releasing the port. Then settle
                // the worker: this failed start() consumed the family's one
                // lifecycle claim, so no Running value will ever exist to
                // join it later — its registry handle would otherwise pin a
                // live thread until the last clone drops. Settling before
                // the log keeps the usual discipline: a panicking logger
                // cannot skip it.
                self.signal_stop();
                settle_server_threads(&self.server_thread_ids, &self.threads);
                error!("Thread[rtmp-server-io] exited with error: {e}");
                return Err(crate::error::Error::RtmpThreadExited);
            }
        }

        // Handoff complete: the accept thread owns the listener, and every
        // terminal transition from here on belongs to stop(), the RAII
        // guards or the worker's own fatal paths.
        start_fail_guard.armed = false;
        Ok(self.into_state())
    }
}

/// Handle for feeding raw, pre-packaged RTMP chunk bytes to a stream published
/// via [`EmbedRtmpServer::<Running>::create_stream_sender`]. Wraps the internal
/// channel so callers do not depend on the channel implementation.
///
/// Cloneable and multi-producer: every clone feeds the same stream, so several
/// producers can push chunks to one stream concurrently.
#[derive(Clone)]
pub struct RtmpStreamSender {
    inner: crossbeam_channel::Sender<Vec<u8>>,
    /// Wakes the reactor after each send so a raw publisher's media is
    /// drained on the next loop turn instead of waiting up to POLL_TIMEOUT_MS
    /// (~100ms). `None` only for the unit-test constructor (no running reactor).
    wake_handle: Option<WakeHandle>,
    /// This publisher's ingress byte account: `send` reserves each chunk's
    /// length before enqueueing it and the reactor returns each round's
    /// drained bytes in one batch, so a producer running ahead of the drain
    /// parks at the high-water mark instead of queueing unbounded bytes.
    /// Clones share the account exactly as they share the channel.
    budget: Arc<IngressBudget>,
}

impl RtmpStreamSender {
    /// Sends one already-RTMP-chunk-packaged byte buffer to the stream.
    ///
    /// The underlying channel is bounded, so this **blocks** while the stream's
    /// queue is full — at its item capacity, or at its byte high-water mark of
    /// undrained chunk bytes — (the server applying backpressure) and returns
    /// once space frees up. Returns
    /// [`Error::RtmpStreamClosed`](crate::error::Error::RtmpStreamClosed) if the
    /// stream has been torn down — its receiver was dropped because the server
    /// stopped or the stream was removed.
    pub fn send(&self, chunk: Vec<u8>) -> crate::error::Result<()> {
        self.send_quiet(chunk)?;
        // Nudge the reactor so process_publishers drains this Raw channel on the
        // next loop turn rather than after the POLL_TIMEOUT_MS fallback. The
        // internal Feed path (create_rtmp_input) wakes the same way per packet.
        if let Some(wake) = &self.wake_handle {
            wake.wake();
        }
        Ok(())
    }

    /// The budgeted channel send [`send`](Self::send) and the priming loop in
    /// `create_stream_sender` share, minus the per-send reactor wake (priming
    /// keeps its single post-batch wake): reserve the chunk's bytes in the
    /// publisher's ingress account — parking while the account is over its
    /// high-water mark — then forward to the channel, rolling the reservation
    /// back if the channel is dead so sibling clones' accounts stay exact.
    /// A closed account and a dead channel are the same stream teardown, so
    /// both failures map to the one existing error identity.
    fn send_quiet(&self, chunk: Vec<u8>) -> crate::error::Result<()> {
        let len = chunk.len();
        self.budget
            .acquire(len)
            .map_err(|_| crate::error::Error::RtmpStreamClosed)?;
        self.inner.send(chunk).map_err(|_| {
            self.budget.release(len);
            crate::error::Error::RtmpStreamClosed
        })
    }
}

/// The feed-path counterpart of [`RtmpStreamSender`]'s budgeted send: pairs
/// the `create_rtmp_input` bypass feed with its publisher's ingress budget so
/// the FFmpeg muxer thread (inside the AVIO write callback) parks at the byte
/// high-water mark exactly like a raw sender, instead of queueing unbounded
/// tag bytes. Internal only — the write callback owns the sole instance.
struct BudgetedFeedSender {
    inner: crossbeam_channel::Sender<PublisherFeed>,
    budget: Arc<IngressBudget>,
}

impl BudgetedFeedSender {
    /// Reserve the item's accounted bytes ([`PublisherFeed::ingress_len`],
    /// the same measure the reactor's drain releases), then forward it,
    /// rolling the reservation back on a dead channel so the account stays
    /// exact. A closed budget is the same reactor-side teardown that
    /// disconnects the channel, so it is reported in the channel's own error
    /// shape and flows through the caller's existing failure classification.
    fn send(&self, feed: PublisherFeed) -> Result<(), crossbeam_channel::SendError<PublisherFeed>> {
        let len = feed.ingress_len();
        if self.budget.acquire(len).is_err() {
            return Err(crossbeam_channel::SendError(feed));
        }
        self.inner.send(feed).inspect_err(|_| {
            self.budget.release(len);
        })
    }
}

impl EmbedRtmpServer<Running> {
    /// Returns the actual bound socket address of the RTMP server.
    ///
    /// This is particularly useful when binding to port 0 (random port allocation),
    /// as it allows you to discover which port the OS assigned.
    ///
    /// # Returns
    ///
    /// * `Option<std::net::SocketAddr>` - The actual bound address, or `None` if not available.
    ///
    /// # Example
    ///
    /// ```rust,ignore
    /// let server = EmbedRtmpServer::new("127.0.0.1:0").start().unwrap();
    /// let actual_port = server.local_addr().unwrap().port();
    /// println!("Server listening on port: {}", actual_port);
    /// ```
    pub fn local_addr(&self) -> Option<std::net::SocketAddr> {
        self.bound_addr
    }

    /// Creates an RTMP "input" endpoint for this server (from the server's perspective),
    /// returning an [`Output`] that can be used by FFmpeg to push media data.
    ///
    /// From the FFmpeg standpoint, the returned [`Output`] is where media content is
    /// sent (i.e., FFmpeg "outputs" to this RTMP server). After obtaining this [`Output`],
    /// you can pass it to your FFmpeg job or scheduler to start streaming data into the server.
    ///
    /// # Parameters
    ///
    /// * `app_name` - The RTMP application name, typically corresponding to the `app` part
    ///   of an RTMP URL (e.g., `rtmp://host:port/app/stream_key`).
    /// * `stream_key` - The stream key (or "stream name"). If a stream with the same key
    ///   already exists, an error will be returned.
    ///
    /// # Returns
    ///
    /// * [`Output`] - An output object preconfigured for streaming to this RTMP server.
    ///   This can be passed to the FFmpeg SDK for actual data push.
    /// * [`crate::error::Error`] - If a stream with the same key already exists, the server
    ///   is not ready, or an internal error occurs, the corresponding error is returned.
    ///
    /// # Example
    ///
    /// ```rust,ignore
    /// # // Assume there are definitions and initializations for FfmpegContext, FfmpegScheduler, etc.
    ///
    /// // 1. Create and start the RTMP server
    /// let mut rtmp_server = EmbedRtmpServer::new("localhost:1935");
    /// rtmp_server.start().expect("Failed to start RTMP server");
    ///
    /// // 2. Create an RTMP "input" with app_name="my-app" and stream_key="my-stream"
    /// let output = rtmp_server
    ///     .create_rtmp_input("my-app", "my-stream")
    ///     .expect("Failed to create RTMP input");
    ///
    /// // 3. Prepare the FFmpeg context to push a local file to the newly created `Output`
    /// let context = FfmpegContext::builder()
    ///     .input("test.mp4")
    ///     .output(output)
    ///     .build()
    ///     .expect("Failed to build Ffmpeg context");
    ///
    /// // 4. Start FFmpeg to push "test.mp4" to the local RTMP server on "my-app/my-stream"
    /// FfmpegScheduler::new(context)
    ///     .start()
    ///     .expect("Failed to start Ffmpeg job");
    /// ```
    pub fn create_rtmp_input(
        &self,
        app_name: impl Into<String>,
        stream_key: impl Into<String>,
    ) -> crate::error::Result<Output> {
        // PERF-5a serialize-bypass: steady-state audio/video FLV tags are
        // handed to the scheduler already parsed (PublisherFeed::Media),
        // skipping the serialize→loopback→re-parse round-trip. Metadata and
        // control still travel as serialized RTMP chunk bytes on the same FIFO
        // feed (PublisherFeed::Raw), so the scheduler observes an identical,
        // in-order sequence to the pure-serialize path. External TCP clients
        // are unaffected — they never touch this feed.
        let feed_sender = self.create_bypass_feed_sender(app_name, stream_key)?;
        // PERF-3: both feed paths hold a WakeHandle — this internal Feed path
        // and raw `create_stream_sender` users (RtmpStreamSender::send wakes
        // per chunk). Wake once now so the reactor flushes the queued
        // connect/createStream/publish handshake immediately instead of
        // waiting for the first media frame or the 100ms poll fallback —
        // otherwise stream setup carries avoidable startup latency.
        let wake_handle = self.wake_handle.clone();
        if let Some(waker) = &wake_handle {
            waker.wake();
        }

        let mut flv_buffer = FlvBuffer::new();
        let mut serializer = ChunkSerializer::new();
        // A feed send fails only once the receiver died. Under a deliberate
        // stop that is the expected end of this publisher — classify it as
        // such instead of logging the send error a genuine failure gets.
        // The callback must still fail the write (the FFmpeg job has to
        // end), so the classification changes the reporting, not the flow.
        //
        // Scope: this classifies the SERVER lifecycle, not this feed's own
        // history. A feed torn down for its own fatal protocol error is
        // reported loudly at its removal site (the reactor warns when it
        // rejects the publisher), independent of what this classification
        // says later — a deliberate stop landing between that removal and
        // this send failure makes the calm line below true of the server
        // while the earlier warn still records why the feed itself died.
        //
        // The classification is best-effort by construction. First-writer-
        // wins on `terminal_cause` means a server crash that beat a late
        // stop() stays CAUSE_FATAL — never relabeled calm. In the other
        // direction the load below can, in principle, still read a stale
        // CAUSE_NONE during a deliberate stop: the send-failure observation
        // travels through the channel's disconnect flag, whose sender-side
        // fast path is a relaxed read, so it carries no happens-before edge
        // for this unrelated atomic. A stale read only ever shows the OLDER
        // value (CAUSE_NONE -> the loud error branch), so the worst case is
        // the pre-classification behavior for one racing write, never a
        // server crash reported as deliberate.
        let terminal_cause = self.terminal_cause.clone();
        let classify_feed_send_failure = move |what: &str, e: &dyn std::fmt::Debug| {
            if terminal_cause.load(Ordering::Acquire) == CAUSE_DELIBERATE {
                info!("The rtmp server was deliberately stopped; ending the in-process publisher (failing its {what} write)");
            } else {
                error!("Failed to send in-process {what}: {e:?}");
            }
        };
        let write_callback: Box<dyn FnMut(&[u8]) -> i32 + Send> =
            Box::new(move |buf: &[u8]| -> i32 {
                flv_buffer.write_data(buf);
                // One AVIO write can carry many FLV tags (the muxer hands over
                // 64KB blocks): drain every complete tag now, or the backlog
                // grows and the final tags of the stream are never sent.
                while let Some(mut flv_tag) = flv_buffer.get_flv_tag() {
                    flv_tag.header.stream_id = 1;
                    let tag_type = flv_tag.header.tag_type;

                    // 0x08 audio / 0x09 video: bypass the serializer. The
                    // (timestamp, data) handed over is byte-identical to what
                    // flv_tag_to_message_payload would build and the RTMP chunk
                    // round-trip would reconstruct, so the scheduler's sequence-
                    // header / keyframe-gate / GOP semantics are preserved exactly.
                    if tag_type == 0x08 || tag_type == 0x09 {
                        let timestamp = flv_tag.header.timestamp
                            | ((flv_tag.header.timestamp_ext as u32) << 24);
                        let feed = PublisherFeed::Media {
                            tag_type,
                            timestamp: RtmpTimestamp { value: timestamp },
                            data: flv_tag.data,
                        };
                        if let Err(e) = feed_sender.send(feed) {
                            classify_feed_send_failure("media tag", &e);
                            return -1;
                        }
                        // PERF-3: wake the reactor for each bypassed media tag, the
                        // same as the Raw path below — without it the parsed tag
                        // waits in the feed until the 100ms poll fallback, negating
                        // the PERF-5a bypass. The token coalesces repeated wakes.
                        if let Some(waker) = &wake_handle {
                            waker.wake();
                        }
                        continue;
                    }

                    // 0x12 metadata and anything else keep the serialize path: the
                    // scheduler consumes the parsed StreamMetadataChanged event,
                    // which needs the @setDataFrame wrapping + AMF decode.
                    match serializer.serialize(&flv_tag_to_message_payload(flv_tag), false, true) {
                        Ok(packet) => {
                            if let Err(e) = feed_sender.send(PublisherFeed::Raw(packet.bytes)) {
                                classify_feed_send_failure("RTMP packet", &e);
                                return -1;
                            }
                            // Wake the reactor for each enqueued packet. Unconditional
                            // (no message_sender.is_empty() gate): the reactor can
                            // drain the queue and sleep in poll() between an emptiness
                            // check and this send, so a was_empty gate loses the
                            // wakeup and the packet stalls until the 100ms poll
                            // fallback. Per-packet rather than once-after-the-batch:
                            // the channel is bounded (1024), so a large batch would
                            // block in send() before a post-batch wake ever ran,
                            // stranding the reactor. The waker's userspace gate
                            // coalesces the wakes into a single reactor drain, so a
                            // repeat wake costs an atomic flag test, not a syscall.
                            if let Some(waker) = &wake_handle {
                                waker.wake();
                            }
                        }
                        Err(e) => {
                            error!("Failed to serialize RTMP message: {:?}", e);
                            return -1;
                        }
                    }
                }
                buf.len() as i32
            });

        let output: Output = write_callback.into();

        Ok(output
            .set_format("flv")
            .set_video_codec("h264")
            .set_audio_codec("aac")
            .set_format_opt("flvflags", "no_duration_filesize"))
    }

    /// Creates a sender channel for an RTMP stream, identified by `app_name` and `stream_key`.
    /// Call this to publish an in-process stream directly by feeding raw,
    /// pre-packaged RTMP chunk bytes into the server's handling pipeline.
    ///
    /// # Parameters
    ///
    /// * `app_name` - The RTMP application name.
    /// * `stream_key` - The unique name (or key) for this stream. Must not already be in use.
    ///
    /// # Returns
    ///
    /// * [`RtmpStreamSender`] - A handle whose [`send`](RtmpStreamSender::send) feeds
    ///   raw RTMP bytes into the server's handling pipeline.
    /// * [`crate::error::Error`] - If a stream with the same key already exists or other
    ///   internal issues occur, an error is returned.
    ///
    /// # Notes
    ///
    /// * This function sets up the initial RTMP "connect" and "publish" commands automatically.
    /// * If you manually send bytes to the resulting channel, they should already be properly
    ///   packaged as RTMP chunks. Otherwise, the server might fail to parse them.
    pub fn create_stream_sender(
        &self,
        app_name: impl Into<String>,
        stream_key: impl Into<String>,
    ) -> crate::error::Result<RtmpStreamSender> {
        let stream_key = stream_key.into();
        // Claim the key atomically: `claim` inserts-or-fails, so two
        // concurrent creates for the same key cannot both pass. A separate
        // contains() check would let both pass before either registration
        // reached the reactor; both would return Ok and the loser would only
        // fail later with an opaque send error.
        let Ok(claim) = StreamKeyClaim::claim(self.stream_keys.clone(), stream_key.clone()) else {
            return Err(RtmpStreamAlreadyExists(stream_key));
        };

        let (sender, receiver) = crossbeam_channel::bounded(PUBLISHER_CHANNEL_CAPACITY);
        let (budget_guard, budget) = IngressBudget::new(PUBLISHER_INGRESS_HIGH_WATER_BYTES);
        self.register_publisher(claim, PublisherSource::Raw(receiver), budget_guard)?;

        let stream_sender = RtmpStreamSender {
            inner: sender,
            wake_handle: self.wake_handle.clone(),
            budget,
        };
        // Prime the raw byte channel with the connect / createStream / publish
        // handshake the server session expects before any media. The budgeted
        // quiet send keeps the account exact from the very first byte (the
        // reactor releases what it drains, primed handshake included) without
        // adding per-item wakes the plain send never had; a few hundred
        // control bytes into a fresh account can never park.
        for packet_bytes in build_publish_control(app_name.into(), stream_key)? {
            if stream_sender.send_quiet(packet_bytes).is_err() {
                error!("Can't send publish control command to rtmp server.");
                return Err(RtmpCreateStream.into());
            }
        }
        // The registration and its primed handshake ride a queue and a
        // channel the poller cannot see. Wake the reactor once so it picks
        // them up now instead of on the next 100ms poll fallback — the same
        // wake create_rtmp_input performs after priming; RtmpStreamSender
        // wakes per send, but nothing else announces this handshake.
        if let Some(wake_handle) = &self.wake_handle {
            wake_handle.wake();
        }
        Ok(stream_sender)
    }

    /// Registers an in-process publisher whose steady-state audio/video is
    /// delivered already parsed (PERF-5a serialize-bypass). Metadata and
    /// control still ride the same feed as serialized RTMP chunk bytes, so the
    /// scheduler sees an identical, in-order message sequence to the raw path.
    ///
    /// Returns the budget-gated feed sender, primed with the publish handshake.
    fn create_bypass_feed_sender(
        &self,
        app_name: impl Into<String>,
        stream_key: impl Into<String>,
    ) -> crate::error::Result<BudgetedFeedSender> {
        let stream_key = stream_key.into();
        // Claim the key atomically (insert-or-fail); see create_stream_sender.
        let Ok(claim) = StreamKeyClaim::claim(self.stream_keys.clone(), stream_key.clone()) else {
            return Err(RtmpStreamAlreadyExists(stream_key));
        };

        let (sender, receiver) = crossbeam_channel::bounded(PUBLISHER_CHANNEL_CAPACITY);
        let (budget_guard, budget) = IngressBudget::new(PUBLISHER_INGRESS_HIGH_WATER_BYTES);
        self.register_publisher(claim, PublisherSource::Feed(receiver), budget_guard)?;

        let feed_sender = BudgetedFeedSender {
            inner: sender,
            budget,
        };
        // Prime the feed with the same connect / createStream / publish bytes
        // the raw path would send, wrapped as PublisherFeed::Raw. The budgeted
        // send accounts these bytes like any media item, so the reactor's
        // per-round release never returns bytes nobody acquired; a handshake
        // into a fresh account can never park.
        for packet_bytes in build_publish_control(app_name.into(), stream_key)? {
            if feed_sender.send(PublisherFeed::Raw(packet_bytes)).is_err() {
                error!("Can't send publish control command to rtmp server.");
                return Err(RtmpCreateStream.into());
            }
        }
        Ok(feed_sender)
    }

    /// Hands a newly registered publisher's receiving end to the reactor.
    ///
    /// The registration carries the caller's `stream_keys` claim, and the
    /// claim is a drop-releasing guard: whichever side ends up holding the
    /// registration when it dies releases the key, with no path releasing
    /// twice. The ingress-budget guard rides the same lifecycle, so those
    /// same drops also close the budget and wake any producer parked at the
    /// byte gate. Concretely:
    /// - no registration queue, or the enqueue is refused because the intake
    ///   is closed (worker gone, server stopped) or at capacity: the
    ///   registration drops here, releasing the claim — the refusal and a
    ///   successful enqueue are the same critical section, so the reactor
    ///   side can never have seen it;
    /// - the reactor consumes the registration: `add_publisher` either
    ///   moves the still-armed claim into the accepted publisher's state
    ///   (released when that state drops — `remove_publisher`, or reactor
    ///   teardown) or drops a refused one;
    /// - the worker dies — cleanly, by panic, or before the reactor even
    ///   existed — with the registration still queued: the worker's
    ///   `RegistrationKillSwitch` drains it, releasing the claim.
    fn register_publisher(
        &self,
        claim: StreamKeyClaim,
        source: PublisherSource,
        budget: IngressBudgetGuard,
    ) -> crate::error::Result<()> {
        let registration = PublisherRegistration {
            claim,
            source,
            budget,
        };
        let registrations = match self.registrations.as_ref() {
            Some(registrations) => registrations,
            None => {
                error!("Publisher registration queue not initialized");
                return Err(RtmpCreateStream.into());
            }
        };

        match registrations.enqueue(registration) {
            Ok(()) => Ok(()),
            Err(EnqueueRefused::Closed(registration)) => {
                // The worker is gone, or the server was signaled to stop:
                // nothing will ever consume this registration. Dropping it
                // here releases its stream-key claim immediately.
                drop(registration);
                if self.status.load(Ordering::Acquire) != STATUS_END {
                    warn!("Rtmp server worker already exited. Can't create stream sender.");
                } else {
                    error!("Rtmp Server aborted. Can't create stream sender.");
                }
                Err(RtmpCreateStream)
            }
            Err(EnqueueRefused::Full(registration)) => {
                // The worker is alive but the reactor has a full queue of
                // earlier registrations to pick up. Refusing keeps the
                // parked key claims bounded; dropping the refusal reopens
                // this key immediately, so the caller may retry once the
                // backlog drains.
                drop(registration);
                warn!("Rtmp registration queue is full. Can't create stream sender.");
                Err(RtmpRegistrationQueueFull)
            }
        }
    }

    /// Stops the RTMP server: signals the listening and connection-handling
    /// threads to terminate, then joins them, so this returns only once
    /// teardown has settled — the listener socket is released, every
    /// connection is closed, and every publisher is torn down with its
    /// stream key freed. A caller can rebind the address or reuse a stream
    /// key immediately after this returns, without polling.
    ///
    /// Blocking is bounded but not hard-real-time: the reactor finishes the
    /// loop turn it is in, then drains still-queued tails for roughly its
    /// graceful-shutdown window (a few seconds, re-checked between drain
    /// passes, and only spent when a peer stopped reading); the accept
    /// thread notices within its ~100ms accept cycle. Concurrent stops on
    /// clones serialize behind one settlement and each returns only after
    /// it.
    ///
    /// One reentrant edge degrades to signal-only: user code reached
    /// through a user-installed global logger runs ON the server threads,
    /// and if such code calls `stop()`, joining from there would deadlock —
    /// that call silently skips the joins (silently, because logging from
    /// inside the logger's own frame could deadlock on the logger's lock),
    /// leaves them available to any other caller, and returns with only
    /// the signal sent.
    ///
    /// More generally: the joined threads log through the global logger
    /// while they wind down, so blocking in `stop()` while holding ANY
    /// resource that logger may acquire is a lock inversion. That has two
    /// shapes, neither detectable from this crate:
    /// - do not call `stop()` from inside a logger implementation (on any
    ///   thread) — a `log()` blocking here while holding the logger's own
    ///   internal lock deadlocks against the winding-down threads' log
    ///   calls;
    /// - do not call `stop()` while holding a lock your logger sink also
    ///   takes (e.g. an `Arc<Mutex<_>>` shared between application code
    ///   and a custom logger) — the server threads block on that lock in
    ///   their shutdown logging while `stop()` blocks joining them.
    ///
    /// The same discipline applies to joining any thread that logs; the
    /// server-thread reentrant shape above is the only one the crate can
    /// detect cheaply, and it is guarded.
    ///
    /// An FFmpeg job still pushing to this server (via
    /// [`create_rtmp_input`](Self::create_rtmp_input)) loses its feed and
    /// fails its next write; the server classifies that failure — best
    /// effort, by the server's terminal cause — as a deliberate stop
    /// rather than as an opaque send error. A feed that already died for
    /// its own protocol error before the stop keeps the warning it got at
    /// removal time. Stop such jobs first if their clean completion
    /// matters.
    ///
    /// # Example
    /// ```rust,ignore
    /// let server = EmbedRtmpServer::new("localhost:1935");
    /// // ... start and handle streaming
    /// server.stop();
    /// assert!(server.is_stopped());
    /// ```
    pub fn stop(self) -> EmbedRtmpServer<Ended> {
        self.signal_stop();
        settle_server_threads(&self.server_thread_ids, &self.threads);
        self.into_state()
    }
}

/// Handle connections using optimized Reactor
///
/// Replaces old multi-threaded handle_connections with single-threaded event-driven model:
/// - Uses epoll/kqueue/WSAPoll for IO multiplexing
/// - Write queue with backpressure management
/// - Strict drain until WouldBlock semantics
/// Whether an accept error means the process (EMFILE) or system (ENFILE) ran
/// out of file descriptors. `io::ErrorKind` has no stable variant for these,
/// so match on the raw OS error code.
fn is_fd_exhaustion(e: &std::io::Error) -> bool {
    #[cfg(unix)]
    {
        matches!(e.raw_os_error(), Some(code) if code == libc::EMFILE || code == libc::ENFILE)
    }
    #[cfg(windows)]
    {
        // WSAEMFILE: too many open sockets.
        e.raw_os_error() == Some(10024)
    }
    #[cfg(not(any(unix, windows)))]
    {
        let _ = e;
        false
    }
}

/// The single unwind boundary of the rtmp-server-worker thread.
///
/// The reactor is the only consumer of the connection/publisher channels. An
/// uncontained panic — in the reactor's construction just as much as in its
/// loop — would kill the worker thread without ever publishing `STATUS_END`:
/// `is_stopped()` would stay `false` forever and the accept thread — whose
/// loop exits only on the status flag or a disconnected channel send — would
/// keep feeding connections nobody drains. Containing the unwind here closes
/// the registration intake and publishes the terminal status so the accept
/// thread exits on its next ~100ms cycle, and returns normally so the
/// caller's `Reactor` drop still runs, closing every accepted connection.
///
/// Both terminal steps are idempotent: a clean return only reaches
/// `STATUS_END` through `signal_stop` (or the worker's own fatal paths, all
/// of which close the intake first), and this function touches neither the
/// intake nor the status unless it actually caught an unwind.
fn contain_reactor_panic(
    registrations: &RegistrationHandoff,
    status: &AtomicUsize,
    terminal_cause: &AtomicUsize,
    run_reactor: impl FnOnce(),
) {
    // AssertUnwindSafe: the closure borrows the caller's reactor slot mutably,
    // and `&mut T` is not `UnwindSafe`. That is acceptable here because after
    // an unwind the slot's reactor is never used again — the caller only
    // drops it.
    if let Err(payload) = std::panic::catch_unwind(std::panic::AssertUnwindSafe(run_reactor)) {
        // Publish the terminal state BEFORE logging: `error!` can run a
        // user-installed logger that itself panics, and that unwind must not
        // skip the store and leave the server half-dead after all. The
        // funnel closes the intake ahead of the store, so no create path
        // can observe the stopped status and still be told Ok; the worker's
        // kill switch re-closes it during its terminal drain — a no-op.
        close_intake_and_publish_end(registrations, status, terminal_cause);
        let msg = payload
            .downcast_ref::<&str>()
            .copied()
            .or_else(|| payload.downcast_ref::<String>().map(String::as_str))
            .unwrap_or("non-string panic payload")
            .to_string();
        let report = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
            error!("Rtmp reactor panicked ({msg}); the server is stopped and all connections will be closed.");
        }));
        if let Err(report_payload) = report {
            crate::core::packet_sink::dispose_panic_payload(report_payload);
        }
        crate::core::packet_sink::dispose_panic_payload(payload);
    }
}

fn handle_connections(
    connection_receiver: crossbeam_channel::Receiver<TcpStream>,
    registrations: Arc<RegistrationHandoff>,
    gop_limit: usize,
    max_connections: Option<usize>,
    status: Arc<AtomicUsize>,
    terminal_cause: Arc<AtomicUsize>,
    waker: Option<Waker>,
) {
    // FIRST statement of the worker, before the fallible reactor
    // construction and before any log call: arm the kill switch. However
    // this thread ends — `Reactor::new` failing, a user-installed logger
    // panicking, `run()` unwinding, or a clean return — the switch's drop
    // closes the registration intake and releases the key claims of every
    // registration still queued. Declared before `reactor_slot` so it drops
    // AFTER the reactor: the drain it performs is the worker's last word,
    // with nothing left that could enqueue concurrently forever.
    let kill_switch = RegistrationKillSwitch::arm(registrations);
    // The reactor is CREATED inside the contained closure so that a panicking
    // constructor also publishes `STATUS_END`, but STORED in this outer slot
    // so that on a contained panic it is dropped HERE, after the terminal
    // status is published — a `Drop` panicking mid-unwind would abort the
    // process instead. This drop is also what releases the key claims of
    // publishers still accepted when the worker dies: each claim lives in
    // its `PublisherState`, torn down with the reactor's publisher slab.
    let mut reactor_slot: Option<Reactor> = None;
    contain_reactor_panic(kill_switch.handoff(), &status, &terminal_cause, || {
        let reactor = match Reactor::new(gop_limit, max_connections, status.clone()) {
            Ok(r) => r,
            Err(e) => {
                // Publish the terminal state BEFORE logging, mirroring the
                // panic arm above: `error!` can run a user-installed logger
                // that itself panics, and that unwind must not leave the
                // worker dead with the status still running and the accept
                // loop parked forever. The funnel closes the intake ahead
                // of the store; the kill switch re-closes it on the way
                // out — a no-op.
                close_intake_and_publish_end(kill_switch.handoff(), &status, &terminal_cause);
                error!("Failed to create Reactor: {:?}", e);
                return;
            }
        };
        reactor_slot
            .insert(reactor)
            .run(connection_receiver, kill_switch.handoff(), waker)
    });

    if status.load(Ordering::Acquire) != STATUS_END {
        close_intake_and_publish_end(kill_switch.handoff(), &status, &terminal_cause);
        let report = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
            error!("Rtmp Server aborted.");
        }));
        if let Err(payload) = report {
            crate::core::packet_sink::dispose_panic_payload(payload);
        }
    }
}

/// Serializes the `connect` / `createStream` / `publish` command sequence an
/// in-process publisher sends before any media. Shared by both the raw byte
/// path ([`create_stream_sender`](EmbedRtmpServer<Running>::create_stream_sender))
/// and the media-bypass path ([`create_rtmp_input`](EmbedRtmpServer<Running>::create_rtmp_input))
/// so their control bytes stay identical.
pub(crate) fn build_publish_control(
    app_name: String,
    stream_key: String,
) -> crate::error::Result<[Vec<u8>; 3]> {
    let mut serializer = ChunkSerializer::new();

    // connect
    let mut properties: HashMap<String, Amf0Value> = HashMap::new();
    properties.insert("app".to_string(), Amf0Value::Utf8String(app_name));
    let connect_cmd = RtmpMessage::Amf0Command {
        command_name: "connect".to_string(),
        transaction_id: 1.0,
        command_object: Amf0Value::Object(properties),
        additional_arguments: Vec::new(),
    }
    .into_message_payload(RtmpTimestamp { value: 0 }, 0)
    .map_err(|e| {
        error!("Failed to create connect command: {:?}", e);
        RtmpCreateStream
    })?;
    let connect_packet = serializer
        .serialize(&connect_cmd, false, true)
        .map_err(|e| {
            error!("Failed to serialize connect command: {:?}", e);
            RtmpCreateStream
        })?;

    // createStream
    let create_stream_cmd = RtmpMessage::Amf0Command {
        command_name: "createStream".to_string(),
        transaction_id: 2.0,
        command_object: Amf0Value::Null,
        additional_arguments: Vec::new(),
    }
    .into_message_payload(RtmpTimestamp { value: 0 }, 1)
    .map_err(|e| {
        error!("Failed to create createStream command: {:?}", e);
        RtmpCreateStream
    })?;
    let create_stream_packet = serializer
        .serialize(&create_stream_cmd, false, true)
        .map_err(|e| {
            error!("Failed to serialize createStream command: {:?}", e);
            RtmpCreateStream
        })?;

    // publish
    let arguments = vec![
        Amf0Value::Utf8String(stream_key),
        Amf0Value::Utf8String("live".into()),
    ];
    let publish_cmd = RtmpMessage::Amf0Command {
        command_name: "publish".to_string(),
        transaction_id: 3.0,
        command_object: Amf0Value::Null,
        additional_arguments: arguments,
    }
    .into_message_payload(RtmpTimestamp { value: 0 }, 1)
    .map_err(|e| {
        error!("Failed to create publish command: {:?}", e);
        RtmpCreateStream
    })?;
    let publish_packet = serializer
        .serialize(&publish_cmd, false, true)
        .map_err(|e| {
            error!("Failed to serialize publish command: {:?}", e);
            RtmpCreateStream
        })?;

    Ok([
        connect_packet.bytes,
        create_stream_packet.bytes,
        publish_packet.bytes,
    ])
}

pub(crate) fn flv_tag_to_message_payload(flv_tag: FlvTag) -> MessagePayload {
    let timestamp = flv_tag.header.timestamp | ((flv_tag.header.timestamp_ext as u32) << 24);

    let type_id = flv_tag.header.tag_type;
    let message_stream_id = flv_tag.header.stream_id;

    let data = if type_id == 0x12 {
        wrap_metadata(flv_tag.data)
    } else {
        flv_tag.data
    };

    MessagePayload {
        timestamp: RtmpTimestamp { value: timestamp },
        type_id,
        message_stream_id,
        data,
    }
}

fn wrap_metadata(data: Bytes) -> Bytes {
    let s = "@setDataFrame";

    let insert_len = 16;

    let mut bytes = bytes::BytesMut::with_capacity(insert_len + data.len());

    bytes.put_u8(0x02);
    bytes.put_u16(s.len() as u16);
    bytes.put(s.as_bytes());

    bytes.put(data);

    bytes.freeze()
}

// ============================================================================
// StreamBuilder API - Simplified RTMP streaming interface
// ============================================================================

use crate::core::context::ffmpeg_context::FfmpegContext;
use crate::core::context::input::Input;
use crate::core::scheduler::ffmpeg_scheduler::{FfmpegScheduler, Running as SchedulerRunning};
use crate::error::StreamError;
use std::path::{Path, PathBuf};

/// A builder for creating RTMP streaming sessions with a simplified API.
///
/// This builder provides a fluent interface for configuring and starting
/// RTMP streaming without needing to manually manage the server lifecycle.
///
/// # Example
///
/// ```rust,ignore
/// use ez_ffmpeg::rtmp::embed_rtmp_server::EmbedRtmpServer;
///
/// let handle = EmbedRtmpServer::stream_builder()
///     .address("localhost:1935")
///     .app_name("live")
///     .stream_key("stream1")
///     .input_file("video.mp4")
///     // readrate defaults to 1.0 (realtime)
///     .start()?;
///
/// handle.wait()?;
/// ```
/// RAII guard that stops a started server unless explicitly disarmed.
///
/// `EmbedRtmpServer` cannot implement `Drop` (`into_state` moves out of `self`,
/// which `Drop` forbids), so a partially-built [`StreamHandle`] would otherwise
/// leak the running server on any post-start failure: the two worker threads
/// keep polling `status` and hold the listener port bound. This guard is armed
/// right after `server.start()` and disarmed only once the server is handed to
/// a `StreamHandle`; any early return drops it and calls `signal_stop`.
struct ServerStopGuard {
    server: Option<Arc<EmbedRtmpServer<Running>>>,
}

impl Drop for ServerStopGuard {
    fn drop(&mut self) {
        if let Some(server) = &self.server {
            server.signal_stop();
        }
    }
}

impl ServerStopGuard {
    /// Take ownership of the server out of the guard, so its `Drop` becomes a
    /// no-op. Called on the success path where a `StreamHandle` assumes the
    /// stop responsibility.
    fn disarm(mut self) -> Arc<EmbedRtmpServer<Running>> {
        self.server
            .take()
            .expect("ServerStopGuard is armed exactly once before disarm")
    }
}

pub struct StreamBuilder {
    address: Option<String>,
    app_name: Option<String>,
    stream_key: Option<String>,
    input_file: Option<PathBuf>,
    readrate: Option<f32>,
    gop_limit: Option<usize>,
    max_connections: Option<usize>,
}

impl Default for StreamBuilder {
    fn default() -> Self {
        Self::new()
    }
}

impl StreamBuilder {
    /// Creates a new `StreamBuilder` with default settings.
    ///
    /// By default, `readrate` is set to `1.0` (real-time playback speed),
    /// which is equivalent to FFmpeg's `-re` flag. This is the recommended
    /// setting for live RTMP streaming scenarios.
    pub fn new() -> Self {
        Self {
            address: None,
            app_name: None,
            stream_key: None,
            input_file: None,
            readrate: Some(1.0), // Default to real-time speed for live streaming
            gop_limit: None,
            max_connections: None,
        }
    }

    /// Sets the address for the RTMP server (e.g., "localhost:1935").
    pub fn address(mut self, address: impl Into<String>) -> Self {
        self.address = Some(address.into());
        self
    }

    /// Sets the RTMP application name.
    pub fn app_name(mut self, app_name: impl Into<String>) -> Self {
        self.app_name = Some(app_name.into());
        self
    }

    /// Sets the stream key (publishing name).
    pub fn stream_key(mut self, stream_key: impl Into<String>) -> Self {
        self.stream_key = Some(stream_key.into());
        self
    }

    /// Sets the input file path to stream.
    pub fn input_file(mut self, path: impl AsRef<Path>) -> Self {
        self.input_file = Some(path.as_ref().to_path_buf());
        self
    }

    /// Sets the read rate for the input file.
    ///
    /// A value of 1.0 means realtime playback speed.
    /// This is useful for simulating live streaming from a file.
    pub fn readrate(mut self, rate: f32) -> Self {
        self.readrate = Some(rate);
        self
    }

    /// Sets the GOP (Group of Pictures) limit for the RTMP server.
    ///
    /// This controls how many GOPs are buffered for new subscribers.
    pub fn gop_limit(mut self, limit: usize) -> Self {
        self.gop_limit = Some(limit);
        self
    }

    /// Sets the maximum number of connections the server will accept.
    pub fn max_connections(mut self, max: usize) -> Self {
        self.max_connections = Some(max);
        self
    }

    /// Starts the RTMP streaming session.
    ///
    /// This method validates all required parameters, starts the RTMP server,
    /// and begins streaming the input file.
    ///
    /// # Required Parameters
    ///
    /// - `address`: The server address
    /// - `app_name`: The RTMP application name
    /// - `stream_key`: The stream key (publishing name)
    /// - `input_file`: The file to stream
    ///
    /// # Returns
    ///
    /// A `StreamHandle` that can be used to wait for completion or manage the stream.
    ///
    /// # Errors
    ///
    /// Returns `StreamError` if:
    /// - Any required parameter is missing
    /// - The input file does not exist
    /// - The server fails to start
    /// - FFmpeg context creation fails
    pub fn start(self) -> Result<StreamHandle, StreamError> {
        // Validate required parameters
        let address = self
            .address
            .ok_or(StreamError::MissingParameter("address"))?;
        let app_name = self
            .app_name
            .ok_or(StreamError::MissingParameter("app_name"))?;
        let stream_key = self
            .stream_key
            .ok_or(StreamError::MissingParameter("stream_key"))?;
        let input_file = self
            .input_file
            .ok_or(StreamError::MissingParameter("input_file"))?;

        // Validate input file exists and is a file (not a directory)
        if !input_file.is_file() {
            return Err(StreamError::InputNotFound { path: input_file });
        }

        // Create and configure the server
        let mut server = if let Some(gop_limit) = self.gop_limit {
            EmbedRtmpServer::new_with_gop_limit(&address, gop_limit)
        } else {
            EmbedRtmpServer::new(&address)
        };

        if let Some(max_conn) = self.max_connections {
            server = server.set_max_connections(max_conn);
        }

        // Start the server, then immediately arm a stop guard. EmbedRtmpServer
        // has no Drop, so any `?` failure below (create_rtmp_input, FFmpeg
        // build/start) would otherwise drop the Arc without stopping the two
        // server threads: they leak forever and the port stays AddrInUse. The
        // guard is disarmed only once ownership passes to the StreamHandle,
        // whose own Drop then owns the stop.
        let server = server.start().map_err(StreamError::Ffmpeg)?;
        let guard = ServerStopGuard {
            server: Some(Arc::new(server)),
        };

        // Create the RTMP output (a `?` here drops the guard -> signal_stop).
        let output = guard
            .server
            .as_ref()
            .unwrap()
            .create_rtmp_input(&app_name, &stream_key)
            .map_err(StreamError::Ffmpeg)?;

        // Create the input with optional readrate
        let input_path = input_file.to_string_lossy().to_string();
        let mut input = Input::from(input_path);
        if let Some(rate) = self.readrate {
            input = input.set_readrate(rate);
        }

        // Build and start the FFmpeg context (a `?` here drops the guard too).
        let scheduler = FfmpegContext::builder()
            .input(input)
            .output(output)
            .build()
            .map_err(StreamError::Ffmpeg)?
            .start()
            .map_err(StreamError::Ffmpeg)?;

        // Success: hand the server to the StreamHandle and disarm the guard so
        // the handle's Drop (not the guard's) owns the stop from here on.
        let server = guard.disarm();
        Ok(StreamHandle {
            server,
            scheduler: Some(scheduler),
        })
    }
}

/// A handle to a running RTMP streaming session.
///
/// This handle manages the lifecycle of both the RTMP server and the FFmpeg
/// streaming context. When dropped, it will attempt to clean up resources.
///
/// # Example
///
/// ```rust,ignore
/// let handle = EmbedRtmpServer::stream_builder()
///     .address("localhost:1935")
///     .app_name("live")
///     .stream_key("stream1")
///     .input_file("video.mp4")
///     .start()?;
///
/// // Wait for streaming to complete
/// handle.wait()?;
/// ```
pub struct StreamHandle {
    server: Arc<EmbedRtmpServer<Running>>,
    scheduler: Option<FfmpegScheduler<SchedulerRunning>>,
}

impl StreamHandle {
    /// Waits for the streaming session to complete.
    ///
    /// This method blocks until the FFmpeg context finishes processing
    /// (e.g., when the input file ends or an error occurs).
    ///
    /// # Returns
    ///
    /// Returns `Ok(())` if streaming completed successfully, or an error
    /// if something went wrong during streaming.
    pub fn wait(mut self) -> Result<(), StreamError> {
        if let Some(scheduler) = self.scheduler.take() {
            scheduler.wait().map_err(StreamError::Ffmpeg)?;
        }
        Ok(())
    }

    /// The actual bound socket address of the underlying RTMP server.
    ///
    /// Useful when the builder was given `"127.0.0.1:0"`: the OS assigns a real
    /// port, and this surfaces it (delegating to
    /// [`EmbedRtmpServer::local_addr`]) so tests and callers can observe the
    /// port without a bind/drop/rebind race on a pre-probed one.
    pub fn local_addr(&self) -> Option<std::net::SocketAddr> {
        self.server.local_addr()
    }
}

impl Drop for StreamHandle {
    fn drop(&mut self) {
        // Wait-then-signal. Waiting first preserves the drain semantics: a
        // handle dropped mid-stream still delivers the remaining frames to
        // connected watchers before the server goes away — best-effort, not
        // absolute: a watcher whose join-replay budget was exhausted keeps
        // only its bounded backlog, and stop-time teardown does not re-run
        // finished-status delivery for it (cancelling the FFmpeg job instead
        // is a separate concern, out of scope here). The explicit stop after
        // it is what actually releases the listener port and the worker
        // threads — dropping the Arc alone never did: the threads own clones
        // of the status flag and keep running (and keep the port bound)
        // until the flag flips.
        if let Some(scheduler) = self.scheduler.take() {
            let _ = scheduler.wait();
        }
        self.server.signal_stop();
    }
}

impl EmbedRtmpServer<Initialization> {
    /// Creates a new `StreamBuilder` for simplified RTMP streaming.
    ///
    /// This is the recommended entry point for simple streaming scenarios
    /// where you want to stream a file to an embedded RTMP server.
    ///
    /// # Example
    ///
    /// ```rust,ignore
    /// use ez_ffmpeg::rtmp::embed_rtmp_server::EmbedRtmpServer;
    ///
    /// let handle = EmbedRtmpServer::stream_builder()
    ///     .address("localhost:1935")
    ///     .app_name("live")
    ///     .stream_key("stream1")
    ///     .input_file("video.mp4")
    ///     .start()?;
    ///
    /// handle.wait()?;
    /// ```
    ///
    /// For more complex scenarios requiring full control over the server
    /// and FFmpeg context, use the traditional API:
    ///
    /// ```rust,ignore
    /// let server = EmbedRtmpServer::new("localhost:1935").start()?;
    /// let output = server.create_rtmp_input("app", "stream")?;
    /// // ... configure Input and FfmpegContext manually
    /// ```
    pub fn stream_builder() -> StreamBuilder {
        StreamBuilder::new()
    }
}

#[cfg(test)]
mod bypass_parity_tests {
    //! PERF-5a: the in-process media bypass hands the scheduler a
    //! `(timestamp, data)` pair taken directly from the parsed FLV tag. These
    //! tests prove that pair is byte-identical to what the pure-serialize path
    //! reconstructs (FLV tag -> `flv_tag_to_message_payload` -> RTMP chunk
    //! serialize -> deserialize -> `MessagePayload`), so the scheduler observes
    //! an identical sequence either way.
    use super::*;
    use crate::flv::flv_tag::FlvTag;
    use crate::flv::flv_tag_header::FlvTagHeader;
    use rml_rtmp::chunk_io::ChunkDeserializer;

    fn make_tag(tag_type: u8, timestamp: u32, timestamp_ext: u8, data: Vec<u8>) -> FlvTag {
        FlvTag {
            header: FlvTagHeader {
                tag_type,
                data_size: data.len() as u32,
                timestamp,
                timestamp_ext,
                stream_id: 1,
            },
            data: Bytes::from(data),
            previous_tag_size: 0,
        }
    }

    /// Assert the bypass `(timestamp, data)` equals the serialize round-trip.
    fn assert_parity(tag_type: u8, timestamp: u32, timestamp_ext: u8, data: Vec<u8>) {
        let tag = make_tag(tag_type, timestamp, timestamp_ext, data);

        // What the bypass path hands to the scheduler, straight from the tag.
        let bypass_timestamp = tag.header.timestamp | ((tag.header.timestamp_ext as u32) << 24);
        let bypass_data = tag.data.clone();

        // What the serialize path reconstructs: payload -> chunk bytes ->
        // deserialize -> payload.
        let payload = flv_tag_to_message_payload(tag);
        let mut serializer = ChunkSerializer::new();
        let packet = serializer
            .serialize(&payload, false, true)
            .expect("serialize");
        let mut deserializer = ChunkDeserializer::new();
        let round = deserializer
            .get_next_message(&packet.bytes)
            .expect("deserialize")
            .expect("a complete message from the serialized chunks");

        assert_eq!(
            round.type_id, tag_type,
            "tag type parity for {tag_type:#04x}"
        );
        assert_eq!(
            round.timestamp.value, bypass_timestamp,
            "timestamp parity for tag {tag_type:#04x}"
        );
        assert_eq!(
            round.data, bypass_data,
            "payload parity for tag {tag_type:#04x}"
        );
    }

    #[test]
    fn video_sequence_header_round_trips_identically() {
        // AVC sequence header (0x17 0x00 ...), timestamp 0.
        assert_parity(
            0x09,
            0,
            0,
            vec![0x17, 0x00, 0x00, 0x00, 0x00, 0x01, 0x64, 0x00, 0x1f],
        );
    }

    #[test]
    fn audio_sequence_header_round_trips_identically() {
        // AAC AudioSpecificConfig (0xaf 0x00 ...).
        assert_parity(0x08, 0, 0, vec![0xaf, 0x00, 0x12, 0x10]);
    }

    #[test]
    fn large_keyframe_spanning_multiple_chunks_round_trips_identically() {
        // A keyframe larger than the default 128-byte chunk size forces the
        // serializer to split it into continuation chunks; the deserializer
        // must reassemble the exact same bytes.
        let mut data = vec![0x17, 0x01, 0x00, 0x00, 0x00];
        data.extend((0u16..400).map(|i| (i & 0xff) as u8));
        assert_parity(0x09, 0x1234, 0, data);
    }

    #[test]
    fn delta_frame_round_trips_identically() {
        assert_parity(0x09, 0x0001_0000, 0, vec![0x27, 0x01, 0x00, 0x11, 0x22]);
    }

    #[test]
    fn extended_timestamp_round_trips_identically() {
        // timestamp field saturated (0xFFFFFF) plus an extension byte forces
        // the RTMP extended-timestamp encoding; the full 32-bit value must
        // survive the round-trip.
        assert_parity(0x08, 0x00ff_ffff, 0x01, vec![0xaf, 0x01, 0xAA, 0xBB]);
    }

    #[test]
    fn audio_and_video_tags_never_wrap_their_payload() {
        // Unlike 0x12 metadata (which flv_tag_to_message_payload prefixes with
        // @setDataFrame), media payloads must pass through untouched — the
        // bypass relies on this to skip the serializer entirely.
        let audio = make_tag(0x08, 10, 0, vec![0xaf, 0x01, 0x01, 0x02, 0x03]);
        let video = make_tag(0x09, 10, 0, vec![0x27, 0x01, 0x09, 0x08, 0x07]);
        assert_eq!(
            flv_tag_to_message_payload(audio.clone()).data,
            audio.data,
            "audio payload must not be wrapped"
        );
        assert_eq!(
            flv_tag_to_message_payload(video.clone()).data,
            video.data,
            "video payload must not be wrapped"
        );
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::core::context::ffmpeg_context::FfmpegContext;
    use crate::core::context::input::Input;
    use crate::core::context::output::Output;
    use crate::core::scheduler::ffmpeg_scheduler::FfmpegScheduler;
    use ffmpeg_next::time::current;
    use std::sync::atomic::AtomicBool;
    use std::thread::sleep;
    use std::time::Duration;

    /// Poll (up to ~2s) until `addr` can be bound again. The accept thread
    /// releases the listener within one ~100ms accept cycle of the stop
    /// signal, so a successful rebind proves the stop actually tore the
    /// server down rather than merely flipping a flag.
    fn wait_for_port_release(addr: std::net::SocketAddr) -> bool {
        let deadline = std::time::Instant::now() + Duration::from_secs(2);
        loop {
            match std::net::TcpListener::bind(addr) {
                Ok(_) => return true,
                Err(_) if std::time::Instant::now() < deadline => sleep(Duration::from_millis(20)),
                Err(_) => return false,
            }
        }
    }

    // API-hygiene regression: the stream sender must report a *stream*-scoped
    // close (not a server-thread exit) when its consumer is gone, and stay
    // multi-producer like the `crossbeam_channel::Sender` it used to be.
    #[test]
    fn stream_sender_reports_stream_closed_and_clones_share_the_stream() {
        let (tx, rx) = crossbeam_channel::bounded::<Vec<u8>>(4);
        // The reactor-side guard stays alive for the whole test: only the
        // channel disconnect below may produce the error.
        let (_budget_guard, budget) = IngressBudget::new(PUBLISHER_INGRESS_HIGH_WATER_BYTES);
        let sender = RtmpStreamSender {
            inner: tx,
            wake_handle: None,
            budget,
        };

        // A clone feeds the same stream: a chunk pushed through the clone is
        // observed on the single receiver (the old handle was `Clone`).
        let clone = sender.clone();
        clone
            .send(b"chunk".to_vec())
            .expect("send on a live stream");
        assert_eq!(rx.recv().unwrap(), b"chunk".to_vec());

        // Dropping the consumer is a stream close, not a server-thread exit:
        // the error must be RtmpStreamClosed, never RtmpThreadExited.
        drop(rx);
        assert_eq!(
            sender.send(b"late".to_vec()),
            Err(crate::error::Error::RtmpStreamClosed),
        );
    }

    // H7 regression: stop() used to only flip the status flag; the accept
    // thread parked on the listener kept the port bound, so a start-stop-start
    // cycle on the same address failed with AddrInUse.
    #[test]
    fn stopped_server_releases_its_port_for_rebind() {
        let server = EmbedRtmpServer::new("127.0.0.1:0").start().expect("start");
        let addr = server.local_addr().expect("bound address");

        // Sanity: while the server runs, the port is genuinely held.
        assert!(
            std::net::TcpListener::bind(addr).is_err(),
            "the running server must hold its port"
        );

        let stopped = server.stop();
        assert!(stopped.is_stopped());
        assert!(
            wait_for_port_release(addr),
            "the port must be rebindable within 2s of stop()"
        );
    }

    // H7: a StreamHandle drop must stop the server it holds. Before the fix
    // its Drop only waited on the scheduler and let the Arc'd server leak its
    // threads and port forever.
    #[test]
    fn stream_handle_drop_stops_the_server() {
        let server = EmbedRtmpServer::new("127.0.0.1:0").start().expect("start");
        let addr = server.local_addr().expect("bound address");
        let server = Arc::new(server);
        let observer = server.clone();

        let handle = StreamHandle {
            server,
            scheduler: None,
        };
        drop(handle);

        assert!(
            observer.is_stopped(),
            "dropping the handle must signal the server to stop"
        );
        assert!(
            wait_for_port_release(addr),
            "the port must be rebindable within 2s of the handle drop"
        );
    }

    // F6: the post-start failure path — a StreamBuilder that starts the server
    // but then fails to build the FFmpeg job must not leak it. Rather than a
    // racy probe/drop/rebind on a fixed port (which a parallel test could steal
    // between the drop and the builder's bind), drive the exact RAII path
    // directly: start on port 0, read the OS-assigned address, arm the
    // ServerStopGuard, then drop it as any post-start `?` would — no
    // reserve/drop/rebind window.
    #[test]
    fn server_stop_guard_drop_releases_the_port() {
        let server = EmbedRtmpServer::new("127.0.0.1:0").start().expect("start");
        let addr = server.local_addr().expect("bound address");

        // The port is genuinely held while the guard owns the running server.
        assert!(
            std::net::TcpListener::bind(addr).is_err(),
            "the running server must hold its port while the guard is armed"
        );

        // Arm the guard exactly as StreamBuilder::start does, then simulate the
        // post-start failure by dropping it — its Drop must signal_stop.
        let guard = ServerStopGuard {
            server: Some(Arc::new(server)),
        };
        drop(guard);

        assert!(
            wait_for_port_release(addr),
            "dropping the armed guard (a post-start failure) must release the port"
        );
    }

    // H8.b regression: the reactor used to receive a `.clone()` of the
    // DashSet — a deep copy — so keys it inserted never became visible to the
    // duplicate-key check in create_* and RtmpStreamAlreadyExists was dead
    // code: two publishers could claim the same stream key.
    #[test]
    fn duplicate_stream_key_is_rejected_once_registered() {
        let server = EmbedRtmpServer::new("127.0.0.1:0").start().expect("start");
        // Keep the Output alive: dropping it drops the feed sender, which
        // deregisters the publisher and frees the key again.
        let _output = server
            .create_rtmp_input("app", "dup-key")
            .expect("first create must succeed");

        // The key is claimed synchronously inside create_*, before the
        // registration ever reaches the reactor thread.
        assert!(
            server.stream_keys.contains("dup-key"),
            "the stream key must be claimed by the time create returns"
        );

        let second = server.create_rtmp_input("app", "dup-key");
        assert!(
            matches!(
                second,
                Err(crate::error::Error::RtmpStreamAlreadyExists(ref key)) if key == "dup-key"
            ),
            "a second create for a registered key must fail with RtmpStreamAlreadyExists"
        );

        server.stop();
    }

    // Claiming a stream key must be atomic with the duplicate check: when two
    // threads race create with the same key, exactly one may get Ok and the
    // other must fail immediately with the typed RtmpStreamAlreadyExists
    // error, not with a later opaque send failure.
    #[test]
    fn racing_creates_for_same_key_yield_exactly_one_winner() {
        let server = EmbedRtmpServer::new("127.0.0.1:0").start().expect("start");

        for round in 0..8 {
            let key = format!("race-key-{round}");
            let barrier = std::sync::Barrier::new(2);
            let (a, b) = std::thread::scope(|s| {
                let ta = s.spawn(|| {
                    barrier.wait();
                    server.create_rtmp_input("app", key.as_str())
                });
                let tb = s.spawn(|| {
                    barrier.wait();
                    server.create_rtmp_input("app", key.as_str())
                });
                (ta.join().expect("thread a"), tb.join().expect("thread b"))
            });

            let oks = a.is_ok() as usize + b.is_ok() as usize;
            assert_eq!(
                oks, 1,
                "round {round}: exactly one racing create may claim the key, got {oks} Ok"
            );
            let loser = if a.is_ok() { b } else { a };
            assert!(
                matches!(
                    loser,
                    Err(crate::error::Error::RtmpStreamAlreadyExists(ref k)) if k == &key
                ),
                "round {round}: the losing create must fail with RtmpStreamAlreadyExists"
            );
        }

        server.stop();
    }

    // H7: signal_stop is idempotent — a second signal (or a stop() after a
    // signal) must be a harmless no-op.
    #[test]
    fn double_stop_signal_is_idempotent() {
        let server = EmbedRtmpServer::new("127.0.0.1:0").start().expect("start");
        let addr = server.local_addr().expect("bound address");

        server.signal_stop();
        assert!(server.is_stopped());
        server.signal_stop();
        assert!(server.is_stopped());

        let ended = server.stop();
        assert!(ended.is_stopped());
        assert!(wait_for_port_release(addr));
    }

    // R-A2: stop() is a settlement barrier, not just a signal. When it
    // returns, both server threads must have exited: the listener is
    // released (a bind succeeds on the FIRST attempt, no retry loop) and
    // every in-process publisher torn down with its key claim freed (no
    // polling for the reactor to "get around to it").
    #[test]
    fn stop_settles_the_server_threads_before_returning() {
        let server = EmbedRtmpServer::new("127.0.0.1:0").start().expect("start");
        let addr = server.local_addr().expect("bound address");
        let observer = server.clone();

        // A live in-process publisher whose claim only the worker's teardown
        // can release. Kept alive across stop(): the release must come from
        // the joined reactor, not from this Output dropping.
        let _live = server
            .create_rtmp_input("app", "settlement-key")
            .expect("create while running must succeed");
        assert!(observer.stream_keys.contains("settlement-key"));

        let ended = server.stop();
        assert!(ended.is_stopped());

        // No wait_for_port_release here — that helper exists for signal-only
        // paths. A settled stop has already torn everything down.
        assert!(
            !observer.stream_keys.contains("settlement-key"),
            "stop() must not return before the worker released the key claims"
        );
        assert!(
            std::net::TcpListener::bind(addr).is_ok(),
            "stop() must not return before the accept thread released the listener"
        );
    }

    // The settlement barrier must never deadlock on reentrancy: user code
    // reached through the global logger runs ON the server threads and may
    // call stop(). Such a call must return promptly WITHOUT touching the
    // registry — the handles stay available, so a later stop() from any
    // other thread still settles for real.
    #[test]
    fn settlement_from_a_server_thread_is_signal_only_and_preserves_handles() {
        let server_thread_ids: Arc<Mutex<Vec<std::thread::ThreadId>>> = Default::default();
        let threads: Arc<Mutex<Vec<JoinHandle<()>>>> = Default::default();
        let (registered_tx, registered_rx) = std::sync::mpsc::channel::<()>();
        let (settled_tx, settled_rx) = std::sync::mpsc::channel::<()>();

        let ids_probe = server_thread_ids.clone();
        let threads_probe = threads.clone();
        let handle = std::thread::Builder::new()
            .name("reentrancy-probe".to_string())
            .spawn(move || {
                // Hold until the spawner registered this thread's id and
                // handle, then run the settlement from inside that thread —
                // the shape of a logger-held clone calling stop().
                registered_rx
                    .recv()
                    .expect("the registration signal must arrive");
                settle_server_threads(&ids_probe, &threads_probe);
                settled_tx
                    .send(())
                    .expect("report that the settlement returned");
            })
            .expect("spawn the probe thread");

        server_thread_ids.lock().unwrap().push(handle.thread().id());
        threads.lock().unwrap().push(handle);
        registered_tx.send(()).expect("signal the registration");

        // Deadline-bounded receive, not synchronization: the reentrant call
        // returns at once; a self-join would hang forever and only this
        // bound turns that hang into a test failure.
        settled_rx
            .recv_timeout(Duration::from_secs(5))
            .expect("settlement on a server thread must be signal-only, not a deadlock");
        assert_eq!(
            threads.lock().unwrap().len(),
            1,
            "the reentrant call must leave the registry untouched"
        );

        // Any other thread can still run the real settlement afterwards.
        settle_server_threads(&server_thread_ids, &threads);
        assert!(
            threads.lock().unwrap().is_empty(),
            "a later settlement from a non-server thread must consume and join the handle"
        );
    }

    // Racing stops on clones must EACH return only after the server threads
    // exited: the second caller serializes behind the first's joins on the
    // registry lock. Each settler asserts the exit flag the moment its
    // settlement returns — an implementation that returned early would trip
    // it.
    #[test]
    fn concurrent_settlements_both_wait_for_the_thread_exit() {
        let server_thread_ids: Arc<Mutex<Vec<std::thread::ThreadId>>> = Default::default();
        let threads: Arc<Mutex<Vec<JoinHandle<()>>>> = Default::default();
        let exited = Arc::new(AtomicBool::new(false));
        let (release_tx, release_rx) = std::sync::mpsc::channel::<()>();

        let exited_worker = exited.clone();
        let worker = std::thread::Builder::new()
            .name("held-worker".to_string())
            .spawn(move || {
                // Parks until released — stands in for a server thread that
                // is still settling when the stops arrive.
                release_rx.recv().expect("the release signal must arrive");
                exited_worker.store(true, Ordering::Release);
            })
            .expect("spawn the held worker");
        server_thread_ids.lock().unwrap().push(worker.thread().id());
        threads.lock().unwrap().push(worker);

        let settler = |ids: Arc<Mutex<Vec<std::thread::ThreadId>>>,
                       threads: Arc<Mutex<Vec<JoinHandle<()>>>>,
                       exited: Arc<AtomicBool>| {
            std::thread::spawn(move || {
                settle_server_threads(&ids, &threads);
                assert!(
                    exited.load(Ordering::Acquire),
                    "settlement returned before the registered thread exited"
                );
            })
        };
        let first = settler(server_thread_ids.clone(), threads.clone(), exited.clone());

        // Deadline-bounded entry probe, not synchronization: wait until the
        // first settler demonstrably holds the registry lock while joining
        // the still-parked worker. Only then is the second settler spawned,
        // so it provably contends with a settlement in progress — and an
        // implementation that never held the lock across the join would
        // time this probe out.
        let deadline = std::time::Instant::now() + Duration::from_secs(5);
        while threads.try_lock().is_ok() {
            assert!(
                std::time::Instant::now() < deadline,
                "the first settler must be inside the settlement (holding the registry lock) within 5s"
            );
            sleep(Duration::from_millis(1));
        }
        let second = settler(server_thread_ids, threads, exited);

        // One-sided guard on the second settler: while the worker is still
        // parked, the first settler holds the registry lock inside its
        // join, so a correct second settlement CANNOT return — it is either
        // not yet scheduled or blocked on the lock. An early-returning
        // implementation finishes here and fails; a correct one can never
        // trip this, so the bounded window adds no flake risk (slow
        // scheduling only makes the check vacuous, never wrong).
        for _ in 0..50 {
            assert!(
                !second.is_finished(),
                "the second settlement returned while the worker was still parked"
            );
            sleep(Duration::from_millis(1));
        }

        release_tx.send(()).expect("release the held worker");
        first
            .join()
            .expect("the first settler must observe the exit");
        second
            .join()
            .expect("the second settler must observe the exit");
    }

    // A poisoned registry is NOT a settled registry: settlement must ride
    // over the poison and still join whatever handles remain, or a panic in
    // one caller would turn every later stop() into a false barrier.
    #[test]
    fn settlement_joins_handles_out_of_a_poisoned_registry() {
        let server_thread_ids: Arc<Mutex<Vec<std::thread::ThreadId>>> = Default::default();
        let threads: Arc<Mutex<Vec<JoinHandle<()>>>> = Default::default();
        let exited = Arc::new(AtomicBool::new(false));
        let (release_tx, release_rx) = std::sync::mpsc::channel::<()>();

        let exited_worker = exited.clone();
        let worker = std::thread::spawn(move || {
            release_rx.recv().expect("the release signal must arrive");
            exited_worker.store(true, Ordering::Release);
        });
        threads.lock().unwrap().push(worker);

        // Poison the registry the way a panicking holder would.
        let poisoner = threads.clone();
        let _ = std::thread::spawn(move || {
            let _guard = poisoner.lock().unwrap();
            panic!("poison the registry");
        })
        .join();
        assert!(threads.lock().is_err(), "the registry must be poisoned");

        release_tx.send(()).expect("release the worker");
        settle_server_threads(&server_thread_ids, &threads);
        assert!(
            exited.load(Ordering::Acquire),
            "settlement must still join the handle held by the poisoned registry"
        );
        assert!(
            threads
                .lock()
                .unwrap_or_else(PoisonError::into_inner)
                .is_empty(),
            "the poisoned registry must be drained by the settlement"
        );
    }

    // A registered thread that died to an uncontained panic is just as
    // terminated: settlement must report it and return normally instead of
    // rethrowing the payload out of stop().
    #[test]
    fn settlement_contains_a_panicked_thread_join() {
        let server_thread_ids: Arc<Mutex<Vec<std::thread::ThreadId>>> = Default::default();
        let threads: Arc<Mutex<Vec<JoinHandle<()>>>> = Default::default();
        threads.lock().unwrap().push(std::thread::spawn(|| {
            panic!("uncontained thread panic");
        }));

        settle_server_threads(&server_thread_ids, &threads);
        assert!(
            threads.lock().unwrap().is_empty(),
            "the panicked thread's handle must still be consumed"
        );
    }

    // A panic PAYLOAD is arbitrary user data whose destructor can itself
    // panic (panic_any with a drop bomb). Disposing of it mid-drain would
    // unwind with the registry guard live: the remaining handles would be
    // detached and the registry poisoned into a state that reads as
    // settled. Settlement must join EVERY handle — including those behind
    // the bomb — and return normally.
    #[test]
    fn settlement_contains_a_panicking_join_payload() {
        struct PayloadBomb;
        impl Drop for PayloadBomb {
            fn drop(&mut self) {
                panic!("panic payload drop bomb");
            }
        }

        let server_thread_ids: Arc<Mutex<Vec<std::thread::ThreadId>>> = Default::default();
        let threads: Arc<Mutex<Vec<JoinHandle<()>>>> = Default::default();
        let survivor_joined = Arc::new(AtomicBool::new(false));

        // The bomb thread is registered FIRST so its payload is handled
        // while another handle still sits behind it in the drain.
        threads.lock().unwrap().push(std::thread::spawn(|| {
            std::panic::panic_any(PayloadBomb);
        }));
        let survivor_flag = survivor_joined.clone();
        threads.lock().unwrap().push(std::thread::spawn(move || {
            survivor_flag.store(true, Ordering::Release);
        }));

        settle_server_threads(&server_thread_ids, &threads);
        assert!(
            survivor_joined.load(Ordering::Acquire),
            "the handle behind the bomb payload must still be joined"
        );
        assert!(
            threads.lock().unwrap().is_empty(),
            "settlement must consume every handle despite the bomb payload"
        );
    }

    // Settlement never runs a payload's drop glue at all: a payload whose
    // destructor re-panics with another bomb, and even one whose TWO field
    // destructors both panic (an abort inside its own drop glue that no
    // catch can regain control from), must be leaked untouched — the
    // barrier completes, every handle behind the bombs is joined, and
    // nothing unwinds or aborts.
    #[test]
    fn settlement_contains_a_chained_panic_payload() {
        struct ChainBomb(u32);
        impl Drop for ChainBomb {
            fn drop(&mut self) {
                if self.0 > 0 {
                    std::panic::panic_any(ChainBomb(self.0 - 1));
                }
            }
        }
        struct PlainBomb;
        impl Drop for PlainBomb {
            fn drop(&mut self) {
                panic!("plain payload bomb");
            }
        }
        // Two panicking field destructors in one box: dropping this ANYWHERE
        // is a guaranteed process abort (panic during panic-unwind).
        struct TwoBomb {
            _a: PlainBomb,
            _b: PlainBomb,
        }

        let server_thread_ids: Arc<Mutex<Vec<std::thread::ThreadId>>> = Default::default();
        let threads: Arc<Mutex<Vec<JoinHandle<()>>>> = Default::default();
        let survivor_joined = Arc::new(AtomicBool::new(false));

        // Chain bomb, plain bomb, the unabortable two-bomb, then a live
        // survivor: none of the payloads may be dropped, and the survivor
        // behind all of them must still be joined.
        threads.lock().unwrap().push(std::thread::spawn(|| {
            std::panic::panic_any(ChainBomb(2));
        }));
        threads.lock().unwrap().push(std::thread::spawn(|| {
            std::panic::panic_any(PlainBomb);
        }));
        threads.lock().unwrap().push(std::thread::spawn(|| {
            std::panic::panic_any(TwoBomb {
                _a: PlainBomb,
                _b: PlainBomb,
            });
        }));
        let survivor_flag = survivor_joined.clone();
        threads.lock().unwrap().push(std::thread::spawn(move || {
            survivor_flag.store(true, Ordering::Release);
        }));

        settle_server_threads(&server_thread_ids, &threads);
        assert!(
            survivor_joined.load(Ordering::Acquire),
            "the survivor behind the bombs must still be joined"
        );
        assert!(
            threads.lock().unwrap().is_empty(),
            "settlement must consume every handle despite the bomb payloads"
        );
    }

    // The terminal cause drives the publisher write callbacks' failure
    // classification, first-writer-wins: signal_stop (every user-initiated
    // path) claims CAUSE_DELIBERATE, the bare terminal funnel (worker
    // death, contained reactor panic) claims CAUSE_FATAL, and whichever
    // transition lands first keeps the label — in particular, a stop()
    // arriving AFTER a crash must not relabel the crash as deliberate.
    #[test]
    fn terminal_cause_is_first_writer_wins() {
        // Deliberate first: the cause is claimed as deliberate and a later
        // fatal funnel (the worker noticing the stop and winding down) must
        // not overwrite it.
        let server = EmbedRtmpServer::<Running> {
            address: String::new(),
            bound_addr: None,
            status: Arc::new(AtomicUsize::new(STATUS_RUN)),
            stream_keys: Default::default(),
            registrations: Some(Arc::new(RegistrationHandoff::new())),
            wake_handle: None,
            threads: Default::default(),
            server_thread_ids: Default::default(),
            terminal_cause: Default::default(),
            gop_limit: 1,
            max_connections: None,
            state: PhantomData,
        };
        assert_eq!(server.terminal_cause.load(Ordering::Acquire), CAUSE_NONE);
        server.signal_stop();
        assert!(server.is_stopped());
        assert_eq!(
            server.terminal_cause.load(Ordering::Acquire),
            CAUSE_DELIBERATE,
            "signal_stop must claim the deliberate cause"
        );
        let registrations = server.registrations.clone().expect("handoff installed");
        close_intake_and_publish_end(&registrations, &server.status, &server.terminal_cause);
        assert_eq!(
            server.terminal_cause.load(Ordering::Acquire),
            CAUSE_DELIBERATE,
            "a fatal funnel after a deliberate stop must not relabel it"
        );

        // Fatal first: the crash claims the cause, and a LATE stop() on a
        // surviving clone must not repaint it as deliberate — the racing
        // publisher's failing write keeps its loud classification.
        let crashed = EmbedRtmpServer::<Running> {
            address: String::new(),
            bound_addr: None,
            status: Arc::new(AtomicUsize::new(STATUS_RUN)),
            stream_keys: Default::default(),
            registrations: Some(Arc::new(RegistrationHandoff::new())),
            wake_handle: None,
            threads: Default::default(),
            server_thread_ids: Default::default(),
            terminal_cause: Default::default(),
            gop_limit: 1,
            max_connections: None,
            state: PhantomData,
        };
        let registrations = crashed.registrations.clone().expect("handoff installed");
        close_intake_and_publish_end(&registrations, &crashed.status, &crashed.terminal_cause);
        assert!(crashed.is_stopped());
        assert_eq!(
            crashed.terminal_cause.load(Ordering::Acquire),
            CAUSE_FATAL,
            "a fatal terminal transition must claim the fatal cause"
        );
        crashed.signal_stop();
        assert_eq!(
            crashed.terminal_cause.load(Ordering::Acquire),
            CAUSE_FATAL,
            "a stop() after the crash must not relabel the crash as deliberate"
        );
    }

    // signal_stop must close the registration intake in the same breath as
    // publishing the stopped status. Without that, a server clone could
    // observe is_stopped() == true, still enqueue a registration, and be
    // told Ok even though no reactor round would ever consume it — the kill
    // switch would eventually release the claim, but the Ok reported a
    // stream that could never exist. No socket is bound here: the create
    // path touches only the handoff, the key set and the status flag.
    #[test]
    fn stop_signal_closes_the_registration_intake() {
        let server = EmbedRtmpServer::<Running> {
            address: String::new(),
            bound_addr: None,
            status: Arc::new(AtomicUsize::new(STATUS_RUN)),
            stream_keys: Default::default(),
            registrations: Some(Arc::new(RegistrationHandoff::new())),
            wake_handle: None,
            threads: Default::default(),
            server_thread_ids: Default::default(),
            terminal_cause: Default::default(),
            gop_limit: 1,
            max_connections: None,
            state: PhantomData,
        };

        let _live = server
            .create_rtmp_input("app", "before")
            .expect("create while running must succeed");

        server.signal_stop();
        assert!(server.is_stopped());

        let after = server.create_rtmp_input("app", "after");
        assert!(
            matches!(after, Err(crate::error::Error::RtmpCreateStream)),
            "a create issued after the stop signal must fail with the stopped error"
        );
        assert!(
            !server.stream_keys.contains("after"),
            "the refused create must release its key claim immediately"
        );
    }

    // The claim-release chain end to end, through the public lifecycle: a
    // create claims the key and queues the registration, the running worker
    // consumes it and moves the claim into the accepted publisher's state,
    // and stop() must unwind that whole chain so the key is claimable again
    // afterwards. The server is assembled exactly as start() builds it —
    // same shared status, key set and handoff, and the same worker body
    // start() spawns (handle_connections) — minus the TCP listener and
    // accept thread, so no port is bound and every wait is a bounded poll
    // against a deadline.
    #[test]
    fn stopped_server_releases_accepted_stream_keys() {
        let registrations = Arc::new(RegistrationHandoff::new());
        let status = Arc::new(AtomicUsize::new(STATUS_RUN));
        let server = EmbedRtmpServer::<Running> {
            address: String::new(),
            bound_addr: None,
            status: status.clone(),
            stream_keys: Default::default(),
            registrations: Some(registrations.clone()),
            wake_handle: None,
            threads: Default::default(),
            server_thread_ids: Default::default(),
            terminal_cause: Default::default(),
            gop_limit: 1,
            max_connections: None,
            state: PhantomData,
        };

        // The accept thread's side of the connection channel, kept open and
        // idle for the worker's lifetime — a listener that never accepts.
        let (_connection_sender, connection_receiver) = crossbeam_channel::bounded::<TcpStream>(1);
        let worker = {
            let status = status.clone();
            std::thread::Builder::new()
                .name("rtmp-server-worker".to_string())
                .spawn(move || {
                    handle_connections(
                        connection_receiver,
                        registrations,
                        1,
                        None,
                        status,
                        Default::default(),
                        None,
                    )
                })
                .expect("spawn the worker thread")
        };

        // The create claims the key synchronously and queues the
        // registration, primed with the connect / createStream / publish
        // handshake, for the worker.
        let sender = server
            .create_stream_sender("app", "lifecycle-key")
            .expect("create on the running server must succeed");

        // Wait until the reactor drains that primed handshake (with no wake
        // handle it picks the registration up on a ~100ms poll timeout). An
        // empty channel means the worker consumed the registration, so the
        // claim now lives in the reactor's publisher state — the ownership
        // stop() must tear down.
        let deadline = std::time::Instant::now() + Duration::from_secs(5);
        while !sender.inner.is_empty() {
            assert!(
                std::time::Instant::now() < deadline,
                "the worker must drain the primed publish handshake within 5s"
            );
            sleep(Duration::from_millis(10));
        }

        // The accepted publisher holds the key: a duplicate create is
        // refused with the typed already-exists error.
        assert!(
            matches!(
                server.create_stream_sender("app", "lifecycle-key"),
                Err(crate::error::Error::RtmpStreamAlreadyExists(ref key)) if key == "lifecycle-key"
            ),
            "the key must stay held while its publisher is accepted and live"
        );

        // Stop through the public seam; a surviving clone observes the
        // aftermath, as any second owner of the server value would.
        let observer = server.clone();
        assert!(server.stop().is_stopped());

        // The worker notices the stop flag on its next poll cycle and
        // exits; joining it orders every teardown release before the
        // assertions below.
        let deadline = std::time::Instant::now() + Duration::from_secs(5);
        while !worker.is_finished() {
            assert!(
                std::time::Instant::now() < deadline,
                "the worker must exit within 5s of stop()"
            );
            sleep(Duration::from_millis(10));
        }
        worker.join().expect("the worker must exit cleanly");

        // The key is claimable again: the second create gets past the
        // duplicate-key gate, and what refuses it is the stopped server's
        // closed intake — not a lingering claim.
        assert!(
            matches!(
                observer.create_stream_sender("app", "lifecycle-key"),
                Err(crate::error::Error::RtmpCreateStream)
            ),
            "after stop the key must be free; only the closed intake may refuse the create"
        );
        assert!(
            !observer.stream_keys.contains("lifecycle-key"),
            "no claim may survive the worker's teardown"
        );
    }

    /// A minimal registration for probing whether an intake accepts or
    /// refuses enqueues. Its claim lives in a key set private to the probe.
    fn probe_registration(key: &str) -> PublisherRegistration {
        let (_feed_tx, feed_rx) = crossbeam_channel::bounded::<PublisherFeed>(1);
        let (budget, _) = IngressBudget::new(PUBLISHER_INGRESS_HIGH_WATER_BYTES);
        PublisherRegistration {
            claim: StreamKeyClaim::claim(Arc::new(dashmap::DashSet::new()), key.to_string())
                .expect("a fresh key set accepts its first claim"),
            source: PublisherSource::Feed(feed_rx),
            budget,
        }
    }

    // The byte gate keeps the sender's error identity: a send parked at the
    // high-water mark returns the same RtmpStreamClosed a dead channel
    // produces, once the reactor-side guard drops (stream teardown).
    #[test]
    fn stream_sender_send_maps_budget_close_to_stream_closed() {
        // `_rx` stays alive: the parked send below must fail through the
        // budget's close, not through a channel disconnect.
        let (tx, _rx) = crossbeam_channel::bounded::<Vec<u8>>(4);
        let (budget_guard, budget) = IngressBudget::new(8);
        let sender = RtmpStreamSender {
            inner: tx,
            wake_handle: None,
            budget,
        };
        sender
            .send(vec![0u8; 6])
            .expect("a send into an empty account passes");

        let blocked = sender.clone();
        let (done_tx, done_rx) = std::sync::mpsc::channel();
        let producer = std::thread::spawn(move || {
            done_tx
                .send(blocked.send(vec![0u8; 6]))
                .expect("report the send result");
        });
        // One-sided park check: a correct gate cannot complete this send
        // while 6 of 8 bytes are queued; slow scheduling only makes the
        // window vacuous, never wrong.
        assert!(
            done_rx.recv_timeout(Duration::from_millis(100)).is_err(),
            "a send past the byte high-water mark must park"
        );

        drop(budget_guard);
        assert_eq!(
            done_rx
                .recv_timeout(Duration::from_secs(10))
                .expect("the guard drop must wake the parked send"),
            Err(crate::error::Error::RtmpStreamClosed),
            "a send woken by stream teardown reports the stream closed"
        );
        producer.join().expect("producer thread exits");
    }

    // A reactor panic must not leave the server half-dead: the unwind
    // boundary closes the registration intake and publishes STATUS_END so
    // is_stopped() flips true, creates are refused, and the accept thread's
    // per-iteration status check terminates its loop.
    #[test]
    fn reactor_panic_publishes_terminal_status() {
        let registrations = RegistrationHandoff::new();
        let status = AtomicUsize::new(STATUS_RUN);
        let terminal_cause = AtomicUsize::new(CAUSE_NONE);
        contain_reactor_panic(&registrations, &status, &terminal_cause, || {
            panic!("injected reactor panic")
        });
        assert_eq!(status.load(Ordering::Acquire), STATUS_END);
        assert_eq!(
            terminal_cause.load(Ordering::Acquire),
            CAUSE_FATAL,
            "a contained panic is a fatal cause"
        );
        assert!(
            matches!(
                registrations.enqueue(probe_registration("panicked")),
                Err(EnqueueRefused::Closed(_))
            ),
            "the contained panic must close the intake, not just flip the status"
        );
    }

    // The clean path owns no transition at all: a normally-returning reactor
    // reaches STATUS_END only through signal_stop (or the worker's own fatal
    // paths), and the containment must force neither STATUS_END onto a live
    // status nor a close onto an open intake.
    #[test]
    fn reactor_clean_return_leaves_status_untouched() {
        let registrations = RegistrationHandoff::new();
        let status = AtomicUsize::new(STATUS_RUN);
        let terminal_cause = AtomicUsize::new(CAUSE_NONE);
        contain_reactor_panic(&registrations, &status, &terminal_cause, || {});
        assert_eq!(status.load(Ordering::Acquire), STATUS_RUN);
        assert_eq!(
            terminal_cause.load(Ordering::Acquire),
            CAUSE_NONE,
            "a clean return must not claim any terminal cause"
        );
        assert!(
            registrations
                .enqueue(probe_registration("still-open"))
                .is_ok(),
            "a clean reactor return must leave the intake open"
        );
    }

    // Fatal worker paths must uphold the same ordering as signal_stop: the
    // intake closes BEFORE STATUS_END becomes observable. The reactor checks
    // the stop flag before draining registrations, so a create accepted
    // after that flag is up would be an Ok for a stream that can never
    // exist — the kill switch discards the queued registration without any
    // reactor round consuming it. This drives the panic-containment path
    // against the real create path, wired to one handoff and status exactly
    // as start() builds them.
    #[test]
    fn reactor_panic_refuses_creates_on_surviving_handles() {
        let registrations = Arc::new(RegistrationHandoff::new());
        let status = Arc::new(AtomicUsize::new(STATUS_RUN));
        let server = EmbedRtmpServer::<Running> {
            address: String::new(),
            bound_addr: None,
            status: status.clone(),
            stream_keys: Default::default(),
            registrations: Some(registrations.clone()),
            wake_handle: None,
            threads: Default::default(),
            server_thread_ids: Default::default(),
            terminal_cause: Default::default(),
            gop_limit: 1,
            max_connections: None,
            state: PhantomData,
        };

        let _live = server
            .create_rtmp_input("app", "before-panic")
            .expect("create while running must succeed");

        contain_reactor_panic(&registrations, &status, &server.terminal_cause, || {
            panic!("injected reactor panic")
        });

        assert!(
            server.is_stopped(),
            "the contained panic must stop the server"
        );
        let refused = server.create_rtmp_input("app", "after-panic").err();
        assert!(
            matches!(refused, Some(crate::error::Error::RtmpCreateStream)),
            "a create observing the panic-stopped server must be refused, got {refused:?}"
        );
        assert!(
            !server.stream_keys.contains("after-panic"),
            "the refused create must release its key claim immediately"
        );
    }

    // StartFailGuard is the unwind backstop for start()'s window between
    // the lifecycle claim (the INIT->RUN CAS) and the accept-thread
    // handoff: a log call in that window can panic in a user-installed
    // logger, and the escaping unwind must still publish the terminal
    // state — otherwise every clone reports a started server no one can
    // ever stop. Dropped by that unwind, the guard must run the same
    // funnel as signal_stop: intake closed first, STATUS_END second.
    #[test]
    fn armed_start_fail_guard_publishes_terminal_state_on_unwind() {
        let registrations = Arc::new(RegistrationHandoff::new());
        let status = Arc::new(AtomicUsize::new(STATUS_RUN));
        let guard = StartFailGuard {
            armed: true,
            registrations: registrations.clone(),
            status: status.clone(),
            wake_handle: None,
            threads: Default::default(),
            server_thread_ids: Default::default(),
            terminal_cause: Default::default(),
        };

        let unwind = std::panic::catch_unwind(std::panic::AssertUnwindSafe(move || {
            let _guard = guard;
            panic!("injected panic between the claim and the handoff");
        }));
        assert!(unwind.is_err(), "the injected panic must unwind");

        assert_eq!(status.load(Ordering::Acquire), STATUS_END);
        assert!(
            matches!(
                registrations.enqueue(probe_registration("after-unwind")),
                Err(EnqueueRefused::Closed(_))
            ),
            "the guard must close the intake, not just flip the status"
        );
    }

    // The success handoff disarms the guard: its drop must then leave the
    // running lifecycle alone — no closed intake, no STATUS_END.
    #[test]
    fn disarmed_start_fail_guard_leaves_the_lifecycle_running() {
        let registrations = Arc::new(RegistrationHandoff::new());
        let status = Arc::new(AtomicUsize::new(STATUS_RUN));
        let mut guard = StartFailGuard {
            armed: true,
            registrations: registrations.clone(),
            status: status.clone(),
            wake_handle: None,
            threads: Default::default(),
            server_thread_ids: Default::default(),
            terminal_cause: Default::default(),
        };
        guard.armed = false;
        drop(guard);

        assert_eq!(status.load(Ordering::Acquire), STATUS_RUN);
        assert!(
            registrations
                .enqueue(probe_registration("still-open"))
                .is_ok(),
            "a disarmed guard must leave the intake open"
        );
    }

    /// A hand-assembled `Initialization` sibling of `family`: it shares the
    /// lifecycle (status flag) and key set exactly as clone() would, but
    /// aims at `address` instead of the address the family was created
    /// with, which clone() copies verbatim.
    fn family_sibling<S: 'static>(
        family: &EmbedRtmpServer<S>,
        address: impl Into<String>,
    ) -> EmbedRtmpServer<Initialization> {
        EmbedRtmpServer::<Initialization> {
            address: address.into(),
            bound_addr: None,
            status: family.status.clone(),
            stream_keys: family.stream_keys.clone(),
            registrations: None,
            wake_handle: None,
            threads: Default::default(),
            server_thread_ids: Default::default(),
            terminal_cause: Default::default(),
            gop_limit: 1,
            max_connections: None,
            state: PhantomData,
        }
    }

    // Initialization clones share one status flag and key set — one
    // lifecycle. Each start() would otherwise mint its own registration
    // intake around that shared status: stopping one started clone closed
    // only its own intake while publishing the shared STATUS_END, so the
    // other kept accepting creates on handles that reported stopped. The
    // single-start gate makes that state unreachable: exactly one start()
    // per family may ever succeed.
    #[test]
    fn second_start_of_a_cloned_family_is_refused() {
        let first = EmbedRtmpServer::new("127.0.0.1:0");
        let second = first.clone();
        let third = first.clone();

        let running = first.start().expect("the family's first start must win");
        let addr = running.local_addr().expect("bound address");

        // While the winner runs: a sibling clone must not start the family
        // again. Its bind would even succeed (another ephemeral port); the
        // shared lifecycle refuses it before that bind is ever attempted.
        let refused = second.start().err();
        assert!(
            matches!(refused, Some(crate::error::Error::RtmpServerAlreadyStarted)),
            "a second start on a running family must be refused, got {refused:?}"
        );
        assert!(
            !running.is_stopped(),
            "a refused start must not perturb the running server"
        );

        // The fixed-port shape of the same refusal: a sibling aimed at THE
        // port the winner holds. The admission check must refuse it before
        // any bind attempt — binding first would surface Error::IO
        // (AddrInUse) from the winner's port instead of the typed
        // lifecycle error.
        let refused = family_sibling(&running, addr.to_string()).start().err();
        assert!(
            matches!(refused, Some(crate::error::Error::RtmpServerAlreadyStarted)),
            "a second start on the winner's own port must get the typed refusal, got {refused:?}"
        );
        assert!(
            !running.is_stopped(),
            "the fixed-port refusal must not perturb the running server"
        );

        // After stop, STATUS_END is terminal for the family: restarting
        // would flip stopped handles back to not-stopped around an intake
        // that is closed for good.
        let ended = running.stop();
        assert!(ended.is_stopped());
        let refused = third.start().err();
        assert!(
            matches!(refused, Some(crate::error::Error::RtmpServerAlreadyStarted)),
            "a start after the family stopped must be refused, got {refused:?}"
        );

        // The fixed-port shape right after stop(): the refusal must be
        // decided by the lifecycle alone — never by a bind whose outcome
        // depends on how quickly the stopped accept thread releases the
        // listener (typed refusal one moment, AddrInUse the next).
        let refused = family_sibling(&ended, addr.to_string()).start().err();
        assert!(
            matches!(refused, Some(crate::error::Error::RtmpServerAlreadyStarted)),
            "a fixed-port start right after stop must get the typed refusal, got {refused:?}"
        );
    }

    // Bind failures are pre-claim: they must surface as the typed IO error
    // and leave the family's lifecycle at INIT, retryable through a clone —
    // the admission refusal is reserved for families that actually started.
    #[test]
    fn failed_bind_leaves_the_family_retryable() {
        // Hold a port so the family's first bind deterministically fails.
        let blocker = std::net::TcpListener::bind("127.0.0.1:0").expect("reserve a port");
        let blocked_addr = blocker.local_addr().expect("blocker address");

        let first = EmbedRtmpServer::new(blocked_addr.to_string());
        let survivor = first.clone();
        let failed = first.start().err();
        assert!(
            matches!(failed, Some(crate::error::Error::IO(_))),
            "binding a held port must surface the typed IO error, got {failed:?}"
        );

        // The failed bind claimed nothing: a sibling of the same family can
        // still win its one start, on a bindable address.
        let running = family_sibling(&survivor, "127.0.0.1:0")
            .start()
            .expect("a failed bind must leave the family startable");
        assert!(running.stop().is_stopped());
    }

    #[test]
    #[ignore] // Integration test: requires exclusive port 1935 and test.mp4
    fn test_concat_stream_loop() {
        let _ = env_logger::builder()
            .filter_level(log::LevelFilter::Trace)
            .is_test(true)
            .try_init();

        let embed_rtmp_server = EmbedRtmpServer::new("localhost:1935");
        let embed_rtmp_server = embed_rtmp_server.start().unwrap();

        let output = embed_rtmp_server
            .create_rtmp_input("my-app", "my-stream")
            .unwrap();

        let start = current();

        let result = FfmpegContext::builder()
            .input(Input::from("test.mp4").set_readrate(1.0).set_stream_loop(3))
            .input(Input::from("test.mp4").set_readrate(1.0).set_stream_loop(3))
            .input(Input::from("test.mp4").set_readrate(1.0).set_stream_loop(3))
            .filter_desc("[0:v][0:a][1:v][1:a][2:v][2:a]concat=n=3:v=1:a=1")
            .output(output)
            .build()
            .unwrap()
            .start()
            .unwrap()
            .wait();

        assert!(result.is_ok());
        info!("elapsed time: {}", current() - start);
    }

    #[test]
    #[ignore] // Integration test: requires exclusive port 1935 and test.mp4
    fn test_stream_loop() {
        let _ = env_logger::builder()
            .filter_level(log::LevelFilter::Trace)
            .is_test(true)
            .try_init();

        let embed_rtmp_server = EmbedRtmpServer::new("localhost:1935");
        let embed_rtmp_server = embed_rtmp_server.start().unwrap();

        let output = embed_rtmp_server
            .create_rtmp_input("my-app", "my-stream")
            .unwrap();

        let start = current();

        let result = FfmpegContext::builder()
            .input(
                Input::from("test.mp4")
                    .set_readrate(1.0)
                    .set_stream_loop(-1),
            )
            // .filter_desc("hue=s=0")
            .output(output.set_video_codec("h264_videotoolbox"))
            .build()
            .unwrap()
            .start()
            .unwrap()
            .wait();

        assert!(result.is_ok());

        info!("elapsed time: {}", current() - start);
    }

    #[test]
    #[ignore] // Integration test: requires exclusive port 1935 and test.mp4
    fn test_concat_realtime() {
        let _ = env_logger::builder()
            .filter_level(log::LevelFilter::Trace)
            .is_test(true)
            .try_init();

        let embed_rtmp_server = EmbedRtmpServer::new("localhost:1935");
        let embed_rtmp_server = embed_rtmp_server.start().unwrap();

        let output = embed_rtmp_server
            .create_rtmp_input("my-app", "my-stream")
            .unwrap();

        let start = current();

        let result = FfmpegContext::builder()
            .independent_readrate()
            .input(Input::from("test.mp4").set_readrate(1.0))
            .input(Input::from("test.mp4").set_readrate(1.0))
            .input(Input::from("test.mp4").set_readrate(1.0))
            .filter_desc("[0:v][0:a][1:v][1:a][2:v][2:a]concat=n=3:v=1:a=1")
            .output(output)
            .build()
            .unwrap()
            .start()
            .unwrap()
            .wait();

        assert!(result.is_ok());

        sleep(Duration::from_secs(1));
        info!("elapsed time: {}", current() - start);
    }

    #[test]
    #[ignore] // Integration test: requires exclusive port 1935 and test.mp4
    fn test_realtime() {
        let _ = env_logger::builder()
            .filter_level(log::LevelFilter::Trace)
            .is_test(true)
            .try_init();

        let embed_rtmp_server = EmbedRtmpServer::new("localhost:1935");
        let embed_rtmp_server = embed_rtmp_server.start().unwrap();

        let output = embed_rtmp_server
            .create_rtmp_input("my-app", "my-stream")
            .unwrap();

        let start = current();

        let result = FfmpegContext::builder()
            .input(Input::from("test.mp4").set_readrate(1.0))
            .output(output)
            .build()
            .unwrap()
            .start()
            .unwrap()
            .wait();

        assert!(result.is_ok());

        info!("elapsed time: {}", current() - start);
    }

    #[test]
    #[ignore] // Integration test: requires test.mp4
    fn test_readrate() {
        let _ = env_logger::builder()
            .filter_level(log::LevelFilter::Trace)
            .is_test(true)
            .try_init();

        let mut output: Output = "output.flv".into();
        output.audio_codec = Some("adpcm_swf".to_string());

        let mut input: Input = "test.mp4".into();
        input.readrate = Some(1.0);

        let context = FfmpegContext::builder()
            .input(input)
            .output(output)
            .build()
            .unwrap();

        let result = FfmpegScheduler::new(context).start().unwrap().wait();
        if let Err(error) = result {
            println!("Error: {error}");
        }
    }

    #[test]
    #[ignore] // Integration test: requires exclusive port 1935 and test.mp4
    fn test_embed_rtmp_server() {
        let _ = env_logger::builder()
            .filter_level(log::LevelFilter::Trace)
            .is_test(true)
            .try_init();

        let embed_rtmp_server = EmbedRtmpServer::new("localhost:1935");
        let embed_rtmp_server = embed_rtmp_server.start().unwrap();

        let output = embed_rtmp_server
            .create_rtmp_input("my-app", "my-stream")
            .unwrap();
        let mut input: Input = "test.mp4".into();
        input.readrate = Some(1.0);

        let context = FfmpegContext::builder()
            .input(input)
            .output(output)
            .build()
            .unwrap();

        let result = FfmpegScheduler::new(context).start().unwrap().wait();

        assert!(result.is_ok());

        sleep(Duration::from_secs(3));
    }
}
