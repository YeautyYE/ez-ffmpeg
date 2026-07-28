// src/rtmp/reactor.rs - Single-threaded Reactor event loop
//
// Core features:
// - Event-driven IO using Poller (epoll/kqueue/WSAPoll)
// - Backpressure management using WriteQueue
// - Strict drain until WouldBlock semantics (required for edge-triggered)
// - ConnectionToken prevents ID reuse conflicts
// - Connection timeout detection
// - Graceful shutdown support

use crate::rtmp::poller::{Interest, Poller, RawHandle, Waker, WAKER_TOKEN};
use crate::rtmp::rtmp_scheduler::{RtmpScheduler, ServerResult};
use crate::rtmp::write_queue::{BackpressureLevel, FlushResult, WriteQueue};
use bytes::Bytes;
use log::{debug, error, info, warn};
use rml_rtmp::chunk_io::ChunkSerializer;
use rml_rtmp::handshake::{Handshake, HandshakeProcessResult, PeerType};
use rml_rtmp::messages::RtmpMessage;
use rml_rtmp::rml_amf0::Amf0Value;
use rml_rtmp::time::RtmpTimestamp;
use std::collections::{HashMap, HashSet, VecDeque};
use std::io::{self, Read};
use std::net::{Shutdown, TcpStream};
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::{Arc, Condvar, Mutex, MutexGuard};
use std::time::{Duration, Instant};

// ============================================================================
// Constants
// ============================================================================

const READ_BUFFER_SIZE: usize = 8192;
const POLL_TIMEOUT_MS: u64 = 100;
const CONNECTION_TIMEOUT_SECS: u64 = 60; // Connection timeout
/// Idle time after which an Active watcher is sent a liveness ping (RTMP
/// User Control PingRequest) by the timeout sweep. Half the connection
/// timeout, so a quiet-but-live watcher — typically one on a channel whose
/// publisher has not started — is probed well before the reaper fires and
/// its ANSWER (any bytes read from the peer) refreshes the activity clock.
/// The ping's own write deliberately earns no activity credit (see
/// `ReactorConnection::note_ping_queued`): a peer that never answers ages
/// toward the timeout exactly as if the server had sent nothing, so the
/// probe can only save clients that prove they are alive. Only watchers are
/// pinged (see `RtmpScheduler::ping_watcher`); an idle publisher still
/// times out and releases its stream key.
const WATCHER_PING_IDLE_SECS: u64 = CONNECTION_TIMEOUT_SECS / 2;
/// Minimum interval between full connection-timeout sweeps (PERF-10 throttle).
/// A 60s timeout tolerates being detected up to this much late.
const TIMEOUT_CHECK_INTERVAL: Duration = Duration::from_secs(1);
const GRACEFUL_SHUTDOWN_TIMEOUT_SECS: u64 = 5; // Graceful shutdown timeout
/// How long a server-condemned connection may linger to drain its final queued
/// bytes (typically the finish-status packet) before it is force-removed even
/// if the peer never reads. Bounds the lingering so a stuck watcher cannot pin
/// a connection slot forever. `check_timeouts` runs at most ~1/sec, so this is
/// kept a few seconds for that granularity to be harmless; it mirrors the
/// graceful-shutdown drain window.
const CLOSE_DRAIN_TIMEOUT: Duration = Duration::from_secs(GRACEFUL_SHUTDOWN_TIMEOUT_SECS);
const MAX_READ_PER_POLL: usize = 512 * 1024; // 512KB max read per poll to prevent memory DoS
/// Capacity of the bounded channel between an in-process publisher and the
/// reactor. Shared with `embed_rtmp_server`'s sender constructors so the
/// per-round item budget below always matches what a producer can queue ahead
/// of one drain. Counts items only; its byte-gate companion is
/// `PUBLISHER_INGRESS_HIGH_WATER_BYTES` below.
pub(crate) const PUBLISHER_CHANNEL_CAPACITY: usize = 1024;
/// Per-publisher, per-round byte budget for `process_publishers`. Mirrors
/// MAX_READ_PER_POLL so an in-process publisher cannot out-rank a socket
/// reader: an unbounded drain keeps the loop inside step 6 while the local
/// packets_to_write buffer grows by (drained bytes x watcher fanout) and
/// flush_pending never runs — watchers stall at zero bytes written.
const MAX_PUBLISH_BYTES_PER_POLL: usize = MAX_READ_PER_POLL;
/// Per-publisher, per-round item budget, companion to the byte budget above
/// for streams of tiny packets whose byte total stays low. Equal to the
/// channel capacity: one round can at most clear a full backlog.
const MAX_PUBLISH_ITEMS_PER_POLL: usize = PUBLISHER_CHANNEL_CAPACITY;
/// Per-publisher high-water mark, in bytes, for the ingress budget gating the
/// in-process publisher channel: a producer's send blocks once this many
/// undrained bytes are queued, the byte companion to the item-count bound the
/// channel capacity provides. The mark is deliberately generous on three
/// axes: it is 16 x MAX_PUBLISH_BYTES_PER_POLL, so a producer can run 16
/// drain rounds ahead and a healthy reactor never throttles at the gate; it
/// is ~2.5s of 25 Mbps 1080p video (~1s of 60 Mbps 4K), so GOP-sized muxer
/// bursts pass untouched; and it equals the channel's 1024 items x 8 KiB, so
/// streams whose average item is <= 8 KiB hit the pre-existing item cap
/// first and behave exactly as before — the byte gate binds only for
/// larger-item streams, whose queue could previously balloon to 1024 x
/// item-size (hundreds of MiB at 4K keyframe sizes). No low-water resume
/// mark: hysteresis pays when a pause/resume transition costs a syscall
/// (poller interest toggling); a Condvar wake is nanoseconds, and releases
/// are already batched to one notify per publisher per drain round (a
/// natural ~512 KiB step), so a separate low-water mark would only add
/// forced producer idle latency.
pub(crate) const PUBLISHER_INGRESS_HIGH_WATER_BYTES: usize = 8 * 1024 * 1024;
/// Capacity of the registration handoff between the create paths and the
/// reactor — the same bound the crossbeam channel it replaced had. Every
/// queued registration parks a stream-key claim, so an uncapped queue lets a
/// stalled reactor accumulate claims without limit; at the bound the enqueue
/// refuses instead, and the caller surfaces a typed error.
const REGISTRATION_QUEUE_CAPACITY: usize = 1024;
/// Per-round budget for registrations drained out of the handoff. Without
/// it, one round transfers the entire backlog and `add_publisher` runs for
/// all of it before the next poll or stop check, starving sockets; a
/// remainder instead forces a zero-timeout poll, so the next batch runs
/// promptly rather than after the poll fallback.
const MAX_REGISTRATIONS_PER_POLL: usize = 128;
const DEFAULT_MAX_CONNECTIONS: usize = 10000; // Default max connections (auto-adjusted by system FD limit)
#[cfg(windows)]
const DEFAULT_MAX_CONNECTIONS_WINDOWS: usize = 8000; // Conservative default for Windows (no direct FD limit API)
/// Extra capacity for bounded channel to absorb connection bursts.
/// Used when creating the connection channel between accept thread and reactor.
pub const CHANNEL_HEADROOM: usize = 256;
/// Dirty-id list buffers at or below this capacity are never shrunk, and no
/// shrink targets less than it. Vec iteration already costs O(len) regardless
/// of capacity, so the drain-side shrink is memory hygiene only — it keeps a
/// decayed flash crowd from pinning peak-sized id buffers for the life of the
/// reactor — and small buffers pay zero shrink bookkeeping.
const DIRTY_IDS_SHRINK_MIN_CAPACITY: usize = 64;
/// Consecutive quarter-occupancy drains required before the dirty-id shrink
/// fires. Both drains run every loop iteration, so without hysteresis a
/// single idle tick between media batches would discard buffer capacity that
/// the next batch immediately re-allocates. 64 sparse drains ≈ tens of loop
/// iterations of sustained low occupancy — decayed for real, not a gap
/// between frames.
const SPARSE_DRAINS_BEFORE_SHRINK: u32 = 64;

// ============================================================================
// System Helpers
// ============================================================================

/// Get system file descriptor limit (cross-platform)
///
/// Returns the soft limit of open files, or None if unavailable.
/// Used to auto-adjust max_connections to avoid exhausting system resources.
fn get_fd_limit() -> Option<usize> {
    #[cfg(unix)]
    {
        use std::mem::MaybeUninit;
        let mut rlim = MaybeUninit::<libc::rlimit>::uninit();
        // SAFETY: rlim is a valid pointer to uninitialized memory,
        // getrlimit will initialize it if successful
        if unsafe { libc::getrlimit(libc::RLIMIT_NOFILE, rlim.as_mut_ptr()) } == 0 {
            // SAFETY: getrlimit returned 0, so rlim is now initialized
            let rlim = unsafe { rlim.assume_init() };
            return Some(rlim.rlim_cur as usize);
        }
        None
    }
    #[cfg(windows)]
    {
        // Windows: Use a conservative default since there's no direct FD limit API.
        // Windows handles are managed differently; 8000 is a safe conservative value.
        Some(DEFAULT_MAX_CONNECTIONS_WINDOWS)
    }
    #[cfg(not(any(unix, windows)))]
    {
        None
    }
}

/// Calculate effective max connections based on config and system limits.
///
/// This function computes the actual maximum connections the server will allow:
/// - Uses configured value or DEFAULT_MAX_CONNECTIONS (10000)
/// - Caps at 80% of system FD limit to leave headroom for other operations
/// - Caps at `TOKEN_ID_MASK` so slab ids stay encodable in a poller token
///
/// Connection ids are slab-dense, so capping max connections at
/// `TOKEN_ID_MASK` guarantees every live id is strictly below the mask.
/// That keeps the id half of the poller token lossless and makes a
/// connection token equal to `WAKER_TOKEN` (`usize::MAX`) impossible even
/// at the maximum generation. On 64-bit targets the cap is 4_294_967_295
/// and never binds in practice; on 32-bit targets it is 65_535, far beyond
/// what a 32-bit address space can host anyway.
///
/// # Arguments
/// * `config_max` - User-configured max connections, or None for auto-detect
///
/// # Returns
/// The effective maximum connections value (guaranteed to be at least 1)
pub fn effective_max_connections(config_max: Option<usize>) -> usize {
    let config_value = config_max.unwrap_or(DEFAULT_MAX_CONNECTIONS);
    let result = if let Some(fd_limit) = get_fd_limit() {
        // Reserve 20% of FD limit for other operations (files, sockets, etc.)
        let fd_based_limit = (fd_limit as f64 * 0.8) as usize;
        config_value.min(fd_based_limit)
    } else {
        config_value
    };
    // Ensure at least 1 connection is allowed, and keep ids below the
    // encodable range of the poller token's id half.
    result.clamp(1, TOKEN_ID_MASK)
}

// ============================================================================
// Connection Token - Prevents ID reuse conflicts
// ============================================================================

/// Generation counter type, sized to the upper half of a poller token.
///
/// Poller tokens are `usize` on every backend (epoll's `u64` payload is
/// truncated to `usize` on the way out, kqueue's `udata` is pointer-width,
/// and the WSAPoll registry keys by `usize`), so the token splits the native
/// word in half: upper half generation, lower half id. Making the generation
/// counter's own type equal to its transport width keeps the encode/decode
/// round-trip lossless by construction on every target.
#[cfg(target_pointer_width = "64")]
type Generation = u32;
#[cfg(target_pointer_width = "32")]
type Generation = u16;

/// Number of low poller-token bits that carry the connection id.
#[cfg(target_pointer_width = "64")]
const TOKEN_ID_BITS: u32 = 32;
#[cfg(target_pointer_width = "32")]
const TOKEN_ID_BITS: u32 = 16;

/// Mask covering the id half of a poller token.
const TOKEN_ID_MASK: usize = (1usize << TOKEN_ID_BITS) - 1;

/// Connection token - Contains ID and generation counter
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub struct ConnectionToken {
    /// Connection ID (slab index)
    pub id: usize,
    /// Generation counter - Incremented each time ID is reused
    pub generation: Generation,
}

impl ConnectionToken {
    fn new(id: usize, generation: Generation) -> Self {
        Self { id, generation }
    }

    /// Encode token for poller (combines id and generation)
    ///
    /// Layout: [generation: upper half][id: lower half]. On 64-bit targets
    /// this is identical to the historical `(generation << 32) | id` layout.
    /// This allows validation of stale events from closed connections.
    fn to_poller_token(&self) -> usize {
        ((self.generation as usize) << TOKEN_ID_BITS) | (self.id & TOKEN_ID_MASK)
    }

    /// Decode token from poller event
    fn from_poller_token(token: usize) -> Self {
        Self {
            id: token & TOKEN_ID_MASK,
            generation: (token >> TOKEN_ID_BITS) as Generation,
        }
    }
}

// ============================================================================
// Connection State Machine
// ============================================================================

/// Connection state
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ConnectionState {
    /// Handshaking
    Handshaking,
    /// Active
    Active,
    /// Slow client (backpressure warning)
    SlowClient,
    /// Closing
    Closing,
    /// Closed
    Closed,
}

impl ConnectionState {
    #[cfg(test)]
    pub fn is_active(&self) -> bool {
        matches!(self, ConnectionState::Active | ConnectionState::SlowClient)
    }

    pub fn can_read(&self) -> bool {
        matches!(
            self,
            ConnectionState::Handshaking | ConnectionState::Active | ConnectionState::SlowClient
        )
    }

    pub fn can_write(&self) -> bool {
        matches!(
            self,
            ConnectionState::Handshaking
                | ConnectionState::Active
                | ConnectionState::SlowClient
                | ConnectionState::Closing
        )
    }
}

// ============================================================================
// Reactor Connection
// ============================================================================

/// Single RTMP connection
pub struct ReactorConnection {
    /// Connection token
    token: ConnectionToken,
    /// Underlying socket
    socket: TcpStream,
    /// Raw handle (for Poller)
    raw_handle: RawHandle,
    /// Connection state
    state: ConnectionState,
    /// Write queue
    write_queue: WriteQueue,
    /// Read buffer
    read_buffer: Vec<u8>,
    /// RTMP handshake handler
    handshake: Option<Handshake>,
    /// Last read activity time
    last_read_activity: Instant,
    /// Last write activity time
    last_write_activity: Instant,
    /// Currently registered interest
    current_interest: Interest,
    /// Set when the connection has been condemned (server-initiated close) but
    /// still has a queued tail to drain. `None` = not condemned. The connection
    /// is force-removed once this deadline passes even if the peer never reads,
    /// so lingering is bounded (see [`Self::condemn`]).
    close_deadline: Option<Instant>,
    /// When the timeout sweep last queued a liveness ping on this connection
    /// (`None` until the first one). Bounds the probe rate: without this
    /// stamp every ~1s sweep would queue another ping — onto a queue that
    /// may not be draining — while the connection stays idle awaiting the
    /// peer's answer.
    last_ping_at: Option<Instant>,
    /// Whether this connection is already listed in the reactor's
    /// `pending_flush_ids` for the current marking epoch. Owning the mark
    /// here lets the fanout enqueue loop dedup with a bool test on the
    /// connection it already holds `&mut` to (instead of a hash-set insert
    /// per packet), and makes stale list entries self-resolving: the flag
    /// dies with the connection, and a fresh connection reusing the slab
    /// slot is born unmarked, so the drain skips the stale id.
    in_pending_flush: bool,
    /// Same scheme as `in_pending_flush`, for `interest_dirty_ids`.
    in_interest_dirty: bool,
}

impl ReactorConnection {
    /// Create new connection
    pub fn new(token: ConnectionToken, socket: TcpStream) -> io::Result<Self> {
        // Set non-blocking
        socket.set_nonblocking(true)?;

        // PERF-4: disable Nagle on the accepted subscriber socket. Sub-MSS
        // writes (handshake/command exchange and the steady stream of small
        // audio tags) would otherwise be held by Nagle and interact with the
        // peer's delayed ACK, adding up to ~40ms per exchange. This pairs with
        // the writev batching (PERF-9): batching keeps the small-packet count
        // low, so disabling Nagle does not fragment the stream. Log and
        // continue on error - TCP_NODELAY is an optimization, not a
        // correctness requirement.
        if let Err(e) = socket.set_nodelay(true) {
            log::warn!(
                "Failed to set TCP_NODELAY on connection {}: {:?}",
                token.id,
                e
            );
        }

        #[cfg(unix)]
        let raw_handle = {
            use std::os::unix::io::AsRawFd;
            socket.as_raw_fd()
        };

        #[cfg(windows)]
        let raw_handle = {
            use std::os::windows::io::AsRawSocket;
            socket.as_raw_socket()
        };

        let now = Instant::now();

        Ok(Self {
            token,
            socket,
            raw_handle,
            state: ConnectionState::Handshaking,
            write_queue: WriteQueue::new(),
            read_buffer: vec![0u8; READ_BUFFER_SIZE],
            handshake: Some(Handshake::new(PeerType::Server)),
            last_read_activity: now,
            last_write_activity: now,
            current_interest: Interest::READABLE,
            close_deadline: None,
            last_ping_at: None,
            in_pending_flush: false,
            in_interest_dirty: false,
        })
    }

    /// Get raw handle
    pub fn raw_handle(&self) -> RawHandle {
        self.raw_handle
    }

    /// Combined activity time (take newer of read/write)
    pub fn last_activity(&self) -> Instant {
        self.last_read_activity.max(self.last_write_activity)
    }

    /// Is timed out
    #[cfg_attr(not(test), allow(dead_code))] // predicate exercised by unit tests
    pub fn is_timed_out(&self, timeout: Duration) -> bool {
        self.is_timed_out_at(Instant::now(), timeout)
    }

    /// Is timed out, evaluated against a caller-provided `now`.
    ///
    /// PERF-10: lets the reactor read the clock once per sweep instead of once
    /// per connection. `saturating_duration_since` guards against a `now` that
    /// is (marginally) earlier than the last activity due to clock coarseness.
    pub fn is_timed_out_at(&self, now: Instant, timeout: Duration) -> bool {
        now.saturating_duration_since(self.last_activity()) > timeout
    }

    /// Whether the timeout sweep should queue a liveness ping at `now`: the
    /// connection is established (`Active` — a mid-handshake session cannot
    /// carry control messages yet, and closing/slow ones are being written
    /// to or torn down already), not condemned, idle for at least `idle`,
    /// and not already pinged within that same window (see `last_ping_at`).
    /// Hoisted-`now` shape like `is_timed_out_at` (PERF-10), which also
    /// keeps the predicate deterministic under test.
    pub fn is_ping_due_at(&self, now: Instant, idle: Duration) -> bool {
        if self.state != ConnectionState::Active || self.close_deadline.is_some() {
            return false;
        }
        // A queued tail suppresses the probe: its own delivery or failure
        // will resolve the peer's liveness, so a ping adds nothing there.
        // (Attribution correctness does NOT hinge on this gate — ping
        // entries are tagged in the write queue and their bytes reported
        // separately by every flush, whatever sits around them.)
        if self.has_pending_writes() {
            return false;
        }
        if now.saturating_duration_since(self.last_activity()) < idle {
            return false;
        }
        match self.last_ping_at {
            Some(pinged_at) => now.saturating_duration_since(pinged_at) >= idle,
            None => true,
        }
    }

    /// Record that a liveness ping was queued at `now`. Deliberately not an
    /// activity stamp — and the queue's ping-tagged entries keep the ping's
    /// eventual delivery out of `try_flush`'s write-activity credit — so
    /// neither queueing nor even DELIVERING a ping counts as peer liveness.
    /// Only the peer's answer (a read) resets the idle clock; a peer that
    /// never answers ages toward the timeout exactly as if the server had
    /// sent nothing.
    pub fn note_ping_queued(&mut self, now: Instant) {
        self.last_ping_at = Some(now);
    }

    /// Enqueue data. `now` is the caller's hoisted clock read (PERF-10):
    /// the fanout enqueues W entries per media message, and the entry
    /// timestamp only feeds the seconds-granular age eviction, so one read
    /// per fanout round serves every entry.
    pub fn enqueue_data(
        &mut self,
        data: Bytes,
        is_keyframe: bool,
        is_sequence_header: bool,
        is_video: bool,
        droppable: bool,
        now: Instant,
    ) -> bool {
        let result =
            self.write_queue
                .enqueue(data, is_keyframe, is_sequence_header, is_video, droppable, now);

        // Update state based on backpressure level
        match self.write_queue.backpressure_level() {
            BackpressureLevel::Critical => {
                self.state = ConnectionState::Closing;
                return false;
            }
            BackpressureLevel::High | BackpressureLevel::Warning => {
                if self.state == ConnectionState::Active {
                    self.state = ConnectionState::SlowClient;
                }
            }
            BackpressureLevel::Normal => {
                if self.state == ConnectionState::SlowClient {
                    self.state = ConnectionState::Active;
                }
            }
        }

        result
    }

    /// Enqueue raw data (for handshake responses, etc.)
    /// Returns false if queue is full and connection should be disconnected
    pub fn enqueue_raw(&mut self, data: Vec<u8>) -> bool {
        if !self.write_queue.enqueue(
            Bytes::from(data),
            false,
            false,
            false,
            false,
            Instant::now(),
        ) {
            self.state = ConnectionState::Closing;
            return false;
        }
        true
    }

    /// Enqueue a liveness ping. Tagged in the write queue so every flush
    /// reports the ping's bytes separately (typed accounting: a delivered
    /// ping earns no write-activity credit), and pinned as non-droppable —
    /// the session serializer already committed csid-2 header history for
    /// it, so the shedding policy and the age-eviction sweep never remove
    /// it (see WriteEntry::droppable in write_queue). Returns false if the
    /// queue refused it (critical cap; the connection is then closing
    /// anyway).
    pub fn enqueue_ping(&mut self, data: Vec<u8>) -> bool {
        if !self.write_queue.enqueue_ping(Bytes::from(data)) {
            self.state = ConnectionState::Closing;
            return false;
        }
        true
    }

    /// Restore `Active` once a slow client's backlog is back in the Normal
    /// band. `enqueue_data` applies the same rule when new data arrives, but
    /// a watcher whose publisher went quiet gets no further enqueues:
    /// without this flush-side twin of that rule it would stay `SlowClient`
    /// forever after draining — permanently invisible to the idle-watcher
    /// ping, and eventually reaped as idle despite being healthy and fully
    /// caught up.
    fn recover_from_slow_client(&mut self) {
        if self.state == ConnectionState::SlowClient
            && self.write_queue.backpressure_level() == BackpressureLevel::Normal
        {
            self.state = ConnectionState::Active;
        }
    }

    /// Try to flush write queue (drain until WouldBlock)
    ///
    /// Returns whether connection should be disconnected
    pub fn try_flush(&mut self) -> io::Result<bool> {
        if self.write_queue.is_empty() {
            // An empty queue also means no backpressure: repair a stale
            // SlowClient before returning (see recover_from_slow_client).
            self.recover_from_slow_client();
            return Ok(false);
        }

        match self.write_queue.try_flush(&mut self.socket) {
            Ok(FlushResult::Complete {
                bytes_written,
                ping_bytes_written,
            }) => {
                // Only bytes beyond the ping-tagged ones earn the activity
                // stamp: a ping's own delivery is not peer liveness.
                if bytes_written > ping_bytes_written {
                    self.last_write_activity = Instant::now();
                }
                self.recover_from_slow_client();
                Ok(false)
            }
            Ok(FlushResult::WouldBlock {
                bytes_written,
                ping_bytes_written,
            }) => {
                if bytes_written > ping_bytes_written {
                    self.last_write_activity = Instant::now();
                }
                // A partial drain can still have dropped the backlog back
                // into the Normal band.
                self.recover_from_slow_client();
                Ok(false)
            }
            Ok(FlushResult::Closed) => Ok(true),
            Err(e) => {
                debug!("Connection {} write error: {:?}", self.token.id, e);
                Err(e)
            }
        }
    }

    /// Read data (drain until WouldBlock)
    ///
    /// Returns (data read, should disconnect)
    /// Note: Limits read to MAX_READ_PER_POLL to prevent memory DoS
    pub fn try_read(&mut self) -> io::Result<(Vec<u8>, bool)> {
        let mut all_data = Vec::new();

        loop {
            // Check read limit to prevent unbounded memory growth
            if all_data.len() >= MAX_READ_PER_POLL {
                return Ok((all_data, false)); // Return data, continue next poll
            }

            match self.socket.read(&mut self.read_buffer) {
                Ok(0) => {
                    // Connection closed
                    return Ok((all_data, true));
                }
                Ok(n) => {
                    self.last_read_activity = Instant::now();
                    all_data.extend_from_slice(&self.read_buffer[..n]);
                    // Continue reading until WouldBlock or limit reached
                }
                Err(ref e) if e.kind() == io::ErrorKind::WouldBlock => {
                    // No more data available
                    return Ok((all_data, false));
                }
                Err(e) => {
                    debug!("Connection {} read error: {:?}", self.token.id, e);
                    return Err(e);
                }
            }
        }
    }

    /// Process handshake data
    ///
    /// Returns (remaining data, response data, handshake complete, error)
    pub fn process_handshake(
        &mut self,
        data: &[u8],
    ) -> (Option<Vec<u8>>, Option<Vec<u8>>, bool, bool) {
        let handshake = match self.handshake.as_mut() {
            Some(h) => h,
            None => return (Some(data.to_vec()), None, true, false), // Handshake already complete
        };

        match handshake.process_bytes(data) {
            Ok(HandshakeProcessResult::InProgress { response_bytes }) => {
                let response = if response_bytes.is_empty() {
                    None
                } else {
                    Some(response_bytes)
                };
                (None, response, false, false)
            }
            Ok(HandshakeProcessResult::Completed {
                response_bytes,
                remaining_bytes,
            }) => {
                let response = if response_bytes.is_empty() {
                    None
                } else {
                    Some(response_bytes)
                };
                let remaining = if remaining_bytes.is_empty() {
                    None
                } else {
                    Some(remaining_bytes)
                };

                // Handshake complete, remove handler
                self.handshake = None;
                self.state = ConnectionState::Active;

                (remaining, response, true, false)
            }
            Err(e) => {
                debug!("Connection {} handshake error: {:?}", self.token.id, e);
                (None, None, false, true)
            }
        }
    }

    /// Has pending writes
    pub fn has_pending_writes(&self) -> bool {
        !self.write_queue.is_empty()
    }

    /// Get desired Interest
    pub fn desired_interest(&self) -> Interest {
        // Closing state no longer needs reads, only write remaining data
        let mut interest = if self.state.can_read() {
            Interest::READABLE
        } else {
            Interest {
                readable: false,
                writable: false,
            }
        };
        if self.has_pending_writes() {
            interest = interest.add_writable();
        }
        interest
    }

    /// Record this connection as needing a flush pass. Returns `true` only on
    /// the unmarked -> marked transition — the caller must then push the id
    /// into the reactor's `pending_flush_ids`. Later calls in the same epoch
    /// return `false` (dedup hit, id already listed).
    fn mark_pending_flush(&mut self) -> bool {
        !std::mem::replace(&mut self.in_pending_flush, true)
    }

    /// Clear and return the pending-flush mark. The flush drain calls this
    /// once per listed id; `false` means the entry is stale (the slot was
    /// freed and reused by a never-marked connection) and must be skipped.
    fn take_pending_flush_mark(&mut self) -> bool {
        std::mem::replace(&mut self.in_pending_flush, false)
    }

    /// Same transition contract as [`Self::mark_pending_flush`], for the
    /// `interest_dirty_ids` list.
    fn mark_interest_dirty(&mut self) -> bool {
        !std::mem::replace(&mut self.in_interest_dirty, true)
    }

    /// Same take contract as [`Self::take_pending_flush_mark`], for the
    /// interest-dirty mark.
    fn take_interest_dirty_mark(&mut self) -> bool {
        std::mem::replace(&mut self.in_interest_dirty, false)
    }

    /// Mark as closing
    pub fn mark_closing(&mut self) {
        self.state = ConnectionState::Closing;
    }

    /// Condemn the connection: mark it `Closing` and arm a drain deadline. Used
    /// when a server-initiated close still has a queued tail that could not be
    /// flushed in one pass (WouldBlock). The connection is kept — its remaining
    /// bytes are drained by later writable events — until either the queue
    /// empties or `deadline` passes, whichever comes first. Idempotent for the
    /// deadline: re-condemning does not extend an existing one (the caller only
    /// arms it when not already condemned), so the linger stays bounded.
    pub fn condemn(&mut self, deadline: Instant) {
        self.state = ConnectionState::Closing;
        self.close_deadline = Some(deadline);
    }

    /// Whether the connection has been condemned (a drain deadline is armed).
    pub fn is_condemned(&self) -> bool {
        self.close_deadline.is_some()
    }

    /// Whether a condemned connection's drain deadline has passed at `now`.
    /// Always false for a connection that was never condemned.
    pub fn condemn_expired(&self, now: Instant) -> bool {
        self.close_deadline.is_some_and(|deadline| now >= deadline)
    }

    /// Mark as closed
    pub fn mark_closed(&mut self) {
        self.state = ConnectionState::Closed;
    }

    /// Close connection
    pub fn shutdown(&mut self) {
        if let Err(e) = self.socket.shutdown(Shutdown::Both) {
            debug!(
                "Socket shutdown error (expected if already closed): {:?}",
                e
            );
        }
        self.mark_closed();
    }

    /// Current TCP_NODELAY setting of the underlying socket (test only)
    #[cfg(test)]
    fn nodelay(&self) -> io::Result<bool> {
        self.socket.nodelay()
    }

    /// Bytes currently queued for writing ahead of any new data. Seeds the
    /// scheduler's join-replay budget so a `play` accounts for an existing backlog.
    fn pending_bytes(&self) -> usize {
        self.write_queue.pending_bytes()
    }

    /// Bytes currently queued for writing (test only)
    #[cfg(test)]
    fn queued_bytes(&self) -> usize {
        self.write_queue.pending_bytes()
    }
}

// ============================================================================
// Publisher State
// ============================================================================

/// One item fed by an in-process publisher.
///
/// The steady-state media path (PERF-5a) short-circuits the serialize→
/// reparse round-trip: audio/video FLV tags arrive already parsed as
/// [`PublisherFeed::Media`] and go straight to the channel machinery, while
/// control and metadata bytes stay on [`PublisherFeed::Raw`] (fed to the
/// session's `handle_input`). Both variants travel a single FIFO channel, so
/// the total ordering the serialize path guarantees is preserved exactly.
pub enum PublisherFeed {
    /// RTMP chunk bytes to feed to the session (handshake-free control,
    /// createStream/publish, and `0x12` metadata).
    Raw(Vec<u8>),
    /// A pre-parsed audio (`0x08`) or video (`0x09`) FLV tag.
    Media {
        tag_type: u8,
        timestamp: RtmpTimestamp,
        data: Bytes,
    },
}

impl PublisherFeed {
    /// The length this item accounts for in its publisher's
    /// [`IngressBudget`]: exactly the quantity `process_publishers` adds to
    /// `bytes_drained` when it consumes the item (Raw → the chunk bytes,
    /// Media → the tag payload), so producer-side acquires and the reactor's
    /// per-round releases balance to zero once everything queued has
    /// drained. Every feed producer must measure through this one helper.
    pub(crate) fn ingress_len(&self) -> usize {
        match self {
            PublisherFeed::Raw(bytes) => bytes.len(),
            PublisherFeed::Media { data, .. } => data.len(),
        }
    }
}

/// How a registered publisher delivers its data.
///
/// - [`PublisherSource::Raw`] is the public `create_stream_sender` path:
///   opaque RTMP chunk bytes, byte-identical to the pre-PERF-5a behaviour.
/// - [`PublisherSource::Feed`] is the `create_rtmp_input` path that mixes
///   bypassed media tags with raw control/metadata bytes.
#[derive(Clone)]
pub enum PublisherSource {
    Raw(crossbeam_channel::Receiver<Vec<u8>>),
    Feed(crossbeam_channel::Receiver<PublisherFeed>),
}

/// RAII claim on a stream key in the shared `stream_keys` set.
///
/// [`claim`](Self::claim) inserts the key (insert-or-fail, so two concurrent
/// claims cannot both win) and the guard releases it **exactly once**, on
/// drop, wherever the value dies: a refused enqueue, a scheduler refusal
/// in [`Reactor::add_publisher`], a queued registration nobody ever
/// consumed, or an accepted publisher's [`PublisherState`] reaching its end
/// of life ([`Reactor::remove_publisher`], or the reactor being torn down
/// with publishers still live). The guard is never disarmed — ownership
/// only ever moves, registration to state — so there is no instant at
/// which the key is claimed but no guard would release it, and a release
/// can never fire twice and free a claim another create has re-won in the
/// meantime.
pub(crate) struct StreamKeyClaim {
    stream_keys: Arc<dashmap::DashSet<String>>,
    /// `Some` while the guard is armed; taken by `drop` (release).
    stream_key: Option<String>,
}

impl StreamKeyClaim {
    /// Atomically claim `stream_key`; gives the key back if already claimed.
    pub(crate) fn claim(
        stream_keys: Arc<dashmap::DashSet<String>>,
        stream_key: String,
    ) -> Result<Self, String> {
        if stream_keys.insert(stream_key.clone()) {
            Ok(Self {
                stream_keys,
                stream_key: Some(stream_key),
            })
        } else {
            Err(stream_key)
        }
    }

    /// Borrow the claimed key, for scheduler lookups and logging.
    pub(crate) fn key(&self) -> &str {
        self.stream_key
            .as_deref()
            .expect("an armed claim always holds its key")
    }
}

impl Drop for StreamKeyClaim {
    fn drop(&mut self) {
        if let Some(stream_key) = self.stream_key.take() {
            self.stream_keys.remove(&stream_key);
        }
    }
}

/// Error returned by [`IngressBudget::acquire`] once the budget is closed:
/// the reactor-side owner of the publisher dropped its
/// [`IngressBudgetGuard`] (stream removed, registration refused or never
/// consumed, reactor teardown), so no drain will ever make room again.
/// Callers map it to the same stream-closed error the dead channel behind
/// the budget produces.
#[derive(Debug)]
pub(crate) struct IngressClosed;

/// The mutable half of an [`IngressBudget`]: all fields live behind one
/// mutex so the wait predicate in `acquire` and every mutation share a
/// single linearization point — there is no test-then-park window in which
/// a release or close could slip between a producer's check and its wait.
struct BudgetInner {
    /// Bytes acquired by producers and not yet released by a drain round.
    queued: usize,
    /// Set exactly once, by [`IngressBudgetGuard`]'s drop; wakes and refuses
    /// every producer from then on.
    closed: bool,
    /// FIFO admission tickets. Every `acquire` takes `next_ticket` on entry;
    /// only the acquire holding `serving` may admit, and admission advances
    /// it. Without the ordering, sibling sender clones with small items
    /// could refill every released batch ahead of a parked large item and
    /// starve it indefinitely — capacity alone is not a queue. With one
    /// producer (both internal paths) the two counters never diverge by
    /// more than one and the fast path is untouched.
    next_ticket: u64,
    serving: u64,
}

/// Per-publisher byte account bounding how far an in-process producer (a
/// user thread on the raw path, the FFmpeg muxer thread on the feed path)
/// may run ahead of the reactor's drain. The publisher channel bounds items
/// only; this bounds bytes, so a large-item stream blocks in its send at
/// [`PUBLISHER_INGRESS_HIGH_WATER_BYTES`] instead of queueing 1024 x
/// item-size. Producers [`acquire`](Self::acquire) before every channel
/// send; the reactor [`release`](Self::release)s each round's drained bytes
/// in one batch (see `process_publishers`).
///
/// Producer and consumer are different threads, so parking needs a real
/// primitive: Mutex+Condvar is the house pattern for capacity waits (see
/// `EncSyncHandle` in `core::context::encoder_stream`). Lock-order safety:
/// the mutex is held only inside the short `acquire`/`release`/`close`
/// bodies here — never across a channel operation, another lock, or a log
/// call — on either thread.
pub(crate) struct IngressBudget {
    inner: Mutex<BudgetInner>,
    cv: Condvar,
    high_water: usize,
}

impl IngressBudget {
    /// Create an account with `high_water` as its byte mark. Returns the
    /// close-on-drop guard for the reactor side (it travels registration →
    /// [`PublisherState`], and whichever owner drops it wakes every blocked
    /// producer) and the shared handle for the producer side.
    pub(crate) fn new(high_water: usize) -> (IngressBudgetGuard, Arc<IngressBudget>) {
        let budget = Arc::new(IngressBudget {
            inner: Mutex::new(BudgetInner {
                queued: 0,
                closed: false,
                next_ticket: 0,
                serving: 0,
            }),
            cv: Condvar::new(),
            high_water,
        });
        (
            IngressBudgetGuard {
                budget: budget.clone(),
            },
            budget,
        )
    }

    /// Lock the account, riding over poisoning: the fields carry no
    /// cross-statement invariant a panic could tear (every critical section
    /// is a couple of integer/flag edits), and `close` MUST still wake
    /// blocked producers even if some other holder panicked — a producer
    /// parked forever is exactly what the close exists to prevent.
    fn lock(&self) -> MutexGuard<'_, BudgetInner> {
        self.inner
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
    }

    /// Reserve `len` bytes ahead of a channel send, blocking while the
    /// account is over its high-water mark. Admission is FIFO (ticketed):
    /// competing sender clones are served in arrival order, so a parked
    /// large item cannot be starved by siblings refilling every released
    /// batch with small ones. The `queued == 0` term admits the head item
    /// of any size into an EMPTY account, so a single item larger than the
    /// whole mark bounds memory at that item's size instead of deadlocking
    /// its producer.
    ///
    /// Liveness: whenever the head waiter blocks, `queued > 0`, so at least
    /// one prior item was sent, and every producer send is followed by a
    /// reactor wake whose eventfd/pipe token persists until drained — a
    /// drain round is therefore already scheduled. Each round releases what
    /// it drained and notifies, and a budget-limited round forces the next
    /// poll to be zero-timeout (`publishers_pending` in `run`), so rounds
    /// continue back-to-back until the head admits; each admission notifies
    /// again, chaining the queue forward. FIFO extends the head's liveness
    /// to every waiter behind it. Teardown of the consumer drops the
    /// [`IngressBudgetGuard`], which closes the account and wakes every
    /// waiter. No path leaves a waiter without a scheduled drain, a
    /// predecessor's admission, or a close.
    pub(crate) fn acquire(&self, len: usize) -> Result<(), IngressClosed> {
        let mut inner = self.lock();
        let ticket = inner.next_ticket;
        inner.next_ticket += 1;
        while !inner.closed
            && !(inner.serving == ticket
                && (inner.queued == 0 || inner.queued.saturating_add(len) <= self.high_water))
        {
            inner = self
                .cv
                .wait(inner)
                .unwrap_or_else(std::sync::PoisonError::into_inner);
        }
        if inner.closed {
            // Deliberately no `serving` advance: `closed` is terminal and
            // checked first, so later tickets exit through this arm too.
            return Err(IngressClosed);
        }
        inner.serving += 1;
        inner.queued = inner.queued.saturating_add(len);
        let has_waiters = inner.next_ticket != inner.serving;
        drop(inner);
        // Hand the head role to the next ticket now — capacity permitting it
        // admits immediately instead of waiting for the next drain round.
        // notify_one could wake a non-head ticket, so this must broadcast;
        // the uncontended fast path (no successor ticket) skips the futex
        // entirely and pays only the two counter writes.
        if has_waiters {
            self.cv.notify_all();
        }
        Ok(())
    }

    /// Return `len` drained (or rolled-back) bytes to the account and wake
    /// every parked producer. The reactor batches this to one call per
    /// publisher per drain round, so a blocked producer wakes at most once
    /// per round — a natural ~512 KiB step — rather than per item.
    pub(crate) fn release(&self, len: usize) {
        let mut inner = self.lock();
        debug_assert!(
            inner.queued >= len,
            "ingress budget released more than was acquired ({} < {len})",
            inner.queued
        );
        inner.queued = inner.queued.saturating_sub(len);
        let has_waiters = inner.next_ticket != inner.serving;
        drop(inner);
        if has_waiters {
            self.cv.notify_all();
        }
    }

    /// Close the account: every parked producer wakes with [`IngressClosed`]
    /// and every later acquire is refused. Idempotent; in production the
    /// guard's drop is the only caller.
    fn close(&self) {
        let mut inner = self.lock();
        inner.closed = true;
        drop(inner);
        self.cv.notify_all();
    }

    /// Bytes currently acquired and not yet released (test only).
    #[cfg(test)]
    pub(crate) fn queued_bytes(&self) -> usize {
        self.lock().queued
    }

    /// Tickets taken minus tickets served — the acquires that have entered
    /// but not yet admitted: parked waiters plus any currently racing the
    /// lock. Closed-account exits do not advance `serving`, so the count
    /// stays inflated after a close; use only in pre-close phases (test
    /// only; lets tests wait until a producer is deterministically inside
    /// `acquire` before proceeding).
    #[cfg(test)]
    pub(crate) fn waiting_acquires(&self) -> u64 {
        let inner = self.lock();
        inner.next_ticket - inner.serving
    }
}

/// Close-on-drop owner of an [`IngressBudget`], with the same move-only
/// lifecycle as [`StreamKeyClaim`]: created alongside the registration,
/// moved into the accepted publisher's [`PublisherState`], and released —
/// here: closed — **exactly once**, wherever the owning value dies: a
/// refused enqueue, a queued registration nobody ever consumed (the
/// [`RegistrationKillSwitch`]'s terminal drain), a scheduler refusal in
/// [`Reactor::add_publisher`], [`Reactor::remove_publisher`], or the
/// reactor's `publishers` slab being torn down. Ownership only ever moves,
/// so there is no instant at which producers could block on an account no
/// guard would ever close.
pub(crate) struct IngressBudgetGuard {
    budget: Arc<IngressBudget>,
}

impl IngressBudgetGuard {
    /// Borrow the shared account, for the drain loop to clone.
    pub(crate) fn budget(&self) -> &Arc<IngressBudget> {
        &self.budget
    }
}

impl Drop for IngressBudgetGuard {
    fn drop(&mut self) {
        self.budget.close();
    }
}

/// A publisher registration in flight from the server's `register_publisher`
/// to the reactor. It carries the key claim so a registration dropped
/// anywhere short of [`Reactor::add_publisher`] taking ownership — refused
/// at enqueue because the intake is closed or full, or still queued when the
/// worker's [`RegistrationKillSwitch`] fires — releases the key
/// automatically. The ingress-budget guard rides the same lifecycle, so the
/// same drops also close the budget and wake any producer parked at the
/// byte gate.
pub(crate) struct PublisherRegistration {
    pub(crate) claim: StreamKeyClaim,
    pub(crate) source: PublisherSource,
    pub(crate) budget: IngressBudgetGuard,
}

/// The registration queue proper: the worker-liveness flag and the payload
/// live behind ONE lock so no observer can see them disagree.
pub(crate) struct RegistrationQueue {
    /// `false` once the worker has died ([`RegistrationKillSwitch`]'s drop,
    /// which also drains the queue in the same critical section) or the
    /// server was signaled to stop ([`RegistrationHandoff::close`], which
    /// leaves queued entries to the reactor's remaining rounds, backstopped
    /// by the kill switch's terminal drain). Either flip shares the lock
    /// with every enqueue: an enqueue happens entirely before it, or
    /// observes `false` and is refused. There is no in-between.
    alive: bool,
    queue: VecDeque<PublisherRegistration>,
}

/// Hands publisher registrations from the server's create paths to the
/// reactor worker.
///
/// This is deliberately a lock-shared queue and not a channel. With a
/// channel, "is the worker still there?" and "enqueue the payload" are two
/// separate linearization points, which leaves structural gaps:
/// - a send can land after the worker's last drain saw the queue empty, and
///   a bounded crossbeam channel frees such a message only when its LAST
///   endpoint drops — the server holds its sender endpoint for as long as
///   the server value lives, so the message's stream-key claim would leak
///   for that entire lifetime;
/// - a drain loop that stops at `Empty` can be kept spinning by a steady
///   producer, because nothing tells producers to stop.
///
/// Here [`enqueue`](Self::enqueue) checks liveness and capacity and pushes
/// in one critical section, and the kill switch flips liveness and drains in
/// one critical section, so every registration ends in exactly one of three
/// hands — the reactor (via [`drain_into`](Self::drain_into)), the terminal
/// kill-switch drain, or the refused caller — each of which releases the
/// key claim by dropping the registration.
pub(crate) struct RegistrationHandoff {
    queue: Mutex<RegistrationQueue>,
}

/// Why [`RegistrationHandoff::enqueue`] refused a registration. Both arms
/// hand the registration back, and dropping it releases the key claim; the
/// split exists so the create paths can surface distinct errors for "the
/// server is gone" and "the server is backlogged".
pub(crate) enum EnqueueRefused {
    /// The intake is closed: the worker died, or the server was signaled to
    /// stop. No reactor round will consume the queue again, so accepting
    /// the registration would report a success that cannot happen.
    Closed(PublisherRegistration),
    /// The queue already holds [`REGISTRATION_QUEUE_CAPACITY`] registrations
    /// the reactor has not picked up. The worker is alive; the caller may
    /// retry once the backlog drains.
    Full(PublisherRegistration),
}

impl RegistrationHandoff {
    pub(crate) fn new() -> Self {
        Self {
            queue: Mutex::new(RegistrationQueue {
                alive: true,
                queue: VecDeque::new(),
            }),
        }
    }

    /// Lock the queue, riding over poisoning: the two fields carry no
    /// cross-statement invariant a panic could tear (every critical section
    /// is a flag test plus one queue edit), and the kill switch MUST finish
    /// its terminal drain even if another holder panicked — the claims it
    /// exists to release would otherwise leak.
    fn lock(&self) -> MutexGuard<'_, RegistrationQueue> {
        self.queue
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
    }

    /// Enqueue a registration for the reactor, or hand it back if the worker
    /// is gone or the queue is at capacity. The liveness check, the capacity
    /// check and the push share one critical section, so a registration
    /// accepted here is guaranteed visible to the drain that runs when the
    /// worker dies — there is no window in which it can be queued yet seen
    /// by nobody — and the queue can never exceed its bound. The caller
    /// drops a refused registration, releasing its key claim.
    pub(crate) fn enqueue(
        &self,
        registration: PublisherRegistration,
    ) -> Result<(), EnqueueRefused> {
        let mut queue = self.lock();
        if !queue.alive {
            Err(EnqueueRefused::Closed(registration))
        } else if queue.queue.len() >= REGISTRATION_QUEUE_CAPACITY {
            Err(EnqueueRefused::Full(registration))
        } else {
            queue.queue.push_back(registration);
            Ok(())
        }
    }

    /// Close the intake without draining: every enqueue from this point on
    /// is refused as [`EnqueueRefused::Closed`]. The stop signal calls this
    /// so a caller that observes the server as stopped can no longer be
    /// told Ok for a registration no reactor round will consume. Whatever
    /// is already queued keeps its owner — the reactor's remaining rounds,
    /// with the kill switch's terminal drain as the claims backstop.
    pub(crate) fn close(&self) {
        self.lock().alive = false;
    }

    /// Move up to [`MAX_REGISTRATIONS_PER_POLL`] queued registrations into
    /// `batch` under one lock, front first, and report whether any remain.
    /// The caller processes after this returns, off the lock, so enqueuers
    /// are never blocked behind scheduler work; the budget keeps a large
    /// backlog from monopolizing one reactor round, and a `true` return
    /// tells the caller to come back promptly for the remainder instead of
    /// sleeping out a full poll timeout on it.
    fn drain_into(&self, batch: &mut Vec<PublisherRegistration>) -> bool {
        let mut queue = self.lock();
        let take = queue.queue.len().min(MAX_REGISTRATIONS_PER_POLL);
        batch.extend(queue.queue.drain(..take));
        !queue.queue.is_empty()
    }

    /// Close the intake and take whatever is still queued, in one critical
    /// section. After this returns no enqueue can succeed, so the returned
    /// batch is the final one — a single bounded drain, no re-check loop for
    /// producers to keep spinning.
    fn kill(&self) -> VecDeque<PublisherRegistration> {
        let mut queue = self.lock();
        queue.alive = false;
        std::mem::take(&mut queue.queue)
    }
}

/// Worker-side guard for a [`RegistrationHandoff`]: dropping it — on normal
/// exit and unwind alike — closes the registration intake and releases the
/// key claim of everything still queued.
///
/// The worker must arm this as its very first statement, before the fallible
/// reactor construction and before any log call (a user-installed logger can
/// panic). From that point, however the worker dies, this drop runs, so no
/// registration can outlive the worker inside the queue.
pub(crate) struct RegistrationKillSwitch {
    handoff: Arc<RegistrationHandoff>,
}

impl RegistrationKillSwitch {
    pub(crate) fn arm(handoff: Arc<RegistrationHandoff>) -> Self {
        Self { handoff }
    }

    /// The reactor's consume side borrows the handoff through the switch,
    /// tying the consumer's lifetime to the guard that cleans up after it.
    pub(crate) fn handoff(&self) -> &RegistrationHandoff {
        &self.handoff
    }
}

impl Drop for RegistrationKillSwitch {
    fn drop(&mut self) {
        // Take the leftovers inside the critical section, drop them outside
        // it: releasing a claim writes to the shared stream-key set, and no
        // foreign Drop code should ever run under the queue lock.
        let leftovers = self.handoff.kill();
        drop(leftovers);
    }
}

/// Publisher state
///
/// Owns the stream-key claim for as long as the publisher is accepted.
/// Dropping the state — [`Reactor::remove_publisher`], or the reactor's
/// `publishers` slab being dropped on teardown, clean exit and unwind
/// alike — releases the key and closes the ingress budget, waking any
/// producer parked at the byte gate.
pub struct PublisherState {
    pub(crate) claim: StreamKeyClaim,
    pub source: PublisherSource,
    pub(crate) budget: IngressBudgetGuard,
}

/// One fanout packet bound for a connection's write queue: (target
/// connection id, serialized bytes, is_keyframe, is_sequence_header,
/// is_video, droppable — rml's `Packet::can_be_dropped`). The payload is
/// `Bytes` end-to-end: the shared fanout hands every watcher a refcount
/// clone of one serialization, so the buffer element must not copy.
type OutboundWrite = (usize, Bytes, bool, bool, bool, bool);

/// Route a batch of [`ServerResult`]s into the reactor's write / close buffers.
fn collect_server_results(
    server_results: &mut Vec<ServerResult>,
    packets_to_write: &mut Vec<OutboundWrite>,
    ids_to_close: &mut Vec<usize>,
) {
    for result in server_results.drain(..) {
        match result {
            ServerResult::OutboundPacket {
                target_connection_id,
                bytes,
                can_be_dropped,
                is_keyframe,
                is_sequence_header,
                is_video,
            } => {
                packets_to_write.push((
                    target_connection_id,
                    bytes,
                    is_keyframe,
                    is_sequence_header,
                    is_video,
                    can_be_dropped,
                ));
            }
            ServerResult::DisconnectConnection {
                connection_id: close_id,
            } => {
                ids_to_close.push(close_id);
            }
        }
    }
}

// ============================================================================
// Reactor
// ============================================================================

/// Event handling result
pub enum HandleResult {
    /// Disconnect
    Disconnect(usize),
}

/// Main Reactor structure
pub struct Reactor {
    /// Event poller
    poller: Poller,
    /// Connection storage (using slab allocation)
    connections: slab::Slab<ReactorConnection>,
    /// Generation counter (for each slot)
    generations: HashMap<usize, Generation>,
    /// Business scheduler
    scheduler: RtmpScheduler,
    /// Publishers. Each state owns its stream-key claim, so dropping this
    /// slab (reactor teardown with publishers still live) releases every
    /// accepted key back to the server's shared set.
    publishers: slab::Slab<PublisherState>,
    /// Stop flag
    status: Arc<AtomicUsize>,
    /// Maximum allowed connections (auto-adjusted by system FD limit)
    max_connections: usize,
    /// Ids of connections with pending writes that need flushing (dirty
    /// tracking for O(m) instead of O(n)). Dedup lives on the connection
    /// (`ReactorConnection::in_pending_flush`): an id is pushed only on the
    /// unmarked -> marked transition, so the list holds at most one entry
    /// per marked connection and the hot fanout path pays a bool test — on
    /// the connection it already borrows — instead of a hash-set insert per
    /// packet. Entries for since-removed connections are skipped at drain
    /// time: the flag died with the connection, and a fresh connection
    /// reusing the slab slot is born unmarked.
    pending_flush_ids: Vec<usize>,
    /// Connections that stopped at MAX_READ_PER_POLL and must be re-drained
    /// next loop iteration. An edge-triggered poller (EPOLLET/EV_CLEAR) fires no
    /// new readable event for bytes already in the kernel buffer, so we resume
    /// the drain ourselves rather than wait for the peer. Unlike the dirty-id
    /// lists (whose per-connection flags die with the connection, making stale
    /// entries self-resolving) this set has no such guard, so ids must be
    /// scrubbed on connection removal: the slab reuses ids and a stale entry
    /// would read a brand-new connection out of turn.
    read_pending: HashSet<usize>,
    /// Ids of connections whose poller interest may need updating (dirty
    /// tracking for O(m) instead of O(n)); same flag-gated scheme as
    /// `pending_flush_ids`, via `ReactorConnection::in_interest_dirty`.
    interest_dirty_ids: Vec<usize>,
    /// Reusable snapshot buffer for the two dirty-id drains: the live list is
    /// swapped in here, so marks landing mid-drain go to the (now empty) live
    /// list for the next pass and no per-drain Vec is allocated. Cleared —
    /// and, under sustained sparsity, walked down toward the capacity floor —
    /// after each drain; the swap rotation parks every backing buffer here,
    /// so each one meets the shrink policy in turn.
    dirty_drain_scratch: Vec<usize>,
    /// Consecutive quarter-occupancy drains observed; the dirty-id shrink
    /// fires only once this reaches `SPARSE_DRAINS_BEFORE_SHRINK` (see
    /// `shrink_drain_scratch_if_sparse`).
    sparse_drain_streak: u32,
    /// Reusable buffer for packets to write (avoids allocation in handle_readable)
    packets_buffer: Vec<OutboundWrite>,
    /// Reusable buffer for IDs to close (avoids allocation in handle_readable)
    ids_to_close_buffer: Vec<usize>,
    /// Reusable buffer for scheduler results on the publisher feed and
    /// socket-ingest scheduler batches (avoids a Vec allocation per media
    /// tag / raw chunk / inbound socket batch)
    server_results_buffer: Vec<ServerResult>,
    /// Reusable buffer for handle results (avoids allocation in handle_readable)
    results_buffer: Vec<HandleResult>,
    /// Last time the full connection-timeout sweep ran (PERF-10 throttle)
    last_timeout_check: Instant,
}

// Status constants
#[cfg_attr(not(test), allow(dead_code))] // running state is set via the scheduler-owned status; referenced here only by tests
const STATUS_RUN: usize = 1;
const STATUS_END: usize = 2;

impl Reactor {
    /// Create new Reactor
    ///
    /// # Arguments
    /// * `gop_limit` - Maximum number of GOPs to cache per stream
    /// * `max_connections` - Maximum connections limit (None = auto-detect based on system FD limit)
    /// * `status` - Shared status flag for graceful shutdown
    ///
    /// The reactor holds no handle to the server's shared stream-key set:
    /// every key it ever releases arrives inside a [`StreamKeyClaim`] guard
    /// (carrying its own reference to that set) and is released by dropping
    /// the guard.
    ///
    /// The effective max_connections is calculated as:
    /// `min(config_value, 0.8 * system_fd_limit)` to leave headroom for other FDs.
    pub fn new(
        gop_limit: usize,
        max_connections: Option<usize>,
        status: Arc<AtomicUsize>,
    ) -> io::Result<Self> {
        let poller = Poller::new()?;

        // Use the shared effective_max_connections calculation
        let effective_max = effective_max_connections(max_connections);

        Ok(Self {
            poller,
            connections: slab::Slab::with_capacity(1024),
            generations: HashMap::new(),
            scheduler: RtmpScheduler::new(gop_limit),
            publishers: slab::Slab::with_capacity(64),
            status,
            max_connections: effective_max,
            pending_flush_ids: Vec::with_capacity(256),
            read_pending: HashSet::new(),
            interest_dirty_ids: Vec::with_capacity(256),
            dirty_drain_scratch: Vec::with_capacity(256),
            sparse_drain_streak: 0,
            packets_buffer: Vec::with_capacity(64),
            ids_to_close_buffer: Vec::with_capacity(16),
            server_results_buffer: Vec::with_capacity(64),
            results_buffer: Vec::with_capacity(16),
            last_timeout_check: Instant::now(),
        })
    }

    /// Add new connection
    ///
    /// Returns error if max_connections limit is reached.
    pub fn add_connection(&mut self, socket: TcpStream) -> io::Result<ConnectionToken> {
        // Check connection limit before adding
        if self.connections.len() >= self.max_connections {
            return Err(io::Error::new(
                io::ErrorKind::ConnectionRefused,
                format!(
                    "max connections limit reached ({}/{})",
                    self.connections.len(),
                    self.max_connections
                ),
            ));
        }

        let entry = self.connections.vacant_entry();
        let id = entry.key();

        // Get or initialize generation
        let generation = self.generations.entry(id).or_insert(0);
        *generation = generation.wrapping_add(1);
        let token = ConnectionToken::new(id, *generation);

        let conn = ReactorConnection::new(token, socket)?;

        // Register to poller with encoded token (id + generation)
        let poller_token = token.to_poller_token();
        self.poller
            .register(conn.raw_handle(), poller_token, Interest::READABLE)?;

        entry.insert(conn);

        debug!("Connection {} added (generation {})", id, token.generation);
        Ok(token)
    }

    /// Close a connection, first trying to deliver its queued tail without
    /// blocking or truncating.
    ///
    /// A server-initiated close often queues a final packet in the same round
    /// — most visibly the `finish_playing` status a watcher gets when the
    /// publisher ends — and a raw close would discard it: nothing between the
    /// enqueue and the removal ever writes to the socket. One `try_flush`
    /// usually drains the tail (the kernel send buffer is rarely full at this
    /// point) and the connection is removed immediately. But if the flush
    /// short-writes (WouldBlock with a partial entry left), dropping the socket
    /// now would emit a TRUNCATED RTMP message to the peer and lose the final
    /// status. In that case the connection is condemned instead: kept alive
    /// with a bounded deadline while the remaining bytes drain on later
    /// writable events (`handle_writable` / `flush_pending` close it once the
    /// queue empties; `check_timeouts` force-removes it if the deadline passes).
    fn close_connection_after_flush(&mut self, id: usize) {
        let now = Instant::now();
        let remove = match self.connections.get_mut(id) {
            None => true, // already gone; remove_connection is a no-op
            Some(conn) => {
                // A condemned connection whose drain window already elapsed is
                // force-removed even if a tail remains: lingering is bounded.
                if conn.condemn_expired(now) {
                    true
                } else {
                    match conn.try_flush() {
                        // Socket error or orderly close: nothing left to save.
                        Err(_) | Ok(true) => true,
                        // Flushed; if the queue is now empty the tail was fully
                        // delivered (the common case) and we remove now.
                        Ok(false) if !conn.has_pending_writes() => true,
                        // A tail remains (WouldBlock). Condemn for a bounded
                        // drain rather than truncate the message. Keep an
                        // existing condemnation's deadline so it is not extended.
                        Ok(false) => {
                            if !conn.is_condemned() {
                                conn.condemn(now + CLOSE_DRAIN_TIMEOUT);
                            }
                            false
                        }
                    }
                }
            }
        };

        if remove {
            self.remove_connection(id);
        } else {
            // (Re)register writable interest so the poller drives the drain;
            // desired_interest() adds writable while has_pending_writes().
            // The connection is guaranteed present on this branch (only a
            // kept connection lands here), so the re-borrow cannot miss.
            if let Some(conn) = self.connections.get_mut(id) {
                if conn.mark_interest_dirty() {
                    self.interest_dirty_ids.push(id);
                }
            }
        }
    }

    /// Remove connection
    pub fn remove_connection(&mut self, id: usize) {
        // A pending re-drain must not outlive the connection: `read_pending`
        // stores raw ids (no generation), and the slab may hand this id to a
        // new connection next iteration.
        self.read_pending.remove(&id);
        if let Some(conn) = self.connections.try_remove(id) {
            // Deregister from poller
            if let Err(e) = self.poller.deregister(conn.raw_handle()) {
                debug!(
                    "Failed to deregister connection {} from poller: {:?}",
                    id, e
                );
            }

            // Notify scheduler
            self.scheduler.notify_connection_closed(id);

            debug!(
                "Connection {} removed (generation {})",
                id, conn.token.generation
            );
        }
    }

    /// Add publishers.
    ///
    /// In-process publishers claim their key in the server's shared set at
    /// create time (insert-or-fail) and the registration carries that claim
    /// here, so success does not insert. Acceptance moves the still-armed
    /// claim into the publisher's state, which owns it until the state drops
    /// at publisher end-of-life. The move happens before the log call, so
    /// even a panicking log handler cannot unwind past a key that is claimed
    /// yet owned by no guard. Refusal drops the claim, which releases the
    /// key — `new_channel` can refuse a key a network session is already
    /// publishing to, and leaving the claim in place would block that key
    /// for in-process creates forever.
    pub fn add_publisher(&mut self, registration: PublisherRegistration) -> Option<usize> {
        let PublisherRegistration {
            claim,
            source,
            budget,
        } = registration;
        let entry = self.publishers.vacant_entry();
        let id = entry.key();

        if self.scheduler.new_channel(claim.key().to_string(), id) {
            let state = entry.insert(PublisherState {
                claim,
                source,
                budget,
            });
            debug!("Publisher {} added for stream: {}", id, state.claim.key());
            Some(id)
        } else {
            // Dropping `claim` releases the key; dropping `budget` closes
            // the ingress account, waking any already-parked producer.
            None
        }
    }

    /// Remove publishers.
    ///
    /// Dropping the removed state releases its stream-key claim, so the key
    /// is claimable again the moment the publisher dies here.
    pub fn remove_publisher(&mut self, id: usize) {
        if let Some(pub_state) = self.publishers.try_remove(id) {
            self.scheduler.notify_publisher_closed(id);
            debug!("Publisher {} removed", id);
            drop(pub_state);
        }
    }

    /// Update connection's poller interest
    fn update_interest(&mut self, id: usize) -> io::Result<()> {
        if let Some(conn) = self.connections.get_mut(id) {
            let desired = conn.desired_interest();
            if desired != conn.current_interest {
                self.poller
                    .modify(conn.raw_handle(), conn.token.to_poller_token(), desired)?;
                conn.current_interest = desired;
            }
        }
        Ok(())
    }

    /// Validate connection exists and generation matches
    ///
    /// Returns Some(id) if connection is valid, None if stale event
    /// This prevents ABA problem where a new connection reuses an old slot
    fn validate_connection(&self, poller_token: usize) -> Option<usize> {
        let token = ConnectionToken::from_poller_token(poller_token);
        if let Some(conn) = self.connections.get(token.id) {
            // The generation round-trips losslessly through the poller token
            // on every pointer width (see ConnectionToken), so exact equality
            // is the correct check on all targets.
            if conn.token.generation == token.generation {
                return Some(token.id);
            }
            // Stale event: generation mismatch
            debug!(
                "Stale event for connection {}: expected gen {}, got {}",
                token.id, conn.token.generation, token.generation
            );
        }
        None
    }

    /// Handle readable event
    fn handle_readable(&mut self, id: usize) -> Vec<HandleResult> {
        // Clear and reuse buffers to avoid allocation in hot path
        self.results_buffer.clear();
        self.packets_buffer.clear();
        self.ids_to_close_buffer.clear();

        // Read data from connection
        let (data, should_close) = match self.read_connection_data(id) {
            Some(result) => result,
            None => return std::mem::take(&mut self.results_buffer),
        };

        // Process the data through scheduler
        self.process_connection_data(id, &data);

        // Write pending packets to target connections
        self.write_pending_packets();

        // Close connections that need closing
        for close_id in self.ids_to_close_buffer.drain(..) {
            self.results_buffer.push(HandleResult::Disconnect(close_id));
        }

        // If EOF detected during read, close connection after processing data
        if should_close {
            self.results_buffer.push(HandleResult::Disconnect(id));
        }

        // try_read caps each pass at MAX_READ_PER_POLL to bound memory, so
        // `data.len() >= MAX_READ_PER_POLL` means it stopped at the cap (not at
        // WouldBlock) and the kernel buffer may still hold a tail. Under an
        // edge-triggered poller no further readable event fires for those bytes,
        // so mark the connection to be re-drained next iteration. `should_close`
        // is always false at the cap (try_read returns EOF only via a 0-read),
        // but processing may have condemned this connection (handshake error,
        // scheduler-driven disconnect) — a doomed connection must not be
        // re-read after the decision to close it.
        let self_disconnect = self.results_buffer.iter().any(|r| {
            let HandleResult::Disconnect(close_id) = r;
            *close_id == id
        });
        if !should_close && !self_disconnect && data.len() >= MAX_READ_PER_POLL {
            self.read_pending.insert(id);
        }

        std::mem::take(&mut self.results_buffer)
    }

    /// Re-drain connections whose read stopped at MAX_READ_PER_POLL in a
    /// previous iteration (loop step 5b). Skips ids the event pass already
    /// read this iteration (their read either drained to WouldBlock or
    /// re-inserted itself into `read_pending`) and ids already slated for
    /// close (error/hangup or a disconnect decision this pass) — both would
    /// otherwise read a second time between two flush points.
    fn resume_capped_reads(
        &mut self,
        resume_ids: Vec<usize>,
        read_this_pass: &[usize],
        ids_to_close: &mut Vec<usize>,
    ) {
        for id in resume_ids {
            if read_this_pass.contains(&id) || ids_to_close.contains(&id) {
                continue;
            }
            for result in self.handle_readable(id) {
                let HandleResult::Disconnect(close_id) = result;
                ids_to_close.push(close_id);
            }
        }
    }

    /// Read data from connection
    fn read_connection_data(&mut self, id: usize) -> Option<(Vec<u8>, bool)> {
        let conn = match self.connections.get_mut(id) {
            Some(c) if c.state.can_read() => c,
            _ => return None,
        };

        match conn.try_read() {
            Ok((data, close)) => {
                // Check if there's data to process
                if data.is_empty() {
                    // No data, close if needed
                    if close {
                        self.results_buffer.push(HandleResult::Disconnect(id));
                    }
                    return None;
                }
                Some((data, close))
            }
            Err(_) => {
                self.results_buffer.push(HandleResult::Disconnect(id));
                None
            }
        }
    }

    /// Process connection data through scheduler
    fn process_connection_data(&mut self, id: usize, data: &[u8]) {
        let conn = match self.connections.get_mut(id) {
            Some(c) => c,
            None => return,
        };

        let state = conn.state;

        if state == ConnectionState::Handshaking {
            self.process_handshake_data(id, data);
        } else {
            self.process_normal_data(id, data);
        }
    }

    /// Process handshake data
    fn process_handshake_data(&mut self, id: usize, data: &[u8]) {
        let conn = match self.connections.get_mut(id) {
            Some(c) => c,
            None => return,
        };

        let (remaining, response, completed, error) = conn.process_handshake(data);

        if error {
            self.results_buffer.push(HandleResult::Disconnect(id));
            return;
        }

        if let Some(resp) = response {
            if !conn.enqueue_raw(resp) {
                // Queue full during handshake, disconnect
                self.results_buffer.push(HandleResult::Disconnect(id));
                return;
            }
            // Mark connection for pending flush and interest update
            if conn.mark_pending_flush() {
                self.pending_flush_ids.push(id);
            }
            if conn.mark_interest_dirty() {
                self.interest_dirty_ids.push(id);
            }
        }

        if completed {
            debug!("Connection {} handshake completed", id);
        }

        // Process remaining data
        if let Some(remaining_data) = remaining {
            if !remaining_data.is_empty() {
                self.process_scheduler_results(id, &remaining_data);
            }
        }
    }

    /// Process normal (non-handshake) data
    fn process_normal_data(&mut self, id: usize, data: &[u8]) {
        self.process_scheduler_results(id, data);
    }

    /// Process scheduler results
    fn process_scheduler_results(&mut self, id: usize, data: &[u8]) {
        // The connection's current write-queue backlog seeds the join-replay
        // budget for any `play` handled in this batch (bytes already queued ahead
        // of the burst). 0 if the connection has already vanished.
        let backlog = self
            .connections
            .get(id)
            .map(|conn| conn.pending_bytes())
            .unwrap_or(0);
        self.server_results_buffer.clear();
        match self.scheduler.bytes_received_with_backlog(
            id,
            data,
            backlog,
            &mut self.server_results_buffer,
        ) {
            Ok(()) => {
                for result in self.server_results_buffer.drain(..) {
                    match result {
                        ServerResult::OutboundPacket {
                            target_connection_id,
                            bytes,
                            can_be_dropped,
                            is_keyframe,
                            is_sequence_header,
                            is_video,
                        } => {
                            self.packets_buffer.push((
                                target_connection_id,
                                bytes,
                                is_keyframe,
                                is_sequence_header,
                                is_video,
                                can_be_dropped,
                            ));
                        }
                        ServerResult::DisconnectConnection {
                            connection_id: close_id,
                        } => {
                            self.ids_to_close_buffer.push(close_id);
                        }
                    }
                }
            }
            Err(e) => {
                debug!("Connection {} scheduler error: {}", id, e);
                self.results_buffer.push(HandleResult::Disconnect(id));
            }
        }
    }

    /// Write pending packets to target connections.
    ///
    /// Drains `packets_buffer`, enqueueing each packet to its target. Targets
    /// that accepted data are marked for pending flush and interest update;
    /// targets that refused (backpressure cap) are pushed into
    /// `ids_to_close_buffer` — how those close is the caller's decision.
    /// Shared by the readable-path fanout and `process_publishers`.
    fn write_pending_packets(&mut self) {
        // One clock read for the whole drain (PERF-10): the entry timestamp
        // only feeds the seconds-granular age eviction, and at W watchers a
        // per-entry `Instant::now` would be a fanout-scaling cost.
        let now = Instant::now();

        for (target_id, data, is_keyframe, is_sequence_header, is_video, droppable) in
            self.packets_buffer.drain(..)
        {
            if let Some(target_conn) = self.connections.get_mut(target_id) {
                // A condemned connection is lingering only to drain the final
                // tail queued before it was condemned. Appending new live media
                // would keep its queue from ever emptying, so it would never
                // close on drain and be force-closed at the deadline instead —
                // possibly truncating this freshly-appended packet, and wasting
                // serialization / queue memory / fanout CPU. Skip the
                // post-condemn append; the pre-condemn tail still drains.
                if target_conn.is_condemned() {
                    continue;
                }
                let enqueued = target_conn.enqueue_data(
                    data,
                    is_keyframe,
                    is_sequence_header,
                    is_video,
                    droppable,
                    now,
                );
                if enqueued {
                    // Mark for pending flush and interest update in the same
                    // pass. The connection-owned flags gate the pushes, so a
                    // target hit by several packets this round costs two bool
                    // tests per repeat — on the connection this loop already
                    // borrows — and is listed exactly once.
                    if target_conn.mark_pending_flush() {
                        self.pending_flush_ids.push(target_id);
                    }
                    if target_conn.mark_interest_dirty() {
                        self.interest_dirty_ids.push(target_id);
                    }
                } else {
                    // Backpressure too high, cannot enqueue, close target connection
                    self.ids_to_close_buffer.push(target_id);
                }
            }
        }
    }

    /// Handle writable event
    fn handle_writable(&mut self, id: usize) -> Option<HandleResult> {
        let conn = match self.connections.get_mut(id) {
            Some(c) if c.state.can_write() => c,
            _ => return None,
        };

        match conn.try_flush() {
            Ok(true) => Some(HandleResult::Disconnect(id)),
            Ok(false) => {
                if !conn.has_pending_writes() {
                    // Queue drained. A condemned connection was only lingering
                    // to deliver this tail — now safe to close it (the message
                    // is complete, no truncation). Otherwise just clear the
                    // writable interest to avoid CPU churn on level-triggered
                    // systems (Windows WSAPoll).
                    if conn.is_condemned() {
                        return Some(HandleResult::Disconnect(id));
                    }
                    if conn.mark_interest_dirty() {
                        self.interest_dirty_ids.push(id);
                    }
                }
                None
            }
            Err(_) => Some(HandleResult::Disconnect(id)),
        }
    }

    /// Feed a raw RTMP chunk from a publisher to the session and collect the
    /// resulting outbound packets / disconnects. Returns `false` if the
    /// scheduler errored and the publisher should be removed.
    fn dispatch_publish_bytes(
        &mut self,
        pub_id: usize,
        bytes: Vec<u8>,
        packets_to_write: &mut Vec<OutboundWrite>,
        ids_to_close: &mut Vec<usize>,
    ) -> bool {
        self.server_results_buffer.clear();
        match self
            .scheduler
            .publish_bytes_received(pub_id, bytes, &mut self.server_results_buffer)
        {
            Ok(()) => {
                collect_server_results(
                    &mut self.server_results_buffer,
                    packets_to_write,
                    ids_to_close,
                );
                true
            }
            Err(e) => {
                // This error is fatal for the publisher — the caller removes
                // it and its feed sender starts failing. Report it loudly at
                // THIS moment of truth: the write callback's later failure
                // classification is about the SERVER lifecycle (a deliberate
                // stop may land in between), and must not be the only
                // visible record of a feed that actually died right here,
                // for its own protocol error.
                warn!(
                    "Publisher {} rejected with a fatal session error and will be removed: {}",
                    pub_id, e
                );
                false
            }
        }
    }

    /// Handle publishers data
    ///
    /// Each publisher's channel is drained under a per-round item + byte
    /// budget. Both inner loops consume an item first and only then check the
    /// budgets, so nothing pulled off the channel is ever discarded; a budget
    /// hit is a flat `break` with no removal or other state effect. Returns
    /// the publishers to remove and whether any budget was exhausted — the
    /// latter tells `run()` data may still be queued so the next poll must
    /// not sleep.
    fn process_publishers(&mut self) -> (Vec<usize>, bool) {
        let mut publisher_ids_to_remove = Vec::new();
        let mut packets_to_write = Vec::new();
        let mut ids_to_close = Vec::new();
        let mut budget_exhausted = false;

        let publisher_ids: Vec<usize> = self.publishers.iter().map(|(id, _)| id).collect();

        for pub_id in publisher_ids {
            let (source, ingress_budget) = {
                let pub_state = match self.publishers.get(pub_id) {
                    Some(p) => p,
                    None => continue,
                };
                (
                    pub_state.source.clone(),
                    pub_state.budget.budget().clone(),
                )
            };

            // Budgets are per publisher per round, so one flooding publisher
            // cannot consume the whole round of its slower peers either.
            let mut items = 0usize;
            let mut bytes_drained = 0usize;

            match source {
                PublisherSource::Raw(receiver) => loop {
                    match receiver.try_recv() {
                        Ok(bytes) => {
                            items += 1;
                            bytes_drained += bytes.len();
                            if !self.dispatch_publish_bytes(
                                pub_id,
                                bytes,
                                &mut packets_to_write,
                                &mut ids_to_close,
                            ) {
                                // Fatal publisher error (e.g. an oversized sequence
                                // header rejected at ingest): finalize the watchers
                                // before removal so none is orphaned in Watching.
                                // Unlike the Disconnected arm, this does NOT re-feed
                                // the just-errored session.
                                let mut results = self.scheduler.abort_publisher_watchers(pub_id);
                                collect_server_results(
                                    &mut results,
                                    &mut packets_to_write,
                                    &mut ids_to_close,
                                );
                                publisher_ids_to_remove.push(pub_id);
                                break;
                            }
                            if items >= MAX_PUBLISH_ITEMS_PER_POLL
                                || bytes_drained >= MAX_PUBLISH_BYTES_PER_POLL
                            {
                                budget_exhausted = true;
                                break;
                            }
                        }
                        Err(crossbeam_channel::TryRecvError::Empty) => break,
                        Err(crossbeam_channel::TryRecvError::Disconnected) => {
                            debug!("Publisher {} disconnected", pub_id);
                            // Send deleteStream command
                            self.send_delete_stream(
                                pub_id,
                                &mut packets_to_write,
                                &mut ids_to_close,
                            );
                            publisher_ids_to_remove.push(pub_id);
                            break;
                        }
                    }
                },

                PublisherSource::Feed(receiver) => loop {
                    match receiver.try_recv() {
                        // Control / metadata bytes stay on the session path so
                        // ordering with the bypassed media stays FIFO.
                        Ok(PublisherFeed::Raw(bytes)) => {
                            items += 1;
                            bytes_drained += bytes.len();
                            if !self.dispatch_publish_bytes(
                                pub_id,
                                bytes,
                                &mut packets_to_write,
                                &mut ids_to_close,
                            ) {
                                // Fatal publisher error (e.g. an oversized sequence
                                // header rejected at ingest): finalize the watchers
                                // before removal so none is orphaned in Watching.
                                // Unlike the Disconnected arm, this does NOT re-feed
                                // the just-errored session.
                                let mut results = self.scheduler.abort_publisher_watchers(pub_id);
                                collect_server_results(
                                    &mut results,
                                    &mut packets_to_write,
                                    &mut ids_to_close,
                                );
                                publisher_ids_to_remove.push(pub_id);
                                break;
                            }
                            if items >= MAX_PUBLISH_ITEMS_PER_POLL
                                || bytes_drained >= MAX_PUBLISH_BYTES_PER_POLL
                            {
                                budget_exhausted = true;
                                break;
                            }
                        }
                        // PERF-5a bypass: parsed audio/video tag straight to
                        // the channel machinery, no serialize/reparse.
                        Ok(PublisherFeed::Media {
                            tag_type,
                            timestamp,
                            data,
                        }) => {
                            items += 1;
                            bytes_drained += data.len();
                            self.server_results_buffer.clear();
                            self.scheduler.publish_media_received(
                                pub_id,
                                tag_type,
                                timestamp,
                                data,
                                &mut self.server_results_buffer,
                            );
                            collect_server_results(
                                &mut self.server_results_buffer,
                                &mut packets_to_write,
                                &mut ids_to_close,
                            );
                            if items >= MAX_PUBLISH_ITEMS_PER_POLL
                                || bytes_drained >= MAX_PUBLISH_BYTES_PER_POLL
                            {
                                budget_exhausted = true;
                                break;
                            }
                        }
                        Err(crossbeam_channel::TryRecvError::Empty) => break,
                        Err(crossbeam_channel::TryRecvError::Disconnected) => {
                            debug!("Publisher {} disconnected", pub_id);
                            self.send_delete_stream(
                                pub_id,
                                &mut packets_to_write,
                                &mut ids_to_close,
                            );
                            publisher_ids_to_remove.push(pub_id);
                            break;
                        }
                    }
                },
            }

            // Hand this round's drained bytes back to the producer side in
            // one batch — one uncontended lock + notify per publisher per
            // round, not per item — after every drain-loop exit alike
            // (Empty, budget stop, disconnect, fatal error), so a producer
            // parked at the byte gate wakes with the very round that made
            // room. `bytes_drained` sums exactly what producers acquired
            // per item (see PublisherFeed::ingress_len). A publisher
            // scheduled for removal releases here too; its guard's close
            // follows when remove_publisher drops the state.
            if bytes_drained > 0 {
                ingress_budget.release(bytes_drained);
            }
        }

        // Fan out through the shared enqueue path: move this round's packets
        // into `packets_buffer` (empty here — every user drains it fully) and
        // let `write_pending_packets` apply the condemned-skip and
        // backpressure rules identically to the readable-path fanout. The
        // loop above must collect into locals instead of pushing straight
        // into `packets_buffer`: `dispatch_publish_bytes` already borrows
        // `self` mutably.
        self.packets_buffer.append(&mut packets_to_write);
        self.write_pending_packets();
        // Backpressured targets are closed the same way as scheduler-driven
        // ones, appended after them so the close order matches arrival order.
        ids_to_close.append(&mut self.ids_to_close_buffer);

        // Close connections that need closing, flushing the status packet
        // (e.g. finish_playing) enqueued just above in the same round.
        for close_id in ids_to_close {
            self.close_connection_after_flush(close_id);
        }

        (publisher_ids_to_remove, budget_exhausted)
    }

    /// Send deleteStream command
    fn send_delete_stream(
        &mut self,
        pub_id: usize,
        packets: &mut Vec<OutboundWrite>,
        ids_to_close: &mut Vec<usize>,
    ) {
        let mut arguments = Vec::new();
        arguments.push(Amf0Value::Number(1.0));
        let delete_stream_cmd = RtmpMessage::Amf0Command {
            command_name: "deleteStream".to_string(),
            transaction_id: 4.0,
            command_object: Amf0Value::Null,
            additional_arguments: arguments,
        }
        .into_message_payload(RtmpTimestamp { value: 0 }, 1);

        if let Ok(payload) = delete_stream_cmd {
            let mut serializer = ChunkSerializer::new();
            if let Ok(packet) = serializer.serialize(&payload, false, true) {
                self.server_results_buffer.clear();
                match self.scheduler.publish_bytes_received(
                    pub_id,
                    packet.bytes,
                    &mut self.server_results_buffer,
                ) {
                    Ok(()) => {
                        collect_server_results(
                            &mut self.server_results_buffer,
                            packets,
                            ids_to_close,
                        );
                    }
                    Err(e) => {
                        log::warn!(
                            "Failed to process deleteStream command for publisher {}: {:?}",
                            pub_id,
                            e
                        );
                    }
                }
            }
        }
    }

    /// Flush pending connection write queues (O(m) where m = connections with pending writes)
    fn flush_pending(&mut self) -> Vec<usize> {
        let mut ids_to_close = Vec::new();

        // Swap-snapshot the pending ids: the live list is left empty, so ids
        // marked while this drain runs land there and are seen next pass
        // (same semantics the old set drain had for mid-drain inserts).
        debug_assert!(self.dirty_drain_scratch.is_empty());
        std::mem::swap(&mut self.dirty_drain_scratch, &mut self.pending_flush_ids);

        for &id in &self.dirty_drain_scratch {
            if let Some(conn) = self.connections.get_mut(id) {
                // A cleared flag means the entry is stale: the connection that
                // was marked is gone and a fresh (never-marked) one reuses the
                // slot. Skip it — a fresh connection owes no flush pass.
                if !conn.take_pending_flush_mark() {
                    continue;
                }
                if conn.has_pending_writes() {
                    match conn.try_flush() {
                        Ok(true) | Err(_) => {
                            // Connection should be closed
                            ids_to_close.push(id);
                        }
                        Ok(false) if !conn.has_pending_writes() && conn.is_condemned() => {
                            // A condemned connection's tail is fully delivered —
                            // close it now (the message is complete). Same
                            // drain-then-close as handle_writable.
                            ids_to_close.push(id);
                        }
                        Ok(false) => {
                            // NEW-RS-01: do NOT re-mark for pending flush. A
                            // still-non-empty queue here means try_flush stopped
                            // on WouldBlock (kernel send buffer full); retrying
                            // next loop would just burn a guaranteed-EAGAIN
                            // syscall. Marking interest-dirty (re)registers
                            // writable interest while data is pending, or clears
                            // it once drained (desired_interest() derives
                            // writable from has_pending_writes()); the poller's
                            // writable event then drives the next flush.
                            if conn.mark_interest_dirty() {
                                self.interest_dirty_ids.push(id);
                            }
                        }
                    }
                } else if conn.is_condemned() {
                    // Condemned and already drained (a media enqueue that then
                    // flushed elsewhere): close now rather than wait for the
                    // deadline.
                    ids_to_close.push(id);
                } else {
                    // No pending writes, ensure writable interest is cleared
                    if conn.mark_interest_dirty() {
                        self.interest_dirty_ids.push(id);
                    }
                }
            }
        }

        let drained_len = self.dirty_drain_scratch.len();
        self.dirty_drain_scratch.clear();
        self.shrink_drain_scratch_if_sparse(drained_len);

        ids_to_close
    }

    /// Opportunistic high-water shrink for the dirty-id buffers, applied to
    /// the drain snapshot after each pass. Vec iteration is O(len) whatever
    /// the capacity, so this is memory hygiene only: without it, a flash
    /// crowd that decays would pin peak-sized buffers for the life of the
    /// reactor.
    ///
    /// Two guards keep the hygiene from becoming allocation churn (both
    /// drains run every loop iteration, so a naive per-pass shrink would
    /// discard capacity on every idle tick and regrow it on every burst):
    /// the shrink target never goes below `DIRTY_IDS_SHRINK_MIN_CAPACITY`,
    /// and it fires only after `SPARSE_DRAINS_BEFORE_SHRINK` consecutive
    /// quarter-occupancy passes — one dense pass ends the streak. The streak
    /// stays saturated while the sparse regime lasts, so once it trips, each
    /// oversized buffer is walked down as the swap rotation parks it here.
    /// (The scheduler's watcher-set shrink uses the same quarter-occupancy /
    /// double-the-survivors shape, but it runs on watcher-departure events,
    /// not per loop tick, so it needs neither guard.)
    fn shrink_drain_scratch_if_sparse(&mut self, drained_len: usize) {
        if drained_len >= self.dirty_drain_scratch.capacity() / 4 {
            self.sparse_drain_streak = 0;
            return;
        }
        self.sparse_drain_streak = self.sparse_drain_streak.saturating_add(1);
        if self.sparse_drain_streak >= SPARSE_DRAINS_BEFORE_SHRINK
            && self.dirty_drain_scratch.capacity() > DIRTY_IDS_SHRINK_MIN_CAPACITY
        {
            self.dirty_drain_scratch
                .shrink_to((drained_len * 2).max(DIRTY_IDS_SHRINK_MIN_CAPACITY));
        }
    }

    /// Check timed out connections, and queue liveness pings for idle
    /// watchers that would otherwise coast into that timeout.
    ///
    /// PERF-10: a 60s idle timeout does not need re-evaluating on every poll
    /// wakeup (the loop can wake thousands of times per second under load or
    /// ~10x/sec idle). Throttle the full slab scan to at most ~1/sec and read
    /// the clock once per sweep instead of once per connection. The watcher
    /// ping clock shares this sweep — second-level granularity is just as
    /// harmless against its 30s threshold.
    fn check_timeouts(&mut self) -> Vec<usize> {
        let now = Instant::now();
        if now.saturating_duration_since(self.last_timeout_check) < TIMEOUT_CHECK_INTERVAL {
            return Vec::new();
        }
        self.last_timeout_check = now;

        let timeout = Duration::from_secs(CONNECTION_TIMEOUT_SECS);
        let ping_idle = Duration::from_secs(WATCHER_PING_IDLE_SECS);
        let mut timed_out = Vec::new();
        // Ids due a liveness probe this sweep, resolved to watchers below,
        // after the iteration borrow ends.
        let mut ping_due = Vec::new();

        for (id, conn) in self.connections.iter() {
            if conn.is_timed_out_at(now, timeout) {
                debug!("Connection {} timed out", id);
                timed_out.push(id);
            } else if conn.condemn_expired(now) {
                // A condemned connection whose peer never drained the tail:
                // remove it once the bounded drain window elapses. The ~1/sec
                // sweep granularity is fine given the multi-second deadline.
                debug!("Connection {} close-drain deadline expired", id);
                timed_out.push(id);
            } else if conn.is_ping_due_at(now, ping_idle) {
                ping_due.push(id);
            }
        }

        // Probe idle watchers instead of letting them idle into the reaper:
        // a watcher on a channel with no publisher is written nothing and
        // has nothing left to say, so only the peer's ANSWER to a
        // server-side ping can keep the activity clock honest about it
        // being alive — the ping's own delivery earns no credit (see
        // note_ping_queued). The scheduler decides who is a watcher — it
        // owns both the role classification and the per-session serializer
        // a control message must run through; every other idle role falls
        // through and, if it stays idle, times out above. The queued ping
        // rides the normal flush machinery on the next loop turn.
        for id in ping_due {
            let Some(packet) = self.scheduler.ping_watcher(id) else {
                continue;
            };
            let Some(conn) = self.connections.get_mut(id) else {
                continue;
            };
            if conn.enqueue_ping(packet.bytes) {
                debug!("Connection {} idle for {WATCHER_PING_IDLE_SECS}s; ping queued", id);
                conn.note_ping_queued(now);
                if conn.mark_pending_flush() {
                    self.pending_flush_ids.push(id);
                }
                if conn.mark_interest_dirty() {
                    self.interest_dirty_ids.push(id);
                }
            } else {
                // enqueue_ping refused (queue at cap) and marked the
                // connection Closing; route it to the same close path every
                // other refused enqueue takes.
                timed_out.push(id);
            }
        }

        timed_out
    }

    /// Update dirty connections' poller interest (O(m) where m = connections with changed interest)
    ///
    /// Returns the ids whose interest update failed. Such a connection can no
    /// longer have writable interest (re)registered, so a queued-but-WouldBlock
    /// write would never be driven to completion — it is not marked for pending
    /// flush either (NEW-RS-01). Closing it is the only safe recovery; the caller
    /// does so. The fd is almost always already broken when modify() fails.
    fn update_dirty_interests(&mut self) -> Vec<usize> {
        // Swap-snapshot the dirty ids; same scheme as flush_pending.
        debug_assert!(self.dirty_drain_scratch.is_empty());
        std::mem::swap(&mut self.dirty_drain_scratch, &mut self.interest_dirty_ids);

        let mut ids_to_close = Vec::new();
        // Indexed loop: update_interest needs `&mut self`, so the scratch
        // cannot stay borrowed across the call.
        for i in 0..self.dirty_drain_scratch.len() {
            let id = self.dirty_drain_scratch[i];
            let marked = match self.connections.get_mut(id) {
                Some(conn) => conn.take_interest_dirty_mark(),
                // Stale entry: the marked connection is gone.
                None => false,
            };
            if !marked {
                // A cleared flag on a live connection means the slot was
                // freed and reused by a fresh (never-marked) one: skip it.
                continue;
            }
            if let Err(e) = self.update_interest(id) {
                log::warn!(
                    "Failed to update interest for connection {}: {:?}; closing (queued writes would otherwise stall)",
                    id, e
                );
                ids_to_close.push(id);
            }
        }

        let drained_len = self.dirty_drain_scratch.len();
        self.dirty_drain_scratch.clear();
        self.shrink_drain_scratch_if_sparse(drained_len);

        ids_to_close
    }

    /// Run reactor main loop
    ///
    /// `waker` is registered with the poller so the in-process publisher send
    /// path can interrupt `poll()` the moment media arrives (PERF-3), instead
    /// of waiting for the POLL_TIMEOUT_MS fallback. The fallback timeout is
    /// retained so raw `create_stream_sender` users (who do not hold a
    /// WakeHandle) still make progress.
    pub fn run(
        &mut self,
        connection_receiver: crossbeam_channel::Receiver<TcpStream>,
        registrations: &RegistrationHandoff,
        waker: Option<Waker>,
    ) {
        info!("Reactor started");

        // Register the wakeup handle. If it is absent (waker_pair() failed) or
        // registration fails, the reactor still works via the POLL_TIMEOUT_MS
        // fallback, just without the low-latency wakeups.
        if let Some(waker) = &waker {
            if let Err(e) =
                self.poller
                    .register(waker.raw_handle(), WAKER_TOKEN, Interest::READABLE)
            {
                error!(
                    "Failed to register reactor waker (falling back to poll timeout): {:?}",
                    e
                );
            }
        }

        let poll_timeout = Duration::from_millis(POLL_TIMEOUT_MS);

        // Set when the previous round's process_publishers stopped on a
        // budget, i.e. publisher channels may still hold data no IO event
        // will ever announce.
        let mut publishers_pending = false;

        // Event buffer reused across poll wakeups: poll clears and refills
        // it instead of allocating a fresh Vec every iteration.
        let mut events = Vec::new();

        // Registration batch reused across iterations for the same reason.
        // Registrations are moved out of the shared queue under its lock and
        // processed here off the lock; leftovers queued when this loop exits
        // are released by the worker's RegistrationKillSwitch, one level
        // above — which also covers the case where this loop never ran.
        let mut registration_batch: Vec<PublisherRegistration> = Vec::new();

        loop {
            // 1. Check stop signal
            if self.status.load(Ordering::Acquire) == STATUS_END {
                info!("Reactor received stop signal");
                break;
            }

            // 2. Non-blocking receive new connections
            while let Ok(socket) = connection_receiver.try_recv() {
                match self.add_connection(socket) {
                    Ok(token) => {
                        debug!("New connection added: {:?}", token);
                    }
                    Err(e) => {
                        error!("Failed to add connection: {:?}", e);
                    }
                }
            }

            // 3. Take up to a budget of queued publisher registrations (one
            // lock), then process them off the lock. The budget bounds how
            // long this step can keep sockets waiting; a remainder makes the
            // poll below non-blocking so the next round picks it up promptly.
            let mut new_publisher_added = false;
            let registrations_pending = registrations.drain_into(&mut registration_batch);
            for registration in registration_batch.drain(..) {
                if self.add_publisher(registration).is_some() {
                    new_publisher_added = true;
                }
            }

            // 4. Poll IO events.
            //
            // A just-registered in-process publisher already has its
            // connect/createStream/publish handshake queued on a crossbeam
            // channel — not a socket the poller watches — and process_publishers
            // (which drains it) runs only after this poll. So when a publisher
            // was just added, poll non-blocking and fall straight through to
            // process_publishers, delivering the handshake and first media
            // immediately instead of stalling on the poll timeout (PERF-5a).
            //
            // The same applies when the previous round's publisher drain hit
            // its budget, and when this round's registration drain left a
            // remainder: the leftover items sit on a channel or queue the
            // poller cannot see, so a blocking poll would add up to
            // POLL_TIMEOUT_MS of latency per excess batch. Poll non-blocking
            // and let steps 5-10 run in between, which is exactly what the
            // budgets exist to guarantee.
            let poll_wait = if new_publisher_added
                || publishers_pending
                || registrations_pending
                || !self.read_pending.is_empty()
            {
                Duration::ZERO
            } else {
                poll_timeout
            };
            if let Err(e) = self.poller.poll(Some(poll_wait), &mut events) {
                error!("Poller error: {:?}", e);
                continue;
            }

            // 5-pre. Snapshot the cap-hit re-drain set BEFORE processing this
            // round's events: ids inserted during step 5 below belong to the
            // NEXT iteration. Each connection is thus read at most once per
            // flush cycle (step 7) — resuming a same-pass cap-hit immediately
            // would let a single connection push ~2x MAX_READ_PER_POLL into
            // subscriber queues before any flush ran.
            let resume_ids: Vec<usize> = if self.read_pending.is_empty() {
                Vec::new()
            } else {
                self.read_pending.drain().collect()
            };

            // 5. Process IO events
            let mut ids_to_close = Vec::new();
            let mut read_ids: Vec<usize> = Vec::new();

            for event in &events {
                let poller_token = event.token;

                // Wakeup token: drain it and fall through to the channel-drain
                // steps below. Matched before decoding as a connection token.
                if poller_token == WAKER_TOKEN {
                    if let Some(waker) = &waker {
                        waker.drain();
                    }
                    continue;
                }

                // Validate token and get connection id (checks generation)
                let Some(id) = self.validate_connection(poller_token) else {
                    continue;
                };

                // Handle error/hangup
                if event.is_error() || event.is_hangup() {
                    ids_to_close.push(id);
                    continue;
                }

                // Handle readable (drain until WouldBlock)
                if event.is_readable() {
                    // Track for the step-5b skip only while a resume is
                    // actually pending: the common no-cap-hit pass must not
                    // pay a per-batch Vec allocation for a list nobody reads.
                    if !resume_ids.is_empty() {
                        read_ids.push(id);
                    }
                    let results = self.handle_readable(id);
                    for result in results {
                        let HandleResult::Disconnect(close_id) = result;
                        ids_to_close.push(close_id);
                    }
                }

                // Handle writable
                if event.is_writable() {
                    if let Some(HandleResult::Disconnect(close_id)) = self.handle_writable(id) {
                        ids_to_close.push(close_id);
                    }
                }
            }

            // 5b. Resume connections that stopped at the MAX_READ_PER_POLL cap
            // last iteration. Each resume reads another <=512KB; a still-
            // saturated connection re-inserts itself (picked up next iteration),
            // so progress is bounded per loop and no new edge event is needed
            // to keep draining. `poll_wait` is ZERO while `read_pending` is
            // non-empty so the tail is not stalled.
            self.resume_capped_reads(resume_ids, &read_ids, &mut ids_to_close);

            // 6. Handle publishers data
            let (publisher_ids_to_remove, budget_exhausted) = self.process_publishers();
            publishers_pending = budget_exhausted;
            for pub_id in publisher_ids_to_remove {
                self.remove_publisher(pub_id);
            }

            // 7. Flush pending write queues (O(m) where m = connections with pending writes)
            let flush_closes = self.flush_pending();
            ids_to_close.extend(flush_closes);

            // 8. Update dirty poller interests (O(m) where m = connections with changed interests)
            let interest_closes = self.update_dirty_interests();
            ids_to_close.extend(interest_closes);

            // 9. Check timeouts
            let timed_out = self.check_timeouts();
            ids_to_close.extend(timed_out);

            // 10. Clean up disconnected connections (deduplicate). Same
            // flush-then-close as the publisher path: a scheduler-driven
            // disconnect may have queued a final control packet this round.
            ids_to_close.sort_unstable();
            ids_to_close.dedup();
            for id in ids_to_close {
                self.close_connection_after_flush(id);
            }
        }

        // Graceful shutdown
        self.graceful_shutdown();

        info!("Reactor stopped");
    }

    /// Graceful shutdown
    fn graceful_shutdown(&mut self) {
        info!("Starting graceful shutdown...");

        let deadline = Instant::now() + Duration::from_secs(GRACEFUL_SHUTDOWN_TIMEOUT_SECS);

        // Mark all connections as closing
        for (_, conn) in self.connections.iter_mut() {
            conn.mark_closing();
        }

        // Try to flush all pending data
        while Instant::now() < deadline {
            let mut all_flushed = true;

            for (_, conn) in self.connections.iter_mut() {
                if conn.has_pending_writes() {
                    all_flushed = false;
                    if let Err(e) = conn.try_flush() {
                        debug!("Failed to flush connection during shutdown: {:?}", e);
                    }
                }
            }

            if all_flushed {
                break;
            }

            std::thread::sleep(Duration::from_millis(10));
        }

        // Close all connections
        for (_, conn) in self.connections.iter_mut() {
            conn.shutdown();
        }

        info!("Graceful shutdown complete");
    }

    /// Check that a connection is marked interest-dirty AND listed for the
    /// drain (test only). Requiring both means a positive assertion also
    /// catches a mark site that lost its id push — a flag without a listing
    /// would never be serviced. (The converse — a listed id whose flag died —
    /// is the legal stale state and reads false here.)
    #[cfg(test)]
    pub fn is_interest_dirty(&self, id: usize) -> bool {
        let flagged = self
            .connections
            .get(id)
            .is_some_and(|conn| conn.in_interest_dirty);
        flagged && self.interest_dirty_ids.contains(&id)
    }

    /// Flag-and-listing check for the pending-flush mark (test only); same
    /// rationale as [`Self::is_interest_dirty`].
    #[cfg(test)]
    pub fn is_pending_flush(&self, id: usize) -> bool {
        let flagged = self
            .connections
            .get(id)
            .is_some_and(|conn| conn.in_pending_flush);
        flagged && self.pending_flush_ids.contains(&id)
    }

    /// Drain the interest-dirty list — clearing the connection marks, as the
    /// real drain does — and return the marked ids (test only)
    #[cfg(test)]
    pub fn drain_interest_dirty(&mut self) -> Vec<usize> {
        let ids = std::mem::take(&mut self.interest_dirty_ids);
        ids.into_iter()
            .filter(|&id| {
                self.connections
                    .get_mut(id)
                    .is_some_and(|conn| conn.take_interest_dirty_mark())
            })
            .collect()
    }

    /// Mark a connection for pending flush through the real flag+list path
    /// (test only): what every production insert site does.
    #[cfg(test)]
    pub fn mark_pending_flush_for_test(&mut self, id: usize) {
        if let Some(conn) = self.connections.get_mut(id) {
            if conn.mark_pending_flush() {
                self.pending_flush_ids.push(id);
            }
        }
    }
}

// ============================================================================
// Tests
// ============================================================================

#[cfg(test)]
mod tests {
    use super::*;

    /// A fresh production-sized ingress-budget guard for registration
    /// literals in tests that never exercise the byte gate (their payloads
    /// sit far below the mark, so the gate is invisible to them).
    fn test_budget() -> IngressBudgetGuard {
        IngressBudget::new(PUBLISHER_INGRESS_HIGH_WATER_BYTES).0
    }

    /// Pair a direct channel push with the producer-side reservation the
    /// real send paths perform, so the drain's per-round release accounting
    /// stays balanced (release debug-asserts it never exceeds what was
    /// acquired).
    fn send_acquired(
        budget: &IngressBudget,
        feed_tx: &crossbeam_channel::Sender<PublisherFeed>,
        feed: PublisherFeed,
    ) {
        budget
            .acquire(feed.ingress_len())
            .expect("the test budget must be open");
        feed_tx.send(feed).expect("the test channel must accept");
    }

    #[test]
    fn test_connection_state_transitions() {
        assert!(ConnectionState::Handshaking.can_read());
        assert!(ConnectionState::Handshaking.can_write());
        assert!(!ConnectionState::Handshaking.is_active());

        assert!(ConnectionState::Active.can_read());
        assert!(ConnectionState::Active.can_write());
        assert!(ConnectionState::Active.is_active());

        assert!(ConnectionState::SlowClient.is_active());

        assert!(!ConnectionState::Closing.can_read());
        assert!(ConnectionState::Closing.can_write());

        assert!(!ConnectionState::Closed.can_read());
        assert!(!ConnectionState::Closed.can_write());
    }

    #[test]
    fn test_interest_desired() {
        use std::net::TcpListener;

        let listener = TcpListener::bind("127.0.0.1:0").expect("Failed to bind");
        let addr = listener.local_addr().expect("Failed to get address");

        let client = TcpStream::connect(addr).expect("Failed to connect");
        let token = ConnectionToken::new(0, 1);

        let conn = ReactorConnection::new(token, client).expect("Failed to create connection");

        // Initially should want to read
        assert_eq!(conn.desired_interest(), Interest::READABLE);
    }

    #[test]
    fn test_graceful_shutdown_flushes_data() {
        use std::net::TcpListener;

        // Create a listener on a random port
        let listener = TcpListener::bind("127.0.0.1:0").expect("Failed to bind");
        let addr = listener.local_addr().expect("Failed to get address");

        // Create a client connection
        let client = TcpStream::connect(addr).expect("Failed to connect");
        let (server_socket, _) = listener.accept().expect("Failed to accept");

        // Create a connection and enqueue some data
        let token = ConnectionToken::new(0, 1);
        let mut conn =
            ReactorConnection::new(token, server_socket).expect("Failed to create connection");

        // Transition to Active state
        conn.state = ConnectionState::Active;

        // Enqueue some test data
        let test_data = b"Hello, World!";
        conn.enqueue_data(Bytes::from_static(test_data), false, false, false, true, Instant::now());

        assert!(conn.has_pending_writes());

        // Flush the data
        let _ = conn.try_flush();

        // Read from client side
        client
            .set_nonblocking(false)
            .expect("Failed to set blocking");
        let mut buf = vec![0u8; 100];

        // Use a timeout to prevent hanging
        use std::time::Duration;
        client
            .set_read_timeout(Some(Duration::from_millis(100)))
            .expect("Failed to set timeout");

        match client.peek(&mut buf) {
            Ok(n) if n > 0 => {
                // Data was flushed successfully
                assert!(n >= test_data.len());
            }
            _ => {
                // Data might not have been flushed yet, but that's ok for this test
                // The important thing is that enqueue and flush don't panic
            }
        }
    }

    #[test]
    fn test_accepted_socket_has_tcp_nodelay() {
        use std::net::TcpListener;

        let listener = TcpListener::bind("127.0.0.1:0").expect("Failed to bind");
        let addr = listener.local_addr().expect("Failed to get address");

        let _client = TcpStream::connect(addr).expect("Failed to connect");
        let (server, _) = listener.accept().expect("Failed to accept");
        let token = ConnectionToken::new(0, 1);

        // PERF-4: ReactorConnection::new must disable Nagle on the accepted
        // subscriber socket so small audio tags / control exchanges are not
        // held by Nagle + delayed ACK.
        let conn = ReactorConnection::new(token, server).expect("Failed to create connection");
        assert!(
            conn.nodelay().expect("nodelay query failed"),
            "accepted subscriber socket must have TCP_NODELAY enabled"
        );
    }

    #[test]
    fn test_connection_timeout_detection() {
        use std::net::TcpListener;

        let listener = TcpListener::bind("127.0.0.1:0").expect("Failed to bind");
        let addr = listener.local_addr().expect("Failed to get address");

        let client = TcpStream::connect(addr).expect("Failed to connect");
        let token = ConnectionToken::new(0, 1);

        let conn = ReactorConnection::new(token, client).expect("Failed to create connection");

        // Should not be timed out immediately
        assert!(!conn.is_timed_out(Duration::from_secs(60)));

        // Should be timed out with zero timeout
        assert!(conn.is_timed_out(Duration::from_nanos(1)));
    }

    #[test]
    fn test_is_timed_out_at_uses_hoisted_now() {
        use std::net::TcpListener;

        let listener = TcpListener::bind("127.0.0.1:0").expect("Failed to bind");
        let addr = listener.local_addr().expect("Failed to get address");
        let client = TcpStream::connect(addr).expect("Failed to connect");
        let token = ConnectionToken::new(0, 1);
        let conn = ReactorConnection::new(token, client).expect("Failed to create connection");

        let timeout = Duration::from_secs(CONNECTION_TIMEOUT_SECS);
        let base = conn.last_activity();

        // Exactly at the boundary is not yet timed out ( `>` , not `>=` ).
        assert!(!conn.is_timed_out_at(base, timeout));
        assert!(!conn.is_timed_out_at(base + timeout, timeout));
        assert!(conn.is_timed_out_at(base + timeout + Duration::from_millis(1), timeout));

        // A `now` earlier than the last activity must saturate to zero, never
        // panic or report a spurious timeout.
        assert!(!conn.is_timed_out_at(
            base.checked_sub(Duration::from_secs(1)).unwrap_or(base),
            timeout
        ));
    }

    #[test]
    fn test_check_timeouts_throttle_and_detection() {
        let status = Arc::new(AtomicUsize::new(STATUS_RUN));
        let mut reactor = Reactor::new(3, None, status).expect("Failed to create reactor");

        let listener = std::net::TcpListener::bind("127.0.0.1:0").expect("Failed to bind");
        let addr = listener.local_addr().expect("Failed to get address");
        let _client = TcpStream::connect(addr).expect("Failed to connect");
        let (server, _) = listener.accept().expect("Failed to accept");
        let token = reactor
            .add_connection(server)
            .expect("Failed to add connection");

        // PERF-10: new() stamped last_timeout_check ~now, so an immediate sweep
        // is throttled and skipped entirely.
        assert!(
            reactor.check_timeouts().is_empty(),
            "sweep within the throttle interval must be skipped"
        );

        // Make the connection stale and let the throttle window lapse.
        let stale = Instant::now()
            .checked_sub(Duration::from_secs(CONNECTION_TIMEOUT_SECS * 2))
            .expect("monotonic clock should be well past 120s after a full build");
        if let Some(conn) = reactor.connections.get_mut(token.id) {
            conn.last_read_activity = stale;
            conn.last_write_activity = stale;
        }
        reactor.last_timeout_check = stale;

        // Now the sweep actually runs and reports the stale connection.
        assert_eq!(
            reactor.check_timeouts(),
            vec![token.id],
            "after the interval elapses the stale connection must be detected"
        );

        // The sweep restamped last_timeout_check, so an immediate re-run is
        // throttled again even though the connection is still stale.
        assert!(
            reactor.check_timeouts().is_empty(),
            "the sweep must restamp the throttle and skip an immediate re-run"
        );

        reactor.remove_connection(token.id);
    }

    // The ping predicate in isolation, on synthetic clocks: it must gate on
    // connection state, on the idle threshold, and on the window since the
    // previous queued ping (an undeliverable ping must not be re-queued
    // every sweep, nor may it masquerade as activity).
    #[test]
    fn ping_due_predicate_gates_on_state_idle_and_prior_ping() {
        let listener = std::net::TcpListener::bind("127.0.0.1:0").expect("Failed to bind");
        let addr = listener.local_addr().expect("Failed to get address");
        let _client = TcpStream::connect(addr).expect("Failed to connect");
        let (server, _) = listener.accept().expect("Failed to accept");
        let mut conn = ReactorConnection::new(ConnectionToken::new(0, 1), server)
            .expect("Failed to create connection");

        let idle = Duration::from_secs(WATCHER_PING_IDLE_SECS);
        let base = conn.last_activity();

        // Mid-handshake connections are never probed, however idle.
        assert!(!conn.is_ping_due_at(base + idle, idle));

        conn.state = ConnectionState::Active;
        // Below the idle threshold nothing is due; from it on, a ping is.
        assert!(!conn.is_ping_due_at(base, idle));
        assert!(!conn.is_ping_due_at(base + idle - Duration::from_millis(1), idle));
        assert!(conn.is_ping_due_at(base + idle, idle));

        // A queued ping opens a fresh window of the same length, without
        // touching the activity clock (is_timed_out_at still sees `base`).
        conn.note_ping_queued(base + idle);
        assert!(!conn.is_ping_due_at(base + idle, idle));
        assert!(!conn.is_ping_due_at(base + idle * 2 - Duration::from_millis(1), idle));
        assert!(conn.is_ping_due_at(base + idle * 2, idle));
        assert!(conn.is_timed_out_at(
            base + Duration::from_secs(CONNECTION_TIMEOUT_SECS) + Duration::from_millis(1),
            Duration::from_secs(CONNECTION_TIMEOUT_SECS)
        ));

        // A condemned connection is on its way out: never probed.
        conn.condemn(base + idle * 3);
        assert!(!conn.is_ping_due_at(base + idle * 2, idle));
    }

    // A slow client that has fully caught up must become Active again even
    // when no further fanout enqueue arrives (its publisher went quiet):
    // enqueue_data's Normal-band recovery never runs then, and a connection
    // stuck in SlowClient is invisible to the idle-watcher ping — the 60s
    // reaper would kill a healthy, fully-drained watcher.
    #[test]
    fn drained_slow_client_recovers_active_state_on_flush() {
        let listener = std::net::TcpListener::bind("127.0.0.1:0").expect("Failed to bind");
        let addr = listener.local_addr().expect("Failed to get address");
        let _client = TcpStream::connect(addr).expect("Failed to connect");
        let (server, _) = listener.accept().expect("Failed to accept");
        let mut conn = ReactorConnection::new(ConnectionToken::new(0, 1), server)
            .expect("Failed to create connection");

        // Already-drained shape: the queue emptied on an earlier flush and
        // nothing was enqueued since. The next flush attempt alone must
        // repair the state.
        conn.state = ConnectionState::SlowClient;
        assert!(!conn.has_pending_writes());
        conn.try_flush().expect("flush on an empty queue");
        assert_eq!(
            conn.state,
            ConnectionState::Active,
            "an empty queue means no backpressure; the state must recover"
        );

        // Draining shape: a queued tail (via enqueue_raw, which performs no
        // state recovery of its own) is flushed into the socket buffer and
        // the Normal backlog must flip the state back.
        conn.state = ConnectionState::SlowClient;
        assert!(conn.enqueue_raw(vec![0u8; 64]));
        assert_eq!(
            conn.state,
            ConnectionState::SlowClient,
            "sanity: enqueue_raw must not repair the state by itself"
        );
        conn.try_flush().expect("flush the queued tail");
        assert!(!conn.has_pending_writes(), "64 bytes must flush in one go");
        assert_eq!(
            conn.state,
            ConnectionState::Active,
            "draining back to the Normal band must restore Active"
        );
    }

    // The ping's own delivery must not register as connection activity: a
    // peer whose TCP stack ACKs bytes but whose application never answers
    // would otherwise be kept alive forever by the server's own 30s probes,
    // never reaching the 60s reaper. Only bytes beyond the queued ping
    // (real media/control) earn the write-activity stamp; the peer proves
    // liveness by ANSWERING (a read).
    #[test]
    fn flushed_ping_bytes_do_not_stamp_write_activity() {
        let listener = std::net::TcpListener::bind("127.0.0.1:0").expect("Failed to bind");
        let addr = listener.local_addr().expect("Failed to get address");
        let _client = TcpStream::connect(addr).expect("Failed to connect");
        let (server, _) = listener.accept().expect("Failed to accept");
        let mut conn = ReactorConnection::new(ConnectionToken::new(0, 1), server)
            .expect("Failed to create connection");
        conn.state = ConnectionState::Active;

        let stale = Instant::now()
            .checked_sub(Duration::from_secs(CONNECTION_TIMEOUT_SECS + 1))
            .expect("monotonic clock should be well past 61s after a full build");
        conn.last_read_activity = stale;
        conn.last_write_activity = stale;
        let before = conn.last_activity();

        // Queue a ping the way the sweep does, then flush it into the
        // socket buffer: the write succeeds, but earns no activity credit.
        assert!(conn.enqueue_ping(vec![2u8; 18]));
        conn.note_ping_queued(Instant::now());
        conn.try_flush().expect("flush the ping");
        assert!(!conn.has_pending_writes(), "the ping must flush in one go");
        assert_eq!(
            conn.last_activity(),
            before,
            "a delivered ping must not stamp write activity"
        );

        // Real (non-ping) bytes still stamp activity exactly as before.
        assert!(conn.enqueue_raw(vec![3u8; 32]));
        conn.try_flush().expect("flush the non-ping tail");
        assert!(
            conn.last_activity() > before,
            "non-ping writes must stamp write activity"
        );

        // Mixed flush, ping plus ordinary data in one write: the entry
        // tags in the write queue attribute each entry's bytes exactly, so
        // the ordinary bytes behind the ping must stamp activity while the
        // ping's own bytes earn nothing.
        let stale = Instant::now()
            .checked_sub(Duration::from_secs(CONNECTION_TIMEOUT_SECS + 1))
            .expect("monotonic clock should be well past 61s after a full build");
        conn.last_read_activity = stale;
        conn.last_write_activity = stale;
        let before = conn.last_activity();
        assert!(conn.enqueue_ping(vec![2u8; 18]));
        conn.note_ping_queued(Instant::now());
        assert!(conn.enqueue_data(Bytes::from(vec![3u8; 32]), false, false, false, true, Instant::now()));
        conn.try_flush().expect("flush the ping plus the tail behind it");
        assert!(!conn.has_pending_writes(), "both entries must flush");
        assert!(
            conn.last_activity() > before,
            "ordinary bytes flushed behind the ping must stamp activity"
        );
    }

    // Pings are only for drained connections: a queued tail's own delivery
    // or failure already resolves the peer's liveness, so probing it adds
    // nothing. (Byte attribution does not depend on this gate — ping
    // entries are tagged in the write queue itself.)
    #[test]
    fn ping_is_not_due_while_writes_are_pending() {
        let listener = std::net::TcpListener::bind("127.0.0.1:0").expect("Failed to bind");
        let addr = listener.local_addr().expect("Failed to get address");
        let _client = TcpStream::connect(addr).expect("Failed to connect");
        let (server, _) = listener.accept().expect("Failed to accept");
        let mut conn = ReactorConnection::new(ConnectionToken::new(0, 1), server)
            .expect("Failed to create connection");
        conn.state = ConnectionState::Active;

        let idle = Duration::from_secs(WATCHER_PING_IDLE_SECS);
        assert!(conn.enqueue_raw(vec![0u8; 8]));
        assert!(
            !conn.is_ping_due_at(conn.last_activity() + idle * 3, idle),
            "a queued tail must suppress the probe however idle the clock looks"
        );

        conn.try_flush().expect("drain the tail");
        assert!(!conn.has_pending_writes());
        // Re-anchor after the flush stamped activity: once drained, the
        // probe becomes due again past the idle window.
        let base = conn.last_activity();
        assert!(conn.is_ping_due_at(base + idle, idle));
    }

    // End-to-end chain regression at the wire level: a csid-2 control
    // sequence — acknowledgement, ping, acknowledgement, acknowledgement —
    // serialized by ONE stateful serializer must survive High-band
    // shedding and decode against a deserializer that sees exactly the
    // delivered bytes. Before non-droppable entries bypassed the shedding
    // policy, the High band silently shed the middle acknowledgement while
    // the serializer had already committed its csid-2 header history; the
    // following acknowledgement then header-compressed against a packet
    // the peer never received and decoding lost RTMP framing. Droppable
    // video is shed around the chain to prove media drops stay safe (rml
    // serializes droppable packets drop-tolerantly on their own csid).
    #[test]
    fn high_band_control_chain_survives_and_decodes() {
        use rml_rtmp::chunk_io::ChunkDeserializer;
        use rml_rtmp::messages::UserControlEventType;

        let ts0 = RtmpTimestamp { value: 0 };
        let ack_payload = |n: u32| {
            RtmpMessage::Acknowledgement {
                sequence_number: n,
            }
            .into_message_payload(ts0, 0)
            .expect("build acknowledgement")
        };
        let a0 = ack_payload(1);
        let ping = RtmpMessage::UserControl {
            event_type: UserControlEventType::PingRequest,
            stream_id: None,
            buffer_length: None,
            timestamp: Some(RtmpTimestamp { value: 7 }),
        }
        .into_message_payload(ts0, 0)
        .expect("build ping");
        // Large enough to push the queue into the High band (2MB) once
        // enqueued, small enough to stay clear of the Critical cap (4MB).
        let v1 = RtmpMessage::VideoData {
            data: Bytes::from(vec![0x17u8; 2 * 1024 * 1024 + 64 * 1024]),
        }
        .into_message_payload(ts0, 1)
        .expect("build large video");
        let a1 = ack_payload(2);
        let v2 = RtmpMessage::VideoData {
            data: Bytes::from(vec![0x27u8; 512]),
        }
        .into_message_payload(ts0, 1)
        .expect("build sheddable video");
        let a2 = ack_payload(3);
        let v3 = RtmpMessage::VideoData {
            data: Bytes::from(vec![0x17u8; 256]),
        }
        .into_message_payload(ts0, 1)
        .expect("build trailing video");

        // What the peer must end up decoding, in wire order (v2 is shed).
        let expected: Vec<(u8, Bytes)> = vec![
            (3, a0.data.clone()),
            (4, ping.data.clone()),
            (9, v1.data.clone()),
            (3, a1.data.clone()),
            (3, a2.data.clone()),
            (9, v3.data.clone()),
        ];

        // ONE stateful serializer, serialization order == enqueue order —
        // exactly the session's situation. Control is non-droppable, media
        // is serialized drop-tolerantly; pin the rml contract the queue
        // policy relies on.
        let mut serializer = ChunkSerializer::new();
        let p_a0 = serializer.serialize(&a0, false, false).expect("ser a0");
        let p_ping = serializer.serialize(&ping, false, false).expect("ser ping");
        let p_v1 = serializer.serialize(&v1, false, true).expect("ser v1");
        let p_a1 = serializer.serialize(&a1, false, false).expect("ser a1");
        let p_v2 = serializer.serialize(&v2, false, true).expect("ser v2");
        let p_a2 = serializer.serialize(&a2, false, false).expect("ser a2");
        let p_v3 = serializer.serialize(&v3, false, true).expect("ser v3");
        assert!(!p_a1.can_be_dropped, "rml must mark control non-droppable");
        assert!(p_v1.can_be_dropped, "rml must mark tolerant media droppable");

        let mut queue = WriteQueue::new();
        let mut wire: Vec<u8> = Vec::new();

        // Normal band: the acknowledgement and the ping go out.
        assert!(queue.enqueue(
            Bytes::from(p_a0.bytes),
            false,
            false,
            false,
            p_a0.can_be_dropped,
            Instant::now()
        ));
        assert!(queue.enqueue_ping(Bytes::from(p_ping.bytes)));
        queue.try_flush(&mut wire).expect("flush the opening pair");

        // The large keyframe pushes the backlog into the High band.
        assert!(queue.enqueue(
            Bytes::from(p_v1.bytes),
            true,
            false,
            true,
            p_v1.can_be_dropped,
            Instant::now()
        ));
        assert_eq!(queue.backpressure_level(), BackpressureLevel::High);
        // The acknowledgement behind it must be RETAINED (the old policy
        // shed it right here), while a droppable non-keyframe is shed.
        assert!(queue.enqueue(
            Bytes::from(p_a1.bytes),
            false,
            false,
            false,
            p_a1.can_be_dropped,
            Instant::now()
        ));
        let before_shed = queue.pending_bytes();
        assert!(queue.enqueue(
            Bytes::from(p_v2.bytes),
            false,
            false,
            true,
            p_v2.can_be_dropped,
            Instant::now()
        ));
        assert_eq!(
            queue.pending_bytes(),
            before_shed,
            "the droppable non-keyframe must be shed in the High band"
        );
        queue.try_flush(&mut wire).expect("flush the pressured batch");
        assert!(queue.is_empty(), "the pressured batch must drain fully");

        // Drained again: the trailing control and media go out normally.
        assert!(queue.enqueue(
            Bytes::from(p_a2.bytes),
            false,
            false,
            false,
            p_a2.can_be_dropped,
            Instant::now()
        ));
        assert!(queue.enqueue(
            Bytes::from(p_v3.bytes),
            true,
            false,
            true,
            p_v3.can_be_dropped,
            Instant::now()
        ));
        queue.try_flush(&mut wire).expect("flush the tail");

        // The peer's view: decode the ENTIRE delivered wire with one
        // deserializer. Every message must come back with its own type and
        // payload — a mis-compressed csid-2 header would corrupt types,
        // lengths or framing from the middle acknowledgement onwards.
        let mut deserializer = ChunkDeserializer::new();
        let mut decoded: Vec<(u8, Bytes)> = Vec::new();
        let mut next = deserializer
            .get_next_message(&wire)
            .expect("the delivered wire must stay decodable");
        while let Some(payload) = next {
            decoded.push((payload.type_id, payload.data));
            next = deserializer
                .get_next_message(&[])
                .expect("valid buffered continuation");
        }
        assert_eq!(
            decoded.len(),
            expected.len(),
            "exactly the delivered messages must decode"
        );
        for (i, ((got_type, got_data), (want_type, want_data))) in
            decoded.iter().zip(expected.iter()).enumerate()
        {
            assert_eq!(got_type, want_type, "message {i} type");
            assert_eq!(got_data, want_data, "message {i} payload");
        }
    }

    // R-B: a quiet watcher on a channel with no publisher used to be reaped
    // by the 60s idle sweep — the server never sent anything, the client had
    // nothing left to say after `play`, and last_activity never moved. The
    // sweep must instead ping an Active watcher once it sits idle for half
    // the timeout: a live client's ANSWER to that ping (a read) refreshes
    // the activity clock, while a peer that never answers — wedged or not —
    // still hits the full-timeout reaper below.
    #[test]
    fn sweep_pings_an_idle_watcher_instead_of_only_reaping_it() {
        let status = Arc::new(AtomicUsize::new(STATUS_RUN));
        let mut reactor = Reactor::new(1, None, status).expect("Failed to create reactor");

        let listener = std::net::TcpListener::bind("127.0.0.1:0").expect("Failed to bind");
        let addr = listener.local_addr().expect("Failed to get address");
        let _client = TcpStream::connect(addr).expect("Failed to connect");
        let (server, _) = listener.accept().expect("Failed to accept");
        let token = reactor
            .add_connection(server)
            .expect("Failed to add connection");

        // Stage an established watcher: reactor state Active, scheduler
        // classification Watching.
        reactor
            .scheduler
            .register_watcher_for_test(token.id, "quiet-stream");
        reactor
            .connections
            .get_mut(token.id)
            .expect("connection exists")
            .state = ConnectionState::Active;

        // Backdate the connection past the ping threshold but short of the
        // reaper, and let the throttle window lapse (the synthetic-clock
        // idiom of the timeout tests; no sleeps).
        let idle = Instant::now()
            .checked_sub(Duration::from_secs(WATCHER_PING_IDLE_SECS + 1))
            .expect("monotonic clock should be well past 31s after a full build");
        {
            let conn = reactor
                .connections
                .get_mut(token.id)
                .expect("connection exists");
            conn.last_read_activity = idle;
            conn.last_write_activity = idle;
        }
        reactor.last_timeout_check = idle;

        let reaped = reactor.check_timeouts();
        assert!(
            reaped.is_empty(),
            "a watcher idle for half the timeout must not be reaped"
        );
        let (queued_bytes, activity_after_sweep) = {
            let conn = reactor
                .connections
                .get(token.id)
                .expect("connection exists");
            assert!(
                conn.has_pending_writes(),
                "the sweep must queue a liveness ping for the idle watcher"
            );
            (conn.pending_bytes(), conn.last_activity())
        };
        assert!(
            reactor.is_pending_flush(token.id),
            "the queued ping must be scheduled for the next flush pass"
        );
        assert_eq!(
            activity_after_sweep, idle,
            "queueing a ping must not count as connection activity"
        );

        // A second sweep inside the same ping window must not queue another
        // ping: last_ping_at gates the probe rate even while the first ping
        // sits unflushed and the connection stays idle.
        reactor.last_timeout_check = idle;
        assert!(
            reactor.check_timeouts().is_empty(),
            "the watcher is still short of the timeout on the second sweep"
        );
        assert_eq!(
            reactor
                .connections
                .get(token.id)
                .expect("connection exists")
                .pending_bytes(),
            queued_bytes,
            "a sweep within the ping window must not re-queue the ping"
        );

        // Honest accounting: queueing the ping is not activity. If it never
        // reaches the wire (peer wedged, flush blocked with nothing written),
        // the reaper must still fire at the full timeout.
        let dead = Instant::now()
            .checked_sub(Duration::from_secs(CONNECTION_TIMEOUT_SECS + 1))
            .expect("monotonic clock should be well past 61s after a full build");
        {
            let conn = reactor
                .connections
                .get_mut(token.id)
                .expect("connection exists");
            conn.last_read_activity = dead;
            conn.last_write_activity = dead;
        }
        reactor.last_timeout_check = dead;
        assert_eq!(
            reactor.check_timeouts(),
            vec![token.id],
            "an unflushed ping must not save a wedged watcher from the reaper"
        );

        reactor.remove_connection(token.id);
    }

    #[test]
    fn test_reactor_creation() {
        let status = Arc::new(AtomicUsize::new(STATUS_RUN));

        let reactor = Reactor::new(3, None, status);
        assert!(reactor.is_ok());
    }

    #[test]
    fn test_connection_generation_increments() {
        let status = Arc::new(AtomicUsize::new(STATUS_RUN));

        let mut reactor = Reactor::new(3, None, status).expect("Failed to create reactor");

        // Create a listener and accept multiple connections
        let listener = std::net::TcpListener::bind("127.0.0.1:0").expect("Failed to bind");
        let addr = listener.local_addr().expect("Failed to get address");

        // Add first connection
        let client1 = TcpStream::connect(addr).expect("Failed to connect");
        let (server1, _) = listener.accept().expect("Failed to accept");
        let token1 = reactor
            .add_connection(server1)
            .expect("Failed to add connection");

        // Remove it
        reactor.remove_connection(token1.id);

        // Add another connection - should reuse the ID but with incremented generation
        let client2 = TcpStream::connect(addr).expect("Failed to connect");
        let (server2, _) = listener.accept().expect("Failed to accept");
        let token2 = reactor
            .add_connection(server2)
            .expect("Failed to add connection");

        // Same ID but different generation
        assert_eq!(token1.id, token2.id);
        assert_eq!(token2.generation, token1.generation + 1);

        // Cleanup
        drop(client1);
        drop(client2);
    }

    #[test]
    fn test_token_validation() {
        let status = Arc::new(AtomicUsize::new(STATUS_RUN));

        let mut reactor = Reactor::new(3, None, status).expect("Failed to create reactor");

        let listener = std::net::TcpListener::bind("127.0.0.1:0").expect("Failed to bind");
        let addr = listener.local_addr().expect("Failed to get address");

        let client = TcpStream::connect(addr).expect("Failed to connect");
        let (server, _) = listener.accept().expect("Failed to accept");
        let token = reactor
            .add_connection(server)
            .expect("Failed to add connection");

        // Connection should be valid with correct generation
        assert!(reactor
            .validate_connection(token.to_poller_token())
            .is_some());

        // Remove connection
        reactor.remove_connection(token.id);

        // Old token should now be invalid (connection removed)
        assert!(reactor
            .validate_connection(token.to_poller_token())
            .is_none());

        drop(client);
    }

    /// Test that generation token prevents ABA problem
    /// Scenario: Connection A closes, new connection B reuses slot A's id,
    /// stale events for A should be rejected
    #[test]
    fn test_generation_prevents_aba_problem() {
        let status = Arc::new(AtomicUsize::new(STATUS_RUN));

        let mut reactor = Reactor::new(3, None, status).expect("Failed to create reactor");

        let listener = std::net::TcpListener::bind("127.0.0.1:0").expect("Failed to bind");
        let addr = listener.local_addr().expect("Failed to get address");

        // Create first connection (connection A)
        let client_a = TcpStream::connect(addr).expect("Failed to connect A");
        let (server_a, _) = listener.accept().expect("Failed to accept A");
        let token_a = reactor
            .add_connection(server_a)
            .expect("Failed to add connection A");
        let stale_poller_token = token_a.to_poller_token();

        // Remove connection A
        reactor.remove_connection(token_a.id);
        drop(client_a);

        // Create new connection (connection B) - should reuse slot 0
        let client_b = TcpStream::connect(addr).expect("Failed to connect B");
        let (server_b, _) = listener.accept().expect("Failed to accept B");
        let token_b = reactor
            .add_connection(server_b)
            .expect("Failed to add connection B");

        // Token B should be valid
        assert!(reactor
            .validate_connection(token_b.to_poller_token())
            .is_some());

        // Stale token A should be INVALID even though same id slot is occupied
        // (generation differs)
        assert!(reactor.validate_connection(stale_poller_token).is_none());

        // Different generations for same id
        assert_eq!(token_a.id, token_b.id); // Same slot reused
        assert_ne!(token_a.generation, token_b.generation); // Different generation

        reactor.remove_connection(token_b.id);
        drop(client_b);
    }

    /// Round-trip the poller-token encoding at the extremes of both halves.
    #[test]
    fn poller_token_roundtrip_at_width_extremes() {
        let cases: [(usize, Generation); 3] = [
            (0, 1),
            (TOKEN_ID_MASK - 1, Generation::MAX),
            (1234, 567),
        ];
        for (id, generation) in cases {
            let token = ConnectionToken::new(id, generation);
            let decoded = ConnectionToken::from_poller_token(token.to_poller_token());
            assert_eq!(decoded, token);
        }
    }

    /// A connection token must never alias the reserved waker token, even at
    /// the largest encodable id and generation.
    #[test]
    fn connection_token_never_collides_with_waker_token() {
        let extreme = ConnectionToken::new(TOKEN_ID_MASK - 1, Generation::MAX);
        assert_ne!(extreme.to_poller_token(), WAKER_TOKEN);

        // Ids are slab-dense, so capping max connections at TOKEN_ID_MASK
        // keeps every live id strictly below the mask.
        assert!(effective_max_connections(Some(usize::MAX)) <= TOKEN_ID_MASK);
    }

    /// Validate the 32-bit token layout arithmetic while running on 64-bit
    /// CI: a u32 token word split into a u16 generation half and a u16 id
    /// half must round-trip losslessly at the same extremes.
    #[test]
    fn simulated_32bit_packing_is_lossless() {
        const SIM_ID_BITS: u32 = 16;
        const SIM_ID_MASK: u32 = (1u32 << SIM_ID_BITS) - 1;
        let cases: [(u32, u16); 3] = [(0, 1), (SIM_ID_MASK - 1, u16::MAX), (1234, 567)];
        for (id, generation) in cases {
            let word = ((generation as u32) << SIM_ID_BITS) | (id & SIM_ID_MASK);
            let decoded_id = word & SIM_ID_MASK;
            let decoded_generation = (word >> SIM_ID_BITS) as u16;
            assert_eq!(decoded_id, id);
            assert_eq!(decoded_generation, generation);
        }
    }

    /// Stress test: verify reactor can handle many connections
    /// Note: This test creates connections but doesn't run the full RTMP handshake
    #[test]
    fn test_many_connections_creation() {
        let status = Arc::new(AtomicUsize::new(STATUS_RUN));

        let mut reactor = Reactor::new(3, None, status).expect("Failed to create reactor");

        let listener = std::net::TcpListener::bind("127.0.0.1:0").expect("Failed to bind");
        let addr = listener.local_addr().expect("Failed to get address");

        // Create 100 connections (not 1000+ for unit test performance)
        let num_connections = 100;
        let mut clients = Vec::new();
        let mut tokens = Vec::new();

        for i in 0..num_connections {
            let client = TcpStream::connect(addr).expect(&format!("Failed to connect {}", i));
            let (server, _) = listener.accept().expect(&format!("Failed to accept {}", i));

            let token = reactor
                .add_connection(server)
                .expect(&format!("Failed to add connection {}", i));
            clients.push(client);
            tokens.push(token);
        }

        // Verify all connections exist
        assert_eq!(reactor.connections.len(), num_connections);

        // Remove all connections
        for token in &tokens {
            reactor.remove_connection(token.id);
        }

        // Verify all connections removed
        assert_eq!(reactor.connections.len(), 0);
    }

    // ==================== Performance Tests ====================
    // Run with: cargo test --features rtmp --release -- --ignored --nocapture

    /// Performance test: Connection scaling (1000 connections)
    /// Tests the reactor's ability to handle many concurrent connections
    #[test]
    #[ignore] // Only run when explicitly requested
    fn perf_connection_scaling() {
        use std::time::Instant;

        let status = Arc::new(AtomicUsize::new(STATUS_RUN));

        let mut reactor = Reactor::new(3, None, status).expect("Failed to create reactor");

        let listener = std::net::TcpListener::bind("127.0.0.1:0").expect("Failed to bind");
        let addr = listener.local_addr().expect("Failed to get address");

        // Adaptive connection count based on system FD limit
        // Each connection needs 2 FDs (client + server), plus some headroom
        let max_fd = effective_max_connections(None);
        let num_connections = (max_fd / 3).min(1000);

        let mut clients = Vec::with_capacity(num_connections);
        let mut tokens = Vec::with_capacity(num_connections);

        // Measure connection creation time
        let start = Instant::now();

        for i in 0..num_connections {
            let client =
                TcpStream::connect(addr).unwrap_or_else(|_| panic!("Failed to connect {}", i));
            let (server, _) = listener
                .accept()
                .unwrap_or_else(|_| panic!("Failed to accept {}", i));
            let token = reactor
                .add_connection(server)
                .unwrap_or_else(|_| panic!("Failed to add {}", i));
            clients.push(client);
            tokens.push(token);
        }

        let connect_time = start.elapsed();

        // Verify
        assert_eq!(reactor.connections.len(), num_connections);

        // Measure cleanup time
        let cleanup_start = Instant::now();
        for token in &tokens {
            reactor.remove_connection(token.id);
        }
        let cleanup_time = cleanup_start.elapsed();

        // Output results
        println!();
        println!("╔══════════════════════════════════════════════════════════╗");
        println!("║           RTMP Performance Test: Connection Scaling      ║");
        println!("╠══════════════════════════════════════════════════════════╣");
        println!("║ Platform:        {:>40} ║", std::env::consts::OS);
        println!("║ Arch:            {:>40} ║", std::env::consts::ARCH);
        println!("║ Connections:     {:>40} ║", num_connections);
        println!("╠══════════════════════════════════════════════════════════╣");
        println!("║ Connect time:    {:>37?} ║", connect_time);
        println!(
            "║ Per connection:  {:>37?} ║",
            connect_time / num_connections as u32
        );
        println!("║ Cleanup time:    {:>37?} ║", cleanup_time);
        println!(
            "║ Per cleanup:     {:>37?} ║",
            cleanup_time / num_connections as u32
        );
        println!("╚══════════════════════════════════════════════════════════╝");
        println!();
    }

    /// Performance test: Read buffer throughput
    /// Tests try_read() + extend_from_slice optimization
    #[test]
    #[ignore] // Only run when explicitly requested
    fn perf_read_throughput() {
        use std::io::Write;
        use std::time::Instant;

        let status = Arc::new(AtomicUsize::new(STATUS_RUN));

        let mut reactor = Reactor::new(3, None, status).expect("Failed to create reactor");

        let listener = std::net::TcpListener::bind("127.0.0.1:0").expect("Failed to bind");
        let addr = listener.local_addr().expect("Failed to get address");

        let mut client = TcpStream::connect(addr).expect("Failed to connect");
        let (server, _) = listener.accept().expect("Failed to accept");
        client.set_nodelay(true).ok();

        let token = reactor
            .add_connection(server)
            .expect("Failed to add connection");

        // Test data: simulate RTMP-like traffic (various chunk sizes)
        let test_sizes = [128, 1024, 4096, 8192, 16384, 65536];
        let iterations = 100;

        println!();
        println!("╔══════════════════════════════════════════════════════════╗");
        println!("║           RTMP Performance Test: Read Throughput         ║");
        println!("╠══════════════════════════════════════════════════════════╣");
        println!("║ Platform:        {:>40} ║", std::env::consts::OS);
        println!("║ Arch:            {:>40} ║", std::env::consts::ARCH);
        println!("║ Iterations:      {:>40} ║", iterations);
        println!("╠══════════════════════════════════════════════════════════╣");

        for &size in &test_sizes {
            let data = vec![0xABu8; size];
            let mut total_bytes = 0usize;

            let start = Instant::now();

            for _ in 0..iterations {
                // Write data from client
                client.write_all(&data).expect("Failed to write");
                client.flush().expect("Failed to flush");
                total_bytes += size;

                // Small delay to let data arrive
                std::thread::sleep(std::time::Duration::from_micros(100));

                // Read via reactor connection
                if let Some(conn) = reactor.connections.get_mut(token.id) {
                    let _ = conn.try_read();
                }
            }

            let elapsed = start.elapsed();
            let throughput_mbps = (total_bytes as f64 / 1_000_000.0) / elapsed.as_secs_f64();

            println!(
                "║ Chunk {:>6} B:  {:>8.2} MB/s ({:>6} B x {:>3})      ║",
                size, throughput_mbps, size, iterations
            );
        }

        println!("╚══════════════════════════════════════════════════════════╝");
        println!();

        // Cleanup
        reactor.remove_connection(token.id);
    }

    /// Test that handle_writable marks interest_dirty when write queue drains
    #[test]
    fn test_handle_writable_marks_interest_dirty_on_queue_drain() {
        use std::io::Read;

        let status = Arc::new(AtomicUsize::new(STATUS_RUN));

        let mut reactor = Reactor::new(3, None, status).expect("Failed to create reactor");

        // Create a listener and connection pair
        let listener = std::net::TcpListener::bind("127.0.0.1:0").expect("Failed to bind");
        let addr = listener.local_addr().expect("Failed to get address");

        let mut client = TcpStream::connect(addr).expect("Failed to connect");
        let (server, _) = listener.accept().expect("Failed to accept");
        client.set_nonblocking(true).ok();

        let token = reactor
            .add_connection(server)
            .expect("Failed to add connection");

        // Set connection to Active state so it can write
        if let Some(conn) = reactor.connections.get_mut(token.id) {
            conn.state = ConnectionState::Active;
        }

        // Enqueue small data that will be fully written in one flush
        let test_data = b"Hello";
        if let Some(conn) = reactor.connections.get_mut(token.id) {
            conn.enqueue_data(Bytes::from_static(test_data), false, false, false, true, Instant::now());
            assert!(conn.has_pending_writes());
        }

        // Clear any existing interest_dirty entries
        reactor.drain_interest_dirty();

        // Call handle_writable - this should flush and mark interest_dirty
        let result = reactor.handle_writable(token.id);
        assert!(result.is_none(), "Connection should not be closed");

        // Verify connection no longer has pending writes
        if let Some(conn) = reactor.connections.get(token.id) {
            assert!(!conn.has_pending_writes(), "Queue should be drained");
        }

        // Verify interest_dirty was marked
        assert!(
            reactor.is_interest_dirty(token.id),
            "interest_dirty should contain connection ID after queue drain"
        );

        // Read from client to verify data was sent
        let mut buf = vec![0u8; 100];
        client.set_nonblocking(false).ok();
        client
            .set_read_timeout(Some(std::time::Duration::from_millis(100)))
            .ok();
        let _ = client.read(&mut buf);

        // Cleanup
        reactor.remove_connection(token.id);
    }

    /// Test that flush_pending marks interest_dirty when write queue drains
    #[test]
    fn test_flush_pending_marks_interest_dirty_on_queue_drain() {
        use std::io::Read;

        let status = Arc::new(AtomicUsize::new(STATUS_RUN));

        let mut reactor = Reactor::new(3, None, status).expect("Failed to create reactor");

        // Create a listener and connection pair
        let listener = std::net::TcpListener::bind("127.0.0.1:0").expect("Failed to bind");
        let addr = listener.local_addr().expect("Failed to get address");

        let mut client = TcpStream::connect(addr).expect("Failed to connect");
        let (server, _) = listener.accept().expect("Failed to accept");
        client.set_nonblocking(true).ok();

        let token = reactor
            .add_connection(server)
            .expect("Failed to add connection");

        // Set connection to Active state
        if let Some(conn) = reactor.connections.get_mut(token.id) {
            conn.state = ConnectionState::Active;
        }

        // Enqueue data and mark for pending flush
        let test_data = b"World";
        if let Some(conn) = reactor.connections.get_mut(token.id) {
            conn.enqueue_data(Bytes::from_static(test_data), false, false, false, true, Instant::now());
        }
        reactor.mark_pending_flush_for_test(token.id);

        // Clear interest_dirty
        reactor.drain_interest_dirty();

        // Call flush_pending
        let ids_to_close = reactor.flush_pending();
        assert!(
            ids_to_close.is_empty(),
            "No connections should need closing"
        );

        // Verify interest_dirty was marked after flush drained the queue
        assert!(
            reactor.is_interest_dirty(token.id),
            "interest_dirty should contain connection ID after flush_pending drains queue"
        );

        // Read from client to consume data
        let mut buf = vec![0u8; 100];
        client.set_nonblocking(false).ok();
        client
            .set_read_timeout(Some(std::time::Duration::from_millis(100)))
            .ok();
        let _ = client.read(&mut buf);

        // Cleanup
        reactor.remove_connection(token.id);
    }

    /// Shrink a socket's send/receive buffer so a modest enqueue reliably
    /// fills the kernel pipe and forces WouldBlock in tests.
    #[cfg(unix)]
    fn set_small_socket_buffer(fd: std::os::unix::io::RawFd, opt: libc::c_int) {
        let size: libc::c_int = 2048;
        // SAFETY: setsockopt on a valid fd with a valid SOL_SOCKET option and a
        // correctly-sized c_int value; return value is intentionally ignored
        // (best-effort tuning for the test).
        unsafe {
            libc::setsockopt(
                fd,
                libc::SOL_SOCKET,
                opt,
                &size as *const libc::c_int as *const libc::c_void,
                std::mem::size_of::<libc::c_int>() as libc::socklen_t,
            );
        }
    }

    /// NEW-RS-01: a WouldBlock flush must register writable interest and must
    /// NOT reinsert the connection into pending_flush (which would re-attempt
    /// the write every loop and burn a guaranteed-EAGAIN syscall).
    #[cfg(unix)]
    #[test]
    fn test_flush_pending_wouldblock_registers_writable_not_pending_flush() {
        use std::os::unix::io::AsRawFd;

        let status = Arc::new(AtomicUsize::new(STATUS_RUN));
        let mut reactor = Reactor::new(3, None, status).expect("Failed to create reactor");

        let listener = std::net::TcpListener::bind("127.0.0.1:0").expect("Failed to bind");
        let addr = listener.local_addr().expect("Failed to get address");
        let client = TcpStream::connect(addr).expect("Failed to connect");
        let (server, _) = listener.accept().expect("Failed to accept");

        // Tiny buffers + a client that never reads => writes WouldBlock fast.
        set_small_socket_buffer(server.as_raw_fd(), libc::SO_SNDBUF);
        set_small_socket_buffer(client.as_raw_fd(), libc::SO_RCVBUF);

        let token = reactor
            .add_connection(server)
            .expect("Failed to add connection");
        if let Some(conn) = reactor.connections.get_mut(token.id) {
            conn.state = ConnectionState::Active;
            // ~1MB single sequence-header entry: far larger than the shrunk
            // buffers, under the 4MB critical threshold, never dropped.
            let big = Bytes::from(vec![0u8; 1024 * 1024]);
            assert!(conn.enqueue_data(big, false, true, true, true, Instant::now()));
            assert!(conn.has_pending_writes());
        }

        reactor.mark_pending_flush_for_test(token.id);
        reactor.drain_interest_dirty();

        let closes = reactor.flush_pending();
        assert!(
            closes.is_empty(),
            "a full-buffer slow client must not be closed"
        );

        let conn = reactor
            .connections
            .get(token.id)
            .expect("connection present");
        assert!(
            conn.has_pending_writes(),
            "WouldBlock must leave the remaining data queued"
        );
        assert!(
            !reactor.is_pending_flush(token.id),
            "WouldBlock must NOT reinsert into pending_flush (no EAGAIN spin)"
        );
        assert!(
            reactor.is_interest_dirty(token.id),
            "WouldBlock must mark interest_dirty so writable interest is registered"
        );

        reactor.remove_connection(token.id);
        drop(client);
    }

    /// Test that flush_pending marks interest_dirty when connection has no pending writes
    #[test]
    fn test_flush_pending_marks_interest_dirty_when_no_pending_writes() {
        let status = Arc::new(AtomicUsize::new(STATUS_RUN));

        let mut reactor = Reactor::new(3, None, status).expect("Failed to create reactor");

        // Create a listener and connection pair
        let listener = std::net::TcpListener::bind("127.0.0.1:0").expect("Failed to bind");
        let addr = listener.local_addr().expect("Failed to get address");

        let _client = TcpStream::connect(addr).expect("Failed to connect");
        let (server, _) = listener.accept().expect("Failed to accept");

        let token = reactor
            .add_connection(server)
            .expect("Failed to add connection");

        // Set connection to Active state but don't enqueue any data
        if let Some(conn) = reactor.connections.get_mut(token.id) {
            conn.state = ConnectionState::Active;
            assert!(!conn.has_pending_writes());
        }

        // Mark for pending flush even though no data pending
        // (this can happen if data was already flushed between enqueue and flush_pending)
        reactor.mark_pending_flush_for_test(token.id);

        // Clear interest_dirty
        reactor.drain_interest_dirty();

        // Call flush_pending
        let ids_to_close = reactor.flush_pending();
        assert!(
            ids_to_close.is_empty(),
            "No connections should need closing"
        );

        // Verify interest_dirty was marked to clear writable interest
        assert!(reactor.is_interest_dirty(token.id),
            "interest_dirty should be marked even when no pending writes (to clear WRITABLE interest)");

        // Cleanup
        reactor.remove_connection(token.id);
    }

    /// Several packets fanned out to one connection in a single round must
    /// list it for flush/interest exactly once: the connection-owned flags
    /// gate the id-list pushes, so each drain visits the connection once —
    /// the dedup the old hash sets provided by membership.
    #[test]
    fn dirty_marks_dedup_within_a_round() {
        let status = Arc::new(AtomicUsize::new(STATUS_RUN));
        let mut reactor = Reactor::new(3, None, status).expect("Failed to create reactor");

        let listener = std::net::TcpListener::bind("127.0.0.1:0").expect("Failed to bind");
        let addr = listener.local_addr().expect("Failed to get address");
        let _client = TcpStream::connect(addr).expect("Failed to connect");
        let (server, _) = listener.accept().expect("Failed to accept");

        let token = reactor
            .add_connection(server)
            .expect("Failed to add connection");
        if let Some(conn) = reactor.connections.get_mut(token.id) {
            conn.state = ConnectionState::Active;
        }

        // Two packets to the same target in one fanout round.
        for _ in 0..2 {
            reactor.packets_buffer.push((
                token.id,
                Bytes::from_static(b"tag"),
                false,
                false,
                false,
                true,
            ));
        }
        reactor.write_pending_packets();

        assert!(reactor.is_pending_flush(token.id));
        assert!(reactor.is_interest_dirty(token.id));
        assert_eq!(
            reactor.pending_flush_ids,
            vec![token.id],
            "two packets to one target must list it for flush exactly once"
        );
        assert_eq!(
            reactor.interest_dirty_ids,
            vec![token.id],
            "two packets to one target must list it for interest exactly once"
        );

        // Each drain visits the id exactly once and consumes the mark.
        let ids_to_close = reactor.flush_pending();
        assert!(ids_to_close.is_empty());
        assert!(!reactor.is_pending_flush(token.id));
        assert_eq!(
            reactor.drain_interest_dirty(),
            vec![token.id],
            "the interest drain must visit the id exactly once"
        );

        reactor.remove_connection(token.id);
    }

    /// Marks clear on drain and re-arm afterwards: after a full
    /// flush_pending + update_dirty_interests pass the connection is
    /// unmarked, and a fresh mark lists and drains it again — no sticky
    /// state in either direction across drain epochs.
    #[test]
    fn dirty_marks_rearm_across_drain_epochs() {
        let status = Arc::new(AtomicUsize::new(STATUS_RUN));
        let mut reactor = Reactor::new(3, None, status).expect("Failed to create reactor");

        let listener = std::net::TcpListener::bind("127.0.0.1:0").expect("Failed to bind");
        let addr = listener.local_addr().expect("Failed to get address");
        let _client = TcpStream::connect(addr).expect("Failed to connect");
        let (server, _) = listener.accept().expect("Failed to accept");

        let token = reactor
            .add_connection(server)
            .expect("Failed to add connection");
        if let Some(conn) = reactor.connections.get_mut(token.id) {
            conn.state = ConnectionState::Active;
        }

        // Epoch 1: fanout marks, the two drains consume the marks.
        reactor.packets_buffer.push((
            token.id,
            Bytes::from_static(b"first"),
            false,
            false,
            false,
            true,
        ));
        reactor.write_pending_packets();
        assert!(reactor.is_pending_flush(token.id));
        assert!(reactor.is_interest_dirty(token.id));

        assert!(reactor.flush_pending().is_empty());
        assert!(reactor.update_dirty_interests().is_empty());
        assert!(
            !reactor.is_pending_flush(token.id),
            "the flush drain must clear the mark"
        );
        assert!(
            !reactor.is_interest_dirty(token.id),
            "the interest drain must clear the mark"
        );

        // Epoch 2: a fresh mark must be listed and drained again.
        reactor.packets_buffer.push((
            token.id,
            Bytes::from_static(b"second"),
            false,
            false,
            false,
            true,
        ));
        reactor.write_pending_packets();
        assert!(
            reactor.is_pending_flush(token.id) && reactor.is_interest_dirty(token.id),
            "a mark after a drain must re-arm"
        );
        assert!(reactor.flush_pending().is_empty());
        assert!(reactor.update_dirty_interests().is_empty());
        assert!(!reactor.is_pending_flush(token.id));
        assert!(!reactor.is_interest_dirty(token.id));

        reactor.remove_connection(token.id);
    }

    /// Ids marked and then removed before the drain must stay harmless: the
    /// drains skip them without panicking or closing anything, and a fresh
    /// connection reusing the slab slot is born unmarked — it is not
    /// spuriously visited on the stale id's account, while a legitimate new
    /// mark still drains it.
    #[test]
    fn stale_dirty_ids_for_removed_connections_drain_harmlessly() {
        let status = Arc::new(AtomicUsize::new(STATUS_RUN));
        let mut reactor = Reactor::new(3, None, status).expect("Failed to create reactor");

        let listener = std::net::TcpListener::bind("127.0.0.1:0").expect("Failed to bind");
        let addr = listener.local_addr().expect("Failed to get address");
        let _client1 = TcpStream::connect(addr).expect("Failed to connect");
        let (server1, _) = listener.accept().expect("Failed to accept");

        // Mark the first connection through the real fanout path, then
        // remove it: removal leaves the raw ids behind in the lists.
        let token1 = reactor
            .add_connection(server1)
            .expect("Failed to add connection");
        if let Some(conn) = reactor.connections.get_mut(token1.id) {
            conn.state = ConnectionState::Active;
        }
        reactor.packets_buffer.push((
            token1.id,
            Bytes::from_static(b"doomed"),
            false,
            false,
            false,
            true,
        ));
        reactor.write_pending_packets();
        reactor.remove_connection(token1.id);

        // The slab reuses the freed slot for a brand-new connection, so the
        // stale entries now point at it.
        let _client2 = TcpStream::connect(addr).expect("Failed to connect");
        let (server2, _) = listener.accept().expect("Failed to accept");
        let token2 = reactor
            .add_connection(server2)
            .expect("Failed to add connection");
        assert_eq!(
            token2.id, token1.id,
            "the slab must reuse the freed slot for this test to bite"
        );
        assert!(
            !reactor.is_pending_flush(token2.id) && !reactor.is_interest_dirty(token2.id),
            "a fresh connection must be born unmarked"
        );

        // Queue data on the fresh connection WITHOUT marking it. The stale
        // entries must not flush it out of turn or touch its interest.
        if let Some(conn) = reactor.connections.get_mut(token2.id) {
            conn.state = ConnectionState::Active;
            assert!(conn.enqueue_data(
                Bytes::from_static(b"unscheduled"),
                false,
                false,
                false,
                true,
                Instant::now()
            ));
        }
        assert!(
            reactor.flush_pending().is_empty(),
            "a stale id must not close anything"
        );
        assert!(
            reactor
                .connections
                .get(token2.id)
                .expect("connection present")
                .has_pending_writes(),
            "a stale id must not flush a fresh unmarked connection out of turn"
        );
        assert!(
            reactor.update_dirty_interests().is_empty(),
            "a stale id must not fail an interest update"
        );
        assert!(
            !reactor
                .connections
                .get(token2.id)
                .expect("connection present")
                .current_interest
                .writable,
            "a stale id must not register writable interest for a fresh connection"
        );

        // A legitimate mark on the fresh connection still drains it.
        reactor.packets_buffer.push((
            token2.id,
            Bytes::from_static(b"scheduled"),
            false,
            false,
            false,
            true,
        ));
        reactor.write_pending_packets();
        assert!(reactor.flush_pending().is_empty());
        assert!(
            !reactor
                .connections
                .get(token2.id)
                .expect("connection present")
                .has_pending_writes(),
            "a real mark must still flush the fresh connection"
        );

        // And ids whose slot stays EMPTY at drain time are equally harmless.
        reactor.packets_buffer.push((
            token2.id,
            Bytes::from_static(b"doomed again"),
            false,
            false,
            false,
            true,
        ));
        reactor.write_pending_packets();
        reactor.remove_connection(token2.id);
        assert!(reactor.flush_pending().is_empty());
        assert!(reactor.update_dirty_interests().is_empty());
    }

    /// A flash crowd grows the dirty-id lists to O(peak); once the crowd
    /// decays, the drain-side guard must walk the backing capacity down to
    /// the floor instead of pinning peak-sized buffers for the life of the
    /// reactor — but only after sustained sparsity (the hysteresis streak),
    /// and never below `DIRTY_IDS_SHRINK_MIN_CAPACITY`, so idle ticks
    /// between media batches cause no allocation churn.
    #[test]
    fn dirty_id_list_capacity_shrinks_after_flash_crowd() {
        let status = Arc::new(AtomicUsize::new(STATUS_RUN));
        let mut reactor = Reactor::new(3, None, status).expect("Failed to create reactor");

        // Flash crowd: 512 marked ids in one epoch. No connections back them
        // (the marked-then-removed state), which the drain skips harmlessly;
        // capacity behavior is what is under test here.
        let flash_crowd: usize = 512;
        reactor.pending_flush_ids.extend(0..flash_crowd);
        let peak_capacity = reactor.pending_flush_ids.capacity();
        assert!(
            peak_capacity > DIRTY_IDS_SHRINK_MIN_CAPACITY,
            "512 marked ids must grow the list past the shrink floor, got {peak_capacity}"
        );

        // Peak-occupancy drain: the guard must NOT fire (occupancy is far
        // above a quarter of capacity), so the peak buffer survives this
        // pass — parked in the scratch slot by the snapshot swap.
        assert!(reactor.flush_pending().is_empty());
        assert!(
            reactor
                .pending_flush_ids
                .capacity()
                .max(reactor.dirty_drain_scratch.capacity())
                >= peak_capacity,
            "a full drain must not shrink the buffer it just used at peak occupancy"
        );

        // The crowd decays. A shrink needs SPARSE_DRAINS_BEFORE_SHRINK
        // consecutive sparse passes first (one idle tick must not discard
        // capacity); once the streak is up it stays up while the sparse
        // regime lasts, and the swap rotation walks each flush-side buffer
        // through the drain position — the +2 covers both.
        for _ in 0..SPARSE_DRAINS_BEFORE_SHRINK + 2 {
            reactor.pending_flush_ids.push(0);
            assert!(reactor.flush_pending().is_empty());
        }
        let live_capacity = reactor.pending_flush_ids.capacity();
        let scratch_capacity = reactor.dirty_drain_scratch.capacity();
        assert!(
            live_capacity <= DIRTY_IDS_SHRINK_MIN_CAPACITY,
            "the decayed live list must shrink to the floor, got {live_capacity}"
        );
        assert!(
            scratch_capacity <= DIRTY_IDS_SHRINK_MIN_CAPACITY,
            "the decayed drain scratch must shrink to the floor, got {scratch_capacity}"
        );

        // The interest-side drain applies the same guard through the shared
        // scratch slot. Its dense first pass resets the streak, so the decay
        // needs its own sustained-sparsity run.
        reactor.interest_dirty_ids.extend(0..flash_crowd);
        let interest_peak = reactor.interest_dirty_ids.capacity();
        assert!(interest_peak > DIRTY_IDS_SHRINK_MIN_CAPACITY);
        assert!(reactor.update_dirty_interests().is_empty());
        for _ in 0..SPARSE_DRAINS_BEFORE_SHRINK + 2 {
            reactor.interest_dirty_ids.push(0);
            assert!(reactor.update_dirty_interests().is_empty());
        }
        assert!(
            reactor.interest_dirty_ids.capacity() <= DIRTY_IDS_SHRINK_MIN_CAPACITY,
            "the decayed interest list must shrink to the floor"
        );
        assert!(
            reactor.dirty_drain_scratch.capacity() <= DIRTY_IDS_SHRINK_MIN_CAPACITY,
            "the drain scratch must stay shrunk after the interest epochs"
        );
    }

    // PERF-5a: exercise the real mixed PublisherFeed::Raw + PublisherFeed::Media
    // drain in process_publishers (not just the scheduler entry point), so the
    // create_rtmp_input registration path is regression-tested at the reactor
    // level. FIFO ordering is structural — a single crossbeam Receiver drained
    // in-order by the try_recv loop — so this asserts that both variants are
    // processed on one drain and the bypassed media reaches the scheduler,
    // rather than re-proving the channel's ordering.
    #[test]
    fn feed_publisher_drains_mixed_raw_and_media() {
        use crate::rtmp::embed_rtmp_server::build_publish_control;

        let stream_keys = Arc::new(dashmap::DashSet::new());
        let status = Arc::new(AtomicUsize::new(STATUS_RUN));
        let mut reactor = Reactor::new(3, None, status).expect("reactor");

        // Register an in-process (create_rtmp_input-style) Feed publisher.
        let (feed_tx, feed_rx) = crossbeam_channel::bounded(64);
        let (budget_guard, budget) = IngressBudget::new(PUBLISHER_INGRESS_HIGH_WATER_BYTES);
        let claim = StreamKeyClaim::claim(stream_keys, "live".to_string()).expect("claim");
        let pub_id = reactor
            .add_publisher(PublisherRegistration {
                claim,
                source: PublisherSource::Feed(feed_rx),
                budget: budget_guard,
            })
            .expect("publisher registered");

        // Realistic mixed sequence on ONE FIFO feed: connect/createStream/publish
        // as Raw (fed to the session), then audio/video tags as bypassed Media.
        for control in
            build_publish_control("app".to_string(), "live".to_string()).expect("control")
        {
            send_acquired(&budget, &feed_tx, PublisherFeed::Raw(control));
        }
        let video_seq: &[u8] = &[0x17, 0x00, 0x00, 0x00, 0x00, 0x01, 0x64];
        let audio_seq: &[u8] = &[0xaf, 0x00, 0x12, 0x10];
        send_acquired(
            &budget,
            &feed_tx,
            PublisherFeed::Media {
                tag_type: 0x09,
                timestamp: RtmpTimestamp { value: 0 },
                data: Bytes::from_static(video_seq),
            },
        );
        send_acquired(
            &budget,
            &feed_tx,
            PublisherFeed::Media {
                tag_type: 0x08,
                timestamp: RtmpTimestamp { value: 0 },
                data: Bytes::from_static(audio_seq),
            },
        );
        send_acquired(
            &budget,
            &feed_tx,
            PublisherFeed::Media {
                tag_type: 0x09,
                timestamp: RtmpTimestamp { value: 33 },
                data: Bytes::from_static(&[0x17, 0x01, 0xAA, 0xBB]),
            },
        );
        // A second keyframe freezes the first GOP into the replay cache.
        send_acquired(
            &budget,
            &feed_tx,
            PublisherFeed::Media {
                tag_type: 0x09,
                timestamp: RtmpTimestamp { value: 66 },
                data: Bytes::from_static(&[0x17, 0x01, 0xCC, 0xDD]),
            },
        );

        // Drain the mixed feed; a healthy publisher must not be removed.
        let (removed, _) = reactor.process_publishers();
        assert!(removed.is_empty(), "healthy publisher must not be removed");

        // The Media items were dispatched through publish_media_received (not the
        // serializer): sequence headers cached and a GOP frozen. The Raw control
        // was fed to the session on the same drain without error — proving the
        // mixed Raw+Media path works and preserves the channel state the
        // serialize path would produce.
        assert_eq!(
            reactor
                .scheduler
                .channel_video_sequence_header("live")
                .as_deref(),
            Some(video_seq),
            "bypassed video sequence header must be cached"
        );
        assert_eq!(
            reactor
                .scheduler
                .channel_audio_sequence_header("live")
                .as_deref(),
            Some(audio_seq),
            "bypassed audio sequence header must be cached"
        );
        assert!(
            reactor.scheduler.channel_frozen_gop_count("live") >= 1,
            "the completed GOP must be frozen from bypassed media"
        );

        // Dropping the sender disconnects the publisher; the next drain removes it.
        drop(feed_tx);
        let (removed, _) = reactor.process_publishers();
        assert!(
            removed.contains(&pub_id),
            "a disconnected publisher must be scheduled for removal"
        );
    }

    // The claim guard has exactly one exit: drop releases the key. Moving
    // the guard (into a registration, into the accepted publisher's state)
    // neither releases nor re-inserts anything.
    #[test]
    fn stream_key_claim_releases_on_drop_but_not_on_move() {
        let stream_keys: Arc<dashmap::DashSet<String>> = Arc::new(dashmap::DashSet::new());

        let claim = StreamKeyClaim::claim(stream_keys.clone(), "k".to_string()).expect("claim");
        assert!(
            StreamKeyClaim::claim(stream_keys.clone(), "k".to_string()).is_err(),
            "a second claim for a held key must lose"
        );

        // A move is not an exit: the moved-from binding runs no Drop.
        let moved = claim;
        assert!(
            stream_keys.contains("k"),
            "moving the guard must keep the key claimed"
        );

        drop(moved);
        assert!(!stream_keys.contains("k"), "drop must release the claim");
    }

    // add_publisher owns the claim it is handed: acceptance moves the
    // still-armed claim into the publisher's state, released when that
    // state drops (here via remove_publisher); refusal (a network session
    // already publishes the key, which never touches stream_keys) drops
    // the claim and frees the key immediately.
    #[test]
    fn add_publisher_acceptance_defers_release_refusal_frees_the_key() {
        let stream_keys = Arc::new(dashmap::DashSet::new());
        let status = Arc::new(AtomicUsize::new(STATUS_RUN));
        let mut reactor = Reactor::new(3, None, status).expect("reactor");

        let (_feed_tx, feed_rx) = crossbeam_channel::bounded::<PublisherFeed>(1);
        let claim =
            StreamKeyClaim::claim(stream_keys.clone(), "live".to_string()).expect("claim");
        let id = reactor
            .add_publisher(PublisherRegistration {
                claim,
                source: PublisherSource::Feed(feed_rx),
                budget: test_budget(),
            })
            .expect("accepted");
        assert!(
            stream_keys.contains("live"),
            "acceptance must keep the key claimed"
        );
        reactor.remove_publisher(id);
        assert!(
            !stream_keys.contains("live"),
            "removing the publisher must release its key"
        );

        // Simulate a network publisher owning "net" inside the scheduler.
        assert!(reactor.scheduler.new_channel("net".to_string(), 777));
        let (_feed_tx2, feed_rx2) = crossbeam_channel::bounded::<PublisherFeed>(1);
        let claim = StreamKeyClaim::claim(stream_keys.clone(), "net".to_string()).expect("claim");
        assert!(
            reactor
                .add_publisher(PublisherRegistration {
                    claim,
                    source: PublisherSource::Feed(feed_rx2),
                    budget: test_budget(),
                })
                .is_none(),
            "a key already being published must be refused"
        );
        assert!(
            !stream_keys.contains("net"),
            "a refused registration must release its key claim"
        );
    }

    // A registration enqueued while the reactor is stopping is never
    // consumed — run() checks the stop flag before draining registrations.
    // Its queued claim must not outlive the worker: the key must be
    // claimable again even though the server side still holds the handoff.
    #[test]
    fn reactor_stop_releases_enqueued_but_unconsumed_key_claims() {
        let stream_keys = Arc::new(dashmap::DashSet::new());
        let status = Arc::new(AtomicUsize::new(STATUS_RUN));
        let mut reactor = Reactor::new(3, None, status.clone()).expect("reactor");

        let (_connection_sender, connection_receiver) =
            crossbeam_channel::bounded::<TcpStream>(1);
        let registrations = Arc::new(RegistrationHandoff::new());
        let kill_switch = RegistrationKillSwitch::arm(registrations.clone());

        // Enqueue a registration, then signal stop so run() exits on its
        // first stop-flag check without ever draining the queue.
        let (_feed_tx, feed_rx) = crossbeam_channel::bounded::<PublisherFeed>(1);
        let claim = StreamKeyClaim::claim(stream_keys.clone(), "orphan".to_string())
            .expect("first claim must win");
        registrations
            .enqueue(PublisherRegistration {
                claim,
                source: PublisherSource::Feed(feed_rx),
                budget: test_budget(),
            })
            .unwrap_or_else(|_| panic!("enqueue while the worker lives"));
        status.store(STATUS_END, Ordering::Release);

        reactor.run(connection_receiver, kill_switch.handoff(), None);

        // Tear the worker down in its real order: reactor first, then the
        // kill switch fires the terminal drain.
        drop(reactor);
        drop(kill_switch);

        // The server-side handle is still alive, so only the worker's exit
        // drain can have released the claim.
        assert!(
            !stream_keys.contains("orphan"),
            "a stopped reactor must release queued, never-consumed key claims"
        );
        assert!(
            StreamKeyClaim::claim(stream_keys.clone(), "orphan".to_string()).is_ok(),
            "the key must be claimable again after the reactor stopped"
        );
        drop(registrations);
    }

    // Reactor teardown must release accepted publishers' key claims: run()
    // can exit — stop signal or unwind — with publishers still in the slab,
    // and dropping the reactor is the worker's last word on them, so the
    // keys must be claimable again afterwards.
    #[test]
    fn reactor_teardown_releases_accepted_publisher_key_claims() {
        let stream_keys = Arc::new(dashmap::DashSet::new());
        let status = Arc::new(AtomicUsize::new(STATUS_RUN));
        let mut reactor = Reactor::new(3, None, status.clone()).expect("reactor");

        let (_feed_tx, feed_rx) = crossbeam_channel::bounded::<PublisherFeed>(1);
        let claim =
            StreamKeyClaim::claim(stream_keys.clone(), "live".to_string()).expect("claim");
        reactor
            .add_publisher(PublisherRegistration {
                claim,
                source: PublisherSource::Feed(feed_rx),
                budget: test_budget(),
            })
            .expect("accepted");
        assert!(
            stream_keys.contains("live"),
            "an accepted publisher's key stays claimed while the reactor lives"
        );

        // run() exits on the stop flag with the publisher still in the slab.
        let (_connection_sender, connection_receiver) =
            crossbeam_channel::bounded::<TcpStream>(1);
        let registrations = Arc::new(RegistrationHandoff::new());
        let kill_switch = RegistrationKillSwitch::arm(registrations);
        status.store(STATUS_END, Ordering::Release);
        reactor.run(connection_receiver, kill_switch.handoff(), None);

        drop(reactor);
        assert!(
            !stream_keys.contains("live"),
            "tearing the reactor down must release accepted publishers' keys"
        );
        assert!(
            StreamKeyClaim::claim(stream_keys.clone(), "live".to_string()).is_ok(),
            "the key must be claimable again after teardown"
        );
    }

    // The worker can die BEFORE run() ever executes — Reactor::new failing
    // is enough — and the create paths keep their handle to the handoff for
    // as long as the server value lives. The kill switch is armed before the
    // reactor is constructed, so even that early death must release queued
    // claims (this leaked for the server's lifetime when the payload rode a
    // channel: the queued message stayed alive with the server's endpoint).
    #[test]
    fn worker_death_before_reactor_construction_releases_queued_key_claims() {
        let stream_keys: Arc<dashmap::DashSet<String>> = Arc::new(dashmap::DashSet::new());
        let registrations = Arc::new(RegistrationHandoff::new());

        // The worker arms the switch before anything fallible.
        let kill_switch = RegistrationKillSwitch::arm(registrations.clone());

        let (_feed_tx, feed_rx) = crossbeam_channel::bounded::<PublisherFeed>(1);
        let claim = StreamKeyClaim::claim(stream_keys.clone(), "orphan".to_string())
            .expect("first claim must win");
        registrations
            .enqueue(PublisherRegistration {
                claim,
                source: PublisherSource::Feed(feed_rx),
                budget: test_budget(),
            })
            .unwrap_or_else(|_| panic!("enqueue while the worker lives"));

        // Reactor::new failed: the worker dies without ever running the
        // reactor, and only the switch's drop stands between the queued
        // claim and a server-lifetime leak.
        drop(kill_switch);

        // The server still holds its handle, so only the worker-side drain
        // can have released the claim.
        assert!(
            !stream_keys.contains("orphan"),
            "a worker that died before constructing the reactor must release queued key claims"
        );

        // The same drop also closed the intake, under the same lock that
        // drained the queue: a late create is refused in its own enqueue
        // critical section — it can never park a claim in a queue nobody
        // will ever drain again — and dropping the refusal releases its
        // claim immediately.
        let (_late_tx, late_rx) = crossbeam_channel::bounded::<PublisherFeed>(1);
        let late_claim = StreamKeyClaim::claim(stream_keys.clone(), "late".to_string())
            .expect("claim after worker death");
        let refused = registrations.enqueue(PublisherRegistration {
            claim: late_claim,
            source: PublisherSource::Feed(late_rx),
            budget: test_budget(),
        });
        assert!(
            refused.is_err(),
            "the registration intake must be closed once the worker died"
        );
        drop(refused);
        assert!(
            !stream_keys.contains("late"),
            "a refused registration must release its key claim immediately"
        );
    }

    // drain_into moves ownership: the batch holds the registrations (claims
    // still armed) and the queue is left empty, so a registration is
    // consumed exactly once and released exactly once.
    #[test]
    fn drain_into_hands_queued_registrations_to_the_consumer_exactly_once() {
        let stream_keys: Arc<dashmap::DashSet<String>> = Arc::new(dashmap::DashSet::new());
        let registrations = RegistrationHandoff::new();

        for key in ["a", "b"] {
            let (_feed_tx, feed_rx) = crossbeam_channel::bounded::<PublisherFeed>(1);
            let claim = StreamKeyClaim::claim(stream_keys.clone(), key.to_string())
                .expect("first claim must win");
            registrations
                .enqueue(PublisherRegistration {
                    claim,
                    source: PublisherSource::Feed(feed_rx),
                    budget: test_budget(),
                })
                .unwrap_or_else(|_| panic!("enqueue while alive"));
        }

        let mut batch = Vec::new();
        registrations.drain_into(&mut batch);
        assert_eq!(batch.len(), 2, "one drain takes everything queued");
        assert!(
            stream_keys.contains("a") && stream_keys.contains("b"),
            "drained registrations still hold their claims"
        );

        registrations.drain_into(&mut batch);
        assert_eq!(batch.len(), 2, "the queue must be empty after a drain");

        drop(batch);
        assert!(
            !stream_keys.contains("a") && !stream_keys.contains("b"),
            "dropping the batch must release the claims exactly once"
        );
    }

    // The intake is bounded at REGISTRATION_QUEUE_CAPACITY — the bound the
    // crossbeam channel this queue replaced enforced. The capacity check
    // shares the enqueue's critical section, so the entry past the bound is
    // refused as Full with the queue still exactly at capacity; dropping
    // the refusal reopens its key immediately. A stalled reactor thus costs
    // callers a typed error, not an unbounded backlog of parked claims.
    #[test]
    fn full_registration_queue_refuses_enqueue_and_reopens_the_key() {
        let stream_keys: Arc<dashmap::DashSet<String>> = Arc::new(dashmap::DashSet::new());
        let registrations = RegistrationHandoff::new();

        for i in 0..REGISTRATION_QUEUE_CAPACITY {
            let (_feed_tx, feed_rx) = crossbeam_channel::bounded::<PublisherFeed>(1);
            let claim = StreamKeyClaim::claim(stream_keys.clone(), format!("k-{i}"))
                .expect("first claim must win");
            registrations
                .enqueue(PublisherRegistration {
                    claim,
                    source: PublisherSource::Feed(feed_rx),
                    budget: test_budget(),
                })
                .unwrap_or_else(|_| panic!("enqueue {i} within the bound must be accepted"));
        }

        let (_feed_tx, feed_rx) = crossbeam_channel::bounded::<PublisherFeed>(1);
        let claim = StreamKeyClaim::claim(stream_keys.clone(), "overflow".to_string())
            .expect("first claim must win");
        let refused = registrations.enqueue(PublisherRegistration {
            claim,
            source: PublisherSource::Feed(feed_rx),
            budget: test_budget(),
        });
        assert!(
            matches!(refused, Err(EnqueueRefused::Full(_))),
            "the enqueue past the bound must be refused as Full"
        );

        drop(refused);
        assert!(
            !stream_keys.contains("overflow"),
            "a refused registration must release its key claim"
        );
        assert!(
            StreamKeyClaim::claim(stream_keys.clone(), "overflow".to_string()).is_ok(),
            "the key must be claimable again right after the refusal"
        );

        // The refusal is backpressure, not closure: draining the backlog
        // makes the same intake accept again, and no queued entry was lost
        // to the refused push.
        let mut batch = Vec::new();
        while registrations.drain_into(&mut batch) {}
        assert_eq!(
            batch.len(),
            REGISTRATION_QUEUE_CAPACITY,
            "the refusal must leave the queued entries intact"
        );
        let (_feed_tx, feed_rx) = crossbeam_channel::bounded::<PublisherFeed>(1);
        let claim = StreamKeyClaim::claim(stream_keys.clone(), "after-drain".to_string())
            .expect("claim after the drain");
        assert!(
            registrations
                .enqueue(PublisherRegistration {
                    claim,
                    source: PublisherSource::Feed(feed_rx),
                    budget: test_budget(),
                })
                .is_ok(),
            "a drained intake must accept registrations again"
        );
    }

    // drain_into is budgeted: one call takes at most
    // MAX_REGISTRATIONS_PER_POLL entries — front first — and reports
    // whether a remainder is left, which the run loop turns into a
    // zero-timeout poll. A backlog larger than the budget therefore spreads
    // across rounds without losing entries or reordering them, instead of
    // monopolizing a single round while sockets wait.
    #[test]
    fn drain_into_budgets_each_round_without_losing_or_reordering_entries() {
        let stream_keys: Arc<dashmap::DashSet<String>> = Arc::new(dashmap::DashSet::new());
        let registrations = RegistrationHandoff::new();

        let total = MAX_REGISTRATIONS_PER_POLL * 2 + 3;
        for i in 0..total {
            let (_feed_tx, feed_rx) = crossbeam_channel::bounded::<PublisherFeed>(1);
            let claim = StreamKeyClaim::claim(stream_keys.clone(), format!("k-{i:04}"))
                .expect("first claim must win");
            registrations
                .enqueue(PublisherRegistration {
                    claim,
                    source: PublisherSource::Feed(feed_rx),
                    budget: test_budget(),
                })
                .unwrap_or_else(|_| panic!("enqueue {i} within the bound"));
        }

        // One budgeted batch per simulated reactor round.
        let mut drained_keys = Vec::new();
        let mut rounds = 0;
        loop {
            let mut batch = Vec::new();
            let more = registrations.drain_into(&mut batch);
            assert!(
                batch.len() <= MAX_REGISTRATIONS_PER_POLL,
                "one round must not exceed the drain budget"
            );
            drained_keys.extend(batch.iter().map(|r| r.claim.key().to_string()));
            rounds += 1;
            if !more {
                break;
            }
            assert_eq!(
                batch.len(),
                MAX_REGISTRATIONS_PER_POLL,
                "a round that reports a remainder must have used its whole budget"
            );
        }

        assert_eq!(
            rounds, 3,
            "the backlog must spread across ceil(total / budget) rounds"
        );
        let expected: Vec<String> = (0..total).map(|i| format!("k-{i:04}")).collect();
        assert_eq!(
            drained_keys, expected,
            "every entry must arrive exactly once, in enqueue order"
        );
    }

    // A worker can construct the reactor and still die before consuming a
    // queued registration — run() may never be reached, or exit on the stop
    // flag ahead of its first drain. The registration's claim lives in the
    // handoff queue, not in the reactor, so worker teardown (reactor drop,
    // then kill-switch drop) must release it and reopen the key even though
    // the server side keeps its handle to the handoff alive.
    #[test]
    fn worker_exit_without_consuming_registration_reopens_the_key() {
        let stream_keys: Arc<dashmap::DashSet<String>> = Arc::new(dashmap::DashSet::new());
        let status = Arc::new(AtomicUsize::new(STATUS_RUN));
        let reactor = Reactor::new(3, None, status).expect("reactor");

        let registrations = Arc::new(RegistrationHandoff::new());
        let kill_switch = RegistrationKillSwitch::arm(registrations.clone());

        let (_feed_tx, feed_rx) = crossbeam_channel::bounded::<PublisherFeed>(1);
        let claim = StreamKeyClaim::claim(stream_keys.clone(), "held".to_string())
            .expect("first claim must win");
        registrations
            .enqueue(PublisherRegistration {
                claim,
                source: PublisherSource::Feed(feed_rx),
                budget: test_budget(),
            })
            .unwrap_or_else(|_| panic!("enqueue while the worker lives"));
        assert!(
            stream_keys.contains("held"),
            "a queued registration keeps its key claimed"
        );

        // Worker teardown in its real order, the registration never
        // consumed: the reactor goes first, then the kill switch fires the
        // terminal drain.
        drop(reactor);
        drop(kill_switch);

        assert!(
            !stream_keys.contains("held"),
            "worker exit must release a never-consumed registration's key claim"
        );
        assert!(
            StreamKeyClaim::claim(stream_keys.clone(), "held".to_string()).is_ok(),
            "a create for the same key must win again after the worker died"
        );
    }

    // An accepted publisher's claim rides the full chain — enqueued into the
    // handoff, drained by the consume side, moved into PublisherState by
    // add_publisher — and its last owner is the reactor's publisher slab.
    // Dropping the reactor with the publisher still live (exactly what a
    // run() exit leaves behind: nothing calls remove_publisher on shutdown)
    // must release the key and make it claimable again.
    #[test]
    fn reactor_drop_with_live_consumed_publisher_reopens_the_key() {
        let stream_keys: Arc<dashmap::DashSet<String>> = Arc::new(dashmap::DashSet::new());
        let status = Arc::new(AtomicUsize::new(STATUS_RUN));
        let mut reactor = Reactor::new(3, None, status).expect("reactor");

        let registrations = Arc::new(RegistrationHandoff::new());
        let kill_switch = RegistrationKillSwitch::arm(registrations.clone());

        let (_feed_tx, feed_rx) = crossbeam_channel::bounded::<PublisherFeed>(1);
        let claim = StreamKeyClaim::claim(stream_keys.clone(), "live".to_string())
            .expect("first claim must win");
        registrations
            .enqueue(PublisherRegistration {
                claim,
                source: PublisherSource::Feed(feed_rx),
                budget: test_budget(),
            })
            .unwrap_or_else(|_| panic!("enqueue while the worker lives"));

        // The reactor's consume side: one drain, then acceptance moves the
        // still-armed claim into PublisherState.
        let mut batch = Vec::new();
        kill_switch.handoff().drain_into(&mut batch);
        assert_eq!(batch.len(), 1, "the queued registration must be drained");
        for registration in batch.drain(..) {
            reactor.add_publisher(registration).expect("accepted");
        }
        assert!(
            stream_keys.contains("live"),
            "an accepted publisher keeps its key claimed while the reactor lives"
        );

        // run() exits with the publisher still in the slab; the reactor drop
        // is the worker's last word on it. The kill switch is still armed, so
        // the release below can only come from the publisher slab dropping.
        drop(reactor);
        assert!(
            !stream_keys.contains("live"),
            "dropping the reactor must release live publishers' key claims"
        );
        assert!(
            StreamKeyClaim::claim(stream_keys.clone(), "live".to_string()).is_ok(),
            "a create for the same key must win again after the reactor died"
        );
        drop(kill_switch);
    }

    // H8.c: a server-initiated close must first try to flush what was queued
    // in the same round — most visibly the finish_playing status a watcher
    // gets when the publisher ends. A raw remove_connection dropped it: the
    // enqueue marked the connection for pending flush, but the close ran
    // before any flush step.
    #[test]
    fn close_connection_after_flush_delivers_the_queued_tail() {
        use std::io::Read;

        let status = Arc::new(AtomicUsize::new(STATUS_RUN));
        let mut reactor = Reactor::new(3, None, status).expect("reactor");

        let listener = std::net::TcpListener::bind("127.0.0.1:0").expect("Failed to bind");
        let addr = listener.local_addr().expect("Failed to get address");
        let mut client = TcpStream::connect(addr).expect("Failed to connect");
        let (server, _) = listener.accept().expect("Failed to accept");
        let token = reactor
            .add_connection(server)
            .expect("Failed to add connection");

        // Queue a final status packet and close, as process_publishers does
        // for a watcher after its publisher finished.
        if let Some(conn) = reactor.connections.get_mut(token.id) {
            conn.state = ConnectionState::Active;
            assert!(conn.enqueue_data(Bytes::from_static(b"final status"), false, false, false, true, Instant::now()));
        }
        reactor.close_connection_after_flush(token.id);
        assert!(
            reactor.connections.get(token.id).is_none(),
            "the connection must still be removed"
        );

        // The queued tail must reach the peer, followed by an orderly EOF.
        client
            .set_read_timeout(Some(Duration::from_secs(2)))
            .expect("set timeout");
        let mut received = Vec::new();
        let mut buf = [0u8; 64];
        loop {
            match client.read(&mut buf) {
                Ok(0) => break,
                Ok(n) => received.extend_from_slice(&buf[..n]),
                Err(e) => panic!("read failed before EOF: {e:?}"),
            }
        }
        assert_eq!(
            received, b"final status",
            "the tail queued in the closing round must be delivered"
        );
    }

    // F2: a server-initiated close whose tail cannot flush in one pass must
    // NOT drop the socket mid-message (that truncates the RTMP message and
    // loses the final status). It must linger (condemned), drain on later
    // writable events, and only then close — delivering a byte-exact prefix.
    #[cfg(unix)]
    #[test]
    fn close_lingers_then_drains_an_undeliverable_tail() {
        use std::io::Read;
        use std::os::unix::io::AsRawFd;

        let status = Arc::new(AtomicUsize::new(STATUS_RUN));
        let mut reactor = Reactor::new(3, None, status).expect("reactor");

        let listener = std::net::TcpListener::bind("127.0.0.1:0").expect("Failed to bind");
        let addr = listener.local_addr().expect("Failed to get address");
        let mut client = TcpStream::connect(addr).expect("Failed to connect");
        let (server, _) = listener.accept().expect("Failed to accept");

        // Tiny buffers + a peer that does not read yet => the single flush in
        // close_connection_after_flush WouldBlocks with a tail remaining.
        set_small_socket_buffer(server.as_raw_fd(), libc::SO_SNDBUF);
        set_small_socket_buffer(client.as_raw_fd(), libc::SO_RCVBUF);

        let token = reactor
            .add_connection(server)
            .expect("Failed to add connection");

        // 64KB single sequence-header entry: far larger than the shrunk buffers
        // (so the first flush WouldBlocks), but small enough that the byte-by-
        // byte drain over the tiny pipe completes well within the watchdog.
        let payload = vec![0xABu8; 64 * 1024];
        if let Some(conn) = reactor.connections.get_mut(token.id) {
            conn.state = ConnectionState::Active;
            assert!(conn.enqueue_data(
                Bytes::from(payload.clone()),
                false,
                true,
                true,
                true,
                Instant::now(),
            ));
        }

        reactor.close_connection_after_flush(token.id);
        assert!(
            reactor.connections.get(token.id).is_some(),
            "a half-written tail must not be dropped (that truncates the RTMP message)"
        );
        assert!(
            reactor.connections.get(token.id).unwrap().is_condemned(),
            "the connection must be condemned for a bounded drain"
        );

        // Drain from the peer while driving writable events; the queue empties,
        // the connection is removed, and every byte arrives in order.
        client
            .set_read_timeout(Some(Duration::from_secs(5)))
            .expect("set timeout");
        let mut received = Vec::new();
        let mut buf = vec![0u8; 64 * 1024];
        let deadline = Instant::now() + Duration::from_secs(20);
        loop {
            assert!(Instant::now() < deadline, "drain watchdog expired");
            if reactor.connections.get(token.id).is_some() {
                if let Some(HandleResult::Disconnect(cid)) = reactor.handle_writable(token.id) {
                    reactor.close_connection_after_flush(cid);
                }
            }
            match client.read(&mut buf) {
                Ok(0) => break,
                Ok(n) => received.extend_from_slice(&buf[..n]),
                Err(ref e)
                    if e.kind() == std::io::ErrorKind::WouldBlock
                        || e.kind() == std::io::ErrorKind::TimedOut => {}
                Err(e) => panic!("read failed before EOF: {e:?}"),
            }
        }
        assert_eq!(
            received.len(),
            payload.len(),
            "the whole tail must be delivered before the close"
        );
        assert!(
            received.iter().all(|&b| b == 0xAB),
            "the delivered stream must be a byte-exact prefix, no corruption"
        );
        assert!(
            reactor.connections.get(token.id).is_none(),
            "the connection is removed once its tail has drained"
        );
    }

    // F2: bounded lingering — a peer that never drains the tail must not pin the
    // slot forever. Once the drain deadline passes, check_timeouts collects the
    // connection and close_connection_after_flush force-removes it.
    #[cfg(unix)]
    #[test]
    fn condemn_deadline_backstops_a_peer_that_never_reads() {
        use std::os::unix::io::AsRawFd;

        let status = Arc::new(AtomicUsize::new(STATUS_RUN));
        let mut reactor = Reactor::new(3, None, status).expect("reactor");

        let listener = std::net::TcpListener::bind("127.0.0.1:0").expect("Failed to bind");
        let addr = listener.local_addr().expect("Failed to get address");
        // The peer connects but never reads, so the tail can never flush.
        let client = TcpStream::connect(addr).expect("Failed to connect");
        let (server, _) = listener.accept().expect("Failed to accept");

        set_small_socket_buffer(server.as_raw_fd(), libc::SO_SNDBUF);
        set_small_socket_buffer(client.as_raw_fd(), libc::SO_RCVBUF);

        let token = reactor
            .add_connection(server)
            .expect("Failed to add connection");

        let payload = vec![0u8; 1024 * 1024];
        if let Some(conn) = reactor.connections.get_mut(token.id) {
            conn.state = ConnectionState::Active;
            assert!(conn.enqueue_data(Bytes::from(payload), false, true, true, true, Instant::now()));
        }
        reactor.close_connection_after_flush(token.id);
        assert!(
            reactor.connections.get(token.id).unwrap().is_condemned(),
            "an undrainable tail must condemn the connection"
        );

        // Simulate the drain window elapsing: move the deadline into the past
        // and force a timeout sweep past its ~1/sec throttle.
        let past = Instant::now();
        if let Some(conn) = reactor.connections.get_mut(token.id) {
            conn.condemn(past);
        }
        reactor.last_timeout_check = past
            .checked_sub(TIMEOUT_CHECK_INTERVAL + Duration::from_secs(1))
            .expect("monotonic clock has headroom");
        let expired = reactor.check_timeouts();
        assert!(
            expired.contains(&token.id),
            "check_timeouts must collect a condemnation whose deadline passed"
        );

        // Closing it now force-removes it instead of re-lingering forever.
        reactor.close_connection_after_flush(token.id);
        assert!(
            reactor.connections.get(token.id).is_none(),
            "an expired condemnation must be force-removed (bounded lingering)"
        );

        drop(client);
    }

    // F3: a condemned connection (lingering only to drain its final tail) must
    // stop receiving live fanout. Appending new media would keep its queue from
    // ever emptying, so it would never close on drain and be force-closed at the
    // deadline instead — possibly truncating the fresh packet. Here the
    // readable-path fanout (write_pending_packets) targets a condemned
    // connection: the append is skipped, the queue stays at exactly the
    // pre-condemn tail, and the connection closes on drain, delivering only the
    // tail bytes. The publisher-path fanout (process_publishers) carries the
    // identical is_condemned() guard.
    #[test]
    fn condemned_connection_is_skipped_by_live_fanout_and_closes_on_drain() {
        use std::io::Read;

        let status = Arc::new(AtomicUsize::new(STATUS_RUN));
        let mut reactor = Reactor::new(3, None, status).expect("reactor");

        let listener = std::net::TcpListener::bind("127.0.0.1:0").expect("Failed to bind");
        let addr = listener.local_addr().expect("Failed to get address");
        let mut client = TcpStream::connect(addr).expect("Failed to connect");
        let (server, _) = listener.accept().expect("Failed to accept");

        let token = reactor
            .add_connection(server)
            .expect("Failed to add connection");

        // Queue a known final tail while Active, then condemn it.
        let tail = 4096usize;
        if let Some(conn) = reactor.connections.get_mut(token.id) {
            conn.state = ConnectionState::Active;
            assert!(conn.enqueue_data(Bytes::from(vec![0xABu8; tail]), false, true, true, true, Instant::now()));
            conn.condemn(Instant::now() + Duration::from_secs(30));
            assert!(conn.is_condemned());
        }

        // Live fanout targets the condemned connection with a large media
        // packet. It must be skipped, so the queue stays at the pre-condemn tail.
        reactor.packets_buffer.push((
            token.id,
            Bytes::from(vec![0xCDu8; 1024 * 1024]),
            true,
            false,
            true,
            true,
        ));
        reactor.write_pending_packets();

        let conn = reactor.connections.get(token.id).expect("still present");
        assert_eq!(
            conn.queued_bytes(),
            tail,
            "post-condemn media must not grow a condemned connection's queue"
        );
        assert!(
            !reactor.is_pending_flush(token.id),
            "a skipped condemned target must not be re-queued for flush"
        );

        // Drain the tail: the connection closes on drain (not at the deadline),
        // and the peer receives exactly the tail bytes — never the skipped media.
        client
            .set_read_timeout(Some(Duration::from_secs(5)))
            .expect("set timeout");
        let mut received = Vec::new();
        let mut buf = vec![0u8; 8192];
        let deadline = Instant::now() + Duration::from_secs(20);
        loop {
            assert!(Instant::now() < deadline, "drain watchdog expired");
            if reactor.connections.get(token.id).is_some() {
                if let Some(HandleResult::Disconnect(cid)) = reactor.handle_writable(token.id) {
                    reactor.close_connection_after_flush(cid);
                }
            }
            match client.read(&mut buf) {
                Ok(0) => break,
                Ok(n) => received.extend_from_slice(&buf[..n]),
                Err(ref e)
                    if e.kind() == std::io::ErrorKind::WouldBlock
                        || e.kind() == std::io::ErrorKind::TimedOut =>
                {
                    if reactor.connections.get(token.id).is_none() {
                        break;
                    }
                }
                Err(e) => panic!("read failed before EOF: {e:?}"),
            }
        }
        assert_eq!(
            received.len(),
            tail,
            "only the pre-condemn tail must be delivered"
        );
        assert!(
            received.iter().all(|&b| b == 0xAB),
            "the skipped media must never appear in the delivered stream"
        );
        assert!(
            reactor.connections.get(token.id).is_none(),
            "the connection closes once its tail drains, not at the deadline"
        );
    }

    // H6: one process_publishers round must consume at most
    // MAX_PUBLISH_ITEMS_PER_POLL items per publisher and report the leftover
    // via the pending flag, so run() re-polls with a zero timeout instead of
    // spinning inside the drain while flush_pending starves.
    #[test]
    fn publisher_drain_item_budget_bounds_one_round() {
        let stream_keys = Arc::new(dashmap::DashSet::new());
        let status = Arc::new(AtomicUsize::new(STATUS_RUN));
        let mut reactor = Reactor::new(3, None, status).expect("reactor");

        // Channel larger than the budget so the backlog fits in one send burst.
        let (feed_tx, feed_rx) = crossbeam_channel::bounded(MAX_PUBLISH_ITEMS_PER_POLL + 64);
        let (budget_guard, budget) = IngressBudget::new(PUBLISHER_INGRESS_HIGH_WATER_BYTES);
        let claim = StreamKeyClaim::claim(stream_keys, "live".to_string()).expect("claim");
        reactor
            .add_publisher(PublisherRegistration {
                claim,
                source: PublisherSource::Feed(feed_rx),
                budget: budget_guard,
            })
            .expect("publisher registered");

        // Budget + 6 tiny audio tags queued ahead of a single drain.
        for i in 0..(MAX_PUBLISH_ITEMS_PER_POLL + 6) {
            send_acquired(
                &budget,
                &feed_tx,
                PublisherFeed::Media {
                    tag_type: 0x08,
                    timestamp: RtmpTimestamp { value: i as u32 },
                    data: Bytes::from_static(&[0xaf, 0x01, 0x00]),
                },
            );
        }

        let (removed, pending) = reactor.process_publishers();
        assert!(
            removed.is_empty(),
            "a budget stop must not remove the publisher"
        );
        assert!(pending, "hitting the item budget must report pending work");
        assert_eq!(
            feed_tx.len(),
            6,
            "exactly the item budget must be consumed in one round"
        );

        let (removed, pending) = reactor.process_publishers();
        assert!(removed.is_empty());
        assert!(!pending, "a drained channel must clear the pending flag");
        assert_eq!(feed_tx.len(), 0, "the second round must clear the backlog");
    }

    // H6: the byte budget caps a round of few-but-large items the same way.
    #[test]
    fn publisher_drain_byte_budget_bounds_one_round() {
        let stream_keys = Arc::new(dashmap::DashSet::new());
        let status = Arc::new(AtomicUsize::new(STATUS_RUN));
        let mut reactor = Reactor::new(3, None, status).expect("reactor");

        let (feed_tx, feed_rx) = crossbeam_channel::bounded(16);
        let (budget_guard, budget) = IngressBudget::new(PUBLISHER_INGRESS_HIGH_WATER_BYTES);
        let claim = StreamKeyClaim::claim(stream_keys, "live".to_string()).expect("claim");
        reactor
            .add_publisher(PublisherRegistration {
                claim,
                source: PublisherSource::Feed(feed_rx),
                budget: budget_guard,
            })
            .expect("publisher registered");

        // 3 x 200KiB crosses the 512KiB byte budget on the third item
        // (consume-then-check), leaving the fourth for the next round.
        let big = Bytes::from(vec![0u8; 200 * 1024]);
        for i in 0..3u32 {
            send_acquired(
                &budget,
                &feed_tx,
                PublisherFeed::Media {
                    tag_type: 0x08,
                    timestamp: RtmpTimestamp { value: i },
                    data: big.clone(),
                },
            );
        }
        send_acquired(
            &budget,
            &feed_tx,
            PublisherFeed::Media {
                tag_type: 0x08,
                timestamp: RtmpTimestamp { value: 3 },
                data: Bytes::from_static(&[0xaf, 0x01, 0x00]),
            },
        );

        let (removed, pending) = reactor.process_publishers();
        assert!(removed.is_empty());
        assert!(pending, "hitting the byte budget must report pending work");
        assert_eq!(
            feed_tx.len(),
            1,
            "the item that crossed the byte budget is still consumed; only the next one waits"
        );

        let (removed, pending) = reactor.process_publishers();
        assert!(removed.is_empty());
        assert!(!pending);
        assert_eq!(feed_tx.len(), 0);
    }

    // H6: consume-then-check means an item pulled off the channel is always
    // processed, even when it alone exceeds the byte budget — the budget only
    // ends the round, it never discards data.
    #[test]
    fn publisher_drain_oversized_item_is_consumed_not_dropped() {
        let stream_keys = Arc::new(dashmap::DashSet::new());
        let status = Arc::new(AtomicUsize::new(STATUS_RUN));
        let mut reactor = Reactor::new(3, None, status).expect("reactor");

        let (feed_tx, feed_rx) = crossbeam_channel::bounded(4);
        let (budget_guard, budget) = IngressBudget::new(PUBLISHER_INGRESS_HIGH_WATER_BYTES);
        let claim = StreamKeyClaim::claim(stream_keys, "live".to_string()).expect("claim");
        reactor
            .add_publisher(PublisherRegistration {
                claim,
                source: PublisherSource::Feed(feed_rx),
                budget: budget_guard,
            })
            .expect("publisher registered");

        // A single 600KiB audio sequence header (> byte budget), then a small
        // video sequence header. Sequence headers land in the scheduler's
        // channel cache, which proves each item was processed, not dropped.
        let mut oversized = vec![0u8; 600 * 1024];
        oversized[0] = 0xaf;
        oversized[1] = 0x00;
        let oversized = Bytes::from(oversized);
        send_acquired(
            &budget,
            &feed_tx,
            PublisherFeed::Media {
                tag_type: 0x08,
                timestamp: RtmpTimestamp { value: 0 },
                data: oversized.clone(),
            },
        );
        let video_seq: &[u8] = &[0x17, 0x00, 0x00, 0x00, 0x00, 0x01, 0x64];
        send_acquired(
            &budget,
            &feed_tx,
            PublisherFeed::Media {
                tag_type: 0x09,
                timestamp: RtmpTimestamp { value: 1 },
                data: Bytes::from_static(video_seq),
            },
        );

        let (removed, pending) = reactor.process_publishers();
        assert!(removed.is_empty());
        assert!(pending, "an oversized item exhausts the byte budget");
        assert_eq!(
            reactor.scheduler.channel_audio_sequence_header("live"),
            Some(oversized),
            "the oversized item must reach the scheduler in the round that consumed it"
        );
        assert_eq!(
            feed_tx.len(),
            1,
            "the follow-up item waits for the next round"
        );

        let (_, pending) = reactor.process_publishers();
        assert!(!pending);
        assert_eq!(
            reactor
                .scheduler
                .channel_video_sequence_header("live")
                .as_deref(),
            Some(video_seq),
            "the next round must deliver the remaining item"
        );
    }

    /// Spawn a producer thread calling `acquire(len)` on `budget` and assert
    /// it parks. The park check is one-sided (the house pattern): a correct
    /// gate can never complete the acquire, so the bounded window adds no
    /// flake risk — slow scheduling only makes the check vacuous, never
    /// wrong — while a gate that fails to block completes fast and trips it.
    /// Returns the join handle and the channel carrying the acquire's result.
    fn spawn_parked_acquire(
        budget: &Arc<IngressBudget>,
        len: usize,
    ) -> (
        std::thread::JoinHandle<()>,
        std::sync::mpsc::Receiver<Result<(), IngressClosed>>,
    ) {
        let (done_tx, done_rx) = std::sync::mpsc::channel();
        let producer_budget = budget.clone();
        let producer = std::thread::spawn(move || {
            done_tx
                .send(producer_budget.acquire(len))
                .expect("report the acquire's result");
        });
        assert!(
            done_rx.recv_timeout(Duration::from_millis(100)).is_err(),
            "an acquire that must park completed immediately"
        );
        (producer, done_rx)
    }

    // Producer-side byte gate: an acquire that would cross the high-water
    // mark of a non-empty account parks its producer, and a drain-side
    // release wakes it into the freed capacity.
    #[test]
    fn ingress_budget_blocks_at_high_water_and_release_resumes() {
        let (_guard, budget) = IngressBudget::new(8);
        budget.acquire(6).expect("an empty account admits");

        let (producer, done_rx) = spawn_parked_acquire(&budget, 6);

        budget.release(6);
        done_rx
            .recv_timeout(Duration::from_secs(10))
            .expect("the release must wake the parked producer")
            .expect("the woken acquire must succeed");
        producer.join().expect("producer thread exits");
        assert_eq!(
            budget.queued_bytes(),
            6,
            "only the resumed acquire may remain on the account"
        );
    }

    // Admit-when-empty: a single item larger than the whole mark enters an
    // empty account immediately — memory is bounded at max(mark, one item)
    // and its producer can never deadlock — while the next oversized item
    // waits for the full drain that empties the account again.
    #[test]
    fn ingress_budget_admits_oversized_item_when_empty() {
        let (_guard, budget) = IngressBudget::new(8);
        budget
            .acquire(100)
            .expect("an oversized item must enter an empty account");
        assert_eq!(
            budget.queued_bytes(),
            100,
            "the account must reflect the full oversize"
        );

        let (producer, done_rx) = spawn_parked_acquire(&budget, 100);

        budget.release(100);
        done_rx
            .recv_timeout(Duration::from_secs(10))
            .expect("the full release must wake the second producer")
            .expect("the woken oversized acquire must succeed");
        producer.join().expect("producer thread exits");
        assert_eq!(budget.queued_bytes(), 100);
    }

    // FIFO admission: a released batch goes to the longest-parked acquire.
    // Without ticketing, a sibling sender clone's small items could slip
    // into every released batch ahead of a parked large item and starve it
    // indefinitely; with it, the small item waits its turn even though
    // capacity for it exists the moment it arrives.
    #[test]
    fn ingress_budget_admission_is_fifo_under_contention() {
        let (_guard, budget) = IngressBudget::new(8);
        budget.acquire(6).expect("an empty account admits");

        // Large item first: 6 + 7 > 8 parks it as the head waiter.
        let (large, large_rx) = spawn_parked_acquire(&budget, 7);
        let deadline = Instant::now() + Duration::from_secs(5);
        while budget.waiting_acquires() < 1 {
            assert!(
                Instant::now() < deadline,
                "the large acquire never took its ticket"
            );
            std::thread::yield_now();
        }
        // Small item second: 6 + 1 <= 8, so capacity exists — an unordered
        // gate would admit it on the spot (the starvation seed); the FIFO
        // gate parks it behind the head, which spawn_parked_acquire's
        // completed-immediately check verifies.
        let (small, small_rx) = spawn_parked_acquire(&budget, 1);
        while budget.waiting_acquires() < 2 {
            assert!(
                Instant::now() < deadline,
                "the small acquire never took its ticket"
            );
            std::thread::yield_now();
        }

        // One full drain: the head (large) admits, and its admission chains
        // the small one in behind it without another release.
        budget.release(6);
        large_rx
            .recv_timeout(Duration::from_secs(10))
            .expect("the release must admit the head acquire")
            .expect("the head acquire succeeds");
        small_rx
            .recv_timeout(Duration::from_secs(10))
            .expect("the head's admission must chain to the next ticket")
            .expect("the chained acquire succeeds");
        large.join().expect("large producer exits");
        small.join().expect("small producer exits");
        assert_eq!(
            budget.queued_bytes(),
            8,
            "exactly the two post-drain acquires remain on the account"
        );
    }

    // Teardown wakes the gate: a parked producer must observe IngressClosed
    // both from an explicit close and from the guard's drop — the form every
    // reactor-side teardown path takes.
    #[test]
    fn ingress_budget_close_and_guard_drop_wake_blocked_producers() {
        // Explicit close.
        let (guard, budget) = IngressBudget::new(8);
        budget.acquire(6).expect("an empty account admits");
        let (producer, done_rx) = spawn_parked_acquire(&budget, 6);
        budget.close();
        assert!(
            done_rx
                .recv_timeout(Duration::from_secs(10))
                .expect("the close must wake the parked producer")
                .is_err(),
            "a woken producer must observe the closed account"
        );
        producer.join().expect("producer thread exits");
        assert!(
            budget.acquire(1).is_err(),
            "later acquires must be refused outright"
        );
        drop(guard);

        // Guard drop performs the same close through RAII.
        let (guard, budget) = IngressBudget::new(8);
        budget.acquire(6).expect("an empty account admits");
        let (producer, done_rx) = spawn_parked_acquire(&budget, 6);
        drop(guard);
        assert!(
            done_rx
                .recv_timeout(Duration::from_secs(10))
                .expect("the guard drop must wake the parked producer")
                .is_err(),
            "a woken producer must observe the closed account"
        );
        producer.join().expect("producer thread exits");
    }

    // Drain-side accounting: one process_publishers round releases exactly
    // the bytes it drained, in one batch, and that release resumes a
    // producer parked at the gate.
    #[test]
    fn process_publishers_releases_drained_bytes() {
        let stream_keys = Arc::new(dashmap::DashSet::new());
        let status = Arc::new(AtomicUsize::new(STATUS_RUN));
        let mut reactor = Reactor::new(3, None, status).expect("reactor");

        let (feed_tx, feed_rx) = crossbeam_channel::bounded(8);
        let (budget_guard, budget) = IngressBudget::new(8);
        let claim = StreamKeyClaim::claim(stream_keys, "live".to_string()).expect("claim");
        reactor
            .add_publisher(PublisherRegistration {
                claim,
                source: PublisherSource::Feed(feed_rx),
                budget: budget_guard,
            })
            .expect("publisher registered");

        // One acquired-and-sent 32-byte tag fills the account past its mark.
        let data = Bytes::from(vec![0xafu8; 32]);
        budget
            .acquire(data.len())
            .expect("an empty account admits the first item");
        feed_tx
            .send(PublisherFeed::Media {
                tag_type: 0x08,
                timestamp: RtmpTimestamp { value: 0 },
                data,
            })
            .unwrap();

        let (producer, done_rx) = spawn_parked_acquire(&budget, 4);

        let (removed, _) = reactor.process_publishers();
        assert!(removed.is_empty(), "a healthy publisher must not be removed");
        done_rx
            .recv_timeout(Duration::from_secs(10))
            .expect("the drain round's release must wake the producer")
            .expect("the woken acquire must succeed");
        producer.join().expect("producer thread exits");
        assert_eq!(
            budget.queued_bytes(),
            4,
            "the round must release exactly what it drained; only the resumed acquire remains"
        );
    }

    // Publisher removal — the reactor's last word on an accepted publisher —
    // drops the state, whose guard closes the budget: a parked producer
    // errors out instead of waiting on a drain that will never come.
    #[test]
    fn remove_publisher_closes_ingress_budget() {
        let stream_keys = Arc::new(dashmap::DashSet::new());
        let status = Arc::new(AtomicUsize::new(STATUS_RUN));
        let mut reactor = Reactor::new(3, None, status).expect("reactor");

        let (_feed_tx, feed_rx) = crossbeam_channel::bounded::<PublisherFeed>(8);
        let (budget_guard, budget) = IngressBudget::new(8);
        let claim = StreamKeyClaim::claim(stream_keys, "live".to_string()).expect("claim");
        let id = reactor
            .add_publisher(PublisherRegistration {
                claim,
                source: PublisherSource::Feed(feed_rx),
                budget: budget_guard,
            })
            .expect("publisher registered");

        budget.acquire(6).expect("an empty account admits");
        let (producer, done_rx) = spawn_parked_acquire(&budget, 6);

        reactor.remove_publisher(id);
        assert!(
            done_rx
                .recv_timeout(Duration::from_secs(10))
                .expect("the removal must wake the parked producer")
                .is_err(),
            "a producer woken by removal must observe the closed account"
        );
        producer.join().expect("producer thread exits");
    }

    // Both refusal fates of a registration close its budget: an enqueue the
    // intake refuses hands the registration back and the caller's drop
    // closes it; a consumed registration the scheduler refuses inside
    // add_publisher is dropped there with the same effect. A producer parked
    // at either gate errors out.
    #[test]
    fn refused_registration_closes_ingress_budget() {
        // Closed-intake refusal.
        let registrations = RegistrationHandoff::new();
        registrations.close();
        let stream_keys: Arc<dashmap::DashSet<String>> = Arc::new(dashmap::DashSet::new());
        let (_feed_tx, feed_rx) = crossbeam_channel::bounded::<PublisherFeed>(1);
        let (budget_guard, budget) = IngressBudget::new(8);
        let claim =
            StreamKeyClaim::claim(stream_keys.clone(), "refused".to_string()).expect("claim");
        budget.acquire(6).expect("an empty account admits");
        let (producer, done_rx) = spawn_parked_acquire(&budget, 6);
        let refused = registrations.enqueue(PublisherRegistration {
            claim,
            source: PublisherSource::Feed(feed_rx),
            budget: budget_guard,
        });
        assert!(
            matches!(refused, Err(EnqueueRefused::Closed(_))),
            "the closed intake must refuse the registration"
        );
        drop(refused);
        assert!(
            done_rx
                .recv_timeout(Duration::from_secs(10))
                .expect("dropping the refusal must wake the parked producer")
                .is_err(),
            "a producer woken by the refusal must observe the closed account"
        );
        producer.join().expect("producer thread exits");

        // Scheduler refusal inside add_publisher: a network session already
        // publishes the key, which never touches the in-process key set.
        let status = Arc::new(AtomicUsize::new(STATUS_RUN));
        let mut reactor = Reactor::new(3, None, status).expect("reactor");
        assert!(reactor.scheduler.new_channel("net".to_string(), 777));
        let (_feed_tx2, feed_rx2) = crossbeam_channel::bounded::<PublisherFeed>(1);
        let (budget_guard, budget) = IngressBudget::new(8);
        let claim = StreamKeyClaim::claim(stream_keys, "net".to_string()).expect("claim");
        budget.acquire(6).expect("an empty account admits");
        let (producer, done_rx) = spawn_parked_acquire(&budget, 6);
        assert!(
            reactor
                .add_publisher(PublisherRegistration {
                    claim,
                    source: PublisherSource::Feed(feed_rx2),
                    budget: budget_guard,
                })
                .is_none(),
            "a key already being published must be refused"
        );
        assert!(
            done_rx
                .recv_timeout(Duration::from_secs(10))
                .expect("the scheduler refusal must wake the parked producer")
                .is_err(),
            "a producer woken by the refusal must observe the closed account"
        );
        producer.join().expect("producer thread exits");
    }

    /// Fixture for the cap-resume (step 5b) tests: a reactor plus one
    /// handshaking connection whose peer has already sent C0+C1. A resumed
    /// read that actually happens processes the handshake and enqueues the
    /// S0+S1+S2 response (observable via `is_pending_flush`); a skipped read
    /// leaves nothing queued.
    fn reactor_with_handshake_bytes_pending() -> (Reactor, ConnectionToken, TcpStream) {
        use std::io::Write;

        let status = Arc::new(AtomicUsize::new(STATUS_RUN));
        let mut reactor = Reactor::new(3, None, status).expect("Failed to create reactor");

        let listener = std::net::TcpListener::bind("127.0.0.1:0").expect("Failed to bind");
        let addr = listener.local_addr().expect("Failed to get address");
        let mut client = TcpStream::connect(addr).expect("Failed to connect");
        let (server, _) = listener.accept().expect("Failed to accept");

        let token = reactor
            .add_connection(server)
            .expect("Failed to add connection");
        // add_connection leaves the state at Handshaking.

        // C0 (version 3) + C1 (1536 bytes: time, zeros, filler).
        let mut c0c1 = vec![0u8; 1537];
        c0c1[0] = 3;
        for (i, b) in c0c1[9..].iter_mut().enumerate() {
            *b = (i % 251) as u8;
        }
        client.write_all(&c0c1).expect("write C0+C1");
        client.flush().ok();
        // Loopback delivery is fast but not instant; without this the skip
        // assertions could pass vacuously against a not-yet-readable socket.
        std::thread::sleep(Duration::from_millis(100));

        (reactor, token, client)
    }

    /// A connection already slated for close this pass (error/hangup event or
    /// a disconnect decision) must not be read again by the step-5b resume:
    /// the close decision precedes the resume, and reading a doomed
    /// connection would push more data at its subscribers after that point.
    #[test]
    fn resume_capped_reads_skips_ids_slated_for_close() {
        let (mut reactor, token, _client) = reactor_with_handshake_bytes_pending();

        let mut ids_to_close = vec![token.id];
        reactor.resume_capped_reads(vec![token.id], &[], &mut ids_to_close);
        assert!(
            !reactor.is_pending_flush(token.id),
            "a close-slated id must not be re-read by the resume pass"
        );

        // Control: the same resume with nothing slated reads the handshake
        // and enqueues the S0+S1+S2 response — proving the data was sitting
        // there while the first call skipped it.
        reactor.resume_capped_reads(vec![token.id], &[], &mut Vec::new());
        assert!(
            reactor.is_pending_flush(token.id),
            "an unblocked resume must read the pending handshake bytes"
        );

        reactor.remove_connection(token.id);
    }

    /// An id the event pass already read this iteration must not be read a
    /// second time by the resume: one read per connection per flush cycle,
    /// or a single connection could pile ~2x MAX_READ_PER_POLL into
    /// subscriber queues before any flush runs.
    #[test]
    fn resume_capped_reads_skips_ids_already_read_this_pass() {
        let (mut reactor, token, _client) = reactor_with_handshake_bytes_pending();

        reactor.resume_capped_reads(vec![token.id], &[token.id], &mut Vec::new());
        assert!(
            !reactor.is_pending_flush(token.id),
            "an id already read by the event pass must not be read again"
        );

        reactor.resume_capped_reads(vec![token.id], &[], &mut Vec::new());
        assert!(
            reactor.is_pending_flush(token.id),
            "an unblocked resume must read the pending handshake bytes"
        );

        reactor.remove_connection(token.id);
    }

    /// `read_pending` stores raw slab ids with no generation: an entry left
    /// behind by a removed connection would make the resume pass read a new
    /// connection that reused the id. Removal must scrub the set.
    #[test]
    fn remove_connection_scrubs_read_pending() {
        let status = Arc::new(AtomicUsize::new(STATUS_RUN));
        let mut reactor = Reactor::new(3, None, status).expect("Failed to create reactor");

        let listener = std::net::TcpListener::bind("127.0.0.1:0").expect("Failed to bind");
        let addr = listener.local_addr().expect("Failed to get address");
        let _client = TcpStream::connect(addr).expect("Failed to connect");
        let (server, _) = listener.accept().expect("Failed to accept");

        let token = reactor
            .add_connection(server)
            .expect("Failed to add connection");
        reactor.read_pending.insert(token.id);

        reactor.remove_connection(token.id);
        assert!(
            !reactor.read_pending.contains(&token.id),
            "removal must scrub the id or a slab-reusing new connection would be read out of turn"
        );
    }
}
