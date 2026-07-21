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
use log::{debug, error, info};
use rml_rtmp::chunk_io::ChunkSerializer;
use rml_rtmp::handshake::{Handshake, HandshakeProcessResult, PeerType};
use rml_rtmp::messages::RtmpMessage;
use rml_rtmp::rml_amf0::Amf0Value;
use rml_rtmp::time::RtmpTimestamp;
use std::collections::{HashMap, HashSet};
use std::io::{self, Read};
use std::net::{Shutdown, TcpStream};
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::Arc;
use std::time::{Duration, Instant};

// ============================================================================
// Constants
// ============================================================================

const READ_BUFFER_SIZE: usize = 8192;
const POLL_TIMEOUT_MS: u64 = 100;
const CONNECTION_TIMEOUT_SECS: u64 = 60; // Connection timeout
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
/// of one drain.
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
const DEFAULT_MAX_CONNECTIONS: usize = 10000; // Default max connections (auto-adjusted by system FD limit)
#[cfg(windows)]
const DEFAULT_MAX_CONNECTIONS_WINDOWS: usize = 8000; // Conservative default for Windows (no direct FD limit API)
/// Extra capacity for bounded channel to absorb connection bursts.
/// Used when creating the connection channel between accept thread and reactor.
pub const CHANNEL_HEADROOM: usize = 256;

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
    // Ensure at least 1 connection is allowed
    result.max(1)
}

// ============================================================================
// Connection Token - Prevents ID reuse conflicts
// ============================================================================

/// Connection token - Contains ID and generation counter
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub struct ConnectionToken {
    /// Connection ID (slab index)
    pub id: usize,
    /// Generation counter - Incremented each time ID is reused
    pub generation: u32,
}

impl ConnectionToken {
    fn new(id: usize, generation: u32) -> Self {
        Self { id, generation }
    }

    /// Encode token for poller (combines id and generation)
    ///
    /// Layout: [generation: 32 bits][id: 32 bits]
    /// This allows validation of stale events from closed connections
    #[cfg(target_pointer_width = "64")]
    fn to_poller_token(&self) -> usize {
        ((self.generation as usize) << 32) | (self.id & 0xFFFFFFFF)
    }

    /// Decode token from poller event
    #[cfg(target_pointer_width = "64")]
    fn from_poller_token(token: usize) -> Self {
        let id = token & 0xFFFFFFFF;
        let generation = (token >> 32) as u32;
        Self { id, generation }
    }

    /// Fallback for 32-bit systems - no generation encoding possible
    #[cfg(target_pointer_width = "32")]
    fn to_poller_token(&self) -> usize {
        self.id
    }

    /// Fallback for 32-bit systems
    #[cfg(target_pointer_width = "32")]
    fn from_poller_token(token: usize) -> Self {
        Self {
            id: token,
            generation: 0,
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

    /// Enqueue data
    pub fn enqueue_data(
        &mut self,
        data: Bytes,
        is_keyframe: bool,
        is_sequence_header: bool,
        is_video: bool,
    ) -> bool {
        let result = self
            .write_queue
            .enqueue(data, is_keyframe, is_sequence_header, is_video);

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
        if !self
            .write_queue
            .enqueue(Bytes::from(data), false, false, false)
        {
            self.state = ConnectionState::Closing;
            return false;
        }
        true
    }

    /// Try to flush write queue (drain until WouldBlock)
    ///
    /// Returns whether connection should be disconnected
    pub fn try_flush(&mut self) -> io::Result<bool> {
        if self.write_queue.is_empty() {
            return Ok(false);
        }

        match self.write_queue.try_flush(&mut self.socket) {
            Ok(FlushResult::Complete { bytes_written }) => {
                if bytes_written > 0 {
                    self.last_write_activity = Instant::now();
                }
                Ok(false)
            }
            Ok(FlushResult::WouldBlock { bytes_written }) => {
                if bytes_written > 0 {
                    self.last_write_activity = Instant::now();
                }
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

/// Publisher state
pub struct PublisherState {
    pub stream_key: String,
    pub source: PublisherSource,
}

/// Route a batch of [`ServerResult`]s into the reactor's write / close buffers.
fn collect_server_results(
    server_results: Vec<ServerResult>,
    packets_to_write: &mut Vec<(usize, Vec<u8>, bool, bool, bool)>,
    ids_to_close: &mut Vec<usize>,
) {
    for result in server_results {
        match result {
            ServerResult::OutboundPacket {
                target_connection_id,
                packet,
                is_keyframe,
                is_sequence_header,
                is_video,
            } => {
                packets_to_write.push((
                    target_connection_id,
                    packet.bytes,
                    is_keyframe,
                    is_sequence_header,
                    is_video,
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
    generations: HashMap<usize, u32>,
    /// Business scheduler
    scheduler: RtmpScheduler,
    /// Publishers
    publishers: slab::Slab<PublisherState>,
    /// stream_key set, shared with the owning `EmbedRtmpServer`. Must be the
    /// `Arc` the server also reads: `DashSet`'s `Clone` is a deep copy, so a
    /// by-value set here would leave the server's duplicate-key check reading
    /// a set nobody writes to and `RtmpStreamAlreadyExists` dead code.
    stream_keys: Arc<dashmap::DashSet<String>>,
    /// Stop flag
    status: Arc<AtomicUsize>,
    /// Maximum allowed connections (auto-adjusted by system FD limit)
    max_connections: usize,
    /// Connections with pending writes that need flushing (dirty tracking for O(m) instead of O(n))
    pending_flush: HashSet<usize>,
    /// Connections that stopped at MAX_READ_PER_POLL and must be re-drained
    /// next loop iteration. An edge-triggered poller (EPOLLET/EV_CLEAR) fires no
    /// new readable event for bytes already in the kernel buffer, so we resume
    /// the drain ourselves rather than wait for the peer. Unlike `pending_flush`
    /// (drained within the same iteration) this set crosses iterations, so ids
    /// must be scrubbed on connection removal: the slab reuses ids and a stale
    /// entry would read a brand-new connection out of turn.
    read_pending: HashSet<usize>,
    /// Connections whose poller interest may need updating (dirty tracking for O(m) instead of O(n))
    interest_dirty: HashSet<usize>,
    /// Reusable buffer for connection IDs (to avoid Vec allocation in hot path)
    #[allow(dead_code)]
    conn_ids_buffer: Vec<usize>,
    /// Reusable buffer for packets to write (avoids allocation in handle_readable)
    packets_buffer: Vec<(usize, Vec<u8>, bool, bool, bool)>,
    /// Reusable buffer for IDs to close (avoids allocation in handle_readable)
    ids_to_close_buffer: Vec<usize>,
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
    /// * `stream_keys` - Shared set of active stream keys
    /// * `status` - Shared status flag for graceful shutdown
    ///
    /// The effective max_connections is calculated as:
    /// `min(config_value, 0.8 * system_fd_limit)` to leave headroom for other FDs.
    pub fn new(
        gop_limit: usize,
        max_connections: Option<usize>,
        stream_keys: Arc<dashmap::DashSet<String>>,
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
            stream_keys,
            status,
            max_connections: effective_max,
            pending_flush: HashSet::with_capacity(256),
            read_pending: HashSet::new(),
            interest_dirty: HashSet::with_capacity(256),
            conn_ids_buffer: Vec::with_capacity(1024),
            packets_buffer: Vec::with_capacity(64),
            ids_to_close_buffer: Vec::with_capacity(16),
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
            self.interest_dirty.insert(id);
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
    /// In-process publishers claim their key in `stream_keys` at create time
    /// (insert-or-fail), so success does not insert here. A refused
    /// registration releases the claim — `new_channel` can refuse a key a
    /// network session is already publishing to, and leaving the claim in
    /// place would block that key for in-process creates forever.
    pub fn add_publisher(&mut self, stream_key: String, source: PublisherSource) -> Option<usize> {
        let entry = self.publishers.vacant_entry();
        let id = entry.key();

        if self.scheduler.new_channel(stream_key.clone(), id) {
            entry.insert(PublisherState { stream_key, source });
            debug!("Publisher {} added", id);
            Some(id)
        } else {
            self.stream_keys.remove(&stream_key);
            None
        }
    }

    /// Remove publishers
    pub fn remove_publisher(&mut self, id: usize) {
        if let Some(pub_state) = self.publishers.try_remove(id) {
            self.scheduler.notify_publisher_closed(id);
            self.stream_keys.remove(&pub_state.stream_key);
            debug!("Publisher {} removed", id);
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
            // On 64-bit: validate generation matches
            // On 32-bit: generation is always 0, so this check passes
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
            self.pending_flush.insert(id);
            self.interest_dirty.insert(id);
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
        match self
            .scheduler
            .bytes_received_with_backlog(id, data, backlog)
        {
            Ok(server_results) => {
                for result in server_results {
                    match result {
                        ServerResult::OutboundPacket {
                            target_connection_id,
                            packet,
                            is_keyframe,
                            is_sequence_header,
                            is_video,
                        } => {
                            self.packets_buffer.push((
                                target_connection_id,
                                packet.bytes,
                                is_keyframe,
                                is_sequence_header,
                                is_video,
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

    /// Write pending packets to target connections
    fn write_pending_packets(&mut self) {
        // Collect IDs that successfully enqueued data for dirty marking
        let mut enqueued_ids = Vec::new();

        for (target_id, data, is_keyframe, is_sequence_header, is_video) in
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
                    Bytes::from(data),
                    is_keyframe,
                    is_sequence_header,
                    is_video,
                );
                if enqueued {
                    enqueued_ids.push(target_id);
                } else {
                    // Backpressure too high, cannot enqueue, close target connection
                    self.ids_to_close_buffer.push(target_id);
                }
            }
        }

        // Mark all connections that received data for pending flush and interest update
        for id in enqueued_ids {
            self.pending_flush.insert(id);
            self.interest_dirty.insert(id);
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
                    self.interest_dirty.insert(id);
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
        packets_to_write: &mut Vec<(usize, Vec<u8>, bool, bool, bool)>,
        ids_to_close: &mut Vec<usize>,
    ) -> bool {
        match self.scheduler.publish_bytes_received(pub_id, bytes) {
            Ok(server_results) => {
                collect_server_results(server_results, packets_to_write, ids_to_close);
                true
            }
            Err(e) => {
                debug!("Publisher {} scheduler error: {}", pub_id, e);
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
            let source = {
                let pub_state = match self.publishers.get(pub_id) {
                    Some(p) => p,
                    None => continue,
                };
                pub_state.source.clone()
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
                                let results = self.scheduler.abort_publisher_watchers(pub_id);
                                collect_server_results(
                                    results,
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
                                let results = self.scheduler.abort_publisher_watchers(pub_id);
                                collect_server_results(
                                    results,
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
                            let results = self
                                .scheduler
                                .publish_media_received(pub_id, tag_type, timestamp, data);
                            collect_server_results(
                                results,
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
        }

        // Write pending packets and collect IDs that successfully enqueued
        let mut enqueued_ids = Vec::new();
        for (target_id, data, is_keyframe, is_sequence_header, is_video) in packets_to_write {
            if let Some(target_conn) = self.connections.get_mut(target_id) {
                // Same rule as the readable-path fanout: never append new live
                // media to a condemned connection — its final tail must drain and
                // close, not be reopened by a post-condemn packet.
                if target_conn.is_condemned() {
                    continue;
                }
                let enqueued = target_conn.enqueue_data(
                    Bytes::from(data),
                    is_keyframe,
                    is_sequence_header,
                    is_video,
                );
                if enqueued {
                    enqueued_ids.push(target_id);
                } else {
                    // Backpressure too high, close target connection
                    ids_to_close.push(target_id);
                }
            }
        }

        // Mark all connections that received data for pending flush and interest update
        for id in enqueued_ids {
            self.pending_flush.insert(id);
            self.interest_dirty.insert(id);
        }

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
        packets: &mut Vec<(usize, Vec<u8>, bool, bool, bool)>,
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
                match self.scheduler.publish_bytes_received(pub_id, packet.bytes) {
                    Ok(server_results) => {
                        for result in server_results {
                            match result {
                                ServerResult::OutboundPacket {
                                    target_connection_id,
                                    packet,
                                    is_keyframe,
                                    is_sequence_header,
                                    is_video,
                                } => {
                                    packets.push((
                                        target_connection_id,
                                        packet.bytes,
                                        is_keyframe,
                                        is_sequence_header,
                                        is_video,
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

        // Drain pending_flush to get IDs that need flushing
        let pending_ids: Vec<usize> = self.pending_flush.drain().collect();

        for id in pending_ids {
            if let Some(conn) = self.connections.get_mut(id) {
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
                            // NEW-RS-01: do NOT reinsert into pending_flush. A
                            // still-non-empty queue here means try_flush stopped
                            // on WouldBlock (kernel send buffer full); retrying
                            // next loop would just burn a guaranteed-EAGAIN
                            // syscall. Marking interest_dirty (re)registers
                            // writable interest while data is pending, or clears
                            // it once drained (desired_interest() derives
                            // writable from has_pending_writes()); the poller's
                            // writable event then drives the next flush.
                            self.interest_dirty.insert(id);
                        }
                    }
                } else if conn.is_condemned() {
                    // Condemned and already drained (a media enqueue that then
                    // flushed elsewhere): close now rather than wait for the
                    // deadline.
                    ids_to_close.push(id);
                } else {
                    // No pending writes, ensure writable interest is cleared
                    self.interest_dirty.insert(id);
                }
            }
        }

        ids_to_close
    }

    /// Check timed out connections
    ///
    /// PERF-10: a 60s idle timeout does not need re-evaluating on every poll
    /// wakeup (the loop can wake thousands of times per second under load or
    /// ~10x/sec idle). Throttle the full slab scan to at most ~1/sec and read
    /// the clock once per sweep instead of once per connection.
    fn check_timeouts(&mut self) -> Vec<usize> {
        let now = Instant::now();
        if now.saturating_duration_since(self.last_timeout_check) < TIMEOUT_CHECK_INTERVAL {
            return Vec::new();
        }
        self.last_timeout_check = now;

        let timeout = Duration::from_secs(CONNECTION_TIMEOUT_SECS);
        let mut timed_out = Vec::new();

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
            }
        }

        timed_out
    }

    /// Update dirty connections' poller interest (O(m) where m = connections with changed interest)
    ///
    /// Returns the ids whose interest update failed. Such a connection can no
    /// longer have writable interest (re)registered, so a queued-but-WouldBlock
    /// write would never be driven to completion — it is not in `pending_flush`
    /// either (NEW-RS-01). Closing it is the only safe recovery; the caller does
    /// so. The fd is almost always already broken when modify() fails.
    fn update_dirty_interests(&mut self) -> Vec<usize> {
        // Drain interest_dirty to get IDs that need updating
        let dirty_ids: Vec<usize> = self.interest_dirty.drain().collect();

        let mut ids_to_close = Vec::new();
        for id in dirty_ids {
            if let Err(e) = self.update_interest(id) {
                log::warn!(
                    "Failed to update interest for connection {}: {:?}; closing (queued writes would otherwise stall)",
                    id, e
                );
                ids_to_close.push(id);
            }
        }
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
        publisher_receiver: crossbeam_channel::Receiver<(String, PublisherSource)>,
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

            // 3. Non-blocking receive new publishers
            let mut new_publisher_added = false;
            while let Ok((stream_key, source)) = publisher_receiver.try_recv() {
                if self.add_publisher(stream_key.clone(), source).is_some() {
                    debug!("New publisher added for stream: {}", stream_key);
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
            // its budget: the leftover items sit on a channel the poller
            // cannot see, so a blocking poll would add up to POLL_TIMEOUT_MS
            // of latency per excess batch. Poll non-blocking and let steps
            // 5-10 run in between, which is exactly what the budget exists
            // to guarantee.
            let poll_wait =
                if new_publisher_added || publishers_pending || !self.read_pending.is_empty() {
                    Duration::ZERO
                } else {
                    poll_timeout
                };
            let events = match self.poller.poll(Some(poll_wait)) {
                Ok(events) => events,
                Err(e) => {
                    error!("Poller error: {:?}", e);
                    continue;
                }
            };

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

            for event in events {
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

    /// Check if a connection ID is in the interest_dirty set (test only)
    #[cfg(test)]
    pub fn is_interest_dirty(&self, id: usize) -> bool {
        self.interest_dirty.contains(&id)
    }

    /// Check if a connection ID is in the pending_flush set (test only)
    #[cfg(test)]
    pub fn is_pending_flush(&self, id: usize) -> bool {
        self.pending_flush.contains(&id)
    }

    /// Clear interest_dirty set and return its previous contents (test only)
    #[cfg(test)]
    pub fn drain_interest_dirty(&mut self) -> Vec<usize> {
        self.interest_dirty.drain().collect()
    }
}

// ============================================================================
// Tests
// ============================================================================

#[cfg(test)]
mod tests {
    use super::*;

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
        conn.enqueue_data(Bytes::from_static(test_data), false, false, false);

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
        let stream_keys = Arc::new(dashmap::DashSet::new());
        let status = Arc::new(AtomicUsize::new(STATUS_RUN));
        let mut reactor =
            Reactor::new(3, None, stream_keys, status).expect("Failed to create reactor");

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

    #[test]
    fn test_reactor_creation() {
        let stream_keys = Arc::new(dashmap::DashSet::new());
        let status = Arc::new(AtomicUsize::new(STATUS_RUN));

        let reactor = Reactor::new(3, None, stream_keys, status);
        assert!(reactor.is_ok());
    }

    #[test]
    fn test_connection_generation_increments() {
        let stream_keys = Arc::new(dashmap::DashSet::new());
        let status = Arc::new(AtomicUsize::new(STATUS_RUN));

        let mut reactor =
            Reactor::new(3, None, stream_keys, status).expect("Failed to create reactor");

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
        let stream_keys = Arc::new(dashmap::DashSet::new());
        let status = Arc::new(AtomicUsize::new(STATUS_RUN));

        let mut reactor =
            Reactor::new(3, None, stream_keys, status).expect("Failed to create reactor");

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
    #[cfg(target_pointer_width = "64")]
    fn test_generation_prevents_aba_problem() {
        let stream_keys = Arc::new(dashmap::DashSet::new());
        let status = Arc::new(AtomicUsize::new(STATUS_RUN));

        let mut reactor =
            Reactor::new(3, None, stream_keys, status).expect("Failed to create reactor");

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

    /// Stress test: verify reactor can handle many connections
    /// Note: This test creates connections but doesn't run the full RTMP handshake
    #[test]
    fn test_many_connections_creation() {
        let stream_keys = Arc::new(dashmap::DashSet::new());
        let status = Arc::new(AtomicUsize::new(STATUS_RUN));

        let mut reactor =
            Reactor::new(3, None, stream_keys, status).expect("Failed to create reactor");

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

        let stream_keys = Arc::new(dashmap::DashSet::new());
        let status = Arc::new(AtomicUsize::new(STATUS_RUN));

        let mut reactor =
            Reactor::new(3, None, stream_keys, status).expect("Failed to create reactor");

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

        let stream_keys = Arc::new(dashmap::DashSet::new());
        let status = Arc::new(AtomicUsize::new(STATUS_RUN));

        let mut reactor =
            Reactor::new(3, None, stream_keys, status).expect("Failed to create reactor");

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

        let stream_keys = Arc::new(dashmap::DashSet::new());
        let status = Arc::new(AtomicUsize::new(STATUS_RUN));

        let mut reactor =
            Reactor::new(3, None, stream_keys, status).expect("Failed to create reactor");

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
            conn.enqueue_data(Bytes::from_static(test_data), false, false, false);
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

        let stream_keys = Arc::new(dashmap::DashSet::new());
        let status = Arc::new(AtomicUsize::new(STATUS_RUN));

        let mut reactor =
            Reactor::new(3, None, stream_keys, status).expect("Failed to create reactor");

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

        // Enqueue data and add to pending_flush set
        let test_data = b"World";
        if let Some(conn) = reactor.connections.get_mut(token.id) {
            conn.enqueue_data(Bytes::from_static(test_data), false, false, false);
        }
        reactor.pending_flush.insert(token.id);

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

        let stream_keys = Arc::new(dashmap::DashSet::new());
        let status = Arc::new(AtomicUsize::new(STATUS_RUN));
        let mut reactor =
            Reactor::new(3, None, stream_keys, status).expect("Failed to create reactor");

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
            assert!(conn.enqueue_data(big, false, true, true));
            assert!(conn.has_pending_writes());
        }

        reactor.pending_flush.insert(token.id);
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
        let stream_keys = Arc::new(dashmap::DashSet::new());
        let status = Arc::new(AtomicUsize::new(STATUS_RUN));

        let mut reactor =
            Reactor::new(3, None, stream_keys, status).expect("Failed to create reactor");

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

        // Add to pending_flush even though no data pending
        // (this can happen if data was already flushed between enqueue and flush_pending)
        reactor.pending_flush.insert(token.id);

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
        let mut reactor = Reactor::new(3, None, stream_keys, status).expect("reactor");

        // Register an in-process (create_rtmp_input-style) Feed publisher.
        let (feed_tx, feed_rx) = crossbeam_channel::bounded(64);
        let pub_id = reactor
            .add_publisher("live".to_string(), PublisherSource::Feed(feed_rx))
            .expect("publisher registered");

        // Realistic mixed sequence on ONE FIFO feed: connect/createStream/publish
        // as Raw (fed to the session), then audio/video tags as bypassed Media.
        for control in
            build_publish_control("app".to_string(), "live".to_string()).expect("control")
        {
            feed_tx.send(PublisherFeed::Raw(control)).unwrap();
        }
        let video_seq: &[u8] = &[0x17, 0x00, 0x00, 0x00, 0x00, 0x01, 0x64];
        let audio_seq: &[u8] = &[0xaf, 0x00, 0x12, 0x10];
        feed_tx
            .send(PublisherFeed::Media {
                tag_type: 0x09,
                timestamp: RtmpTimestamp { value: 0 },
                data: Bytes::from_static(video_seq),
            })
            .unwrap();
        feed_tx
            .send(PublisherFeed::Media {
                tag_type: 0x08,
                timestamp: RtmpTimestamp { value: 0 },
                data: Bytes::from_static(audio_seq),
            })
            .unwrap();
        feed_tx
            .send(PublisherFeed::Media {
                tag_type: 0x09,
                timestamp: RtmpTimestamp { value: 33 },
                data: Bytes::from_static(&[0x17, 0x01, 0xAA, 0xBB]),
            })
            .unwrap();
        // A second IDR freezes the first GOP into the replay cache.
        feed_tx
            .send(PublisherFeed::Media {
                tag_type: 0x09,
                timestamp: RtmpTimestamp { value: 66 },
                data: Bytes::from_static(&[0x17, 0x01, 0xCC, 0xDD]),
            })
            .unwrap();

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

    // H8.c: a server-initiated close must first try to flush what was queued
    // in the same round — most visibly the finish_playing status a watcher
    // gets when the publisher ends. A raw remove_connection dropped it: the
    // enqueue marked pending_flush, but the close ran before any flush step.
    #[test]
    fn close_connection_after_flush_delivers_the_queued_tail() {
        use std::io::Read;

        let stream_keys = Arc::new(dashmap::DashSet::new());
        let status = Arc::new(AtomicUsize::new(STATUS_RUN));
        let mut reactor = Reactor::new(3, None, stream_keys, status).expect("reactor");

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
            assert!(conn.enqueue_data(Bytes::from_static(b"final status"), false, false, false));
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

        let stream_keys = Arc::new(dashmap::DashSet::new());
        let status = Arc::new(AtomicUsize::new(STATUS_RUN));
        let mut reactor = Reactor::new(3, None, stream_keys, status).expect("reactor");

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
            assert!(conn.enqueue_data(Bytes::from(payload.clone()), false, true, true));
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

        let stream_keys = Arc::new(dashmap::DashSet::new());
        let status = Arc::new(AtomicUsize::new(STATUS_RUN));
        let mut reactor = Reactor::new(3, None, stream_keys, status).expect("reactor");

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
            assert!(conn.enqueue_data(Bytes::from(payload), false, true, true));
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

        let stream_keys = Arc::new(dashmap::DashSet::new());
        let status = Arc::new(AtomicUsize::new(STATUS_RUN));
        let mut reactor = Reactor::new(3, None, stream_keys, status).expect("reactor");

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
            assert!(conn.enqueue_data(Bytes::from(vec![0xABu8; tail]), false, true, true));
            conn.condemn(Instant::now() + Duration::from_secs(30));
            assert!(conn.is_condemned());
        }

        // Live fanout targets the condemned connection with a large media
        // packet. It must be skipped, so the queue stays at the pre-condemn tail.
        reactor
            .packets_buffer
            .push((token.id, vec![0xCDu8; 1024 * 1024], true, false, true));
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
        let mut reactor = Reactor::new(3, None, stream_keys, status).expect("reactor");

        // Channel larger than the budget so the backlog fits in one send burst.
        let (feed_tx, feed_rx) = crossbeam_channel::bounded(MAX_PUBLISH_ITEMS_PER_POLL + 64);
        reactor
            .add_publisher("live".to_string(), PublisherSource::Feed(feed_rx))
            .expect("publisher registered");

        // Budget + 6 tiny audio tags queued ahead of a single drain.
        for i in 0..(MAX_PUBLISH_ITEMS_PER_POLL + 6) {
            feed_tx
                .send(PublisherFeed::Media {
                    tag_type: 0x08,
                    timestamp: RtmpTimestamp { value: i as u32 },
                    data: Bytes::from_static(&[0xaf, 0x01, 0x00]),
                })
                .unwrap();
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
        let mut reactor = Reactor::new(3, None, stream_keys, status).expect("reactor");

        let (feed_tx, feed_rx) = crossbeam_channel::bounded(16);
        reactor
            .add_publisher("live".to_string(), PublisherSource::Feed(feed_rx))
            .expect("publisher registered");

        // 3 x 200KiB crosses the 512KiB byte budget on the third item
        // (consume-then-check), leaving the fourth for the next round.
        let big = Bytes::from(vec![0u8; 200 * 1024]);
        for i in 0..3u32 {
            feed_tx
                .send(PublisherFeed::Media {
                    tag_type: 0x08,
                    timestamp: RtmpTimestamp { value: i },
                    data: big.clone(),
                })
                .unwrap();
        }
        feed_tx
            .send(PublisherFeed::Media {
                tag_type: 0x08,
                timestamp: RtmpTimestamp { value: 3 },
                data: Bytes::from_static(&[0xaf, 0x01, 0x00]),
            })
            .unwrap();

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
        let mut reactor = Reactor::new(3, None, stream_keys, status).expect("reactor");

        let (feed_tx, feed_rx) = crossbeam_channel::bounded(4);
        reactor
            .add_publisher("live".to_string(), PublisherSource::Feed(feed_rx))
            .expect("publisher registered");

        // A single 600KiB audio sequence header (> byte budget), then a small
        // video sequence header. Sequence headers land in the scheduler's
        // channel cache, which proves each item was processed, not dropped.
        let mut oversized = vec![0u8; 600 * 1024];
        oversized[0] = 0xaf;
        oversized[1] = 0x00;
        let oversized = Bytes::from(oversized);
        feed_tx
            .send(PublisherFeed::Media {
                tag_type: 0x08,
                timestamp: RtmpTimestamp { value: 0 },
                data: oversized.clone(),
            })
            .unwrap();
        let video_seq: &[u8] = &[0x17, 0x00, 0x00, 0x00, 0x00, 0x01, 0x64];
        feed_tx
            .send(PublisherFeed::Media {
                tag_type: 0x09,
                timestamp: RtmpTimestamp { value: 1 },
                data: Bytes::from_static(video_seq),
            })
            .unwrap();

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

    /// Fixture for the cap-resume (step 5b) tests: a reactor plus one
    /// handshaking connection whose peer has already sent C0+C1. A resumed
    /// read that actually happens processes the handshake and enqueues the
    /// S0+S1+S2 response (observable via `is_pending_flush`); a skipped read
    /// leaves nothing queued.
    fn reactor_with_handshake_bytes_pending() -> (Reactor, ConnectionToken, TcpStream) {
        use std::io::Write;

        let stream_keys = Arc::new(dashmap::DashSet::new());
        let status = Arc::new(AtomicUsize::new(STATUS_RUN));
        let mut reactor =
            Reactor::new(3, None, stream_keys, status).expect("Failed to create reactor");

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
        let stream_keys = Arc::new(dashmap::DashSet::new());
        let status = Arc::new(AtomicUsize::new(STATUS_RUN));
        let mut reactor =
            Reactor::new(3, None, stream_keys, status).expect("Failed to create reactor");

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
