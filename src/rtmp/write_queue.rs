// src/rtmp/write_queue.rs - Write queue implementation
//
// Core features:
// - Tiered backpressure strategy (Normal/Warning/High/Critical)
// - Partial write support
// - Sequence headers prioritized (never dropped by backpressure policy, but rejected at critical threshold)
// - Special handling for audio-only streams
// - Time-based eviction strategy

use bytes::Bytes;
use std::collections::VecDeque;
use std::io::{self, IoSlice, Write};
use std::time::Instant;

/// Maximum number of `IoSlice`s gathered into a single `write_vectored` call.
/// Capped well below the platform IOV_MAX (Linux 1024) to bound the per-flush
/// gather buffer. GOP replay of ~140 entries collapses to ceil(140/64)=3
/// writev syscalls instead of ~140 individual write() calls (PERF-9).
const MAX_IOV: usize = 64;

// Backpressure threshold constants
// QUEUE_WARN_BYTES is crate-visible: the scheduler derives its join-replay
// burst budget from it so a full burst stays in the Normal band (zero drops).
pub(crate) const QUEUE_WARN_BYTES: usize = 1 * 1024 * 1024; // 1MB warning
const QUEUE_HIGH_BYTES: usize = 2 * 1024 * 1024; // 2MB high watermark
const QUEUE_MAX_BYTES: usize = 4 * 1024 * 1024; // 4MB disconnect
const QUEUE_MAX_AGE_SECS: u64 = 10; // 10 second timeout
const AUDIO_ONLY_MAX_AGE_SECS: u64 = 5; // 5 second timeout for audio-only

/// Queue entry
struct WriteEntry {
    data: Bytes,
    offset: usize,
    timestamp: Instant,
    #[allow(dead_code)]
    is_keyframe: bool,
    // SPS/PPS/AudioConfig provenance. The policy role folded into
    // `droppable` (sequence headers are always non-droppable); kept, like
    // `is_keyframe`, as entry provenance.
    #[allow(dead_code)]
    is_sequence_header: bool,
    /// A liveness ping queued by the reactor's timeout sweep. Flushes report
    /// these bytes separately (`FlushResult::*::ping_bytes_written`) so the
    /// connection can exclude a ping's own delivery from its write-activity
    /// stamp — only the peer's ANSWER may count as liveness. Tagging the
    /// entry (instead of keeping a side counter) keeps the accounting exact
    /// per entry and per partial write.
    is_ping: bool,
    /// Whether the shedding policy may drop this entry (Warning/High frame
    /// drops, age eviction). Carried from rml's `Packet::can_be_dropped`:
    /// only media serialized drop-tolerantly (forced full headers) is
    /// `true`. Everything else — session control responses, handshake
    /// bytes, pings, sequence headers — was serialized against the
    /// session's stateful per-csid header history, and silently dropping
    /// such an entry would make the NEXT message on that chunk stream
    /// header-compress against a packet the peer never received,
    /// corrupting the stream. Non-droppable entries bypass the shedding
    /// policy and pin the age-eviction sweep; the Critical cap still
    /// refuses them (a disconnect, never a silent drop).
    droppable: bool,
}

impl WriteEntry {
    fn remaining(&self) -> &[u8] {
        &self.data[self.offset..]
    }

    fn advance(&mut self, n: usize) {
        self.offset += n;
    }

    fn remaining_bytes(&self) -> usize {
        self.data.len().saturating_sub(self.offset)
    }

    /// Entry age against the caller's hoisted clock (PERF-10: eviction runs
    /// per enqueue in the Warning/High bands, and `elapsed()` here would be
    /// a fresh clock read per checked entry). Saturating, so an entry
    /// stamped marginally after `now` reads as age zero.
    fn age_secs_at(&self, now: Instant) -> u64 {
        now.saturating_duration_since(self.timestamp).as_secs()
    }
}

/// Backpressure level. The shedding policies below apply to DROPPABLE
/// entries only (drop-tolerantly serialized media); non-droppable entries —
/// session control responses, handshake bytes, pings, sequence headers —
/// are always kept in every band short of Critical (see
/// `WriteEntry::droppable`).
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum BackpressureLevel {
    Normal,   // < 1MB: enqueue all
    Warning,  // 1-2MB: shed droppable non-keyframe video; keep droppable audio, keyframes and every non-droppable entry
    High,     // 2-4MB: shed all droppable non-keyframes; keep keyframes and every non-droppable entry
    Critical, // >= 4MB: should disconnect (refuses droppable and non-droppable alike)
}

/// Flush result
#[derive(Debug)]
pub enum FlushResult {
    /// QueueAll flushed
    Complete {
        bytes_written: usize,
        /// The portion of `bytes_written` consumed from ping-tagged entries
        /// (see `WriteEntry::is_ping`); exact per entry, partial writes
        /// included.
        ping_bytes_written: usize,
    },
    /// WouldBlock encountered, partial write
    WouldBlock {
        bytes_written: usize,
        /// See `Complete::ping_bytes_written`.
        ping_bytes_written: usize,
    },
    /// Connection closed
    Closed,
}

/// Write queue
///
/// Write queue with tiered backpressure and partial write support
pub struct WriteQueue {
    queue: VecDeque<WriteEntry>,
    total_bytes: usize,
    has_video: bool, // Used to detect audio-only stream
    dropped_frames: u64,
}

impl WriteQueue {
    pub fn new() -> Self {
        Self {
            queue: VecDeque::with_capacity(64),
            total_bytes: 0,
            has_video: false,
            dropped_frames: 0,
        }
    }

    /// Current backpressure level
    pub fn backpressure_level(&self) -> BackpressureLevel {
        if self.total_bytes >= QUEUE_MAX_BYTES {
            BackpressureLevel::Critical
        } else if self.total_bytes >= QUEUE_HIGH_BYTES {
            BackpressureLevel::High
        } else if self.total_bytes >= QUEUE_WARN_BYTES {
            BackpressureLevel::Warning
        } else {
            BackpressureLevel::Normal
        }
    }

    /// Enqueue data
    ///
    /// # Arguments
    /// * `data` - Data to enqueue
    /// * `is_keyframe` - Whether it's a keyframe
    /// * `is_sequence_header` - Whether it's a sequence header (SPS/PPS/AudioConfig)
    /// * `is_video` - Whether it's video data
    /// * `droppable` - rml's `Packet::can_be_dropped`: whether the shedding
    ///   policy may discard this entry (see `WriteEntry::droppable`)
    /// * `now` - the caller's clock read for the entry timestamp (PERF-10:
    ///   a fanout enqueues W entries per message and the timestamp only
    ///   feeds the seconds-granular age eviction, so callers hoist one
    ///   read per batch instead of paying one per entry)
    ///
    /// # Returns
    /// * `true` - Successfully enqueued or dropped per policy
    /// * `false` - Queue full, should disconnect
    pub fn enqueue(
        &mut self,
        data: Bytes,
        is_keyframe: bool,
        is_sequence_header: bool,
        is_video: bool,
        droppable: bool,
        now: Instant,
    ) -> bool {
        if is_video {
            self.has_video = true;
        }

        // Check critical threshold BEFORE adding to prevent overshoot
        // Use saturating_add to prevent overflow
        if self.total_bytes.saturating_add(data.len()) >= QUEUE_MAX_BYTES {
            return false;
        }

        let level = self.backpressure_level();

        // Sequence headers never dropped
        if is_sequence_header {
            self.push_entry(data, is_keyframe, true, false, now);
            return true;
        }

        // Non-droppable entries (session control, handshake bytes) bypass
        // the shedding policy entirely: the session serializer already
        // committed per-csid header history for them, so a silent drop
        // would desynchronize the peer's chunk-stream decoding (see
        // `WriteEntry::droppable`). The Critical cap above still refused
        // oversized additions — a disconnect, never a silent drop. The
        // age-eviction sweep still runs under pressure so stale DROPPABLE
        // entries around them keep being shed.
        if !droppable {
            self.push_entry(data, is_keyframe, false, false, now);
            if matches!(
                level,
                BackpressureLevel::Warning | BackpressureLevel::High
            ) {
                self.evict_old_entries(now);
            }
            return true;
        }

        match level {
            BackpressureLevel::Normal => {
                self.push_entry(data, is_keyframe, false, true, now);
            }
            BackpressureLevel::Warning => {
                // Drop non-keyframe video, keep audio
                if is_keyframe || !is_video {
                    self.push_entry(data, is_keyframe, false, true, now);
                } else {
                    self.dropped_frames += 1;
                }
                // Perform time-based eviction
                self.evict_old_entries(now);
            }
            BackpressureLevel::High => {
                // Keep keyframes only
                if is_keyframe {
                    self.push_entry(data, is_keyframe, false, true, now);
                } else {
                    self.dropped_frames += 1;
                }
                self.evict_old_entries(now);
            }
            BackpressureLevel::Critical => unreachable!(
                "the critical-cap gate above keeps total_bytes below QUEUE_MAX_BYTES, so the level cannot be Critical"
            ),
        }

        true
    }

    fn push_entry(
        &mut self,
        data: Bytes,
        is_keyframe: bool,
        is_sequence_header: bool,
        droppable: bool,
        now: Instant,
    ) {
        self.push_entry_tagged(data, is_keyframe, is_sequence_header, false, droppable, now);
    }

    fn push_entry_tagged(
        &mut self,
        data: Bytes,
        is_keyframe: bool,
        is_sequence_header: bool,
        is_ping: bool,
        droppable: bool,
        now: Instant,
    ) {
        let len = data.len();
        self.queue.push_back(WriteEntry {
            data,
            offset: 0,
            timestamp: now,
            is_keyframe,
            is_sequence_header,
            is_ping,
            droppable,
        });
        self.total_bytes += len;
    }

    /// Enqueue a liveness ping (see `WriteEntry::is_ping`). Queued by the
    /// reactor's timeout sweep, and only onto a drained queue (the sweep's
    /// predicate refuses to probe while writes are pending), so it takes
    /// the plain Normal-band path; the critical cap stays as a guard.
    pub fn enqueue_ping(&mut self, data: Bytes) -> bool {
        if self.total_bytes.saturating_add(data.len()) >= QUEUE_MAX_BYTES {
            return false;
        }
        self.push_entry_tagged(data, false, false, true, false, Instant::now());
        true
    }

    /// Time-based eviction - Remove stale data. Ages are measured against
    /// the caller's hoisted `now` (PERF-10), never a fresh clock read.
    fn evict_old_entries(&mut self, now: Instant) {
        let max_age = if self.has_video {
            QUEUE_MAX_AGE_SECS
        } else {
            AUDIO_ONLY_MAX_AGE_SECS
        };

        while let Some(entry) = self.queue.front() {
            // Non-droppable entries pin the sweep: sequence headers, pings
            // and session control were all serialized against the session's
            // stateful per-csid header history, and evicting one unsent
            // would make the NEXT message on its chunk stream
            // header-compress against a packet the peer never received —
            // corrupting the stream. Such entries are bytes-trivial; a peer
            // too stalled to drain them still dies via the Critical cap or
            // the connection timeout.
            if !entry.droppable {
                break;
            }
            // A partially written entry is pinned: evicting it mid-send
            // would resume the byte stream inside a different tag and
            // corrupt the whole connection.
            if entry.offset > 0 {
                break;
            }
            if entry.age_secs_at(now) > max_age {
                if let Some(removed) = self.queue.pop_front() {
                    self.total_bytes = self.total_bytes.saturating_sub(removed.remaining_bytes());
                    self.dropped_frames += 1;
                }
            } else {
                break;
            }
        }
    }

    /// Try to flush to writer
    ///
    /// PERF-9: gathers up to `MAX_IOV` queued entries into a single
    /// `write_vectored` call instead of issuing one `write()` per entry, then
    /// walks the returned byte count across those entries. Partial writes are
    /// tracked per entry via `offset`, preserving the pinned-partial-entry
    /// rule enforced by `evict_old_entries` (an entry with `offset > 0` is
    /// never evicted, so the byte stream never resumes mid-tag).
    ///
    /// A short vectored write on a non-blocking socket means the kernel send
    /// buffer is full; this returns `WouldBlock` immediately rather than
    /// retrying, so the reactor waits for the writable event (NEW-RS-01)
    /// instead of burning a guaranteed-EAGAIN syscall.
    pub fn try_flush<W: Write>(&mut self, writer: &mut W) -> io::Result<FlushResult> {
        let mut bytes_written = 0;
        let mut ping_bytes_written = 0;

        loop {
            // Drop any fully-consumed entries at the front (e.g. an entry that
            // a previous flush partially wrote and this one completes).
            self.pop_completed_front();

            if self.queue.is_empty() {
                return Ok(FlushResult::Complete {
                    bytes_written,
                    ping_bytes_written,
                });
            }

            // Gather up to MAX_IOV slices from the front of the queue and issue
            // a single vectored write. The IoSlices borrow the queue entries,
            // so the borrow is scoped and the queue is only mutated afterwards.
            let mut gathered = 0usize;
            let write_result = {
                // Fixed stack array instead of a per-batch Vec::with_capacity:
                // try_flush runs once per writable event per subscriber, so the
                // heap alloc/free was pure hot-loop overhead. IoSlice is Copy and
                // the empty-slice fill is overwritten by the gather below.
                let mut iov = [IoSlice::new(&[]); MAX_IOV];
                let mut n_iov = 0;
                for entry in self.queue.iter() {
                    if n_iov == MAX_IOV {
                        break;
                    }
                    let rem = entry.remaining();
                    if !rem.is_empty() {
                        gathered += rem.len();
                        iov[n_iov] = IoSlice::new(rem);
                        n_iov += 1;
                    }
                }
                // pop_completed_front ran and the queue is non-empty, so the
                // front entry has unsent bytes and iov[..n_iov] is non-empty.
                writer.write_vectored(&iov[..n_iov])
            };

            match write_result {
                Ok(0) => return Ok(FlushResult::Closed),
                Ok(n) => {
                    bytes_written += n;
                    ping_bytes_written += self.advance_front(n);
                    if n < gathered {
                        // Short write: send buffer full, stop and wait for the
                        // writable event rather than retry into EAGAIN.
                        return Ok(FlushResult::WouldBlock {
                            bytes_written,
                            ping_bytes_written,
                        });
                    }
                    // Whole batch drained; loop to gather the next one.
                }
                Err(ref e) if e.kind() == io::ErrorKind::WouldBlock => {
                    return Ok(FlushResult::WouldBlock {
                        bytes_written,
                        ping_bytes_written,
                    });
                }
                Err(e) => return Err(e),
            }
        }
    }

    /// Pop fully-written entries from the front, keeping `total_bytes` in sync.
    fn pop_completed_front(&mut self) {
        while let Some(front) = self.queue.front() {
            if front.remaining().is_empty() {
                let full = front.data.len();
                self.total_bytes = self.total_bytes.saturating_sub(full);
                self.queue.pop_front();
            } else {
                break;
            }
        }
    }

    /// Distribute `n` written bytes across the front entries, popping each one
    /// as it completes. `n` is always <= the bytes gathered for this write, so
    /// the walk never runs past the queue. `total_bytes` tracks the sum of full
    /// entry lengths, so it is decremented by an entry's full length exactly
    /// once, when that entry completes (matching `enqueue`/`evict`).
    ///
    /// Returns how many of the `n` bytes were consumed from ping-tagged
    /// entries — exact per entry and per partial write, so the caller's
    /// activity accounting never credits a ping's delivery as liveness.
    fn advance_front(&mut self, mut n: usize) -> usize {
        let mut ping_bytes = 0;
        while n > 0 {
            let Some(front) = self.queue.front_mut() else {
                break;
            };
            let rem = front.remaining_bytes();
            if rem == 0 {
                // A zero-length entry contributes nothing; drop it.
                let full = front.data.len();
                self.total_bytes = self.total_bytes.saturating_sub(full);
                self.queue.pop_front();
                continue;
            }
            if n >= rem {
                // Entry fully sent this call: account for its full length once.
                if front.is_ping {
                    ping_bytes += rem;
                }
                let full = front.data.len();
                self.total_bytes = self.total_bytes.saturating_sub(full);
                self.queue.pop_front();
                n -= rem;
            } else {
                if front.is_ping {
                    ping_bytes += n;
                }
                front.advance(n);
                n = 0;
            }
        }
        ping_bytes
    }

    /// Is queue empty
    pub fn is_empty(&self) -> bool {
        self.queue.is_empty()
    }

    /// Bytes currently pending to send (the queue's total retained payload).
    /// Read in production to seed the scheduler's join-replay budget with a
    /// connection's existing backlog.
    pub fn pending_bytes(&self) -> usize {
        self.total_bytes
    }

    /// Entry count in queue (test only)
    #[cfg(test)]
    pub fn pending_entries(&self) -> usize {
        self.queue.len()
    }

    /// Dropped frames count (test only)
    #[cfg(test)]
    fn dropped_frames(&self) -> u64 {
        self.dropped_frames
    }

    /// Backdate the front entry past any eviction window (test only)
    #[cfg(test)]
    fn backdate_front(&mut self, secs: u64) {
        if let Some(front) = self.queue.front_mut() {
            if let Some(t) = Instant::now().checked_sub(std::time::Duration::from_secs(secs)) {
                front.timestamp = t;
            }
        }
    }

    /// Write offset of the front entry (test only)
    #[cfg(test)]
    fn front_offset(&self) -> Option<usize> {
        self.queue.front().map(|e| e.offset)
    }

    /// Has video flag (test only)
    #[cfg(test)]
    fn has_video(&self) -> bool {
        self.has_video
    }
}

impl Default for WriteQueue {
    fn default() -> Self {
        Self::new()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn make_data(size: usize) -> Bytes {
        Bytes::from(vec![0u8; size])
    }

    #[test]
    fn test_basic_enqueue_dequeue() {
        let mut queue = WriteQueue::new();

        queue.enqueue(make_data(100), false, false, true, true, Instant::now());
        assert_eq!(queue.pending_bytes(), 100);
        assert_eq!(queue.pending_entries(), 1);
        assert_eq!(queue.backpressure_level(), BackpressureLevel::Normal);
    }

    #[test]
    fn test_backpressure_levels() {
        // Test each level independently with fresh queues

        // Normal level (< 1MB)
        {
            let mut queue = WriteQueue::new();
            queue.enqueue(make_data(512 * 1024), true, false, true, true, Instant::now()); // use keyframe to avoid drops
            assert_eq!(queue.backpressure_level(), BackpressureLevel::Normal);
        }

        // Warning level (>= 1MB, < 2MB)
        {
            let mut queue = WriteQueue::new();
            queue.enqueue(make_data(1500 * 1024), true, false, true, true, Instant::now()); // use keyframe
            assert_eq!(queue.backpressure_level(), BackpressureLevel::Warning);
        }

        // High level (>= 2MB, < 4MB)
        {
            let mut queue = WriteQueue::new();
            queue.enqueue(make_data(3 * 1024 * 1024), true, false, true, true, Instant::now()); // use keyframe
            assert_eq!(queue.backpressure_level(), BackpressureLevel::High);
        }

        // Critical threshold test - enqueue that would reach critical is rejected
        {
            let mut queue = WriteQueue::new();
            // First fill to just below critical (3.5MB)
            queue.enqueue(make_data(3500 * 1024), true, false, true, true, Instant::now());
            assert_eq!(queue.backpressure_level(), BackpressureLevel::High);

            // Try to add 600KB which would push total to 4.1MB >= 4MB (critical)
            // This should be rejected
            let result = queue.enqueue(make_data(600 * 1024), true, false, true, true, Instant::now());
            assert!(
                !result,
                "Enqueue should be rejected when it would reach Critical"
            );
            // Queue should still be at High level (data was rejected)
            assert_eq!(queue.backpressure_level(), BackpressureLevel::High);
        }
    }

    #[test]
    fn test_sequence_header_never_dropped() {
        let mut queue = WriteQueue::new();

        // Fill up to high level
        queue.enqueue(make_data(3 * 1024 * 1024), false, false, true, true, Instant::now());
        assert_eq!(queue.backpressure_level(), BackpressureLevel::High);

        // Sequence header should still be enqueued
        let result = queue.enqueue(make_data(100), false, true, true, true, Instant::now());
        assert!(result);

        // Non-keyframe should be dropped at high level
        let _before = queue.pending_entries();
        queue.enqueue(make_data(100), false, false, true, true, Instant::now());
        // Entry count should not increase for non-keyframe
        assert!(queue.dropped_frames() > 0);
    }

    #[test]
    fn test_keyframe_preserved_at_high_level() {
        let mut queue = WriteQueue::new();

        // Fill up to high level
        queue.enqueue(make_data(3 * 1024 * 1024), false, false, true, true, Instant::now());
        assert_eq!(queue.backpressure_level(), BackpressureLevel::High);

        let before = queue.pending_entries();

        // Keyframe should be accepted
        queue.enqueue(make_data(100), true, false, true, true, Instant::now());
        assert_eq!(queue.pending_entries(), before + 1);
    }

    #[test]
    fn test_audio_preserved_at_warning_level() {
        let mut queue = WriteQueue::new();

        // Fill up to warning level
        queue.enqueue(make_data(1500 * 1024), false, false, true, true, Instant::now());
        assert_eq!(queue.backpressure_level(), BackpressureLevel::Warning);

        let before = queue.pending_entries();

        // Audio should be accepted at warning level
        queue.enqueue(make_data(100), false, false, false, true, Instant::now());
        assert_eq!(queue.pending_entries(), before + 1);

        // Non-keyframe video should be dropped
        let dropped_before = queue.dropped_frames();
        queue.enqueue(make_data(100), false, false, true, true, Instant::now());
        assert!(queue.dropped_frames() > dropped_before);
    }

    #[test]
    fn test_critical_rejects_all() {
        let mut queue = WriteQueue::new();

        // Fill up to just below critical level (3.9MB)
        queue.enqueue(make_data(3900 * 1024), true, false, true, true, Instant::now());
        assert_eq!(queue.backpressure_level(), BackpressureLevel::High);

        // Try to add data that would exceed critical threshold
        // Even keyframes should be rejected
        let result = queue.enqueue(make_data(200 * 1024), true, false, true, true, Instant::now());
        assert!(
            !result,
            "Keyframe should be rejected when it would exceed Critical"
        );

        // Even sequence headers should be rejected when threshold would be exceeded
        // (Note: sequence headers bypass drop policy but not critical threshold)
        let result = queue.enqueue(make_data(200 * 1024), false, true, true, true, Instant::now());
        assert!(
            !result,
            "Sequence header should be rejected when it would exceed Critical"
        );
        assert!(!result);
    }

    /// A writer that accepts up to `capacity` bytes total across all calls,
    /// then returns WouldBlock - modelling a non-blocking socket whose send
    /// buffer fills up. `write_vectored` drains across slices like a real
    /// vectored write so the gather/walk accounting is exercised.
    struct CapWriter {
        inner: Vec<u8>,
        capacity: usize,
    }

    impl Write for CapWriter {
        fn write(&mut self, buf: &[u8]) -> io::Result<usize> {
            if self.inner.len() >= self.capacity {
                return Err(io::Error::from(io::ErrorKind::WouldBlock));
            }
            let room = self.capacity - self.inner.len();
            let n = buf.len().min(room);
            self.inner.extend_from_slice(&buf[..n]);
            Ok(n)
        }

        fn write_vectored(&mut self, bufs: &[IoSlice<'_>]) -> io::Result<usize> {
            if self.inner.len() >= self.capacity {
                return Err(io::Error::from(io::ErrorKind::WouldBlock));
            }
            let mut total = 0;
            for b in bufs {
                let room = self.capacity - self.inner.len();
                if room == 0 {
                    break;
                }
                let n = b.len().min(room);
                self.inner.extend_from_slice(&b[..n]);
                total += n;
                if n < b.len() {
                    break; // slice only partially accepted -> buffer full
                }
            }
            Ok(total)
        }

        fn flush(&mut self) -> io::Result<()> {
            Ok(())
        }
    }

    #[test]
    fn test_partial_write_walks_across_entries() {
        let mut queue = WriteQueue::new();
        queue.enqueue(Bytes::from_static(b"hello"), false, false, true, true, Instant::now());
        queue.enqueue(Bytes::from_static(b"world"), false, false, true, true, Instant::now());
        assert_eq!(queue.pending_bytes(), 10);

        // Capacity 7: one writev writes "hello" (5) then "wo" (2) and fills.
        // The returned count (7) must walk across the entry boundary: pop the
        // first entry, pin the second at offset 2.
        let mut writer = CapWriter {
            inner: Vec::new(),
            capacity: 7,
        };
        let result = queue.try_flush(&mut writer).unwrap();
        assert!(matches!(
            result,
            FlushResult::WouldBlock { bytes_written: 7, .. }
        ));
        assert_eq!(writer.inner, b"hellowo");
        assert_eq!(queue.pending_entries(), 1);
        assert_eq!(queue.front_offset(), Some(2));
        // total_bytes drops by the first entry's full length, not by bytes sent.
        assert_eq!(queue.pending_bytes(), 5);

        // With room, the pinned partial entry resumes from offset 2 - the byte
        // stream must never restart from the beginning of the tag.
        writer.capacity = 100;
        let result = queue.try_flush(&mut writer).unwrap();
        assert!(matches!(result, FlushResult::Complete { bytes_written: 3, .. }));
        assert_eq!(writer.inner, b"helloworld");
        assert!(queue.is_empty());
        assert_eq!(queue.pending_bytes(), 0);
    }

    #[test]
    fn test_would_block_returns_bytes_written() {
        let mut queue = WriteQueue::new();
        queue.enqueue(Bytes::from_static(b"hello"), false, false, true, true, Instant::now());
        queue.enqueue(Bytes::from_static(b"world"), false, false, true, true, Instant::now());

        // Capacity 3: writev writes "hel" then fills mid first entry.
        let mut writer = CapWriter {
            inner: Vec::new(),
            capacity: 3,
        };
        let result = queue.try_flush(&mut writer).unwrap();
        assert!(matches!(
            result,
            FlushResult::WouldBlock { bytes_written: 3, .. }
        ));
        assert_eq!(writer.inner, b"hel");
        assert!(!queue.is_empty());
        assert_eq!(queue.front_offset(), Some(3));

        // An immediate retry with no room returns WouldBlock, zero progress -
        // no partial state is corrupted by the empty write.
        let result = queue.try_flush(&mut writer).unwrap();
        assert!(matches!(
            result,
            FlushResult::WouldBlock { bytes_written: 0, .. }
        ));
        assert_eq!(queue.front_offset(), Some(3));
    }

    // Ping-tagged bytes are reported separately by the flush, exact across
    // entry boundaries, so the connection can refuse to count a ping's own
    // delivery as liveness while still crediting ordinary bytes behind it.
    #[test]
    fn flush_reports_ping_bytes_separately() {
        let mut queue = WriteQueue::new();
        assert!(queue.enqueue_ping(Bytes::from_static(b"ping-bytes-payload")));
        queue.enqueue(Bytes::from_static(b"ordinary-data"), false, false, false, true, Instant::now());

        let mut writer: Vec<u8> = Vec::new();
        let result = queue.try_flush(&mut writer).unwrap();
        match result {
            FlushResult::Complete {
                bytes_written,
                ping_bytes_written,
            } => {
                assert_eq!(bytes_written, 18 + 13);
                assert_eq!(
                    ping_bytes_written, 18,
                    "exactly the ping entry's bytes must be attributed to the ping"
                );
            }
            other => panic!("expected Complete, got {other:?}"),
        }
        assert!(queue.is_empty());
    }

    // A serialized ping is pinned against age eviction: the watcher
    // session's serializer already committed csid-2 header history for it,
    // so dropping it unsent would make the next control message
    // header-compress against a packet the peer never received. However
    // aged, the ping must survive the Warning-band eviction sweep and be
    // delivered (and reported) as ping bytes.
    #[test]
    fn aged_ping_is_never_evicted() {
        let mut queue = WriteQueue::new();
        assert!(queue.enqueue_ping(Bytes::from_static(b"ping-bytes-payload")));
        // Age the ping far past the audio-only eviction horizon (no video
        // was ever enqueued here); it sits unwritten at the front, where
        // the eviction sweep would otherwise reap it.
        queue.queue.front_mut().unwrap().timestamp = Instant::now()
            .checked_sub(std::time::Duration::from_secs(AUDIO_ONLY_MAX_AGE_SECS + 1))
            .expect("monotonic clock should be past the eviction horizon");

        // Fill into the Warning band with audio; the next enqueue runs the
        // age-eviction sweep, which must stop at the pinned ping.
        let audio_chunk = Bytes::from(vec![0u8; QUEUE_WARN_BYTES / 2]);
        queue.enqueue(audio_chunk.clone(), false, false, false, true, Instant::now());
        queue.enqueue(audio_chunk, false, false, false, true, Instant::now());
        queue.enqueue(Bytes::from_static(b"tail"), false, false, false, true, Instant::now());

        let expected_total = 18 + QUEUE_WARN_BYTES / 2 * 2 + 4;
        assert_eq!(
            queue.pending_bytes(),
            expected_total,
            "the aged ping must survive the Warning-band eviction sweep"
        );

        let mut writer: Vec<u8> = Vec::new();
        let result = queue.try_flush(&mut writer).unwrap();
        match result {
            FlushResult::Complete {
                bytes_written,
                ping_bytes_written,
            } => {
                assert_eq!(bytes_written, expected_total);
                assert_eq!(
                    ping_bytes_written, 18,
                    "the delivered ping's bytes must still be reported as ping bytes"
                );
            }
            other => panic!("expected Complete, got {other:?}"),
        }
    }

    // Non-droppable entries (session control, handshake bytes) must NEVER
    // be shed by the backpressure policy: the session serializer already
    // committed per-csid header history for them, and a silent drop would
    // desynchronize the peer's chunk-stream decoding. Only the Critical cap
    // may refuse them — a disconnect, never a silent drop.
    #[test]
    fn shedding_policy_never_drops_non_droppable_entries() {
        let mut queue = WriteQueue::new();
        // Push into the High band with droppable keyframe video.
        queue.enqueue(make_data(QUEUE_HIGH_BYTES + 1024), true, false, true, true, Instant::now());
        assert_eq!(queue.backpressure_level(), BackpressureLevel::High);

        // A droppable non-keyframe is shed silently in the High band (the
        // pre-existing media policy), while a non-droppable control entry
        // of the same shape must be retained.
        let before = queue.pending_bytes();
        assert!(queue.enqueue(make_data(64), false, false, false, true, Instant::now()));
        assert_eq!(
            queue.pending_bytes(),
            before,
            "droppable non-keyframe data is shed in the High band"
        );
        assert!(queue.enqueue(make_data(64), false, false, false, false, Instant::now()));
        assert_eq!(
            queue.pending_bytes(),
            before + 64,
            "a non-droppable control entry must be retained in the High band"
        );
    }

    #[test]
    fn test_flush_batches_many_entries_into_few_writev() {
        // A counting writer with unlimited capacity, recording each
        // write_vectored call and the flattened byte stream.
        struct CountingWriter {
            inner: Vec<u8>,
            writev_calls: usize,
        }
        impl Write for CountingWriter {
            fn write(&mut self, buf: &[u8]) -> io::Result<usize> {
                self.inner.extend_from_slice(buf);
                Ok(buf.len())
            }
            fn write_vectored(&mut self, bufs: &[IoSlice<'_>]) -> io::Result<usize> {
                self.writev_calls += 1;
                let mut total = 0;
                for b in bufs {
                    self.inner.extend_from_slice(b);
                    total += b.len();
                }
                Ok(total)
            }
            fn flush(&mut self) -> io::Result<()> {
                Ok(())
            }
        }

        // Simulate a GOP replay: many small entries.
        let count = MAX_IOV * 2 + 5; // 133 with MAX_IOV = 64
        let mut queue = WriteQueue::new();
        let mut flat = Vec::new();
        for i in 0..count {
            let payload = vec![(i % 251) as u8; 3];
            flat.extend_from_slice(&payload);
            queue.enqueue(Bytes::from(payload), false, false, true, true, Instant::now());
        }

        let mut writer = CountingWriter {
            inner: Vec::new(),
            writev_calls: 0,
        };
        let result = queue.try_flush(&mut writer).unwrap();
        assert!(matches!(result, FlushResult::Complete { .. }));
        assert!(queue.is_empty());
        assert_eq!(queue.pending_bytes(), 0);
        // Byte order across the gathered entries must be preserved exactly.
        assert_eq!(writer.inner, flat, "writev must preserve packet order");
        // PERF-9: ceil(count / MAX_IOV) writev syscalls, not one per entry.
        let expected_calls = (count + MAX_IOV - 1) / MAX_IOV;
        assert_eq!(
            writer.writev_calls, expected_calls,
            "should batch {} entries into {} writev calls",
            count, expected_calls
        );
    }

    #[test]
    fn eviction_keeps_partially_written_front_entry() {
        struct WouldBlockWriter {
            written: usize,
            block_after: usize,
        }

        impl Write for WouldBlockWriter {
            fn write(&mut self, buf: &[u8]) -> io::Result<usize> {
                if self.written >= self.block_after {
                    return Err(io::Error::from(io::ErrorKind::WouldBlock));
                }
                let n = buf.len().min(self.block_after - self.written);
                self.written += n;
                Ok(n)
            }

            fn flush(&mut self) -> io::Result<()> {
                Ok(())
            }
        }

        let mut queue = WriteQueue::new();
        queue.enqueue(Bytes::from_static(b"hello"), false, false, true, true, Instant::now());

        // Send the first 3 bytes: the peer now holds half an FLV tag.
        let mut writer = WouldBlockWriter {
            written: 0,
            block_after: 3,
        };
        let _ = queue.try_flush(&mut writer).unwrap();
        assert_eq!(queue.front_offset(), Some(3));

        // Age it far past the eviction window, then trigger eviction via a
        // Warning-level enqueue. Evicting a half-sent entry would resume the
        // byte stream in the middle of a different tag and corrupt it.
        queue.backdate_front(60);
        queue.enqueue(make_data(QUEUE_WARN_BYTES), true, false, true, true, Instant::now());
        queue.enqueue(make_data(10), true, false, true, true, Instant::now());

        assert_eq!(
            queue.front_offset(),
            Some(3),
            "a partially written entry must never be evicted"
        );
    }

    // PERF-10: the eviction sweep must measure entry ages against the
    // caller's hoisted clock, not read the clock itself. The front entry is
    // stamped with a real `base`, and the eviction-triggering enqueue hands
    // in a synthetic `now` a full eviction window later: only an
    // implementation that uses the caller's clock sees the entry as
    // expired (a fresh `elapsed()` read would see age ~0 and keep it).
    #[test]
    fn eviction_uses_the_callers_hoisted_clock() {
        let mut queue = WriteQueue::new();
        let base = Instant::now();
        // Droppable, unstarted front entry big enough to put the NEXT
        // enqueue in the Warning band.
        queue.enqueue(make_data(QUEUE_WARN_BYTES), true, false, true, true, base);
        assert_eq!(queue.backpressure_level(), BackpressureLevel::Warning);

        let future = base + std::time::Duration::from_secs(QUEUE_MAX_AGE_SECS + 1);
        // Keyframe is kept in the Warning band; its enqueue runs the sweep.
        queue.enqueue(make_data(100), true, false, true, true, future);

        assert_eq!(
            queue.pending_bytes(),
            100,
            "the aged front entry must be evicted by the caller's clock"
        );
        assert_eq!(queue.pending_entries(), 1);
        assert!(queue.dropped_frames() > 0);
    }

    #[test]
    fn test_stats() {
        let mut queue = WriteQueue::new();
        queue.enqueue(make_data(1000), false, false, true, true, Instant::now());

        assert_eq!(queue.pending_bytes(), 1000);
        assert_eq!(queue.pending_entries(), 1);
        assert!(queue.has_video());
        assert_eq!(queue.dropped_frames(), 0);
    }

    #[test]
    fn test_pure_audio_stream() {
        let mut queue = WriteQueue::new();

        // Only enqueue audio
        queue.enqueue(make_data(100), false, false, false, true, Instant::now());
        queue.enqueue(make_data(100), false, false, false, true, Instant::now());

        assert!(!queue.has_video());
    }
}
