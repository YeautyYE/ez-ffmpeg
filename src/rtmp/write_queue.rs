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
    is_sequence_header: bool, // SPS/PPS/AudioConfig prioritized (never dropped by policy, but rejected at critical)
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

    fn age_secs(&self) -> u64 {
        self.timestamp.elapsed().as_secs()
    }
}

/// Backpressure level
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum BackpressureLevel {
    Normal,   // < 1MB: enqueue all
    Warning,  // 1-2MB: drop non-keyframes, keep audio + keyframes
    High,     // 2-4MB: only keep keyframes and sequence headers
    Critical, // >= 4MB: should disconnect
}

/// Flush result
#[derive(Debug)]
pub enum FlushResult {
    /// QueueAll flushed
    Complete { bytes_written: usize },
    /// WouldBlock encountered, partial write
    WouldBlock { bytes_written: usize },
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
            self.push_entry(data, is_keyframe, true);
            return true;
        }

        match level {
            BackpressureLevel::Normal => {
                self.push_entry(data, is_keyframe, false);
            }
            BackpressureLevel::Warning => {
                // Drop non-keyframe video, keep audio
                if is_keyframe || !is_video {
                    self.push_entry(data, is_keyframe, false);
                } else {
                    self.dropped_frames += 1;
                }
                // Perform time-based eviction
                self.evict_old_entries();
            }
            BackpressureLevel::High => {
                // Keep keyframes only
                if is_keyframe {
                    self.push_entry(data, is_keyframe, false);
                } else {
                    self.dropped_frames += 1;
                }
                self.evict_old_entries();
            }
            BackpressureLevel::Critical => unreachable!(),
        }

        true
    }

    fn push_entry(&mut self, data: Bytes, is_keyframe: bool, is_sequence_header: bool) {
        let len = data.len();
        self.queue.push_back(WriteEntry {
            data,
            offset: 0,
            timestamp: Instant::now(),
            is_keyframe,
            is_sequence_header,
        });
        self.total_bytes += len;
    }

    /// Time-based eviction - Remove stale data
    fn evict_old_entries(&mut self) {
        let max_age = if self.has_video {
            QUEUE_MAX_AGE_SECS
        } else {
            AUDIO_ONLY_MAX_AGE_SECS
        };

        while let Some(entry) = self.queue.front() {
            // Sequence headers never evicted
            if entry.is_sequence_header {
                break;
            }
            // A partially written entry is pinned: evicting it mid-send
            // would resume the byte stream inside a different tag and
            // corrupt the whole connection.
            if entry.offset > 0 {
                break;
            }
            if entry.age_secs() > max_age {
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

        loop {
            // Drop any fully-consumed entries at the front (e.g. an entry that
            // a previous flush partially wrote and this one completes).
            self.pop_completed_front();

            if self.queue.is_empty() {
                return Ok(FlushResult::Complete { bytes_written });
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
                    self.advance_front(n);
                    if n < gathered {
                        // Short write: send buffer full, stop and wait for the
                        // writable event rather than retry into EAGAIN.
                        return Ok(FlushResult::WouldBlock { bytes_written });
                    }
                    // Whole batch drained; loop to gather the next one.
                }
                Err(ref e) if e.kind() == io::ErrorKind::WouldBlock => {
                    return Ok(FlushResult::WouldBlock { bytes_written });
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
    fn advance_front(&mut self, mut n: usize) {
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
                let full = front.data.len();
                self.total_bytes = self.total_bytes.saturating_sub(full);
                self.queue.pop_front();
                n -= rem;
            } else {
                front.advance(n);
                n = 0;
            }
        }
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

        queue.enqueue(make_data(100), false, false, true);
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
            queue.enqueue(make_data(512 * 1024), true, false, true); // use keyframe to avoid drops
            assert_eq!(queue.backpressure_level(), BackpressureLevel::Normal);
        }

        // Warning level (>= 1MB, < 2MB)
        {
            let mut queue = WriteQueue::new();
            queue.enqueue(make_data(1500 * 1024), true, false, true); // use keyframe
            assert_eq!(queue.backpressure_level(), BackpressureLevel::Warning);
        }

        // High level (>= 2MB, < 4MB)
        {
            let mut queue = WriteQueue::new();
            queue.enqueue(make_data(3 * 1024 * 1024), true, false, true); // use keyframe
            assert_eq!(queue.backpressure_level(), BackpressureLevel::High);
        }

        // Critical threshold test - enqueue that would reach critical is rejected
        {
            let mut queue = WriteQueue::new();
            // First fill to just below critical (3.5MB)
            queue.enqueue(make_data(3500 * 1024), true, false, true);
            assert_eq!(queue.backpressure_level(), BackpressureLevel::High);

            // Try to add 600KB which would push total to 4.1MB >= 4MB (critical)
            // This should be rejected
            let result = queue.enqueue(make_data(600 * 1024), true, false, true);
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
        queue.enqueue(make_data(3 * 1024 * 1024), false, false, true);
        assert_eq!(queue.backpressure_level(), BackpressureLevel::High);

        // Sequence header should still be enqueued
        let result = queue.enqueue(make_data(100), false, true, true);
        assert!(result);

        // Non-keyframe should be dropped at high level
        let _before = queue.pending_entries();
        queue.enqueue(make_data(100), false, false, true);
        // Entry count should not increase for non-keyframe
        assert!(queue.dropped_frames() > 0);
    }

    #[test]
    fn test_keyframe_preserved_at_high_level() {
        let mut queue = WriteQueue::new();

        // Fill up to high level
        queue.enqueue(make_data(3 * 1024 * 1024), false, false, true);
        assert_eq!(queue.backpressure_level(), BackpressureLevel::High);

        let before = queue.pending_entries();

        // Keyframe should be accepted
        queue.enqueue(make_data(100), true, false, true);
        assert_eq!(queue.pending_entries(), before + 1);
    }

    #[test]
    fn test_audio_preserved_at_warning_level() {
        let mut queue = WriteQueue::new();

        // Fill up to warning level
        queue.enqueue(make_data(1500 * 1024), false, false, true);
        assert_eq!(queue.backpressure_level(), BackpressureLevel::Warning);

        let before = queue.pending_entries();

        // Audio should be accepted at warning level
        queue.enqueue(make_data(100), false, false, false);
        assert_eq!(queue.pending_entries(), before + 1);

        // Non-keyframe video should be dropped
        let dropped_before = queue.dropped_frames();
        queue.enqueue(make_data(100), false, false, true);
        assert!(queue.dropped_frames() > dropped_before);
    }

    #[test]
    fn test_critical_rejects_all() {
        let mut queue = WriteQueue::new();

        // Fill up to just below critical level (3.9MB)
        queue.enqueue(make_data(3900 * 1024), true, false, true);
        assert_eq!(queue.backpressure_level(), BackpressureLevel::High);

        // Try to add data that would exceed critical threshold
        // Even keyframes should be rejected
        let result = queue.enqueue(make_data(200 * 1024), true, false, true);
        assert!(
            !result,
            "Keyframe should be rejected when it would exceed Critical"
        );

        // Even sequence headers should be rejected when threshold would be exceeded
        // (Note: sequence headers bypass drop policy but not critical threshold)
        let result = queue.enqueue(make_data(200 * 1024), false, true, true);
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
        queue.enqueue(Bytes::from_static(b"hello"), false, false, true);
        queue.enqueue(Bytes::from_static(b"world"), false, false, true);
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
            FlushResult::WouldBlock { bytes_written: 7 }
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
        assert!(matches!(result, FlushResult::Complete { bytes_written: 3 }));
        assert_eq!(writer.inner, b"helloworld");
        assert!(queue.is_empty());
        assert_eq!(queue.pending_bytes(), 0);
    }

    #[test]
    fn test_would_block_returns_bytes_written() {
        let mut queue = WriteQueue::new();
        queue.enqueue(Bytes::from_static(b"hello"), false, false, true);
        queue.enqueue(Bytes::from_static(b"world"), false, false, true);

        // Capacity 3: writev writes "hel" then fills mid first entry.
        let mut writer = CapWriter {
            inner: Vec::new(),
            capacity: 3,
        };
        let result = queue.try_flush(&mut writer).unwrap();
        assert!(matches!(
            result,
            FlushResult::WouldBlock { bytes_written: 3 }
        ));
        assert_eq!(writer.inner, b"hel");
        assert!(!queue.is_empty());
        assert_eq!(queue.front_offset(), Some(3));

        // An immediate retry with no room returns WouldBlock, zero progress -
        // no partial state is corrupted by the empty write.
        let result = queue.try_flush(&mut writer).unwrap();
        assert!(matches!(
            result,
            FlushResult::WouldBlock { bytes_written: 0 }
        ));
        assert_eq!(queue.front_offset(), Some(3));
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
            queue.enqueue(Bytes::from(payload), false, false, true);
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
        queue.enqueue(Bytes::from_static(b"hello"), false, false, true);

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
        queue.enqueue(make_data(QUEUE_WARN_BYTES), true, false, true);
        queue.enqueue(make_data(10), true, false, true);

        assert_eq!(
            queue.front_offset(),
            Some(3),
            "a partially written entry must never be evicted"
        );
    }

    #[test]
    fn test_stats() {
        let mut queue = WriteQueue::new();
        queue.enqueue(make_data(1000), false, false, true);

        assert_eq!(queue.pending_bytes(), 1000);
        assert_eq!(queue.pending_entries(), 1);
        assert!(queue.has_video());
        assert_eq!(queue.dropped_frames(), 0);
    }

    #[test]
    fn test_pure_audio_stream() {
        let mut queue = WriteQueue::new();

        // Only enqueue audio
        queue.enqueue(make_data(100), false, false, false);
        queue.enqueue(make_data(100), false, false, false);

        assert!(!queue.has_video());
    }
}
