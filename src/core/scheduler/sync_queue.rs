//! Frame/packet synchronisation queue — a faithful port of the `-shortest`
//! truncation engine in FFmpeg's `fftools/sync_queue.c` (algorithm stable across
//! n7.1 → n8.2).
//!
//! One generic engine backs both the encoder queue (`T = FrameBox`) and the
//! muxer queue (`T = PacketBox`), mirroring FFmpeg's single `SyncQueue` that is
//! generic over `SYNC_QUEUE_FRAMES`/`SYNC_QUEUE_PACKETS`. The engine is
//! payload-agnostic: the CALLER computes each item's presentation-end timestamp
//! (`frame_end` in the C source) and threads it in via [`SyncQueue::send`], so
//! `T` needs no trait bounds. That keeps audio vs. video/packet end-timestamp
//! math (`pts + rescale(nb_samples,{1,sr},tb)` vs. `pts + duration`) at the call
//! site, exactly where FFmpeg computes it in `sq_send`.
//!
//! Ported 1:1 from the sync_queue.c bodies: `tb_update`, `finish_stream`,
//! `stream_update_ts`, `queue_head_update`, `overflow_heartbeat`, the `sq_send`
//! enqueue + `frames_max` accounting, `sq_limit_frames`, the `receive_for_stream`
//! release condition and its EOF predicate ([`SyncQueue::is_stream_drained`]),
//! and the `sq_receive(-1)` receive-any + heartbeat-retry loop
//! ([`SyncQueue::drain_all_releasable`]).
//!
//! Deliberately **omitted**: the `sq_frame_samples` / `receive_samples`
//! constant-frame-size audio splitter (ez resizes audio downstream). The
//! `frame_samples` field and its `frames_sent = samples_sent / frame_samples`
//! branch are ported for `sq_send` parity but stay dormant — there is no
//! `sq_frame_samples` setter, so `frame_samples` is always 0 and every item
//! passes through whole. This does not change the truncation algorithm.
//!
//! Ownership: each stream's FIFO owns its items by value. An over-bound item that
//! never releases stays in the FIFO — exactly as FFmpeg leaves it in the
//! container FIFO until `sq_free` — and is dropped when the queue (or its stream)
//! is dropped, which RAII-frees the underlying `AVFrame` / `AVPacket`. There is
//! deliberately no drop-on-finish path (matches `finish_stream`, which only sets
//! flags); memory is bounded by the cascade-stop + heartbeat, not by dropping.

// The engine's public surface is exercised by the unit tests below and is wired
// into the encoder / mux workers by the integration pass; until then the
// cross-module call sites do not exist yet.
#![allow(dead_code)]

use ffmpeg_sys_next::{av_compare_ts, av_rescale_q, AVRational, AV_NOPTS_VALUE, AV_TIME_BASE_Q};
use std::collections::VecDeque;

/// `av_compare_ts` wrapper. Returns -1/0/1 for `a <=> b` across timebases.
#[inline]
fn compare_ts(ts_a: i64, tb_a: AVRational, ts_b: i64, tb_b: AVRational) -> i32 {
    // SAFETY: av_compare_ts is pure integer arithmetic over the supplied
    // timebases; it dereferences no pointers and cannot block.
    unsafe { av_compare_ts(ts_a, tb_a, ts_b, tb_b) }
}

/// `av_rescale_q` wrapper. Rescales `a` from `from` timebase to `to` timebase.
#[inline]
fn rescale_q(a: i64, from: AVRational, to: AVRational) -> i64 {
    // SAFETY: av_rescale_q is pure integer arithmetic; no pointers, non-blocking.
    unsafe { av_rescale_q(a, from, to) }
}

/// One buffered item plus its cached presentation-end timestamp (in the owning
/// stream's timebase). `end_ts == AV_NOPTS_VALUE` marks an untimestamped item,
/// which is always passed straight through on release (mirrors the C source).
struct QueuedItem<T> {
    item: T,
    end_ts: i64,
}

/// Per-stream state (mirrors `SyncQueueStream`).
struct SqStream<T> {
    /// FIFO tail = front = smallest end time; head = back = largest end time.
    fifo: VecDeque<QueuedItem<T>>,
    /// This stream's timebase. Defaults to `{1,1}` and is captured from the first
    /// item's timebase on `send` (mirrors `tb_update`), so a stream that never
    /// receives a frame still has a valid timebase for `overflow_heartbeat`.
    tb: AVRational,
    /// Largest end timestamp seen. `None` == `AV_NOPTS_VALUE` (explicit validity):
    /// an untimestamped finish must never be read as a `0` bound.
    head_ts: Option<i64>,
    /// Participates in `-shortest` truncation.
    limiting: bool,
    /// No more items will arrive for this stream.
    finished: bool,
    /// Audio samples currently buffered (`sq_send` :371). Write-only in ez: the
    /// constant-frame-size splitter that consumes it is omitted; tracked for
    /// send-path parity with FFmpeg.
    samples_queued: u64,
    /// Total audio samples ever sent (`sq_send` :372), drives `frames_sent` when
    /// a constant `frame_samples` is set.
    samples_sent: u64,
    /// Frames emitted into the queue so far (`sq_send` :374-377); compared against
    /// `frames_max`.
    frames_sent: u64,
    /// Cap on `frames_sent` before the stream self-finishes (`sq_limit_frames`).
    /// Defaults to `u64::MAX` (no cap), matching `sq_add_stream` :620.
    frames_max: u64,
    /// Constant output frame size in samples; 0 = disabled (ez always 0, no
    /// `sq_frame_samples` setter). Kept for `sq_send` accounting parity.
    frame_samples: i32,
}

/// Outcome of [`SyncQueue::send`] for the target stream.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum SqSend {
    /// The item was accepted (or the finish did not retire this stream).
    Accepted,
    /// This stream is now finished — by its own EOF, `frames_max`, or the finish
    /// cascade. The caller flushes and stops feeding this stream.
    StreamFinished,
}

/// The `-shortest` sync queue (mirrors `struct SyncQueue`).
pub(crate) struct SyncQueue<T> {
    streams: Vec<SqStream<T>>,
    /// All streams are finished.
    finished: bool,
    /// The limiting stream with the *smallest* head timestamp; it gates release.
    /// `None` until every limiting stream has produced one timestamp.
    head_stream: Option<usize>,
    /// The finished limiting stream with the smallest finish timestamp.
    head_finished_stream: Option<usize>,
    /// Maximum buffering duration in microseconds (`-shortest_buf_duration`).
    buf_size_us: i64,
    have_limiting: bool,
    /// Cascade-finish side-channel: streams that transitioned unfinished->finished
    /// since the last [`SyncQueue::newly_finished`] drain, in finish order.
    pending_finished: Vec<usize>,
}

impl<T> SyncQueue<T> {
    /// Create an empty queue (mirrors `sq_alloc`).
    pub(crate) fn new(buf_size_us: i64) -> Self {
        SyncQueue {
            streams: Vec::new(),
            finished: false,
            head_stream: None,
            head_finished_stream: None,
            buf_size_us,
            have_limiting: false,
            pending_finished: Vec::new(),
        }
    }

    /// Register a stream and return its index (mirrors `sq_add_stream`).
    pub(crate) fn add_stream(&mut self, limiting: bool) -> usize {
        let idx = self.streams.len();
        self.streams.push(SqStream {
            fifo: VecDeque::new(),
            // Valid default so a stream that never receives a real timebase (and
            // no frames) does not stall the others; cf. `overflow_heartbeat`.
            tb: AVRational { num: 1, den: 1 },
            head_ts: None,
            limiting,
            finished: false,
            samples_queued: 0,
            samples_sent: 0,
            frames_sent: 0,
            frames_max: u64::MAX,
            frame_samples: 0,
        });
        self.have_limiting |= limiting;
        idx
    }

    /// Feed one item or finish the stream. `item = Some(payload)` enqueues; `item
    /// = None` finishes the stream (mirrors `sq_send` null-frame branch :342-346).
    /// `end_ts` is the caller-computed presentation end in `tb` units (`None` ==
    /// `AV_NOPTS_VALUE`); `tb` is this stream's timebase, threaded through because
    /// the engine needs it for cross-stream `av_compare_ts`. `nb_samples` is the
    /// frame's audio sample count (0 for packets / non-audio), used for the
    /// `frames_max` accounting. Mirrors `sq_send` :333-387.
    pub(crate) fn send(
        &mut self,
        idx: usize,
        item: Option<T>,
        end_ts: Option<i64>,
        tb: AVRational,
        nb_samples: i32,
    ) -> SqSend {
        match item {
            // Null item == EOF: run the finish cascade (sq_send frame_null branch).
            None => self.finish_stream(idx),
            Some(payload) => {
                // Sending to an already-finished stream: FFmpeg returns
                // AVERROR_EOF without taking ownership (sq_send :347-348). Our
                // payload is owned, so letting it fall out of scope RAII-frees the
                // underlying frame/packet — the item is over-bound and discarded,
                // matching FFmpeg's net effect.
                if self.streams[idx].finished {
                    return SqSend::StreamFinished;
                }

                self.tb_update(idx, tb);
                let end = end_ts.unwrap_or(AV_NOPTS_VALUE);
                self.streams[idx].fifo.push_back(QueuedItem {
                    item: payload,
                    end_ts: end,
                });
                self.stream_update_ts(idx, end);

                // frames_max accounting (sq_send :371-384). nb_samples is >= 0 by
                // the caller contract (audio sample count, or 0 for packets).
                let ns = nb_samples as u64;
                let reached_max = {
                    let st = &mut self.streams[idx];
                    st.samples_queued += ns;
                    st.samples_sent += ns;
                    if st.frame_samples != 0 {
                        st.frames_sent = st.samples_sent / st.frame_samples as u64;
                    } else {
                        st.frames_sent += 1;
                    }
                    st.frames_sent >= st.frames_max
                };
                if reached_max {
                    self.finish_stream(idx);
                }
            }
        }

        if self.streams[idx].finished {
            SqSend::StreamFinished
        } else {
            SqSend::Accepted
        }
    }

    /// Cap the number of frames this stream will emit into the queue — mirrors
    /// `sq_limit_frames` (:628-638). Sets `frames_max`; if the stream has already
    /// sent at least that many frames, finish it now (which cascades `-shortest`).
    pub(crate) fn sq_limit_frames(&mut self, idx: usize, max: u64) {
        self.streams[idx].frames_max = max;
        if self.streams[idx].frames_sent >= self.streams[idx].frames_max {
            self.finish_stream(idx);
        }
    }

    /// Pop stream `idx`'s releasable items — front (tail) first, in FIFO order —
    /// appending each payload to `out`. An item releases when its end timestamp
    /// does not overtake the queue head, or when it is untimestamped, or when
    /// there are no limiting streams. Never blocks. Mirrors repeated
    /// `receive_for_stream(sq, idx, ...)` until `EAGAIN`/`EOF`.
    pub(crate) fn drain_releasable_into(&mut self, idx: usize, out: &mut Vec<T>) {
        // Read the front end_ts (Copy) without holding a borrow, so the body can
        // pop the FIFO. Stops at the first held item or an empty FIFO.
        while let Some(front_end) = self.streams[idx].fifo.front().map(|q| q.end_ts) {
            if !self.front_releasable(idx, front_end) {
                break;
            }
            let q = self.streams[idx].fifo.pop_front().expect("front present");
            out.push(q.item);
        }
    }

    /// Receive-any drain for the single-threaded muxer path — mirrors looping
    /// `sq_receive(sq, -1)` (sync_queue.c :562-596) until it stops yielding. Each
    /// pass scans every stream (the `receive_internal` stream_idx=-1 branch,
    /// :573-583), releasing all currently-releasable items; when a full pass
    /// releases nothing (EAGAIN) it fires one `overflow_heartbeat` (:592-593) and
    /// re-scans, stopping only when a pass releases nothing and the heartbeat
    /// advances nothing. Every released item is appended to `out`. Never blocks.
    pub(crate) fn drain_all_releasable(&mut self, out: &mut Vec<T>) {
        loop {
            let before = out.len();
            for i in 0..self.streams.len() {
                self.drain_releasable_into(i, out);
            }
            if out.len() == before {
                // Full receive-any scan released nothing: try one heartbeat, then
                // re-scan; stop when the heartbeat cannot advance anything either.
                if !self.heartbeat() {
                    break;
                }
            }
        }
    }

    /// Drain-phase exit predicate — mirrors the `receive_for_stream` EOF condition
    /// (sync_queue.c :558-559): a stream is drained when the whole queue is
    /// finished, or when the stream itself is finished and its FIFO is empty.
    /// Note it is NOT simply "FIFO empty": once the whole queue is finished, a
    /// stream is drained even if its FIFO still holds over-bound frames — those
    /// are abandoned (freed by RAII on drop), exactly as FFmpeg leaves them in the
    /// container FIFO until `sq_free`.
    pub(crate) fn is_stream_drained(&self, idx: usize) -> bool {
        self.finished || (self.streams[idx].finished && self.streams[idx].fifo.is_empty())
    }

    /// Drain the cascade-finish side-channel: every stream that transitioned
    /// unfinished->finished since the last call, each reported exactly once, in
    /// the order it finished. Exposes the `finished = 1` transitions (sq_send EOF
    /// :344, `finish_stream` self :170 and secondary cascade :184-195,
    /// `stream_update_ts` -> finish :255-259, `frames_max` :379-384) that the
    /// `-shortest` dispatch needs to cascade-stop upstream workers.
    pub(crate) fn newly_finished(&mut self, out: &mut Vec<usize>) {
        out.append(&mut self.pending_finished);
    }

    /// Bounded-buffer anti-stall (mirrors `overflow_heartbeat(sq, -1)`): pick the
    /// most-ahead stream, and if its tail lags its head by at least `buf_size_us`,
    /// fake-advance every stream that pins that tail so the ahead stream can
    /// drain. Safe to call on an idle tick with no new item. Returns `true` if it
    /// advanced anything (the C returns `1`/`0`); the caller can then re-drain.
    pub(crate) fn heartbeat(&mut self) -> bool {
        // Pick the stream that is most ahead (largest head_ts).
        let mut best: Option<usize> = None;
        for i in 0..self.streams.len() {
            let Some(hts) = self.streams[i].head_ts else {
                continue;
            };
            match best {
                None => best = Some(i),
                Some(b) => {
                    let b_ts = self.streams[b].head_ts.expect("best has ts");
                    if compare_ts(b_ts, self.streams[b].tb, hts, self.streams[i].tb) < 0 {
                        best = Some(i);
                    }
                }
            }
        }
        // No stream has a timestamp yet -> nothing to do.
        let Some(sidx) = best else { return false };

        let st_tb = self.streams[sidx].tb;
        let st_head_ts = self.streams[sidx].head_ts.expect("best has ts");

        // Tail timestamp = first front item with a real end (scan past NOPTS ones).
        let mut tail_ts = AV_NOPTS_VALUE;
        for q in self.streams[sidx].fifo.iter() {
            tail_ts = q.end_ts;
            if tail_ts != AV_NOPTS_VALUE {
                break;
            }
        }

        // Overflow triggers when the tail is over buf_size_us behind the head.
        if tail_ts == AV_NOPTS_VALUE
            || tail_ts >= st_head_ts
            || rescale_q(st_head_ts - tail_ts, st_tb, AV_TIME_BASE_Q) < self.buf_size_us
        {
            return false;
        }

        // Signal a fake timestamp for every stream that prevents tail_ts's release.
        tail_ts += 1;
        // Report whether we actually fake-advanced at least one stream. The C
        // `overflow_heartbeat` returns 1 unconditionally, but fftools retries the
        // receive-any scan only once; ez's `drain_all_releasable` loops on this
        // return, so a heartbeat that skips every stream (all == sidx / finished /
        // already ahead of tail_ts) must report `false` or the drain spins forever.
        let mut advanced = false;
        for i in 0..self.streams.len() {
            if i == sidx || self.streams[i].finished {
                continue;
            }
            if let Some(hts) = self.streams[i].head_ts {
                if compare_ts(tail_ts, st_tb, hts, self.streams[i].tb) <= 0 {
                    continue;
                }
            }
            let mut ts = rescale_q(tail_ts, st_tb, self.streams[i].tb);
            if let Some(hts) = self.streams[i].head_ts {
                ts = ts.max(hts + 1);
            }
            self.stream_update_ts(i, ts);
            advanced = true;
        }

        advanced
    }

    /// Whether every stream is finished (mirrors `sq->finished`).
    pub(crate) fn is_finished(&self) -> bool {
        self.finished
    }

    // ----- internal ports of the static sync_queue.c helpers ------------------

    /// Record a genuine unfinished->finished transition for `newly_finished`.
    /// Every `finished = 1` assignment in sync_queue.c flows through here so the
    /// cascade side-channel reports each stream exactly once and never doubles.
    fn mark_finished(&mut self, idx: usize) {
        if self.streams[idx].finished {
            return;
        }
        self.streams[idx].finished = true;
        self.pending_finished.push(idx);
    }

    /// Front-item release test — the `receive_for_stream` condition with the
    /// audio-splitting path omitted.
    fn front_releasable(&self, idx: usize, front_end: i64) -> bool {
        // Untimestamped items pass through unconditionally.
        if front_end == AV_NOPTS_VALUE {
            return true;
        }
        // No limiting streams -> the queue is a pass-through reorder buffer.
        if !self.have_limiting {
            return true;
        }
        // Limiting streams exist but the head is not established yet -> hold.
        let Some(h) = self.head_stream else {
            return false;
        };
        let Some(head_ts) = self.streams[h].head_ts else {
            return false;
        };
        compare_ts(front_end, self.streams[idx].tb, head_ts, self.streams[h].tb) <= 0
    }

    /// Capture / update a stream's timebase (mirrors `tb_update`). The timebase is
    /// set from the first item and may only change while the FIFO is empty, in
    /// which case any established head timestamp is rescaled.
    fn tb_update(&mut self, idx: usize, tb: AVRational) {
        let st = &mut self.streams[idx];
        if tb.num == st.tb.num && tb.den == st.tb.den {
            return;
        }
        // Timebase must not change once frames are buffered.
        debug_assert!(st.fifo.is_empty(), "sync queue timebase changed mid-stream");
        if let Some(hts) = st.head_ts {
            st.head_ts = Some(rescale_q(hts, st.tb, tb));
        }
        st.tb = tb;
    }

    /// Raise a stream's head timestamp and propagate (mirrors `stream_update_ts`).
    /// `ts` is in `streams[idx].tb` units.
    fn stream_update_ts(&mut self, idx: usize, ts: i64) {
        if ts == AV_NOPTS_VALUE {
            return;
        }
        if let Some(hts) = self.streams[idx].head_ts {
            if hts >= ts {
                return;
            }
        }
        self.streams[idx].head_ts = Some(ts);

        // If this stream is now at/past a finished stream's head, finish it too.
        if let Some(hf) = self.head_finished_stream {
            let hf_ts = self.streams[hf].head_ts.expect("finished head has ts");
            let hf_tb = self.streams[hf].tb;
            let st_tb = self.streams[idx].tb;
            if compare_ts(hf_ts, hf_tb, ts, st_tb) <= 0 {
                self.finish_stream(idx);
            }
        }

        // Recompute the overall head only if it could have changed.
        if self.streams[idx].limiting
            && (self.head_stream.is_none() || self.head_stream == Some(idx))
        {
            self.queue_head_update();
        }
    }

    /// Recompute the queue head = limiting stream with the smallest head timestamp
    /// (mirrors `queue_head_update`). The head stays unset until every limiting
    /// stream has produced at least one timestamp.
    fn queue_head_update(&mut self) {
        debug_assert!(self.have_limiting);

        if self.head_stream.is_none() {
            let mut first_limiting: Option<usize> = None;
            for i in 0..self.streams.len() {
                if !self.streams[i].limiting {
                    continue;
                }
                // Wait for one timestamp in each limiting stream.
                if self.streams[i].head_ts.is_none() {
                    // EZ divergence from FFmpeg sync_queue.c queue_head_update: a
                    // finished limiting stream that never got a timestamp does not
                    // gate the head (FFmpeg would stall; ez prefers progress). Its
                    // `overflow_heartbeat` rescue is impossible (that helper skips
                    // finished streams), so without this a degenerate finished
                    // untimestamped limiter hangs the whole queue. Non-degenerate
                    // cases are unaffected: a *live* untimestamped limiter still
                    // gates exactly as FFmpeg does.
                    if self.streams[i].finished {
                        continue;
                    }
                    return;
                }
                if first_limiting.is_none() {
                    first_limiting = Some(i);
                }
            }
            match first_limiting {
                Some(fi) => self.head_stream = Some(fi),
                None => return,
            }
        }

        for i in 0..self.streams.len() {
            if !self.streams[i].limiting {
                continue;
            }
            let Some(other_ts) = self.streams[i].head_ts else {
                continue;
            };
            let other_tb = self.streams[i].tb;
            let h = self.head_stream.expect("head set above");
            let head_ts = self.streams[h].head_ts.expect("head has ts");
            let head_tb = self.streams[h].tb;
            if compare_ts(other_ts, other_tb, head_ts, head_tb) < 0 {
                self.head_stream = Some(i);
            }
        }
    }

    /// Finish a stream and cascade `-shortest` truncation (mirrors `finish_stream`).
    /// A limiting stream that finishes with a valid head forces every stream
    /// already at/past that head to finish too. An untimestamped finish
    /// (`head_ts == None`) skips the whole block, so peers are never truncated to
    /// a bogus `0` bound. This only sets flags — it NEVER trims a FIFO, so
    /// over-bound items stay buffered until drop (matches sync_queue.c :161-206).
    fn finish_stream(&mut self, idx: usize) {
        // Primary transition (idempotent), mirrors finish_stream :170.
        self.mark_finished(idx);

        let limiting = self.streams[idx].limiting;
        let head_ts = self.streams[idx].head_ts;
        if limiting {
            if let Some(st_ts) = head_ts {
                let st_tb = self.streams[idx].tb;

                // Is this the new finished head (smallest finished limiting head)?
                // mirrors finish_stream :174-179.
                let take = match self.head_finished_stream {
                    None => true,
                    Some(hf) => {
                        let hf_ts = self.streams[hf].head_ts.expect("finished head has ts");
                        let hf_tb = self.streams[hf].tb;
                        compare_ts(st_ts, st_tb, hf_ts, hf_tb) < 0
                    }
                };
                if take {
                    self.head_finished_stream = Some(idx);
                }

                // Secondary cascade: finish every stream ahead of the finished
                // head (mirrors finish_stream :183-195).
                let hf = self.head_finished_stream.expect("just set");
                let hf_ts = self.streams[hf].head_ts.expect("finished head has ts");
                let hf_tb = self.streams[hf].tb;
                for i in 0..self.streams.len() {
                    if i == hf {
                        continue;
                    }
                    if let Some(other_ts) = self.streams[i].head_ts {
                        let other_tb = self.streams[i].tb;
                        if compare_ts(hf_ts, hf_tb, other_ts, other_tb) <= 0 {
                            self.mark_finished(i);
                        }
                    }
                }
            }
        }

        // The whole queue is finished once every stream is finished (:198-203).
        if self.streams.iter().all(|s| s.finished) {
            self.finished = true;
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[derive(Debug, Clone, Copy, PartialEq, Eq)]
    struct Id(i64);

    fn tb(num: i32, den: i32) -> AVRational {
        AVRational { num, den }
    }

    /// Send a timestamped item on `idx` with timebase `t` (no audio samples).
    fn push(q: &mut SyncQueue<Id>, idx: usize, id: i64, end_ts: i64, t: AVRational) -> SqSend {
        q.send(idx, Some(Id(id)), Some(end_ts), t, 0)
    }

    /// Finish stream `idx` (null item). `tb`/`nb_samples` are unused on this path.
    fn finish(q: &mut SyncQueue<Id>, idx: usize) -> SqSend {
        q.send(idx, None, None, tb(1, 1), 0)
    }

    fn drain(q: &mut SyncQueue<Id>, idx: usize) -> Vec<i64> {
        let mut out = Vec::new();
        q.drain_releasable_into(idx, &mut out);
        out.into_iter().map(|Id(v)| v).collect()
    }

    fn drain_all(q: &mut SyncQueue<Id>) -> Vec<i64> {
        let mut out = Vec::new();
        q.drain_all_releasable(&mut out);
        out.into_iter().map(|Id(v)| v).collect()
    }

    // Release an item whose end == the head, hold one whose end > the head.
    #[test]
    fn release_at_and_just_past_head() {
        let ms = tb(1, 1000);
        let mut q = SyncQueue::<Id>::new(10_000_000);
        let a = q.add_stream(true);
        let b = q.add_stream(true);

        // Head not established until both limiting streams have a timestamp.
        push(&mut q, a, 1, 1000, ms);
        assert!(drain(&mut q, a).is_empty(), "no head yet -> hold");
        push(&mut q, b, 100, 1000, ms);

        // head = 1000; A's front (end 1000) releases at the head.
        assert_eq!(drain(&mut q, a), vec![1]);

        // A moves to 2000, but B pins the head at 1000 -> A's 2000 is held.
        push(&mut q, a, 2, 2000, ms);
        assert!(drain(&mut q, a).is_empty(), "end just past head -> hold");

        // B advances to 2000 -> head 2000 -> A's 2000 now releases.
        push(&mut q, b, 101, 2000, ms);
        assert_eq!(drain(&mut q, a), vec![2]);
    }

    // A limiting stream finishing with a valid head finishes over-bound peers.
    #[test]
    fn finish_stream_finishes_overbound_peers() {
        let ms = tb(1, 1000);
        let mut q = SyncQueue::<Id>::new(10_000_000);
        let a = q.add_stream(true);
        let b = q.add_stream(true);

        push(&mut q, a, 1, 3000, ms); // A head 3000
        for (i, end) in [1000, 2000, 3000, 4000, 5000].iter().enumerate() {
            push(&mut q, b, 100 + i as i64, *end, ms); // B head climbs to 5000
        }

        // A finishes at 3000; B (head 5000) is ahead of the finished head -> finished.
        assert_eq!(finish(&mut q, a), SqSend::StreamFinished);
        assert!(q.is_finished(), "both streams finished");

        // B releases only its <= 3000 items; 4000/5000 stay buffered (over-bound,
        // abandoned — never released, never trimmed).
        assert_eq!(drain(&mut q, b), vec![100, 101, 102]);
    }

    // overflow_heartbeat fires at exactly buf_size_us and releases the stuck tail,
    // across two different timebases; it is a no-op just below the threshold.
    #[test]
    fn heartbeat_releases_at_exactly_buf_size() {
        let a_tb = tb(1, 1000); // milliseconds
        let b_tb = tb(1, 90000); // 90 kHz

        // The A-head(3s) - A-tail(1s) = 2s gap; set buf to exactly 2s.
        let build = |buf_us: i64| {
            let mut q = SyncQueue::<Id>::new(buf_us);
            let a = q.add_stream(true);
            let b = q.add_stream(true);
            push(&mut q, a, 1, 1000, a_tb); // A tail = 1s
            push(&mut q, b, 100, 0, b_tb); // B head = 0 -> queue head pinned low
            push(&mut q, a, 2, 3000, a_tb); // A head = 3s
            (q, a, b)
        };

        // Just below the threshold: no heartbeat, tail stays stuck.
        let (mut q, a, _b) = build(2_000_001);
        assert!(
            drain(&mut q, a).is_empty(),
            "A tail held behind stalled head"
        );
        assert!(!q.heartbeat(), "gap < buf_size_us -> no heartbeat");
        assert!(drain(&mut q, a).is_empty());

        // Exactly at the threshold: heartbeat fires and the tail releases.
        let (mut q, a, _b) = build(2_000_000);
        assert!(drain(&mut q, a).is_empty());
        assert!(q.heartbeat(), "gap == buf_size_us -> heartbeat");
        assert_eq!(
            drain(&mut q, a),
            vec![1],
            "stuck tail released after heartbeat"
        );
    }

    // R4: an untimestamped finish must NOT collapse peers' bound to 0.
    #[test]
    fn untimestamped_finish_does_not_truncate_peers() {
        let ms = tb(1, 1000);
        let mut q = SyncQueue::<Id>::new(10_000_000);
        let a = q.add_stream(true);
        let b = q.add_stream(true);

        for (i, end) in [1000, 2000, 3000, 4000, 5000].iter().enumerate() {
            push(&mut q, b, 100 + i as i64, *end, ms);
        }

        // A finishes without ever producing a timestamp (head_ts == None).
        assert_eq!(finish(&mut q, a), SqSend::StreamFinished);

        // The finish must not treat None as 0: B stays live, queue not finished.
        assert!(
            !q.is_finished(),
            "untimestamped finish must not finish the queue"
        );
        // No further send re-runs queue_head_update, so the head is still
        // unestablished and B holds — but crucially it was NOT truncated to a
        // bogus 0 bound.
        assert!(drain(&mut q, b).is_empty());
    }

    // A finished stream's in-bound tail releases once heartbeat fake-advances the
    // stalled laggard that was pinning the head below it.
    #[test]
    fn finished_stream_fake_advance_then_release() {
        let ms = tb(1, 1000);
        let mut q = SyncQueue::<Id>::new(1_000_000); // 1s

        let a = q.add_stream(true);
        let b = q.add_stream(true);

        push(&mut q, a, 1, 1000, ms); // A tail 1s
        push(&mut q, a, 2, 2000, ms); // A head 2s
        assert_eq!(finish(&mut q, a), SqSend::StreamFinished); // A finished, no peer ts yet

        push(&mut q, b, 100, 500, ms); // B stalls at 0.5s -> head pinned at 0.5s

        // A is finished but its in-bound tail (1s) is stuck behind B's head (0.5s).
        assert!(drain(&mut q, a).is_empty());

        // Gap A-head(2s) - A-tail(1s) = 1s == buf -> heartbeat advances B past 1s.
        assert!(q.heartbeat());
        assert_eq!(drain(&mut q, a), vec![1], "finished stream's tail released");
    }

    // The caller's two frame_end formulas (audio samples, packet duration) feed
    // the engine and truncate correctly.
    #[test]
    fn audio_and_packet_end_ts() {
        let a_tb = tb(1, 48000); // audio, 48 kHz sample-rate timebase
        let sr = 48000;
        let p_tb = tb(1, 90000); // packet stream

        let mut q = SyncQueue::<Id>::new(10_000_000);
        let a = q.add_stream(true); // audio (short, limiting)
        let p = q.add_stream(true); // packets (long, limiting)

        // Audio: end_ts = pts + rescale(nb_samples, {1, sr}, tb). 3 frames, 0.1s.
        let nb_samples = 1600;
        let mut pts = 0i64;
        for i in 0..3 {
            let end = pts + rescale_q(nb_samples, tb(1, sr), a_tb);
            push(&mut q, a, i, end, a_tb);
            pts = end;
        }
        assert_eq!(finish(&mut q, a), SqSend::StreamFinished); // audio ends at 4800 (=0.1s)

        // Packets: end_ts = pts + duration. 4 packets, 0.05s each.
        let dur = 4500i64;
        for i in 0..4 {
            let ppts = i * dur;
            push(&mut q, p, 100 + i, ppts + dur, p_tb);
        }

        // Bound = audio head 0.1s; packets ending at 0.05s and 0.1s release,
        // 0.15s / 0.2s stay buffered (over-bound) by the cascade.
        assert!(q.is_finished());
        assert_eq!(drain(&mut q, p), vec![100, 101]);
    }

    // drain_releasable_into returns ONLY this stream's <= head items, in FIFO order.
    #[test]
    fn drain_fifo_order_only_this_stream() {
        let ms = tb(1, 1000);
        let mut q = SyncQueue::<Id>::new(10_000_000);
        let a = q.add_stream(true);
        let b = q.add_stream(true);

        push(&mut q, a, 10, 1000, ms);
        push(&mut q, a, 11, 2000, ms);
        push(&mut q, a, 12, 3000, ms); // A head 3000
        push(&mut q, b, 20, 1000, ms);
        push(&mut q, b, 21, 2000, ms); // B head 2000 -> queue head = 2000

        // A releases 1000,2000 in order; 3000 is held; no B ids leak in.
        assert_eq!(drain(&mut q, a), vec![10, 11]);
        // B releases both of its own items.
        assert_eq!(drain(&mut q, b), vec![20, 21]);
    }

    // Canonical -shortest: A ends at 3s, B at 5s. B's over-3s items stay buffered
    // (never released) by the finish cascade; A is fully released.
    #[test]
    fn two_stream_shortest() {
        let ms = tb(1, 1000);
        let mut q = SyncQueue::<Id>::new(10_000_000);
        let a = q.add_stream(true);
        let b = q.add_stream(true);

        // Interleave: B reaches 5s (buffered) before A's EOF is observed.
        push(&mut q, a, 1, 1000, ms);
        push(&mut q, b, 101, 1000, ms);
        push(&mut q, a, 2, 2000, ms);
        push(&mut q, b, 102, 2000, ms);
        push(&mut q, a, 3, 3000, ms);
        push(&mut q, b, 103, 3000, ms);
        push(&mut q, b, 104, 4000, ms); // over-bound, buffered
        push(&mut q, b, 105, 5000, ms); // over-bound, buffered

        assert_eq!(finish(&mut q, a), SqSend::StreamFinished); // A ends at 3s
        assert!(q.is_finished(), "cascade finished B at the 3s bound");

        // A fully released; B truncated to its <= 3s prefix.
        assert_eq!(drain(&mut q, a), vec![1, 2, 3]);
        assert_eq!(drain(&mut q, b), vec![101, 102, 103]);
    }

    // ----- new coverage for the drain-phase extensions ------------------------

    // is_stream_drained is NOT "fifo empty": it is false while a finished stream
    // holds over-bound frames and the queue is still live, and flips true once the
    // whole queue finishes even with those frames still buffered.
    #[test]
    fn is_stream_drained_predicate() {
        let ms = tb(1, 1000);
        let mut q = SyncQueue::<Id>::new(10_000_000);
        let a = q.add_stream(true);
        let b = q.add_stream(true);
        let c = q.add_stream(true);

        push(&mut q, a, 1, 3000, ms); // A head 3s (the short one)
        for (i, end) in [1000, 2000, 3000, 4000, 5000].iter().enumerate() {
            push(&mut q, b, 100 + i as i64, *end, ms); // B head 5s
        }
        push(&mut q, c, 50, 1000, ms); // C at 1s: live and *behind* the 3s bound

        // Finish A at 3s -> cascade finishes B (ahead), leaves C (behind) live.
        finish(&mut q, a);
        assert!(!q.is_finished(), "C still live -> queue not finished");

        // B is finished and holds over-bound frames (4s,5s past the 3s bound), but
        // the queue is not finished -> NOT drained (it is not merely "fifo empty").
        assert!(
            !q.is_stream_drained(b),
            "finished stream with over-bound frames, queue live -> not drained"
        );
        // C is not finished and has a frame -> not drained.
        assert!(!q.is_stream_drained(c));

        // Finish C -> now every stream is finished -> the whole queue is finished.
        finish(&mut q, c);
        assert!(q.is_finished());

        // Whole queue finished -> B is drained EVEN though its over-bound 4s,5s are
        // still buffered (they are abandoned, not required to be popped first).
        assert!(
            q.is_stream_drained(b),
            "whole queue finished -> drained despite over-bound frames still in fifo"
        );
        // And its over-bound frames really are still there (never trimmed).
        assert_eq!(q.streams[b].fifo.len(), 5);
    }

    // is_stream_drained is true for a finished stream with an empty fifo, even
    // while the queue as a whole is still live.
    #[test]
    fn is_stream_drained_finished_empty_fifo() {
        let mut q = SyncQueue::<Id>::new(10_000_000);
        let a = q.add_stream(true);
        let b = q.add_stream(true);

        finish(&mut q, a); // A finishes with an empty fifo (no sends)
        assert!(!q.is_finished(), "B still live");
        assert!(q.is_stream_drained(a), "finished + empty fifo -> drained");
        assert!(!q.is_stream_drained(b), "B not finished -> not drained");
    }

    // attack-4 no-loss: a limiting stream that finishes while the *other* limiting
    // stream is still live must lose nothing. B (0..10s) finishes; A (0..5s) is
    // live and behind, so nothing is cascade-finished. B's (5s,10s] stay held (not
    // released, not dropped) until A advances, then release in full.
    #[test]
    fn attack4_no_loss_when_peer_still_live() {
        let ms = tb(1, 1000);
        let mut q = SyncQueue::<Id>::new(10_000_000);
        let a = q.add_stream(true);
        let b = q.add_stream(true);

        // B sends 0..10s (ids 101..110) then finishes.
        for s in 1..=10 {
            push(&mut q, b, 100 + s, s * 1000, ms);
        }
        // A sends 0..5s (ids 1..5) and stays live.
        for s in 1..=5 {
            push(&mut q, a, s, s * 1000, ms);
        }
        // head = min(A 5s, B 10s) = A = 5s.
        finish(&mut q, b);
        assert!(
            !q.is_finished(),
            "A still live -> B's finish does not end the queue"
        );

        // B's <= 5s release; (5s,10s] are held (buffered, NOT dropped).
        assert_eq!(drain(&mut q, b), vec![101, 102, 103, 104, 105]);
        assert_eq!(
            q.streams[b].fifo.len(),
            5,
            "(5s,10s] still buffered, nothing lost"
        );

        // A advances to 10s -> head reaches 10s -> B's (5s,10s] become releasable.
        for s in 6..=10 {
            push(&mut q, a, s, s * 1000, ms);
        }
        assert_eq!(
            drain(&mut q, b),
            vec![106, 107, 108, 109, 110],
            "B's (5s,10s] released once A advances; nothing lost"
        );
    }

    // attack-4 truncation: A finishes at 7s (the shorter stream); B holds 7..10s.
    // After the queue finishes, B's <= 7s release and its (7s,10s] remain in the
    // fifo abandoned (never released), and is_stream_drained(B) is true.
    #[test]
    fn attack4_truncation_abandons_overbound() {
        let ms = tb(1, 1000);
        let mut q = SyncQueue::<Id>::new(10_000_000);
        let a = q.add_stream(true);
        let b = q.add_stream(true);

        for s in 1..=10 {
            push(&mut q, b, 100 + s, s * 1000, ms); // B 1..10s (ids 101..110)
        }
        for s in 1..=7 {
            push(&mut q, a, s, s * 1000, ms); // A 1..7s
        }

        // A finishes at 7s -> cascade finishes B (head 10s >= 7s bound).
        finish(&mut q, a);
        assert!(q.is_finished());
        assert!(
            q.is_stream_drained(b),
            "queue finished -> B drained even though (7s,10s] are still buffered"
        );

        // B releases <= 7s; (7s,10s] = 108,109,110 stay abandoned in the fifo.
        assert_eq!(drain(&mut q, b), vec![101, 102, 103, 104, 105, 106, 107]);
        assert!(
            drain(&mut q, b).is_empty(),
            "over-bound (7s,10s] never release"
        );
        assert_eq!(
            q.streams[b].fifo.len(),
            3,
            "over-bound (7s,10s] remain buffered"
        );
    }

    // frames_max: sq_limit_frames(idx, 1) makes the stream finish right after its
    // first frame, freezing the bound at frame-1's end; a 2nd send is dropped and
    // returns StreamFinished without corrupting state.
    #[test]
    fn frames_max_finishes_after_limit() {
        let ms = tb(1, 1000);
        let mut q = SyncQueue::<Id>::new(10_000_000);
        let a = q.add_stream(true);
        let _b = q.add_stream(true);

        q.sq_limit_frames(a, 1); // cap A at one frame
                                 // frames_sent was 0, so the cap does not finish it yet.
        assert!(!q.streams[a].finished);

        // 1st send: frames_sent hits 1 >= 1 -> finish A, bound frozen at 1000.
        assert_eq!(
            q.send(a, Some(Id(1)), Some(1000), ms, 0),
            SqSend::StreamFinished,
            "reaching frames_max finishes the stream"
        );
        assert!(q.streams[a].finished);
        assert_eq!(
            q.streams[a].head_ts,
            Some(1000),
            "bound frozen at frame-1 end"
        );
        assert_eq!(q.streams[a].fifo.len(), 1, "frame 1 enqueued");

        // 2nd send to the finished stream: dropped, StreamFinished, no corruption.
        assert_eq!(
            q.send(a, Some(Id(2)), Some(2000), ms, 0),
            SqSend::StreamFinished,
            "send to a finished stream is dropped"
        );
        assert_eq!(
            q.streams[a].head_ts,
            Some(1000),
            "bound unchanged by the drop"
        );
        assert_eq!(q.streams[a].fifo.len(), 1, "frame 2 was not enqueued");
    }

    // sq_limit_frames applied *after* the frames were already sent finishes the
    // stream immediately (mirrors sq_limit_frames :636-637).
    #[test]
    fn frames_max_retroactive_finish() {
        let ms = tb(1, 1000);
        let mut q = SyncQueue::<Id>::new(10_000_000);
        let a = q.add_stream(true);
        let _b = q.add_stream(true);

        push(&mut q, a, 1, 1000, ms);
        push(&mut q, a, 2, 2000, ms); // frames_sent == 2
        assert!(!q.streams[a].finished);

        q.sq_limit_frames(a, 2); // already at the cap -> finish now
        assert!(
            q.streams[a].finished,
            "frames_sent >= frames_max -> finished"
        );
    }

    // drain_all_releasable returns releasable items across every stream in
    // stream-index order (mirrors looping sq_receive(-1)).
    #[test]
    fn drain_all_releasable_order() {
        let ms = tb(1, 1000);
        let mut q = SyncQueue::<Id>::new(10_000_000);
        let a = q.add_stream(true);
        let b = q.add_stream(true);

        push(&mut q, a, 10, 1000, ms);
        push(&mut q, a, 11, 2000, ms); // A head 2000
        push(&mut q, b, 20, 1000, ms);
        push(&mut q, b, 21, 2000, ms); // B head 2000 -> queue head 2000

        // One scan drains A's <=2000 then B's <=2000, in that order.
        assert_eq!(drain_all(&mut q), vec![10, 11, 20, 21]);
    }

    // drain_all_releasable fires overflow_heartbeat on an EAGAIN scan, then
    // releases the tail that the heartbeat unblocked.
    #[test]
    fn drain_all_releasable_heartbeat_unblocks() {
        let ms = tb(1, 1000);
        let mut q = SyncQueue::<Id>::new(1_000_000); // 1s buf
        let a = q.add_stream(true);
        let b = q.add_stream(true);

        push(&mut q, a, 1, 1000, ms); // A tail 1s
        push(&mut q, a, 2, 3000, ms); // A head 3s (gap 2s > 1s buf)
        push(&mut q, b, 100, 500, ms); // B head 0.5s -> queue head 0.5s

        // Scan 1 releases B's 500 (<= head). A's 1000 is held. Scan 2 releases
        // nothing -> heartbeat advances B past 1s -> scan 3 releases A's 1000.
        // A's 3000 stays held; a final empty scan + no-op heartbeat stops the loop.
        assert_eq!(drain_all(&mut q), vec![100, 1]);
        assert_eq!(q.streams[a].fifo.len(), 1, "A's 3s item still buffered");
    }

    // newly_finished reports each transition exactly once, including a stream
    // finished by the secondary cascade, and never double-reports across calls.
    #[test]
    fn newly_finished_reports_each_once() {
        let ms = tb(1, 1000);
        let mut q = SyncQueue::<Id>::new(10_000_000);
        let a = q.add_stream(true);
        let b = q.add_stream(true);

        // Nothing finished yet.
        let mut nf = Vec::new();
        q.newly_finished(&mut nf);
        assert!(nf.is_empty());

        push(&mut q, a, 1, 3000, ms); // A head 3s
        for (i, end) in [1000, 2000, 3000, 4000, 5000].iter().enumerate() {
            push(&mut q, b, 100 + i as i64, *end, ms); // B head 5s
        }

        // Finish A -> A finishes itself, B finishes via the secondary cascade.
        finish(&mut q, a);

        let mut nf = Vec::new();
        q.newly_finished(&mut nf);
        assert_eq!(nf, vec![a, b], "self A then secondary-cascade B, each once");

        // The side-channel is drained -> a second call reports nothing.
        let mut nf2 = Vec::new();
        q.newly_finished(&mut nf2);
        assert!(nf2.is_empty(), "no double-report across calls");
    }

    // EZ divergence: a limiting stream that finishes without ever producing a
    // timestamp must NOT stall the queue. Once it is finished it stops gating head
    // establishment, so a peer's later frames still release.
    #[test]
    fn untimestamped_finished_limiter_does_not_stall() {
        let ms = tb(1, 1000);
        let mut q = SyncQueue::<Id>::new(10_000_000);
        let a = q.add_stream(true);
        let b = q.add_stream(true);

        // A finishes untimestamped FIRST (head_ts == None, stays finished).
        finish(&mut q, a);
        assert!(!q.is_finished(), "B still live");

        // B's sends now re-run queue_head_update; the finished untimestamped A is
        // skipped as a gate, so the head establishes on B and B's frames release.
        push(&mut q, b, 20, 1000, ms);
        push(&mut q, b, 21, 2000, ms);
        assert_eq!(
            drain(&mut q, b),
            vec![20, 21],
            "peer releases despite the finished untimestamped limiter"
        );
    }
}
