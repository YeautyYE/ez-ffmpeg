//! Pre-mux packet queue (PERF-12).
//!
//! fftools: `PreMuxQueue` (ffmpeg_sched.c) — packets an encoder produces
//! before its muxer starts (the muxer waits until every output stream is
//! ready) are parked per-stream, metered with FFmpeg's
//! `max_muxing_queue_size` / `muxing_queue_data_threshold` semantics:
//! below the byte threshold the packet cap does not apply
//! (ffmpeg_sched.h: `data_threshold` is "the size of the data past which
//! `max_packets` applies"). Many tiny packets (sparse subtitles) therefore
//! never wedge admission on the packet cap, while large packets are
//! bounded by bytes instead of the old blind 65536-packet window that
//! could park gigabytes.
//!
//! A full queue parks the sender on this queue's condvar — no sleep-poll.
//! Wakes: the mux-start drain (`drain_all`), receiver drop (stop /
//! mux-init failure), or the caller's bounded wait timeout as a
//! lost-notify safety net. Senders never wait while holding the
//! `MuxStartGate` lock; admission itself (`try_push`) runs under that lock
//! via `MuxStartGate::send_pre`, preserving the atomic drain+flip that
//! makes parked packets impossible to lose.

use std::collections::VecDeque;
use std::sync::{Arc, Condvar, Mutex, MutexGuard, PoisonError};
use std::time::Duration;

use ffmpeg_next::packet::Ref;

use super::PacketBox;

/// FFmpeg CLI parity default for `-max_muxing_queue_size`.
pub(crate) const DEFAULT_PRE_MUX_MAX_PACKETS: usize = 128;
/// FFmpeg CLI parity default for `-muxing_queue_data_threshold` (50 MiB).
pub(crate) const DEFAULT_PRE_MUX_DATA_THRESHOLD: usize = 50 * 1024 * 1024;

/// Admission config, FFmpeg names: `max_packets` = `max_muxing_queue_size`,
/// `data_threshold` = `muxing_queue_data_threshold`.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) struct PreMuxQueueConfig {
    pub(crate) max_packets: usize,
    pub(crate) data_threshold: usize,
}

impl Default for PreMuxQueueConfig {
    fn default() -> Self {
        Self {
            max_packets: DEFAULT_PRE_MUX_MAX_PACKETS,
            data_threshold: DEFAULT_PRE_MUX_DATA_THRESHOLD,
        }
    }
}

struct PreMuxQueueState {
    queue: VecDeque<PacketBox>,
    /// Sum of parked packets' payload bytes (`pkt->size`), mirroring
    /// FFmpeg's `data_size` accounting.
    byte_size: usize,
    /// Receiver gone: the muxer never started (init failure / stop) or
    /// already drained and exited. Parked senders must resolve
    /// Disconnected instead of waiting forever.
    closed: bool,
}

struct PreMuxQueueInner {
    state: Mutex<PreMuxQueueState>,
    cv: Condvar,
    config: PreMuxQueueConfig,
}

/// Encoder-side handle. Admission is non-blocking (`try_push`, called under
/// the `MuxStartGate` lock); parking happens in `wait_for_space`, outside
/// any other lock.
#[derive(Clone)]
pub(crate) struct PreMuxQueueSender {
    inner: Arc<PreMuxQueueInner>,
}

/// Muxer-side handle; dropping it closes the queue and wakes parked senders.
pub(crate) struct PreMuxQueueReceiver {
    inner: Arc<PreMuxQueueInner>,
}

pub(crate) enum PreQueueTryPush {
    Sent,
    Full(PacketBox),
    Disconnected(PacketBox),
}

pub(crate) fn channel(config: PreMuxQueueConfig) -> (PreMuxQueueSender, PreMuxQueueReceiver) {
    let inner = Arc::new(PreMuxQueueInner {
        state: Mutex::new(PreMuxQueueState {
            queue: VecDeque::new(),
            byte_size: 0,
            closed: false,
        }),
        cv: Condvar::new(),
        config,
    });
    (
        PreMuxQueueSender {
            inner: inner.clone(),
        },
        PreMuxQueueReceiver { inner },
    )
}

/// Payload bytes the packet contributes to `data_threshold` accounting
/// (`pkt->size`, like FFmpeg). Empty end-of-stream markers weigh 0 — and
/// since a 0-byte add never crosses the threshold, a stream's EOF marker
/// can never be wedged out by a full queue.
pub(crate) fn packet_payload_size(packet_box: &PacketBox) -> usize {
    // SAFETY: the box owns a live packet (possibly an allocated-but-empty
    // EOF marker); reading its size field is valid.
    unsafe {
        let pkt = packet_box.packet.as_ptr();
        if pkt.is_null() || (*pkt).size <= 0 {
            0
        } else {
            (*pkt).size as usize
        }
    }
}

/// FFmpeg parity (ffmpeg_sched.c): full only when BOTH the packet cap is
/// reached AND admitting would keep the queue past the byte threshold.
fn has_space_for(state: &PreMuxQueueState, config: PreMuxQueueConfig, size: usize) -> bool {
    state.queue.len() < config.max_packets
        || state.byte_size.saturating_add(size) <= config.data_threshold
}

fn lock_state(inner: &PreMuxQueueInner) -> MutexGuard<'_, PreMuxQueueState> {
    // A sender panicking mid-push cannot corrupt this state (mutations are
    // plain assignments), so recover from poisoning instead of cascading
    // panics through every worker — same stance as ffmpeg_scheduler.
    inner.state.lock().unwrap_or_else(PoisonError::into_inner)
}

impl PreMuxQueueSender {
    /// Non-blocking admission; called under the `MuxStartGate` lock, so it
    /// must never wait.
    pub(crate) fn try_push(&self, packet_box: PacketBox) -> PreQueueTryPush {
        let size = packet_payload_size(&packet_box);
        let mut state = lock_state(&self.inner);
        if state.closed {
            return PreQueueTryPush::Disconnected(packet_box);
        }
        if !has_space_for(&state, self.inner.config, size) {
            return PreQueueTryPush::Full(packet_box);
        }
        state.byte_size = state.byte_size.saturating_add(size);
        state.queue.push_back(packet_box);
        PreQueueTryPush::Sent
    }

    /// Park until space may exist for `packet_size`, the queue closes, or
    /// `timeout` elapses. Advisory only: the caller re-runs the gated
    /// `MuxStartGate::send_pre`, which is the authoritative admission and
    /// gate-started check (so a drain+flip between this wake and that call
    /// is handled, and spurious wakeups are harmless). Must not be called
    /// with the `MuxStartGate` lock held — the drain could never run.
    pub(crate) fn wait_for_space(&self, packet_size: usize, timeout: Duration) {
        let state = lock_state(&self.inner);
        if state.closed || has_space_for(&state, self.inner.config, packet_size) {
            return;
        }
        let _ = self
            .inner
            .cv
            .wait_timeout(state, timeout)
            .unwrap_or_else(PoisonError::into_inner);
    }
}

impl PreMuxQueueReceiver {
    /// Takes every parked packet in FIFO order and resets byte accounting.
    /// Runs inside `MuxStartGate::start_with` (gate lock held): senders are
    /// locked out of admission for the whole drain, so the snapshot is
    /// atomic with the gate flip and no packet can park after it.
    pub(crate) fn drain_all(&self) -> VecDeque<PacketBox> {
        let mut state = lock_state(&self.inner);
        state.byte_size = 0;
        let drained = std::mem::take(&mut state.queue);
        drop(state);
        // Parked senders re-run send_pre; it resolves Started once the gate
        // flips (they contend on the gate lock until start_with returns).
        self.inner.cv.notify_all();
        drained
    }
}

impl Drop for PreMuxQueueReceiver {
    fn drop(&mut self) {
        let mut state = lock_state(&self.inner);
        state.closed = true;
        drop(state);
        self.inner.cv.notify_all();
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::core::context::{MuxStartGate, PacketData, PreSendOutcome};
    use ffmpeg_sys_next::AVMediaType::AVMEDIA_TYPE_VIDEO;
    use std::sync::mpsc;
    use std::thread;

    fn packet_box(payload: usize) -> PacketBox {
        let packet = if payload == 0 {
            ffmpeg_next::Packet::empty()
        } else {
            ffmpeg_next::Packet::new(payload)
        };
        PacketBox {
            packet,
            packet_data: PacketData {
                dts_est: 0,
                codec_type: AVMEDIA_TYPE_VIDEO,
                output_stream_index: 0,
                is_copy: false,
            },
        }
    }

    fn cfg(max_packets: usize, data_threshold: usize) -> PreMuxQueueConfig {
        PreMuxQueueConfig {
            max_packets,
            data_threshold,
        }
    }

    #[test]
    fn byte_metering_and_threshold_semantics() {
        let (tx, rx) = channel(cfg(2, 10));
        assert!(matches!(tx.try_push(packet_box(6)), PreQueueTryPush::Sent));
        assert!(matches!(tx.try_push(packet_box(4)), PreQueueTryPush::Sent));
        // Packet cap reached AND the next byte crosses the threshold: full.
        assert!(matches!(
            tx.try_push(packet_box(1)),
            PreQueueTryPush::Full(_)
        ));
        // A 0-byte EOF marker never crosses the threshold: always admitted.
        assert!(matches!(tx.try_push(packet_box(0)), PreQueueTryPush::Sent));

        let drained = rx.drain_all();
        let sizes: Vec<usize> = drained.iter().map(packet_payload_size).collect();
        assert_eq!(sizes, vec![6, 4, 0], "FIFO order must be preserved");

        // Accounting reset by the drain: the push that was Full now parks.
        assert!(matches!(tx.try_push(packet_box(1)), PreQueueTryPush::Sent));
    }

    #[test]
    fn packet_cap_does_not_apply_below_byte_threshold() {
        // FFmpeg semantics: data_threshold is the size past which
        // max_packets applies — many small packets must keep parking.
        let (tx, _rx) = channel(cfg(1, 10));
        assert!(matches!(tx.try_push(packet_box(2)), PreQueueTryPush::Sent));
        assert!(matches!(tx.try_push(packet_box(2)), PreQueueTryPush::Sent));
        assert!(matches!(tx.try_push(packet_box(2)), PreQueueTryPush::Sent));
        // Crossing the threshold with the cap already reached: full.
        assert!(matches!(
            tx.try_push(packet_box(9)),
            PreQueueTryPush::Full(_)
        ));
    }

    #[test]
    fn full_sender_parks_and_drain_wakes_it() {
        let (tx, rx) = channel(cfg(1, 4));
        assert!(matches!(tx.try_push(packet_box(4)), PreQueueTryPush::Sent));

        let (done_tx, done_rx) = mpsc::channel();
        thread::spawn(move || {
            let mut pb = packet_box(2);
            loop {
                match tx.try_push(pb) {
                    PreQueueTryPush::Sent => break,
                    PreQueueTryPush::Full(p) => {
                        tx.wait_for_space(2, Duration::from_secs(5));
                        pb = p;
                    }
                    PreQueueTryPush::Disconnected(_) => panic!("unexpected disconnect"),
                }
            }
            let _ = done_tx.send(());
        });

        assert!(
            done_rx.recv_timeout(Duration::from_millis(100)).is_err(),
            "sender must park while the queue is full"
        );
        let drained = rx.drain_all();
        assert_eq!(drained.len(), 1);
        assert!(
            done_rx.recv_timeout(Duration::from_secs(5)).is_ok(),
            "drain must wake the parked sender"
        );
    }

    #[test]
    fn receiver_drop_unparks_disconnected() {
        let (tx, rx) = channel(cfg(1, 4));
        assert!(matches!(tx.try_push(packet_box(4)), PreQueueTryPush::Sent));

        let (done_tx, done_rx) = mpsc::channel();
        thread::spawn(move || {
            let mut pb = packet_box(2);
            loop {
                match tx.try_push(pb) {
                    PreQueueTryPush::Sent => panic!("queue should stay full"),
                    PreQueueTryPush::Full(p) => {
                        tx.wait_for_space(2, Duration::from_secs(5));
                        pb = p;
                    }
                    PreQueueTryPush::Disconnected(_) => break,
                }
            }
            let _ = done_tx.send(());
        });

        thread::sleep(Duration::from_millis(50));
        drop(rx);
        assert!(
            done_rx.recv_timeout(Duration::from_secs(5)).is_ok(),
            "receiver drop must unpark the sender with Disconnected"
        );
    }

    #[test]
    fn gate_start_resolves_started_for_parked_sender() {
        let gate = Arc::new(MuxStartGate::new());
        let (tx, rx) = channel(cfg(1, 4));
        assert!(matches!(
            gate.send_pre(&tx, packet_box(4)),
            PreSendOutcome::Sent
        ));

        let (done_tx, done_rx) = mpsc::channel();
        let gate_clone = gate.clone();
        thread::spawn(move || {
            let mut pb = packet_box(2);
            let outcome = loop {
                match gate_clone.send_pre(&tx, pb) {
                    PreSendOutcome::Sent => break "sent",
                    PreSendOutcome::Started(_) => break "started",
                    PreSendOutcome::Disconnected(_) => break "disconnected",
                    PreSendOutcome::Full(p) => {
                        tx.wait_for_space(2, Duration::from_secs(5));
                        pb = p;
                    }
                }
            };
            let _ = done_tx.send(outcome);
        });

        assert!(
            done_rx.recv_timeout(Duration::from_millis(100)).is_err(),
            "sender must park while the gate is closed and the queue full"
        );

        let mut drained_len = 0;
        gate.start_with(|| {
            drained_len = rx.drain_all().len();
        });
        assert_eq!(drained_len, 1);
        assert_eq!(
            done_rx.recv_timeout(Duration::from_secs(5)).unwrap(),
            "started",
            "a parked sender must observe the opened gate and divert to the live queue"
        );
    }
}
