//! Port of the fftools scheduler's balancing pass (FFmpeg 7.x
//! fftools/ffmpeg_sched.c `schedule_update_locked` / `unchoke_for_stream`
//! / `trailing_dts` / `SCHEDULE_TOLERANCE`): chokes sources whose output
//! streams run too far ahead of the trailing stream. `SchNode` is a
//! reduced form of the graph fftools addresses through `SchedulerNode` —
//! just the demux/filter/mux-stream nodes the balancing pass needs;
//! `InputController` owns what fftools hangs off the `Scheduler` struct
//! itself. fftools 7.x chokes demuxers and filtergraph sources
//! (ffmpeg_sched.c:1286-1291); ez chokes only demuxers and lets bounded
//! channels pace decoders and filtergraphs.

use crate::core::scheduler::ffmpeg_scheduler::is_stopping;
use crate::util::sch_waiter::SchWaiter;
use ffmpeg_sys_next::AV_NOPTS_VALUE;
use std::sync::atomic::{AtomicBool, AtomicI64, AtomicUsize, Ordering};
use std::sync::{Arc, Mutex};

#[derive(Clone)]
pub(crate) enum SchNode {
    Demux {
        waiter: Arc<SchWaiter>,
        task_exited: Arc<AtomicBool>,
    },
    Filter {
        inputs: Vec<Arc<SchNode>>,
        best_input: Arc<AtomicUsize>,
    },
    MuxStream {
        src: Arc<SchNode>,
        last_dts: Arc<AtomicI64>,
        source_finished: Arc<AtomicBool>,
    },
}

const SCHEDULE_TOLERANCE: i64 = 100 * 1000;
pub(crate) struct InputController {
    lock: Mutex<()>,
    /// Whether balancing can ever change a choke decision. With a single
    /// demuxer there is nothing to balance against, so the whole pass is a
    /// no-op and `update_locked` can skip the lock + scan (PERF-6).
    balancing_possible: bool,
    demuxs: Vec<Arc<SchNode>>,
    mux_streams: Vec<Arc<SchNode>>,
}

impl InputController {
    pub(crate) fn new(demuxs: Vec<Arc<SchNode>>, mux_streams: Vec<Arc<SchNode>>) -> Self {
        assert!(
            demuxs
                .iter()
                .all(|node| matches!(**node, SchNode::Demux { .. })),
            "demuxs must contain only SchNode::Demux variants."
        );

        assert!(
            mux_streams
                .iter()
                .all(|node| matches!(**node, SchNode::MuxStream { .. })),
            "mux_streams must contain only SchNode::EncStream variants."
        );

        Self {
            lock: Mutex::new(()),
            balancing_possible: demuxs.len() > 1,
            demuxs,
            mux_streams,
        }
    }

    pub(crate) fn update_locked(&self, scheduler_status: &Arc<AtomicUsize>) {
        // Single-input jobs have nothing to balance: the lone demuxer is always
        // eventually unchoked (via the trailing-stream unchoke or the fallback),
        // and this pass can never newly set a choke. Skip the global lock and
        // the O(streams + demuxers) scan entirely (PERF-6). Multi-input jobs
        // keep the full, fftools-faithful path.
        if !self.balancing_possible {
            return;
        }

        let _guard = self.lock.lock().unwrap();
        if is_stopping(scheduler_status.load(Ordering::Acquire)) {
            return;
        }

        let mut have_unchoked = false;

        let dts = self.trailing_dts();

        // initialize our internal state
        self.demuxs.iter().for_each(|demux| {
            let node = demux.as_ref();
            let SchNode::Demux { waiter, .. } = node else {
                unreachable!()
            };
            waiter.set_choked_prev(waiter.get_choked());
            waiter.set_choked_next(true);
        });

        // figure out the sources that are allowed to proceed
        for mux_stream in self.mux_streams.iter() {
            let node = mux_stream.as_ref();
            let SchNode::MuxStream {
                src,
                last_dts,
                source_finished,
            } = node
            else {
                unreachable!()
            };

            // unblock sources for output streams that are not finished
            // and not too far ahead of the trailing stream
            if source_finished.load(Ordering::Acquire) {
                continue;
            }
            let last_dts = last_dts.load(Ordering::Acquire);
            if dts == AV_NOPTS_VALUE && last_dts != AV_NOPTS_VALUE {
                continue;
            }
            if dts != AV_NOPTS_VALUE && last_dts - dts >= SCHEDULE_TOLERANCE {
                continue;
            }

            // resolve the source to unchoke
            Self::unchoke_for_stream(src);
            have_unchoked = true;
        }

        // No stream steered a source this pass — every mux stream is either
        // finished or too far ahead. Guarantee progress by unchoking EVERY live
        // demuxer, not just one. FFmpeg unchokes a single fallback source because
        // its sync-queue EOF is forwarded up to stop a cascade-cut stream's
        // demuxer; ez does not forward that EOF for encoded streams, so a
        // cascade-cut member still draining needs its own demuxer to keep
        // advancing to the cut. Waking only one starves the rest and deadlocks a
        // `-shortest` job with 3+ encoded streams (a lagging peer's drain waits on
        // a demuxer this pass left choked). Over-unchoking is safe: the next
        // balancing pass re-chokes anything that runs ahead, and the pre-mux queue
        // still bounds memory.
        if !have_unchoked {
            for demux in self.demuxs.iter() {
                let node = demux.as_ref();
                let SchNode::Demux {
                    waiter,
                    task_exited,
                } = node
                else {
                    unreachable!()
                };
                if !task_exited.load(Ordering::Acquire) {
                    waiter.set_choked_next(false);
                }
            }
        }

        for demux in self.demuxs.iter() {
            let node = demux.as_ref();
            let SchNode::Demux { waiter, .. } = node else {
                unreachable!()
            };
            let choked_next = waiter.get_choked_next();
            if waiter.get_choked_prev() != choked_next {
                waiter.set(choked_next);
            }
        }
    }

    fn unchoke_for_stream(mut src: &Arc<SchNode>) {
        loop {
            let node = src.as_ref();
            // fed directly by a demuxer (i.e. not through a filtergraph)
            if let SchNode::Demux { waiter, .. } = node {
                waiter.set_choked_next(false);
                return;
            }

            assert!(matches!(node, SchNode::Filter { .. }));

            let SchNode::Filter { inputs, best_input } = node else {
                unreachable!()
            };

            src = &inputs[best_input.load(Ordering::Acquire)];
        }
    }

    fn trailing_dts(&self) -> i64 {
        let min_dts = self
            .mux_streams
            .iter()
            .filter_map(|mux_stream| {
                let node = mux_stream.as_ref();
                let SchNode::MuxStream {
                    src: _,
                    last_dts,
                    source_finished,
                } = node
                else {
                    unreachable!()
                };
                if source_finished.load(Ordering::Acquire) {
                    None
                } else {
                    let last_dts = last_dts.load(Ordering::Acquire);
                    if last_dts == AV_NOPTS_VALUE {
                        None
                    } else {
                        Some(last_dts)
                    }
                }
            })
            .min();

        match min_dts {
            Some(min_dts) => min_dts,
            None => AV_NOPTS_VALUE,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::core::scheduler::ffmpeg_scheduler::STATUS_RUN;

    fn demux_node() -> Arc<SchNode> {
        Arc::new(SchNode::Demux {
            waiter: Arc::new(SchWaiter::new()),
            task_exited: Arc::new(AtomicBool::new(false)),
        })
    }

    fn mux_stream(src: Arc<SchNode>, last_dts: i64) -> Arc<SchNode> {
        Arc::new(SchNode::MuxStream {
            src,
            last_dts: Arc::new(AtomicI64::new(last_dts)),
            source_finished: Arc::new(AtomicBool::new(false)),
        })
    }

    fn waiter_of(node: &Arc<SchNode>) -> Arc<SchWaiter> {
        match node.as_ref() {
            SchNode::Demux { waiter, .. } => waiter.clone(),
            _ => unreachable!("expected a demux node"),
        }
    }

    fn finish(mux_stream: &Arc<SchNode>) {
        match mux_stream.as_ref() {
            SchNode::MuxStream {
                source_finished, ..
            } => source_finished.store(true, Ordering::Release),
            _ => unreachable!("expected a mux stream node"),
        }
    }

    fn mark_exited(demux: &Arc<SchNode>) {
        match demux.as_ref() {
            SchNode::Demux { task_exited, .. } => task_exited.store(true, Ordering::Release),
            _ => unreachable!("expected a demux node"),
        }
    }

    // PERF-6: a single-input job cannot balance, so update_locked must be a
    // no-op and never choke the lone demuxer.
    #[test]
    fn single_input_update_is_a_noop_and_never_chokes() {
        let status = Arc::new(AtomicUsize::new(STATUS_RUN));
        let demux = demux_node();
        let mux = mux_stream(demux.clone(), 1_000);
        let ctrl = InputController::new(vec![demux.clone()], vec![mux]);
        assert!(!ctrl.balancing_possible, "a single demuxer cannot balance");
        ctrl.update_locked(&status);
        assert!(
            !waiter_of(&demux).get_choked(),
            "the lone demuxer must never be choked"
        );
    }

    // Regression guard: the early return must not affect multi-input jobs — the
    // full balancing pass still runs and chokes a source that is far ahead of
    // the trailing stream while keeping the trailing stream runnable.
    #[test]
    fn multi_input_runs_the_full_balancing_pass() {
        let status = Arc::new(AtomicUsize::new(STATUS_RUN));
        let trailing = demux_node();
        let ahead = demux_node();
        let m_trailing = mux_stream(trailing.clone(), 0);
        let m_ahead = mux_stream(ahead.clone(), 10 * SCHEDULE_TOLERANCE);
        let ctrl = InputController::new(
            vec![trailing.clone(), ahead.clone()],
            vec![m_trailing, m_ahead],
        );
        assert!(ctrl.balancing_possible);
        ctrl.update_locked(&status);
        assert!(
            !waiter_of(&trailing).get_choked(),
            "the trailing stream stays runnable"
        );
        assert!(
            waiter_of(&ahead).get_choked(),
            "a source far ahead of the trailing stream must be choked"
        );
    }

    // Scheduler-deadlock regression (multi-input trim+concat completing while a
    // late input is still choked). Once every output stream has finished, the
    // balancing fallback MUST release EVERY still-choked demuxer in one pass.
    // 0.11.0 unchoked a single demuxer then `break`, stranding the rest: the
    // choked demuxers were the only non-exited workers, so STATUS_END (published
    // only when the worker count hits zero) never fired and
    // `FfmpegScheduler::wait()` hung forever. The choked demuxer's ONLY other
    // exit edge is being unchoked — this pass is that edge.
    #[test]
    fn all_sources_finished_unchokes_every_live_demuxer() {
        let status = Arc::new(AtomicUsize::new(STATUS_RUN));
        let d0 = demux_node();
        let d1 = demux_node();
        let d2 = demux_node();
        let gone = demux_node();
        for d in [&d0, &d1, &d2, &gone] {
            waiter_of(d).set(true); // all choked mid-run
        }
        // `gone` has already exited: the fallback must skip it (never notify a
        // dead worker) and leave its state untouched.
        mark_exited(&gone);

        // One output stream, already finished — the concat/muxer hit EOF.
        let m = mux_stream(d0.clone(), 1_000);
        finish(&m);

        let ctrl = InputController::new(
            vec![d0.clone(), d1.clone(), d2.clone(), gone.clone()],
            vec![m],
        );
        ctrl.update_locked(&status);

        for d in [&d0, &d1, &d2] {
            assert!(
                !waiter_of(d).get_choked(),
                "every LIVE demuxer must be unchoked once all sources finished"
            );
        }
        assert!(
            waiter_of(&gone).get_choked(),
            "an already-exited demuxer must be left untouched"
        );
    }

    // Liveness form of the same regression at the SchWaiter boundary: a demuxer
    // actually parked in `wait_with_scheduler_status` (choked, undelivered tail
    // packets) while the scheduler is still RUNNING (no STATUS_END, because it is
    // itself a non-exited worker) must be released by the muxer's
    // last-stream-finished `update_locked` alone — via the unchoke edge, without
    // any terminal status flip.
    #[test]
    fn parked_choked_demuxer_released_when_all_sources_finish() {
        use std::sync::mpsc;
        use std::thread;
        use std::time::Duration;

        let status = Arc::new(AtomicUsize::new(STATUS_RUN));
        let parked = demux_node();
        let peer = demux_node(); // second input so balancing runs (not PERF-6 short-circuited)
        waiter_of(&parked).set(true); // choked with work still to deliver

        let (tx, rx) = mpsc::channel();
        let w = waiter_of(&parked);
        let st = Arc::clone(&status);
        thread::spawn(move || {
            w.wait_with_scheduler_status(&st, false);
            let _ = tx.send(());
        });

        // Still parked while the job runs and the output has not finished.
        thread::sleep(Duration::from_millis(150));
        assert!(
            rx.try_recv().is_err(),
            "the demuxer must stay parked while the scheduler runs"
        );

        // Muxer's last stream hits EOF -> source_finished -> update_locked.
        let m = mux_stream(peer.clone(), 0);
        finish(&m);
        let ctrl = InputController::new(vec![parked.clone(), peer.clone()], vec![m]);
        ctrl.update_locked(&status);

        rx.recv_timeout(Duration::from_secs(2)).expect(
            "a choked demuxer must be released once all sources finished (no STATUS_END needed)",
        );
    }
}
