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
        /// How many graph input pads fed by this demuxer have already primed
        /// (seen their first decoded frame) while their graph still awaits
        /// other pads. Every frame this demuxer sends such a pad can only
        /// PARK in that graph's bounded pre-config queue (runtime.rs), so
        /// while this is non-zero the pre-configuration fan-out stops
        /// volunteering this demuxer and defers it to the `best_input` walk —
        /// the serial semantics, which kept those queues shallow. Each
        /// graph's filter thread is the writer (loop-top publish scan in
        /// filter_task: +1 when a pad primes into a still-cold graph, -1 for
        /// each counted pad once the graph has every format and configures).
        parked_risk: Arc<AtomicUsize>,
    },
    Filter {
        /// One slot per filter pad, pad-indexed. A pad fed by a demuxer holds
        /// `Some(demux_node)`; a cross-graph-bound pad (fed by another graph's
        /// output) stays `None` — an explicit hole, so demuxer-bound entries
        /// keep their pad index instead of being shifted.
        inputs: Vec<Option<Arc<SchNode>>>,
        best_input: Arc<AtomicUsize>,
        /// Pad-indexed pre-configuration flags: `true` until the pad has seen
        /// the frame that fixes its format (`ifp.format >= 0`) or went EOF.
        /// The graph cannot be configured while any pad still awaits a frame
        /// (filter_task/runtime.rs), and the filter thread is the only writer,
        /// flipping each flag false exactly once. While any flag is set,
        /// `unchoke_for_stream` fans out to EVERY flagged pad's demuxer
        /// instead of following `best_input`, so N cold inputs prime in
        /// parallel — time to the first output packet is max(cold costs), not
        /// their sum. fftools rotates its pre-config `best_input` through the
        /// missing pads one at a time (ffmpeg_filter.c fg_read_frames),
        /// serializing those cold reads; the CLI never notices on local
        /// files, but seek+read on a slow/remote source costs seconds per
        /// input and this crate's windowed multi-input jobs pay it per input.
        awaiting_format: Arc<[AtomicBool]>,
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

    /// Whether the balancing pass can ever change a choke decision (more
    /// than one demuxer). Exposed so per-packet publishers can skip
    /// producing values only that pass reads (PERF-6 companion): when this
    /// is `false`, `update_locked` returns before touching any `last_dts`,
    /// so nothing ever observes the skipped stores.
    pub(crate) fn balancing_possible(&self) -> bool {
        self.balancing_possible
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
        // Set when any eligible stream resolves to NO demuxer (an empty/OOB
        // scheduler input list). Even if another stream unchoked a demuxer, the
        // unresolved one's demuxer would stay choked, so the fallback must still
        // run — `have_unchoked` alone would wrongly suppress it and hang the job.
        let mut resolution_failed = false;

        let dts = self.trailing_dts();

        // initialize our internal state
        self.demuxs.iter().for_each(|demux| {
            let node = demux.as_ref();
            let SchNode::Demux { waiter, .. } = node else {
                unreachable!("new() asserts every demuxs entry is SchNode::Demux")
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
                unreachable!("new() asserts every mux_streams entry is SchNode::MuxStream")
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

            // resolve the source to unchoke; only count it as progress if a
            // demuxer was actually reached, so the all-live-demuxer fallback below
            // still runs when a stream resolves to no demuxer.
            if Self::unchoke_for_stream(src) {
                have_unchoked = true;
            } else {
                resolution_failed = true;
            }
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
        if !have_unchoked || resolution_failed {
            for demux in self.demuxs.iter() {
                let node = demux.as_ref();
                let SchNode::Demux {
                    waiter,
                    task_exited,
                    ..
                } = node
                else {
                    unreachable!("new() asserts every demuxs entry is SchNode::Demux")
                };
                if !task_exited.load(Ordering::Acquire) {
                    waiter.set_choked_next(false);
                }
            }
        }

        for demux in self.demuxs.iter() {
            let node = demux.as_ref();
            let SchNode::Demux { waiter, .. } = node else {
                unreachable!("new() asserts every demuxs entry is SchNode::Demux")
            };
            let choked_next = waiter.get_choked_next();
            if waiter.get_choked_prev() != choked_next {
                waiter.set(choked_next);
            }
        }
    }

    /// Walks up from a mux stream to the demuxer(s) feeding its selected input
    /// and unchokes them. Returns whether the stream's demand was FULLY
    /// resolved to demuxers: a stray empty/out-of-range scheduler input list, a
    /// cross-graph hole, or an awaiting pad that dead-ends in one (a zero-input
    /// graph is rejected at build, but a short/unbound cross-graph list could
    /// still occur) leaves some needed source unreached, so the caller must NOT
    /// count it as progress — otherwise the all-live-demuxer fallback is
    /// skipped and a multi-demuxer job hangs. A partially-resolved fan-out may
    /// have unchoked demuxers AND return `false`: the unchokes stand, and the
    /// fallback additionally releases whatever could not be walked to.
    fn unchoke_for_stream(mut src: &Arc<SchNode>) -> bool {
        loop {
            let node = src.as_ref();
            // fed directly by a demuxer (i.e. not through a filtergraph)
            if let SchNode::Demux { waiter, .. } = node {
                waiter.set_choked_next(false);
                return true;
            }

            assert!(matches!(node, SchNode::Filter { .. }));

            let SchNode::Filter {
                inputs,
                best_input,
                awaiting_format,
            } = node
            else {
                unreachable!("node matched SchNode::Filter in the assert just above")
            };

            // Pre-configuration fan-out: while any pad still awaits its first
            // frame the graph cannot be configured, so following only
            // `best_input` would prime the inputs one at a time and the first
            // output packet would wait for the SUM of every input's cold
            // seek+read cost. Unchoke every awaiting pad's source instead; a
            // pad that primes drops out of the set, so the next pass re-chokes
            // its demuxer (bounding the pre-config frame queues to pipeline
            // drain) while the stragglers keep reading. An awaiting pad that
            // resolves to no demuxer — a cross-graph hole — reports failure so
            // the caller still runs the all-live-demuxer fallback, the only
            // edge that reaches the upstream graph's demuxer.
            //
            // One class of source is NOT volunteered: a demuxer already
            // feeding a primed pad of a still-unconfigured graph (its
            // `parked_risk`, maintained by the filter threads). Demuxers read
            // ALL their streams in file order, so unchoking one for pad A
            // also floods every other pad it feeds — and a primed pad of a
            // cold graph can only PARK those frames in its bounded pre-config
            // queue until the caps fail the job. The serial walk kept such
            // queues shallow by reaching a demuxer only through `best_input`
            // rotation, so a risky demuxer is deferred to exactly that: the
            // selected pad is still walked unconditionally, which keeps
            // liveness (and worst-case queue depth) identical to the
            // pre-fan-out semantics.
            let mut awaiting_any = false;
            let mut resolved = true;
            let selected = best_input.load(Ordering::Acquire);
            for (pad, awaiting) in awaiting_format.iter().enumerate() {
                if !awaiting.load(Ordering::Acquire) {
                    continue;
                }
                awaiting_any = true;
                match inputs.get(pad) {
                    Some(Some(next)) => {
                        if pad == selected || !Self::has_parked_risk(next) {
                            resolved &= Self::unchoke_for_stream(next);
                        }
                    }
                    _ => resolved = false,
                }
            }
            if awaiting_any {
                return resolved;
            }

            // No upstream to walk to — out of range, or a cross-graph hole
            // (`Some(None)`): reached no demuxer.
            let Some(Some(next)) = inputs.get(best_input.load(Ordering::Acquire)) else {
                return false;
            };
            src = next;
        }
    }

    /// Whether this pad source is a demuxer currently feeding at least one
    /// primed pad of a still-unconfigured graph (`SchNode::Demux::parked_risk`
    /// — frames sent to such a pad park in its pre-config queue). Anything
    /// other than a demuxer carries no such state and is never deferred.
    fn has_parked_risk(node: &Arc<SchNode>) -> bool {
        matches!(
            node.as_ref(),
            SchNode::Demux { parked_risk, .. } if parked_risk.load(Ordering::Acquire) > 0
        )
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
                    unreachable!("new() asserts every mux_streams entry is SchNode::MuxStream")
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
            parked_risk: Arc::new(AtomicUsize::new(0)),
        })
    }

    /// Marks `node` as feeding `n` primed pads of still-unconfigured graphs —
    /// what the filter thread's publish scan does when a pad primes into a
    /// graph that is still cold (filter_task).
    fn set_parked_risk(node: &Arc<SchNode>, n: usize) {
        match node.as_ref() {
            SchNode::Demux { parked_risk, .. } => parked_risk.store(n, Ordering::Release),
            _ => unreachable!("expected a demux node"),
        }
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

    /// A CONFIGURED graph node: every pad already primed, so the walk follows
    /// `best_input` — the mode all the pre-existing tests exercise.
    fn filter_node(inputs: Vec<Option<Arc<SchNode>>>, best_input: usize) -> Arc<SchNode> {
        let pad_count = inputs.len();
        filter_node_awaiting(inputs, best_input, &vec![false; pad_count])
    }

    /// A graph node with explicit per-pad awaiting flags (`true` = the pad has
    /// not yet seen its first frame, i.e. the graph is still pre-config).
    fn filter_node_awaiting(
        inputs: Vec<Option<Arc<SchNode>>>,
        best_input: usize,
        awaiting: &[bool],
    ) -> Arc<SchNode> {
        Arc::new(SchNode::Filter {
            inputs,
            best_input: Arc::new(AtomicUsize::new(best_input)),
            awaiting_format: awaiting.iter().map(|&a| AtomicBool::new(a)).collect(),
        })
    }

    // unchoke_for_stream must report whether it actually reached a demuxer. An
    // empty/out-of-range scheduler input list unchokes nothing (and used to PANIC
    // on the index); the caller relies on the `false` return to still run the
    // all-live-demuxer fallback, otherwise a multi-demuxer job hangs.
    #[test]
    fn unchoke_for_stream_reports_whether_a_demuxer_was_reached() {
        // Direct demuxer: reached and unchoked.
        let d = demux_node();
        waiter_of(&d).set_choked_next(true);
        assert!(InputController::unchoke_for_stream(&d));
        assert!(
            !waiter_of(&d).get_choked_next(),
            "the reached demuxer must be unchoked"
        );

        // Filter -> demuxer: reached through the graph.
        let d2 = demux_node();
        waiter_of(&d2).set_choked_next(true);
        let f = filter_node(vec![Some(d2.clone())], 0);
        assert!(InputController::unchoke_for_stream(&f));
        assert!(!waiter_of(&d2).get_choked_next());

        // Empty scheduler inputs: no demuxer reached (would have panicked before).
        assert!(!InputController::unchoke_for_stream(&filter_node(
            vec![],
            0
        )));

        // Out-of-range best_input: no demuxer reached.
        let d3 = demux_node();
        assert!(!InputController::unchoke_for_stream(&filter_node(
            vec![Some(d3)],
            5
        )));
    }

    // A cross-graph-bound pad is a `None` hole in the scheduler-input list.
    // Selecting a hole reaches no demuxer (like an empty/out-of-range list), and
    // a hole before a demuxer-bound pad must NOT shift that pad's index.
    #[test]
    fn unchoke_for_stream_treats_a_cross_graph_hole_as_no_demuxer() {
        // best_input points at a hole -> no demuxer reached.
        assert!(!InputController::unchoke_for_stream(&filter_node(
            vec![None],
            0
        )));

        // Hole at pad 0, demuxer at pad 1: selecting pad 1 still reaches the
        // demuxer (pad indices preserved, not collapsed).
        let d = demux_node();
        waiter_of(&d).set_choked_next(true);
        let f = filter_node(vec![None, Some(d.clone())], 1);
        assert!(InputController::unchoke_for_stream(&f));
        assert!(!waiter_of(&d).get_choked_next());
    }

    // Multi-input cold-start regression: while a graph awaits configuration,
    // ONE balancing pass must unchoke EVERY pad still missing its first frame —
    // not just the single `best_input` pad. Serial priming made the first
    // output packet wait for the SUM of each input's cold seek+read cost
    // (seconds per input on SMB/busy disks); the fan-out makes it the max.
    #[test]
    fn awaiting_pads_unchoke_in_parallel() {
        let status = Arc::new(AtomicUsize::new(STATUS_RUN));
        let d0 = demux_node();
        let d1 = demux_node();
        let d2 = demux_node();
        // Pre-config: best_input still points at pad 0, all pads awaiting.
        let f = filter_node_awaiting(
            vec![Some(d0.clone()), Some(d1.clone()), Some(d2.clone())],
            0,
            &[true, true, true],
        );
        // No packets yet: the stream's last_dts is unset, so it is eligible.
        let m = mux_stream(f, AV_NOPTS_VALUE);
        let ctrl = InputController::new(vec![d0.clone(), d1.clone(), d2.clone()], vec![m]);
        ctrl.update_locked(&status);

        for (i, d) in [&d0, &d1, &d2].iter().enumerate() {
            assert!(
                !waiter_of(d).get_choked(),
                "demuxer {i} must be unchoked while its pad still awaits a frame"
            );
        }
    }

    // The flip side of the fan-out: a pad that HAS primed drops out of the
    // awaiting set, so its demuxer is re-choked while the stragglers keep
    // reading — this is what bounds the pre-config frame queues to pipeline
    // drain instead of letting a fast input decode unchecked for the seconds a
    // slow peer needs (the per-pad frame/byte caps would fail the job).
    #[test]
    fn primed_pad_rechokes_while_peers_still_prime() {
        let status = Arc::new(AtomicUsize::new(STATUS_RUN));
        let primed = demux_node();
        let cold = demux_node();
        let f = filter_node_awaiting(
            vec![Some(primed.clone()), Some(cold.clone())],
            // best_input already rotated to the cold pad, as fg_read_frames
            // does once pad 0 has a format.
            1,
            &[false, true],
        );
        let m = mux_stream(f, AV_NOPTS_VALUE);
        let ctrl = InputController::new(vec![primed.clone(), cold.clone()], vec![m]);
        ctrl.update_locked(&status);

        assert!(
            waiter_of(&primed).get_choked(),
            "a primed pad's demuxer must be re-choked while peers still prime"
        );
        assert!(
            !waiter_of(&cold).get_choked(),
            "an awaiting pad's demuxer must keep reading"
        );
    }

    // An awaiting pad that is a cross-graph hole resolves to no demuxer from
    // this graph. The walk may have unchoked its OTHER awaiting pads, but it
    // must still report failure so update_locked runs the all-live-demuxer
    // fallback — the only edge that reaches the upstream graph's demuxer.
    #[test]
    fn awaiting_cross_graph_hole_still_runs_the_fallback() {
        let status = Arc::new(AtomicUsize::new(STATUS_RUN));
        let direct = demux_node();
        let upstream = demux_node(); // feeds the hole pad through another graph
        let f = filter_node_awaiting(vec![None, Some(direct.clone())], 0, &[true, true]);
        let m = mux_stream(f, AV_NOPTS_VALUE);
        let ctrl = InputController::new(vec![direct.clone(), upstream.clone()], vec![m]);
        ctrl.update_locked(&status);

        assert!(
            !waiter_of(&direct).get_choked(),
            "the walkable awaiting pad is unchoked by the fan-out"
        );
        assert!(
            !waiter_of(&upstream).get_choked(),
            "the hole pad's unreachable source must be released by the fallback"
        );
    }

    // Once every pad is primed the fan-out is over: the walk follows
    // `best_input` again and a non-selected pad's demuxer stays choked. Locks
    // the mode transition back to the fftools balancing semantics.
    #[test]
    fn all_pads_primed_follows_best_input_again() {
        let status = Arc::new(AtomicUsize::new(STATUS_RUN));
        let selected = demux_node();
        let idle = demux_node();
        let f = filter_node_awaiting(
            vec![Some(selected.clone()), Some(idle.clone())],
            0,
            &[false, false],
        );
        let m = mux_stream(f, 1_000);
        let ctrl = InputController::new(vec![selected.clone(), idle.clone()], vec![m]);
        ctrl.update_locked(&status);

        assert!(
            !waiter_of(&selected).get_choked(),
            "the best_input pad's demuxer is unchoked"
        );
        assert!(
            waiter_of(&idle).get_choked(),
            "a non-selected pad's demuxer is choked once the graph is configured"
        );
    }

    // A demuxer read for one graph floods EVERY pad it feeds. If one of those
    // pads has already primed while its graph is still cold, each flooded
    // frame parks in that pad's bounded pre-config queue — so the fan-out
    // must NOT volunteer such a demuxer for a different awaiting pad; it
    // stays deferred to `best_input` rotation exactly like the serial walk,
    // which kept those queues shallow.
    #[test]
    fn parked_risk_defers_a_shared_demuxer_from_the_fanout() {
        let status = Arc::new(AtomicUsize::new(STATUS_RUN));
        let cold = demux_node();
        let shared = demux_node();
        // Another still-cold graph already primed a pad fed by `shared`.
        set_parked_risk(&shared, 1);
        // This graph awaits both pads; best_input points at the cold pad.
        let f = filter_node_awaiting(
            vec![Some(cold.clone()), Some(shared.clone())],
            0,
            &[true, true],
        );
        let m = mux_stream(f, AV_NOPTS_VALUE);
        let ctrl = InputController::new(vec![cold.clone(), shared.clone()], vec![m]);
        ctrl.update_locked(&status);

        assert!(
            !waiter_of(&cold).get_choked(),
            "the risk-free awaiting pad's demuxer is unchoked by the fan-out"
        );
        assert!(
            waiter_of(&shared).get_choked(),
            "a demuxer feeding a primed pad of a cold graph must not be volunteered"
        );
    }

    // The serial-parity floor: the pad `best_input` selects is walked
    // UNCONDITIONALLY, parked risk or not — the pre-fan-out semantics reached
    // the demuxer the same way, and skipping it would strand the pad (its
    // priming REQUIRES reading that demuxer, parked frames and all).
    #[test]
    fn parked_risk_demuxer_is_still_walked_as_the_selected_pad() {
        let status = Arc::new(AtomicUsize::new(STATUS_RUN));
        let other = demux_node();
        let shared = demux_node();
        set_parked_risk(&shared, 1);
        // best_input has rotated to the shared demuxer's pad.
        let f = filter_node_awaiting(
            vec![Some(other.clone()), Some(shared.clone())],
            1,
            &[true, true],
        );
        let m = mux_stream(f, AV_NOPTS_VALUE);
        let ctrl = InputController::new(vec![other.clone(), shared.clone()], vec![m]);
        ctrl.update_locked(&status);

        assert!(
            !waiter_of(&shared).get_choked(),
            "the selected pad's demuxer is unchoked even while it carries parked risk"
        );
        assert!(
            !waiter_of(&other).get_choked(),
            "the risk-free awaiting peer still primes in parallel"
        );
    }

    // Once the graph holding the primed pad configures, its filter thread
    // releases the risk count and the demuxer rejoins the fan-out.
    #[test]
    fn parked_risk_release_restores_the_fanout() {
        let status = Arc::new(AtomicUsize::new(STATUS_RUN));
        let cold = demux_node();
        let shared = demux_node();
        set_parked_risk(&shared, 1);
        let f = filter_node_awaiting(
            vec![Some(cold.clone()), Some(shared.clone())],
            0,
            &[true, true],
        );
        let m = mux_stream(f, AV_NOPTS_VALUE);
        let ctrl = InputController::new(vec![cold.clone(), shared.clone()], vec![m]);
        ctrl.update_locked(&status);
        assert!(
            waiter_of(&shared).get_choked(),
            "deferred while the risk is outstanding"
        );

        set_parked_risk(&shared, 0);
        ctrl.update_locked(&status);
        assert!(
            !waiter_of(&shared).get_choked(),
            "rejoins the fan-out once the blocking graph configured"
        );
    }

    // A mix of resolved and unresolved eligible streams must STILL run the
    // all-live-demuxer fallback: one stream unchoking a demuxer must not suppress
    // unchoking the demuxer behind a stream that resolved to none (an empty/OOB
    // filter). Otherwise the unresolved stream's demuxer stays choked and the job
    // hangs. Exercises update_locked end-to-end, not just unchoke_for_stream.
    #[test]
    fn mixed_resolved_and_unresolved_streams_still_run_the_fallback() {
        let d1 = demux_node();
        let d2 = demux_node();
        let mux_ok = mux_stream(d2.clone(), 1_000);
        let mux_unresolved = mux_stream(filter_node(vec![], 0), 1_000);
        let ctrl = InputController::new(vec![d1.clone(), d2.clone()], vec![mux_ok, mux_unresolved]);
        assert!(ctrl.balancing_possible, "two demuxers can balance");

        let status = Arc::new(AtomicUsize::new(STATUS_RUN));
        ctrl.update_locked(&status);

        // Without the fallback (the pre-fix mixed case), d1 -- fed to no resolved
        // stream -- would stay choked. The fallback unchokes EVERY live demuxer.
        assert!(
            !waiter_of(&d1).get_choked(),
            "d1 must be unchoked by the fallback despite another stream resolving"
        );
        assert!(!waiter_of(&d2).get_choked(), "d2 must be unchoked");
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
