//! Progress API tests: pure telemetry semantics (no FFmpeg involved) plus
//! end-to-end jobs driven through the real scheduler with `lavfi` inputs.

use super::handle::ProgressHandle;
use super::snapshot::OutputProgress;
use super::state::ProgressState;
use super::tracker::{OutputTelemetry, ProgressTracker};
use crate::core::context::ffmpeg_context::FfmpegContext;
use crate::core::context::input::Input;
use crate::core::context::output::Output;
use crate::core::scheduler::ffmpeg_scheduler::{
    FfmpegScheduler, STATUS_END, STATUS_PAUSE, STATUS_RUN,
};
use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};
use std::sync::Arc;
use std::time::Duration;

// ---------------------------------------------------------------- pure

#[test]
fn out_time_is_none_until_every_active_stream_started() {
    let t = OutputTelemetry::new(2);
    assert_eq!(t.out_time_us(), None, "nothing written yet");
    t.record_written(0, Some(5_000_000));
    assert_eq!(
        t.out_time_us(),
        None,
        "fftools-strict: an active stream has not started"
    );
    t.record_written(1, Some(3_000_000));
    assert_eq!(t.out_time_us(), Some(3_000_000), "min over active streams");
}

#[test]
fn finished_stream_leaves_the_active_min() {
    let t = OutputTelemetry::new(2);
    t.record_written(0, Some(10_000_000));
    t.record_written(1, Some(5_000_000));
    assert_eq!(t.out_time_us(), Some(5_000_000));
    // The short stream finishes: it must stop pinning the reported position.
    t.mark_stream_finished(1);
    assert_eq!(t.out_time_us(), Some(10_000_000));
    // Everything finished: freeze at the output's high-water mark.
    t.mark_all_streams_finished();
    assert_eq!(t.out_time_us(), Some(10_000_000));
}

#[test]
fn all_finished_freezes_at_the_maximum_started_watermark() {
    // One stream wrote, the other never started (zero-packet stream): the
    // freeze still reports the committed high-water mark.
    let t = OutputTelemetry::new(2);
    t.record_written(0, Some(2_000_000));
    t.mark_all_streams_finished();
    assert_eq!(t.out_time_us(), Some(2_000_000));

    // No stream ever started: nothing to report, even finished.
    let idle = OutputTelemetry::new(2);
    idle.mark_all_streams_finished();
    assert_eq!(idle.out_time_us(), None);

    // Zero-stream output (streamless muxer): never reports a position.
    let streamless = OutputTelemetry::new(0);
    assert_eq!(streamless.out_time_us(), None);
}

#[test]
fn out_time_some_values_are_monotonic_across_a_scripted_run() {
    let t = OutputTelemetry::new(3);
    let mut seen: Vec<i64> = Vec::new();
    let mut observe = |t: &OutputTelemetry| {
        if let Some(v) = t.out_time_us() {
            seen.push(v);
        }
    };
    observe(&t);
    t.record_written(0, Some(1_000));
    observe(&t);
    t.record_written(1, Some(500));
    observe(&t);
    t.record_written(2, Some(800));
    observe(&t); // all started: min = 500
    t.record_written(1, Some(1_500));
    observe(&t); // min = 800
    t.mark_stream_finished(2);
    observe(&t); // active {0, 1}: min = 1_000
    t.record_written(0, Some(2_500));
    observe(&t); // min = 1_500
    t.mark_stream_finished(1);
    observe(&t); // active {0}: min = 2_500
    t.mark_stream_finished(0);
    observe(&t); // frozen at max = 2_500
    assert!(!seen.is_empty());
    for pair in seen.windows(2) {
        assert!(
            pair[1] >= pair[0],
            "out_time regressed: {} -> {} (sequence {seen:?})",
            pair[0],
            pair[1]
        );
    }
    assert_eq!(*seen.last().unwrap(), 2_500);
}

#[test]
fn video_packets_none_until_selection_then_counts_only_the_selected_stream() {
    let t = OutputTelemetry::new(2);
    assert_eq!(t.video_packets(), None, "selection unresolved");
    t.set_video_stream(Some(0));
    assert_eq!(t.video_packets(), Some(0));
    t.record_written(0, Some(1));
    t.record_written(1, Some(1)); // non-video stream: not counted
    assert_eq!(t.video_packets(), Some(1));

    let no_video = OutputTelemetry::new(1);
    no_video.set_video_stream(None);
    no_video.record_written(0, Some(1));
    assert_eq!(no_video.video_packets(), None, "audio-only output");
}

#[test]
fn total_size_ignores_unavailable_positions() {
    let t = OutputTelemetry::new(1);
    assert_eq!(t.total_size(), None);
    t.set_total_size(-1);
    assert_eq!(t.total_size(), None, "an error position must not become 0");
    t.set_total_size(4096);
    assert_eq!(t.total_size(), Some(4096));
    t.set_total_size(-1);
    assert_eq!(t.total_size(), Some(4096), "never regress to unknown");
}

#[test]
fn percent_of_clamps_and_rejects_nonpositive_totals() {
    let t = OutputTelemetry::new(1);
    t.record_written(0, Some(5_000_000));
    let entry = OutputProgress::collect(0, &t, Duration::from_secs(1));
    assert_eq!(entry.percent_of(10_000_000), Some(50.0));
    assert_eq!(entry.percent_of(2_000_000), Some(100.0), "clamped high");
    assert_eq!(entry.percent_of(0), None);
    assert_eq!(entry.percent_of(-5), None);

    let blind = OutputProgress::collect(0, &OutputTelemetry::new(1), Duration::from_secs(1));
    assert_eq!(blind.percent_of(10_000_000), None, "unknown position");
}

#[test]
fn derived_rates_require_their_inputs() {
    let t = OutputTelemetry::new(1);
    t.set_video_stream(Some(0));
    t.record_written(0, Some(2_000_000));
    t.set_total_size(1_000);

    let zero_elapsed = OutputProgress::collect(0, &t, Duration::ZERO);
    assert_eq!(zero_elapsed.fps(), None, "no wall time yet");
    assert_eq!(zero_elapsed.speed(), None);
    // bitrate depends on media time, not wall time.
    let kbps = zero_elapsed.bitrate_kbps().expect("size and position known");
    assert!((kbps - 4.0).abs() < 1e-9, "1000 B over 2 s = 4 kbit/s, got {kbps}");

    let with_elapsed = OutputProgress::collect(0, &t, Duration::from_secs(1));
    assert_eq!(with_elapsed.fps(), Some(1.0));
    assert_eq!(with_elapsed.speed(), Some(2.0), "2 media seconds per wall second");
}

#[test]
fn handle_state_follows_status_producers_and_latch() {
    let demux_exited = Arc::new(AtomicBool::new(false));
    let tracker = Arc::new(ProgressTracker::new(
        vec![Arc::new(OutputTelemetry::new(1))],
        vec![demux_exited.clone()],
        0,
    ));
    tracker.mark_started();
    let status = Arc::new(AtomicUsize::new(STATUS_RUN));
    let epoch = Arc::new(AtomicUsize::new(0));
    let handle = ProgressHandle::new(status.clone(), epoch.clone(), tracker.clone());

    assert_eq!(handle.snapshot().state(), ProgressState::Running);
    assert!(!handle.is_ended());

    // pause(): epoch even->odd BEFORE the status flip (both windows report
    // Paused), then the status store.
    epoch.fetch_add(1, Ordering::Release);
    assert_eq!(handle.snapshot().state(), ProgressState::Paused);
    status.store(STATUS_PAUSE, Ordering::Release);
    assert_eq!(handle.snapshot().state(), ProgressState::Paused);

    // resume(): status CAS first, epoch odd->even after.
    status.store(STATUS_RUN, Ordering::Release);
    assert_eq!(handle.snapshot().state(), ProgressState::Paused, "transition window");
    epoch.fetch_add(1, Ordering::Release);
    assert_eq!(handle.snapshot().state(), ProgressState::Running);

    // Every producer retired: draining/flushing.
    demux_exited.store(true, Ordering::Release);
    assert_eq!(handle.snapshot().state(), ProgressState::Finishing);

    // Terminal signal published, workers still tearing down.
    status.store(STATUS_END, Ordering::Release);
    assert_eq!(handle.snapshot().state(), ProgressState::Finishing);
    assert!(!handle.is_ended(), "signal is not teardown");

    // Completion latch sealed: Ended, elapsed frozen.
    tracker.seal_completed();
    assert!(handle.is_ended());
    let first = handle.snapshot();
    assert_eq!(first.state(), ProgressState::Ended);
    std::thread::sleep(Duration::from_millis(20));
    assert_eq!(handle.snapshot().elapsed(), first.elapsed(), "frozen clock");
}

#[test]
fn handle_is_send_sync_clone() {
    fn assert_traits<T: Send + Sync + Clone + 'static>() {}
    assert_traits::<ProgressHandle>();
}

/// Defect-1 semantic guardian. This deterministic test pins the CONSEQUENCE
/// the `finished`-before-watermark load order exists to guarantee: once every
/// stream is retired, a finished stream's watermark still counts toward the
/// terminal maximum, so repeated reads return the same frozen max and never
/// collapse to `None` — even when a stream finished at a LOWER watermark than
/// a peer. A reversed load order breaks exactly this invariant (a finished
/// stream contributes a stale `STREAM_NOT_STARTED`), so any regression that
/// drops a finished stream's watermark from the terminal max fails here.
#[test]
fn all_finished_publication_is_stable_and_never_collapses() {
    let t = OutputTelemetry::new(3);
    // Writer discipline mirrors finish_output_stream: raise the watermark,
    // THEN retire the stream.
    t.record_written(0, Some(9_000_000));
    t.mark_stream_finished(0);
    t.record_written(1, Some(4_000_000)); // shorter stream, lower final ts
    t.mark_stream_finished(1);
    t.record_written(2, Some(9_000_000));
    t.mark_stream_finished(2);

    // Repeated terminal reads are stable at the high-water mark, never None.
    for _ in 0..1_000 {
        assert_eq!(
            t.out_time_us(),
            Some(9_000_000),
            "an all-finished read must freeze at the max and never collapse to None"
        );
    }
}

/// Defect-2 gate: the per-packet byte-position probe is armed only once a
/// progress handle exists. A fresh telemetry is unobserved; `mark_observed`
/// (and the tracker-level fan-out `progress_handle()` uses) arms it. Core
/// telemetry (watermark, video_packets) is maintained regardless — only the
/// `total_size` hot-path probe is gated.
#[test]
fn observer_gate_defaults_off_and_arms_on_handle() {
    let t = OutputTelemetry::new(1);
    assert!(!t.is_observed(), "an unobserved job must skip the size probe");
    // Core telemetry still works without an observer.
    t.set_video_stream(Some(0));
    t.record_written(0, Some(1_000_000));
    assert_eq!(
        t.out_time_us(),
        Some(1_000_000),
        "the single started stream defines the active min"
    );
    t.mark_stream_finished(0);
    assert_eq!(t.video_packets(), Some(1));

    t.mark_observed();
    assert!(t.is_observed(), "mark_observed arms the probe");

    // The tracker fan-out arms every output at once (what progress_handle does).
    let tracker = ProgressTracker::new(
        vec![Arc::new(OutputTelemetry::new(1)), Arc::new(OutputTelemetry::new(2))],
        vec![],
        0,
    );
    assert!(tracker.outputs().iter().all(|o| !o.is_observed()));
    tracker.mark_observed();
    assert!(
        tracker.outputs().iter().all(|o| o.is_observed()),
        "progress_handle() must arm every output's probe"
    );
}

/// Functional liveness under real concurrency: one writer retires streams one
/// by one with the finish_output_stream discipline (raise watermark, then
/// mark finished) while several readers poll `out_time_us`. Every `Some` a
/// reader sees must be non-decreasing and, once seen, must never revert to
/// `None`.
///
/// NOTE: this is a liveness/smoke test, not a memory-ordering sentinel — a
/// plain multi-threaded test cannot deterministically expose a reversed
/// `finished`/watermark load order (x86-TSO hides it, and forcing the window
/// needs either a barrier that pollutes the production reader or a Loom
/// model). The load-order invariant is instead documented at the two loads
/// in `out_time_us` and its *semantic* consequence — a finished stream's
/// watermark must count toward the terminal max — is pinned deterministically
/// by [`all_finished_publication_is_stable_and_never_collapses`]. Exhaustive
/// interleaving verification (Loom) is a tracked backlog item.
#[test]
fn out_time_us_never_regresses_under_concurrent_finish() {
    use std::sync::atomic::AtomicBool as StdAtomicBool;

    const STREAMS: usize = 4;
    let telemetry = Arc::new(OutputTelemetry::new(STREAMS));
    // Every stream has committed a first packet up front, so the active min
    // is always defined; the race under test is purely finish-vs-read.
    for i in 0..STREAMS {
        telemetry.record_written(i, Some((i as i64 + 1) * 1_000_000));
    }
    let done = Arc::new(StdAtomicBool::new(false));

    let readers: Vec<_> = (0..3)
        .map(|_| {
            let telemetry = telemetry.clone();
            let done = done.clone();
            std::thread::spawn(move || {
                let mut last: Option<i64> = None;
                let mut seen_some = false;
                loop {
                    let now = telemetry.out_time_us();
                    if let Some(v) = now {
                        seen_some = true;
                        if let Some(prev) = last {
                            assert!(v >= prev, "out_time_us regressed: {prev} -> {v}");
                        }
                        last = Some(v);
                    } else {
                        assert!(
                            !seen_some,
                            "out_time_us collapsed to None after a Some (stale all-finished read)"
                        );
                    }
                    if done.load(Ordering::Acquire) && telemetry.out_time_us().is_some() {
                        break;
                    }
                }
            })
        })
        .collect();

    // Writer: retire streams one at a time, each raising its watermark first
    // (the finish_output_stream order) with a widening delay to expose the
    // publication window.
    for i in 0..STREAMS {
        telemetry.record_written(i, Some((i as i64 + 1) * 10_000_000));
        telemetry.mark_stream_finished(i);
        std::thread::sleep(Duration::from_micros(50));
    }
    done.store(true, Ordering::Release);

    for r in readers {
        r.join().expect("a reader observed a monotonicity violation");
    }
    // Terminal freeze is the max final watermark.
    assert_eq!(telemetry.out_time_us(), Some(STREAMS as i64 * 10_000_000));
}

// ---------------------------------------------------------- real jobs

fn tmp_path(name: &str) -> String {
    let dir = std::env::temp_dir().join(format!("ez_ffmpeg_progress_tests_{}", std::process::id()));
    std::fs::create_dir_all(&dir).unwrap();
    dir.join(name).to_string_lossy().into_owned()
}

/// Encodes a short synthetic clip (`testsrc2`, 25 fps) to `path` with the
/// always-built-in mpeg4 encoder, for use as a stream-copy source.
fn encode_fixture(path: &str, seconds: &str) {
    let source = format!("testsrc2=duration={seconds}:size=192x108:rate=25");
    let context = FfmpegContext::builder()
        .input(Input::from(source.as_str()).set_format("lavfi"))
        .output(Output::from(path).set_video_codec("mpeg4"))
        .build()
        .expect("fixture context");
    FfmpegScheduler::new(context)
        .start()
        .expect("fixture start")
        .wait()
        .expect("fixture job");
}

#[test]
fn streamcopy_remux_is_not_blind_to_progress() {
    let src = tmp_path("progress_copy_src.mp4");
    encode_fixture(&src, "1");

    let out = tmp_path("progress_copy_out.mp4");
    let context = FfmpegContext::builder()
        .input(Input::from(src.as_str()))
        .output(Output::from(out.as_str()).set_video_codec("copy"))
        .build()
        .expect("remux context");
    let scheduler = FfmpegScheduler::new(context).start().expect("start");
    let handle = scheduler.progress_handle();
    scheduler.wait().expect("remux job");

    assert!(handle.is_ended());
    let snapshot = handle.snapshot();
    assert_eq!(snapshot.state(), ProgressState::Ended);
    assert!(snapshot.elapsed() > Duration::ZERO);
    assert_eq!(snapshot.outputs().len(), 1);
    let output = &snapshot.outputs()[0];
    assert_eq!(output.output_index(), 0);
    let out_time = output
        .out_time_us()
        .expect("a stream-copy job must report its mux position");
    assert!(
        out_time > 500_000,
        "~1s of media committed, got {out_time}us"
    );
    let packets = output
        .video_packets()
        .expect("the copied video stream is the selected one");
    assert!(packets >= 20, "25 fps over ~1s, got {packets}");
    assert!(output.total_size().expect("file-backed output size") > 0);
    assert!(output.fps().is_some());
    assert!(output.speed().is_some());
    assert!(output.bitrate_kbps().is_some());
}

#[test]
fn multi_output_reports_each_output_and_nofile_has_no_size() {
    let out0 = tmp_path("progress_multi.mp4");
    let context = FfmpegContext::builder()
        .input(
            Input::from("testsrc2=duration=1:size=192x108:rate=25")
                .set_format("lavfi"),
        )
        .output(Output::from(out0.as_str()).set_video_codec("mpeg4"))
        .output(
            Output::from("progress-null-sink")
                .set_format("null")
                .set_video_codec("mpeg4"),
        )
        .build()
        .expect("multi-output context");
    let scheduler = FfmpegScheduler::new(context).start().expect("start");
    let handle = scheduler.progress_handle();
    scheduler.wait().expect("multi-output job");

    let snapshot = handle.snapshot();
    assert_eq!(snapshot.outputs().len(), 2, "one entry per output, no scalar collapse");
    let file_out = &snapshot.outputs()[0];
    let null_out = &snapshot.outputs()[1];
    assert_eq!(file_out.output_index(), 0);
    assert_eq!(null_out.output_index(), 1);

    assert!(file_out.total_size().expect("mp4 is file-backed") > 0);
    assert!(file_out.out_time_us().is_some());

    assert_eq!(
        null_out.total_size(),
        None,
        "AVFMT_NOFILE muxer must report None, not a fabricated 0"
    );
    assert!(
        null_out.out_time_us().is_some(),
        "packets still commit through the null muxer"
    );
    assert!(null_out.video_packets().unwrap_or(0) > 0);
}

#[test]
fn out_time_is_monotonic_across_live_snapshots() {
    let out = tmp_path("progress_live.mp4");
    let context = FfmpegContext::builder()
        .input(
            Input::from("testsrc2=duration=1.5:size=192x108:rate=25")
                .set_format("lavfi")
                .set_readrate(1.0),
        )
        .output(Output::from(out.as_str()).set_video_codec("mpeg4"))
        .build()
        .expect("paced context");
    let scheduler = FfmpegScheduler::new(context).start().expect("start");
    let handle = scheduler.progress_handle();

    let poller = std::thread::spawn({
        let handle = handle.clone();
        move || {
            let mut seen: Vec<i64> = Vec::new();
            while !handle.is_ended() {
                if let Some(v) = handle.snapshot().outputs()[0].out_time_us() {
                    seen.push(v);
                }
                std::thread::sleep(Duration::from_millis(40));
            }
            if let Some(v) = handle.snapshot().outputs()[0].out_time_us() {
                seen.push(v);
            }
            seen
        }
    });

    scheduler.wait().expect("paced job");
    let seen = poller.join().expect("poller thread");
    assert!(
        !seen.is_empty(),
        "a ~1.5s paced job must yield at least the final position"
    );
    for pair in seen.windows(2) {
        assert!(
            pair[1] >= pair[0],
            "documented monotonic promise violated: {} -> {}",
            pair[0],
            pair[1]
        );
    }
}

#[test]
fn pause_resume_and_stop_states_are_reported() {
    let context = FfmpegContext::builder()
        .input(
            Input::from("testsrc2=duration=5:size=192x108:rate=25")
                .set_format("lavfi")
                .set_readrate(1.0),
        )
        .output(
            Output::from("progress-pause-null")
                .set_format("null")
                .set_video_codec("mpeg4"),
        )
        .build()
        .expect("paced context");
    let scheduler = FfmpegScheduler::new(context).start().expect("start");
    let handle = scheduler.progress_handle();
    assert_eq!(handle.snapshot().state(), ProgressState::Running);
    assert!(!handle.is_ended());

    let paused = scheduler.pause();
    assert_eq!(handle.snapshot().state(), ProgressState::Paused);
    // The Paused-state scheduler hands out an equivalent handle.
    assert_eq!(
        paused.progress_handle().snapshot().state(),
        ProgressState::Paused
    );

    let scheduler = paused.resume();
    assert_eq!(handle.snapshot().state(), ProgressState::Running);

    scheduler.stop().expect("graceful stop");
    assert!(handle.is_ended());
    assert_eq!(handle.snapshot().state(), ProgressState::Ended);
}

#[test]
fn snapshots_survive_scheduler_drop_and_freeze() {
    let out = tmp_path("progress_drop.mp4");
    let context = FfmpegContext::builder()
        .input(Input::from("testsrc2=duration=0.5:size=192x108:rate=25").set_format("lavfi"))
        .output(Output::from(out.as_str()).set_video_codec("mpeg4"))
        .build()
        .expect("context");
    let scheduler = FfmpegScheduler::new(context).start().expect("start");
    let handle = scheduler.progress_handle();

    // Dropping the running scheduler signals termination and JOINS every
    // worker (RunningGuard); the completion latch must be sealed by the
    // time drop() returns, and snapshots must stay safe with the scheduler
    // (and its FFmpeg contexts) gone.
    drop(scheduler);
    assert!(handle.is_ended());
    let first = handle.snapshot();
    assert_eq!(first.state(), ProgressState::Ended);
    std::thread::sleep(Duration::from_millis(30));
    let second = handle.snapshot();
    assert_eq!(second.elapsed(), first.elapsed(), "clock frozen at teardown");

    // Clones keep working across threads after the scheduler is gone.
    let cloned = handle.clone();
    let state = std::thread::spawn(move || cloned.snapshot().state())
        .join()
        .expect("cross-thread snapshot");
    assert_eq!(state, ProgressState::Ended);
}

/// Defect-2 end-to-end gate: a real file-backed job's mux write path takes
/// per-packet byte-position probes ONLY when a progress handle exists.
/// Exercises the public `progress_handle()` and the `write_packet` gate,
/// counting the actual probes via the test-only per-output counter — the
/// private-flag unit test cannot prove either.
#[test]
fn per_packet_size_probe_runs_only_when_observed() {
    // Unobserved job: obtain the tracker WITHOUT arming observation
    // (progress_tracker_for_test does not call mark_observed, unlike
    // progress_handle), so the mux path's is_observed gate stays closed.
    let out = tmp_path("progress_gate_unobserved.mp4");
    let context = FfmpegContext::builder()
        .input(Input::from("testsrc2=duration=1:size=192x108:rate=25").set_format("lavfi"))
        .output(Output::from(out.as_str()).set_video_codec("mpeg4"))
        .build()
        .expect("unobserved context");
    let scheduler = FfmpegScheduler::new(context).start().expect("start");
    let tracker = scheduler.progress_tracker_for_test();
    scheduler.wait().expect("unobserved job");
    assert_eq!(
        tracker.outputs()[0].perpacket_size_probes(),
        0,
        "an unobserved job must take zero per-packet size probes"
    );
    // Sanity: core telemetry still ran (watermark advanced) despite no probe.
    assert!(
        tracker.outputs()[0].out_time_us().is_some(),
        "core progress telemetry must run regardless of observation"
    );

    // Observed job: an UNBOUNDED, real-time-paced source (no `duration`, so
    // the job never ends on its own). This removes the race the paced-but-
    // finite fixture still had — pacing never ordered `start()` returning
    // against `progress_handle()` arming, so a finite job could commit every
    // packet before the arm. With an unbounded source packets are guaranteed
    // to keep committing after observation is armed, so we arm, then POLL the
    // per-output probe count until it advances past zero (with a timeout so a
    // gate regression fails fast instead of hanging), then stop the job.
    let out = tmp_path("progress_gate_observed.mp4");
    let context = FfmpegContext::builder()
        .input(
            Input::from("testsrc2=size=192x108:rate=25")
                .set_format("lavfi")
                .set_readrate(1.0),
        )
        .output(Output::from(out.as_str()).set_video_codec("mpeg4"))
        .build()
        .expect("observed context");
    let scheduler = FfmpegScheduler::new(context).start().expect("start");
    let _handle = scheduler.progress_handle(); // arms observation
    let tracker = scheduler.progress_tracker_for_test();

    // Deterministic: the source is unbounded, so probes must advance once the
    // gate is armed. Poll until the first one lands.
    let deadline = std::time::Instant::now() + Duration::from_secs(15);
    loop {
        if tracker.outputs()[0].perpacket_size_probes() > 0 {
            break;
        }
        assert!(
            std::time::Instant::now() < deadline,
            "an observed job took no per-packet size probe within the timeout"
        );
        std::thread::sleep(Duration::from_millis(10));
    }
    // Observed deterministically; tear the unbounded job down.
    scheduler.stop().expect("stop observed job");
    assert!(
        tracker.outputs()[0].perpacket_size_probes() > 0,
        "an observed job must probe the byte position per committed packet"
    );
    assert!(
        tracker.outputs()[0].total_size().is_some(),
        "the observed probe must have published a size"
    );
}
