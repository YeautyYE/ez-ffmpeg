//! Packet-sink teardown: stop()/drop() mid-stream must terminate cleanly —
//! no `on_end` (packets were still in flight), no hang, no leak — riding the
//! same delayed-start machinery `tests/mux_teardown.rs` sweeps for container
//! outputs.

mod common;

use common::{have_encoder, recording_sink, SinkEv};
use ez_ffmpeg::core::scheduler::ffmpeg_scheduler::Running;
use ez_ffmpeg::{FfmpegContext, FfmpegScheduler, Input, Output};
use std::sync::Mutex;
use std::time::Duration;

/// The sweep delays are only meaningful when the scenarios do not compete for
/// CPU inside the shared test binary: serialize them.
static PROCESS_LOCK: Mutex<()> = Mutex::new(());

/// Paced video so packets are still in flight whenever the stop lands.
fn paced_input() -> Input {
    Input::from("testsrc=size=320x240:rate=25:duration=10")
        .set_format("lavfi")
        .set_readrate(0.2)
}

fn start_sink_job() -> (FfmpegScheduler<Running>, common::SinkLog) {
    let (sink, log) = recording_sink();
    let scheduler = FfmpegContext::builder()
        .input(paced_input())
        .output(
            Output::new_by_packet_sink(sink)
                .set_video_codec("libx264")
                .set_video_codec_opt("preset", "ultrafast")
                .set_video_codec_opt("tune", "zerolatency"),
        )
        .build()
        .unwrap()
        .start()
        .unwrap();
    (scheduler, log)
}

/// stop() on a side thread; a stall past `secs` is a teardown hang.
fn stop_with_watchdog(scheduler: FfmpegScheduler<Running>, secs: u64, scenario: &str) {
    let (tx, rx) = std::sync::mpsc::channel();
    let scenario_owned = scenario.to_string();
    std::thread::spawn(move || {
        scheduler
            .stop()
            .unwrap_or_else(|e| panic!("stop() failed ({scenario_owned}): {e}"));
        let _ = tx.send(());
    });
    assert!(
        rx.recv_timeout(Duration::from_secs(secs)).is_ok(),
        "stop() hung during packet-sink teardown ({scenario})"
    );
}

fn assert_no_terminal_event(log: &common::SinkLog, scenario: &str) {
    let events = log.lock().unwrap();
    assert!(
        !events.iter().any(|e| matches!(e, SinkEv::End { .. })),
        "{scenario}: stop() with packets in flight must not produce on_end"
    );
    assert!(
        !events.iter().any(|e| matches!(e, SinkEv::Error(_))),
        "{scenario}: cancellation is not a delivery error"
    );
}

/// Sweep stop() across the delayed-start window: tiny delays land while the
/// ready-to-init waiter still owns the context, larger ones land in the
/// delivering worker. Every landing must terminate without `on_end`, without
/// `on_error`, and without a hang.
#[test]
fn stop_mid_stream_fires_no_terminal_callback() {
    if !have_encoder("libx264") {
        eprintln!("skipping: libx264 not available in this FFmpeg build");
        return;
    }
    let _lock = PROCESS_LOCK.lock().unwrap_or_else(|e| e.into_inner());
    for delay_ms in [0u64, 30, 120, 400] {
        let (scheduler, log) = start_sink_job();
        std::thread::sleep(Duration::from_millis(delay_ms));
        let scenario = format!("stop_after_{delay_ms}ms");
        stop_with_watchdog(scheduler, 30, &scenario);
        assert_no_terminal_event(&log, &scenario);
        // Whatever was delivered before the stop must still be well-ordered:
        // stream info (if any) strictly precedes every packet.
        let events = log.lock().unwrap();
        if let Some(first_pkt) = events.iter().position(|e| matches!(e, SinkEv::Pkt(_))) {
            assert!(
                events[..first_pkt]
                    .iter()
                    .any(|e| matches!(e, SinkEv::Info { .. })),
                "{scenario}: a packet arrived without a preceding stream info"
            );
        }
    }
}

/// The correctness review's teardown probe: stop() must not return while a
/// blocking callback-capture destructor is still running — user captures are
/// destroyed at a defined point BEFORE the worker's thread slot releases.
#[test]
fn stop_returns_only_after_callback_captures_are_destroyed() {
    if !have_encoder("libx264") {
        eprintln!("skipping: libx264 not available in this FFmpeg build");
        return;
    }
    let _lock = PROCESS_LOCK.lock().unwrap_or_else(|e| e.into_inner());

    /// A capture whose destructor takes measurable time and records its
    /// completion.
    struct SlowCapture {
        destroyed: std::sync::Arc<std::sync::atomic::AtomicBool>,
    }
    impl Drop for SlowCapture {
        fn drop(&mut self) {
            std::thread::sleep(Duration::from_millis(400));
            self.destroyed
                .store(true, std::sync::atomic::Ordering::Release);
        }
    }

    let destroyed = std::sync::Arc::new(std::sync::atomic::AtomicBool::new(false));
    let delivered = std::sync::Arc::new(std::sync::atomic::AtomicBool::new(false));
    let capture = SlowCapture {
        destroyed: destroyed.clone(),
    };
    let delivered_in_cb = delivered.clone();
    let sink = ez_ffmpeg::packet_sink::PacketSink::builder(move |_pkt| {
        // Keep the capture owned by the callback until the sink drops.
        let _hold = &capture;
        delivered_in_cb.store(true, std::sync::atomic::Ordering::Release);
        Ok(())
    })
    .build();

    let scheduler = FfmpegContext::builder()
        .input(paced_input())
        .output(
            Output::from(sink)
                .set_video_codec("libx264")
                .set_video_codec_opt("preset", "ultrafast")
                .set_video_codec_opt("tune", "zerolatency"),
        )
        .build()
        .unwrap()
        .start()
        .unwrap();
    // Let delivery begin so the callback (and its capture) provably moved to
    // the worker.
    let began = std::time::Instant::now();
    while !delivered.load(std::sync::atomic::Ordering::Acquire)
        && began.elapsed() < Duration::from_secs(10)
    {
        std::thread::sleep(Duration::from_millis(10));
    }
    assert!(
        delivered.load(std::sync::atomic::Ordering::Acquire),
        "no packet was delivered before the stop"
    );

    let (tx, rx) = std::sync::mpsc::channel();
    let destroyed_probe = destroyed.clone();
    std::thread::spawn(move || {
        scheduler.stop().expect("stop() failed");
        // Sampled the INSTANT stop() returns: the slow destructor must have
        // completed already.
        let _ = tx.send(destroyed_probe.load(std::sync::atomic::Ordering::Acquire));
    });
    let destroyed_when_stop_returned = rx
        .recv_timeout(Duration::from_secs(30))
        .expect("stop() hung");
    assert!(
        destroyed_when_stop_returned,
        "stop() returned while the callback-capture destructor was still running"
    );
}

/// The channel review probe: capacity 1, a LIVE receiver that never drains,
/// packets in flight — stop() must still terminate (the blocked bounded send
/// observes cancellation), cleanly and without fabricating an error.
#[test]
fn stop_terminates_with_full_undrained_channel() {
    if !have_encoder("libx264") {
        eprintln!("skipping: libx264 not available in this FFmpeg build");
        return;
    }
    let _lock = PROCESS_LOCK.lock().unwrap_or_else(|e| e.into_inner());
    let (sink, receiver) =
        ez_ffmpeg::packet_sink::PacketSink::channel(std::num::NonZeroUsize::new(1).unwrap());
    let scheduler = FfmpegContext::builder()
        .input(paced_input())
        .output(
            Output::from(sink)
                .set_video_codec("libx264")
                .set_video_codec_opt("preset", "ultrafast")
                .set_video_codec_opt("tune", "zerolatency"),
        )
        .build()
        .unwrap()
        .start()
        .unwrap();
    // Give the pipeline time to fill the capacity-1 channel and block.
    std::thread::sleep(Duration::from_millis(700));

    let (tx, rx) = std::sync::mpsc::channel();
    std::thread::spawn(move || {
        let result = scheduler.stop();
        let _ = tx.send(result);
    });
    let result = rx
        .recv_timeout(Duration::from_secs(30))
        .expect("stop() hung with a full, undrained channel");
    result.expect("cooperative channel cancellation must not fabricate a job error");
    // The receiver stayed alive the whole time; drain whatever landed.
    let mut saw_end_or_error = false;
    while let Ok(event) = receiver.try_recv() {
        if matches!(
            event,
            ez_ffmpeg::packet_sink::PacketSinkEvent::End
                | ez_ffmpeg::packet_sink::PacketSinkEvent::Error(_)
        ) {
            saw_end_or_error = true;
        }
    }
    assert!(
        !saw_end_or_error,
        "cancellation must not produce a terminal sink event"
    );
}

/// The round-7 abort-window probe: abort() landing while the final packets
/// are still being delivered must never let a terminal callback fire — the
/// terminal decision reads the status fresh, after the job settled, not from
/// a pre-wait snapshot. The last delivered packet blocks until abort() has
/// provably been issued, making the race deterministic. Native AAC.
#[test]
fn abort_during_final_delivery_fires_no_terminal_callback() {
    let _lock = PROCESS_LOCK.lock().unwrap_or_else(|e| e.into_inner());
    let (abort_request_tx, abort_request_rx) = std::sync::mpsc::channel::<()>();
    let (abort_done_tx, abort_done_rx) = std::sync::mpsc::channel::<()>();

    let log: common::SinkLog = std::sync::Arc::new(std::sync::Mutex::new(Vec::new()));
    let (end_log, err_log) = (log.clone(), log.clone());
    let mut delivered = 0u32;
    let sink = ez_ffmpeg::packet_sink::PacketSink::builder(move |_pkt| {
        delivered += 1;
        if delivered == 10 {
            // Ask the controller to abort, and hold this packet until the
            // abort status has been stored — the terminal region then must
            // observe it.
            let _ = abort_request_tx.send(());
            let _ = abort_done_rx.recv_timeout(Duration::from_secs(10));
        }
        Ok(())
    })
    .on_end(move || {
        end_log.lock().unwrap().push(SinkEv::End {
            thread: std::thread::current().id(),
        })
    })
    .on_delivery_error(move |e| err_log.lock().unwrap().push(SinkEv::Error(e.to_string())))
    .build();

    let scheduler = FfmpegContext::builder()
        .input(Input::from("sine=frequency=440:duration=1").set_format("lavfi"))
        .output(Output::from(sink).set_audio_codec("aac"))
        .build()
        .unwrap()
        .start()
        .unwrap();

    abort_request_rx
        .recv_timeout(Duration::from_secs(20))
        .expect("delivery never reached the abort point");
    // abort() consumes the scheduler and BLOCKS until every worker exits
    // (RunningGuard's drop) — the held callback would deadlock it on this
    // thread, so it runs on a helper. Its ABORT store is its first action;
    // the grace below dwarfs the helper's spawn+store latency, so the store
    // has landed before the held packet is released.
    let abort_thread = std::thread::spawn(move || scheduler.abort());
    std::thread::sleep(Duration::from_millis(500));
    let _ = abort_done_tx.send(());
    abort_thread
        .join()
        .expect("abort() must return once the job tears down");
    assert_no_terminal_event(&log, "abort_during_final_delivery");
}

/// Encoder-independent lifecycle case (native AAC): stop() mid-stream on an
/// audio-only sink terminates without a terminal callback — so CI without
/// libx264 still exercises the real teardown machinery.
#[test]
fn aac_stop_mid_stream_fires_no_terminal_callback() {
    let _lock = PROCESS_LOCK.lock().unwrap_or_else(|e| e.into_inner());
    let (sink, log) = recording_sink();
    let scheduler = FfmpegContext::builder()
        .input(
            Input::from("sine=frequency=440:duration=10")
                .set_format("lavfi")
                .set_readrate(0.2),
        )
        .output(Output::from(sink).set_audio_codec("aac"))
        .build()
        .unwrap()
        .start()
        .unwrap();
    std::thread::sleep(Duration::from_millis(300));
    stop_with_watchdog(scheduler, 30, "aac_stop_mid_stream");
    assert_no_terminal_event(&log, "aac_stop_mid_stream");
}

/// Dropping the running scheduler (no explicit stop) takes the RunningGuard
/// path: same contract — clean exit, no terminal sink callback.
#[test]
fn drop_mid_stream_terminates_cleanly() {
    if !have_encoder("libx264") {
        eprintln!("skipping: libx264 not available in this FFmpeg build");
        return;
    }
    let _lock = PROCESS_LOCK.lock().unwrap_or_else(|e| e.into_inner());
    let (scheduler, log) = start_sink_job();
    std::thread::sleep(Duration::from_millis(200));
    let (tx, rx) = std::sync::mpsc::channel();
    std::thread::spawn(move || {
        drop(scheduler);
        let _ = tx.send(());
    });
    assert!(
        rx.recv_timeout(Duration::from_secs(30)).is_ok(),
        "dropping a running packet-sink job hung"
    );
    assert_no_terminal_event(&log, "drop_mid_stream");
}
