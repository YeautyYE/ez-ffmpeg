//! Packet-sink teardown: stop()/drop() mid-stream must terminate cleanly —
//! no `on_end` (packets were still in flight), no hang, no leak — riding the
//! same delayed-start machinery `tests/mux_teardown.rs` sweeps for container
//! outputs.

mod common;

use common::{have_encoder, recording_sink, SinkEv};
use ez_ffmpeg::core::scheduler::ffmpeg_scheduler::Running;
use ez_ffmpeg::{FfmpegContext, FfmpegScheduler, Input, Output};
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::Mutex;
use std::time::Duration;

/// The sweep delays are only meaningful when the scenarios do not compete for
/// CPU inside the shared test binary: serialize them.
static PROCESS_LOCK: Mutex<()> = Mutex::new(());

/// Abort-store observation latch. While `WATCH_ARMED`, a container muxer's
/// abort-specific records set `ABORT_OBSERVED`: "Muxer detected abort" and
/// "Muxer skipping trailer due to abort" are each logged strictly after an
/// acquire load of the scheduler status returned the ABORT value on that
/// worker's thread (the trailer gate logs the latter on every abort-path
/// container-mux teardown, and abort is never downgraded). The store being
/// waited for therefore happens-before the latch, so a release chained
/// through it is ordered after the store — an observed handshake, not a
/// timing guess. A packet sink's terminal path returns before the trailer
/// gate, so only a container output can emit the records.
static WATCH_ARMED: AtomicBool = AtomicBool::new(false);
static ABORT_OBSERVED: AtomicBool = AtomicBool::new(false);

/// Whether the held packet was released by the acknowledged handshake (the
/// controller's `abort_done` send, which happens only after `ABORT_OBSERVED`
/// latched) rather than by the callback's failsafe timer. A timer release
/// proves nothing about ordering — the packet would go free while the abort
/// store might still be unpublished — so the test asserts this flag instead
/// of trusting the timeout arithmetic.
static RELEASED_BY_HANDSHAKE: AtomicBool = AtomicBool::new(false);

struct MuxAbortWatchLogger;

impl log::Log for MuxAbortWatchLogger {
    fn enabled(&self, _: &log::Metadata) -> bool {
        true
    }

    fn log(&self, record: &log::Record) {
        if !WATCH_ARMED.load(Ordering::Acquire) {
            return;
        }
        let msg = record.args().to_string();
        if msg.contains("Muxer detected abort")
            || msg.contains("Muxer skipping trailer due to abort")
        {
            ABORT_OBSERVED.store(true, Ordering::Release);
        }
    }

    fn flush(&self) {}
}

fn install_watch_logger_once() {
    static INSTALLED: std::sync::OnceLock<()> = std::sync::OnceLock::new();
    INSTALLED.get_or_init(|| {
        log::set_boxed_logger(Box::new(MuxAbortWatchLogger)).expect("logger installs once");
        // The muxer abort records are debug level.
        log::set_max_level(log::LevelFilter::Debug);
    });
}

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
        !events.iter().any(|e| matches!(e, SinkEv::Error { .. })),
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

/// The channel wedge case: capacity 1, a LIVE receiver that never drains,
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

/// The abort-window case: abort() landing while packets are still
/// being delivered must never let a terminal callback fire — the terminal
/// decision reads the status fresh, after the job settled, not from a
/// pre-wait snapshot. An ARBITRARY mid-stream packet (the 10th of ~40) is
/// held until a sibling container muxer (fed by a second, infinite input)
/// OBSERVES the abort store — its abort-specific teardown record latches
/// `ABORT_OBSERVED` — so the store happens-before the packet's release: an
/// acknowledged handshake. Native AAC.
#[test]
fn abort_during_final_delivery_fires_no_terminal_callback() {
    let _lock = PROCESS_LOCK.lock().unwrap_or_else(|e| e.into_inner());
    install_watch_logger_once();
    WATCH_ARMED.store(false, Ordering::Release);
    ABORT_OBSERVED.store(false, Ordering::Release);
    RELEASED_BY_HANDSHAKE.store(false, Ordering::Release);
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
            // observe it. The release is GATED on the controller's signal
            // (sent only after `ABORT_OBSERVED` latched); the timeout is a
            // failsafe for a wrecked run and sits deliberately far above the
            // controller's 30s observation deadline. A failsafe below that
            // deadline would let this packet go free on the timer while the
            // controller still polls, silently voiding the happens-before
            // edge under test. The failsafe also never delays a failing run:
            // the observation poll's panic drops `abort_done_tx`, which
            // releases this recv immediately (Disconnected).
            let _ = abort_request_tx.send(());
            let released_by_signal = abort_done_rx
                .recv_timeout(Duration::from_secs(90))
                .is_ok();
            RELEASED_BY_HANDSHAKE.store(released_by_signal, Ordering::Release);
        }
        Ok(())
    })
    .on_end(move || {
        end_log.lock().unwrap().push(SinkEv::End {
            thread: std::thread::current().id(),
        })
    })
    .on_delivery_error(move |e| {
        err_log.lock().unwrap().push(SinkEv::Error {
            message: e.to_string(),
            thread: std::thread::current().id(),
        })
    })
    .build();

    // The second chain is INFINITE and realtime-paced: its container muxer
    // is guaranteed alive whenever the abort lands, and its teardown always
    // crosses the trailer gate, whose abort branch acknowledges the store.
    let scheduler = FfmpegContext::builder()
        .input(Input::from("sine=frequency=440:duration=1").set_format("lavfi"))
        .input(
            Input::from("sine=frequency=330")
                .set_format("lavfi")
                .set_readrate(1.0),
        )
        .output(
            Output::from(sink)
                .set_audio_codec("aac")
                .add_stream_map("0:a"),
        )
        .output(
            Output::from("abort-probe.null")
                .set_format("null")
                .set_audio_codec("pcm_s16le")
                .add_stream_map("1:a"),
        )
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
    // the held packet is released only after the sibling muxer's abort
    // record proves that store is published (see the latch's doc comment),
    // so the terminal region's fresh load is ordered after it.
    WATCH_ARMED.store(true, Ordering::Release);
    // abort() returns () — the channel carries completion, giving join a
    // bound: a teardown regression after the observation record must FAIL
    // the recv_timeout below, not hang the suite in an unbounded join.
    let (abort_completed_tx, abort_completed_rx) = std::sync::mpsc::channel();
    let abort_thread = std::thread::spawn(move || {
        scheduler.abort();
        let _ = abort_completed_tx.send(());
    });
    let deadline = std::time::Instant::now() + Duration::from_secs(30);
    while !ABORT_OBSERVED.load(Ordering::Acquire) {
        assert!(
            std::time::Instant::now() < deadline,
            "no worker ever observed the abort store"
        );
        std::thread::sleep(Duration::from_millis(2));
    }
    WATCH_ARMED.store(false, Ordering::Release);
    let _ = abort_done_tx.send(());
    // 120 s sits comfortably above the callback's own 90 s failsafe, so a
    // hang detected here is a real teardown wedge, not the failsafe firing.
    abort_completed_rx
        .recv_timeout(Duration::from_secs(120))
        .expect("abort() never returned after the held packet was released");
    abort_thread
        .join()
        .expect("abort() must return once the job tears down");
    // abort() returns only after the delivering worker exited, so the
    // callback's store is visible here: the release must have come from the
    // handshake, or the ordering the assertions below rely on never held.
    assert!(
        RELEASED_BY_HANDSHAKE.load(Ordering::Acquire),
        "the held packet was released by the failsafe timer, not by the \
         observed-abort handshake"
    );
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
