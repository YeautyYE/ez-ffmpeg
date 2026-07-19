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
