//! Job-failure summary end to end: a sibling output's failure reaches a
//! registered `on_job_failed` observer as a structured summary immediately
//! before the synthesized `JobFailed` terminal, while `wait()` keeps the
//! original error; a sink without the observer keeps the exact pre-summary
//! terminal shape.

mod common;

use common::wait_with_watchdog;
use ez_ffmpeg::error::{Error, PacketSinkError};
use ez_ffmpeg::packet_sink::{
    JobFailureKind, JobFailureSummary, PacketCallbackResult, PacketSink, PacketSinkHandler,
    PacketView,
};
use ez_ffmpeg::{FfmpegContext, Input, Output};
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant};

/// One recorded terminal-side observation.
#[derive(Clone, Debug)]
enum Ev {
    JobFailed {
        kind: JobFailureKind,
        message: String,
    },
    DeliveryError(String),
    End,
}

/// An audio job with two outputs: a healthy packet sink on input 0 and a
/// container sibling on input 1 whose write callback fails with EIO once a
/// little over 1 KiB was written (the tiny AVIO buffer guarantees the
/// failing flush happens mid-run). The sibling's failure is the job error;
/// the sink's own delivery stays clean.
///
/// `sink_delivered` orders the failure AFTER the sink's delivery started:
/// the caller's sink sets it from its first `on_packet`, and the sibling
/// holds its EIO until then. Unsynchronized, the EIO can be recorded
/// before the sink's configuration collection, tearing the sink down with
/// NO terminal callback (the documented pre-collection shape) — a
/// scheduling-dependent empty event log instead of the JobFailed terminal
/// these tests pin. The wait is bounded so a sink that never delivers
/// degrades to a loud test failure, not a hang.
fn run_with_failing_sibling(
    sink: PacketSink,
    sink_delivered: Arc<AtomicBool>,
    scenario: &str,
) -> Error {
    let mut written = 0usize;
    let sibling = Output::new_by_write_callback(move |buf: &[u8]| {
        written += buf.len();
        if written > 1024 {
            let deadline = Instant::now() + Duration::from_secs(60);
            while !sink_delivered.load(Ordering::Acquire) && Instant::now() < deadline {
                std::thread::sleep(Duration::from_millis(2));
            }
            ffmpeg_sys_next::AVERROR(ffmpeg_sys_next::EIO)
        } else {
            buf.len() as i32
        }
    })
    .set_format("mpegts")
    .set_audio_codec("aac")
    .set_io_buffer_size(512);

    let scheduler = FfmpegContext::builder()
        .input(Input::from("sine=frequency=440:duration=2").set_format("lavfi"))
        .input(Input::from("sine=frequency=330:duration=2").set_format("lavfi"))
        .output(
            Output::from(sink)
                .set_audio_codec("aac")
                .add_stream_map("0:a"),
        )
        .output(sibling.add_stream_map("1:a"))
        .build()
        .unwrap()
        .start()
        .unwrap();
    let result = wait_with_watchdog(scheduler, 120, scenario);
    result.expect_err("the sibling write failure must fail the job")
}

/// (b) Registered observer: the sibling failure produces exactly one
/// structured summary (kind `Mux`, message == the job error's Display
/// output) immediately before exactly one `JobFailed` terminal, and never
/// an `on_end`.
#[test]
fn sibling_failure_reaches_on_job_failed_with_a_mux_summary() {
    let log: Arc<Mutex<Vec<Ev>>> = Arc::new(Mutex::new(Vec::new()));
    let delivered = Arc::new(AtomicBool::new(false));
    let delivered_cb = delivered.clone();
    let (jf_log, end_log, err_log) = (log.clone(), log.clone(), log.clone());
    let sink = PacketSink::builder(move |_pkt: &PacketView<'_>| {
        delivered_cb.store(true, Ordering::Release);
        Ok(())
    })
        .on_job_failed(move |summary: &JobFailureSummary| {
            jf_log.lock().unwrap().push(Ev::JobFailed {
                kind: summary.kind(),
                message: summary.message().to_string(),
            });
        })
        .on_end(move || end_log.lock().unwrap().push(Ev::End))
        .on_delivery_error(move |e| {
            let message = match e {
                PacketSinkError::JobFailed { message } => message.clone(),
                other => format!("unexpected terminal: {other}"),
            };
            err_log.lock().unwrap().push(Ev::DeliveryError(message));
        })
        .build();

    let job_err = run_with_failing_sibling(sink, delivered, "job_failure_summary_registered");
    assert!(
        matches!(job_err, Error::Muxing(_)),
        "the sibling's muxing failure must stay the job result: {job_err:?}"
    );

    let events = log.lock().unwrap().clone();
    assert_eq!(
        events.len(),
        2,
        "exactly one observer + one terminal event: {events:?}"
    );
    match &events[0] {
        Ev::JobFailed { kind, message } => {
            assert_eq!(*kind, JobFailureKind::Mux);
            // Byte identity with the authoritative job error.
            assert_eq!(*message, job_err.to_string());
        }
        other => panic!("the observer must fire first, got {other:?}"),
    }
    match &events[1] {
        Ev::DeliveryError(message) => assert_eq!(*message, job_err.to_string()),
        other => panic!("the JobFailed terminal must follow, got {other:?}"),
    }
}

/// Handler shape of (b): the same sibling failure reaches an overridden
/// `PacketSinkHandler::on_job_failed` with the structured summary (kind
/// `Mux`, message == the job error's Display output) exactly once,
/// immediately before the synthesized `JobFailed` terminal, and never an
/// `on_end` — while `wait()` keeps the original error.
#[test]
fn sibling_failure_reaches_the_handler_on_job_failed_override() {
    struct RecordingHandler {
        log: Arc<Mutex<Vec<Ev>>>,
        delivered: Arc<AtomicBool>,
    }
    impl PacketSinkHandler for RecordingHandler {
        fn on_packet(&mut self, _packet: &PacketView<'_>) -> PacketCallbackResult {
            self.delivered.store(true, Ordering::Release);
            Ok(())
        }
        fn on_end(&mut self) {
            self.log.lock().unwrap().push(Ev::End);
        }
        fn on_job_failed(&mut self, summary: &JobFailureSummary) {
            self.log.lock().unwrap().push(Ev::JobFailed {
                kind: summary.kind(),
                message: summary.message().to_string(),
            });
        }
        fn on_delivery_error(&mut self, error: &PacketSinkError) {
            let message = match error {
                PacketSinkError::JobFailed { message } => message.clone(),
                other => format!("unexpected terminal: {other}"),
            };
            self.log.lock().unwrap().push(Ev::DeliveryError(message));
        }
    }

    let log: Arc<Mutex<Vec<Ev>>> = Arc::new(Mutex::new(Vec::new()));
    let delivered = Arc::new(AtomicBool::new(false));
    let sink = PacketSink::from_handler(RecordingHandler {
        log: log.clone(),
        delivered: delivered.clone(),
    });

    let job_err = run_with_failing_sibling(sink, delivered, "job_failure_summary_handler");
    assert!(
        matches!(job_err, Error::Muxing(_)),
        "the sibling's muxing failure must stay the job result: {job_err:?}"
    );

    let events = log.lock().unwrap().clone();
    assert_eq!(
        events.len(),
        2,
        "exactly one observer + one terminal event: {events:?}"
    );
    match &events[0] {
        Ev::JobFailed { kind, message } => {
            assert_eq!(*kind, JobFailureKind::Mux);
            // Byte identity with the authoritative job error.
            assert_eq!(*message, job_err.to_string());
        }
        other => panic!("the handler override must fire first, got {other:?}"),
    }
    match &events[1] {
        Ev::DeliveryError(message) => assert_eq!(*message, job_err.to_string()),
        other => panic!("the JobFailed terminal must follow, got {other:?}"),
    }
}

/// (b) Unregistered observer: the same failing job keeps the exact
/// pre-summary terminal shape — one `JobFailed` delivery error carrying the
/// job error's Display output, no `on_end`, nothing else.
#[test]
fn unregistered_on_job_failed_keeps_the_baseline_terminal_shape() {
    let log: Arc<Mutex<Vec<Ev>>> = Arc::new(Mutex::new(Vec::new()));
    let delivered = Arc::new(AtomicBool::new(false));
    let delivered_cb = delivered.clone();
    let (end_log, err_log) = (log.clone(), log.clone());
    let sink = PacketSink::builder(move |_pkt: &PacketView<'_>| {
        delivered_cb.store(true, Ordering::Release);
        Ok(())
    })
        .on_end(move || end_log.lock().unwrap().push(Ev::End))
        .on_delivery_error(move |e| {
            let message = match e {
                PacketSinkError::JobFailed { message } => message.clone(),
                other => format!("unexpected terminal: {other}"),
            };
            err_log.lock().unwrap().push(Ev::DeliveryError(message));
        })
        .build();

    let job_err = run_with_failing_sibling(sink, delivered, "job_failure_summary_unregistered");
    assert!(matches!(job_err, Error::Muxing(_)));

    let events = log.lock().unwrap().clone();
    assert_eq!(events.len(), 1, "exactly the terminal event: {events:?}");
    match &events[0] {
        Ev::DeliveryError(message) => assert_eq!(*message, job_err.to_string()),
        other => panic!("expected the JobFailed terminal, got {other:?}"),
    }
}
