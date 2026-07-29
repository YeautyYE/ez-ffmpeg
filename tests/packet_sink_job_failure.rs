//! Job-failure summary end to end: a sibling output's failure reaches a
//! registered `on_job_failed` observer as a structured summary immediately
//! before the synthesized `JobFailed` terminal, while `wait()` keeps the
//! original error; a sink without the observer keeps the exact pre-summary
//! terminal shape.

mod common;

use common::{tmp_path_in, wait_with_watchdog};
use ez_ffmpeg::error::{Error, MuxingOperationError, PacketSinkError};
use ez_ffmpeg::filter::frame_filter::{FrameFilter, FrameFilterError, RequestFrameMode};
use ez_ffmpeg::filter::frame_filter_context::FrameFilterContext;
use ez_ffmpeg::filter::frame_pipeline_builder::FramePipelineBuilder;
use ez_ffmpeg::packet_sink::{
    JobFailureKind, JobFailureSummary, PacketCallbackResult, PacketSink, PacketSinkHandler,
    PacketView,
};
use ez_ffmpeg::{AVMediaType, FfmpegContext, Frame, Input, Output};
use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};
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

fn custom_input_fixture() -> Vec<u8> {
    let path = tmp_path_in(
        "ez_ffmpeg_packet_sink_job_failure_tests",
        "custom_input_worker_panic.ts",
    );
    let scheduler = FfmpegContext::builder()
        .input(Input::from("sine=frequency=440:duration=20").set_format("lavfi"))
        .output(
            Output::from(path.as_str())
                .set_format("mpegts")
                .set_audio_codec("aac"),
        )
        .build()
        .unwrap()
        .start()
        .unwrap();
    wait_with_watchdog(scheduler, 60, "custom_input_fixture").unwrap();
    std::fs::read(path).unwrap()
}

#[test]
fn custom_read_panic_after_build_fails_the_running_job() {
    let bytes = Arc::new(custom_input_fixture());
    let position = Arc::new(Mutex::new(0usize));
    let armed = Arc::new(AtomicBool::new(false));
    let panic_calls = Arc::new(AtomicUsize::new(0));

    let read_bytes = bytes.clone();
    let read_position = position.clone();
    let read_armed = armed.clone();
    let read_panic_calls = panic_calls.clone();
    let input = Input::new_by_read_callback(move |buf: &mut [u8]| -> i32 {
        if read_armed.load(Ordering::Acquire) {
            read_panic_calls.fetch_add(1, Ordering::AcqRel);
            panic!("test-injected custom read panic");
        }
        let mut position = read_position.lock().unwrap();
        if *position == read_bytes.len() {
            return ffmpeg_sys_next::AVERROR_EOF;
        }
        let len = buf.len().min(read_bytes.len() - *position);
        buf[..len].copy_from_slice(&read_bytes[*position..*position + len]);
        *position += len;
        len as i32
    })
    .set_format("mpegts")
    .set_io_buffer_size(512);

    let sink = PacketSink::builder(|_packet: &PacketView<'_>| Ok(())).build();
    let context = FfmpegContext::builder()
        .input(input)
        .output(Output::from(sink).set_audio_codec("aac"))
        .build()
        .unwrap();
    assert!(
        *position.lock().unwrap() < bytes.len(),
        "the fixture must retain worker-side reads after probing"
    );

    armed.store(true, Ordering::Release);
    let scheduler = context.start().unwrap();
    let result = wait_with_watchdog(scheduler, 60, "custom_read_worker_panic");
    assert!(
        matches!(result, Err(Error::Demuxing(_))),
        "the contained read panic must remain a demuxing job error: {result:?}"
    );
    assert_eq!(
        panic_calls.load(Ordering::Acquire),
        1,
        "a poisoned custom input must not re-enter the callback"
    );
}

#[derive(Default)]
struct SeekableOutput {
    bytes: Vec<u8>,
    position: usize,
}

struct HoldVideoUntilArmed(Arc<AtomicBool>);

impl FrameFilter for HoldVideoUntilArmed {
    fn media_type(&self) -> AVMediaType {
        AVMediaType::AVMEDIA_TYPE_VIDEO
    }

    fn filter_frame(
        &mut self,
        frame: Frame,
        _ctx: &mut FrameFilterContext,
    ) -> Result<Option<Frame>, FrameFilterError> {
        while !self.0.load(Ordering::Acquire) {
            std::thread::sleep(Duration::from_millis(1));
        }
        Ok(Some(frame))
    }

    fn request_frame_mode(&self) -> RequestFrameMode {
        RequestFrameMode::Never
    }
}

#[test]
fn final_custom_output_seek_panic_is_a_trailer_error() {
    let output_state = Arc::new(Mutex::new(SeekableOutput::default()));
    let probe_armed = Arc::new(AtomicBool::new(false));
    let panic_calls = Arc::new(AtomicUsize::new(0));

    let write_state = output_state.clone();
    let output = Output::new_by_write_callback(move |buf: &[u8]| -> i32 {
        let mut state = write_state.lock().unwrap();
        let start = state.position;
        let end = start + buf.len();
        if state.bytes.len() < end {
            state.bytes.resize(end, 0);
        }
        state.bytes[start..end].copy_from_slice(buf);
        state.position = end;
        buf.len() as i32
    });

    let seek_state = output_state.clone();
    let seek_armed = probe_armed.clone();
    let seek_panic_calls = panic_calls.clone();
    let output = output
        .set_seek_callback(move |offset: i64, whence: i32| -> i64 {
            let whence = whence & !ffmpeg_sys_next::AVSEEK_FORCE;
            if whence == ffmpeg_sys_next::AVSEEK_SIZE {
                let state = seek_state.lock().unwrap();
                return state.bytes.len() as i64;
            }
            if whence == ffmpeg_sys_next::SEEK_SET && seek_armed.load(Ordering::Acquire) {
                seek_panic_calls.fetch_add(1, Ordering::AcqRel);
                panic!("test-injected final seek panic");
            }

            let mut state = seek_state.lock().unwrap();
            let target = match whence {
                ffmpeg_sys_next::SEEK_SET => offset,
                ffmpeg_sys_next::SEEK_CUR => state.position as i64 + offset,
                ffmpeg_sys_next::SEEK_END => state.bytes.len() as i64 + offset,
                _ => return ffmpeg_sys_next::AVERROR(ffmpeg_sys_next::ESPIPE) as i64,
            };
            if target < 0 {
                return ffmpeg_sys_next::AVERROR(ffmpeg_sys_next::ESPIPE) as i64;
            }
            state.position = target as usize;
            target
        })
        .set_format("mp4")
        .set_video_codec("mpeg4")
        .set_io_buffer_size(4096);

    let gate = FramePipelineBuilder::new(AVMediaType::AVMEDIA_TYPE_VIDEO).filter(
        "hold-until-progress-observed",
        Box::new(HoldVideoUntilArmed(probe_armed.clone())),
    );
    let scheduler = FfmpegContext::builder()
        .input(
            Input::from("color=c=blue:s=320x240:r=30:d=2")
                .set_format("lavfi")
                .add_frame_pipeline(gate),
        )
        .output(output)
        .build()
        .unwrap()
        .start()
        .unwrap();
    probe_armed.store(true, Ordering::Release);

    let result = wait_with_watchdog(scheduler, 60, "final_custom_output_seek_panic");
    assert!(
        matches!(
            result,
            Err(Error::Muxing(MuxingOperationError::TrailerWriteError(_)))
        ),
        "the contained final seek panic must be a trailer error: {result:?}"
    );
    assert_eq!(
        panic_calls.load(Ordering::Acquire),
        1,
        "a poisoned custom output must not re-enter the seek callback"
    );
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
