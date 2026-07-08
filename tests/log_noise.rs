//! Regression net for log noise: normal-path pipeline runs must not emit
//! WARN or ERROR logs (neither from ez-ffmpeg itself nor forwarded from FFmpeg).
//!
//! Scenarios mirror real-world "false error" reports: screenshot via
//! max_frames=1, clip via recording_time, graceful stop(), abort(), and a
//! plain full-EOF transcode as control.

use ez_ffmpeg::{FfmpegContext, Input, Output};
use log::{Level, LevelFilter, Metadata, Record};
use std::sync::{Mutex, OnceLock};
use std::time::Duration;

struct Recorder {
    entries: Mutex<Vec<(Level, String, String)>>,
}

impl log::Log for Recorder {
    fn enabled(&self, metadata: &Metadata) -> bool {
        metadata.level() <= Level::Warn
    }

    fn log(&self, record: &Record) {
        if record.level() <= Level::Warn {
            let msg = record.args().to_string();
            // Benign, self-correcting FFmpeg notice: with threads=auto (the
            // CLI-parity default the encoder now uses), a slice-threaded encoder
            // like mpeg4 caps the thread count on a many-core host and encodes
            // fine. It is machine-dependent (never fires on few-core CI) and is
            // not the kind of "false error" this net guards against.
            if msg.contains("too many threads/slices") {
                return;
            }
            self.entries
                .lock()
                .unwrap()
                .push((record.level(), record.target().to_string(), msg));
        }
    }

    fn flush(&self) {}
}

static RECORDER: Recorder = Recorder {
    entries: Mutex::new(Vec::new()),
};

/// Serializes tests: the recorder is process-global, so scenarios must not overlap.
static TEST_LOCK: Mutex<()> = Mutex::new(());

/// A failed (panicked) test poisons the lock; later tests still need to run.
fn test_guard() -> std::sync::MutexGuard<'static, ()> {
    TEST_LOCK
        .lock()
        .unwrap_or_else(|poisoned| poisoned.into_inner())
}

fn init_logging() {
    static INIT: OnceLock<()> = OnceLock::new();
    INIT.get_or_init(|| {
        log::set_logger(&RECORDER).expect("set test logger");
        log::set_max_level(LevelFilter::Warn);
    });
}

fn clear_recorded() {
    RECORDER.entries.lock().unwrap().clear();
}

fn recorded() -> Vec<(Level, String, String)> {
    RECORDER.entries.lock().unwrap().clone()
}

/// Detached worker threads may log shortly after wait()/stop() returns.
/// Wait until the recorder has been quiet for a while (no new entries for
/// 300ms, capped at 2s) so stragglers are caught by the assertion without
/// racing the next test under CI load.
fn settle() {
    let mut last_len = recorded().len();
    let mut quiet_for = 0u32;
    for _ in 0..20 {
        std::thread::sleep(Duration::from_millis(100));
        let len = recorded().len();
        if len == last_len {
            quiet_for += 1;
            if quiet_for >= 3 {
                return;
            }
        } else {
            last_len = len;
            quiet_for = 0;
        }
    }
}

fn assert_no_noise(scenario: &str) {
    settle();
    let entries = recorded();
    assert!(
        entries.is_empty(),
        "scenario `{scenario}` emitted {} WARN/ERROR log(s):\n{}",
        entries.len(),
        entries
            .iter()
            .map(|(lvl, target, msg)| format!("  [{lvl}] {target}: {msg}"))
            .collect::<Vec<_>>()
            .join("\n")
    );
}

fn tmp_path(name: &str) -> String {
    let dir =
        std::env::temp_dir().join(format!("ez_ffmpeg_log_noise_tests_{}", std::process::id()));
    std::fs::create_dir_all(&dir).unwrap();
    dir.join(name).to_string_lossy().into_owned()
}

fn lavfi_input() -> Input {
    Input::from("color=c=black:s=320x240:r=30").set_format("lavfi")
}

#[test]
fn normal_eof_transcode_emits_no_warn_or_error() {
    init_logging();
    let _guard = test_guard();

    // Fixture: a short finite file, produced before log capture starts.
    let fixture = tmp_path("fixture_eof.mp4");
    FfmpegContext::builder()
        .input(lavfi_input())
        .output(
            Output::from(fixture.as_str())
                .set_video_codec("mpeg4")
                .set_recording_time_us(500_000),
        )
        .build()
        .unwrap()
        .start()
        .unwrap()
        .wait()
        .unwrap();

    clear_recorded();

    let out = tmp_path("normal_eof_out.mp4");
    let result = FfmpegContext::builder()
        .input(Input::from(fixture.as_str()))
        .output(Output::from(out.as_str()).set_video_codec("mpeg4"))
        .build()
        .unwrap()
        .start()
        .unwrap()
        .wait();

    assert!(result.is_ok(), "normal EOF transcode failed: {result:?}");
    assert_no_noise("normal EOF transcode");
}

#[test]
fn max_frames_screenshot_emits_no_warn_or_error() {
    init_logging();
    let _guard = test_guard();
    clear_recorded();

    let out = tmp_path("screenshot.png");
    let result = FfmpegContext::builder()
        .input(lavfi_input())
        .output(Output::from(out.as_str()).set_max_video_frames(1))
        .build()
        .unwrap()
        .start()
        .unwrap()
        .wait();

    assert!(result.is_ok(), "screenshot task failed: {result:?}");
    assert!(
        std::fs::metadata(&out)
            .map(|m| m.len() > 0)
            .unwrap_or(false),
        "screenshot output missing or empty"
    );
    assert_no_noise("max_frames=1 screenshot");
}

#[test]
fn pattern_filename_screenshot_keeps_sequence_naming() {
    init_logging();
    let _guard = test_guard();
    clear_recorded();

    // A '%03d' sequence pattern must NOT trigger the single-image 'update'
    // mode: image2 should keep expanding the pattern (shot_001.png).
    let pattern = tmp_path("shot_%03d.png");
    let expanded = tmp_path("shot_001.png");
    let _ = std::fs::remove_file(&expanded);
    let _ = std::fs::remove_file(&pattern);

    let result = FfmpegContext::builder()
        .input(lavfi_input())
        .output(Output::from(pattern.as_str()).set_max_video_frames(1))
        .build()
        .unwrap()
        .start()
        .unwrap()
        .wait();

    assert!(result.is_ok(), "pattern screenshot task failed: {result:?}");
    assert!(
        std::fs::metadata(&expanded)
            .map(|m| m.len() > 0)
            .unwrap_or(false),
        "expected pattern-expanded output shot_001.png to exist"
    );
    assert!(
        std::fs::metadata(&pattern).is_err(),
        "a literal 'shot_%03d.png' file must not be created"
    );
    assert_no_noise("pattern filename screenshot");
}

#[test]
fn recording_time_clip_emits_no_warn_or_error() {
    init_logging();
    let _guard = test_guard();
    clear_recorded();

    let out = tmp_path("clip.mp4");
    let result = FfmpegContext::builder()
        .input(lavfi_input())
        .output(
            Output::from(out.as_str())
                .set_video_codec("mpeg4")
                .set_recording_time_us(300_000),
        )
        .build()
        .unwrap()
        .start()
        .unwrap()
        .wait();

    assert!(result.is_ok(), "clip task failed: {result:?}");
    assert_no_noise("recording_time clip");
}

#[test]
fn graceful_stop_emits_no_warn_or_error() {
    init_logging();
    let _guard = test_guard();
    clear_recorded();

    let out = tmp_path("stopped.mp4");
    let scheduler = FfmpegContext::builder()
        .input(lavfi_input())
        .output(Output::from(out.as_str()).set_video_codec("mpeg4"))
        .build()
        .unwrap()
        .start()
        .unwrap();

    std::thread::sleep(Duration::from_millis(200));
    scheduler.stop();

    assert_no_noise("graceful stop()");
}

#[test]
fn abort_emits_no_warn_or_error() {
    init_logging();
    let _guard = test_guard();
    clear_recorded();

    let out = tmp_path("aborted.mp4");
    let scheduler = FfmpegContext::builder()
        .input(lavfi_input())
        .output(Output::from(out.as_str()).set_video_codec("mpeg4"))
        .build()
        .unwrap()
        .start()
        .unwrap();

    std::thread::sleep(Duration::from_millis(200));
    scheduler.abort();

    // abort() returns immediately; detached threads wind down on their own.
    assert_no_noise("abort()");
}
