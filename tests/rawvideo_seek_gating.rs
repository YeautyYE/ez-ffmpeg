//! Regression coverage for the rawvideo-callback seek gate in
//! `open_input_file` (H1).
//!
//! A read-callback input has no seek callback, so `open_input_file` decides
//! whether the format needs one. For a rawvideo elementary stream the demuxer
//! is purely sequential and never seeks; the generic `input_requires_seek`
//! probe would nonetheless run a destructive seek test that reads (and
//! discards) frames through the callback. For a live push source — a frame
//! producer that only exists once `open()` returns — that probe blocks `open()`
//! forever.
//!
//! The gate skips the probe for rawvideo and rejects the input with a
//! deterministic `SeekFunctionMissing` ONLY when the caller asked for a
//! reposition (`start_time_us` / non-zero `stream_loop`) that cannot be honored
//! without a seek callback. Every scenario bounds `build()` on a side thread:
//! reverting the gate makes the blocking callback stall `build()`, which the
//! watchdog turns into a failure instead of a suite-wide hang.

use ez_ffmpeg::error::{Error, OpenInputError};
use ez_ffmpeg::{FfmpegContext, Input, Output};
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::Arc;
use std::time::Duration;

mod common;
use common::tmp_path_in;

fn tmp_path(name: &str) -> String {
    tmp_path_in("ez_ffmpeg_rawvideo_seek_gating", name)
}

/// FFmpeg 7.x reads the stream head inside `avformat_open_input` to probe for an
/// ID3v2 tag (`id3v2_read_internal` → `ffio_ensure_seekback`), which blocks a
/// rawvideo push callback that has no data yet. That probe became a per-demuxer
/// opt-in only in FFmpeg 8.0 (libavformat major 62), so the callback-driven
/// scenarios below run on 8.0+ and are skipped under it.
fn rawvideo_callback_open_supported() -> bool {
    (unsafe { ffmpeg_sys_next::avformat_version() } >> 16) >= 62
}

macro_rules! skip_unless_supported {
    () => {
        if !rawvideo_callback_open_supported() {
            eprintln!("skipping: rawvideo push callback requires FFmpeg 8.0+ (libavformat >= 62)");
            return;
        }
    };
}

/// Read callback that records whether FFmpeg ever invoked it and then blocks,
/// standing in for a live push source that has produced no frame yet. On the
/// fixed code path `build()` never calls it (rawvideo `read_header` reads only
/// options); on the reverted path the destructive seek probe calls it and
/// stalls, which the watchdog below reports.
fn blocking_callback() -> (
    Arc<AtomicBool>,
    impl FnMut(&mut [u8]) -> i32 + Send + 'static,
) {
    let called = Arc::new(AtomicBool::new(false));
    let flag = called.clone();
    let cb = move |_buf: &mut [u8]| -> i32 {
        flag.store(true, Ordering::SeqCst);
        // Simulate a live source with no frame available. The deadline in
        // `build_within` fires long before this returns.
        std::thread::sleep(Duration::from_secs(30));
        0
    };
    (called, cb)
}

/// A rawvideo read-callback input matching exactly what the frame-push bridge
/// assembles: known format + dimensions, probing disabled so `open()` reads no
/// data.
fn rawvideo_callback_input(cb: impl FnMut(&mut [u8]) -> i32 + Send + 'static) -> Input {
    Input::new_by_read_callback(cb)
        .set_format("rawvideo")
        .set_format_opt("video_size", "64x48")
        .set_format_opt("pixel_format", "rgba")
        .set_format_opt("framerate", "30")
        .set_find_stream_info(false)
}

/// Runs `build()` on a side thread and turns a stall into a named failure
/// instead of hanging the whole suite. Returns the build result on success.
fn build_within(
    secs: u64,
    scenario: &str,
    job: impl FnOnce() -> ez_ffmpeg::error::Result<FfmpegContext> + Send + 'static,
) -> ez_ffmpeg::error::Result<FfmpegContext> {
    let (tx, rx) = std::sync::mpsc::channel();
    std::thread::spawn(move || {
        let _ = tx.send(job());
    });
    match rx.recv_timeout(Duration::from_secs(secs)) {
        Ok(result) => result,
        Err(std::sync::mpsc::RecvTimeoutError::Timeout) => {
            panic!("scenario `{scenario}`: build() did not finish within {secs}s (hang)")
        }
        Err(std::sync::mpsc::RecvTimeoutError::Disconnected) => {
            panic!("scenario `{scenario}`: build() thread panicked before reporting")
        }
    }
}

fn is_seek_missing(err: &Error) -> bool {
    matches!(
        err,
        Error::OpenInputStream(OpenInputError::SeekFunctionMissing)
    )
}

/// `FfmpegContext` is not `Debug`, so `expect_err` is unavailable; match by hand.
fn assert_seek_missing(result: ez_ffmpeg::error::Result<FfmpegContext>, scenario: &str) {
    match result {
        Ok(_) => panic!("{scenario}: expected SeekFunctionMissing, got a built context"),
        Err(e) => assert!(is_seek_missing(&e), "{scenario}: unexpected error: {e:?}"),
    }
}

/// No reposition requested: `open()` must return without running the
/// destructive probe and without ever calling the (blocking) callback.
#[test]
fn rawvideo_callback_without_reposition_opens_without_probe() {
    skip_unless_supported!();
    let (called, cb) = blocking_callback();
    let out = tmp_path("passthrough.mp4");
    let ctx = build_within(8, "rawvideo passthrough", move || {
        FfmpegContext::builder()
            .input(rawvideo_callback_input(cb))
            .output(Output::from(out.as_str()).set_video_codec("mpeg4"))
            .build()
    });
    assert!(ctx.is_ok(), "build failed: {:?}", ctx.err());
    assert!(
        !called.load(Ordering::SeqCst),
        "read callback was invoked during open — the destructive seek probe ran"
    );
    // Drop the context without starting: closes the input, no scheduler joins.
    drop(ctx);
}

/// `stream_loop(0)` is not a reposition request, so the input still opens.
#[test]
fn rawvideo_callback_stream_loop_zero_opens() {
    skip_unless_supported!();
    let (called, cb) = blocking_callback();
    let out = tmp_path("loop_zero.mp4");
    let ctx = build_within(8, "rawvideo stream_loop=0", move || {
        FfmpegContext::builder()
            .input(rawvideo_callback_input(cb).set_stream_loop(0))
            .output(Output::from(out.as_str()).set_video_codec("mpeg4"))
            .build()
    });
    assert!(ctx.is_ok(), "build failed: {:?}", ctx.err());
    assert!(!called.load(Ordering::SeqCst));
}

/// `start_time_us` on a seekless rawvideo callback input is unhonorable: a
/// deterministic `SeekFunctionMissing`, not a silent wrong-output.
#[test]
fn rawvideo_callback_start_time_returns_seek_missing() {
    skip_unless_supported!();
    let (_called, cb) = blocking_callback();
    let out = tmp_path("start_time.mp4");
    let result = build_within(8, "rawvideo start_time", move || {
        FfmpegContext::builder()
            .input(rawvideo_callback_input(cb).set_start_time_us(1_000_000))
            .output(Output::from(out.as_str()).set_video_codec("mpeg4"))
            .build()
    });
    assert_seek_missing(result, "rawvideo start_time");
}

/// A finite positive `stream_loop` needs a rewind the input cannot perform.
#[test]
fn rawvideo_callback_stream_loop_positive_returns_seek_missing() {
    skip_unless_supported!();
    let (_called, cb) = blocking_callback();
    let out = tmp_path("loop_positive.mp4");
    let result = build_within(8, "rawvideo stream_loop=2", move || {
        FfmpegContext::builder()
            .input(rawvideo_callback_input(cb).set_stream_loop(2))
            .output(Output::from(out.as_str()).set_video_codec("mpeg4"))
            .build()
    });
    assert_seek_missing(result, "rawvideo stream_loop=2");
}

/// Infinite `stream_loop` (`-1`) is a reposition request too.
#[test]
fn rawvideo_callback_stream_loop_infinite_returns_seek_missing() {
    skip_unless_supported!();
    let (_called, cb) = blocking_callback();
    let out = tmp_path("loop_infinite.mp4");
    let result = build_within(8, "rawvideo stream_loop=-1", move || {
        FfmpegContext::builder()
            .input(rawvideo_callback_input(cb).set_stream_loop(-1))
            .output(Output::from(out.as_str()).set_video_codec("mpeg4"))
            .build()
    });
    assert_seek_missing(result, "rawvideo stream_loop=-1");
}
