//! Regression net for the decoder's ignored-error handling.
//!
//! Background: an ignored decode error must NOT end the drain early — a
//! frame-threaded decoder may still have buffered frames to deliver, and a
//! `null`-packet flush must still reach `AVERROR_EOF` so the caller runs
//! `avcodec_flush_buffers`. So the drain keeps going on an ignored error. Only a
//! decoder that is genuinely STUCK — returning errors without advancing
//! `frame_num`, e.g. one that does not consume input on error — is bounded (by a
//! progress-aware budget) so it cannot spin forever, and the drain also checks
//! for a stop request each iteration so it stays interruptible.
//!
//! The exact stuck-decoder trigger (a persistent no-progress receive error) is
//! decoder-specific and not portably reproducible from the public API, so these
//! tests pin the surrounding, reachable contract instead:
//!   * with error-exit off (the default), a corrupt stream is TOLERATED and the
//!     job COMPLETES — the watchdog turns any hang into a test failure; and
//!   * with `set_exit_on_error(true)`, the decode error is SURFACED.
//! Every decode below also drives the drain loop's in-loop stop check.

use ez_ffmpeg::filter::frame_filter::{FrameFilter, FrameFilterError};
use ez_ffmpeg::filter::frame_filter_context::FrameFilterContext;
use ez_ffmpeg::filter::frame_pipeline_builder::FramePipelineBuilder;
use ez_ffmpeg::{AVMediaType, FfmpegContext, FfmpegScheduler, Frame, Input, Output};
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::Arc;
use std::time::Duration;

fn tmp_path(name: &str) -> String {
    let dir = std::env::temp_dir().join(format!(
        "ez_ffmpeg_decode_error_recovery_{}",
        std::process::id()
    ));
    std::fs::create_dir_all(&dir).unwrap();
    dir.join(name).to_string_lossy().into_owned()
}

/// Runs `wait()` on a side thread so a decoder that fails to terminate surfaces
/// here as a test failure instead of stalling the whole suite.
fn wait_with_watchdog(
    scheduler: FfmpegScheduler<ez_ffmpeg::core::scheduler::ffmpeg_scheduler::Running>,
    secs: u64,
    scenario: &str,
) -> ez_ffmpeg::error::Result<()> {
    let (tx, rx) = std::sync::mpsc::channel();
    std::thread::spawn(move || {
        let _ = tx.send(scheduler.wait());
    });
    match rx.recv_timeout(Duration::from_secs(secs)) {
        Ok(result) => result,
        Err(std::sync::mpsc::RecvTimeoutError::Timeout) => {
            panic!("scenario `{scenario}` did not finish within {secs}s (hang)")
        }
        Err(std::sync::mpsc::RecvTimeoutError::Disconnected) => {
            panic!("scenario `{scenario}`: wait() thread panicked before reporting")
        }
    }
}

/// Encodes a 1s mpeg4 raw elementary stream (`.m4v`). A raw ES is used on
/// purpose: the demuxer resyncs on VOP start codes, so mangled payload keeps
/// reaching the decoder (rather than failing to demux) — which is what drives
/// the decoder's error handling.
fn encode_m4v(name: &str) -> String {
    let path = tmp_path(name);
    let scheduler = FfmpegContext::builder()
        .input(Input::from("testsrc2=s=320x240:r=30").set_format("lavfi"))
        .output(
            Output::from(path.as_str())
                .set_format("m4v")
                .set_video_codec("mpeg4")
                .set_recording_time_us(1_000_000),
        )
        .build()
        .unwrap()
        .start()
        .unwrap();
    wait_with_watchdog(scheduler, 60, "encode m4v fixture").unwrap();
    path
}

/// Copies `src` to `dst`, flipping a byte every 7th position after `skip_head`
/// bytes. The head is preserved so the first keyframe still decodes; the rest
/// mangles payload while leaving enough start codes for the demuxer to resync.
fn corrupt_tail(src: &str, dst: &str, skip_head: usize) {
    let mut bytes = std::fs::read(src).unwrap();
    let mut i = skip_head.min(bytes.len());
    while i < bytes.len() {
        bytes[i] ^= 0xFF;
        i += 7;
    }
    std::fs::write(dst, &bytes).unwrap();
}

/// Counts decoded (post-decode) video frames.
struct FrameCounter {
    count: Arc<AtomicUsize>,
}

impl FrameFilter for FrameCounter {
    fn media_type(&self) -> AVMediaType {
        AVMediaType::AVMEDIA_TYPE_VIDEO
    }

    fn filter_frame(
        &mut self,
        frame: Frame,
        _ctx: &mut FrameFilterContext,
    ) -> Result<Option<Frame>, FrameFilterError> {
        unsafe {
            if !frame.as_ptr().is_null() && !frame.is_empty() {
                self.count.fetch_add(1, Ordering::SeqCst);
            }
        }
        Ok(Some(frame))
    }
}

fn m4v_input(src: &str) -> Input {
    Input::from(src).set_format("m4v")
}

/// Transcodes `input`, returning the decoded-frame count. `secs` bounds the run.
fn decode_frames(
    input: Input,
    out_name: &str,
    secs: u64,
    scenario: &str,
) -> ez_ffmpeg::error::Result<usize> {
    let count = Arc::new(AtomicUsize::new(0));
    let pipeline = FramePipelineBuilder::new(AVMediaType::AVMEDIA_TYPE_VIDEO).filter(
        "frame_counter",
        Box::new(FrameCounter {
            count: Arc::clone(&count),
        }),
    );
    let out = tmp_path(out_name);
    let scheduler = FfmpegContext::builder()
        .input(input.add_frame_pipeline(pipeline))
        .output(Output::from(out.as_str()).set_video_codec("mpeg4"))
        .build()?
        .start()?;
    wait_with_watchdog(scheduler, secs, scenario)?;
    Ok(count.load(Ordering::SeqCst))
}

#[test]
fn corrupt_stream_is_tolerated_and_completes_when_errors_are_ignored() {
    let clean = encode_m4v("clean.m4v");
    let clean_frames =
        decode_frames(m4v_input(&clean), "clean_out.mp4", 60, "clean baseline").unwrap();
    assert_eq!(
        clean_frames, 30,
        "baseline: the clean fixture decodes fully"
    );

    let corrupt = tmp_path("corrupt.m4v");
    corrupt_tail(&clean, &corrupt, 256);

    // Default error-exit is off: the decoder tolerates the errors and the job
    // COMPLETES (the watchdog fails the test on any hang), still decoding the
    // leading good frame(s) but fewer than the clean stream.
    let n = decode_frames(
        m4v_input(&corrupt),
        "corrupt_out.mp4",
        30,
        "ignored-error decode must complete, not hang",
    )
    .expect("ignored-error decode must complete without surfacing an error");
    assert!(n >= 1, "at least the leading keyframe decodes (got {n})");
    assert!(
        n < clean_frames,
        "corruption must drop frames vs the clean stream (got {n} of {clean_frames})"
    );
}

#[test]
fn corrupt_stream_is_surfaced_when_exit_on_error_is_true() {
    let clean = encode_m4v("clean_strict.m4v");
    let corrupt = tmp_path("corrupt_strict.m4v");
    corrupt_tail(&clean, &corrupt, 256);

    let result = decode_frames(
        m4v_input(&corrupt).set_exit_on_error(true),
        "corrupt_strict_out.mp4",
        30,
        "exit_on_error must surface the decode failure",
    );
    assert!(
        result.is_err(),
        "exit_on_error=true must surface the decode error, got {result:?}"
    );
}
