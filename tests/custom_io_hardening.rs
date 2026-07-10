//! Hardening coverage for the custom-IO AVIO trampolines.
//!
//! FFmpeg drives custom IO through four `extern "C"` trampolines (read,
//! write, input seek, output seek) that invoke user closures on FFmpeg's own
//! threads. A closure that panics would unwind across the `extern "C"`
//! boundary — undefined behavior that in practice aborts the whole process —
//! so each trampoline contains the panic with `catch_unwind` and hands FFmpeg
//! an ordinary error instead (`AVERROR(EIO)`; seek: `AVERROR(ESPIPE)`). The
//! read trampoline additionally rejects a callback that *claims* more bytes
//! than the buffer holds, which would otherwise let avio advance `buf_end`
//! past the allocation and read out of bounds.
//!
//! Every scenario drives a real job through the public custom-IO API
//! (`Input::new_by_read_callback`, `Output::new_by_write_callback`,
//! `set_seek_callback`). For the panic scenarios the load-bearing assertion
//! is that the test process survives to observe a job result at all; the
//! injected callbacks are expected to print panic messages to stderr while
//! the suite runs. Media fixtures are synthetic lavfi renders, so the tests
//! stay offline and deterministic.

use ez_ffmpeg::stream_info::{find_video_stream_info, StreamInfo};
use ez_ffmpeg::{FfmpegContext, Input, Output};
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::{Arc, Mutex};
use std::time::Duration;

mod common;
use common::{tmp_path_in, wait_with_watchdog};

fn tmp_path(name: &str) -> String {
    tmp_path_in("ez_ffmpeg_custom_io_hardening_tests", name)
}

/// Chains build -> start -> wait, surfacing the first error from any stage.
/// Custom inputs are probed inside `build()` (a panicking read callback
/// already fails there), while a mid-stream write failure only surfaces from
/// `wait()`; the tests only care that *some* stage reports `Err`.
fn run_job(
    context: ez_ffmpeg::error::Result<FfmpegContext>,
    secs: u64,
    scenario: &str,
) -> ez_ffmpeg::error::Result<()> {
    wait_with_watchdog(context?.start()?, secs, scenario)
}

/// Bounds the WHOLE job — including `build()`, where custom inputs are
/// probed — instead of only `wait()`. The seek-panic scenarios must prove
/// the job comes back within a generous deadline no matter which stage the
/// contained panic hits.
fn run_job_within<F>(secs: u64, scenario: &str, job: F) -> ez_ffmpeg::error::Result<()>
where
    F: FnOnce() -> ez_ffmpeg::error::Result<()> + Send + 'static,
{
    let (tx, rx) = std::sync::mpsc::channel();
    std::thread::spawn(move || {
        let _ = tx.send(job());
    });
    match rx.recv_timeout(Duration::from_secs(secs)) {
        Ok(result) => result,
        Err(std::sync::mpsc::RecvTimeoutError::Timeout) => {
            panic!("scenario `{scenario}` did not finish within {secs}s (hang)")
        }
        Err(std::sync::mpsc::RecvTimeoutError::Disconnected) => {
            panic!("scenario `{scenario}`: job thread panicked before reporting")
        }
    }
}

/// Renders a small deterministic media file offline (lavfi color source) and
/// returns its raw bytes for the in-memory sources below.
fn fixture_bytes(name: &str, format: &str, codec: &str, size: &str, frames: i64) -> Vec<u8> {
    let path = tmp_path(name);
    let result = run_job(
        FfmpegContext::builder()
            .input(Input::from(format!("color=c=blue:s={size}:r=30")).set_format("lavfi"))
            .output(
                Output::from(path.as_str())
                    .set_format(format)
                    .set_video_codec(codec)
                    .set_max_video_frames(frames),
            )
            .build(),
        60,
        "fixture",
    );
    assert!(result.is_ok(), "fixture render failed: {result:?}");
    std::fs::read(&path).unwrap()
}

/// Shared cursor over an in-memory media file, backing the well-behaved
/// read/seek callback pair below (the same shape as the
/// `custom_input_output` example).
struct MemSource {
    data: Vec<u8>,
    pos: i64,
}

fn mem_read_callback(src: Arc<Mutex<MemSource>>) -> impl FnMut(&mut [u8]) -> i32 + Send + 'static {
    move |buf: &mut [u8]| -> i32 {
        let mut src = src.lock().unwrap();
        let pos = src.pos as usize;
        if pos >= src.data.len() {
            return ffmpeg_sys_next::AVERROR_EOF;
        }
        let n = buf.len().min(src.data.len() - pos);
        buf[..n].copy_from_slice(&src.data[pos..pos + n]);
        src.pos += n as i64;
        n as i32
    }
}

fn mem_seek_callback(src: Arc<Mutex<MemSource>>) -> impl FnMut(i64, i32) -> i64 + Send + 'static {
    move |offset: i64, whence: i32| -> i64 {
        let mut src = src.lock().unwrap();
        let len = src.data.len() as i64;
        let target = match whence {
            ffmpeg_sys_next::AVSEEK_SIZE => return len,
            ffmpeg_sys_next::SEEK_SET => offset,
            ffmpeg_sys_next::SEEK_CUR => src.pos + offset,
            ffmpeg_sys_next::SEEK_END => len + offset,
            _ => return ffmpeg_sys_next::AVERROR(ffmpeg_sys_next::ESPIPE) as i64,
        };
        if target < 0 {
            return ffmpeg_sys_next::AVERROR(ffmpeg_sys_next::ESPIPE) as i64;
        }
        src.pos = target;
        target
    }
}

/// Positional in-memory output file. The mp4 muxer seeks back into already
/// written data to patch sizes, so a plain append buffer is not enough.
#[derive(Default)]
struct MemSink {
    buf: Vec<u8>,
    pos: usize,
}

fn mem_write_callback(sink: Arc<Mutex<MemSink>>) -> impl FnMut(&[u8]) -> i32 + Send + 'static {
    move |data: &[u8]| -> i32 {
        let mut sink = sink.lock().unwrap();
        let pos = sink.pos;
        let end = pos + data.len();
        if sink.buf.len() < end {
            sink.buf.resize(end, 0);
        }
        sink.buf[pos..end].copy_from_slice(data);
        sink.pos = end;
        data.len() as i32
    }
}

fn mem_sink_seek_callback(
    sink: Arc<Mutex<MemSink>>,
) -> impl FnMut(i64, i32) -> i64 + Send + 'static {
    move |offset: i64, whence: i32| -> i64 {
        let mut sink = sink.lock().unwrap();
        let len = sink.buf.len() as i64;
        let target = match whence {
            ffmpeg_sys_next::AVSEEK_SIZE => return len,
            ffmpeg_sys_next::SEEK_SET => offset,
            ffmpeg_sys_next::SEEK_CUR => sink.pos as i64 + offset,
            ffmpeg_sys_next::SEEK_END => len + offset,
            _ => return ffmpeg_sys_next::AVERROR(ffmpeg_sys_next::ESPIPE) as i64,
        };
        if target < 0 {
            return ffmpeg_sys_next::AVERROR(ffmpeg_sys_next::ESPIPE) as i64;
        }
        sink.pos = target as usize;
        target
    }
}

/// A read callback that panics on its very first invocation. Without
/// containment the panic would unwind out of the read trampoline during
/// `avformat_open_input` probing and abort the process; with it, FFmpeg sees
/// `AVERROR(EIO)` and the job surfaces a plain `Err`.
#[test]
fn read_callback_panic_is_contained_and_fails_the_job() {
    let input = Input::new_by_read_callback(|_buf: &mut [u8]| -> i32 {
        panic!("test-injected read panic");
    });

    let out = tmp_path("read_panic_out.mp4");
    let result = run_job(
        FfmpegContext::builder()
            .input(input)
            .output(Output::from(out.as_str()).set_video_codec("mpeg4"))
            .build(),
        30,
        "read panic",
    );
    assert!(
        result.is_err(),
        "a panicking read callback must fail the job, got {result:?}"
    );
}

/// A read callback that fills the buffer with real media bytes but *claims*
/// to have produced more than the buffer holds. Trusting that length would
/// let avio advance `buf_end` past the allocation and read out of bounds;
/// the trampoline must reject it with `AVERROR(EIO)` before it reaches avio.
#[test]
fn read_callback_forged_length_is_rejected() {
    let data = fixture_bytes("forged_len_src.ts", "mpegts", "mpeg2video", "320x240", 30);
    let src = Arc::new(Mutex::new(MemSource { data, pos: 0 }));

    let mut read = mem_read_callback(src);
    let input = Input::new_by_read_callback(move |buf: &mut [u8]| -> i32 {
        let ret = read(buf);
        if ret <= 0 {
            return ret;
        }
        // Otherwise plausible: real bytes were copied into `buf`, only the
        // reported length lies — 64 bytes beyond what the buffer holds.
        buf.len() as i32 + 64
    });

    let out = tmp_path("forged_len_out.mp4");
    let result = run_job(
        FfmpegContext::builder()
            .input(input)
            .output(Output::from(out.as_str()).set_video_codec("mpeg4"))
            .build(),
        30,
        "forged read length",
    );
    assert!(
        result.is_err(),
        "a read callback over-claiming its length must fail the job, got {result:?}"
    );
}

/// The write callback behaves for the first flushes of a real mpegts encode
/// (a 4 KiB avio buffer forces frequent flushes), then panics mid-stream.
/// The muxer must observe `AVERROR(EIO)` and fail the job instead of the
/// process aborting on an FFmpeg thread.
#[test]
fn write_callback_panic_mid_stream_fails_the_job() {
    let calls = Arc::new(AtomicUsize::new(0));

    let write_calls = Arc::clone(&calls);
    let output = Output::new_by_write_callback(move |data: &[u8]| -> i32 {
        if write_calls.fetch_add(1, Ordering::SeqCst) >= 3 {
            panic!("test-injected write panic");
        }
        data.len() as i32
    })
    .set_format("mpegts")
    .set_video_codec("mpeg2video")
    .set_max_video_frames(120)
    .set_io_buffer_size(4096);

    let result = run_job(
        FfmpegContext::builder()
            .input(Input::from("color=c=blue:s=320x240:r=30").set_format("lavfi"))
            .output(output)
            .build(),
        60,
        "write panic",
    );
    assert!(
        result.is_err(),
        "a write callback panicking mid-stream must fail the job, got {result:?}"
    );
    assert!(
        calls.load(Ordering::SeqCst) >= 4,
        "the panic was supposed to hit mid-stream, after several healthy flushes"
    );
}

/// An input seek callback that panics on every invocation while reads stay
/// healthy. The fixture's `mdat` is large enough that skipping it during mp4
/// probing exceeds both the 4 KiB avio buffer and avio's short-seek
/// threshold, so probing performs a genuine seek-callback call instead of
/// reading forward through the buffer.
///
/// Observed outcome with FFmpeg 7.x: every contained seek returns
/// `AVERROR(ESPIPE)`, mp4 probing cannot reach the trailing `moov` atom, and
/// the job fails cleanly during `build()` with an open-input error. The
/// contract asserted here is deliberately looser — the job must come back
/// within a generous bound (no hang, no abort) and the panicking callback
/// must actually have been exercised.
#[test]
fn input_seek_callback_panic_is_contained() {
    let data = fixture_bytes("seek_panic_src.mp4", "mp4", "mpeg4", "640x480", 150);
    let src = Arc::new(Mutex::new(MemSource { data, pos: 0 }));

    let seek_calls = Arc::new(AtomicUsize::new(0));
    let seek_probe = Arc::clone(&seek_calls);
    let input = Input::new_by_read_callback(mem_read_callback(src))
        .set_seek_callback(move |_offset: i64, _whence: i32| -> i64 {
            seek_probe.fetch_add(1, Ordering::SeqCst);
            panic!("test-injected input seek panic");
        })
        .set_io_buffer_size(4096);

    let out = tmp_path("seek_panic_out.mp4");
    let result = run_job_within(30, "input seek panic", move || {
        FfmpegContext::builder()
            .input(input)
            .output(Output::from(out.as_str()).set_video_codec("mpeg4"))
            .build()?
            .start()?
            .wait()
    });

    // Reaching this line at all is the core guarantee: the panic did not
    // cross the extern "C" boundary and take the process down.
    assert!(
        seek_calls.load(Ordering::SeqCst) >= 1,
        "mp4 probing was supposed to attempt at least one seek"
    );
    assert!(
        result.is_err(),
        "with every seek failing, mp4 probing cannot find the trailing moov \
         and the job must error, got {result:?}"
    );
}

/// An output seek callback that panics on every invocation while writes stay
/// healthy. The mp4 muxer must seek back into the written stream at trailer
/// time to patch sizes, so the panicking callback is guaranteed to run.
///
/// A panicking callback poisons its custom-IO context: the panic itself is
/// contained (EIO instead of unwinding across extern "C"), and every LATER
/// callback on the same context short-circuits with EIO without invoking the
/// torn user closure again. That containment-plus-poison is what this test
/// pins; the job result itself is not asserted, because how hard the failure
/// surfaces depends on the muxer (the mov muxer ignores the failed size-patch
/// seek itself — the poisoned follow-up writes are what make the failure
/// visible).
#[test]
fn output_seek_callback_panic_is_contained() {
    let sink = Arc::new(Mutex::new(MemSink::default()));
    let seek_calls = Arc::new(AtomicUsize::new(0));
    let seek_probe = Arc::clone(&seek_calls);

    let output = Output::new_by_write_callback(mem_write_callback(Arc::clone(&sink)))
        .set_seek_callback(move |_offset: i64, _whence: i32| -> i64 {
            seek_probe.fetch_add(1, Ordering::SeqCst);
            panic!("test-injected output seek panic");
        })
        .set_format("mp4")
        .set_video_codec("mpeg4")
        .set_max_video_frames(30)
        .set_io_buffer_size(4096);

    // Ok or Err are both acceptable here (see above); either way the failure
    // stayed contained and the context was poisoned.
    let _result = run_job_within(60, "output seek panic", move || {
        FfmpegContext::builder()
            .input(Input::from("color=c=blue:s=320x240:r=30").set_format("lavfi"))
            .output(output)
            .build()?
            .start()?
            .wait()
    });

    // Reaching this line at all is the core guarantee: the panic did not
    // cross the extern "C" boundary and take the process down.
    //
    // Exactly one: the first invocation panics and poisons the context, so
    // every later seek attempt must short-circuit inside the wrapper without
    // re-entering the torn user closure.
    assert_eq!(
        seek_calls.load(Ordering::SeqCst),
        1,
        "a panicked custom-IO context must not invoke the user seek closure again"
    );
}

/// Positive control: a healthy in-memory roundtrip. A lavfi-rendered mp4 is read
/// through the custom read+seek callbacks and transcoded into an in-memory
/// mp4 through the custom write+seek callbacks; all four trampolines run
/// their normal pass-through path, the job succeeds, and the output bytes
/// re-probe as a well-formed video file.
#[test]
fn in_memory_roundtrip_via_custom_io_succeeds() {
    let data = fixture_bytes("roundtrip_src.mp4", "mp4", "mpeg4", "320x240", 30);
    let src = Arc::new(Mutex::new(MemSource { data, pos: 0 }));
    let input = Input::new_by_read_callback(mem_read_callback(Arc::clone(&src)))
        .set_seek_callback(mem_seek_callback(src));

    let sink = Arc::new(Mutex::new(MemSink::default()));
    let output = Output::new_by_write_callback(mem_write_callback(Arc::clone(&sink)))
        .set_seek_callback(mem_sink_seek_callback(Arc::clone(&sink)))
        .set_format("mp4")
        .set_video_codec("mpeg4");

    let result = run_job(
        FfmpegContext::builder().input(input).output(output).build(),
        60,
        "in-memory roundtrip",
    );
    assert!(result.is_ok(), "healthy custom-IO roundtrip failed: {result:?}");

    let bytes = sink.lock().unwrap().buf.clone();
    assert!(
        bytes.len() > 1024,
        "roundtrip output should be a real mp4, got {} bytes",
        bytes.len()
    );

    // The in-memory output must be a well-formed file: re-probe it.
    let reprobe = tmp_path("roundtrip_reprobe.mp4");
    std::fs::write(&reprobe, &bytes).unwrap();
    match find_video_stream_info(&reprobe)
        .expect("failed to probe roundtrip output")
        .expect("roundtrip output has no video stream")
    {
        StreamInfo::Video {
            width, nb_frames, ..
        } => {
            assert_eq!(width, 320, "roundtrip must preserve the frame width");
            assert_eq!(nb_frames, 30, "roundtrip must keep every frame");
        }
        other => panic!("expected video stream info, got {other:?}"),
    }
}
