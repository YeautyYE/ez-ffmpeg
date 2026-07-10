//! Regression net: muxer teardown ordering across the delayed mux-init window.
//!
//! When every output stream is ENCODED, the muxer is not ready at `start()`:
//! `ready_to_init_mux` parks the output `AVFormatContext` in a waiter thread
//! until each encoder signals its stream ready (first packet), and only then
//! runs mux init and the muxer worker. Every teardown path out of that state
//! machine — stop() while the waiter still owns the context, stop() mid-worker,
//! or a mux-init failure — must join this muxer's encoder threads BEFORE the
//! `AVFormatContext` is freed: `avformat_free_context` frees the `AVStream`s
//! the encoder threads still dereference, so freeing first is a use-after-free
//! that surfaces as intermittent crashes, aborts, or hangs.
//!
//! These tests sweep stop() across the delayed-start window (waiter window at
//! small delays, muxer worker at larger ones) and loop the hottest delays. A
//! regression shows up as a crashed test process or a stop() that never
//! returns (each stop runs under a 30s watchdog).

mod common;

use common::{tmp_path_in, wait_with_watchdog};
use ez_ffmpeg::core::scheduler::ffmpeg_scheduler::Running;
use ez_ffmpeg::{FfmpegContext, FfmpegScheduler, Input, Output};
use std::sync::Mutex;
use std::time::Duration;

/// The sweep delays are only meaningful if the three tests do not compete for
/// CPU inside the shared test binary: serialize them.
static PROCESS_LOCK: Mutex<()> = Mutex::new(());

const TMP_SUBDIR: &str = "ez_ffmpeg_mux_teardown";

fn testsrc_video() -> Input {
    Input::from("testsrc=size=320x240:rate=30:duration=10").set_format("lavfi")
}

/// `paced` throttles demuxing to 0.2x so the audio encoder keeps feeding the
/// muxer slowly for the whole job instead of racing through the 10s of media;
/// the readrate initial burst (0.5s of media at full speed) means the FIRST
/// audio packet is not delayed, so the not-ready window itself stays natural.
fn sine_audio(paced: bool) -> Input {
    let input = Input::from("sine=frequency=440:duration=10").set_format("lavfi");
    if paced {
        input.set_readrate(0.2)
    } else {
        input
    }
}

/// Two encoded streams (video + audio) force the delayed mux-init path: the
/// muxer cannot be ready at start() until BOTH encoders produce a packet.
fn encoded_av_output(out: &str) -> Output {
    Output::from(out)
        .add_stream_map("0:v")
        .add_stream_map("1:a")
        .set_video_codec("mpeg4")
        .set_audio_codec("aac")
}

fn start_dual_encoded_job(out: &str, paced: bool) -> FfmpegScheduler<Running> {
    FfmpegContext::builder()
        .input(testsrc_video())
        .input(sine_audio(paced))
        .output(encoded_av_output(out))
        .build()
        .unwrap()
        .start()
        .unwrap()
}

/// stop() on a side thread; a stall past `secs` is a teardown hang (typically
/// a join ordering bug), turned into a panic naming `scenario`.
fn stop_with_watchdog(scheduler: FfmpegScheduler<Running>, secs: u64, scenario: &str) {
    let (tx, rx) = std::sync::mpsc::channel();
    std::thread::spawn(move || {
        scheduler.stop();
        let _ = tx.send(());
    });
    assert!(
        rx.recv_timeout(Duration::from_secs(secs)).is_ok(),
        "stop() hung during mux teardown ({scenario})"
    );
}

fn output_size(path: &str) -> u64 {
    std::fs::metadata(path).map(|m| m.len()).unwrap_or(0)
}

/// Sweep stop() across the delayed-start window. Small delays land while the
/// ready-to-init waiter still owns the output context (pre-init teardown);
/// larger ones land after mux init, in the muxer worker (normal-stop
/// teardown). Both paths must join the encoders before freeing the context.
#[test]
fn stop_mid_delayed_start_sweep() {
    let _lock = PROCESS_LOCK.lock().unwrap_or_else(|e| e.into_inner());
    for delay_ms in [0u64, 10, 25, 50, 100, 250] {
        let out = tmp_path_in(TMP_SUBDIR, &format!("stop_mid_{delay_ms}.mp4"));
        let scheduler = start_dual_encoded_job(&out, true);
        std::thread::sleep(Duration::from_millis(delay_ms));
        stop_with_watchdog(scheduler, 30, &format!("sweep, delay {delay_ms}ms"));

        // Failure-triage breadcrumb: 0 bytes means stop() landed before mux
        // init wrote the header (waiter window); >0 means mid-worker.
        eprintln!(
            "sweep delay {delay_ms}ms -> output size {}B",
            output_size(&out)
        );
    }
}

/// The two hottest delays from local sweeps, looped with fresh contexts:
/// at 0ms stop() lands in the ready-to-init waiter window (header not yet
/// written), at 10ms just after mux init, in the early worker with the
/// encoders still hot — the two sides of the init boundary. A use-after-free
/// here is intermittent by nature (an encoder thread must lose the race with
/// `avformat_free_context`), so repetition raises detection odds.
#[test]
fn stop_mid_delayed_start_stress() {
    let _lock = PROCESS_LOCK.lock().unwrap_or_else(|e| e.into_inner());
    for delay_ms in [0u64, 10] {
        for iter in 0..8 {
            let out = tmp_path_in(TMP_SUBDIR, &format!("stress_{delay_ms}_{iter}.mp4"));
            let scheduler = start_dual_encoded_job(&out, true);
            std::thread::sleep(Duration::from_millis(delay_ms));
            stop_with_watchdog(
                scheduler,
                30,
                &format!("stress, delay {delay_ms}ms, iteration {iter}"),
            );
        }
    }
}

/// Deterministic waiter-window stop: the second video stream runs through
/// `realtime` (paces frames to the 30fps wall clock, first frame as baseline)
/// followed by `select='gte(n,60)'` (drops the first 60 frames), so its FIRST
/// surviving frame reaches its encoder only after ~2s of wall clock — until
/// then the muxer is not ready and the ready-to-init waiter owns the output
/// context. A stop() at 500ms therefore lands in the waiter window BY
/// CONSTRUCTION (unlike the sweep above, which relies on machine timing),
/// pinned by the header-never-written assertion. The waiter teardown must
/// join the fast stream's live encoder before the context is freed; without
/// that ordering this is the exact use-after-free AddressSanitizer catches
/// on this scenario.
#[test]
fn stop_in_waiter_window_is_deterministic() {
    let _lock = PROCESS_LOCK.lock().unwrap_or_else(|e| e.into_inner());
    let out = tmp_path_in(TMP_SUBDIR, "waiter_window.mp4");
    let scheduler = FfmpegContext::builder()
        .input(testsrc_video())
        .filter_desc("[0:v]split[fast][slowsrc];[slowsrc]realtime,select='gte(n,60)'[slow]")
        .output(
            Output::from(out.as_str())
                .add_stream_map("[fast]")
                .add_stream_map("[slow]")
                .set_video_codec("mpeg4"),
        )
        .build()
        .unwrap()
        .start()
        .unwrap();

    std::thread::sleep(Duration::from_millis(500));
    stop_with_watchdog(scheduler, 30, "deterministic waiter window");

    assert_eq!(
        output_size(&out),
        0,
        "the slow stream's first frame is ~2s out, so a 500ms stop must land \
         BEFORE mux init writes the header; a non-empty file means the test \
         no longer exercises the waiter window"
    );
}

/// Positive control: the same dual-encoder job, bounded by per-stream frame
/// caps (~1s of media), must still run through the delayed mux init to a
/// clean wait() and produce a real file. Pins that the teardown-ordering
/// fixes did not break the normal completion path.
#[test]
fn wait_completes_after_full_delayed_start() {
    let _lock = PROCESS_LOCK.lock().unwrap_or_else(|e| e.into_inner());
    let out = tmp_path_in(TMP_SUBDIR, "positive_control.mp4");
    let scheduler = FfmpegContext::builder()
        .input(testsrc_video())
        // Unpaced: the control checks completion, not stop() timing, and
        // readrate would slow the run for nothing.
        .input(sine_audio(false))
        .output(
            encoded_av_output(&out)
                // 30 video frames @30fps and 44 aac frames @1024/44100 are
                // both ~1s of media, so neither stream starves the other.
                .set_max_video_frames(30)
                .set_max_audio_frames(44),
        )
        .build()
        .unwrap()
        .start()
        .unwrap();

    let result = wait_with_watchdog(scheduler, 60, "delayed-start job runs to completion");
    assert!(result.is_ok(), "dual-encoder job failed: {result:?}");

    let size = output_size(&out);
    assert!(
        size > 1024,
        "output should hold ~1s of encoded A/V, got {size}B"
    );
}
