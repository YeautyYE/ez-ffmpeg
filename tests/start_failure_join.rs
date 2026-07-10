//! Regression net for `start()` failures after worker threads exist.
//!
//! When `FfmpegScheduler::start()` fails at a point where worker threads have
//! already been spawned, it must publish the terminal status and JOIN every
//! spawned worker (`fail_start`) before returning `Err`: the error return
//! drops the `FfmpegContext`, sole owner of the `InterruptState` that the
//! workers' AVIO interrupt callbacks dereference through a raw pointer.
//! Returning early instead used to free that state under a live muxer worker
//! still writing its trailer (use-after-free).
//!
//! Each scenario drives one synchronously reachable `start()` failure site
//! through the public API and asserts `start()` returns `Err` within a
//! watchdog bound — no hang (a stuck join) and no crash (a worker touching
//! the freed context):
//!
//! - muxer-loop `mux_init` failure: a ready (all-stream-copy) output whose
//!   nonexistent bitstream filter fails `init_bitstream_filters`, after the
//!   healthy sibling copy output's muxer worker was already spawned;
//! - encoder-loop `enc_init` failure: an unparsable value for a known encoder
//!   option fails `set_encoder_opts` on the caller thread, after the healthy
//!   sibling copy output's muxer worker and this output's mux waiter thread
//!   were already spawned;
//! - decoder-loop `dec_init` failure: an unparsable value for a known decoder
//!   option fails `dec_open` on the caller thread, after the output side
//!   (mux waiter + encoder worker + filter graph) was already running.
//!
//! The remaining `fail_start` site — `ready_to_init_mux` — only errors when
//! the OS refuses to spawn the waiter thread, which the public API cannot
//! trigger deterministically; its join handling is exercised indirectly by
//! the scenarios above (their `fail_start` joins those same waiter threads).
//! `enc_init_bad_encoder_opt_with_live_sibling_encoder_worker` additionally
//! pins the encoder-loop failure shape whose join depends on the encoder
//! honoring the terminal status on its receive-timeout path.
//!
//! Note: a nonexistent BSF on an ENCODED stream (as in `bsf.rs`) does not
//! reach these paths — a not-yet-ready muxer initializes its BSFs inside the
//! waiter thread, so `start()` succeeds and the error surfaces from `wait()`.
//! Only a ready (all-copy) output fails `mux_init` on the `start()` thread.

mod common;

use common::{tmp_path_in, wait_with_watchdog};
use ez_ffmpeg::core::scheduler::ffmpeg_scheduler::Running;
use ez_ffmpeg::{FfmpegContext, FfmpegScheduler, Input, Output};
use std::sync::mpsc::RecvTimeoutError;
use std::sync::OnceLock;
use std::time::Duration;

/// Namespace for this binary's scratch files under `$TMPDIR`.
const SUBDIR: &str = "ez_ffmpeg_start_failure_tests";

/// A tiny (~30 frame) mpeg4 MP4 the copy scenarios remux from. Built once per
/// test process with the always-available lavfi `testsrc` + native `mpeg4`
/// encoder, so the suite stays offline and deterministic.
fn fixture_mp4() -> &'static str {
    static FIXTURE: OnceLock<String> = OnceLock::new();
    FIXTURE.get_or_init(|| {
        let path = tmp_path_in(SUBDIR, "src.mp4");
        let running = FfmpegContext::builder()
            .input(Input::from("testsrc=size=320x240:rate=30:duration=1").set_format("lavfi"))
            .output(
                Output::from(path.as_str())
                    .set_video_codec("mpeg4")
                    .set_max_video_frames(30),
            )
            .build()
            .expect("fixture context must build")
            .start()
            .expect("fixture job must start");
        let result = wait_with_watchdog(running, 60, "mpeg4 fixture encode");
        assert!(result.is_ok(), "fixture encode failed: {result:?}");
        path
    })
}

/// Runs `build + start` for a scenario whose `start()` MUST fail, on a side
/// thread so a hang inside `start()` (a join stuck on a worker that was never
/// released) fails this test instead of stalling the whole suite. Returns the
/// error message for the caller to assert on.
fn start_failure_with_watchdog<F>(scenario: &str, secs: u64, build_and_start: F) -> String
where
    F: FnOnce() -> ez_ffmpeg::error::Result<FfmpegScheduler<Running>> + Send + 'static,
{
    let (tx, rx) = std::sync::mpsc::channel();
    std::thread::spawn(move || {
        let outcome = match build_and_start() {
            // Dropping the Running scheduler stops and joins the job, so an
            // unexpected success cannot leak workers into later scenarios.
            Ok(running) => {
                drop(running);
                None
            }
            Err(e) => Some(format!("{e}")),
        };
        let _ = tx.send(outcome);
    });
    match rx.recv_timeout(Duration::from_secs(secs)) {
        Ok(Some(msg)) => msg,
        Ok(None) => panic!("scenario `{scenario}`: start() succeeded but must fail"),
        Err(RecvTimeoutError::Timeout) => panic!(
            "scenario `{scenario}` did not finish within {secs}s: start() hung \
             instead of joining the spawned workers and returning Err"
        ),
        Err(RecvTimeoutError::Disconnected) => {
            panic!("scenario `{scenario}`: start() thread panicked before reporting")
        }
    }
}

/// The two-output copy job behind the mux-init scenarios. Output #0 is a
/// healthy stream copy: being all-copy it is ready at `start()`, so its muxer
/// worker spawns and writes the MP4 header in the muxer loop. Output #1 is
/// the same copy wired through a nonexistent bitstream filter, which fails
/// `mux_init` while output #0's worker is live; `fail_start` must join that
/// worker (mid trailer I/O against the shared `InterruptState`) before
/// `start()` returns the BSF error.
fn two_output_bad_bsf_start(
    fixture: &str,
    out_ok: String,
    out_bad: String,
) -> ez_ffmpeg::error::Result<FfmpegScheduler<Running>> {
    FfmpegContext::builder()
        .input(Input::from(fixture))
        .output(Output::from(out_ok.as_str()).add_stream_map_with_copy("0:v"))
        .output(
            Output::from(out_bad.as_str())
                .add_stream_map_with_copy("0:v")
                .set_video_bsf("definitely_not_a_bsf"),
        )
        .build()
        .expect("two-output copy context must build; the failure under test comes from start()")
        .start()
}

#[test]
fn mux_init_bsf_failure_fails_start_and_joins_healthy_mux_worker() {
    let fixture = fixture_mp4();
    let out_ok = tmp_path_in(SUBDIR, "bsf_join_ok.mp4");
    let out_bad = tmp_path_in(SUBDIR, "bsf_join_bad.mp4");
    let msg = start_failure_with_watchdog("two-output bad bsf", 30, move || {
        two_output_bad_bsf_start(fixture, out_ok, out_bad)
    });
    assert!(
        msg.contains("bitstream filter"),
        "error should identify the bitstream filter failure, got: {msg}"
    );
}

/// Without a sanitizer the old use-after-free (context freed under a live
/// muxer worker's trailer I/O) surfaces as an intermittent crash, not a
/// deterministic assert; repeating the scenario with fresh contexts raises
/// the odds that any regression takes the process down right here.
#[test]
fn mux_init_bsf_failure_stress_fresh_contexts() {
    let fixture = fixture_mp4();
    for i in 0..12 {
        let out_ok = tmp_path_in(SUBDIR, &format!("bsf_stress_{i}_ok.mp4"));
        let out_bad = tmp_path_in(SUBDIR, &format!("bsf_stress_{i}_bad.mp4"));
        let scenario = format!("bad bsf stress iteration {i}");
        let msg = start_failure_with_watchdog(&scenario, 30, move || {
            two_output_bad_bsf_start(fixture, out_ok, out_bad)
        });
        assert!(
            msg.contains("bitstream filter"),
            "iteration {i}: expected the bitstream filter error, got: {msg}"
        );
    }
}

/// `start()` fails in the encoder loop (`enc_init` -> `set_encoder_opts`): a
/// known encoder option carrying an unparsable value makes `av_opt_set_dict2`
/// fail on the caller thread, before the encoder worker for that stream
/// spawns. Output #0 is a healthy stream copy whose muxer worker is already
/// live (spawned by the muxer loop), and output #1 has already spawned its
/// ready-to-init waiter thread; `fail_start` must join both before returning
/// the error.
#[test]
fn enc_init_bad_encoder_opt_fails_start_and_joins_live_mux_worker() {
    let fixture = fixture_mp4();
    let out_ok = tmp_path_in(SUBDIR, "enc_opt_ok.mp4");
    let out_bad = tmp_path_in(SUBDIR, "enc_opt_bad.mp4");
    let msg = start_failure_with_watchdog("bad encoder option value", 30, move || {
        FfmpegContext::builder()
            .input(Input::from(fixture))
            .output(Output::from(out_ok.as_str()).add_stream_map_with_copy("0:v"))
            .output(
                Output::from(out_bad.as_str())
                    .set_video_codec("mpeg4")
                    .set_video_codec_opt("b", "not_a_number")
                    .set_max_video_frames(30),
            )
            .build()
            .expect("copy+encode context must build; the failure under test comes from start()")
            .start()
    });
    assert!(
        msg.contains("encoder"),
        "error should come from the encoder option path, got: {msg}"
    );
}

/// Same `enc_init` failure site, but with a healthy sibling ENCODER worker
/// (not a muxer worker) already spawned when `enc_init` fails for the second
/// output. When the failing `enc_init` runs, no filter graph exists yet to
/// feed (or disconnect) the sibling encoder's frame channel, so that worker
/// can only exit by observing the terminal status from its `recv_timeout`
/// timeout path — `receive_from` must check `is_stopping` there, or
/// `fail_start`'s join never returns and `start()` hangs instead of
/// reporting the error.
#[test]
fn enc_init_bad_encoder_opt_with_live_sibling_encoder_worker() {
    let out_ok = tmp_path_in(SUBDIR, "enc_sibling_ok.mp4");
    let out_bad = tmp_path_in(SUBDIR, "enc_sibling_bad.mp4");
    let msg = start_failure_with_watchdog("bad encoder option, encoder sibling", 30, move || {
        FfmpegContext::builder()
            .input(Input::from("testsrc=size=320x240:rate=30:duration=1").set_format("lavfi"))
            .output(
                Output::from(out_ok.as_str())
                    .set_video_codec("mpeg4")
                    .set_max_video_frames(30),
            )
            .output(
                Output::from(out_bad.as_str())
                    .set_video_codec("mpeg4")
                    .set_video_codec_opt("b", "not_a_number")
                    .set_max_video_frames(30),
            )
            .build()
            .expect("encode context must build; the failure under test comes from start()")
            .start()
    });
    assert!(
        msg.contains("encoder"),
        "error should come from the encoder option path, got: {msg}"
    );
}

/// `start()` fails in the decoder loop (`dec_init` -> `dec_open`): a known
/// decoder option carrying an unparsable value makes `av_opt_set_dict2` fail
/// before the decoder worker spawns. By then the output side (mux waiter +
/// encoder worker) is already live; `fail_start` must join it before the
/// error return drops the context.
#[test]
fn dec_init_bad_decoder_opt_fails_start_and_joins_spawned_workers() {
    let out = tmp_path_in(SUBDIR, "dec_opt_out.mp4");
    let msg = start_failure_with_watchdog("bad decoder option value", 30, move || {
        FfmpegContext::builder()
            .input(
                Input::from("testsrc=size=320x240:rate=30:duration=1")
                    .set_format("lavfi")
                    .set_video_codec_opt("threads", "not_a_number"),
            )
            .output(
                Output::from(out.as_str())
                    .set_video_codec("mpeg4")
                    .set_max_video_frames(30),
            )
            .build()
            .expect("decode context must build; the failure under test comes from start()")
            .start()
    });
    assert!(
        msg.contains("decoder"),
        "error should come from the decoder option path, got: {msg}"
    );
}
