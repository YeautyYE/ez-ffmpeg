//! GitHub #43 regression: a decoder-less DATA stream (e.g. a mov `tmcd`
//! timecode track) must not abort the whole job.
//!
//! Before the fix, `dec_init` required a decoder for every input stream and
//! returned "Decoder codec pointer is null for stream N" for the data stream —
//! even though such a stream is never decoded, only demuxed/stream-copied or
//! dropped. The null-codec guard now runs *after* the "not connected to a
//! decoder" skip (`connect_stream` only wires a source for streams routed to a
//! decoder), mirroring the ffmpeg CLI which decodes only mapped streams.

use ez_ffmpeg::{FfmpegContext, FfmpegScheduler, Input, Output};
use std::process::Command;
use std::time::Duration;

fn tmp_path(name: &str) -> String {
    let dir = std::env::temp_dir().join(format!(
        "ez_ffmpeg_data_stream_tests_{}",
        std::process::id()
    ));
    std::fs::create_dir_all(&dir).unwrap();
    dir.join(name).to_string_lossy().into_owned()
}

/// Build a `.mov` carrying a video stream plus a `tmcd` timecode DATA stream via
/// the ffmpeg CLI. Returns `None` only when the ffmpeg/ffprobe CLI is
/// unavailable, so the test skips cleanly in environments without it.
///
/// A standard ffmpeg build always adds the `tmcd` track for `-timecode`, so the
/// *absence* of a data stream is a broken fixture, not a skip condition: it is
/// asserted here rather than passed over, otherwise the regression test would
/// silently degrade into a no-op ("nothing to skip -> transcode succeeds").
fn tmcd_mov_fixture() -> Option<String> {
    let path = tmp_path("with_tmcd.mov");
    let built = Command::new("ffmpeg")
        .args([
            "-y",
            "-loglevel",
            "error",
            "-f",
            "lavfi",
            "-i",
            "testsrc=d=1:s=320x240:r=15",
            "-timecode",
            "00:00:00:00",
            "-c:v",
            "mpeg4",
            &path,
        ])
        .status()
        .ok()?; // ffmpeg CLI absent -> skip
    if !built.success() || !std::path::Path::new(&path).exists() {
        return None;
    }

    // Confirm the fixture really carries a data stream; without it the test
    // below would assert on an input that no longer exercises the bug.
    let probe = Command::new("ffprobe")
        .args([
            "-v", "error",
            "-select_streams", "d",
            "-show_entries", "stream=index",
            "-of", "csv=p=0",
            &path,
        ])
        .output()
        .ok()?; // ffprobe CLI absent -> skip
    let data_streams = String::from_utf8_lossy(&probe.stdout)
        .lines()
        .filter(|l| !l.trim().is_empty())
        .count();
    assert!(
        probe.status.success() && data_streams >= 1,
        "fixture built without a data stream (tmcd); the #43 regression test \
         would be a no-op. ffprobe stdout: {:?}",
        String::from_utf8_lossy(&probe.stdout)
    );

    Some(path)
}

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

/// Transcoding a file whose input carries a `tmcd` data stream must succeed:
/// the data stream has no decoder and is not connected to a decode path, so it
/// is skipped rather than aborting the job.
#[test]
fn data_stream_does_not_abort_transcode() {
    let Some(input) = tmcd_mov_fixture() else {
        eprintln!(
            "skipping data_stream_does_not_abort_transcode: \
             ffmpeg CLI unavailable to build the tmcd fixture"
        );
        return;
    };

    let out = tmp_path("transcoded.mp4");
    let context = FfmpegContext::builder()
        .input(Input::from(input.as_str()))
        .output(Output::from(out.as_str()).set_video_codec("mpeg4"))
        .build()
        .unwrap();

    // The regression surfaced at start(): dec_init runs there and returned
    // OpenDecoder(...) for the data stream. Assert start() succeeds first so a
    // failure points at the right place, then that the job runs to completion.
    let running = match context.start() {
        Ok(running) => running,
        Err(e) => panic!(
            "a decoder-less data stream must not abort the job at start (GitHub #43): {e:?}"
        ),
    };
    let result = wait_with_watchdog(running, 60, "data stream transcode");
    assert!(
        result.is_ok(),
        "a decoder-less data stream must not abort the job (GitHub #43): {result:?}"
    );
}
