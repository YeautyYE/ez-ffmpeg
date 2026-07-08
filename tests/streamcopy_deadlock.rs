//! Regression net for the streamcopy + encode co-mux deadlock.
//!
//! An output muxing at least one STREAMCOPY stream together with at least one
//! ENCODED stream could hang permanently. The copy stream's packets went
//! straight to the muxer's live `bounded(8)` channel, which the mux worker
//! drains only AFTER every output stream is ready; an encoded stream is not
//! ready until its encoder opens, and the encoder opens only once its first
//! input frame arrives. So a dense copy stream fills `bounded(8)` and blocks the
//! shared single-threaded demuxer BEFORE it can advance to the packet that would
//! open a slow/sparse co-muxed encoder -> the muxer never starts -> the live
//! queue never drains -> permanent hang, with no timeout escape.
//!
//! fftools routes demux->mux copy packets through the same `sch_send` path as
//! encoders (ffmpeg_sched.c:2038-2077) — copy and encode are uniform there. The
//! fix routes streamcopy through the encoders' `MuxStartGate` + per-stream
//! pre-mux queue, so a dense copy stream parks pre-start (byte-metered) instead
//! of blocking the demuxer.
//!
//! Deterministic trigger: a single input holding a DENSE video stream (dense
//! from t=0) and a SPARSE subtitle stream whose only cue lands seconds in (so
//! the subtitle provably has no packet until then). The job streamcopies the
//! video and re-encodes the subtitle: the copy fills `bounded(8)` within its
//! first ~8 frames (~0.27s at 30fps), long before the demuxer reaches the
//! subtitle packet that would open the subtitle encoder. Before the fix this
//! hangs (the watchdog turns the hang into a failure); after, it finishes and
//! the output carries both streams.

use ez_ffmpeg::stream_info::{find_subtitle_stream_info, find_video_stream_info, StreamInfo};
use ez_ffmpeg::{FfmpegContext, Input, Output};

mod common;
use common::{tmp_path_in, wait_with_watchdog};

fn tmp_path(name: &str) -> String {
    tmp_path_in("ez_ffmpeg_streamcopy_deadlock_tests", name)
}

/// A SubRip fixture whose only cue starts `start_s` seconds in, so the muxed
/// subtitle stream provably has no packet until then — that gap is what starves
/// the co-muxed subtitle encoder while the dense copy stream blocks the demuxer.
fn sparse_srt_fixture(name: &str, start_s: u32) -> String {
    let path = tmp_path(name);
    std::fs::write(
        &path,
        format!(
            "1\n00:00:{:02},000 --> 00:00:{:02},000\nlate cue\n\n",
            start_s,
            start_s + 1
        ),
    )
    .unwrap();
    path
}

/// Build the single-file fixture: a dense `secs`-second mpeg4 video plus a
/// subrip subtitle whose only cue lands at `sub_start_s`. The subtitle is copied
/// (the srt input is already subrip) so job 2 has an encodable subrip stream to
/// re-encode. Returns `false` (so the caller skips) if the environment lacks the
/// mpeg4/subrip codecs — the "gate on codec availability" the integration tests
/// rely on. Two separate single-stream inputs mean two demuxers here, so this
/// build cannot itself deadlock (the deadlock needs a copy + a late encoder in
/// the SAME demuxer).
fn build_fixture(mkv: &str, secs: u32, sub_start_s: u32) -> bool {
    let srt = sparse_srt_fixture("deadlock_in.srt", sub_start_s);
    let ctx = match FfmpegContext::builder()
        .input(Input::from(format!("color=c=black:s=320x240:r=30:d={secs}")).set_format("lavfi"))
        .input(Input::from(srt.as_str()))
        .output(
            Output::from(mkv)
                .set_video_codec("mpeg4")
                .set_subtitle_codec("copy"),
        )
        .build()
    {
        Ok(c) => c,
        Err(e) => {
            eprintln!("skipping: fixture build failed (codec unavailable?): {e:?}");
            return false;
        }
    };
    let scheduler = match ctx.start() {
        Ok(s) => s,
        Err(e) => {
            eprintln!("skipping: fixture start failed (codec unavailable?): {e:?}");
            return false;
        }
    };
    if let Err(e) = wait_with_watchdog(scheduler, 60, "build video+subtitle fixture") {
        eprintln!("skipping: fixture run failed (codec unavailable?): {e:?}");
        return false;
    }
    // The fixture is only usable if it actually carries both streams.
    find_video_stream_info(mkv).ok().flatten().is_some()
        && find_subtitle_stream_info(mkv).ok().flatten().is_some()
}

/// Copying a dense stream while re-encoding a sparse co-muxed stream must not
/// hang. The video is copied (`set_video_codec("copy")`), the subtitle is
/// re-encoded (`set_subtitle_codec("subrip")`, an encode since it is not
/// "copy"); the subtitle encoder cannot open until the demuxer reaches the late
/// cue, which the dense copy stream would block forever on the pre-fix code. The
/// 30s watchdog turns that hang into a failure.
#[test]
fn streamcopy_plus_late_encoder_does_not_deadlock() {
    let mkv = tmp_path("deadlock_fixture.mkv");
    let out = tmp_path("deadlock_out.mkv");

    if !build_fixture(&mkv, 4, 2) {
        eprintln!("streamcopy_deadlock: fixture unavailable, skipping");
        return;
    }

    let scheduler = FfmpegContext::builder()
        .input(Input::from(mkv.as_str()))
        .output(
            Output::from(out.as_str())
                .set_video_codec("copy")
                .set_subtitle_codec("subrip"),
        )
        .build()
        .unwrap()
        .start()
        .unwrap();

    // Pre-fix: this hangs (the copy stream blocks the demuxer before the
    // subtitle encoder can open) and the watchdog panics. Post-fix: it finishes.
    let result = wait_with_watchdog(scheduler, 30, "streamcopy + late encoder");
    assert!(result.is_ok(), "streamcopy+encode job failed: {result:?}");

    // Both streams must reach the output. The subtitle (its only cue at 2s) being
    // present proves the demuxer advanced well past the ~0.27s point where the
    // dense copy stream used to block it forever — i.e. the deadlock is broken.
    assert!(
        find_video_stream_info(&out).ok().flatten().is_some(),
        "output must contain the copied video stream"
    );
    assert!(
        find_subtitle_stream_info(&out).ok().flatten().is_some(),
        "output must contain the re-encoded subtitle stream alongside the copy"
    );

    // matroska does not store per-stream frame counts, so re-encode just the
    // copied video into an mp4 (which does) and count it: this proves the
    // streamcopy carried the full ~120-frame video, not a truncated/empty stream
    // (a spurious queue-full would instead have failed the job above).
    let probe = tmp_path("deadlock_probe.mp4");
    let scheduler = FfmpegContext::builder()
        .input(Input::from(out.as_str()))
        .output(
            Output::from(probe.as_str())
                .add_stream_map("0:v")
                .set_video_codec("mpeg4"),
        )
        .build()
        .unwrap()
        .start()
        .unwrap();
    let result = wait_with_watchdog(scheduler, 60, "re-encode copied video to count frames");
    assert!(result.is_ok(), "probe re-encode failed: {result:?}");
    let frames = match find_video_stream_info(&probe)
        .expect("failed to probe re-encoded video")
        .expect("re-encoded output must contain the video stream")
    {
        StreamInfo::Video { nb_frames, .. } => nb_frames,
        other => panic!("expected video stream info, got {other:?}"),
    };
    assert!(
        frames >= 100,
        "the copied 4s/30fps video must carry ~120 frames (not truncated), got {frames}"
    );
}
