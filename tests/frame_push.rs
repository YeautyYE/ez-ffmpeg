//! End-to-end coverage for frame-push inputs
//! ([`ez_ffmpeg::FramePushSource`]) — VFR timestamp passthrough from Rust
//! code through a real context build, filter graph, encode, and mux.
//!
//! The rawvideo encoder ships with every FFmpeg build and accepts arbitrary
//! time bases, so the passthrough scenarios are codec-portable and exact: a
//! `.mov` output stores the microsecond time base verbatim. Readback uses
//! `PacketScanner` (muxed packet timestamps in the stream time base, no
//! decode) — the container's own numbers must reproduce every pushed `pts`
//! bit-for-bit, proving no stage re-gridded, quantized, or dropped a
//! timestamp. `FrameExtractor` is deliberately NOT the instrument here: its
//! export graph rewrites frame pts into a 1/framerate output time base
//! (documented on its sink), which would quantize a VFR grid on readback.

use ez_ffmpeg::error::Error;
use ez_ffmpeg::packet_scanner::PacketScanner;
use ez_ffmpeg::stream_info::{find_all_stream_infos, StreamInfo};
use ez_ffmpeg::{FfmpegContext, FramePushError, FramePushSource, Input, Output, PushError};
use std::time::Duration;

mod common;
use common::{tmp_path_in, wait_with_watchdog};

fn tmp_path(name: &str) -> String {
    tmp_path_in("ez_ffmpeg_frame_push", name)
}

/// rawvideo is present in every FFmpeg build and accepts any time base, so
/// the timestamp scenarios do not depend on optional encoders; `.mov`
/// carries rawvideo and adopts the stream's microsecond time base exactly.
fn raw_mov_output(path: &str) -> Output {
    Output::from(path).set_video_codec("rawvideo")
}

/// One solid-colour RGBA frame for a `w x h` source.
fn frame(w: usize, h: usize, value: u8) -> Vec<u8> {
    vec![value; w * h * 4]
}

/// Muxed video-packet `(pts, duration)` pairs, exactly rescaled from the
/// stream time base to microseconds (i128 arithmetic, no rounding for the
/// time bases these tests produce).
fn muxed_video_timing_us(path: &str) -> Vec<(i64, i64)> {
    let tb = find_all_stream_infos(path)
        .expect("probe output")
        .into_iter()
        .find_map(|info| match info {
            StreamInfo::Video { time_base, .. } => Some(time_base),
            _ => None,
        })
        .expect("output has a video stream");
    let to_us = |v: i64| -> i64 {
        (v as i128 * tb.num as i128 * 1_000_000 / tb.den as i128)
            .try_into()
            .expect("timestamp fits i64")
    };
    let mut scanner = PacketScanner::open(path).expect("open output for scan");
    let mut out = Vec::new();
    while let Some(pkt) = scanner.next_packet().expect("scan packet") {
        if pkt.is_video() {
            let pts = pkt.pts().expect("muxed video packet must carry pts");
            out.push((to_us(pts), to_us(pkt.duration())));
        }
    }
    out
}

/// Muxed video-packet pts only, in microseconds.
fn muxed_video_pts_us(path: &str) -> Vec<i64> {
    muxed_video_timing_us(path).into_iter().map(|(p, _)| p).collect()
}

/// Acceptance 1 (core): a genuinely variable grid — deltas jitter between
/// 32/33/34 ms like a real VFR capture — survives push → filter → encode →
/// mux with every pts exactly preserved and the frame count unchanged.
#[test]
fn vfr_pts_pass_through_exactly() {
    let out = tmp_path("vfr_roundtrip.mov");
    // Irregular deltas in µs; cumulative pts starting at 0.
    let deltas = [33_366i64, 32_000, 34_100, 33_367, 32_999, 34_001, 33_366];
    let mut pts_list = vec![0i64];
    for d in &deltas {
        pts_list.push(pts_list.last().unwrap() + d);
    }

    let (source, mut handle) = FramePushSource::builder(64, 48)
        .pixel_format("rgba")
        .build()
        .expect("build source");
    let scheduler = FfmpegContext::builder()
        .input(source)
        .filter_desc("[0:v]null")
        .output(raw_mov_output(&out))
        .build()
        .expect("build context")
        .start()
        .expect("start");

    for (i, &pts) in pts_list.iter().enumerate() {
        // Explicit tail duration: the last frame has no successor to derive
        // one from, and mov stores per-sample durations.
        let duration = if i + 1 == pts_list.len() {
            Some(33_366)
        } else {
            None
        };
        handle
            .push(&frame(64, 48, (i * 20) as u8), pts, duration)
            .expect("push");
    }
    handle.finish();
    wait_with_watchdog(scheduler, 30, "vfr_roundtrip").expect("job");

    let timing = muxed_video_timing_us(&out);
    assert_eq!(
        timing.iter().map(|&(p, _)| p).collect::<Vec<_>>(),
        pts_list,
        "every pushed pts must come back exactly; any drift means a stage re-gridded"
    );
    // mov derives inner durations from the pts deltas; the explicit tail
    // duration must survive as the last sample's.
    let expected_durations: Vec<i64> = deltas.iter().copied().chain([33_366]).collect();
    assert_eq!(
        timing.iter().map(|&(_, d)| d).collect::<Vec<_>>(),
        expected_durations,
        "per-sample durations must match the pushed grid, tail included"
    );
}

/// Acceptance 2: equidistant timestamps through the same VFR API — the CFR
/// special case — round-trip exactly too.
#[test]
fn equidistant_pts_pass_through_exactly() {
    let out = tmp_path("cfr_roundtrip.mov");
    let pts_list: Vec<i64> = (0..8).map(|i| i * 33_333).collect();

    let (source, mut handle) = FramePushSource::builder(64, 48).build().expect("build");
    let scheduler = FfmpegContext::builder()
        .input(source)
        .filter_desc("[0:v]null")
        .output(raw_mov_output(&out))
        .build()
        .expect("build context")
        .start()
        .expect("start");
    for &pts in &pts_list {
        handle
            .push(&frame(64, 48, 128), pts, Some(33_333))
            .expect("push");
    }
    handle.finish();
    wait_with_watchdog(scheduler, 30, "cfr_roundtrip").expect("job");

    assert_eq!(muxed_video_pts_us(&out), pts_list);
}

/// A caller-declared rational time base (90 kHz ticks) is honored: pushed
/// tick values land as the equivalent microsecond timestamps on decode.
#[test]
fn rational_time_base_is_honored() {
    let out = tmp_path("tb90k.mov");
    // 90 kHz ticks: 3000 ticks = 33_333.3µs. Use multiples of 9 to keep the
    // µs equivalents integral (9 ticks = 100µs).
    let ticks: Vec<i64> = vec![0, 2_997, 6_003, 9_000, 12_006];
    let expected_us: Vec<i64> = ticks.iter().map(|t| t * 100 / 9).collect();

    let (source, mut handle) = FramePushSource::builder(64, 48)
        .time_base(1, 90_000)
        .build()
        .expect("build");
    let scheduler = FfmpegContext::builder()
        .input(source)
        .filter_desc("[0:v]null")
        .output(raw_mov_output(&out))
        .build()
        .expect("build context")
        .start()
        .expect("start");
    for &t in &ticks {
        handle.push(&frame(64, 48, 40), t, Some(2_997)).expect("push");
    }
    handle.finish();
    wait_with_watchdog(scheduler, 30, "tb90k").expect("job");

    assert_eq!(muxed_video_pts_us(&out), expected_us);
}

/// Acceptance 4: a mixed job — pushed VFR video plus a demuxed audio input in
/// ONE context — muxes both streams; the video grid is preserved and the
/// audio stream arrives intact. Input positions: 0 = push, 1 = audio file,
/// exercising the position-translated binding ([0:v] push, audio auto-map).
#[test]
fn mixed_push_video_and_demuxed_audio() {
    // Fixture: a short AAC-in-mp4 audio file generated by the crate itself.
    let audio_src = tmp_path("audio_src.mp4");
    {
        let scheduler = FfmpegContext::builder()
            .input(Input::from("sine=frequency=440:duration=0.4").set_format("lavfi"))
            .output(Output::from(audio_src.as_str()).set_audio_codec("aac"))
            .build()
            .expect("build audio fixture")
            .start()
            .expect("start audio fixture");
        wait_with_watchdog(scheduler, 30, "audio_fixture").expect("audio fixture job");
    }

    let out = tmp_path("mixed_av.mov");
    let pts_list: Vec<i64> = vec![0, 33_366, 65_366, 99_466, 132_833];

    let (source, mut handle) = FramePushSource::builder(64, 48).build().expect("build");
    let scheduler = FfmpegContext::builder()
        .input(source)
        .input(Input::from(audio_src.as_str()))
        .filter_desc("[0:v]null")
        .output(raw_mov_output(&out).set_audio_codec("aac"))
        .build()
        .expect("build context")
        .start()
        .expect("start");
    for (i, &pts) in pts_list.iter().enumerate() {
        handle
            .push(&frame(64, 48, (i * 30) as u8), pts, Some(33_366))
            .expect("push");
    }
    handle.finish();
    wait_with_watchdog(scheduler, 30, "mixed_av").expect("job");

    assert_eq!(muxed_video_pts_us(&out), pts_list, "video grid preserved");
    let has_audio = ez_ffmpeg::stream_info::find_all_stream_infos(out.as_str())
        .expect("probe mixed output")
        .iter()
        .any(|info| matches!(info, ez_ffmpeg::stream_info::StreamInfo::Audio { .. }));
    assert!(has_audio, "audio stream must be muxed alongside the pushed video");
}

/// EOF before the first frame: dropping the handle without pushing must
/// complete the job cleanly through the zero-frame fallback (which now
/// carries the VFR time base) instead of hanging or failing configuration.
#[test]
fn eof_before_first_frame_completes() {
    let out = tmp_path("zero_frames.mov");
    let (source, handle) = FramePushSource::builder(64, 48).build().expect("build");
    let scheduler = FfmpegContext::builder()
        .input(source)
        .filter_desc("[0:v]null")
        .output(raw_mov_output(&out))
        .build()
        .expect("build context")
        .start()
        .expect("start");
    drop(handle); // EOS with zero frames pushed
    wait_with_watchdog(scheduler, 30, "zero_frames").expect("zero-frame job must complete");
}

/// Push after the job is gone fails fast with PipelineClosed (bounded by the
/// status poll, not by the queue), and the owned buffer comes back.
#[test]
fn push_after_teardown_fails_fast() {
    let out = tmp_path("teardown.mov");
    let (source, mut handle) = FramePushSource::builder(64, 48)
        .queue_capacity(1)
        .build()
        .expect("build");
    let scheduler = FfmpegContext::builder()
        .input(source)
        .filter_desc("[0:v]null")
        .output(raw_mov_output(&out))
        .build()
        .expect("build context")
        .start()
        .expect("start");
    handle.push(&frame(64, 48, 1), 0, None).expect("first push");
    scheduler.abort();

    // The worker exits on its next status poll; every subsequent push must
    // fail within bounded time instead of blocking forever.
    let deadline = std::time::Instant::now() + Duration::from_secs(20);
    let mut pts = 1;
    loop {
        match handle.push_owned(frame(64, 48, 2), pts, None) {
            Err(rejected) => {
                assert!(matches!(rejected.error(), PushError::PipelineClosed));
                break;
            }
            Ok(()) => {
                // A frame may still be admitted while the queue drains.
                assert!(
                    std::time::Instant::now() < deadline,
                    "pushes must start failing after abort"
                );
                pts += 1;
            }
        }
    }
}

/// Build-time error surfaces: every misuse is a typed error, not silence.
/// (`FfmpegContext` has no `Debug`, so the failures are unwrapped by match.)
#[test]
fn build_time_misuse_is_typed() {
    fn build_err(
        builder: ez_ffmpeg::core::context::ffmpeg_context_builder::FfmpegContextBuilder,
        what: &str,
    ) -> Error {
        match builder.build() {
            Err(err) => err,
            Ok(_) => panic!("{what}: the build must fail"),
        }
    }

    // (a) No filter graph consumes the source.
    let (source, _handle) = FramePushSource::builder(64, 48).build().expect("build");
    let err = build_err(
        FfmpegContext::builder()
            .input(source)
            .output(raw_mov_output(&tmp_path("unbound.mov"))),
        "unconsumed push source",
    );
    assert!(
        matches!(
            err,
            Error::FramePush(FramePushError::UnboundSource { input_index: 0 })
        ),
        "got {err:?}"
    );

    // (b) A non-video specifier referencing the push input.
    let (source, _handle) = FramePushSource::builder(64, 48).build().expect("build");
    let err = build_err(
        FfmpegContext::builder()
            .input(source)
            .filter_desc("[0:a]anull")
            .output(raw_mov_output(&tmp_path("nonvideo.mov"))),
        "audio specifier on a push input",
    );
    assert!(
        matches!(
            err,
            Error::FramePush(FramePushError::NotVideo { input_index: 0, .. })
        ),
        "got {err:?}"
    );

    // (c) A stream map referencing the push input.
    let (source, _handle) = FramePushSource::builder(64, 48).build().expect("build");
    let err = build_err(
        FfmpegContext::builder()
            .input(source)
            .filter_desc("[0:v]null")
            .output(raw_mov_output(&tmp_path("mapped.mov")).add_stream_map("0:v")),
        "stream map naming a push input",
    );
    assert!(
        matches!(
            err,
            Error::FramePush(FramePushError::StreamMapReferencesFramePush { input_index: 0 })
        ),
        "got {err:?}"
    );

    // (d) An Input option on the push input.
    let (source, _handle) = FramePushSource::builder(64, 48).build().expect("build");
    let err = build_err(
        FfmpegContext::builder()
            .input(Input::from(source).set_start_time_us(1_000))
            .filter_desc("[0:v]null")
            .output(raw_mov_output(&tmp_path("opt.mov"))),
        "input option on a push input",
    );
    assert!(
        matches!(
            err,
            Error::FramePush(FramePushError::UnsupportedInputOption {
                option: "set_start_time_us"
            })
        ),
        "got {err:?}"
    );

    // (e) Two pads consuming the same push input.
    let (source, _handle) = FramePushSource::builder(64, 48).build().expect("build");
    let err = build_err(
        FfmpegContext::builder()
            .input(source)
            .filter_desc("[0:v]null")
            .filter_desc("[0:v]null")
            .output(raw_mov_output(&tmp_path("twice.mov"))),
        "two pads on one push source",
    );
    assert!(
        matches!(
            err,
            Error::FramePush(FramePushError::SourceBoundTwice { input_index: 0 })
        ),
        "got {err:?}"
    );
}

/// An unlabeled single-pad graph auto-binds to the push source by position —
/// the writer-style spelling works on the general context path too.
#[test]
fn unlabeled_graph_auto_binds_push_source() {
    let out = tmp_path("autobind.mov");
    let pts_list: Vec<i64> = vec![0, 20_000, 50_000];
    let (source, mut handle) = FramePushSource::builder(64, 48).build().expect("build");
    let scheduler = FfmpegContext::builder()
        .input(source)
        .filter_desc("null")
        .output(raw_mov_output(&out))
        .build()
        .expect("build context")
        .start()
        .expect("start");
    for &pts in &pts_list {
        handle.push(&frame(64, 48, 90), pts, Some(20_000)).expect("push");
    }
    handle.finish();
    wait_with_watchdog(scheduler, 30, "autobind").expect("job");
    assert_eq!(muxed_video_pts_us(&out), pts_list);
}

/// Two frame-push inputs composited by one multi-pad graph: the overlay pad
/// (pad 1) shares the graph's frame channel with the base pad, so routing
/// runs entirely on the pad index stamped per frame. A misroute starves
/// pad 1 of frames AND of its EOF marker (the job would hang until the
/// watchdog aborts it); swapped pads would flip the output to the overlay's
/// dimensions.
#[test]
fn two_push_inputs_compose_in_one_graph() {
    let out = tmp_path("two_push_overlay.mov");
    let pts_list: Vec<i64> = vec![0, 33_366, 65_366, 99_466];

    let (base_src, mut base) = FramePushSource::builder(64, 48).build().expect("build base");
    let (over_src, mut over) = FramePushSource::builder(32, 24).build().expect("build overlay");
    let scheduler = FfmpegContext::builder()
        .input(base_src)
        .input(over_src)
        .filter_desc("[0:v][1:v]overlay")
        .output(raw_mov_output(&out))
        .build()
        .expect("build context")
        .start()
        .expect("start");
    // Interleave so framesync always has both inputs available and neither
    // bounded queue is asked to hold a whole stream.
    for &pts in &pts_list {
        base.push(&frame(64, 48, 10), pts, Some(33_366)).expect("push base");
        over.push(&frame(32, 24, 200), pts, Some(33_366)).expect("push overlay");
    }
    base.finish();
    over.finish();
    wait_with_watchdog(scheduler, 30, "two_push_overlay").expect("job");

    assert_eq!(
        muxed_video_pts_us(&out),
        pts_list,
        "identical grids must compose 1:1 with the base timestamps preserved"
    );
    let (w, h) = find_all_stream_infos(&out)
        .expect("probe overlay output")
        .into_iter()
        .find_map(|info| match info {
            StreamInfo::Video { width, height, .. } => Some((width, height)),
            _ => None,
        })
        .expect("output has a video stream");
    assert_eq!(
        (w, h),
        (64, 48),
        "the output must take the base pad's dimensions, not the overlay's"
    );
}
