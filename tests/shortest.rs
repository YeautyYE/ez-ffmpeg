//! Integration coverage for `Output::set_shortest` (FFmpeg `-shortest`).
//!
//! The sync-queue engine is unit-tested in
//! `src/core/scheduler/sync_queue.rs`. Here we prove the end-to-end behavior
//! against real encoders:
//!   * with two encoded audio/video streams, the longer stream is truncated to
//!     the shorter one BEFORE the output ends (the frame-level `sq_enc` path),
//!     and
//!   * an INFINITE longer stream is actually stopped — a broken `-shortest`
//!     shows up as a hang, which the watchdog turns into a test failure, not a
//!     suite timeout.
//!
//! Inputs are synthetic lavfi sources, so no media fixtures are needed and the
//! tests run wherever FFmpeg's `lavfi`, `mpeg4` and `aac` are available.

use ez_ffmpeg::stream_info::{find_audio_stream_info, find_video_stream_info, StreamInfo};
use ez_ffmpeg::{FfmpegContext, Input, Output};

mod common;
use common::{tmp_path_in, wait_with_watchdog};

fn tmp_path(name: &str) -> String {
    tmp_path_in("ez_ffmpeg_shortest_tests", name)
}

/// An infinite lavfi video source (30 fps): `-shortest` must stop it.
fn lavfi_video_infinite() -> Input {
    Input::from("color=c=black:s=320x240:r=30").set_format("lavfi")
}

/// A finite lavfi video source of `secs` seconds at 30 fps.
fn lavfi_video_secs(secs: u32) -> Input {
    Input::from(format!("color=c=black:s=320x240:r=30:d={secs}")).set_format("lavfi")
}

/// A finite lavfi audio source of `secs` seconds.
fn lavfi_audio_secs(secs: u32) -> Input {
    Input::from(format!(
        "sine=frequency=440:duration={secs}:sample_rate=44100"
    ))
    .set_format("lavfi")
}

fn video_nb_frames(url: &str) -> i64 {
    match find_video_stream_info(url)
        .unwrap()
        .expect("output has no video stream")
    {
        StreamInfo::Video { nb_frames, .. } => nb_frames,
        other => panic!("expected video stream info, got {other:?}"),
    }
}

fn audio_duration_secs(url: &str) -> f64 {
    match find_audio_stream_info(url)
        .unwrap()
        .expect("output has no audio stream")
    {
        StreamInfo::Audio {
            duration,
            time_base,
            ..
        } => duration as f64 * time_base.num as f64 / time_base.den as f64,
        other => panic!("expected audio stream info, got {other:?}"),
    }
}

fn video_duration_secs(url: &str) -> f64 {
    match find_video_stream_info(url)
        .unwrap()
        .expect("output has no video stream")
    {
        StreamInfo::Video {
            duration,
            time_base,
            ..
        } => duration as f64 * time_base.num as f64 / time_base.den as f64,
        other => panic!("expected video stream info, got {other:?}"),
    }
}

/// `sq_enc` frame-level truncation stops an INFINITE longer stream. Video is an
/// endless lavfi source; audio is 2 s. With `-shortest` and two encoded streams,
/// the video must be cut at ~2 s and the job must terminate — a hang here means
/// the longer stream was never stopped.
#[test]
fn shortest_stops_infinite_video_at_short_audio() {
    let out = tmp_path("shortest_infinite_video.mp4");
    let scheduler = FfmpegContext::builder()
        .input(lavfi_video_infinite())
        .input(lavfi_audio_secs(2))
        .output(
            Output::from(out.as_str())
                .set_video_codec("mpeg4")
                .set_audio_codec("aac")
                .set_shortest(true),
        )
        .build()
        .unwrap()
        .start()
        .unwrap();

    let result = wait_with_watchdog(scheduler, 60, "shortest stops infinite video");
    assert!(result.is_ok(), "-shortest job failed: {result:?}");

    // 2 s at 30 fps ~= 60 frames; allow slack for the exact boundary frame and
    // encoder flush. The key assertion is that it is bounded (not thousands).
    let frames = video_nb_frames(&out);
    assert!(
        (45..=75).contains(&frames),
        "video should be truncated to ~2s (~60 frames), got {frames}"
    );
    let vdur = video_duration_secs(&out);
    assert!(
        (1.5..=2.6).contains(&vdur),
        "video duration should be ~2s, got {vdur:.3}s"
    );
}

/// Three encoded streams exercise the `sq_enc` cascade-cut path. Audio (10 s) is
/// the shortest; two INFINITE videos are both cascade-cut when it ends. With 3+
/// streams, a cascade-cut stream that keeps its now-stale timestamp in the input
/// balancer can choke a peer that another cascade-cut stream's drain is waiting
/// on — a deadlock that a 2-stream job never hits (the sole other stream is
/// always the trailing one). Both videos must be cut at ~10 s and the job must
/// terminate; a hang here means a cascade-cut stream never left the balancer.
#[test]
fn shortest_stops_three_streams_with_two_infinite_videos() {
    let out = tmp_path("shortest_three_streams.mp4");
    let scheduler = FfmpegContext::builder()
        // Long enough that input balancing starts before this synthetic source
        // reaches EOF. With 2 s the demuxer can win the startup race and hide a
        // bug that chokes the finite source mid-stream.
        .input(lavfi_audio_secs(10))
        .input(lavfi_video_infinite())
        .input(lavfi_video_infinite())
        .output(
            Output::from(out.as_str())
                .add_stream_map("0:a")
                .add_stream_map("1:v")
                .add_stream_map("2:v")
                .set_video_codec("mpeg4")
                .set_audio_codec("aac")
                .set_shortest(true),
        )
        .build()
        .unwrap()
        .start()
        .unwrap();

    let result = wait_with_watchdog(scheduler, 60, "shortest stops three streams");
    assert!(result.is_ok(), "-shortest 3-stream job failed: {result:?}");

    // Both infinite videos are cut at the 10 s audio bound. `find_video_stream_info`
    // reports the first video; a bounded count (not thousands) proves the cut —
    // and termination at all proves BOTH infinite videos were stopped. The upper
    // bound is looser than the two-stream test: with three encoders competing for
    // cores the cascade can land a few frames late under heavy parallelism, but a
    // runaway (un-cut) infinite source would be in the thousands, so this still
    // fails loudly on a real regression.
    let frames = video_nb_frames(&out);
    assert!(
        (240..=390).contains(&frames),
        "video should be truncated near the 10s audio bound, got {frames}"
    );
}

/// Two FINITE encoded streams: the longer (video, 4 s) is truncated to the
/// shorter (audio, 1 s). Proves the truncation cut, not just termination.
#[test]
fn shortest_truncates_longer_finite_stream() {
    let out = tmp_path("shortest_finite_streams.mp4");
    let scheduler = FfmpegContext::builder()
        .input(lavfi_video_secs(4))
        .input(lavfi_audio_secs(1))
        .output(
            Output::from(out.as_str())
                .set_video_codec("mpeg4")
                .set_audio_codec("aac")
                .set_shortest(true),
        )
        .build()
        .unwrap()
        .start()
        .unwrap();

    let result = wait_with_watchdog(scheduler, 60, "shortest truncates longer finite stream");
    assert!(result.is_ok(), "-shortest job failed: {result:?}");

    // Bound = 1 s audio; the 4 s video must be cut to ~1 s (~30 frames), NOT 4 s
    // (~120 frames).
    let frames = video_nb_frames(&out);
    assert!(
        (20..=45).contains(&frames),
        "video should be truncated to ~1s (~30 frames), got {frames}"
    );
    let adur = audio_duration_secs(&out);
    assert!(
        (0.8..=1.4).contains(&adur),
        "audio (the shortest) should be ~1s, got {adur:.3}s"
    );
}

/// Without `-shortest`, the same two finite streams keep their own lengths — the
/// video runs its full 4 s. Guards against `-shortest` bleeding into the default.
#[test]
fn without_shortest_streams_keep_their_own_length() {
    let out = tmp_path("no_shortest_streams.mp4");
    let scheduler = FfmpegContext::builder()
        .input(lavfi_video_secs(4))
        .input(lavfi_audio_secs(1))
        .output(
            Output::from(out.as_str())
                .set_video_codec("mpeg4")
                .set_audio_codec("aac"),
        )
        .build()
        .unwrap()
        .start()
        .unwrap();

    let result = wait_with_watchdog(scheduler, 60, "no -shortest keeps lengths");
    assert!(result.is_ok(), "job failed: {result:?}");

    // The video keeps its full 4 s (~120 frames), well past the 1 s audio.
    let frames = video_nb_frames(&out);
    assert!(
        frames >= 100,
        "without -shortest the 4s video must keep ~120 frames, got {frames}"
    );
}

/// `-shortest` + `set_max_video_frames(N)` must still bound the output. Two INFINITE
/// videos have no natural end, so the per-stream frame cap is the ONLY terminator.
/// The cap is wired into the sync queue (`sq_enc` -> `sq_limit_frames`), so both are
/// cut at ~N frames and the job ends. Before the cap was wired, `set_max_*_frames`
/// was silently ignored whenever `sq_enc` was active, so this job ran forever — a
/// hang the watchdog turns into a failure.
#[test]
fn shortest_with_max_frames_bounds_infinite_streams() {
    let out = tmp_path("shortest_max_frames.mp4");
    let scheduler = FfmpegContext::builder()
        .input(lavfi_video_infinite())
        .input(lavfi_video_infinite())
        .output(
            Output::from(out.as_str())
                .add_stream_map("0:v")
                .add_stream_map("1:v")
                .set_video_codec("mpeg4")
                .set_shortest(true)
                .set_max_video_frames(30),
        )
        .build()
        .unwrap()
        .start()
        .unwrap();

    let result = wait_with_watchdog(
        scheduler,
        60,
        "shortest + max_video_frames bounds infinite streams",
    );
    assert!(
        result.is_ok(),
        "-shortest + max_frames job failed: {result:?}"
    );

    // Both infinite videos are capped at 30 frames. A bounded count (not thousands)
    // proves the cap took effect; termination at all proves neither infinite source
    // ran away.
    let frames = video_nb_frames(&out);
    assert!(
        (20..=45).contains(&frames),
        "video should be capped near 30 frames, got {frames}"
    );
}

/// H1 regression: an encoder that stops at `recording_time` (LimitReached)
/// must still tell the sync queue its stream is finished. Coarse video frames
/// (2 fps: half-second end timestamps) plus fine audio frames pin the queue
/// head at the audio's boundary; without the finish, the video's post-limit
/// frames gate forever, its EOF marker never reaches the muxer, and the job
/// hangs — the heartbeat mathematically cannot fire with a stuck window this
/// far below the 10s default buffer.
#[test]
fn shortest_recording_time_finishes_sibling_with_coarse_frames() {
    let out = tmp_path("shortest_recording_time.mp4");
    let scheduler = FfmpegContext::builder()
        .input(Input::from("sine=frequency=440:sample_rate=44100").set_format("lavfi"))
        .input(Input::from("color=c=black:s=320x240:r=2:d=4").set_format("lavfi"))
        .output(
            Output::from(out.as_str())
                .add_stream_map("0:a")
                .add_stream_map("1:v")
                .set_audio_codec("aac")
                .set_video_codec("mpeg4")
                .set_shortest(true)
                .set_recording_time_us(2_000_000),
        )
        .build()
        .unwrap()
        .start()
        .unwrap();

    let result = wait_with_watchdog(scheduler, 60, "recording_time under -shortest");
    assert!(result.is_ok(), "recording_time job failed: {result:?}");

    let frames = video_nb_frames(&out);
    assert!(
        (3..=6).contains(&frames),
        "2 fps video cut at ~2s should hold 3..=6 frames, got {frames}"
    );
    let audio_secs = audio_duration_secs(&out);
    assert!(
        (1.5..=2.6).contains(&audio_secs),
        "audio should stop near the 2s recording_time, got {audio_secs}"
    );
}

/// H1 with three sync-queue members: the encoder-side finish must cascade
/// through N>2 streams too.
#[test]
fn shortest_recording_time_cascades_across_three_streams() {
    let out = tmp_path("shortest_recording_time_three.mp4");
    let scheduler = FfmpegContext::builder()
        .input(Input::from("sine=frequency=440:sample_rate=44100").set_format("lavfi"))
        .input(Input::from("color=c=black:s=320x240:r=2:d=4").set_format("lavfi"))
        .input(Input::from("color=c=white:s=320x240:r=30").set_format("lavfi"))
        .output(
            Output::from(out.as_str())
                .add_stream_map("0:a")
                .add_stream_map("1:v")
                .add_stream_map("2:v")
                .set_audio_codec("aac")
                .set_video_codec("mpeg4")
                .set_shortest(true)
                .set_recording_time_us(2_000_000),
        )
        .build()
        .unwrap()
        .start()
        .unwrap();

    let result = wait_with_watchdog(scheduler, 60, "recording_time cascade across three");
    assert!(
        result.is_ok(),
        "three-member recording_time job failed: {result:?}"
    );

    // The first video stream is the 2 fps one: bounded by the 2s limit.
    let frames = video_nb_frames(&out);
    assert!(
        (3..=6).contains(&frames),
        "2 fps video cut at ~2s should hold 3..=6 frames, got {frames}"
    );
}

/// H2 regression: a cascade-cut encoder must hand its input channel back to a
/// SHARED filtergraph before waiting out its drain. The graph is paced to
/// wall clock and its `[slow]` branch dries up at 1.5s without reaching EOF,
/// so pre-fix the graph parks in send() on the cut `[fast]` encoder's full
/// channel while `[fast]` waits on a head only `[slow]` could advance — a
/// three-party deadlock the heartbeat cannot break.
#[test]
fn shortest_shared_graph_survives_cascade_cut() {
    let out = tmp_path("shortest_shared_graph.mp4");
    let scheduler = FfmpegContext::builder()
        .input(
            Input::from("color=c=black:s=320x240:r=30:d=6")
                .set_format("lavfi")
                .set_readrate(1.0),
        )
        .input(Input::from("sine=frequency=440:duration=2").set_format("lavfi"))
        .filter_desc("[0:v]split=2[fast][pre];[pre]select='lt(n,45)'[slow]")
        .output(
            Output::from(out.as_str())
                .add_stream_map("[fast]")
                .add_stream_map("[slow]")
                .add_stream_map("1:a")
                .set_video_codec("mpeg4")
                .set_audio_codec("aac")
                .set_shortest(true),
        )
        .build()
        .unwrap()
        .start()
        .unwrap();

    let result = wait_with_watchdog(scheduler, 60, "shared-graph cascade cut");
    assert!(result.is_ok(), "shared-graph job failed: {result:?}");

    let frames = video_nb_frames(&out);
    assert!(
        (30..=75).contains(&frames),
        "the fast branch should truncate near the 2s audio, got {frames}"
    );
}

/// H1 + H2 composed: an encoder-side finish (recording_time) on streams fed
/// by one shared graph — the finish must reach the queue AND the graph must
/// get both channels back.
#[test]
fn shortest_recording_time_with_shared_graph_composes() {
    let out = tmp_path("shortest_rt_shared_graph.mp4");
    let scheduler = FfmpegContext::builder()
        .input(Input::from("color=c=black:s=320x240:r=30:d=6").set_format("lavfi"))
        .input(Input::from("sine=frequency=440:sample_rate=44100").set_format("lavfi"))
        .filter_desc("[0:v]split=2[a][b]")
        .output(
            Output::from(out.as_str())
                .add_stream_map("[a]")
                .add_stream_map("[b]")
                .add_stream_map("1:a")
                .set_video_codec("mpeg4")
                .set_audio_codec("aac")
                .set_shortest(true)
                .set_recording_time_us(2_000_000),
        )
        .build()
        .unwrap()
        .start()
        .unwrap();

    let result = wait_with_watchdog(scheduler, 60, "recording_time + shared graph");
    assert!(result.is_ok(), "composed job failed: {result:?}");

    let frames = video_nb_frames(&out);
    assert!(
        (40..=75).contains(&frames),
        "30 fps video cut at ~2s should hold 40..=75 frames, got {frames}"
    );
}

/// A cascade-finish must not end an encoder BEFORE it has opened: opening
/// publishes the ready signal the muxer's init waits on. Deterministic
/// pre-open cascade: `set_max_video_frames(0)` finishes the video stream at
/// sync-queue SETUP, the fast 1s audio's first frames propagate that finish
/// into the encoder-visible flag within milliseconds — while the video leg
/// routes through `realtime,select='gte(n,60)'`, so its first frame cannot
/// reach its encoder before ~2s. Every video-encoder loop iteration in
/// between sees the cascade-finish while still unopened. Honoring it there
/// (the step-0 break without the `opened` gate) kills the encoder before it
/// ever publishes ready and the muxer never initializes; the watchdog (or
/// the resulting error) turns that into a failure.
#[test]
fn zero_frame_cap_cascade_before_open_still_opens_the_encoder() {
    let out = tmp_path("zero_cap_pre_open.mp4");
    let scheduler = FfmpegContext::builder()
        .input(lavfi_video_infinite())
        .input(lavfi_audio_secs(1))
        .filter_desc("[0:v]realtime,select='gte(n,60)'[slow]")
        .output(
            Output::from(out.as_str())
                .add_stream_map("[slow]")
                .add_stream_map("1:a")
                .set_video_codec("mpeg4")
                .set_audio_codec("aac")
                .set_shortest(true)
                .set_max_video_frames(0),
        )
        .build()
        .unwrap()
        .start()
        .unwrap();

    let result = wait_with_watchdog(
        scheduler,
        60,
        "zero-frame cap + delayed first frame still opens the encoder",
    );
    assert!(result.is_ok(), "pre-open cascade job failed: {result:?}");

    // The muxer finalized with the audio intact: the video encoder opened
    // (publishing ready) despite its stream being finished before its first
    // frame ever arrived.
    let audio_secs = audio_duration_secs(&out);
    assert!(
        audio_secs > 0.5,
        "audio should survive the pre-open video cascade, got {audio_secs}s"
    );
}
