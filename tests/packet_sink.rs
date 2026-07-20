//! Packet-sink delivery correctness (strict tier).
//!
//! Scenarios needing libx264 skip when the linked FFmpeg build lacks it (the
//! CI build is configured without GPL components); the strict-tier validation
//! logic itself is unit-tested encoder-free in `src/core/packet_sink/`.

mod common;

use common::{
    have_encoder, parse_avcc_au, rational_eq, recording_sink, sink_packets, tmp_path_in,
    wait_with_watchdog, SinkEv,
};
use ez_ffmpeg::packet_sink::{PacketSink, PacketSinkEvent};
use ez_ffmpeg::stream_info::StreamInfo;
use ez_ffmpeg::{AVRational, FfmpegContext, Input, Output};

const TMP_SUBDIR: &str = "ez_ffmpeg_packet_sink";

fn testsrc(seconds: u32) -> Input {
    Input::from(format!("testsrc=size=320x240:rate=25:duration={seconds}")).set_format("lavfi")
}

fn x264_output(sink: PacketSink) -> Output {
    Output::new_by_packet_sink(sink)
        .set_video_codec("libx264")
        .set_video_codec_opt("preset", "ultrafast")
}

fn run(input: Input, output: Output, scenario: &str) -> ez_ffmpeg::error::Result<()> {
    wait_with_watchdog(
        FfmpegContext::builder()
            .input(input)
            .output(output)
            .build()
            .unwrap()
            .start()
            .unwrap(),
        120,
        scenario,
    )
}

/// Validates the strict-tier avcC structural contract (v1.2 three checks).
fn assert_valid_avcc(avcc: &[u8]) {
    assert!(avcc.len() >= 7, "avcC too short: {} bytes", avcc.len());
    assert_eq!(avcc[0], 1, "configurationVersion");
    assert_eq!(avcc[4] & 0x03, 3, "lengthSizeMinusOne must be 3 (4-byte)");
    assert!(avcc[5] & 0x1F >= 1, "at least one SPS");
}

#[test]
fn strict_happy_path_single_video_stream() {
    if !have_encoder("libx264") {
        eprintln!("skipping: libx264 not available in this FFmpeg build");
        return;
    }
    let (sink, log) = recording_sink();
    // Pin the output rate: the crate stamps a stream frame rate only when the
    // pipeline decided one (fps conversion), and the strict tier reports it
    // verbatim rather than guessing.
    run(
        testsrc(2),
        x264_output(sink).set_framerate(25, 1),
        "strict_happy_path",
    )
    .expect("job failed");

    let events = log.lock().unwrap().clone();
    assert!(events.len() >= 3, "expected info + packets + end");

    // S9 exhaustive order: exactly one leading stream info, then only
    // packets, then exactly one trailing on_end — nothing after it, nothing
    // lost before it.
    let SinkEv::Info { streams, thread } = &events[0] else {
        panic!("first event must be on_stream_info, got {:?}", events[0]);
    };
    assert_eq!(streams.len(), 1);
    let info = &streams[0];
    assert!(info.is_video);
    assert!(
        info.codec_string.starts_with("avc1."),
        "WebCodecs codec string, got {}",
        info.codec_string
    );
    assert_eq!((info.width, info.height), (320, 240));
    assert_eq!(info.frame_rate, Some(AVRational { num: 25, den: 1 }));
    assert!(info.time_base.num > 0 && info.time_base.den > 0);
    assert_valid_avcc(&info.extradata);

    let last = events.len() - 1;
    let SinkEv::End { thread: end_thread } = &events[last] else {
        panic!("last event must be on_end, got {:?}", events[last]);
    };
    let mut delivered = 0usize;
    let mut prev_dts: Option<i64> = None;
    for event in &events[1..last] {
        let SinkEv::Pkt(p) = event else {
            panic!("only packets may sit between stream info and end: {event:?}");
        };
        delivered += 1;
        // Every callback runs on the one delivery thread.
        assert_eq!(p.thread, *thread);
        assert_eq!(p.thread, *end_thread);
        assert_eq!(p.stream_index, 0);
        assert_eq!(p.time_base, info.time_base, "verbatim encoder time base");
        // Single stream: the anchor stream is zero-based and non-negative.
        assert!(p.dts >= 0, "anchor-stream dts must be non-negative");
        assert!(p.pts >= p.dts, "pts must not precede dts");
        assert!(p.duration > 0, "strict tier guarantees a positive duration");
        if let Some(prev) = prev_dts {
            assert!(p.dts > prev, "dts must be strictly increasing");
        }
        prev_dts = Some(p.dts);
        // AU-complete AVCC payload, reparsed independently.
        let nals = parse_avcc_au(&p.data);
        assert!(
            !nals
                .iter()
                .any(|n| matches!(n[0] & 0x1F, 7 | 8)),
            "no in-band parameter sets in strict-tier AUs"
        );
    }
    // 2 s at 25 fps, one AU per frame.
    assert_eq!(delivered, 50, "no packet may be lost before on_end");

    let packets = sink_packets(&log);
    assert_eq!(packets[0].dts, 0, "first delivered dts anchors at zero");
    assert!(packets[0].is_key, "the stream must open on an IDR");
    let offset = packets[0].applied_offset;
    assert!(
        packets.iter().all(|p| p.applied_offset == offset),
        "one shift per stream"
    );
}

/// A4 semantic golden: the same pinned encoder configuration muxed to real
/// fMP4 must yield the same access units (NAL-for-NAL and byte-for-byte),
/// origin-aligned rational timestamps, durations and key flags as the sink
/// delivery.
#[test]
fn golden_matches_fmp4_baseline() {
    if !have_encoder("libx264") {
        eprintln!("skipping: libx264 not available in this FFmpeg build");
        return;
    }
    // Pinned configuration: no B-frames (decode order == presentation
    // order on both paths), fixed GOP, no scene cut.
    let pin = |output: Output| {
        output
            .set_video_codec("libx264")
            .set_video_codec_opt("preset", "ultrafast")
            .set_video_codec_opt("bf", "0")
            .set_video_codec_opt("g", "25")
            .set_video_codec_opt("sc_threshold", "0")
    };

    // Sink run.
    let (sink, log) = recording_sink();
    run(
        testsrc(2),
        pin(Output::new_by_packet_sink(sink)),
        "golden_sink_run",
    )
    .expect("sink job failed");
    let packets = sink_packets(&log);
    assert!(!packets.is_empty());

    // Baseline run: identical encode muxed by the real mp4 muxer into fMP4.
    let baseline_path = tmp_path_in(TMP_SUBDIR, "golden_baseline.mp4");
    run(
        testsrc(2),
        pin(Output::from(baseline_path.as_str()))
            .set_format_opt("movflags", "+frag_keyframe+empty_moov"),
        "golden_baseline_run",
    )
    .expect("baseline job failed");

    let mut scanner =
        ez_ffmpeg::packet_scanner::PacketScanner::open(baseline_path.as_str()).unwrap();
    scanner.set_capture_data(true);
    let video_tb = match scanner.video_stream().expect("baseline video stream") {
        StreamInfo::Video { time_base, .. } => *time_base,
        other => panic!("expected video stream info, got {other:?}"),
    };
    let baseline: Vec<_> = scanner
        .packets()
        .map(|p| p.expect("baseline packet"))
        .filter(|p| p.is_video())
        .collect();

    assert_eq!(
        packets.len(),
        baseline.len(),
        "sink and fMP4 must carry the same access units"
    );
    let sink_tb = packets[0].time_base;
    let sink_dts0 = packets[0].dts; // 0 by the origin contract
    let base_dts0 = baseline[0].dts().expect("baseline dts");
    for (i, (sp, bp)) in packets.iter().zip(baseline.iter()).enumerate() {
        // Primary caliber: normalized AU / NAL semantics.
        let sink_nals = parse_avcc_au(&sp.data);
        let base_nals = parse_avcc_au(bp.data().expect("captured baseline payload"));
        assert_eq!(sink_nals, base_nals, "AU {i}: NAL sequence differs");
        // Secondary caliber (pinned config): byte equality of the whole AU.
        assert_eq!(
            sp.data,
            bp.data().unwrap(),
            "AU {i}: byte-level payload differs"
        );
        // Origin-aligned rational timestamps + duration.
        let b_pts = bp.pts().expect("baseline pts") - base_dts0;
        let b_dts = bp.dts().expect("baseline dts") - base_dts0;
        assert!(
            rational_eq(sp.pts - sink_dts0, sink_tb, b_pts, video_tb),
            "AU {i}: pts differs ({} @{}/{} vs {} @{}/{})",
            sp.pts,
            sink_tb.num,
            sink_tb.den,
            b_pts,
            video_tb.num,
            video_tb.den
        );
        assert!(
            rational_eq(sp.dts - sink_dts0, sink_tb, b_dts, video_tb),
            "AU {i}: dts differs"
        );
        assert!(
            rational_eq(sp.duration, sink_tb, bp.duration(), video_tb),
            "AU {i}: duration differs"
        );
        assert_eq!(sp.is_key, bp.is_keyframe(), "AU {i}: key flag differs");
    }
}

#[test]
fn av_job_shares_one_time_origin() {
    if !have_encoder("libx264") {
        eprintln!("skipping: libx264 not available in this FFmpeg build");
        return;
    }
    let (sink, log) = recording_sink();
    let output = Output::new_by_packet_sink(sink)
        .add_stream_map("0:v")
        .add_stream_map("1:a")
        .set_video_codec("libx264")
        .set_video_codec_opt("preset", "ultrafast")
        .set_audio_codec("aac");
    let result = wait_with_watchdog(
        FfmpegContext::builder()
            .input(testsrc(2))
            .input(Input::from("sine=frequency=440:duration=2").set_format("lavfi"))
            .output(output)
            .build()
            .unwrap()
            .start()
            .unwrap(),
        120,
        "av_shared_origin",
    );
    result.expect("av job failed");

    let events = log.lock().unwrap().clone();
    let SinkEv::Info { streams, .. } = &events[0] else {
        panic!("first event must be on_stream_info");
    };
    assert_eq!(streams.len(), 2);
    let audio = streams
        .iter()
        .find(|s| s.codec_string.starts_with("mp4a.40."))
        .expect("aac stream info");
    assert!(
        !audio.extradata.is_empty(),
        "AAC must carry its AudioSpecificConfig"
    );
    assert_eq!(audio.sample_rate, 44100);
    assert!(matches!(events.last(), Some(SinkEv::End { .. })));

    let packets = sink_packets(&log);
    let anchor = &packets[0];
    assert_eq!(anchor.dts, 0, "the first delivered packet anchors at zero");

    // Every stream's shift is the SAME instant (the anchor's original dts)
    // expressed in that stream's time base: the microsecond projections must
    // agree within one tick of the coarser stream — verified through the
    // safe *_us conveniences, no manual rescaling.
    let anchor_offset_us = anchor.applied_offset_us;
    for info in streams {
        let stream_packets: Vec<_> = packets
            .iter()
            .filter(|p| p.stream_index == info.stream_index)
            .collect();
        assert!(!stream_packets.is_empty(), "both streams must deliver");
        let tick_us =
            1_000_000i64 * info.time_base.num as i64 / info.time_base.den as i64 + 1;
        let offset_us = stream_packets[0].applied_offset_us;
        assert!(
            (offset_us - anchor_offset_us).abs() <= tick_us,
            "stream {}: shared-origin shift diverges ({offset_us} vs {anchor_offset_us} us)",
            info.stream_index
        );
        // Per-stream invariants hold on the shifted timeline too.
        let mut prev: Option<i64> = None;
        for p in &stream_packets {
            assert!(p.pts >= p.dts);
            assert!(p.duration > 0);
            if let Some(prev) = prev {
                assert!(p.dts > prev, "per-stream dts monotonicity");
            }
            prev = Some(p.dts);
        }
        if !info.is_video {
            assert!(stream_packets.iter().all(|p| p.is_key));
        }
    }
}

#[test]
fn on_end_fires_after_recording_time_truncation() {
    if !have_encoder("libx264") {
        eprintln!("skipping: libx264 not available in this FFmpeg build");
        return;
    }
    let (sink, log) = recording_sink();
    run(
        testsrc(4),
        x264_output(sink).set_recording_time_us(800_000),
        "recording_time_truncation",
    )
    .expect("truncated job failed");

    let events = log.lock().unwrap().clone();
    assert!(
        matches!(events.last(), Some(SinkEv::End { .. })),
        "configured truncation is a recognized terminal state"
    );
    assert_eq!(
        events
            .iter()
            .filter(|e| matches!(e, SinkEv::End { .. }))
            .count(),
        1
    );
    let packets = sink_packets(&log);
    assert!(!packets.is_empty());
    assert!(
        packets.len() < 100,
        "0.8 s of a 4 s input must not deliver the whole stream ({} packets)",
        packets.len()
    );
}

/// The correctness review's two-output probe: a SIBLING output failing after
/// this sink drained must never let the sink report success — on_end from a
/// transiently clean snapshot followed by wait() == Err was the bug. The
/// settlement barrier waits for every job thread to settle; the sink then
/// reports the job failure through on_delivery_error and wait() keeps the
/// original error.
#[test]
fn sibling_failure_after_sink_drain_reports_delivery_error_not_end() {
    if !have_encoder("libx264") {
        eprintln!("skipping: libx264 not available in this FFmpeg build");
        return;
    }
    let (sink, log) = recording_sink();
    // Sibling container: a byte sink that fails hard after 64 KiB.
    let mut written = 0usize;
    let failing_sibling = Output::new_by_write_callback(move |buf: &[u8]| {
        written += buf.len();
        if written > 64 * 1024 {
            ffmpeg_sys_next::AVERROR(ffmpeg_sys_next::EIO)
        } else {
            buf.len() as i32
        }
    })
    .set_format("mpegts")
    .set_video_codec("mpeg4");

    let result = wait_with_watchdog(
        FfmpegContext::builder()
            .input(testsrc(3))
            .output(
                x264_output(sink)
                    .add_stream_map("0:v")
                    .set_recording_time_us(300_000),
            )
            .output(failing_sibling.add_stream_map("0:v"))
            .build()
            .unwrap()
            .start()
            .unwrap(),
        120,
        "sibling_failure",
    );
    // wait() keeps the sibling's original error.
    let error = result.expect_err("the sibling write failure must fail the job");
    assert!(
        !matches!(error, ez_ffmpeg::error::Error::PacketSink(_)),
        "the sink must not overwrite the sibling's error, got {error:?}"
    );

    let events = log.lock().unwrap().clone();
    assert!(
        !events.iter().any(|e| matches!(e, SinkEv::End { .. })),
        "on_end must not fire when the job failed elsewhere"
    );
    assert!(
        matches!(events.last(), Some(SinkEv::Error(message)) if !message.is_empty()),
        "the settled job failure surfaces as on_delivery_error, got {:?}",
        events.last()
    );
}

/// Settlement covers sibling TEARDOWN, not just sibling I/O: a sibling
/// container's custom-IO callback state is destroyed inside that worker's
/// context free (before its slot release), and a panic THERE must still
/// prevent on_end — the sink's barrier waits for the sibling's slot, behind
/// which the panic was already recorded. This is INVARIANT coverage of that
/// ordering, not a deterministic reproduction of the original late-failure
/// window (under the settled ordering the panic can land on either side of
/// the sink's drain; both converge on no-on_end). The deterministic pin of
/// the late window is the parked-filter probe in the unwind suite. Native
/// AAC on both outputs, so CI exercises it without libx264.
#[test]
fn sibling_custom_io_destruction_panic_prevents_on_end() {
    /// Panics when the sibling worker frees its output context (unless that
    /// thread is already unwinding — never double-panics).
    struct PanicOnDrop;
    impl Drop for PanicOnDrop {
        fn drop(&mut self) {
            if !std::thread::panicking() {
                panic!("sibling custom-IO capture panicked during context destruction");
            }
        }
    }

    let (sink, log) = recording_sink();
    let bomb = PanicOnDrop;
    let sibling = Output::new_by_write_callback(move |buf: &[u8]| {
        let _hold = &bomb;
        buf.len() as i32
    })
    .set_format("mpegts")
    .set_audio_codec("aac");

    let result = wait_with_watchdog(
        FfmpegContext::builder()
            .input(Input::from("sine=frequency=440:duration=1").set_format("lavfi"))
            .output(Output::from(sink).set_audio_codec("aac").add_stream_map("0:a"))
            .output(sibling.add_stream_map("0:a"))
            .build()
            .unwrap()
            .start()
            .unwrap(),
        120,
        "sibling_destruction_panic",
    );
    match result {
        Err(ez_ffmpeg::error::Error::WorkerPanicked(name)) => {
            assert!(
                name.contains("muxer"),
                "expected the sibling muxer's panic, got {name:?}"
            );
        }
        other => panic!("expected WorkerPanicked from the sibling teardown, got {other:?}"),
    }

    let events = log.lock().unwrap().clone();
    assert!(
        !events.iter().any(|e| matches!(e, SinkEv::End { .. })),
        "on_end must not fire when a sibling's teardown panicked: {events:?}"
    );
    assert!(
        events.iter().any(|e| matches!(e, SinkEv::Error(_))),
        "the sibling teardown failure surfaces as on_delivery_error: {events:?}"
    );
}

/// The multi-sink settlement rendezvous: two sinks on inputs of very
/// different lengths both reach the barrier; the SECOND registration must
/// itself wake the first, already-parked waiter (slot releases are the only
/// other notifier, and neither sink has released yet). The on_end callbacks
/// rendezvous with each other under a bounded timeout — a lost wakeup shows
/// up as exactly one side timing out.
#[test]
fn two_sinks_settle_and_both_reach_their_terminals() {
    let (tx_short, rx_short) = std::sync::mpsc::channel::<()>();
    let (tx_long, rx_long) = std::sync::mpsc::channel::<()>();
    let met_short = std::sync::Arc::new(std::sync::atomic::AtomicBool::new(false));
    let met_long = std::sync::Arc::new(std::sync::atomic::AtomicBool::new(false));

    let met = met_short.clone();
    let sink_short = ez_ffmpeg::packet_sink::PacketSink::builder(move |_pkt| Ok(()))
        .on_end(move || {
            let _ = tx_short.send(());
            met.store(
                rx_long.recv_timeout(std::time::Duration::from_secs(15)).is_ok(),
                std::sync::atomic::Ordering::Release,
            );
        })
        .build();
    let met = met_long.clone();
    let sink_long = ez_ffmpeg::packet_sink::PacketSink::builder(move |_pkt| Ok(()))
        .on_end(move || {
            let _ = tx_long.send(());
            met.store(
                rx_short.recv_timeout(std::time::Duration::from_secs(15)).is_ok(),
                std::sync::atomic::Ordering::Release,
            );
        })
        .build();

    let result = wait_with_watchdog(
        FfmpegContext::builder()
            .input(Input::from("sine=frequency=440:duration=0.1").set_format("lavfi"))
            .input(Input::from("sine=frequency=330:duration=2").set_format("lavfi"))
            .output(
                Output::from(sink_short)
                    .set_audio_codec("aac")
                    .add_stream_map("0:a"),
            )
            .output(
                Output::from(sink_long)
                    .set_audio_codec("aac")
                    .add_stream_map("1:a"),
            )
            .build()
            .unwrap()
            .start()
            .unwrap(),
        120,
        "two_sink_settlement",
    );
    result.expect("the healthy two-sink job must succeed");
    assert!(
        met_short.load(std::sync::atomic::Ordering::Acquire)
            && met_long.load(std::sync::atomic::Ordering::Acquire),
        "both on_end callbacks must run concurrently-reachable (a lost \
         settlement wakeup parks one sink past the other's rendezvous window)"
    );
}

/// The MIXED healthy/truncated settlement cycle: sink A drains cleanly and
/// waits at the barrier; paced sink B is truncated by a sibling failure.
/// B must REGISTER as settled on reaching its terminal region (uniform
/// registration) before dispatching its failure callback — a
/// register-only-when-healthy rule left A waiting on the live-but-
/// unregistered B while B's blocking terminal callback waited back on A's.
/// Both on_delivery_error callbacks rendezvous under a bounded timeout.
/// Covers the window where the truncated sink still HOLDS its slot; the
/// post-release accounting (a departed register-only sink must leave no
/// ghost registration) is covered by
/// `on_end_waits_for_live_peers_after_a_register_only_sink_departs`.
#[test]
fn mixed_truncated_and_healthy_sinks_settle_without_deadlock() {
    let (tx_a, rx_a) = std::sync::mpsc::channel::<()>();
    let (tx_b, rx_b) = std::sync::mpsc::channel::<()>();
    let met_a = std::sync::Arc::new(std::sync::atomic::AtomicBool::new(false));
    let met_b = std::sync::Arc::new(std::sync::atomic::AtomicBool::new(false));
    let delivered_a = std::sync::Arc::new(std::sync::atomic::AtomicU32::new(0));

    let (met, count) = (met_a.clone(), delivered_a.clone());
    let sink_a = ez_ffmpeg::packet_sink::PacketSink::builder(move |_pkt| {
        count.fetch_add(1, std::sync::atomic::Ordering::Release);
        Ok(())
    })
    .on_delivery_error(move |_e| {
        let _ = tx_a.send(());
        met.store(
            rx_b.recv_timeout(std::time::Duration::from_secs(10)).is_ok(),
            std::sync::atomic::Ordering::Release,
        );
    })
    .build();
    let met = met_b.clone();
    let sink_b = ez_ffmpeg::packet_sink::PacketSink::builder(move |_pkt| {
        // Pacing: B is reliably mid-delivery when the sibling failure lands.
        std::thread::sleep(std::time::Duration::from_millis(25));
        Ok(())
    })
    .on_delivery_error(move |_e| {
        let _ = tx_b.send(());
        met.store(
            rx_a.recv_timeout(std::time::Duration::from_secs(10)).is_ok(),
            std::sync::atomic::Ordering::Release,
        );
    })
    .build();

    // The sibling parks at its failure point (nothing recorded yet) until
    // the test confirms A has drained and entered settlement.
    let sibling_failing = std::sync::Arc::new(std::sync::atomic::AtomicBool::new(false));
    let sibling_go = std::sync::Arc::new(std::sync::atomic::AtomicBool::new(false));
    let (failing_cb, go_cb) = (sibling_failing.clone(), sibling_go.clone());
    let mut written = 0usize;
    let sibling = Output::new_by_write_callback(move |buf: &[u8]| {
        written += buf.len();
        if written > 1024 {
            failing_cb.store(true, std::sync::atomic::Ordering::Release);
            while !go_cb.load(std::sync::atomic::Ordering::Acquire) {
                std::thread::sleep(std::time::Duration::from_millis(2));
            }
            ffmpeg_sys_next::AVERROR(ffmpeg_sys_next::EIO)
        } else {
            buf.len() as i32
        }
    })
    .set_format("mpegts")
    .set_audio_codec("aac")
    .set_io_buffer_size(512);
    struct GateOnDrop(std::sync::Arc<std::sync::atomic::AtomicBool>);
    impl Drop for GateOnDrop {
        fn drop(&mut self) {
            self.0.store(true, std::sync::atomic::Ordering::Release);
        }
    }

    let scheduler = FfmpegContext::builder()
        .input(Input::from("sine=frequency=440:duration=0.2").set_format("lavfi"))
        .input(Input::from("sine=frequency=330:duration=2").set_format("lavfi"))
        .input(Input::from("sine=frequency=550:duration=2").set_format("lavfi"))
        .output(
            Output::from(sink_a)
                .set_audio_codec("aac")
                .add_stream_map("0:a"),
        )
        .output(
            Output::from(sink_b)
                .set_audio_codec("aac")
                .add_stream_map("1:a"),
        )
        .output(sibling.add_stream_map("2:a"))
        .build()
        .unwrap()
        .start()
        .unwrap();
    // AFTER the scheduler: on an assertion panic this drops first and
    // unparks the sibling before scheduler teardown waits on it.
    let _open_gate_on_unwind = GateOnDrop(sibling_go.clone());

    // 1. The sibling reaches its parked failure point.
    let deadline = std::time::Instant::now() + std::time::Duration::from_secs(30);
    while !sibling_failing.load(std::sync::atomic::Ordering::Acquire) {
        assert!(
            std::time::Instant::now() < deadline,
            "the sibling never reached its write-failure point"
        );
        std::thread::sleep(std::time::Duration::from_millis(2));
    }
    // 2. A drains (delivery quiescence) and sits at the settlement barrier
    // while paced B is still mid-delivery.
    let deadline = std::time::Instant::now() + std::time::Duration::from_secs(30);
    let mut last = delivered_a.load(std::sync::atomic::Ordering::Acquire);
    let mut stable_since = std::time::Instant::now();
    loop {
        std::thread::sleep(std::time::Duration::from_millis(50));
        assert!(
            std::time::Instant::now() < deadline,
            "sink A's delivery never quiesced"
        );
        let now_count = delivered_a.load(std::sync::atomic::Ordering::Acquire);
        if now_count != last {
            last = now_count;
            stable_since = std::time::Instant::now();
        } else if stable_since.elapsed() >= std::time::Duration::from_millis(500) {
            break;
        }
    }
    assert!(last > 0, "sink A must have delivered packets");
    // 3. Release the failure: B is truncated mid-delivery and must
    // register-then-dispatch; A's barrier must admit the registered B.
    sibling_go.store(true, std::sync::atomic::Ordering::Release);

    let result = wait_with_watchdog(scheduler, 120, "mixed_settlement");
    assert!(result.is_err(), "the sibling write failure must fail the job");
    assert!(
        met_a.load(std::sync::atomic::Ordering::Acquire)
            && met_b.load(std::sync::atomic::Ordering::Acquire),
        "both failure callbacks must run concurrently-reachable (a truncated \
         sink that skips settlement registration parks the healthy sibling \
         past the rendezvous window)"
    );
}

/// The settlement accounting LIFETIME: after a register-only sink departs
/// (truncated by clean stop(), registered, dispatched nothing, released its
/// slot), a healthy sink at the barrier must keep waiting while any
/// ordinary peer is still live — a ghost registration outliving its slot
/// admitted on_end while a sibling's capture destruction was still pending,
/// and its panic then failed the "already successful" job (on_end => Ok
/// violated). Complements `mixed_truncated_and_healthy_sinks_settle_...`,
/// which covers the window where the truncated sink still HOLDS its slot;
/// this test covers the accounting after it RELEASES it.
#[test]
fn on_end_waits_for_live_peers_after_a_register_only_sink_departs() {
    let a_events: std::sync::Arc<std::sync::Mutex<Vec<String>>> =
        std::sync::Arc::new(std::sync::Mutex::new(Vec::new()));
    let b_events: std::sync::Arc<std::sync::Mutex<Vec<String>>> =
        std::sync::Arc::new(std::sync::Mutex::new(Vec::new()));
    let delivered_a = std::sync::Arc::new(std::sync::atomic::AtomicU32::new(0));

    let (ev_end, ev_err, count) = (a_events.clone(), a_events.clone(), delivered_a.clone());
    let sink_a = ez_ffmpeg::packet_sink::PacketSink::builder(move |_pkt| {
        count.fetch_add(1, std::sync::atomic::Ordering::Release);
        Ok(())
    })
    .on_end(move || ev_end.lock().unwrap().push("end".to_string()))
    .on_delivery_error(move |e| ev_err.lock().unwrap().push(format!("error: {e}")))
    .build();
    let (ev_end, ev_err) = (b_events.clone(), b_events.clone());
    let sink_b = ez_ffmpeg::packet_sink::PacketSink::builder(move |_pkt| {
        // Pacing: B is reliably mid-delivery (truncated, register-only)
        // when stop() lands.
        std::thread::sleep(std::time::Duration::from_millis(25));
        Ok(())
    })
    .on_end(move || ev_end.lock().unwrap().push("end".to_string()))
    .on_delivery_error(move |e| ev_err.lock().unwrap().push(format!("error: {e}")))
    .build();

    // The sibling's custom-IO capture PARKS in its Drop (context free,
    // slot still held) and then panics — the live peer the healthy sink
    // must keep waiting for.
    let capture_parked = std::sync::Arc::new(std::sync::atomic::AtomicBool::new(false));
    let capture_gate = std::sync::Arc::new(std::sync::atomic::AtomicBool::new(false));
    struct ParkThenPanicOnDrop {
        parked: std::sync::Arc<std::sync::atomic::AtomicBool>,
        gate: std::sync::Arc<std::sync::atomic::AtomicBool>,
    }
    impl Drop for ParkThenPanicOnDrop {
        fn drop(&mut self) {
            self.parked.store(true, std::sync::atomic::Ordering::Release);
            while !self.gate.load(std::sync::atomic::Ordering::Acquire) {
                std::thread::sleep(std::time::Duration::from_millis(2));
            }
            if !std::thread::panicking() {
                panic!("sibling capture panicked during context destruction");
            }
        }
    }
    let bomb = ParkThenPanicOnDrop {
        parked: capture_parked.clone(),
        gate: capture_gate.clone(),
    };
    let sibling = Output::new_by_write_callback(move |buf: &[u8]| {
        let _hold = &bomb;
        buf.len() as i32
    })
    .set_format("mpegts")
    .set_audio_codec("aac");
    struct GateOnDrop(std::sync::Arc<std::sync::atomic::AtomicBool>);
    impl Drop for GateOnDrop {
        fn drop(&mut self) {
            self.0.store(true, std::sync::atomic::Ordering::Release);
        }
    }

    let scheduler = FfmpegContext::builder()
        .input(Input::from("sine=frequency=440:duration=0.2").set_format("lavfi"))
        .input(Input::from("sine=frequency=330:duration=2").set_format("lavfi"))
        .input(Input::from("sine=frequency=550:duration=2").set_format("lavfi"))
        .output(
            Output::from(sink_a)
                .set_audio_codec("aac")
                .add_stream_map("0:a"),
        )
        .output(
            Output::from(sink_b)
                .set_audio_codec("aac")
                .add_stream_map("1:a"),
        )
        .output(sibling.add_stream_map("2:a"))
        .build()
        .unwrap()
        .start()
        .unwrap();
    // AFTER the scheduler (declaration-order lesson): an assertion panic
    // opens the capture gate before anything waits on the parked sibling.
    let _open_gate_on_unwind = GateOnDrop(capture_gate.clone());

    // 1. A drains and sits at the settlement barrier.
    let deadline = std::time::Instant::now() + std::time::Duration::from_secs(30);
    let mut last = delivered_a.load(std::sync::atomic::Ordering::Acquire);
    let mut stable_since = std::time::Instant::now();
    loop {
        std::thread::sleep(std::time::Duration::from_millis(50));
        assert!(
            std::time::Instant::now() < deadline,
            "sink A's delivery never quiesced"
        );
        let now_count = delivered_a.load(std::sync::atomic::Ordering::Acquire);
        if now_count != last {
            last = now_count;
            stable_since = std::time::Instant::now();
        } else if stable_since.elapsed() >= std::time::Duration::from_millis(500) {
            break;
        }
    }
    assert!(last > 0, "sink A must have delivered packets");

    // 2. Clean stop(): B truncates mid-delivery (register-only, silent per
    // the stop contract, releases its slot); the sibling's teardown parks
    // in its capture Drop with its slot still held. stop() blocks until
    // every slot releases, so it runs on a helper thread.
    let stop_thread = std::thread::spawn(move || scheduler.stop());
    let deadline = std::time::Instant::now() + std::time::Duration::from_secs(30);
    while !capture_parked.load(std::sync::atomic::Ordering::Acquire) {
        assert!(
            std::time::Instant::now() < deadline,
            "the sibling capture never reached its parked destruction"
        );
        std::thread::sleep(std::time::Duration::from_millis(2));
    }
    // 3. THE DISCRIMINATOR: while the never-registered sibling is live
    // (parked in its capture Drop), A's barrier must hold — no on_end.
    // A ghost registration from the departed B admitted A here.
    std::thread::sleep(std::time::Duration::from_millis(600));
    assert!(
        a_events.lock().unwrap().is_empty(),
        "no terminal may fire while a never-registered peer is still live: {:?}",
        a_events.lock().unwrap()
    );

    // 4. Release: the capture panics (recorded as the sibling worker's
    // panic BEFORE its slot release), A passes the barrier and reports the
    // settled failure — never on_end.
    capture_gate.store(true, std::sync::atomic::Ordering::Release);
    let result = stop_thread.join().expect("stop() must return");
    match result {
        Err(ez_ffmpeg::error::Error::WorkerPanicked(name)) => {
            assert!(name.contains("muxer"), "expected the sibling muxer, got {name:?}");
        }
        other => panic!("expected WorkerPanicked from the sibling capture, got {other:?}"),
    }
    let evs = a_events.lock().unwrap().clone();
    assert!(
        !evs.iter().any(|e| e == "end"),
        "on_end must not fire when the settled job failed: {evs:?}"
    );
    assert!(
        evs.iter().any(|e| e.starts_with("error")),
        "the settled failure surfaces as on_delivery_error: {evs:?}"
    );
    assert!(
        b_events.lock().unwrap().is_empty(),
        "a cleanly stopped mid-delivery sink stays silent: {:?}",
        b_events.lock().unwrap()
    );
}

/// Multi-sink terminal containment: one sink's on_end panics AFTER the job
/// settled; the panic is contained — the sibling sink's on_end still stands
/// and wait() returns Ok. Without containment the panic re-arms the worker
/// panic publisher and rewrites the settled result to WorkerPanicked.
#[test]
fn sibling_sink_terminal_panic_never_rewrites_the_settled_result() {
    let started_short = std::sync::Arc::new(std::sync::atomic::AtomicBool::new(false));
    let ended_long = std::sync::Arc::new(std::sync::atomic::AtomicBool::new(false));

    let started = started_short.clone();
    let sink_short = ez_ffmpeg::packet_sink::PacketSink::builder(move |_pkt| Ok(()))
        .on_end(move || {
            started.store(true, std::sync::atomic::Ordering::Release);
            panic!("injected terminal-callback panic");
        })
        .build();
    let ended = ended_long.clone();
    let sink_long = ez_ffmpeg::packet_sink::PacketSink::builder(move |_pkt| Ok(()))
        .on_end(move || ended.store(true, std::sync::atomic::Ordering::Release))
        .build();

    let result = wait_with_watchdog(
        FfmpegContext::builder()
            .input(Input::from("sine=frequency=440:duration=0.1").set_format("lavfi"))
            .input(Input::from("sine=frequency=330:duration=2").set_format("lavfi"))
            .output(
                Output::from(sink_short)
                    .set_audio_codec("aac")
                    .add_stream_map("0:a"),
            )
            .output(
                Output::from(sink_long)
                    .set_audio_codec("aac")
                    .add_stream_map("1:a"),
            )
            .build()
            .unwrap()
            .start()
            .unwrap(),
        120,
        "terminal_panic_containment",
    );
    assert!(
        started_short.load(std::sync::atomic::Ordering::Acquire),
        "the probe must actually reach the panicking on_end"
    );
    assert!(
        ended_long.load(std::sync::atomic::Ordering::Acquire),
        "the sibling sink's on_end must be delivered"
    );
    result.expect(
        "a terminal-callback panic is contained and must not rewrite the settled job result",
    );
}

/// The finalize-gating probe: a packet sink must never hold the
/// scheduler-wide I/O finalize exemption. With a sink waiting in its terminal
/// coordinator and a sibling container blocked on an unread TCP peer,
/// stop() must still cut the sibling within the grace window and return —
/// an (ungated) sink-held finalize guard would suppress the cut and hang.
#[test]
fn blocked_sibling_stays_interruptible_while_sink_finalizes() {
    if !have_encoder("libx264") {
        eprintln!("skipping: libx264 not available in this FFmpeg build");
        return;
    }
    let listener = std::net::TcpListener::bind("127.0.0.1:0").unwrap();
    let addr = listener.local_addr().unwrap();
    let accept_thread = std::thread::spawn(move || {
        if let Ok((stream, _)) = listener.accept() {
            // Hold the socket open without reading until the test ends.
            std::thread::sleep(std::time::Duration::from_secs(30));
            drop(stream);
        }
    });

    let (sink, log) = recording_sink();
    let scheduler = FfmpegContext::builder()
        // High-entropy source so the non-reading peer's buffers fill fast.
        .input(Input::from("testsrc2=s=1280x720:r=30").set_format("lavfi"))
        .output(
            x264_output(sink)
                .add_stream_map("0:v")
                .set_recording_time_us(300_000),
        )
        .output(
            Output::from(format!("tcp://{addr}?send_buffer_size=16384"))
                .add_stream_map("0:v")
                .set_format("mpegts")
                .set_video_codec("mpeg4")
                .set_video_codec_opt("qscale", "1"),
        )
        .build()
        .unwrap()
        .start()
        .unwrap();
    // Let the sink drain (300 ms of media) and the sibling wedge on the
    // unread socket.
    std::thread::sleep(std::time::Duration::from_millis(1500));

    let (tx, rx) = std::sync::mpsc::channel();
    std::thread::spawn(move || {
        let _ = tx.send(scheduler.stop());
    });
    let result = rx
        .recv_timeout(std::time::Duration::from_secs(30))
        .expect("stop() hung: the sink held the finalize exemption over a blocked sibling");
    // The sibling was either cut mid-write (an error) or exited between
    // writes (clean); both are acceptable — the property under test is that
    // stop() RETURNS.
    let _ = result;
    let events = log.lock().unwrap();
    let ends = events
        .iter()
        .filter(|e| matches!(e, SinkEv::End { .. }))
        .count();
    let errors = events
        .iter()
        .filter(|e| matches!(e, SinkEv::Error(_)))
        .count();
    assert!(
        ends + errors <= 1,
        "at most one terminal sink event may fire ({ends} end, {errors} error)"
    );
    drop(accept_thread);
}

/// The round-7 correctness probe: a delivered on_end must imply wait() ==
/// Ok. The single documented carve-out is user code failing AFTER the fact —
/// here a callback capture whose Drop panics during teardown: the panic is
/// caught at the worker's defined destruction point, logged, and must NOT
/// override the settled result. Native AAC, so CI exercises it everywhere.
#[test]
fn on_end_survives_a_panicking_capture_destructor() {
    struct PanicOnDrop;
    impl Drop for PanicOnDrop {
        fn drop(&mut self) {
            panic!("consumer capture panicked during teardown");
        }
    }

    let ended = std::sync::Arc::new(std::sync::atomic::AtomicBool::new(false));
    let ended_cb = ended.clone();
    let capture = PanicOnDrop;
    let sink = ez_ffmpeg::packet_sink::PacketSink::builder(move |_pkt| {
        let _hold = &capture;
        Ok(())
    })
    .on_end(move || {
        ended_cb.store(true, std::sync::atomic::Ordering::Release);
    })
    .build();

    let result = wait_with_watchdog(
        FfmpegContext::builder()
            .input(Input::from("sine=frequency=440:duration=1").set_format("lavfi"))
            .output(Output::from(sink).set_audio_codec("aac"))
            .build()
            .unwrap()
            .start()
            .unwrap(),
        60,
        "on_end_drop_panic",
    );
    assert!(
        ended.load(std::sync::atomic::Ordering::Acquire),
        "the healthy job must deliver on_end"
    );
    result.expect("a capture Drop panic after on_end must not fail the settled job");
}

/// Native-AAC end-to-end delivery through the owned-run iterator — runs on
/// FFmpeg builds without GPL components, so CI exercises real delivery (not
/// just compile/skip paths) everywhere. Also covers into_events(): events
/// stream while the job runs and the scheduler is joined exactly once.
#[test]
fn aac_only_delivery_streams_through_into_events() {
    let (sink, receiver) =
        ez_ffmpeg::packet_sink::PacketSink::channel(std::num::NonZeroUsize::new(4).unwrap());
    let scheduler = FfmpegContext::builder()
        .input(Input::from("sine=frequency=440:duration=2").set_format("lavfi"))
        .output(Output::from(sink).set_audio_codec("aac"))
        .build()
        .unwrap()
        .start()
        .unwrap();

    let mut saw_info = false;
    let mut saw_end = false;
    let mut packets = 0usize;
    let mut prev_dts: Option<i64> = None;
    for event in receiver.into_events(scheduler) {
        match event.expect("job failed") {
            ez_ffmpeg::packet_sink::PacketSinkEvent::StreamInfo(streams) => {
                assert!(!saw_info, "stream info must arrive exactly once");
                saw_info = true;
                assert_eq!(streams.len(), 1);
                let audio = streams[0].audio().expect("audio configuration");
                assert_eq!(audio.codec_string(), "mp4a.40.2");
                assert!(!audio.codec_config().is_empty(), "ASC must be present");
                assert_eq!(audio.sample_rate(), 44100);
                assert!(!audio.channel_layout().is_empty());
            }
            ez_ffmpeg::packet_sink::PacketSinkEvent::Packet(packet) => {
                assert!(saw_info, "no packet before the stream info");
                assert!(!saw_end, "no packet after End");
                packets += 1;
                assert!(packet.is_key(), "every AAC frame is a sync sample");
                assert!(packet.duration() > 0);
                if let Some(prev) = prev_dts {
                    assert!(packet.dts() > prev, "per-stream dts monotonicity");
                }
                prev_dts = Some(packet.dts());
                assert!(!packet.data().is_empty());
            }
            ez_ffmpeg::packet_sink::PacketSinkEvent::End => saw_end = true,
            other => panic!("unexpected event {other:?}"),
        }
    }
    assert!(saw_info && saw_end, "info and End must both arrive");
    assert!(packets > 50, "2 s of AAC is ~86 frames, got {packets}");
}

#[test]
fn channel_adapter_delivers_owned_events_in_order() {
    if !have_encoder("libx264") {
        eprintln!("skipping: libx264 not available in this FFmpeg build");
        return;
    }
    let (sink, receiver) = PacketSink::channel(std::num::NonZeroUsize::new(1024).unwrap());
    run(testsrc(1), x264_output(sink), "channel_adapter").expect("job failed");

    let events: Vec<_> = receiver.iter().collect();
    assert!(matches!(events.first(), Some(PacketSinkEvent::StreamInfo(s)) if s.len() == 1));
    assert!(matches!(events.last(), Some(PacketSinkEvent::End)));
    let packets: Vec<_> = events
        .iter()
        .filter_map(|e| match e {
            PacketSinkEvent::Packet(p) => Some(p),
            _ => None,
        })
        .collect();
    assert_eq!(packets.len(), 25);
    assert_eq!(events.len(), packets.len() + 2, "info + packets + end only");
    assert!(packets[0].is_key());
    assert_eq!(packets[0].dts(), 0);
    parse_avcc_au(packets[0].data());
}
