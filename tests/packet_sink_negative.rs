//! Packet-sink rejection paths: build-time validation, the strict-tier
//! encoder whitelist, open-GOP `is_key` semantics, and the failing-callback
//! circuit breaker.

mod common;

use common::{have_encoder, recording_sink, sink_packets, tmp_path_in, wait_with_watchdog, SinkEv};
use ez_ffmpeg::error::{Error, PacketSinkError};
use ez_ffmpeg::packet_sink::PacketSink;
use ez_ffmpeg::{FfmpegContext, Input, Output};

const TMP_SUBDIR: &str = "ez_ffmpeg_packet_sink_negative";

fn testsrc(seconds: u32) -> Input {
    Input::from(format!("testsrc=size=320x240:rate=25:duration={seconds}")).set_format("lavfi")
}

fn sine(seconds: u32) -> Input {
    Input::from(format!("sine=frequency=440:duration={seconds}")).set_format("lavfi")
}

fn build_err(input: Input, output: Output) -> Error {
    match FfmpegContext::builder().input(input).output(output).build() {
        Ok(_) => panic!("build() must reject this packet-sink configuration"),
        Err(e) => e,
    }
}

fn noop_sink() -> PacketSink {
    PacketSink::discard()
}

#[test]
fn build_rejects_muxer_only_options() {
    let cases: Vec<(&str, Output)> = vec![
        (
            "set_format",
            Output::new_by_packet_sink(noop_sink()).set_format("mp4"),
        ),
        (
            "set_seek_callback",
            Output::new_by_packet_sink(noop_sink()).set_seek_callback(|_, _| 0),
        ),
        (
            "set_io_buffer_size",
            Output::new_by_packet_sink(noop_sink()).set_io_buffer_size(128 * 1024),
        ),
        (
            "set_video_bsf",
            Output::new_by_packet_sink(noop_sink()).set_video_bsf("h264_mp4toannexb"),
        ),
        (
            "set_audio_bsf",
            Output::new_by_packet_sink(noop_sink()).set_audio_bsf("aac_adtstoasc"),
        ),
        (
            "set_format_opt",
            Output::new_by_packet_sink(noop_sink()).set_format_opt("movflags", "+faststart"),
        ),
        (
            "set_subtitle_codec",
            Output::new_by_packet_sink(noop_sink()).set_subtitle_codec("mov_text"),
        ),
        (
            "add_metadata",
            Output::new_by_packet_sink(noop_sink()).add_metadata("title", "x"),
        ),
        (
            "add_stream_metadata",
            Output::new_by_packet_sink(noop_sink())
                .add_stream_metadata("v:0", "language", "eng")
                .expect("stream spec parses"),
        ),
        (
            "disable_auto_copy_metadata",
            Output::new_by_packet_sink(noop_sink()).disable_auto_copy_metadata(),
        ),
    ];
    for (expected, output) in cases {
        match build_err(testsrc(1), output) {
            Error::PacketSink(PacketSinkError::UnsupportedOption(option)) => {
                assert_eq!(option, expected)
            }
            other => panic!("{expected}: expected UnsupportedOption, got {other:?}"),
        }
    }
}

/// Sink validation must WIN against generic option validation: an invalid
/// format name, an invalid IO buffer size, and a default-valued buffer size
/// all surface as the documented typed error, never a generic
/// `OpenOutputError` (the typed validator runs first and tracks setter USE,
/// not the stored value).
#[test]
fn sink_validation_runs_before_generic_validation() {
    match build_err(
        testsrc(1),
        Output::new_by_packet_sink(noop_sink()).set_format("no-such-format"),
    ) {
        Error::PacketSink(PacketSinkError::UnsupportedOption("set_format")) => {}
        other => panic!("invalid format name must hit sink validation first, got {other:?}"),
    }
    match build_err(
        testsrc(1),
        Output::new_by_packet_sink(noop_sink()).set_io_buffer_size(0),
    ) {
        Error::PacketSink(PacketSinkError::UnsupportedOption("set_io_buffer_size")) => {}
        other => panic!("invalid buffer size must hit sink validation first, got {other:?}"),
    }
    match build_err(
        testsrc(1),
        Output::new_by_packet_sink(noop_sink()).set_io_buffer_size(64 * 1024),
    ) {
        Error::PacketSink(PacketSinkError::UnsupportedOption("set_io_buffer_size")) => {}
        other => panic!("default-valued buffer size must still be rejected, got {other:?}"),
    }
}

/// The strict tier owns AV_CODEC_FLAG_GLOBAL_HEADER; a user `flags` codec
/// option (such as `flags=-global_header`, the review probe) is applied after
/// the policy flag and could clear it, so it is rejected typed at build time.
#[test]
fn build_rejects_codec_flags_that_could_clear_global_header() {
    match build_err(
        testsrc(1),
        Output::new_by_packet_sink(noop_sink())
            .set_video_codec("libx264")
            .set_video_codec_opt("flags", "-global_header"),
    ) {
        Error::PacketSink(PacketSinkError::UnsupportedOption(option)) => {
            assert!(option.contains("flags"), "got option {option}")
        }
        other => panic!("expected the flags rejection, got {other:?}"),
    }
}

#[test]
fn build_rejects_stream_copy() {
    match build_err(
        testsrc(1),
        Output::new_by_packet_sink(noop_sink()).set_video_codec("copy"),
    ) {
        Error::PacketSink(PacketSinkError::StreamCopyUnsupported) => {}
        other => panic!("expected StreamCopyUnsupported, got {other:?}"),
    }
    // Copy requested through a stream map instead of the codec option.
    match build_err(
        testsrc(1),
        Output::new_by_packet_sink(noop_sink()).add_stream_map_with_copy("0:v"),
    ) {
        Error::PacketSink(PacketSinkError::StreamCopyUnsupported) => {}
        other => panic!("expected StreamCopyUnsupported via map, got {other:?}"),
    }
}

#[test]
fn build_rejects_non_whitelisted_encoders() {
    // mpeg4 ships in every FFmpeg build, so this rejection is CI-stable.
    match build_err(
        testsrc(1),
        Output::new_by_packet_sink(noop_sink()).set_video_codec("mpeg4"),
    ) {
        Error::PacketSink(PacketSinkError::EncoderNotWhitelisted {
            kind: "video",
            encoder,
            ..
        }) => assert_eq!(encoder, "mpeg4"),
        other => panic!("expected the video whitelist rejection, got {other:?}"),
    }
    // Audio must be AAC.
    match build_err(
        sine(1),
        Output::new_by_packet_sink(noop_sink()).set_audio_codec("mp2"),
    ) {
        Error::PacketSink(PacketSinkError::EncoderNotWhitelisted {
            kind: "audio",
            encoder,
            ..
        }) => assert_eq!(encoder, "mp2"),
        other => panic!("expected the audio whitelist rejection, got {other:?}"),
    }
}

/// §2.4 blind spot: with open-GOP, the encoder raises its raw key flag on
/// non-IDR recovery points; the sink must expose only fresh-decoder-safe
/// random access points (IDR) as key.
#[test]
fn open_gop_non_idr_recovery_points_are_not_key() {
    if !have_encoder("libx264") {
        eprintln!("skipping: libx264 not available in this FFmpeg build");
        return;
    }
    let pin = |output: Output| {
        output
            .set_video_codec("libx264")
            .set_video_codec_opt("preset", "medium")
            .set_video_codec_opt("x264-params", "open-gop=1:keyint=12:min-keyint=12:scenecut=0")
    };

    // Premise check on the container path: the raw KEY flag marks more than
    // one packet (the non-IDR recovery points). If a given x264 build does
    // not produce them, the scenario cannot bite — skip loudly.
    let baseline_path = tmp_path_in(TMP_SUBDIR, "open_gop_baseline.mp4");
    wait_with_watchdog(
        FfmpegContext::builder()
            .input(testsrc(3))
            .output(pin(Output::from(baseline_path.as_str()))
                .set_format_opt("movflags", "+frag_keyframe+empty_moov"))
            .build()
            .unwrap()
            .start()
            .unwrap(),
        120,
        "open_gop_baseline",
    )
    .expect("baseline job failed");
    let mut scanner =
        ez_ffmpeg::packet_scanner::PacketScanner::open(baseline_path.as_str()).unwrap();
    let raw_key_flags = scanner
        .packets()
        .map(|p| p.expect("baseline packet"))
        .filter(|p| p.is_video() && p.is_keyframe())
        .count();
    if raw_key_flags <= 1 {
        eprintln!("skipping: this x264 build produced no open-GOP recovery points");
        return;
    }

    let (sink, log) = recording_sink();
    wait_with_watchdog(
        FfmpegContext::builder()
            .input(testsrc(3))
            .output(pin(Output::new_by_packet_sink(sink)))
            .build()
            .unwrap()
            .start()
            .unwrap(),
        120,
        "open_gop_sink",
    )
    .expect("sink job failed");

    let packets = sink_packets(&log);
    let idr_count = packets.iter().filter(|p| p.is_key).count();
    assert!(packets[0].is_key, "the first access unit is an IDR");
    assert!(
        idr_count < raw_key_flags,
        "is_key must be stricter than the raw key flag ({idr_count} vs {raw_key_flags})"
    );
    // Cross-check against the payload itself: is_key iff the AU carries an
    // IDR NAL (type 5).
    for (i, p) in packets.iter().enumerate() {
        let has_idr = common::parse_avcc_au(&p.data)
            .iter()
            .any(|n| n[0] & 0x1F == 5);
        assert_eq!(p.is_key, has_idr, "AU {i}: is_key must mirror IDR presence");
    }
}

#[test]
fn failing_packet_callback_stops_the_job_without_on_end() {
    if !have_encoder("libx264") {
        eprintln!("skipping: libx264 not available in this FFmpeg build");
        return;
    }
    let log: common::SinkLog = std::sync::Arc::new(std::sync::Mutex::new(Vec::new()));
    let (pkt_log, end_log, err_log) = (log.clone(), log.clone(), log.clone());
    let mut delivered = 0u32;
    let sink = PacketSink::builder(move |pkt: &ez_ffmpeg::packet_sink::PacketView<'_>| {
        delivered += 1;
        pkt_log
            .lock()
            .unwrap()
            .push(SinkEv::Pkt(common::SinkPkt::from_view(pkt)));
        if delivered >= 3 {
            Err(ez_ffmpeg::packet_sink::PacketCallbackError::new(
                "consumer gave up",
            ))
        } else {
            Ok(())
        }
    })
    .on_end(move || {
        end_log.lock().unwrap().push(SinkEv::End {
            thread: std::thread::current().id(),
        })
    })
    .on_delivery_error(move |e: &PacketSinkError| {
        err_log.lock().unwrap().push(SinkEv::Error(e.to_string()))
    })
    .build();

    let result = wait_with_watchdog(
        FfmpegContext::builder()
            .input(testsrc(2))
            .output(
                Output::new_by_packet_sink(sink)
                    .set_video_codec("libx264")
                    .set_video_codec_opt("preset", "ultrafast"),
            )
            .build()
            .unwrap()
            .start()
            .unwrap(),
        120,
        "failing_callback",
    );
    match result {
        Err(Error::PacketSink(PacketSinkError::PacketCallbackFailed {
            stream_index: 0,
            error,
        })) => assert_eq!(error.to_string(), "consumer gave up"),
        other => panic!("expected the typed callback failure, got {other:?}"),
    }

    let events = log.lock().unwrap().clone();
    assert!(
        !events.iter().any(|e| matches!(e, SinkEv::End { .. })),
        "a failed callback must never be followed by on_end"
    );
    assert!(
        matches!(events.last(), Some(SinkEv::Error(_))),
        "the terminal event is on_error"
    );
    let delivered = events
        .iter()
        .filter(|e| matches!(e, SinkEv::Pkt(_)))
        .count();
    assert_eq!(
        delivered, 3,
        "delivery stops at the failing callback, nothing after"
    );
}

#[test]
fn zero_stream_packet_sink_fails_typed_at_build() {
    // Audio-only input with every stream disabled: the packet sink maps no
    // stream. The zero-stream build validation reports the sink-specific
    // typed error (NoStreams), not the generic container NotContainStream.
    match build_err(
        sine(1),
        Output::new_by_packet_sink(noop_sink())
            .disable_video()
            .disable_audio(),
    ) {
        Error::PacketSink(PacketSinkError::NoStreams) => {}
        other => panic!("expected PacketSink(NoStreams), got {other:?}"),
    }
}
