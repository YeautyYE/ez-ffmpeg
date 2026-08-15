//! Named-failure contract for the LGPL minimum FFmpeg profile.
//!
//! `#[ignore]` so a GPL fat package does not fail the default suite. The
//! dedicated `lgpl-contract` CI job runs this binary with `--ignored`.
//! There is **no availability-based skip**.
//!
//! These assertions pin **current** crate behavior. A named failure here is
//! not an invitation to skip or silently fall back.

use std::path::Path;

use ez_ffmpeg::error::{Error, HlsEncoderSelectionError, OpenOutputError, PacketSinkError};
use ez_ffmpeg::packet_sink::PacketSink;
use ez_ffmpeg::recipes::HlsLadder;
use ez_ffmpeg::{FfmpegContext, Input, Output};

mod common;
use common::tmp_path_in;

const SUBDIR: &str = "ez_ffmpeg_lgpl_expected_failures";

fn fixture_mp4() -> String {
    let path = Path::new(env!("CARGO_MANIFEST_DIR")).join("test.mp4");
    assert!(path.exists(), "missing repo fixture test.mp4");
    path.to_string_lossy().into_owned()
}

#[test]
#[ignore]
fn default_hls_ladder_names_missing_libx264_and_creates_no_tree() {
    let out_dir = tmp_path_in(SUBDIR, "hls_default");
    let _ = std::fs::remove_dir_all(&out_dir);

    let result = HlsLadder::new(fixture_mp4(), &out_dir)
        .rendition(320, 240, "300k")
        .segment_duration(1.0)
        .fps(10, 1)
        .build_context();

    match result {
        Err(Error::HlsEncoderSelection(inner)) => match inner.as_ref() {
            HlsEncoderSelectionError::HistoricalDefaultUnavailable { .. } => {}
            other => panic!(
                "expected HistoricalDefaultUnavailable for the historical HLS default, got {other:?}"
            ),
        },
        Err(other) => panic!(
            "expected HlsEncoderSelection for the historical HLS default, got {other:?}"
        ),
        Ok(_) => panic!("default HlsLadder must not succeed without libx264"),
    }

    let displayed = match HlsLadder::new(fixture_mp4(), &out_dir)
        .rendition(320, 240, "300k")
        .segment_duration(1.0)
        .fps(10, 1)
        .build_context()
    {
        Err(e) => e.to_string(),
        Ok(_) => panic!("second default HlsLadder build unexpectedly succeeded"),
    };
    assert!(
        displayed.contains("libx264"),
        "HLS default error must name libx264, got {displayed:?}"
    );
    assert!(
        !Path::new(&out_dir).exists(),
        "a failed HLS default must not create the output directory tree"
    );
}

#[test]
#[ignore]
fn video_codec_auto_names_every_candidate_when_none_open() {
    let out_dir = tmp_path_in(SUBDIR, "hls_auto");
    let _ = std::fs::remove_dir_all(&out_dir);

    let result = HlsLadder::new(fixture_mp4(), &out_dir)
        .rendition(320, 240, "300k")
        .segment_duration(1.0)
        .fps(10, 1)
        .video_codec_auto()
        .build_context();

    match result {
        Err(Error::HlsEncoderSelection(inner)) => match inner.as_ref() {
            HlsEncoderSelectionError::AutoSelectionFailed { attempts } => {
                let names: Vec<&str> = attempts.iter().map(|a| a.encoder.as_str()).collect();
                assert_eq!(
                    names,
                    ["h264_videotoolbox", "h264_nvenc", "h264_qsv", "libopenh264"]
                );
            }
            other => panic!("expected AutoSelectionFailed for video_codec_auto, got {other:?}"),
        },
        Err(other) => panic!("expected HlsEncoderSelection for video_codec_auto, got {other:?}"),
        Ok(_) => panic!("video_codec_auto must not succeed on the LGPL minimum profile"),
    }

    let displayed = match HlsLadder::new(fixture_mp4(), &out_dir)
        .rendition(320, 240, "300k")
        .segment_duration(1.0)
        .fps(10, 1)
        .video_codec_auto()
        .build_context()
    {
        Err(e) => e.to_string(),
        Ok(_) => panic!("second video_codec_auto build unexpectedly succeeded"),
    };
    for name in ["h264_videotoolbox", "h264_nvenc", "h264_qsv", "libopenh264"] {
        assert!(
            displayed.contains(name),
            "auto-selection error must name {name}, got {displayed:?}"
        );
    }
    assert!(
        !Path::new(&out_dir).exists(),
        "a failed HLS auto selection must not create the output directory tree"
    );
}

#[test]
#[ignore]
fn packet_sink_distinguishes_mpeg4_whitelist_from_missing_libx264() {
    let mpeg4_err = match FfmpegContext::builder()
        .input(Input::from("testsrc=size=64x48:rate=15:duration=0.2").set_format("lavfi"))
        .output(Output::new_by_packet_sink(PacketSink::discard()).set_video_codec("mpeg4"))
        .build()
    {
        Ok(_) => panic!("mpeg4 packet-sink video must be rejected by the whitelist"),
        Err(e) => e,
    };
    match mpeg4_err {
        Error::PacketSink(PacketSinkError::EncoderNotWhitelisted {
            kind: "video",
            encoder,
            ..
        }) => assert_eq!(encoder, "mpeg4"),
        other => panic!("expected mpeg4 EncoderNotWhitelisted, got {other:?}"),
    }

    let x264_err = match FfmpegContext::builder()
        .input(Input::from("testsrc=size=64x48:rate=15:duration=0.2").set_format("lavfi"))
        .output(Output::new_by_packet_sink(PacketSink::discard()).set_video_codec("libx264"))
        .build()
    {
        Ok(_) => panic!("libx264 packet-sink must fail when the encoder is not in the build"),
        Err(e) => e,
    };
    match x264_err {
        Error::OpenOutput(OpenOutputError::EncoderUnavailable { name }) => {
            assert_eq!(name, "libx264");
        }
        other => panic!(
            "expected EncoderUnavailable {{ name: libx264 }} for missing whitelisted encoder, got {other:?}"
        ),
    }
}
