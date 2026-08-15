//! Per-stream codec FourCC (`Output::set_video_codec_tag`, FFmpeg `-tag:v`).
//!
//! The public `StreamInfo` API does not expose `codecpar->codec_tag`, so these
//! tests re-open the output with `ffmpeg-sys-next` after the job finishes.

mod common;

use common::{tmp_path_in, wait_with_watchdog};
use ez_ffmpeg::error::{Error, OpenOutputError};
use ez_ffmpeg::{FfmpegContext, Input, Output};
use ffmpeg_sys_next::{
    avformat_close_input, avformat_find_stream_info, avformat_open_input, AVFormatContext,
    AVMediaType,
};
use std::ffi::CString;
use std::ptr;

fn tmp_path(name: &str) -> String {
    tmp_path_in("ez_ffmpeg_codec_tag_tests", name)
}

fn run(context: FfmpegContext, scenario: &str) -> ez_ffmpeg::error::Result<()> {
    wait_with_watchdog(context.start()?, 60, scenario)
}

fn mktag(tag: &str) -> u32 {
    let b = tag.as_bytes();
    assert_eq!(b.len(), 4, "test helper expects a 4-byte FourCC");
    u32::from(b[0]) | (u32::from(b[1]) << 8) | (u32::from(b[2]) << 16) | (u32::from(b[3]) << 24)
}

fn video_codec_tag(path: &str) -> u32 {
    unsafe {
        let c_path = CString::new(path).unwrap();
        let mut fmt: *mut AVFormatContext = ptr::null_mut();
        let ret = avformat_open_input(&mut fmt, c_path.as_ptr(), ptr::null_mut(), ptr::null_mut());
        assert!(ret >= 0, "avformat_open_input({path}) failed: {ret}");
        let ret = avformat_find_stream_info(fmt, ptr::null_mut());
        assert!(ret >= 0, "avformat_find_stream_info failed: {ret}");
        let mut tag = None;
        let nb = (*fmt).nb_streams as usize;
        for i in 0..nb {
            let st = *(*fmt).streams.add(i);
            let par = (*st).codecpar;
            if (*par).codec_type == AVMediaType::AVMEDIA_TYPE_VIDEO {
                tag = Some((*par).codec_tag);
                break;
            }
        }
        avformat_close_input(&mut fmt);
        tag.expect("expected a video stream")
    }
}

fn avi_mpeg4_fixture(name: &str) -> String {
    let path = tmp_path(name);
    run(
        FfmpegContext::builder()
            .input(Input::from("color=c=black:s=320x240:r=30").set_format("lavfi"))
            .output(
                Output::from(path.as_str())
                    .set_video_codec("mpeg4")
                    .set_recording_time_us(300_000),
            )
            .build()
            .unwrap(),
        "avi mpeg4 fixture",
    )
    .unwrap();
    path
}

#[test]
fn streamcopy_user_tag_mp4v_is_written() {
    let avi = avi_mpeg4_fixture("tag_src.avi");
    let out = tmp_path("tagged.mp4");
    run(
        FfmpegContext::builder()
            .input(avi.as_str())
            .output(
                Output::from(out.as_str())
                    .set_video_codec("copy")
                    .set_video_codec_tag("mp4v"),
            )
            .build()
            .unwrap(),
        "copy with mp4v tag",
    )
    .unwrap();
    assert_eq!(video_codec_tag(&out), mktag("mp4v"));
}

#[test]
fn streamcopy_user_tag_bypasses_auto_clear_and_fails_write_header() {
    // Without a user tag, AVI FMP4 → mp4 copy clears the incompatible tag
    // (tests/stream_selection.rs). An explicit FMP4 tag must be honored, so
    // the mp4 muxer rejects write_header — proof the setter reached codecpar.
    let avi = avi_mpeg4_fixture("tag_fmp4_src.avi");
    let result = run(
        FfmpegContext::builder()
            .input(avi.as_str())
            .output(
                Output::from(tmp_path("tag_fmp4.mp4").as_str())
                    .set_video_codec("copy")
                    .set_video_codec_tag("FMP4"),
            )
            .build()
            .unwrap(),
        "copy with FMP4 tag into mp4",
    );
    assert!(
        result.is_err(),
        "explicit FMP4 tag must not be auto-cleared: {result:?}"
    );
}

#[test]
fn encode_mpeg4_mp4v_tag_is_written() {
    // AVI's mpeg4 default is FMP4; mp4v is accepted and distinct, so a match
    // cannot be the encoder default leaking through.
    let out = tmp_path("enc_mp4v.avi");
    run(
        FfmpegContext::builder()
            .input(Input::from("color=c=black:s=160x120:r=30").set_format("lavfi"))
            .output(
                Output::from(out.as_str())
                    .set_video_codec("mpeg4")
                    .set_video_codec_tag("mp4v")
                    .set_recording_time_us(300_000),
            )
            .build()
            .unwrap(),
        "encode mpeg4 with mp4v tag",
    )
    .unwrap();
    assert_eq!(video_codec_tag(&out), mktag("mp4v"));
}

#[test]
fn empty_codec_tag_is_rejected_at_build() {
    let err = match FfmpegContext::builder()
        .input(Input::from("color=c=black:s=160x120:r=30").set_format("lavfi"))
        .output(
            Output::from(tmp_path("empty_tag.mp4").as_str())
                .set_video_codec("mpeg4")
                .set_video_codec_tag(""),
        )
        .build()
    {
        Ok(_) => panic!("expected build() to fail"),
        Err(e) => e,
    };
    assert!(
        matches!(err, Error::OpenOutput(OpenOutputError::InvalidOption(_))),
        "unexpected error: {err:?}"
    );
}

#[test]
fn video_writer_honors_video_codec_tag() {
    use ez_ffmpeg::VideoWriter;
    let out = tmp_path("writer_mp4v.avi");
    let mut writer = VideoWriter::builder(64, 48)
        .fps(30, 1)
        .open(
            Output::from(out.as_str())
                .set_video_codec("mpeg4")
                .set_video_qscale(6)
                .set_video_codec_tag("mp4v"),
        )
        .unwrap();
    let frame = vec![0u8; writer.frame_size()];
    for _ in 0..5 {
        writer.write(&frame).unwrap();
    }
    writer.finish().unwrap();
    assert_eq!(video_codec_tag(&out), mktag("mp4v"));
}
