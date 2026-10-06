//! Auto-scale of a mid-stream resolution change (`-autoscale`, on by default).
//!
//! When a decoder starts emitting a different frame size partway through an
//! input, the filtergraph is reconfigured. FFmpeg's CLI then presses the new
//! frames back to the size the encoder was opened with by inserting a
//! `scale=<first width>:<height>` at the end of the output chain
//! (`configure_output_video_filter`, fftools/ffmpeg_filter.c). ez-ffmpeg set
//! `OFILTER_FLAG_AUTOSCALE` on every output but never read it, so the new size
//! reached an encoder sized for the old frame; libx264 rejected it
//! ("Input picture width (W) is greater than stride (S)") and the job failed.
//!
//! The fixtures are concatenated mpegts segments with different resolutions,
//! which is what a single changing stream looks like on the wire. Everything is
//! built through the crate itself — no `ffmpeg`/`ffprobe` binary — so the cases
//! run on every CI lane rather than skipping where the CLI is not on PATH.
//! `mpeg4` is the encoder under test rather than `libx264`, because the LGPL CI
//! lane links an FFmpeg without GPL components; it fails the same way without
//! the scale filter (verified against the CLI with `-noautoscale`, where the
//! native encoder crashes outright).

mod common;
use common::{tmp_path_in, wait_with_watchdog};

use ez_ffmpeg::frame_export::{FrameExtractor, PixelLayout};
use ez_ffmpeg::{FfmpegContext, Input, Output};
use ffmpeg_sys_next::{
    avformat_close_input, avformat_find_stream_info, avformat_open_input, AVFormatContext,
    AVMediaType,
};
use std::ffi::CString;
use std::ptr;

fn tmp_path(name: &str) -> String {
    tmp_path_in("ez_ffmpeg_autoscale", name)
}

fn run(context: FfmpegContext, scenario: &str) {
    wait_with_watchdog(context.start().expect("start"), 60, scenario).expect("job failed");
}

/// `(width, height)` of the first video stream, or `None` when the file has
/// none. The public `StreamInfo` API reports the size the stream declares, but
/// this scenario's whole point is that a file can carry frames of more than one
/// size, so the probe reads the container directly.
fn video_size(path: &str) -> Option<(i32, i32)> {
    unsafe {
        let c_path = CString::new(path).unwrap();
        let mut fmt: *mut AVFormatContext = ptr::null_mut();
        let ret = avformat_open_input(&mut fmt, c_path.as_ptr(), ptr::null_mut(), ptr::null_mut());
        assert!(ret >= 0, "avformat_open_input({path}) failed: {ret}");
        assert!(avformat_find_stream_info(fmt, ptr::null_mut()) >= 0);
        let size = (0..(*fmt).nb_streams as usize).find_map(|i| {
            let st = *(*fmt).streams.add(i);
            let par = (*st).codecpar;
            if (*par).codec_type != AVMediaType::AVMEDIA_TYPE_VIDEO {
                return None;
            }
            Some(((*par).width, (*par).height))
        });
        avformat_close_input(&mut fmt);
        size
    }
}

/// Every decoded frame's size, so a file whose stream header claims one size
/// while its frames carry another cannot pass as fixed.
fn frame_sizes(path: &str) -> Vec<(u32, u32)> {
    FrameExtractor::new(Input::from(path))
        .pixel(PixelLayout::Gray8)
        .collect_frames()
        .expect("frame extraction failed")
        .iter()
        .map(|f| (f.width(), f.height()))
        .collect()
}

/// One 2s mpegts segment of a solid-resolution synthetic source. `mpeg2video`
/// is a native encoder, so this builds on every lane including the LGPL one.
fn segment(path: &str, width: u32, height: u32) {
    run(
        FfmpegContext::builder()
            .input(Input::from(format!("testsrc=size={width}x{height}:rate=20")).set_format("lavfi"))
            .output(
                Output::from(path)
                    .set_video_codec("mpeg2video")
                    .set_recording_time_us(2_000_000),
            )
            .build()
            .expect("segment build"),
        "fixture segment",
    );
}

/// Two equal-length mpegts segments whose video stream changes resolution at
/// the join, concatenated byte-wise into one transport stream.
fn changing_resolution_fixture(name: &str, first: (u32, u32), second: (u32, u32)) -> String {
    let mut bytes = Vec::new();
    for (i, (w, h)) in [first, second].into_iter().enumerate() {
        let seg = tmp_path(&format!("{name}.{i}.ts"));
        segment(&seg, w, h);
        bytes.extend_from_slice(&std::fs::read(&seg).expect("read segment"));
    }

    let joined = tmp_path(&format!("{name}.ts"));
    std::fs::write(&joined, bytes).expect("write joined segment");

    assert_eq!(
        video_size(&joined),
        Some((first.0 as i32, first.1 as i32)),
        "fixture does not start at the first resolution"
    );
    joined
}

/// A resolution change mid-stream transcodes instead of failing, and the
/// output keeps the size the encoder was opened with.
#[test]
fn a_mid_stream_resolution_change_is_scaled_back_for_the_encoder() {
    let input = changing_resolution_fixture("shrink", (608, 1280), (456, 960));

    let out = tmp_path("shrink_out.mp4");
    run(
        FfmpegContext::builder()
            .input(Input::from(input.as_str()))
            .output(Output::from(out.as_str()).set_video_codec("mpeg4"))
            .build()
            .expect("build"),
        "mid-stream resolution change",
    );

    assert_eq!(
        video_size(&out),
        Some((608, 1280)),
        "output video size is not the first segment's"
    );
    let sizes = frame_sizes(&out);
    assert!(
        !sizes.is_empty(),
        "no decoded frames to check the per-frame size"
    );
    assert!(
        sizes.iter().all(|&s| s == (608, 1280)),
        "frames escaped the auto-inserted scale: {sizes:?}"
    );
}

/// The auto-inserted scale sits after the user's own chain, so an explicit
/// `-vf` still sees the source size and the output is still forced back to the
/// encoder's size.
#[test]
fn the_auto_scale_follows_a_user_supplied_video_filter() {
    let input = changing_resolution_fixture("user_filter", (608, 1280), (456, 960));

    let out = tmp_path("user_filter_out.mp4");
    run(
        FfmpegContext::builder()
            .input(Input::from(input.as_str()))
            .output(
                Output::from(out.as_str())
                    .set_video_codec("mpeg4")
                    .set_video_filter("hflip"),
            )
            .build()
            .expect("build"),
        "resolution change through a user filter",
    );

    assert_eq!(
        video_size(&out),
        Some((608, 1280)),
        "output video size is not the first segment's"
    );
    let sizes = frame_sizes(&out);
    assert!(
        sizes.iter().all(|&s| s == (608, 1280)),
        "frames escaped the auto-inserted scale: {sizes:?}"
    );
}
