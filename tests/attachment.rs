//! Integration coverage for `Output::add_attachment` (FFmpeg `-attach`).
//!
//! An attachment is delivered as a stream's `codecpar->extradata` and written
//! into the container header by the muxer (no packet is ever produced). These
//! tests build a real job with one mapped video stream plus an attachment, run
//! it to completion, then re-open the output and inspect the raw AVStream via
//! `ffmpeg-sys-next` (the public `StreamInfo` API does not expose `extradata`).

use ez_ffmpeg::error::{Error, OpenOutputError};
use ez_ffmpeg::{FfmpegContext, FfmpegScheduler, Input, Output};
use ffmpeg_sys_next::{
    av_dict_get, avformat_close_input, avformat_find_stream_info, avformat_open_input, AVFormatContext,
    AVMediaType, AVStream,
};
use std::ffi::{CStr, CString};
use std::ptr;
use std::time::Duration;

fn tmp_path(name: &str) -> String {
    let dir = std::env::temp_dir().join(format!("ez_ffmpeg_attachment_tests_{}", std::process::id()));
    std::fs::create_dir_all(&dir).unwrap();
    dir.join(name).to_string_lossy().into_owned()
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

fn run(context: FfmpegContext, scenario: &str) -> ez_ffmpeg::error::Result<()> {
    wait_with_watchdog(context.start().unwrap(), 60, scenario)
}

/// A short finite lavfi video source, so every job terminates on its own.
fn video_input() -> Input {
    Input::from("color=c=black:s=128x128:r=15").set_format("lavfi")
}

#[derive(Debug)]
struct AttachmentReadback {
    extradata_size: i32,
    extradata: Vec<u8>,
    filename: Option<String>,
    mimetype: Option<String>,
}

/// Re-open `path` and return the first `AVMEDIA_TYPE_ATTACHMENT` stream's
/// extradata + filename/mimetype tags, or `None` if there is no attachment.
fn read_first_attachment(path: &str) -> Option<AttachmentReadback> {
    unsafe {
        let c_path = CString::new(path).unwrap();
        let mut fmt: *mut AVFormatContext = ptr::null_mut();
        let ret = avformat_open_input(&mut fmt, c_path.as_ptr(), ptr::null(), ptr::null_mut());
        assert!(ret >= 0, "avformat_open_input({path}) failed: {ret}");
        assert!(!fmt.is_null());
        let ret = avformat_find_stream_info(fmt, ptr::null_mut());
        assert!(ret >= 0, "avformat_find_stream_info failed: {ret}");

        let mut result = None;
        let nb = (*fmt).nb_streams as usize;
        for i in 0..nb {
            let st = *(*fmt).streams.add(i);
            let par = (*st).codecpar;
            if (*par).codec_type == AVMediaType::AVMEDIA_TYPE_ATTACHMENT {
                let size = (*par).extradata_size;
                let data = if !(*par).extradata.is_null() && size > 0 {
                    std::slice::from_raw_parts((*par).extradata, size as usize).to_vec()
                } else {
                    Vec::new()
                };
                result = Some(AttachmentReadback {
                    extradata_size: size,
                    extradata: data,
                    filename: dict_get(st, "filename"),
                    mimetype: dict_get(st, "mimetype"),
                });
                break;
            }
        }
        avformat_close_input(&mut fmt);
        result
    }
}

unsafe fn dict_get(st: *mut AVStream, key: &str) -> Option<String> {
    let c_key = CString::new(key).unwrap();
    let entry = av_dict_get((*st).metadata, c_key.as_ptr(), ptr::null(), 0);
    if entry.is_null() {
        return None;
    }
    Some(CStr::from_ptr((*entry).value).to_string_lossy().into_owned())
}

/// A deterministic opaque payload covering many byte values, with distinctive
/// sentinel bytes at the tail so a truncation/off-by-one is caught.
fn sample_blob() -> Vec<u8> {
    let mut blob: Vec<u8> = (0..97u16).map(|i| (i.wrapping_mul(31).wrapping_add(7)) as u8).collect();
    blob.extend_from_slice(&[0x00, 0xDE, 0xAD, 0xBE, 0xEF]);
    blob
}

#[test]
fn attachment_roundtrips_into_mkv_with_explicit_mimetype() {
    let out = tmp_path("roundtrip.mkv");
    let attach = tmp_path("payload.bin");
    let blob = sample_blob();
    std::fs::write(&attach, &blob).unwrap();

    let ctx = FfmpegContext::builder()
        .input(video_input())
        .output(
            Output::from(out.as_str())
                .set_video_codec("mpeg4")
                .set_recording_time_us(200_000)
                .add_attachment_with_mimetype(attach.as_str(), "application/octet-stream"),
        )
        .build()
        .expect("build with attachment should succeed");
    run(ctx, "mkv attachment roundtrip").expect("job should complete");

    let att = read_first_attachment(&out).expect("output .mkv must contain an attachment stream");
    assert_eq!(att.extradata_size as usize, blob.len(), "extradata_size must equal payload length");
    assert_eq!(att.extradata, blob, "extradata bytes must equal the source payload");
    assert_eq!(att.filename.as_deref(), Some("payload.bin"), "filename tag must be the basename");
    assert_eq!(
        att.mimetype.as_deref(),
        Some("application/octet-stream"),
        "explicit mimetype must be preserved"
    );
}

#[test]
fn attachment_mimetype_guessed_from_ttf_extension() {
    let out = tmp_path("guessed.mkv");
    // Arbitrary bytes — the payload is opaque, no real font is required.
    let attach = tmp_path("MyFont.ttf");
    let blob = sample_blob();
    std::fs::write(&attach, &blob).unwrap();

    let ctx = FfmpegContext::builder()
        .input(video_input())
        .output(
            Output::from(out.as_str())
                .set_video_codec("mpeg4")
                .set_recording_time_us(200_000)
                .add_attachment(attach.as_str()),
        )
        .build()
        .expect("build should succeed");
    run(ctx, "mkv attachment guessed mimetype").expect("job should complete");

    let att = read_first_attachment(&out).expect("output must contain an attachment stream");
    assert_eq!(att.extradata, blob);
    assert_eq!(att.filename.as_deref(), Some("MyFont.ttf"));
    assert_eq!(
        att.mimetype.as_deref(),
        Some("application/x-truetype-font"),
        ".ttf must be guessed as a truetype font mimetype"
    );
}

#[test]
fn no_attachment_produces_no_attachment_stream() {
    // Backward-compat guard: without add_attachment, the output must not gain
    // an attachment stream (no accidental stream creation).
    let out = tmp_path("no_attachment.mkv");
    let ctx = FfmpegContext::builder()
        .input(video_input())
        .output(
            Output::from(out.as_str())
                .set_video_codec("mpeg4")
                .set_recording_time_us(200_000),
        )
        .build()
        .expect("build should succeed");
    run(ctx, "mkv without attachment").expect("job should complete");

    assert!(
        read_first_attachment(&out).is_none(),
        "an output built without add_attachment must have no attachment stream"
    );
}

#[test]
fn missing_attachment_file_errors_without_panic() {
    let out = tmp_path("missing.mkv");
    let result = FfmpegContext::builder()
        .input(video_input())
        .output(
            Output::from(out.as_str())
                .set_video_codec("mpeg4")
                .set_recording_time_us(100_000)
                .add_attachment("/no/such/dir/missing-attachment.ttf"),
        )
        .build()
        .err(); // FfmpegContext is not Debug; keep only the Error for assertion.
    assert!(
        matches!(result, Some(Error::OpenOutput(OpenOutputError::AttachmentRead(_, _)))),
        "a missing attachment file must fail the build with AttachmentRead, got {result:?}"
    );
}

#[test]
fn empty_attachment_file_errors() {
    let out = tmp_path("empty.mkv");
    let empty = tmp_path("zero_bytes.bin");
    std::fs::write(&empty, b"").unwrap();

    let result = FfmpegContext::builder()
        .input(video_input())
        .output(
            Output::from(out.as_str())
                .set_video_codec("mpeg4")
                .set_recording_time_us(100_000)
                .add_attachment(empty.as_str()),
        )
        .build()
        .err();
    assert!(
        matches!(result, Some(Error::OpenOutput(OpenOutputError::AttachmentEmpty(_)))),
        "an empty attachment file must fail the build with AttachmentEmpty, got {result:?}"
    );
}

#[test]
fn empty_explicit_mimetype_errors() {
    let out = tmp_path("empty_mime.mkv");
    let attach = tmp_path("has_bytes.bin");
    std::fs::write(&attach, b"non-empty").unwrap();

    let result = FfmpegContext::builder()
        .input(video_input())
        .output(
            Output::from(out.as_str())
                .set_video_codec("mpeg4")
                .set_recording_time_us(100_000)
                .add_attachment_with_mimetype(attach.as_str(), ""),
        )
        .build()
        .err();
    assert!(
        matches!(result, Some(Error::OpenOutput(OpenOutputError::AttachmentEmptyMimetype(_)))),
        "an empty explicit mimetype must fail the build, got {result:?}"
    );
}

#[test]
fn attachment_on_mp4_is_unsupported() {
    // MP4 does not accept a generic attachment stream. We do not pre-validate
    // the muxer (matching FFmpeg), so the build may succeed but the job must not
    // complete successfully — the mp4 muxer rejects the extra stream at header
    // write. Assert the job does NOT succeed (documented-unsupported behavior).
    let out = tmp_path("unsupported.mp4");
    let attach = tmp_path("mp4_blob.bin");
    std::fs::write(&attach, b"generic-attachment-payload").unwrap();

    let built = FfmpegContext::builder()
        .input(video_input())
        .output(
            Output::from(out.as_str())
                .set_video_codec("mpeg4")
                .set_recording_time_us(100_000)
                .add_attachment(attach.as_str()),
        )
        .build();

    match built {
        Ok(ctx) => {
            let result = run(ctx, "mp4 attachment (unsupported)");
            assert!(
                result.is_err(),
                "mp4 must not accept a generic attachment stream (expected a write-header error)"
            );
        }
        Err(_) => {
            // Also acceptable: rejected even before the run.
        }
    }
}
