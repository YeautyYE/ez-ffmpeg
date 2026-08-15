//! Hard LGPL-minimum registry contract.
//!
//! These tests are `#[ignore]` so a full Homebrew / vcpkg FFmpeg (which
//! typically ships `libx264` and `cropdetect`) does not fail the default
//! `cargo test` suite. The dedicated CI job runs them with `--ignored`
//! against a pinned `--disable-gpl --disable-nonfree` FFmpeg. There is
//! **no availability-based skip**: a green run on a GPL fat package is a
//! false pass and must fail.

use ez_ffmpeg::capabilities;
use std::ffi::CStr;

/// Linked `libavutil` license string. The CI profile must report LGPL;
/// a GPL string means the job linked the wrong FFmpeg.
fn avutil_license() -> String {
    // SAFETY: `avutil_license` returns a process-lifetime C string.
    let ptr = unsafe { ffmpeg_sys_next::avutil_license() };
    assert!(!ptr.is_null(), "avutil_license returned null");
    unsafe { CStr::from_ptr(ptr) }
        .to_string_lossy()
        .into_owned()
}

#[test]
#[ignore]
fn linked_build_reports_lgpl_license() {
    let license = avutil_license();
    // A GPL build reports "GPL version 2 or later" (no leading "L"), so the
    // prefix check both proves LGPL and excludes GPL. A substring check for
    // "gpl v2" could never fire and would silently pass on GPL builds.
    assert!(
        license.to_ascii_lowercase().starts_with("lgpl"),
        "linked libavutil license must be LGPL (not GPL), got {license:?}"
    );
}

#[test]
#[ignore]
fn gpl_components_are_absent() {
    assert!(
        !capabilities::is_encoder_available("libx264"),
        "libx264 must be absent from the LGPL minimum profile"
    );
    assert!(
        !capabilities::is_encoder_available("libx265"),
        "libx265 must be absent from the LGPL minimum profile"
    );
    assert!(
        !capabilities::is_encoder_available("libfdk_aac"),
        "libfdk_aac must be absent from the LGPL minimum profile"
    );
    assert!(
        !capabilities::is_filter_available("cropdetect"),
        "cropdetect must be absent from the LGPL minimum profile"
    );
}

#[test]
#[ignore]
fn native_lgpl_components_are_present() {
    for name in ["aac", "mjpeg", "gif", "mpeg4"] {
        assert!(
            capabilities::is_encoder_available(name),
            "native encoder {name:?} must be present"
        );
    }
    for name in ["h264", "hevc", "vp9", "aac", "mp3"] {
        assert!(
            capabilities::is_decoder_available(name),
            "native decoder {name:?} must be present"
        );
    }
    // `colorspace` backs the HDR cookbook's wide-gamut chain and `sidedata`
    // its HDR side-data cleanup; pin them so the no-skip lane would catch a
    // profile losing either native filter.
    for name in [
        "scale",
        "blackdetect",
        "silencedetect",
        "scdet",
        "ebur128",
        "colorspace",
        "sidedata",
    ] {
        assert!(
            capabilities::is_filter_available(name),
            "built-in filter {name:?} must be present"
        );
    }
    for name in [
        "matroska", "mpegts", "mp4", "mov", "flv", "gif", "hls", "null",
    ] {
        assert!(
            capabilities::is_muxer_available(name),
            "native muxer {name:?} must be present"
        );
    }
    assert!(
        capabilities::is_input_protocol_available("file"),
        "file input protocol must be present"
    );
    assert!(
        capabilities::is_output_protocol_available("file"),
        "file output protocol must be present"
    );
    assert!(
        capabilities::is_output_protocol_available("rtmp"),
        "rtmp output protocol must be present"
    );
    assert!(
        !capabilities::is_input_protocol_available("https"),
        "https input must be absent under --disable-autodetect with no TLS backend"
    );
    assert!(
        !capabilities::is_output_protocol_available("srt"),
        "srt output must be absent without --enable-libsrt"
    );
}
