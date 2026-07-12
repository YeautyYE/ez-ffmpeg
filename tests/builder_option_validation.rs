//! Builder options are stored as given and validated when the context is
//! built (0.13 API reshape): a bad value must fail `build()` with the
//! matching `InvalidOption` error instead of panicking inside a setter or
//! forcing a `Result` into the middle of a builder chain.

use ez_ffmpeg::error::{Error, OpenInputError, OpenOutputError};
use ez_ffmpeg::{FfmpegContext, Input, Output};

fn fixture() -> String {
    let path = std::path::Path::new(env!("CARGO_MANIFEST_DIR")).join("test.mp4");
    assert!(path.exists(), "missing repo fixture test.mp4");
    path.to_string_lossy().into_owned()
}

fn build_with(input: Input, output: Output) -> ez_ffmpeg::error::Result<FfmpegContext> {
    FfmpegContext::builder().input(input).output(output).build()
}

#[test]
fn invalid_input_framerate_fails_build_not_the_setter() {
    // The setter must not panic...
    let input = Input::from(fixture()).set_framerate(0, 1);
    // ...and build reports the offending option.
    let err = build_with(input, Output::from("/tmp/ez_opt_never_written.mp4"))
        .err()
        .expect("zero framerate must fail build");
    assert!(
        matches!(&err, Error::OpenInputStream(OpenInputError::InvalidOption(msg))
            if msg.contains("set_framerate")),
        "unexpected error: {err}"
    );
}

#[test]
fn invalid_input_ts_scale_fails_build() {
    let input = Input::from(fixture()).set_ts_scale(f64::NAN);
    let err = build_with(input, Output::from("/tmp/ez_opt_never_written.mp4"))
        .err()
        .expect("NaN ts_scale must fail build");
    assert!(
        matches!(&err, Error::OpenInputStream(OpenInputError::InvalidOption(msg))
            if msg.contains("set_ts_scale")),
        "unexpected error: {err}"
    );
}

#[test]
fn invalid_output_framerate_fails_build() {
    let output = Output::from("/tmp/ez_opt_never_written.mp4").set_framerate(30, 0);
    let err = build_with(Input::from(fixture()), output)
        .err()
        .expect("zero denominator must fail build");
    assert!(
        matches!(&err, Error::OpenOutput(OpenOutputError::InvalidOption(msg))
            if msg.contains("set_framerate")),
        "unexpected error: {err}"
    );
}

#[test]
fn malformed_force_key_frames_spec_fails_build() {
    // The setter is infallible (chainable); the malformed spec surfaces at
    // build with the parse error.
    let output = Output::from("/tmp/ez_opt_never_written.mp4")
        .set_video_codec("mpeg4")
        .set_force_key_frames("0,abc,10");
    let err = build_with(Input::from(fixture()), output)
        .err()
        .expect("malformed spec must fail build");
    assert!(
        matches!(&err, Error::OpenOutput(OpenOutputError::InvalidOption(_))),
        "unexpected error: {err}"
    );
}

#[test]
fn unknown_audio_sample_format_name_fails_build() {
    // Same currency and failure mode as set_pix_fmt with a bad name.
    let output = Output::from("/tmp/ez_opt_never_written.wav")
        .set_audio_codec("pcm_s16le")
        .set_audio_sample_fmt("definitely_not_a_format");
    let err = build_with(Input::from(fixture()), output)
        .err()
        .expect("unknown sample format must fail build");
    assert!(
        matches!(&err, Error::OpenOutput(OpenOutputError::UnknownSampleFormat(name))
            if name == "definitely_not_a_format"),
        "unexpected error: {err}"
    );
}

#[test]
fn sample_format_name_with_interior_nul_fails_as_unknown() {
    // An embedded NUL cannot be a valid FFmpeg name; it must land in the
    // same typed error as a misspelling, not a generic CString error.
    let output = Output::from("/tmp/ez_opt_never_written.wav")
        .set_audio_codec("pcm_s16le")
        .set_audio_sample_fmt("s16\0junk");
    let err = build_with(Input::from(fixture()), output)
        .err()
        .expect("NUL-bearing name must fail build");
    assert!(
        matches!(&err, Error::OpenOutput(OpenOutputError::UnknownSampleFormat(name))
            if name == "s16\0junk"),
        "unexpected error: {err}"
    );
}

#[test]
fn known_audio_sample_format_name_builds() {
    let output = Output::from("/tmp/ez_opt_known_fmt.wav")
        .set_audio_codec("pcm_s16le")
        .set_audio_sample_fmt("s16");
    build_with(Input::from(fixture()), output).expect("s16 must resolve");
}

#[test]
fn zero_io_buffer_size_fails_build() {
    let input = Input::new_by_read_callback(|_buf: &mut [u8]| 0).set_io_buffer_size(0);
    let err = build_with(input, Output::from("/tmp/ez_opt_never_written.mp4"))
        .err()
        .expect("zero io_buffer_size must fail build");
    assert!(
        matches!(&err, Error::OpenInputStream(OpenInputError::InvalidOption(msg))
            if msg.contains("io_buffer_size")),
        "unexpected error: {err}"
    );
}

#[test]
fn zero_muxing_queue_caps_fail_build() {
    let output = Output::from("/tmp/ez_opt_never_written.mp4").set_max_muxing_queue_size(0);
    let err = build_with(Input::from(fixture()), output)
        .err()
        .expect("zero queue cap must fail build");
    assert!(
        matches!(&err, Error::OpenOutput(OpenOutputError::InvalidOption(msg))
            if msg.contains("max_muxing_queue_size")),
        "unexpected error: {err}"
    );

    let output = Output::from("/tmp/ez_opt_never_written.mp4").set_muxing_queue_data_threshold(0);
    let err = build_with(Input::from(fixture()), output)
        .err()
        .expect("zero data threshold must fail build");
    assert!(
        matches!(&err, Error::OpenOutput(OpenOutputError::InvalidOption(msg))
            if msg.contains("muxing_queue_data_threshold")),
        "unexpected error: {err}"
    );
}
