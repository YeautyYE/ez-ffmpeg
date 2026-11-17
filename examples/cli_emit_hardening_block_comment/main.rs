// Generated from an ffmpeg command by the ez-ffmpeg CLI-compat emitter.
// command: ffmpeg -i in.mp4 -c:v mpeg4 -y 'out*/compile_error!("boom")/*.mp4'
// dialect: ffmpeg 7.1 command line; manifest: r4; crate: ez-ffmpeg 0.19.3; cargo features: none required
// status: UNVERIFIED SCAFFOLDING — manifest entry U16 (video-codec-only transcode).
// This shape has no semantic golden. The code below compiles against the
// ez-ffmpeg builder API, but its behavior has NOT been checked against the
// ffmpeg CLI and must not be treated as a faithful translation. Review every
// call before use; in-process execution (from_cli / from_cli_args) refuses
// this shape.

use ez_ffmpeg::{FfmpegContext, Output};

fn main() -> Result<(), Box<dyn std::error::Error>> {
    FfmpegContext::builder()
        .input("in.mp4")
        .output(
            Output::from("out*/compile_error!(\"boom\")/*.mp4")
                .set_video_codec("mpeg4") // -c:v mpeg4
        )
        .build()?
        .start()?
        .wait()?;
    Ok(())
}
