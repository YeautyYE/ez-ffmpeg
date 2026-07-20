// Generated from an ffmpeg command by the ez-ffmpeg CLI-compat emitter.
// command: ffmpeg -ss 5 -i in.mp4 -an -c:v mjpeg -frames:v 1 -y thumb.jpg
// dialect: ffmpeg 7.1 command line; manifest: r2; crate: ez-ffmpeg 0.14.0; cargo features: none required
// status: verified shape V4 (single-frame thumbnail (input -ss, -an, mjpeg)) — backed by the semantic golden `golden_v4_thumbnail` against the ffmpeg CLI

use ez_ffmpeg::{FfmpegContext, Input, Output};

fn main() -> Result<(), Box<dyn std::error::Error>> {
    FfmpegContext::builder()
        .input(
            Input::from("in.mp4")
                .set_start_time_us(5_000_000) // -ss (input side, seconds -> microseconds)
        )
        .output(
            Output::from("thumb.jpg")
                .disable_audio() // -an
                .set_video_codec("mjpeg") // -c:v mjpeg
                .set_max_video_frames(1) // -frames:v 1 (image2 update mode is applied automatically)
        )
        .build()?
        .start()?
        .wait()?;
    Ok(())
}
