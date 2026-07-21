// Generated from an ffmpeg command by the ez-ffmpeg CLI-compat emitter.
// command: ffmpeg -ss 10 -i in.mp4 -t 4 -c:v libx264 -crf 23 -c:a aac -y clip.mp4
// dialect: ffmpeg 7.1 command line; manifest: r4; crate: ez-ffmpeg 0.14.0; cargo features: none required
// status: verified shape V2 (re-encoded clip (input -ss, output -t)) — verified by the manifest-driven semantic golden suite (oracle: Clip) against the ffmpeg CLI; canonical emission compile-pinned as examples/cli_emitted_clip.rs

use ez_ffmpeg::{FfmpegContext, Input, Output};

fn main() -> Result<(), Box<dyn std::error::Error>> {
    FfmpegContext::builder()
        .input(
            Input::from("in.mp4")
                .set_start_time_us(10_000_000) // -ss (input side, seconds -> microseconds)
        )
        .output(
            Output::from("clip.mp4")
                .set_recording_time_us(4_000_000) // -t
                .set_video_codec("libx264") // -c:v libx264
                .set_audio_codec("aac") // -c:a aac
                .set_video_codec_opt("crf", "23") // -crf 23
        )
        .build()?
        .start()?
        .wait()?;
    Ok(())
}
