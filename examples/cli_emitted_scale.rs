// Generated from an ffmpeg command by the ez-ffmpeg CLI-compat emitter.
// command: ffmpeg -i in.mp4 -vf scale=1280:-2 -c:v libx264 -crf 23 -preset fast -c:a aac -y scaled.mp4
// dialect: ffmpeg 7.1 command line; manifest: r2; crate: ez-ffmpeg 0.14.0; cargo features: none required
// status: verified shape V5 (scaled H.264/AAC transcode (-vf scale)) — backed by the semantic golden `golden_v5_scale` against the ffmpeg CLI; canonical emission compile-pinned as examples/cli_emitted_scale.rs
// precondition: -vf requires the input to contain exactly ONE video stream;
// in-process execution enforces this after probing (see from_cli_args), and
// this generated code inherits the same assumption.

use ez_ffmpeg::{FfmpegContext, Output};

fn main() -> Result<(), Box<dyn std::error::Error>> {
    FfmpegContext::builder()
        .input("in.mp4")
        .output(
            Output::from("scaled.mp4")
                .set_video_codec("libx264") // -c:v libx264
                .set_audio_codec("aac") // -c:a aac
                .set_video_codec_opt("crf", "23") // -crf 23
                .set_video_codec_opt("preset", "fast") // -preset fast
                .set_video_filter("scale=1280:-2") // -vf scale=1280:-2
        )
        .build()?
        .start()?
        .wait()?;
    Ok(())
}
