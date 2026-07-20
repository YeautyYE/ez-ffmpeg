// Generated from an ffmpeg command by the ez-ffmpeg CLI-compat emitter.
// command: ffmpeg -i in.mkv -c:v libx264 -crf 23 -preset fast -c:a aac -y out.mp4
// dialect: ffmpeg 7.1 command line; manifest: r1; crate: ez-ffmpeg 0.14.0; cargo features: none required
// status: verified shape V1 (H.264/AAC transcode (crf + preset)) — backed by the semantic golden `golden_v1_transcode` against the ffmpeg CLI

use ez_ffmpeg::{FfmpegContext, Output};

fn main() -> Result<(), Box<dyn std::error::Error>> {
    FfmpegContext::builder()
        .input("in.mkv")
        .output(
            Output::from("out.mp4")
                .set_video_codec("libx264") // -c:v libx264
                .set_audio_codec("aac") // -c:a aac
                .set_video_codec_opt("crf", "23") // -crf 23
                .set_video_codec_opt("preset", "fast") // -preset fast
        )
        .build()?
        .start()?
        .wait()?;
    Ok(())
}
