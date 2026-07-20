// Generated from an ffmpeg command by the ez-ffmpeg CLI-compat emitter.
// command: ffmpeg -i in.mp4 -vn -c:a aac -b:a 192k -y out.m4a
// dialect: ffmpeg 7.1 command line; manifest: r3; crate: ez-ffmpeg 0.14.0; cargo features: none required
// status: verified shape V3 (audio extract (-vn, AAC)) — verified by the manifest-driven semantic golden suite (oracle: AudioExtract) against the ffmpeg CLI; canonical emission compile-pinned as examples/cli_emitted_audio_extract.rs

use ez_ffmpeg::{FfmpegContext, Output};

fn main() -> Result<(), Box<dyn std::error::Error>> {
    FfmpegContext::builder()
        .input("in.mp4")
        .output(
            Output::from("out.m4a")
                .disable_video() // -vn
                .set_audio_codec("aac") // -c:a aac
                .set_audio_bitrate("192k") // -b:a 192k
        )
        .build()?
        .start()?
        .wait()?;
    Ok(())
}
