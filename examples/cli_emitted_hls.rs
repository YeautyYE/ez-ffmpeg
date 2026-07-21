// Generated from an ffmpeg command by the ez-ffmpeg CLI-compat emitter.
// command: ffmpeg -i in.mp4 -c:v libx264 -crf 23 -c:a aac -f hls -hls_time 6 -hls_playlist_type vod -hls_list_size 0 -hls_segment_filename seg_%03d.ts -y out.m3u8
// dialect: ffmpeg 7.1 command line; manifest: r4; crate: ez-ffmpeg 0.14.0; cargo features: none required
// status: verified shape V6 (single-rendition VOD HLS) — verified by the manifest-driven semantic golden suite (oracle: Hls) against the ffmpeg CLI; canonical emission compile-pinned as examples/cli_emitted_hls.rs

use ez_ffmpeg::{FfmpegContext, Output};

fn main() -> Result<(), Box<dyn std::error::Error>> {
    FfmpegContext::builder()
        .input("in.mp4")
        .output(
            Output::from("out.m3u8")
                .set_format("hls") // -f hls
                .set_video_codec("libx264") // -c:v libx264
                .set_audio_codec("aac") // -c:a aac
                .set_video_codec_opt("crf", "23") // -crf 23
                .set_format_opt("hls_time", "6") // -hls_time 6
                .set_format_opt("hls_playlist_type", "vod") // -hls_playlist_type vod
                .set_format_opt("hls_list_size", "0") // -hls_list_size 0
                .set_format_opt("hls_segment_filename", "seg_%03d.ts") // -hls_segment_filename seg_%03d.ts
        )
        .build()?
        .start()?
        .wait()?;
    Ok(())
}
