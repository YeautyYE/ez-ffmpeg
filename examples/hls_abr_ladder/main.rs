use ez_ffmpeg::recipes::HlsLadder;

/// Build a VOD adaptive-bitrate HLS ladder: one decode is fanned out to three
/// scaled renditions, each fixed-GOP aligned so segments are switchable, plus a
/// Rust-generated master playlist.
fn main() -> Result<(), Box<dyn std::error::Error>> {
    HlsLadder::new("test.mp4", "hls_out")
        .segment_duration(6.0)
        .rendition(1920, 1080, "5000k")
        .rendition(1280, 720, "2800k")
        .rendition(854, 480, "1400k")
        .audio_bitrate("128k")
        .fps(30, 1) // CFR frame rate; omit to probe it from the input.
        .master("master.m3u8")
        .run()?;
    println!("wrote hls_out/master.m3u8 and per-rendition playlists");
    Ok(())
}
