//! Single-resource HTTP(S) input via rustls (`--features http-input`).
//!
//! Usage: `cargo run --example http_input --features http-input -- <url> <output>`
//!
//! HLS / DASH URLs are rejected. `Input::from(url)` is not used here on
//! purpose: that path still goes through FFmpeg's own protocols.

use ez_ffmpeg::http_input::HttpInput;
use ez_ffmpeg::{FfmpegContext, FfmpegScheduler};

fn main() -> Result<(), Box<dyn std::error::Error>> {
    let mut args = std::env::args().skip(1);
    let url = args
        .next()
        .ok_or("usage: http_input <http(s)-url> <output>")?;
    let output = args
        .next()
        .ok_or("usage: http_input <http(s)-url> <output>")?;

    let input = HttpInput::builder(url)
        // Reconnect is off by default. For a seekable VOD file:
        // .reconnect(ez_ffmpeg::http_input::ReconnectPolicy::seekable_default())
        // For a non-seekable live stream of unknown length:
        // .reconnect(ez_ffmpeg::http_input::ReconnectPolicy::streamed_default())
        .build()?;
    let context = FfmpegContext::builder()
        .input(input)
        .output(output)
        .build()?;
    FfmpegScheduler::new(context).start()?.wait()?;
    Ok(())
}
