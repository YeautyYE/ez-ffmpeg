//! HDR-to-SDR tone-mapping cookbook.
//!
//! Converting HDR footage (HDR10/PQ or HLG, the default capture format of most
//! recent phones) to SDR with a naive `scale,format=yuv420p` produces a
//! washed-out, gray image: the PQ/HLG brightness curve is reinterpreted as if
//! it were SDR gamma, and the BT.2020 gamut is crushed into BT.709 without a
//! tone-mapping curve. Getting it right needs an explicit chain that
//! linearizes the signal, maps the gamut in linear light, applies a
//! tone-mapping curve, and re-tags the output as BT.709 limited-range.
//!
//! This example does four things, all through ez-ffmpeg's safe API:
//!
//! 1. **Detect** the input's color transfer / primaries (the new
//!    [`StreamInfo::Video`] color fields) and route on the TRANSFER axis:
//!    PQ and HLG need tone mapping, wide-gamut-but-SDR needs only a gamut
//!    conversion, and true SDR is left untouched. Routing on transfer (not
//!    just primaries) avoids darkening standards-valid SDR BT.2020 clips.
//! 2. **Probe** which tone-mapping filters this FFmpeg build actually has
//!    (`ez_ffmpeg::hwaccel::is_filter_available`) — `zscale`/`tonemap` (needs
//!    libzimg) or `libplacebo` (needs a Vulkan-capable build). Homebrew's
//!    ffmpeg, for example, ships neither.
//! 3. **Build** the correct filter chain with the parameters that keep the
//!    output from graying out (explicit `peak`, `desat=0`, full re-tagging).
//! 4. **Run** it, copying audio through unchanged.
//!
//! The routing and chain logic lives in `routing.rs`, included below; the
//! integration tests include the same file, so the decision has one source of
//! truth without becoming a public API.
//!
//! # Real-test protocol (read before trusting the output)
//!
//! The chains here are parameter-correct against the FFmpeg filter source and
//! the color standards, but tone-mapping *look* is subjective and
//! content-dependent. Before shipping a pipeline built on this recipe:
//!
//! - Verify the look on YOUR real HDR footage, not a synthetic clip. Adjust
//!   `peak` to your master's real peak luminance (`peak = nits / 100`, so a
//!   1000-nit master is `peak=10`, a 4000-nit master `peak=40`) and try
//!   `mobius` if `hable` looks too dark.
//! - The FFmpeg 8 `scale` fallback chain (shown in the error message when no
//!   tone-mapping filter is compiled in) is UNVERIFIED here; test it on a real
//!   FFmpeg 8 build before relying on it.
//!
//! # Usage
//!
//! ```sh
//! cargo run --example hdr_to_sdr -- input_hdr.mp4 output_sdr.mp4
//! # optional third argument selects the GPU chain: `libplacebo`
//! cargo run --example hdr_to_sdr -- input_hdr.mp4 output_sdr.mp4 libplacebo
//! ```

#[path = "routing.rs"]
mod routing;

use std::error::Error;

use ez_ffmpeg::stream_info::{find_video_stream_info, StreamInfo};
use ez_ffmpeg::{FfmpegContext, FfmpegScheduler, Output};
use routing::{build_chain, classify, no_backend_message, pick_backend, ColorKind};

fn main() -> Result<(), Box<dyn Error>> {
    let mut args = std::env::args().skip(1);
    let input_path = args
        .next()
        .ok_or("usage: hdr_to_sdr <input> <output> [libplacebo]")?;
    let output_path = args
        .next()
        .ok_or("usage: hdr_to_sdr <input> <output> [libplacebo]")?;
    let prefer_gpu = args.next().as_deref() == Some("libplacebo");

    // Probe the input's color tags. find_video_stream_info returns
    // Ok(None) when the file has no video stream at all.
    let video = find_video_stream_info(&input_path)?.ok_or("input has no video stream")?;
    let (color_transfer, color_primaries, color_space) = match video {
        StreamInfo::Video {
            color_transfer,
            color_primaries,
            color_space,
            ..
        } => (color_transfer, color_primaries, color_space),
        // find_video_stream_info only ever yields a Video variant.
        _ => return Err("expected a video stream".into()),
    };

    let kind = classify(color_transfer, color_primaries, color_space);
    println!("detected color: {kind:?}");

    // True SDR: nothing to tone-map. Fail loudly rather than silently produce
    // an identical file the user did not ask for.
    if kind == ColorKind::Sdr {
        return Err("input is already SDR (BT.709 or smaller); no tone mapping needed".into());
    }

    // Probe capabilities for THIS kind: wide-gamut SDR needs only `zscale`,
    // while PQ/HLG additionally need `tonemap` (or libplacebo).
    let backend = pick_backend(kind, prefer_gpu).ok_or_else(no_backend_message)?;
    println!("using backend: {backend:?}");

    // Explicit peak for the common 1000-nit consumer HDR master. Change to your
    // master's real peak (nits / 100) for best results. Ignored for the
    // gamut-only (wide-gamut SDR) chain.
    let filter_chain =
        build_chain(kind, backend, 10).ok_or("no conversion chain for this input")?;
    println!("filter chain: {filter_chain}");

    // Per-output simple video filter + audio stream copy: the cookbook shape.
    // The color tags the chain writes propagate into the encoder, so the
    // output file is correctly tagged BT.709.
    let output = Output::from(output_path.as_str())
        .set_video_filter(filter_chain)
        .set_audio_codec("copy");

    let context = FfmpegContext::builder()
        .input(input_path.as_str())
        .output(output)
        .build()?;

    FfmpegScheduler::new(context).start()?.wait()?;
    println!("done: {output_path}");
    Ok(())
}
