//! One-shot "recipe" helpers over the ez-ffmpeg builder for common FFmpeg
//! workflows. Each recipe generates a filtergraph plus output configuration
//! from a small typed options struct and runs it to completion; the raw
//! [`filter_desc`](crate::FfmpegContext) escape hatch remains available for
//! anything these do not cover.
//!
//! - [`thumbnail`](crate::core::recipes::thumbnail::thumbnail) /
//!   [`sprite_sheet`](crate::core::recipes::thumbnail::sprite_sheet):
//!   still-frame and storyboard extraction.
//! - [`animated_gif`](crate::core::recipes::gif::animated_gif): high-quality
//!   GIF export (palettegen/paletteuse).
//! - [`HlsLadder`](crate::core::recipes::hls::HlsLadder): VOD adaptive-bitrate
//!   HLS ladder.
//!
//! # HDR-to-SDR tone mapping (cookbook, no typed helper)
//!
//! Converting HDR footage (HDR10/PQ or HLG — the default capture format of
//! most recent phones) to SDR is a filtergraph recipe, not a typed helper: the
//! filters it needs (`zscale`+`tonemap`, or `libplacebo`) are optional in the
//! FFmpeg build, so this crate documents the chains and ships a runnable
//! `examples/hdr_to_sdr` rather than a `hdr_to_sdr()` function that would
//! silently fail on a build without them. Probe first with
//! [`is_filter_available`](crate::hwaccel::is_filter_available), then drive it
//! through [`Output::set_video_filter`](crate::Output::set_video_filter).
//!
//! A naive `scale,format=yuv420p` produces a washed-out, gray image: it
//! reinterprets the PQ/HLG brightness curve as SDR gamma and crushes the
//! BT.2020 gamut into BT.709 with no tone-mapping curve. The correct chains:
//!
//! - **CPU (needs libzimg):**
//!   ```text
//!   zscale=t=linear:npl=100,format=gbrpf32le,zscale=p=bt709,tonemap=tonemap=hable:desat=0:peak=10,zscale=t=bt709:m=bt709:r=tv,format=yuv420p
//!   ```
//! - **GPU (needs libplacebo + a runtime Vulkan device):**
//!   ```text
//!   libplacebo=tonemapping=bt.2390:tonemapping_param=0.5:colorspace=bt709:color_primaries=bt709:color_trc=bt709:range=tv:format=yuv420p
//!   ```
//! - **FFmpeg 8+ swscale (no external library):**
//!   ```text
//!   scale=out_color_matrix=bt709:out_primaries=bt709:out_transfer=bt709:out_range=tv:intent=perceptual,format=yuv420p
//!   ```
//!
//! Route on the **transfer characteristic**, not the primaries: PQ
//! (`smpte2084`) and HLG (`arib-std-b67`) need tone mapping; a BT.2020 gamut
//! with a BT.709 transfer is wide-gamut *SDR* and must only have its gamut
//! converted — tone-mapping it darkens the picture. Read the transfer from the
//! [`StreamInfo::Video`](crate::stream_info::StreamInfo) `color_transfer`
//! field. The parameters that keep the output from graying out: `desat=0`
//! (the default `2` mixes highlights toward gray), an **explicit** `peak`
//! (`peak = master_nits / 100`, so 1000 nits is `peak=10`; on FFmpeg 8 the
//! `zscale` linearization strips the HDR metadata automatic peak detection
//! reads — FFmpeg 7.1 kept it — so a fixed peak is the only version-stable
//! choice), and re-tagging the output BT.709 limited-range (`r=tv` /
//! `range=tv` / `out_range=tv`).
//!
//! **Verify the look on your own footage.** These chains are
//! parameter-correct, but tone-mapping is subjective and content-dependent;
//! the FFmpeg 8 `scale` chain in particular is not exercised by this crate's
//! CI. Treat `examples/hdr_to_sdr` as a starting point, not a guarantee of a
//! "correct" picture on every source.

pub mod gif;
pub mod hls;
pub mod hls_master;
pub mod thumbnail;

pub use gif::*;
pub use hls::*;
pub use thumbnail::*;
