//! Shared HDR-to-SDR routing and chain assembly for the `hdr_to_sdr` example.
//!
//! Included via `#[path = ...] mod routing;` by both the example binary and the
//! integration tests, so the routing decision has a single source of truth and
//! stays testable WITHOUT exposing a public tone-mapping API — this file is not
//! part of the crate's public surface.
#![allow(dead_code)] // each includer (example / tests) uses a different subset

use ez_ffmpeg::hwaccel::is_filter_available;
use ffmpeg_sys_next::{AVColorPrimaries, AVColorSpace, AVColorTransferCharacteristic};

/// How the input's color should be treated, decided on the transfer axis.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ColorKind {
    /// PQ (SMPTE ST 2084) — HDR10/HDR10+/Dolby-Vision base layer. Tone-map.
    Pq,
    /// HLG (ARIB STD-B67) — broadcast / phone HDR. Tone-map (same chain, with
    /// the caveat that HLG's ideal curve may differ; see the module docs).
    Hlg,
    /// BT.2020 gamut but an SDR transfer: NOT HDR. Only the gamut needs
    /// converting; tone-mapping it would wrongly darken the picture.
    WideGamutSdr,
    /// Ordinary SDR (BT.709 or smaller). Nothing to convert.
    Sdr,
}

/// Which tone-mapping backend to build the chain for.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Backend {
    /// CPU: `zscale` (+ `tonemap` for HDR). Needs a libzimg-enabled FFmpeg.
    Zscale,
    /// GPU: `libplacebo` (needs a Vulkan-capable FFmpeg *and* a runtime ICD).
    Libplacebo,
}

/// Classifies the stream's color on the TRANSFER axis first.
///
/// The transfer characteristic is what actually distinguishes HDR from SDR:
/// PQ and HLG are HDR, everything else is SDR even when tagged BT.2020. Only
/// after ruling out an HDR transfer do we look at the primaries to catch
/// wide-gamut SDR that still needs a gamut conversion.
pub fn classify(color_transfer: i32, color_primaries: i32, color_space: i32) -> ColorKind {
    let pq = AVColorTransferCharacteristic::AVCOL_TRC_SMPTE2084 as i32;
    let hlg = AVColorTransferCharacteristic::AVCOL_TRC_ARIB_STD_B67 as i32;
    if color_transfer == pq {
        return ColorKind::Pq;
    }
    if color_transfer == hlg {
        return ColorKind::Hlg;
    }

    // Not an HDR transfer. A BT.2020 gamut here means wide-gamut SDR: convert
    // the gamut but do NOT tone-map (that is the 34%-too-dark trap).
    let pri_2020 = AVColorPrimaries::AVCOL_PRI_BT2020 as i32;
    let spc_2020_ncl = AVColorSpace::AVCOL_SPC_BT2020_NCL as i32;
    let spc_2020_cl = AVColorSpace::AVCOL_SPC_BT2020_CL as i32;
    if color_primaries == pri_2020 || color_space == spc_2020_ncl || color_space == spc_2020_cl {
        return ColorKind::WideGamutSdr;
    }

    ColorKind::Sdr
}

/// Whether this kind needs a tone-mapping curve (as opposed to a gamut-only
/// conversion). Only PQ and HLG do.
pub fn needs_tone_map(kind: ColorKind) -> bool {
    matches!(kind, ColorKind::Pq | ColorKind::Hlg)
}

/// Builds the tone-map chain for an HDR (PQ/HLG) input on the chosen backend.
/// `peak` is the normalized signal peak (`nits / 100`).
pub fn tone_map_chain(backend: Backend, peak: u32) -> String {
    match backend {
        // CPU chain: PQ->linear (npl=100 pins linear 1.0 to 100 cd/m2), gamut
        // to BT.709 in linear light, Hable tone-map with an EXPLICIT peak, then
        // re-tag BT.709 / limited range. The peak is explicit because on
        // FFmpeg 8 the zscale linearization strips the MaxCLL / mastering side
        // data that automatic peak detection reads (FFmpeg 7.1 kept it), so a
        // fixed peak is the only version-stable choice. desat=0 disables the
        // default desaturation that grays out highlights.
        Backend::Zscale => format!(
            "zscale=t=linear:npl=100,format=gbrpf32le,zscale=p=bt709,\
             tonemap=tonemap=hable:desat=0:peak={peak},\
             zscale=t=bt709:m=bt709:r=tv,format=yuv420p"
        ),
        // GPU chain: libplacebo does linearization, gamut and tone mapping in
        // one pass. tonemapping_param=0.5 restores the BT.2390 report curve's
        // knee (libplacebo's default knee is 1.0). range=tv tags limited range.
        Backend::Libplacebo => {
            "libplacebo=tonemapping=bt.2390:tonemapping_param=0.5:colorspace=bt709:\
             color_primaries=bt709:color_trc=bt709:range=tv:format=yuv420p"
                .to_string()
        }
    }
}

/// Builds the gamut-only chain for wide-gamut SDR (no tone mapping).
pub fn gamut_only_chain(backend: Backend) -> String {
    match backend {
        // Convert primaries/matrix/transfer to BT.709 limited range without a
        // tone-mapping curve — the transfer is already SDR.
        Backend::Zscale => "zscale=p=bt709:t=bt709:m=bt709:r=tv,format=yuv420p".to_string(),
        Backend::Libplacebo => "libplacebo=colorspace=bt709:color_primaries=bt709:\
             color_trc=bt709:range=tv:format=yuv420p"
            .to_string(),
    }
}

/// Builds the filter chain for a classified input, or `None` for plain SDR
/// (which needs no conversion).
pub fn build_chain(kind: ColorKind, backend: Backend, peak: u32) -> Option<String> {
    match kind {
        ColorKind::Pq | ColorKind::Hlg => Some(tone_map_chain(backend, peak)),
        ColorKind::WideGamutSdr => Some(gamut_only_chain(backend)),
        ColorKind::Sdr => None,
    }
}

/// Pure capability -> backend decision, split out so it can be tested without a
/// live FFmpeg build.
///
/// The key rule (the fix for the "wide-gamut SDR needs no tonemap" bug): the
/// zscale backend can run the **gamut-only** chain with just the `zscale`
/// filter; only the **tone-map** chain additionally needs `tonemap`. So a
/// build with `zscale` but no `tonemap` can still convert wide-gamut SDR.
pub fn backend_from_caps(
    kind: ColorKind,
    prefer_gpu: bool,
    has_zscale: bool,
    has_tonemap: bool,
    has_libplacebo: bool,
) -> Option<Backend> {
    let zscale_ok = has_zscale && (!needs_tone_map(kind) || has_tonemap);
    if prefer_gpu && has_libplacebo {
        return Some(Backend::Libplacebo);
    }
    if zscale_ok {
        return Some(Backend::Zscale);
    }
    if has_libplacebo {
        return Some(Backend::Libplacebo);
    }
    None
}

/// Probes the linked FFmpeg and picks a backend for this `kind`.
///
/// Prefers the deterministic CPU chain; falls back to libplacebo. Returns
/// `None` when nothing usable is compiled in — the caller then fails closed
/// rather than run a naive chain that would gray out the picture.
pub fn pick_backend(kind: ColorKind, prefer_gpu: bool) -> Option<Backend> {
    backend_from_caps(
        kind,
        prefer_gpu,
        is_filter_available("zscale"),
        is_filter_available("tonemap"),
        is_filter_available("libplacebo"),
    )
}

/// The actionable error shown when no tone-mapping filter is available.
pub fn no_backend_message() -> String {
    // FFmpeg 8's swscale can tone-map without any external library, but the
    // filter probe cannot tell FFmpeg 7 from 8 (the `scale` filter exists in
    // both, only the color options are new), so we surface it as a manual
    // option instead of running it blindly on FFmpeg 7 (where it would produce
    // the naive, grayed-out result).
    "this FFmpeg build has neither the 'zscale'+'tonemap' filters (needs \
     libzimg) nor 'libplacebo' (needs a Vulkan-capable build), so HDR tone \
     mapping cannot run.\n\
     Options: install an FFmpeg built with --enable-libzimg or \
     --enable-libplacebo, or, on FFmpeg 8+, use the swscale chain manually:\n  \
     scale=out_color_matrix=bt709:out_primaries=bt709:out_transfer=bt709:\
     out_range=tv:intent=perceptual,format=yuv420p"
        .to_string()
}
