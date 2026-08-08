//! Strict-tier verified-encoder registry: the single source of truth for
//! the video encoders a packet-sink output accepts.
//!
//! # Why admission is a registry, not a runtime check
//!
//! Every other strict-tier promise is enforced per packet on the delivery
//! path (NAL framing, timestamp ordering, in-band parameter-set rejection,
//! configuration fingerprinting — see `strict.rs`). The one property that
//! cannot be verified there is **one packet == one access unit**: detecting
//! an access-unit boundary inside a packet requires decoding slice headers
//! against the active parameter sets, which the delivery path deliberately
//! does not do. That property is therefore established per encoder wrapper
//! ahead of time, and the result is recorded here.
//!
//! # Admission requirements
//!
//! A video encoder enters [`STRICT_TIER_VIDEO_ENCODERS`] only with all of:
//!
//! 1. **Wrapper audit** against both FFmpeg versions CI pins (currently 7.1
//!    and 8.1): exactly one `AVPacket` per encoded picture, honors
//!    `AV_CODEC_FLAG_GLOBAL_HEADER` (parameter sets in extradata at open
//!    time, none in-band), monotonically increasing dts.
//! 2. **Emission-shape fixtures** in the strict-tier unit suite covering the
//!    wrapper's packet shapes, so CI pins acceptance without the hardware.
//! 3. **A hardware acceptance line**: a skip-guarded integration test plus a
//!    machine that actually runs it (maintainer or requesting consumer).
//! 4. A note in the module rustdoc documenting the verified scope.
//!
//! # Verified entries
//!
//! * `libx264` — the v1 baseline; software encoder, exercised end-to-end in
//!   CI. With `GLOBAL_HEADER` it emits avcC extradata and length-prefixed
//!   packets (FFmpeg `libx264.c` `set_avcc_extradata`; older releases emit
//!   Annex-B extradata, which the sink normalizes).
//! * `h264_nvenc` — audited against FFmpeg 7.1/8.1 `libavcodec/nvenc.c`:
//!   one `NvEncLockBitstream` per picture becomes one packet
//!   (`process_output_surface`); `GLOBAL_HEADER` sets `disableSPSPPS = 1`
//!   and populates Annex-B extradata at init (`nvenc_setup_extradata`);
//!   with `bf=0` the reorder-delay path is bypassed and `dts == pts`;
//!   forced keyframes are IDR under `forced-idr=1`; `aud` defaults off.
//!   Packets are Annex-B (normalized by the sink) and may carry SEI
//!   prefixes and, under CBR padding, filler NAL units — all inside the
//!   same access unit. Availability still depends on the linked FFmpeg
//!   build and NVIDIA hardware at open time; admission only lifts the
//!   build-time rejection.
//!
//! # Future direction
//!
//! If admission pressure grows, the registry gate can be replaced by a
//! per-packet access-unit-boundary verifier (`first_mb_in_slice` is the
//! first syntax element of every slice header; a second in-packet VCL NAL
//! decoding it to zero announces a second access unit). That would relax
//! admission to any H.264 encoder — strictly widening, never breaking — and
//! demote this registry to documentation.

/// Video encoders verified to satisfy the strict-tier delivery contract.
///
/// Order is cosmetic (it is rendered into the whitelist error message);
/// membership is the contract. Keep [`STRICT_TIER_VIDEO_ALLOWED`] in sync —
/// a unit test enforces it.
pub(crate) const STRICT_TIER_VIDEO_ENCODERS: &[&str] = &["libx264", "h264_nvenc"];

/// The comma-joined registry, rendered into the typed whitelist error
/// (`PacketSinkError::EncoderNotWhitelisted::allowed`).
pub(crate) const STRICT_TIER_VIDEO_ALLOWED: &str = "libx264, h264_nvenc";

/// Whether `name` (an `AVCodec.name`) is admitted for strict-tier video.
pub(crate) fn is_strict_tier_video_encoder(name: &str) -> bool {
    STRICT_TIER_VIDEO_ENCODERS.contains(&name)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn admitted_encoders_pass_and_everything_else_is_rejected() {
        assert!(is_strict_tier_video_encoder("libx264"));
        assert!(is_strict_tier_video_encoder("h264_nvenc"));
        // Same-codec hardware wrappers stay out until they go through
        // admission; other codecs stay out regardless.
        for name in [
            "h264_qsv",
            "h264_amf",
            "h264_videotoolbox",
            "hevc_nvenc",
            "libx265",
            "mpeg4",
            "",
        ] {
            assert!(
                !is_strict_tier_video_encoder(name),
                "{name:?} must be rejected"
            );
        }
    }

    #[test]
    fn allowed_string_stays_in_sync_with_the_registry() {
        assert_eq!(
            STRICT_TIER_VIDEO_ALLOWED,
            STRICT_TIER_VIDEO_ENCODERS.join(", ")
        );
    }
}
