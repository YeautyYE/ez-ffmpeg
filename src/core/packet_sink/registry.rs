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
//! against the active parameter sets, which the Trusted delivery path
//! deliberately does not do. That property is therefore established per
//! encoder wrapper ahead of time, and the result is recorded here.
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
//! Admission is not availability. A name on this list only lifts the
//! build-time rejection; the linked FFmpeg build must still contain the
//! encoder, and any required hardware must still open.
//!
//! # Verified entries
//!
//! * `libx264` — the v1 baseline; software encoder, exercised end-to-end in
//!   CI. With `GLOBAL_HEADER` it emits avcC extradata and length-prefixed
//!   packets (FFmpeg `libx264.c` `set_avcc_extradata`; older releases emit
//!   Annex-B extradata, which the sink normalizes). Explicit non-zero `bf`
//!   / `max_b_frames` is rejected at build. Unset `bf` keeps FFmpeg's
//!   libx264 default (typically 3 B-frames); runtime still requires
//!   `pts >= dts`. Set `bf=0` for the verified `dts == pts` scope.
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
//! * `h264_videotoolbox` — audited against FFmpeg 7.1/8.1
//!   `libavcodec/videotoolboxenc.c`: one compression callback sample is
//!   converted into one packet (`vtenc_cm_to_avpacket`); `GLOBAL_HEADER`
//!   obtains Annex-B SPS/PPS during open (`vtenc_populate_extradata`) and
//!   omits parameter sets from key packets. **Verified scope is `bf=0`
//!   only**: frame reordering is disabled and `dts == pts`. Enabling B
//!   frames can produce `pts < dts`, which the existing timestamp contract
//!   rejects. The explicit-B-frame build gate is **VideoToolbox-only**:
//!   `"0"` / `"00"` / `" 0"` are admitted; `"3"`, `"0.0"`, `"1b"`, empty
//!   are rejected. Unset keys keep the wrapper default (VideoToolbox: no
//!   B-frames). `libx264` / `h264_nvenc` / `libopenh264` keep runtime
//!   `pts >= dts` enforcement for explicit B-frames. This crate does
//!   **not** rewrite the user's options.
//!   VideoToolbox may emit SEI and may preserve AUD/filler NAL units inside
//!   the same access unit. Availability still depends on the Apple platform
//!   and encoder hardware; Linux/Windows builds do not have this wrapper.
//! * `libopenh264` — audited against FFmpeg 7.1/8.1
//!   `libavcodec/libopenh264enc.c`: one `EncodeFrame` result, including all
//!   slices, is copied into one packet; `GLOBAL_HEADER` copies Annex-B
//!   parameter sets into extradata and skips the IDR parameter-set layer.
//!   OpenH264 does not encode B-frames, so the no-delay path yields
//!   `dts == pts`. Availability requires an FFmpeg build configured with
//!   `--enable-libopenh264`; that flag does not require `--enable-gpl` or
//!   `--enable-nonfree`. OpenH264's source copyright license is BSD-2-Clause;
//!   that is a **copyright** fact, not a patent grant. Cisco's patent
//!   arrangement covers its official binary module under its own terms —
//!   a distro-built or self-built `libopenh264` does not inherit that
//!   coverage. This crate does not claim H.264 patent safety.
//!
//! # Future direction
//!
//! A per-packet access-unit-boundary verifier lives in
//! `codec::avc::au_boundary` as a test-backed prototype. It is **not**
//! wired into the Trusted delivery path: audited wrappers keep zero extra
//! per-NAL work. The correct simplified check (H.264 7.4.1.2.3 / 7.4.1.2.4)
//! counts `first_mb_in_slice == 0` **order-independently** (ASO may delay
//! the macroblock-0 slice), treats AUD/SEI/SPS/PPS after VCL as the start
//! of the next AU, and fail-closes on redundant coded pictures, separate
//! colour planes, and VCL extension types 20/21. A "second VCL with
//! `first_mb == 0`" detector would mis-reject ASO and is not the algorithm.
//! Unaudited H.264 wrappers stay build-time rejected until that verifier
//! is enforced on a `ValidateBoundaries` path.

use std::collections::HashMap;
use std::ffi::CString;

/// Video encoders verified to satisfy the strict-tier delivery contract.
///
/// Order is cosmetic (it is rendered into the whitelist error message);
/// membership is the contract. Keep [`STRICT_TIER_VIDEO_ALLOWED`] in sync —
/// a unit test enforces it.
pub(crate) const STRICT_TIER_VIDEO_ENCODERS: &[&str] =
    &["libx264", "h264_nvenc", "h264_videotoolbox", "libopenh264"];

/// The comma-joined registry, rendered into the typed whitelist error
/// (`PacketSinkError::EncoderNotWhitelisted::allowed`).
pub(crate) const STRICT_TIER_VIDEO_ALLOWED: &str =
    "libx264, h264_nvenc, h264_videotoolbox, libopenh264";

/// Whether `name` (an `AVCodec.name`) is admitted for strict-tier video.
pub(crate) fn is_strict_tier_video_encoder(name: &str) -> bool {
    STRICT_TIER_VIDEO_ENCODERS.contains(&name)
}

/// Packet-sink B-frame admission for VideoToolbox.
///
/// Verified VideoToolbox scope is `bf=0` (`dts == pts`). For keys `bf` and
/// `max_b_frames`, the value is ASCII-trimmed and parsed as `i32`: missing
/// keys are admitted, parsed `0` is admitted, any other integer or
/// unparseable value is rejected at build. Other registry entries are not
/// gated here — their B-frames still have to satisfy runtime `pts >= dts`.
/// `max_b_frames` is a **policy-recognized admission key only** — it is
/// *not* an FFmpeg `AVOption` alias of `bf`, so a leftover
/// `max_b_frames=0` does not set the encoder's `bf` field and does not by
/// itself guarantee `dts == pts`; only `bf` reaches the encoder. This helper
/// never rewrites `opts`. Unset keys therefore keep the wrapper default:
/// VideoToolbox / NVENC / OpenH264 default to no B-frames, but **libx264's
/// FFmpeg default is B-frames** (`bf` unset is typically 3).
///
/// Pass the **effective** option table (per-map keys overlay per-type keys).
pub(crate) fn admit_strict_tier_b_frame_opts(
    encoder: &str,
    opts: Option<&HashMap<CString, CString>>,
) -> crate::error::Result<()> {
    use crate::error::PacketSinkError;

    if encoder != "h264_videotoolbox" {
        return Ok(());
    }
    let Some(opts) = opts else {
        return Ok(());
    };
    for (key, value) in opts {
        if matches!(key.as_bytes(), b"bf" | b"max_b_frames") && !b_frame_opt_is_zero(value) {
            return Err(PacketSinkError::BFramesUnsupported {
                encoder: encoder.to_string(),
            }
            .into());
        }
    }
    Ok(())
}

/// True iff `value` is an ASCII-trimmed `i32` equal to 0.
///
/// Unparseable input (empty, `"0.0"`, `"1b"`, non-UTF-8) returns false so
/// admission fail-closes. The option table is not mutated.
fn b_frame_opt_is_zero(value: &CString) -> bool {
    std::str::from_utf8(value.as_bytes())
        .ok()
        .and_then(|s| s.trim_ascii().parse::<i32>().ok())
        == Some(0)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn admitted_encoders_pass_and_everything_else_is_rejected() {
        assert!(is_strict_tier_video_encoder("libx264"));
        assert!(is_strict_tier_video_encoder("h264_nvenc"));
        assert!(is_strict_tier_video_encoder("h264_videotoolbox"));
        assert!(is_strict_tier_video_encoder("libopenh264"));
        // Same-codec hardware wrappers stay out until they go through
        // admission; other codecs stay out regardless.
        for name in [
            "h264_qsv",
            "h264_amf",
            "h264_vaapi",
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

    fn copts(pairs: &[(&str, &str)]) -> HashMap<CString, CString> {
        pairs
            .iter()
            .map(|(k, v)| (CString::new(*k).unwrap(), CString::new(*v).unwrap()))
            .collect()
    }

    fn assert_admitted(opts: Option<&HashMap<CString, CString>>) {
        admit_strict_tier_b_frame_opts("h264_videotoolbox", opts)
            .unwrap_or_else(|e| panic!("expected VideoToolbox admission, got {e}"));
    }

    fn assert_b_frames_rejected(opts: &HashMap<CString, CString>) {
        match admit_strict_tier_b_frame_opts("h264_videotoolbox", Some(opts)) {
            Err(crate::error::Error::PacketSink(
                crate::error::PacketSinkError::BFramesUnsupported { encoder },
            )) => {
                assert_eq!(encoder, "h264_videotoolbox");
            }
            other => panic!("expected BFramesUnsupported for VT B-frames, got {other:?}"),
        }
    }

    #[test]
    fn videotoolbox_unset_and_explicit_zero_are_admitted() {
        assert_admitted(None);
        assert_admitted(Some(&HashMap::new()));
        assert_admitted(Some(&copts(&[("bf", "0")])));
        assert_admitted(Some(&copts(&[("max_b_frames", "0")])));
        assert_admitted(Some(&copts(&[("bf", "0"), ("max_b_frames", "0")])));
        assert_admitted(Some(&copts(&[("g", "25")])));
        // Integer parse, not a raw byte match: padded / ASCII-trimmed zero.
        assert_admitted(Some(&copts(&[("bf", "00")])));
        assert_admitted(Some(&copts(&[("bf", " 0")])));
        assert_admitted(Some(&copts(&[("max_b_frames", "00")])));
        assert_admitted(Some(&copts(&[("max_b_frames", " 0")])));
    }

    #[test]
    fn non_videotoolbox_explicit_b_frames_are_admitted() {
        for encoder in STRICT_TIER_VIDEO_ENCODERS {
            if *encoder == "h264_videotoolbox" {
                continue;
            }
            admit_strict_tier_b_frame_opts(encoder, Some(&copts(&[("bf", "3")])))
                .unwrap_or_else(|e| panic!("{encoder}: expected admission, got {e}"));
        }
    }

    #[test]
    fn videotoolbox_explicit_nonzero_b_frames_are_rejected() {
        assert_b_frames_rejected(&copts(&[("bf", "3")]));
        assert_b_frames_rejected(&copts(&[("max_b_frames", "2")]));
        assert_b_frames_rejected(&copts(&[("bf", "0"), ("max_b_frames", "2")]));
        assert_b_frames_rejected(&copts(&[("bf", "3"), ("max_b_frames", "0")]));
    }

    #[test]
    fn videotoolbox_unparseable_b_frame_opts_are_rejected() {
        assert_b_frames_rejected(&copts(&[("bf", "")]));
        assert_b_frames_rejected(&copts(&[("bf", "0.0")]));
        assert_b_frames_rejected(&copts(&[("bf", "1b")]));
        assert_b_frames_rejected(&copts(&[("max_b_frames", "")]));
        assert_b_frames_rejected(&copts(&[("max_b_frames", "0.0")]));
        assert_b_frames_rejected(&copts(&[("max_b_frames", "1b")]));
    }
}
