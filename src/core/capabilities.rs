//! Probes for what the linked FFmpeg build contains.
//!
//! FFmpeg builds differ widely in which muxers and protocols are compiled in
//! (e.g. the `whip` muxer requires FFmpeg 8 with a DTLS backend, the `srt`
//! protocol requires `--enable-libsrt`). These helpers let applications check
//! for a component up front and fail with an actionable error instead of a
//! mid-pipeline failure:
//!
//! ```rust,no_run
//! use ez_ffmpeg::capabilities;
//!
//! // Muxers (output formats) and I/O protocols are separate namespaces.
//! let has_whip_muxer = capabilities::is_muxer_available("whip");
//! let has_srt_protocol = capabilities::is_output_protocol_available("srt");
//! ```
//!
//! A `true` result only means the component is compiled into the linked
//! FFmpeg; encoders, TLS backends, endpoint compatibility, and network
//! reachability are separate concerns.
//!
//! The rest of this page documents the two streaming outputs these probes
//! are most often used for: WHIP and SRT.
//!
//! # WHIP output (experimental, FFmpeg 8+)
//!
//! **Status:** FFmpeg 8 ships an upstream `whip` muxer that publishes WebRTC
//! streams to WHIP endpoints (Twitch/IVS, Cloudflare Stream, LiveKit,
//! MediaMTX, ...). The muxer is marked **experimental** upstream and has a
//! known upstream FIXME on Opus timestamp handling, and ez-ffmpeg's own CI
//! cannot exercise it (its FFmpeg builds carry no DTLS backend) — treat this
//! section as status and instructions, not as a verified recipe.
//!
//! Requirements, all imposed by the upstream muxer:
//!
//! - **FFmpeg 8.0 or newer, built with a DTLS-capable TLS backend.** FFmpeg
//!   8.0 supports OpenSSL or Schannel for DTLS; FFmpeg 8.1 accepts any of
//!   OpenSSL, GnuTLS, Schannel, or mbedTLS. Without one of these, the `whip`
//!   muxer is not compiled in — `is_muxer_available("whip")` returns
//!   `false`.
//! - **Video: H.264 with B-frames disabled.** The muxer rejects B-frames
//!   (real-time WebRTC playout does not reorder frames) and needs the H.264
//!   profile/level present in global headers — ez-ffmpeg raises the
//!   encoder's global-header flag automatically whenever a muxer requires
//!   it, so that part needs no configuration. The muxer writes whatever
//!   profile you encode into the SDP; **Baseline/Constrained Baseline is the
//!   conservative interoperability choice** for WebRTC playout (the example
//!   pins it), not a muxer-enforced restriction.
//! - **Audio: Opus at 48 kHz stereo** — the muxer enforces this combination.
//!
//! Discover the encoders your build offers with
//! [`get_encoders`](crate::codec::get_encoders). H.264 encoders are commonly
//! `libx264` (GPL — mind your licensing), `libopenh264`, or hardware
//! encoders such as `h264_nvenc` / `h264_videotoolbox`; Opus is commonly
//! `libopus`.
//!
//! ```rust,no_run
//! use ez_ffmpeg::{capabilities, FfmpegContext, FfmpegScheduler, Input, Output};
//!
//! fn main() -> Result<(), Box<dyn std::error::Error>> {
//!     if !capabilities::is_muxer_available("whip") {
//!         return Err("this FFmpeg build has no 'whip' muxer \
//!                     (WHIP needs FFmpeg >= 8.0 with a DTLS backend)"
//!             .into());
//!     }
//!
//!     // File inputs must be paced to real time for live publishing.
//!     let input = Input::from("input.mp4").set_readrate(1.0);
//!
//!     let output = Output::from("https://example.com/whip/endpoint")
//!         .set_format("whip") // required: never auto-guessed from the URL
//!         .set_format_opt("authorization", "<token>") // raw token; FFmpeg itself adds "Bearer "
//!         .set_video_codec("libx264") // pick from codec::get_encoders()
//!         .set_video_codec_opt("profile", "baseline") // conservative WebRTC interop choice
//!         .set_video_codec_opt("bf", "0") // WHIP: no B-frames
//!         .set_audio_codec("libopus")
//!         .set_audio_sample_rate(48000) // the muxer requires 48 kHz stereo Opus
//!         .set_audio_channels(2);
//!
//!     FfmpegScheduler::new(FfmpegContext::builder().input(input).output(output).build()?)
//!         .start()?
//!         .wait()?;
//!     Ok(())
//! }
//! ```
//!
//! # SRT output
//!
//! SRT output needs two independent components in the linked FFmpeg build:
//! the `srt` **protocol** (built with `--enable-libsrt`) for transport, and
//! a container muxer for the payload — MPEG-TS below. Probe both:
//!
//! ```rust,no_run
//! use ez_ffmpeg::capabilities;
//!
//! let srt_ready = capabilities::is_output_protocol_available("srt")
//!     && capabilities::is_muxer_available("mpegts");
//! ```
//!
//! > **Never test SRT streaming support with `is_muxer_available("srt")`.**
//! > That name matches the SubRip **subtitle** muxer, which exists in
//! > practically every FFmpeg build, so the probe returns `true` whether or
//! > not the SRT transport is present — it tells you nothing about SRT
//! > streaming support.
//!
//! ```rust,no_run
//! use ez_ffmpeg::{capabilities, FfmpegContext, FfmpegScheduler, Output};
//!
//! fn main() -> Result<(), Box<dyn std::error::Error>> {
//!     if !(capabilities::is_output_protocol_available("srt")
//!         && capabilities::is_muxer_available("mpegts"))
//!     {
//!         return Err("this FFmpeg build lacks the srt protocol \
//!                     (--enable-libsrt) or the mpegts muxer"
//!             .into());
//!     }
//!
//!     let output = Output::from(
//!         // ALL protocol options live in the URL query; latency is in MICROSECONDS.
//!         "srt://127.0.0.1:9000?mode=caller&transtype=live&latency=120000&payload_size=1316",
//!     )
//!     .set_format("mpegts");
//!
//!     FfmpegScheduler::new(FfmpegContext::builder().input("input.mp4").output(output).build()?)
//!         .start()?
//!         .wait()?;
//!     Ok(())
//! }
//! ```
//!
//! Three warnings worth reading twice:
//!
//! - **Protocol options go in the URL query — only.** On the output side,
//!   ez-ffmpeg opens the network connection without an options dictionary,
//!   so [`Output::set_format_opt`](crate::Output::set_format_opt) feeds the
//!   MPEG-TS **muxer**, never the SRT protocol. A `passphrase` set that way
//!   only draws an "option not recognized" warning from the muxer and never
//!   reaches the transport, so the stream goes out **unencrypted** with no
//!   hard error. Encryption parameters (`passphrase`, `pbkeylen`) must go in
//!   the URL query; percent-encode the passphrase if it contains characters
//!   reserved in URLs.
//! - **`latency` is in microseconds, not milliseconds.** `latency=120000`
//!   means 120 ms. libsrt truncates the value to whole milliseconds by
//!   integer division, so a value of `120` collapses to 0 ms — an unusable
//!   budget.
//! - **Redact stream URLs in logs.** With SRT, credentials (`passphrase`)
//!   are part of the URL itself, so any log line printing the URL leaks
//!   them.
//!
//! On the **input** side, `srt://` is a live network protocol with no seek
//! support, and — unlike the output side —
//! [`Input::set_format_opt`](crate::Input::set_format_opt) options do reach
//! the `avformat_open_input` call.

use ffmpeg_sys_next::{av_guess_format, avio_enum_protocols};
use std::ffi::{c_void, CStr, CString};
use std::ptr::{null, null_mut};

/// Returns whether the linked FFmpeg build contains a muxer (output format)
/// with this short name.
///
/// A `true` result only means the muxer is registered in the linked FFmpeg
/// build; it does not guarantee that the encoders, TLS/DTLS backends, or
/// network endpoints the format needs at runtime are also available. `name`
/// is the muxer short name (e.g. `"matroska"`, `"mpegts"`, `"whip"`), not a
/// file name — no file-extension guessing is applied. Names containing an
/// interior NUL byte return `false`.
///
/// Note: muxer names and protocol names are separate namespaces. The `srt`
/// *muxer* is the SubRip subtitle format, unrelated to the SRT streaming
/// protocol — use [`is_output_protocol_available`] for protocols.
///
/// ```rust,ignore
/// assert!(ez_ffmpeg::capabilities::is_muxer_available("matroska"));
/// let has_whip = ez_ffmpeg::capabilities::is_muxer_available("whip");
/// ```
pub fn is_muxer_available(name: &str) -> bool {
    // Device output formats (sdl2, alsa, pulse, ...) only enter the muxer
    // iteration after avdevice registration; without this the answer would
    // depend on whether some other crate API ran first in this process.
    crate::core::initialize_ffmpeg();
    let Ok(name_cstr) = CString::new(name) else {
        return false;
    };
    !unsafe { av_guess_format(name_cstr.as_ptr(), null(), null()) }.is_null()
}

/// Returns whether the linked FFmpeg build contains an I/O protocol with this
/// name that supports **output** (writing).
///
/// A `true` result only means the protocol is registered for output in the
/// linked FFmpeg build; it does not guarantee that the TLS backends, remote
/// endpoints, or network paths a stream needs at runtime are also available.
/// `name` is the protocol name as it appears before `://` in a URL (e.g.
/// `"file"`, `"srt"`, `"rtmp"`), not a URL or file name.
///
/// The probe is direction-aware: input-only protocols are not matched.
///
/// ```rust,ignore
/// assert!(ez_ffmpeg::capabilities::is_output_protocol_available("file"));
/// let has_srt = ez_ffmpeg::capabilities::is_output_protocol_available("srt");
/// ```
pub fn is_output_protocol_available(name: &str) -> bool {
    // Same process-history concern as is_muxer_available: keep probe results
    // independent of which crate API ran first.
    crate::core::initialize_ffmpeg();
    let mut opaque: *mut c_void = null_mut();
    loop {
        let protocol = unsafe { avio_enum_protocols(&mut opaque, 1) };
        if protocol.is_null() {
            return false;
        }
        // Byte comparison against the C name: a &str with an interior NUL
        // can never match, so such inputs fall out as `false` with no guard.
        if unsafe { CStr::from_ptr(protocol) }.to_bytes() == name.as_bytes() {
            return true;
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_is_muxer_available() {
        // "matroska" is compiled into every default FFmpeg build; garbage
        // names and interior-NUL names never match. Deliberately no
        // assertions on optional muxers (whip, ...): their presence depends
        // on the local FFmpeg build configuration.
        assert!(is_muxer_available("matroska"));
        assert!(!is_muxer_available("definitely_not_a_muxer_xyz"));
        assert!(!is_muxer_available("bad\0name"));
    }

    #[test]
    fn test_is_output_protocol_available() {
        // "file" supports output in every default FFmpeg build. Deliberately
        // no assertions on optional protocols (srt, ...): their presence
        // depends on the local FFmpeg build configuration.
        assert!(is_output_protocol_available("file"));
        assert!(!is_output_protocol_available(
            "definitely_not_a_protocol_xyz"
        ));
        assert!(!is_output_protocol_available("bad\0name"));
        assert!(!is_output_protocol_available(""));
    }
}
