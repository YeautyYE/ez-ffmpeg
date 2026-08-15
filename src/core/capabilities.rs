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
//!
//! // Codecs, filters, and input protocols have their own probes.
//! let has_x264 = capabilities::is_encoder_available("libx264");
//! let has_h264_decode = capabilities::is_decoder_available("h264");
//! let has_cropdetect = capabilities::is_filter_available("cropdetect");
//! let has_scale_intent = capabilities::is_filter_option_available("scale", "intent");
//! let has_https_input = capabilities::is_input_protocol_available("https");
//! ```
//!
//! A `true` result only means the component is compiled into the linked
//! FFmpeg; device/driver readiness (hardware encoders), TLS backends,
//! endpoint compatibility, and network reachability are separate concerns.
//! A compiled-in filter option also does not mean the option performs the
//! intended color transform — for example `scale` exists in FFmpeg 7.1, but
//! `intent=perceptual` is an FFmpeg 8 libswscale option.
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

use ffmpeg_sys_next::{
    av_guess_format, av_opt_find, avcodec_find_decoder_by_name, avcodec_find_encoder_by_name,
    avfilter_get_by_name, avfilter_get_class, avfilter_graph_alloc_filter, avio_enum_protocols,
    AVClass, AVFilter, AV_OPT_SEARCH_CHILDREN, AV_OPT_SEARCH_FAKE_OBJ,
};
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

/// Returns whether the linked FFmpeg build contains an encoder with this
/// exact name.
///
/// A `true` result only means the encoder is registered — compiled into the
/// linked FFmpeg build. It does not prove the encoder can be opened on this
/// machine: hardware encoders such as `h264_nvenc` or `h264_videotoolbox`
/// still need a device, driver, and a free encoding session at open time,
/// and any encoder can still reject a specific resolution, pixel format, or
/// option set. `name` is the encoder name as listed by
/// [`get_encoders`](crate::codec::get_encoders) (e.g. `"libx264"`,
/// `"h264_videotoolbox"`) — encoder names are not always codec names.
/// Names containing an interior NUL byte return `false`.
///
/// ```rust,ignore
/// // The native AAC encoder is part of every FFmpeg build.
/// assert!(ez_ffmpeg::capabilities::is_encoder_available("aac"));
/// let has_x264 = ez_ffmpeg::capabilities::is_encoder_available("libx264");
/// ```
pub fn is_encoder_available(name: &str) -> bool {
    // Same process-history concern as is_muxer_available: keep probe results
    // independent of which crate API ran first.
    crate::core::initialize_ffmpeg();
    let Ok(name_cstr) = CString::new(name) else {
        return false;
    };
    // SAFETY: `name_cstr` is a live NUL-terminated string borrowed for the
    // call only; the returned registry pointer is only null-checked, never
    // dereferenced.
    !unsafe { avcodec_find_encoder_by_name(name_cstr.as_ptr()) }.is_null()
}

/// Returns whether the linked FFmpeg build contains a decoder with this
/// exact name.
///
/// A `true` result only means the decoder is registered in the linked FFmpeg
/// build; hardware decoders still need a device and driver at open time.
/// `name` is the decoder name (e.g. `"h264"`, `"libdav1d"`), which is not
/// always the codec name. Names containing an interior NUL byte return
/// `false`.
///
/// ```rust,ignore
/// // The native H.264 decoder is part of every FFmpeg build.
/// assert!(ez_ffmpeg::capabilities::is_decoder_available("h264"));
/// ```
pub fn is_decoder_available(name: &str) -> bool {
    crate::core::initialize_ffmpeg();
    let Ok(name_cstr) = CString::new(name) else {
        return false;
    };
    // SAFETY: see is_encoder_available — pointer is only null-checked.
    !unsafe { avcodec_find_decoder_by_name(name_cstr.as_ptr()) }.is_null()
}

/// Returns whether the linked FFmpeg build contains a filter with this name.
///
/// A `true` result only means the filter is compiled in; it does not prove a
/// specific option set is accepted, and hardware filters still need a device
/// at graph-configuration time. Some filters are build-gated: for example
/// `cropdetect` only exists in GPL-enabled builds (`--enable-gpl`), and
/// `zscale` requires `--enable-libzimg`. [`crate::analysis::VideoDetector::Crop`]
/// uses the crate's Rust scanner and does not need `cropdetect`. Names
/// containing an interior NUL byte return `false`.
///
/// This is the general-purpose filter probe; it returns the same answer as
/// [`hwaccel::is_filter_available`](crate::hwaccel::is_filter_available),
/// which predates it and remains available.
///
/// ```rust,ignore
/// assert!(ez_ffmpeg::capabilities::is_filter_available("scale"));
/// let has_cropdetect = ez_ffmpeg::capabilities::is_filter_available("cropdetect");
/// ```
pub fn is_filter_available(name: &str) -> bool {
    crate::core::initialize_ffmpeg();
    crate::core::hwaccel::is_filter_available(name)
}

/// Returns whether the named option is declared on the named filter, or on
/// one of that filter's child AVClasses.
///
/// This probes **options**, not FFmpeg version strings and not merely the
/// filter name. `scale` exists in FFmpeg 7.1 and 8.x alike; the color-management
/// options `out_primaries`, `out_transfer`, and the SwsContext child option
/// `intent` exist only in builds that compiled them in (typically FFmpeg 8+).
/// Distro backports and trimmed builds can disagree with the version, so
/// callers must probe these options rather than parse `av_version_info()`.
///
/// A `true` result only means the option is compiled into the linked FFmpeg
/// filter. It does **not** mean applying that option performs the intended
/// color transform: `scale` without `intent=perceptual` will not tone-map
/// PQ/HLG, and even with the option present the perceptual curve is not a
/// stable pixel ABI across FFmpeg 8.x releases.
///
/// Empty names and names containing an interior NUL byte return `false`.
/// Unknown filters and unknown options return `false`. Child options such
/// as SwsContext `intent` are discovered with
/// `AV_OPT_SEARCH_FAKE_OBJ | AV_OPT_SEARCH_CHILDREN` on the filter's
/// private class — a live `scale` instance does not create its SwsContext
/// until init, so searching only a bare instance would miss `intent`.
///
/// ```rust,ignore
/// assert!(ez_ffmpeg::capabilities::is_filter_option_available("scale", "w"));
/// let has_perceptual = ez_ffmpeg::capabilities::is_filter_option_available("scale", "intent");
/// ```
pub fn is_filter_option_available(filter: &str, option: &str) -> bool {
    crate::core::initialize_ffmpeg();
    if filter.is_empty() || option.is_empty() {
        return false;
    }
    let Ok(filter_c) = CString::new(filter) else {
        return false;
    };
    let Ok(option_c) = CString::new(option) else {
        return false;
    };

    // SAFETY: `filter_c` is a live NUL-terminated string borrowed only for
    // this call; a null return means the filter is not registered.
    let avfilter = unsafe { avfilter_get_by_name(filter_c.as_ptr()) };
    if avfilter.is_null() {
        return false;
    }

    // SAFETY: `avfilter` is a non-null registry pointer; `priv_class` is
    // a static AVClass pointer (possibly null, handled below). Search
    // children so SwsContext options such as `intent` are visible.
    if avclass_has_option(
        unsafe { (*avfilter).priv_class },
        &option_c,
        AV_OPT_SEARCH_FAKE_OBJ | AV_OPT_SEARCH_CHILDREN,
    ) {
        return true;
    }
    // SAFETY: `avfilter_get_class` returns the static AVFilterContext class.
    // Do NOT set SEARCH_CHILDREN here: that class's child iterator walks
    // every registered filter's priv_class, which would make `scale`'s `w`
    // appear on unrelated filters.
    if avclass_has_option(
        unsafe { avfilter_get_class() },
        &option_c,
        AV_OPT_SEARCH_FAKE_OBJ,
    ) {
        return true;
    }

    // Isolated graph + filter instance, as a second look for options that
    // exist on the live object. Child classes that are not instantiated
    // until init (SwsContext) are already covered by the FAKE_OBJ search
    // above; this path must not be the only probe for `intent`.
    live_filter_has_option(avfilter, &option_c)
}

fn avclass_has_option(class: *const AVClass, option: &CStr, search_flags: libc::c_int) -> bool {
    if class.is_null() {
        return false;
    }
    let mut class_ptr = class;
    // SAFETY: `AV_OPT_SEARCH_FAKE_OBJ` treats `obj` as a pointer to an
    // `AVClass` pointer. `class_ptr` is a stack local that outlives the
    // call; the returned `AVOption` is in static filter metadata and is
    // only null-checked.
    let found = unsafe {
        av_opt_find(
            (&mut class_ptr as *mut *const AVClass).cast::<c_void>(),
            option.as_ptr(),
            null(),
            0,
            search_flags,
        )
    };
    !found.is_null()
}

fn live_filter_has_option(filter: *const AVFilter, option: &CStr) -> bool {
    let Some(graph) = crate::raw::FilterGraph::alloc() else {
        return false;
    };
    // SAFETY: `graph` owns the AVFilterGraph for this scope. The instance
    // name is a static C string. `avfilter_graph_alloc_filter` either
    // returns a context owned by `graph` (freed on Drop) or null.
    let ctx = unsafe { avfilter_graph_alloc_filter(graph.as_ptr(), filter, c"opt_probe".as_ptr()) };
    if ctx.is_null() {
        return false;
    }
    // SAFETY: `ctx` is a live filter context owned by `graph`. The option
    // CStr outlives the call. The AVOption pointer is only null-checked.
    let found = unsafe {
        av_opt_find(
            ctx.cast::<c_void>(),
            option.as_ptr(),
            null(),
            0,
            AV_OPT_SEARCH_CHILDREN,
        )
    };
    !found.is_null()
}

/// Returns whether the linked FFmpeg build contains an I/O protocol with
/// this name that supports **input** (reading).
///
/// A `true` result only means the protocol is registered for input in the
/// linked FFmpeg build; it does not guarantee that TLS backends, remote
/// endpoints, or network paths are usable at runtime. `name` is the protocol
/// name as it appears before `://` in a URL (e.g. `"file"`, `"https"`,
/// `"srt"`), not a URL or file name.
///
/// The probe is direction-aware: output-only protocols are not matched. Use
/// this — not [`is_output_protocol_available`] — to check whether
/// `https://` **inputs** can work (HTTPS support requires an FFmpeg build
/// with a TLS backend such as GnuTLS or OpenSSL).
///
/// ```rust,ignore
/// assert!(ez_ffmpeg::capabilities::is_input_protocol_available("file"));
/// let has_https = ez_ffmpeg::capabilities::is_input_protocol_available("https");
/// ```
pub fn is_input_protocol_available(name: &str) -> bool {
    crate::core::initialize_ffmpeg();
    let mut opaque: *mut c_void = null_mut();
    loop {
        // SAFETY: avio_enum_protocols iterates a static registry; `opaque` is
        // the iterator state it owns, and the returned C string is borrowed
        // only for the comparison below.
        let protocol = unsafe { avio_enum_protocols(&mut opaque, 0) };
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

    #[test]
    fn test_is_encoder_available() {
        // The native AAC encoder is part of every FFmpeg build. Deliberately
        // no assertions on optional encoders (libx264, hardware wrappers,
        // ...): their presence depends on the local build configuration.
        assert!(is_encoder_available("aac"));
        assert!(!is_encoder_available("definitely_not_an_encoder_xyz"));
        assert!(!is_encoder_available("bad\0name"));
        assert!(!is_encoder_available(""));
    }

    #[test]
    fn test_is_decoder_available() {
        // The native H.264 decoder is part of every FFmpeg build.
        assert!(is_decoder_available("h264"));
        assert!(!is_decoder_available("definitely_not_a_decoder_xyz"));
        assert!(!is_decoder_available("bad\0name"));
        assert!(!is_decoder_available(""));
    }

    #[test]
    fn test_is_filter_available() {
        // "scale" is compiled into every FFmpeg build. Deliberately no
        // assertion on GPL-gated filters (cropdetect, ...): their presence
        // depends on the local build configuration.
        assert!(is_filter_available("scale"));
        assert!(!is_filter_available("definitely_not_a_filter_xyz"));
        assert!(!is_filter_available("bad\0name"));

        // The general probe and the hwaccel probe must agree.
        for name in ["scale", "cropdetect", "definitely_not_a_filter_xyz"] {
            assert_eq!(
                is_filter_available(name),
                crate::core::hwaccel::is_filter_available(name),
            );
        }
    }

    #[test]
    fn test_is_filter_option_available() {
        // `w` is a scale option in every FFmpeg build. Empty / interior-NUL
        // / unknown names never match. Deliberately no assertion that
        // FFmpeg 8 color-management options (`intent`, `out_primaries`) are
        // present: that depends on the linked FFmpeg, which is why this
        // probe exists.
        assert!(is_filter_option_available("scale", "w"));
        assert!(is_filter_option_available("scale", "width"));
        assert!(!is_filter_option_available(
            "scale",
            "definitely_not_a_scale_option_xyz"
        ));
        assert!(!is_filter_option_available(
            "definitely_not_a_filter_xyz",
            "w"
        ));
        assert!(!is_filter_option_available("", "w"));
        assert!(!is_filter_option_available("scale", ""));
        assert!(!is_filter_option_available("bad\0name", "w"));
        assert!(!is_filter_option_available("scale", "bad\0name"));

        // A compiled-in option on one filter is not reported for another.
        // `hflip` exists in every build and has no `w`; `null` is the same.
        assert!(!is_filter_option_available("hflip", "w"));
        assert!(!is_filter_option_available("null", "w"));
    }

    #[test]
    fn test_is_input_protocol_available() {
        // "file" supports input in every default FFmpeg build. Deliberately
        // no assertion on TLS-backed protocols (https, ...): their presence
        // depends on the local FFmpeg build configuration.
        assert!(is_input_protocol_available("file"));
        assert!(!is_input_protocol_available(
            "definitely_not_a_protocol_xyz"
        ));
        assert!(!is_input_protocol_available("bad\0name"));
        assert!(!is_input_protocol_available(""));
    }
}
