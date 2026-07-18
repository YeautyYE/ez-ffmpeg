//! Probes for what the linked FFmpeg build contains.
//!
//! FFmpeg builds differ widely in which muxers and protocols are compiled in
//! (e.g. the `whip` muxer requires FFmpeg 8 with a DTLS backend, the `srt`
//! protocol requires `--enable-libsrt`). These helpers let applications check
//! for a component up front and fail with an actionable error instead of a
//! mid-pipeline failure.

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
