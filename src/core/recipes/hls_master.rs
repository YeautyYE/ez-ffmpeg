//! Pure-Rust generation of an HLS master playlist (`master.m3u8`).
//!
//! This module has **no FFmpeg dependency**: it turns a list of already-resolved
//! [`MasterVariant`] descriptors into the text of an
//! [RFC 8216](https://www.rfc-editor.org/rfc/rfc8216) multivariant (master)
//! playlist. Keeping the text generation separate from the transcode pipeline
//! makes it trivially unit-testable and keeps [`crate::core::recipes::hls`]
//! focused on wiring FFmpeg outputs.
//!
//! The generator is intentionally conservative (see the MVP boundary notes on
//! `HlsLadder`): it emits `#EXT-X-VERSION:<version>` (the hls recipe passes
//! `3` for MPEG-TS ladders and `7` for fMP4 ones), one `#EXT-X-STREAM-INF`
//! per variant with `BANDWIDTH` and `RESOLUTION`, and an optional `CODECS`
//! attribute. Variants are sorted ascending by `BANDWIDTH` so the first entry
//! is the lowest-bitrate fallback, matching HLS client expectations.

use std::fmt::Write as _;

/// A single resolved entry of the master playlist.
///
/// This is the "rendition-like" descriptor consumed by
/// [`generate_master_playlist`]. Unlike [`crate::core::recipes::hls::Rendition`]
/// (user input), every field here is already computed: the peak `bandwidth`
/// in bits per second, the pixel `width`/`height`, the relative child playlist
/// `uri`, and the optional `codecs` attribute string.
#[derive(Debug, Clone)]
pub(crate) struct MasterVariant {
    /// Peak muxed bandwidth in **bits per second**, written verbatim as the
    /// `BANDWIDTH` attribute. Also the sort key (ascending).
    pub(crate) bandwidth: u64,

    /// Coded width in pixels (the `RESOLUTION` width component).
    pub(crate) width: u32,

    /// Coded height in pixels (the `RESOLUTION` height component).
    pub(crate) height: u32,

    /// Relative URI of this variant's media playlist, e.g. `"720p/index.m3u8"`.
    /// Must be a forward-slash relative path so the master resolves it against
    /// its own directory.
    pub(crate) uri: String,

    /// Optional `CODECS` attribute value (e.g. `"avc1.640028,mp4a.40.2"`).
    /// `None` omits the attribute entirely — the conservative default, since a
    /// precise codec string depends on the encoder's chosen profile/level.
    pub(crate) codecs: Option<String>,
}

/// Renders the master playlist text for `variants`, declaring `version` as
/// the `EXT-X-VERSION` (the hls recipe passes `3` for MPEG-TS segments and
/// `7` for fMP4 segments).
///
/// The variants are sorted ascending by [`MasterVariant::bandwidth`] (the input
/// slice is not mutated). Each variant produces a two-line block:
///
/// ```text
/// #EXT-X-STREAM-INF:BANDWIDTH=<bps>,RESOLUTION=<w>x<h>[,CODECS="<codecs>"]
/// <uri>
/// ```
///
/// An empty `variants` slice still yields a valid (header-only) playlist; the
/// caller is expected to reject empty ladders before reaching this point.
pub(crate) fn generate_master_playlist(variants: &[MasterVariant], version: u32) -> String {
    // Sort references so we never disturb the caller's ordering.
    let mut ordered: Vec<&MasterVariant> = variants.iter().collect();
    ordered.sort_by_key(|v| v.bandwidth);

    let mut out = format!("#EXTM3U\n#EXT-X-VERSION:{version}\n");
    for v in ordered {
        // `write!` into a String is infallible, but honour the Result to keep
        // clippy happy without unwrapping.
        let _ = write!(
            out,
            "#EXT-X-STREAM-INF:BANDWIDTH={},RESOLUTION={}x{}",
            v.bandwidth, v.width, v.height
        );
        if let Some(codecs) = &v.codecs {
            let _ = write!(out, ",CODECS=\"{codecs}\"");
        }
        out.push('\n');
        out.push_str(&v.uri);
        out.push('\n');
    }
    out
}

#[cfg(test)]
mod tests {
    use super::*;

    fn variant(bandwidth: u64, width: u32, height: u32, uri: &str) -> MasterVariant {
        MasterVariant {
            bandwidth,
            width,
            height,
            uri: uri.to_string(),
            codecs: None,
        }
    }

    #[test]
    fn header_carries_the_requested_version() {
        let text = generate_master_playlist(&[variant(800_000, 640, 360, "360p/index.m3u8")], 3);
        assert!(text.starts_with("#EXTM3U\n#EXT-X-VERSION:3\n"));

        // fMP4 ladders pass 7; the version is written verbatim.
        let text = generate_master_playlist(&[variant(800_000, 640, 360, "360p/index.m3u8")], 7);
        assert!(text.starts_with("#EXTM3U\n#EXT-X-VERSION:7\n"));
    }

    #[test]
    fn stream_inf_has_bandwidth_and_resolution_then_uri() {
        let text = generate_master_playlist(&[variant(1_500_000, 1280, 720, "720p/index.m3u8")], 3);
        assert!(text.contains("#EXT-X-STREAM-INF:BANDWIDTH=1500000,RESOLUTION=1280x720\n"));
        // The child URI is on its own line directly after the STREAM-INF tag.
        assert!(text.contains("RESOLUTION=1280x720\n720p/index.m3u8\n"));
        // CODECS omitted by default.
        assert!(!text.contains("CODECS"));
    }

    #[test]
    fn variants_sorted_ascending_by_bandwidth() {
        // Deliberately supplied high -> low; output must be low -> high.
        let variants = vec![
            variant(5_000_000, 1920, 1080, "1080p/index.m3u8"),
            variant(800_000, 640, 360, "360p/index.m3u8"),
            variant(1_500_000, 1280, 720, "720p/index.m3u8"),
        ];
        let text = generate_master_playlist(&variants, 3);

        let pos_360 = text.find("360p/index.m3u8").unwrap();
        let pos_720 = text.find("720p/index.m3u8").unwrap();
        let pos_1080 = text.find("1080p/index.m3u8").unwrap();
        assert!(pos_360 < pos_720, "360p must precede 720p");
        assert!(pos_720 < pos_1080, "720p must precede 1080p");

        // First fallback entry is the lowest bandwidth.
        let first_inf = text.find("#EXT-X-STREAM-INF").unwrap();
        assert!(text[first_inf..].starts_with("#EXT-X-STREAM-INF:BANDWIDTH=800000"));
    }

    #[test]
    fn codecs_attribute_is_quoted_when_present() {
        let mut v = variant(1_500_000, 1280, 720, "720p/index.m3u8");
        v.codecs = Some("avc1.640028,mp4a.40.2".to_string());
        let text = generate_master_playlist(&[v], 3);
        assert!(text.contains(",CODECS=\"avc1.640028,mp4a.40.2\"\n"));
    }

    #[test]
    fn empty_ladder_yields_header_only() {
        let text = generate_master_playlist(&[], 3);
        assert_eq!(text, "#EXTM3U\n#EXT-X-VERSION:3\n");
    }
}
