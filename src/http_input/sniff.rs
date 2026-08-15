//! HLS / DASH preflight: URL suffix, Content-Type, and a 4 KiB prefix sniff.

use crate::http_input::error::ManifestKind;

pub(crate) const SNIFF_LIMIT: usize = 4096;

/// Path-suffix check used at builder time (no network).
pub(crate) fn manifest_from_url(url: &str) -> Option<ManifestKind> {
    let path = url.split_once("://").map(|(_, rest)| rest).unwrap_or(url);
    let path = path.split(['?', '#']).next().unwrap_or(path);
    let path = path.rsplit('/').next().unwrap_or(path);
    let lower = path.to_ascii_lowercase();
    if lower.ends_with(".m3u8") || lower.ends_with(".m3u") {
        Some(ManifestKind::Hls)
    } else if lower.ends_with(".mpd") {
        Some(ManifestKind::Dash)
    } else {
        None
    }
}

pub(crate) fn manifest_from_format(format: &str) -> Option<ManifestKind> {
    match format.trim().to_ascii_lowercase().as_str() {
        "hls" | "applehttp" | "m3u8" => Some(ManifestKind::Hls),
        "dash" => Some(ManifestKind::Dash),
        _ => None,
    }
}

pub(crate) fn manifest_from_content_type(content_type: &str) -> Option<ManifestKind> {
    let mime = content_type
        .split(';')
        .next()
        .unwrap_or(content_type)
        .trim()
        .to_ascii_lowercase();
    match mime.as_str() {
        "application/vnd.apple.mpegurl" | "application/x-mpegurl" | "audio/mpegurl" => {
            Some(ManifestKind::Hls)
        }
        "application/dash+xml" => Some(ManifestKind::Dash),
        _ => None,
    }
}

/// Sniff at most 4 KiB. Bytes are not consumed; the caller keeps the prefix.
pub(crate) fn manifest_from_prefix(prefix: &[u8]) -> Option<ManifestKind> {
    let slice = if prefix.len() > SNIFF_LIMIT {
        &prefix[..SNIFF_LIMIT]
    } else {
        prefix
    };
    let mut i = 0;
    if slice.starts_with(&[0xEF, 0xBB, 0xBF]) {
        i = 3;
    }
    while i < slice.len() && slice[i].is_ascii_whitespace() {
        i += 1;
    }
    let rest = &slice[i..];
    if rest.len() >= 7 && rest[..7].eq_ignore_ascii_case(b"#EXTM3U") {
        return Some(ManifestKind::Hls);
    }
    // XML declaration or whitespace already skipped; look for `<MPD`.
    let rest_l = ascii_lower_owned(rest);
    if rest_l.contains("<mpd") {
        return Some(ManifestKind::Dash);
    }
    None
}

fn ascii_lower_owned(bytes: &[u8]) -> String {
    bytes
        .iter()
        .map(|b| b.to_ascii_lowercase() as char)
        .collect()
}

/// Whether more body bytes are required before a negative prefix decision.
///
/// A split `#` / `EXTM3U` signature and a short XML prologue are
/// inconclusive until [`SNIFF_LIMIT`] or a non-manifest first byte.
pub(crate) fn prefix_needs_more(prefix: &[u8]) -> bool {
    if prefix.len() >= SNIFF_LIMIT || manifest_from_prefix(prefix).is_some() {
        return false;
    }
    // Incomplete UTF-8 BOM: wait for the remaining bytes before a negative
    // prefix decision, otherwise a split inside EF BB BF escapes sniffing.
    if matches!(prefix, [0xEF] | [0xEF, 0xBB]) {
        return true;
    }
    let mut i = 0;
    if prefix.starts_with(&[0xEF, 0xBB, 0xBF]) {
        i = 3;
    }
    while i < prefix.len() && prefix[i].is_ascii_whitespace() {
        i += 1;
    }
    let rest = &prefix[i..];
    if rest.is_empty() {
        return true;
    }
    if rest[0] == b'#' {
        return rest.len() < 7;
    }
    rest[0] == b'<'
}

/// Combined preflight used after headers + prefix are available.
pub(crate) fn sniff_manifest(
    url: &str,
    content_type: Option<&str>,
    prefix: &[u8],
) -> Option<ManifestKind> {
    manifest_from_url(url)
        .or_else(|| content_type.and_then(manifest_from_content_type))
        .or_else(|| manifest_from_prefix(prefix))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn url_suffix() {
        assert_eq!(
            manifest_from_url("https://ex/live.m3u8?token=1"),
            Some(ManifestKind::Hls)
        );
        assert_eq!(
            manifest_from_url("https://ex/manifest.mpd"),
            Some(ManifestKind::Dash)
        );
        assert_eq!(manifest_from_url("https://ex/video.mp4"), None);
    }

    #[test]
    fn content_type() {
        assert_eq!(
            manifest_from_content_type("application/vnd.apple.mpegurl; charset=utf-8"),
            Some(ManifestKind::Hls)
        );
        assert_eq!(
            manifest_from_content_type("APPLICATION/X-MPEGURL"),
            Some(ManifestKind::Hls)
        );
        assert_eq!(
            manifest_from_content_type("application/dash+xml"),
            Some(ManifestKind::Dash)
        );
        assert_eq!(manifest_from_content_type("video/mp4"), None);
    }

    #[test]
    fn prefix_hls_with_bom_and_whitespace() {
        let mut body = vec![0xEF, 0xBB, 0xBF];
        body.extend_from_slice(b"\n  #EXTM3U\n#EXTINF:1,\nseg.ts\n");
        assert_eq!(manifest_from_prefix(&body), Some(ManifestKind::Hls));
    }

    #[test]
    fn prefix_dash_after_xml_decl() {
        let body = b"<?xml version=\"1.0\"?>\n<MPD xmlns=\"urn:mpeg:dash:schema:mpd:2011\">";
        assert_eq!(manifest_from_prefix(body), Some(ManifestKind::Dash));
    }

    #[test]
    fn prefix_needs_more_for_split_extm3u() {
        assert!(prefix_needs_more(b"#"));
        assert!(prefix_needs_more(b"#EXT"));
        assert!(prefix_needs_more(b"\n  #"));
        assert!(!prefix_needs_more(b"#EXTM3U"));
        assert!(!prefix_needs_more(b"\0\0\0\x20ftypisom"));
        assert!(prefix_needs_more(b"<?xml"));
        let mut long_xml = vec![b'<'; SNIFF_LIMIT];
        assert!(!prefix_needs_more(&long_xml));
        long_xml.pop();
        assert!(prefix_needs_more(&long_xml));
    }

    #[test]
    fn prefix_needs_more_for_partial_bom() {
        assert!(prefix_needs_more(b"\xEF"));
        assert!(prefix_needs_more(b"\xEF\xBB"));
        assert!(prefix_needs_more(b"\xEF\xBB\xBF"));
        assert!(prefix_needs_more(b"\xEF\xBB\xBF#"));
        assert!(!prefix_needs_more(b"\xEF\xBB\xBF#EXTM3U"));
    }

    #[test]
    fn prefix_false_positive_mp4() {
        // ftyp box is not a manifest.
        let body = b"\0\0\0\x20ftypisom";
        assert_eq!(manifest_from_prefix(body), None);
    }

    #[test]
    fn format_names() {
        assert_eq!(manifest_from_format("hls"), Some(ManifestKind::Hls));
        assert_eq!(manifest_from_format("dash"), Some(ManifestKind::Dash));
        assert_eq!(manifest_from_format("mp4"), None);
    }
}
