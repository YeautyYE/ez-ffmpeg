//! URL validation, redaction, and the sanitized filename handed to FFmpeg.

use crate::http_input::error::HttpInputError;
use reqwest::Url;

const MAX_URL_BYTES: usize = 8 * 1024;

/// Parse and accept only `http` / `https` without userinfo.
pub(crate) fn parse_input_url(raw: &str) -> Result<Url, HttpInputError> {
    if raw.len() > MAX_URL_BYTES {
        return Err(HttpInputError::InvalidUrl {
            reason: "URL exceeds 8 KiB",
        });
    }
    let url = Url::parse(raw).map_err(|_| HttpInputError::InvalidUrl {
        reason: "URL did not parse",
    })?;
    if url.scheme() != "http" && url.scheme() != "https" {
        return Err(HttpInputError::InvalidUrl {
            reason: "only http and https URLs are supported",
        });
    }
    if !url.username().is_empty() || url.password().is_some() {
        return Err(HttpInputError::UserinfoForbidden);
    }
    if url.host_str().is_none() {
        return Err(HttpInputError::InvalidUrl {
            reason: "URL is missing a host",
        });
    }
    Ok(url)
}

/// Drop userinfo, query, and fragment for logs and error context.
pub(crate) fn redact_url(url: &Url) -> String {
    let mut redacted = url.clone();
    let _ = redacted.set_username("");
    let _ = redacted.set_password(None);
    redacted.set_query(None);
    redacted.set_fragment(None);
    redacted.to_string()
}

/// Filename passed to `avformat_open_input`: real host/path/query never leave Rust.
///
/// Keeps the scheme and a safe basename+extension so FFmpeg can probe by
/// extension. Host is the fixed `.invalid` name from RFC 2606.
pub(crate) fn sanitized_display_url(url: &Url) -> String {
    let path = url.path();
    let file = path
        .rsplit('/')
        .find(|s| !s.is_empty())
        .unwrap_or("resource");
    let (stem, ext) = match file.rsplit_once('.') {
        Some((stem, ext)) if !stem.is_empty() && ext.chars().all(|c| c.is_ascii_alphanumeric()) => {
            (stem, Some(ext))
        }
        _ => (file, None),
    };
    let safe_stem: String = stem
        .chars()
        .map(|c| {
            if c.is_ascii_alphanumeric() || c == '-' || c == '_' {
                c
            } else {
                '_'
            }
        })
        .collect();
    let stem = if safe_stem.is_empty() {
        "resource"
    } else {
        safe_stem.as_str()
    };
    match ext {
        Some(ext) => format!("{}://http-input.invalid/{stem}.{ext}", url.scheme()),
        None => format!("{}://http-input.invalid/{stem}", url.scheme()),
    }
}

pub(crate) fn same_origin(a: &Url, b: &Url) -> bool {
    a.scheme() == b.scheme()
        && a.host() == b.host()
        && a.port_or_known_default() == b.port_or_known_default()
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn rejects_userinfo() {
        let err = parse_input_url("https://user:pass@example.com/v.mp4").unwrap_err();
        assert!(matches!(err, HttpInputError::UserinfoForbidden));
    }

    #[test]
    fn rejects_non_http_scheme() {
        let err = parse_input_url("file:///tmp/v.mp4").unwrap_err();
        assert!(matches!(err, HttpInputError::InvalidUrl { .. }));
    }

    #[test]
    fn accepts_http_and_https() {
        parse_input_url("https://media.example/video.mp4").unwrap();
        parse_input_url("http://127.0.0.1:8080/live.ts").unwrap();
    }

    #[test]
    fn redacts_query_and_userinfo() {
        let url = Url::parse("https://cdn.example/signed/video.mp4?token=secret#frag").unwrap();
        let redacted = redact_url(&url);
        assert!(!redacted.contains("token"), "{redacted}");
        assert!(!redacted.contains("secret"), "{redacted}");
        assert!(!redacted.contains("frag"), "{redacted}");
        assert!(redacted.starts_with("https://cdn.example/signed/video.mp4"));
    }

    #[test]
    fn sanitized_display_keeps_extension_drops_host() {
        let url = Url::parse("https://cdn.example.com/signed/video.mp4?token=secret").unwrap();
        let display = sanitized_display_url(&url);
        assert_eq!(display, "https://http-input.invalid/video.mp4");
        assert!(!display.contains("cdn.example.com"));
        assert!(!display.contains("token"));
    }
}
