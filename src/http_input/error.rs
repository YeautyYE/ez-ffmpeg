//! Typed errors for the experimental `http-input` feature.
//!
//! Sensitive header values and URL userinfo / query / fragment must never
//! appear in these messages.

use std::fmt;

/// Kind of multi-resource manifest that `HttpInput` refuses.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
#[non_exhaustive]
pub enum ManifestKind {
    /// HLS (`application/vnd.apple.mpegurl`, `#EXTM3U`, `.m3u8`).
    Hls,
    /// MPEG-DASH (`application/dash+xml`, `<MPD`, `.mpd`).
    Dash,
}

impl fmt::Display for ManifestKind {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Hls => f.write_str("HLS"),
            Self::Dash => f.write_str("DASH"),
        }
    }
}

/// Failure from building or opening an [`crate::http_input::HttpInput`].
///
/// This type is boxed into [`crate::error::Error::HttpInput`] so the crate
/// `Error` layout stays within 64 bytes.
#[derive(Debug, Clone, thiserror::Error)]
#[non_exhaustive]
pub enum HttpInputError {
    /// The URL is not a usable `http://` or `https://` resource.
    #[error("invalid HTTP input URL: {reason}")]
    InvalidUrl {
        /// Stable English reason (no URL payload).
        reason: &'static str,
    },

    /// Userinfo in the URL is rejected; use the Authorization header API.
    #[error(
        "HTTP input URLs must not contain userinfo; pass credentials with the Authorization header"
    )]
    UserinfoForbidden,

    /// HLS / DASH (or another nested-resource format) is out of v1 scope.
    #[error(
        "Rust HTTP input supports one HTTP resource per input; {kind} manifests \
         open nested resources and are not supported. Use Input::from(url) with an \
         FFmpeg build that includes HTTPS/TLS, or resolve the manifest outside \
         ez-ffmpeg and provide a single media stream"
    )]
    ManifestUnsupported {
        /// Which manifest family was detected.
        kind: ManifestKind,
    },

    /// The demuxer asked to open a second resource.
    #[error(
        "Rust HTTP input rejected a nested resource requested by the demuxer. This \
         input format is not supported by the single-resource HttpInput API"
    )]
    NestedResourceUnsupported,

    /// Server compressed the entity body; Range offsets would not match.
    #[error("HTTP server returned a non-identity Content-Encoding; media input requires identity")]
    UnsupportedContentEncoding,

    /// HTTP status that does not have a more specific variant.
    #[error("HTTP request failed with status {code}")]
    Status {
        /// Status code from the origin.
        code: u16,
    },

    /// HTTP 401.
    #[error("HTTP authentication required")]
    AuthenticationRequired,

    /// HTTP 403.
    #[error("HTTP access denied")]
    AccessDenied,

    /// HTTP 407.
    #[error("HTTP proxy authentication required")]
    ProxyAuthenticationRequired,

    /// HTTP 404 / 410.
    #[error("HTTP resource not found")]
    NotFound,

    /// Connect / response-header deadline.
    #[error("HTTP request timed out")]
    Timeout,

    /// No body bytes arrived within the idle window.
    #[error("HTTP body idle timeout")]
    ReadIdleTimeout,

    /// rustls rejected the certificate or hostname.
    #[error("TLS certificate or hostname verification failed")]
    TlsVerification,

    /// Range request could not be satisfied.
    #[error("HTTP range is not satisfiable")]
    RangeNotSatisfiable,

    /// Non-zero Range was ignored (server returned 200 for a mid-resource seek).
    #[error("The HTTP server does not support byte-range requests required to seek this input")]
    RangeIgnored,

    /// ETag / Last-Modified / size changed across Range or reconnect.
    #[error("HTTP resource changed between requests")]
    ResourceChanged,

    /// Redirect loop or hop limit.
    #[error("too many HTTP redirects")]
    TooManyRedirects,

    /// HTTPS followed by an `http://` Location.
    #[error("refusing HTTPS to HTTP redirect")]
    HttpsDowngrade,

    /// Caller tried to set a hop-by-hop or transport header the crate owns.
    #[error("HTTP header '{name}' is reserved by HttpInput")]
    HeaderReserved {
        /// Header name the caller attempted to set.
        name: String,
    },

    /// Header name or value failed HTTP validation.
    #[error("invalid HTTP header '{name}'")]
    HeaderInvalid {
        /// Header name that failed validation.
        name: String,
    },

    /// System / custom trust store produced no usable anchors.
    #[error("no usable TLS trust anchors; add a custom CA or install system roots")]
    NoTrustAnchors,

    /// PEM did not contain a certificate.
    #[error("invalid TLS certificate PEM")]
    InvalidCertificate,

    /// PEM identity was missing a cert chain or an unencrypted private key.
    #[error("invalid TLS client identity PEM (need cert chain and unencrypted private key)")]
    IdentityInvalid,

    /// Proxy URL or credentials were unusable.
    #[error("invalid HTTP proxy configuration")]
    InvalidProxy,

    /// Timeout builder rejected a zero duration.
    #[error("HTTP timeouts must be greater than zero")]
    InvalidTimeout,

    /// Transport / DNS / connection failure (message has no secrets).
    #[error("HTTP transport error: {message}")]
    Transport {
        /// Redacted transport description.
        message: String,
    },

    /// Scheduler stop / abort observed on the AVIO poll tick.
    #[error("HTTP input interrupted")]
    Interrupted,

    /// Socket EOF arrived before the declared Content-Length or Content-Range total.
    #[error("HTTP response body was truncated before the declared length")]
    TruncatedBody,
}

impl HttpInputError {
    pub(crate) fn to_errno(&self) -> i32 {
        use ffmpeg_sys_next::{
            AVERROR, AVERROR_EXIT, EACCES, ECONNRESET, EHOSTUNREACH, EINVAL, EIO, ELOOP, ENOENT,
            ENOSYS, EPERM, ETIMEDOUT,
        };
        match self {
            Self::Interrupted => AVERROR_EXIT,
            Self::NotFound => AVERROR(ENOENT),
            Self::Timeout | Self::ReadIdleTimeout => AVERROR(ETIMEDOUT),
            Self::AuthenticationRequired
            | Self::AccessDenied
            | Self::ProxyAuthenticationRequired
            | Self::TlsVerification => AVERROR(EACCES),
            Self::NestedResourceUnsupported => AVERROR(EPERM),
            Self::TooManyRedirects => AVERROR(ELOOP),
            Self::InvalidUrl { .. }
            | Self::UserinfoForbidden
            | Self::HeaderReserved { .. }
            | Self::HeaderInvalid { .. }
            | Self::RangeNotSatisfiable
            | Self::Status { code: 400 } => AVERROR(EINVAL),
            Self::RangeIgnored => AVERROR(ENOSYS),
            Self::Transport { message } if message.contains("reset") => AVERROR(ECONNRESET),
            Self::Transport { message }
                if message.contains("dns") || message.contains("resolve") =>
            {
                AVERROR(EHOSTUNREACH)
            }
            _ => AVERROR(EIO),
        }
    }
}
