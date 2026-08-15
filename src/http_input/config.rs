//! Timeouts, reconnect policy, proxy, and header validation.

use crate::http_input::error::HttpInputError;
use reqwest::header::{HeaderName, HeaderValue};
use std::fmt;
use std::time::Duration;

/// Default connect timeout (design §8.7).
pub const DEFAULT_CONNECT_TIMEOUT: Duration = Duration::from_secs(10);
/// Default response-header timeout.
pub const DEFAULT_HEADER_TIMEOUT: Duration = Duration::from_secs(15);
/// Default body idle timeout; `None` disables it.
pub const DEFAULT_READ_IDLE_TIMEOUT: Duration = Duration::from_secs(30);
/// AVIO / runtime stop poll tick. Not a network idle timeout.
pub const STOP_POLL_TICK: Duration = Duration::from_millis(25);

const MAX_HEADER_NAME_BYTES: usize = 8 * 1024;
const MAX_HEADER_VALUE_BYTES: usize = 8 * 1024;
const MAX_HEADER_TOTAL_BYTES: usize = 32 * 1024;

/// Connect / header / body-idle deadlines for one input.
#[derive(Debug, Clone)]
#[non_exhaustive]
pub struct HttpTimeouts {
    /// TCP + TLS connect budget. Applied at
    /// [`crate::http_input::HttpClientBuilder::build`] to the reqwest client;
    /// [`crate::http_input::HttpInputBuilder::timeouts`] cannot change it.
    pub connect: Duration,
    /// Budget from request start until response headers.
    pub response_headers: Duration,
    /// Reset after every body chunk. `None` means no idle limit.
    pub read_idle: Option<Duration>,
}

impl Default for HttpTimeouts {
    fn default() -> Self {
        Self {
            connect: DEFAULT_CONNECT_TIMEOUT,
            response_headers: DEFAULT_HEADER_TIMEOUT,
            read_idle: Some(DEFAULT_READ_IDLE_TIMEOUT),
        }
    }
}

impl HttpTimeouts {
    pub(crate) fn validate(&self) -> Result<(), HttpInputError> {
        if self.connect.is_zero() || self.response_headers.is_zero() {
            return Err(HttpInputError::InvalidTimeout);
        }
        if let Some(idle) = self.read_idle {
            if idle.is_zero() {
                return Err(HttpInputError::InvalidTimeout);
            }
        }
        Ok(())
    }
}

/// Application-level reconnect. Default matches FFmpeg `reconnect=0`.
#[derive(Debug, Clone)]
#[non_exhaustive]
pub struct ReconnectPolicy {
    /// When false, no application-level retry after a response has started.
    pub enabled: bool,
    /// Allow reconnect on unknown-length non-seekable / live streams.
    /// Seekable resources never use this path (Range resume or
    /// `TruncatedBody`). A declared `Content-Length` without
    /// `Accept-Ranges` is treated as VOD and also refuses a restart from
    /// byte 0 — that would duplicate prefix bytes already given to FFmpeg.
    pub reconnect_streamed: bool,
    /// Reconnect after a clean EOF. For unknown-length live streams this
    /// re-GETs the resource; for known-length VOD it is a no-op (the body
    /// already ended).
    pub reconnect_at_eof: bool,
    /// Maximum application retries.
    pub max_retries: u32,
    /// Cap on a single backoff delay.
    pub max_delay: Duration,
    /// Cap on summed backoff. Zero means no total cap.
    pub max_total_delay: Duration,
    /// Honor a valid `Retry-After` when retrying.
    pub respect_retry_after: bool,
    /// HTTP statuses eligible for retry after `enabled`.
    pub retry_http_statuses: Vec<u16>,
    /// Seekable reconnect / Range continuation requires ETag or Last-Modified.
    /// Default is true; set false only when the caller accepts an unverified splice.
    pub require_validator: bool,
}

impl Default for ReconnectPolicy {
    fn default() -> Self {
        Self {
            enabled: false,
            reconnect_streamed: false,
            reconnect_at_eof: false,
            max_retries: 0,
            max_delay: Duration::from_secs(30),
            max_total_delay: Duration::ZERO,
            respect_retry_after: true,
            retry_http_statuses: vec![408, 429, 500, 502, 503, 504],
            require_validator: true,
        }
    }
}

impl ReconnectPolicy {
    /// Conservative seekable-resource retry (still opt-in via `enabled`).
    pub fn seekable_default() -> Self {
        Self {
            enabled: true,
            reconnect_streamed: false,
            reconnect_at_eof: false,
            max_retries: 5,
            max_delay: Duration::from_secs(30),
            max_total_delay: Duration::from_secs(60),
            respect_retry_after: true,
            retry_http_statuses: vec![408, 429, 500, 502, 503, 504],
            require_validator: true,
        }
    }

    /// Live / unknown-length streamed retry (restart from the live edge).
    ///
    /// Sets `reconnect_streamed` together with `enabled` and a non-zero
    /// `max_retries` (5, matching [`seekable_default`](Self::seekable_default)).
    /// Setting `reconnect_streamed = true` on a hand-built policy without also
    /// raising `max_retries` above the default 0 used to be a silent no-op:
    /// every retry check consults the retry budget first. `reconnect_at_eof`
    /// is on because live unknown-length streams typically want to re-GET
    /// after a clean EOF.
    pub fn streamed_default() -> Self {
        Self {
            enabled: true,
            reconnect_streamed: true,
            reconnect_at_eof: true,
            max_retries: 5,
            max_delay: Duration::from_secs(30),
            max_total_delay: Duration::from_secs(60),
            respect_retry_after: true,
            retry_http_statuses: vec![408, 429, 500, 502, 503, 504],
            require_validator: true,
        }
    }
}

/// Where TLS trust anchors come from.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
#[non_exhaustive]
pub enum RootPolicy {
    /// System store plus any extra PEMs. Default.
    System,
    /// Only extra PEMs; system store is not consulted.
    CustomOnly,
}

/// Proxy selection. Credentials are never printed by [`Debug`].
#[derive(Clone, Default)]
#[non_exhaustive]
pub enum ProxyPolicy {
    /// Snapshot `HTTP_PROXY` / `HTTPS_PROXY` / `ALL_PROXY` / `NO_PROXY` at client build.
    #[default]
    Environment,
    /// Direct connect.
    Disabled,
    /// Explicit proxy URL (and optional credentials).
    Explicit(ProxyConfig),
}

impl fmt::Debug for ProxyPolicy {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Environment => f.write_str("Environment"),
            Self::Disabled => f.write_str("Disabled"),
            Self::Explicit(cfg) => f.debug_tuple("Explicit").field(cfg).finish(),
        }
    }
}

/// Explicit proxy endpoint. Password is redacted in [`Debug`].
#[derive(Clone)]
pub struct ProxyConfig {
    url: String,
    username: Option<String>,
    password: Option<String>,
}

impl fmt::Debug for ProxyConfig {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("ProxyConfig")
            .field("url", &redact_proxy_url(&self.url))
            .field("username", &self.username.as_ref().map(|_| "***"))
            .field("password", &self.password.as_ref().map(|_| "***"))
            .finish()
    }
}

fn redact_proxy_url(raw: &str) -> String {
    match reqwest::Url::parse(raw) {
        Ok(mut url) => {
            let _ = url.set_username("");
            let _ = url.set_password(None);
            url.set_query(None);
            url.set_fragment(None);
            url.to_string()
        }
        Err(_) => "[invalid-proxy-url]".into(),
    }
}

impl ProxyConfig {
    /// `url` is the proxy origin (`http://127.0.0.1:8080`). Credentials are separate.
    pub fn new(url: impl Into<String>) -> Self {
        Self {
            url: url.into(),
            username: None,
            password: None,
        }
    }

    /// Basic-auth username for the proxy. Not sent to the origin.
    pub fn username(mut self, username: impl Into<String>) -> Self {
        self.username = Some(username.into());
        self
    }

    /// Basic-auth password for the proxy. Not sent to the origin.
    pub fn password(mut self, password: impl Into<String>) -> Self {
        self.password = Some(password.into());
        self
    }

    pub(crate) fn url(&self) -> &str {
        &self.url
    }

    pub(crate) fn username_ref(&self) -> Option<&str> {
        self.username.as_deref()
    }

    pub(crate) fn password_ref(&self) -> Option<&str> {
        self.password.as_deref()
    }
}

const RESERVED_HEADERS: &[&str] = &[
    "host",
    "content-length",
    "content-range",
    "transfer-encoding",
    "connection",
    "range",
    "if-range",
    "accept-encoding",
    "proxy-authorization",
];

pub(crate) fn is_reserved_header(name: &str) -> bool {
    RESERVED_HEADERS
        .iter()
        .any(|reserved| name.eq_ignore_ascii_case(reserved))
}

pub(crate) fn validate_header(name: &str, value: &str) -> Result<(), HttpInputError> {
    if name.len() > MAX_HEADER_NAME_BYTES || value.len() > MAX_HEADER_VALUE_BYTES {
        return Err(HttpInputError::HeaderInvalid {
            name: name.to_string(),
        });
    }
    if is_reserved_header(name) {
        return Err(HttpInputError::HeaderReserved {
            name: name.to_string(),
        });
    }
    if HeaderName::from_bytes(name.as_bytes()).is_err()
        || HeaderValue::from_bytes(value.as_bytes()).is_err()
    {
        return Err(HttpInputError::HeaderInvalid {
            name: name.to_string(),
        });
    }
    Ok(())
}

pub(crate) fn validate_header_set(headers: &[(String, String)]) -> Result<(), HttpInputError> {
    let mut total = 0usize;
    for (name, value) in headers {
        validate_header(name, value)?;
        total = total.saturating_add(name.len()).saturating_add(value.len());
        if total > MAX_HEADER_TOTAL_BYTES {
            return Err(HttpInputError::HeaderInvalid { name: name.clone() });
        }
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn reserved_headers_are_rejected() {
        for name in ["Accept-Encoding", "RANGE", "host", "Proxy-Authorization"] {
            let err = validate_header(name, "x").unwrap_err();
            assert!(
                matches!(err, HttpInputError::HeaderReserved { .. }),
                "{err}"
            );
        }
    }

    #[test]
    fn authorization_is_allowed() {
        validate_header("Authorization", "Bearer abc").unwrap();
        validate_header("Cookie", "a=b").unwrap();
    }

    #[test]
    fn zero_timeout_is_rejected() {
        let t = HttpTimeouts {
            connect: Duration::ZERO,
            ..Default::default()
        };
        assert!(matches!(t.validate(), Err(HttpInputError::InvalidTimeout)));
    }

    #[test]
    fn proxy_debug_redacts_password() {
        let cfg = ProxyConfig::new("http://127.0.0.1:8080")
            .username("u")
            .password("super-secret");
        let rendered = format!("{cfg:?}");
        assert!(!rendered.contains("super-secret"), "{rendered}");
        assert!(rendered.contains("***"), "{rendered}");
    }

    #[test]
    fn proxy_debug_redacts_userinfo() {
        let cfg = ProxyConfig::new("http://user:hunter2@127.0.0.1:8080").username("u");
        let rendered = format!("{cfg:?}");
        assert!(!rendered.contains("hunter2"), "{rendered}");
        assert!(!rendered.contains("user:"), "{rendered}");
        assert!(!rendered.contains("username: Some(\"u\")"), "{rendered}");
        assert!(rendered.contains("127.0.0.1:8080"), "{rendered}");
    }

    #[test]
    fn require_validator_defaults_true() {
        assert!(ReconnectPolicy::default().require_validator);
        assert!(ReconnectPolicy::seekable_default().require_validator);
        assert!(!ReconnectPolicy::default().enabled);
        assert!(ReconnectPolicy::seekable_default().enabled);
    }

    #[test]
    fn streamed_default_has_a_usable_retry_budget() {
        let policy = ReconnectPolicy::streamed_default();
        assert!(policy.enabled);
        assert!(policy.reconnect_streamed);
        assert!(policy.reconnect_at_eof);
        assert!(
            policy.max_retries > 0,
            "reconnect_streamed without max_retries is a silent no-op"
        );
        assert!(policy.require_validator);
    }
}
