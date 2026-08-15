//! Shared `HttpClient`: rustls roots, reqwest builder, per-client runtime.

use crate::http_input::config::{HttpTimeouts, ProxyPolicy, RootPolicy};
use crate::http_input::error::HttpInputError;
use crate::http_input::runtime::RuntimeHandle;
use crate::http_input::HttpInputBuilder;
use std::fmt;
use std::sync::{Arc, Mutex};

/// Reusable HTTP stack: TLS config, proxy, and a dedicated current-thread Tokio runtime.
///
/// Several [`HttpInput`](crate::http_input::HttpInput)s created from the same
/// client share the connection pool
/// and the runtime thread. The last drop shuts the runtime down.
#[derive(Clone)]
pub struct HttpClient {
    pub(crate) inner: Arc<HttpClientInner>,
}

pub(crate) struct HttpClientInner {
    pub(crate) client: reqwest::Client,
    pub(crate) timeouts: HttpTimeouts,
    pub(crate) user_agent: Option<String>,
    pub(crate) redirect_limit: u32,
    runtime: Mutex<Option<RuntimeHandle>>,
}

impl fmt::Debug for HttpClient {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("HttpClient")
            .field("user_agent", &self.inner.user_agent)
            .finish_non_exhaustive()
    }
}

impl HttpClient {
    /// Start a client builder (system roots, environment proxy).
    pub fn builder() -> HttpClientBuilder {
        HttpClientBuilder::default()
    }

    /// Open an input that shares this client's pool and runtime.
    ///
    /// The input starts with this client's response-header and body-idle
    /// timeouts. [`HttpInputBuilder::timeouts`] can override those two per
    /// input. Connect timeout is fixed when this client is built
    /// ([`HttpClientBuilder::timeouts`]); reqwest applies it at client
    /// construction, not per request.
    pub fn input(&self, url: impl Into<String>) -> HttpInputBuilder {
        HttpInputBuilder::new(url.into())
            .client(self.clone())
            .timeouts(self.inner.timeouts.clone())
    }

    pub(crate) fn ensure_runtime(&self) -> Result<(), HttpInputError> {
        let mut guard = self.inner.runtime.lock().unwrap_or_else(|e| e.into_inner());
        if guard.is_none() {
            *guard = Some(RuntimeHandle::start()?);
        }
        Ok(())
    }

    pub(crate) fn runtime(&self) -> Result<RuntimeHandle, HttpInputError> {
        self.ensure_runtime()?;
        let guard = self.inner.runtime.lock().unwrap_or_else(|e| e.into_inner());
        guard
            .as_ref()
            .cloned()
            .ok_or_else(|| HttpInputError::Transport {
                message: "http runtime failed to start".into(),
            })
    }
}

/// Builder for [`HttpClient`]. Certificate and identity bytes are parsed at
/// [`build`](Self::build); failures do not wait for the first request.
pub struct HttpClientBuilder {
    root_policy: RootPolicy,
    extra_roots_pem: Vec<Vec<u8>>,
    identity_pem: Option<Vec<u8>>,
    user_agent: Option<String>,
    disable_ua: bool,
    proxy: ProxyPolicy,
    timeouts: HttpTimeouts,
    redirect_limit: u32,
}

impl fmt::Debug for HttpClientBuilder {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("HttpClientBuilder")
            .field("root_policy", &self.root_policy)
            .field("extra_roots", &self.extra_roots_pem.len())
            .field("has_identity", &self.identity_pem.is_some())
            .field("user_agent", &self.user_agent)
            .field("proxy", &self.proxy)
            .finish_non_exhaustive()
    }
}

impl Default for HttpClientBuilder {
    fn default() -> Self {
        Self {
            root_policy: RootPolicy::System,
            extra_roots_pem: Vec::new(),
            identity_pem: None,
            user_agent: None,
            disable_ua: false,
            proxy: ProxyPolicy::Environment,
            timeouts: HttpTimeouts::default(),
            redirect_limit: 10,
        }
    }
}

impl HttpClientBuilder {
    /// System store (default) or custom PEMs only.
    pub fn root_policy(mut self, policy: RootPolicy) -> Self {
        self.root_policy = policy;
        self
    }

    /// Append a PEM certificate to the trust store. Parsed immediately.
    pub fn add_root_certificate_pem(
        mut self,
        pem: impl AsRef<[u8]>,
    ) -> Result<Self, HttpInputError> {
        let pem = pem.as_ref();
        if !pem
            .windows(27)
            .any(|window| window.eq_ignore_ascii_case(b"-----BEGIN CERTIFICATE-----"))
        {
            return Err(HttpInputError::InvalidCertificate);
        }
        reqwest::Certificate::from_pem(pem).map_err(|_| HttpInputError::InvalidCertificate)?;
        self.extra_roots_pem.push(pem.to_vec());
        Ok(self)
    }

    /// PEM client identity (cert chain + unencrypted private key) for mTLS.
    pub fn client_identity_pem(mut self, pem: impl Into<Vec<u8>>) -> Self {
        self.identity_pem = Some(pem.into());
        self
    }

    /// Override the default `ez-ffmpeg/<version>` User-Agent.
    pub fn user_agent(mut self, ua: impl Into<String>) -> Self {
        self.user_agent = Some(ua.into());
        self.disable_ua = false;
        self
    }

    /// Send no User-Agent header.
    pub fn disable_user_agent(mut self) -> Self {
        self.disable_ua = true;
        self.user_agent = None;
        self
    }

    /// Proxy policy. Default is environment variables snapshotted at build.
    pub fn proxy(mut self, policy: ProxyPolicy) -> Self {
        self.proxy = policy;
        self
    }

    /// Default timeouts inherited by [`HttpClient::input`] unless the input
    /// overrides them. `response_headers` and `read_idle` can be overridden
    /// per input. `connect` is applied once to the reqwest client at
    /// [`build`](Self::build) and cannot be changed per input afterwards.
    pub fn timeouts(mut self, timeouts: HttpTimeouts) -> Self {
        self.timeouts = timeouts;
        self
    }

    /// Maximum redirect hops (default 10). `0` follows no redirect: the
    /// original request is sent once and a 3xx response is an error.
    pub fn redirect_limit(mut self, limit: u32) -> Self {
        self.redirect_limit = limit;
        self
    }

    /// Build the reqwest client and load trust anchors. No network I/O.
    pub fn build(self) -> Result<HttpClient, HttpInputError> {
        self.timeouts.validate()?;
        let mut roots: Vec<reqwest::Certificate> = Vec::new();
        if self.root_policy == RootPolicy::System {
            let loaded = rustls_native_certs::load_native_certs();
            for cert in loaded.certs {
                if let Ok(parsed) = reqwest::Certificate::from_der(&cert) {
                    roots.push(parsed);
                }
            }
        }
        for pem in &self.extra_roots_pem {
            roots.push(
                reqwest::Certificate::from_pem(pem)
                    .map_err(|_| HttpInputError::InvalidCertificate)?,
            );
        }
        if roots.is_empty() {
            return Err(HttpInputError::NoTrustAnchors);
        }

        let mut builder = reqwest::Client::builder()
            .use_rustls_tls()
            .tls_built_in_root_certs(false)
            .http1_only()
            .redirect(reqwest::redirect::Policy::none())
            .connect_timeout(self.timeouts.connect)
            .pool_max_idle_per_host(4)
            .https_only(false);

        for cert in roots {
            builder = builder.add_root_certificate(cert);
        }

        if let Some(mut pem) = self.identity_pem {
            let identity = reqwest::Identity::from_pem(&pem).map_err(|_| {
                pem.fill(0);
                HttpInputError::IdentityInvalid
            })?;
            pem.fill(0);
            builder = builder.identity(identity);
        }

        builder = match self.proxy {
            ProxyPolicy::Environment => builder,
            ProxyPolicy::Disabled => builder.no_proxy(),
            ProxyPolicy::Explicit(cfg) => {
                let mut proxy =
                    reqwest::Proxy::all(cfg.url()).map_err(|_| HttpInputError::InvalidProxy)?;
                if let (Some(user), Some(pass)) = (cfg.username_ref(), cfg.password_ref()) {
                    proxy = proxy.basic_auth(user, pass);
                }
                builder.proxy(proxy)
            }
        };

        let client = builder.build().map_err(|e| HttpInputError::Transport {
            message: sanitize_transport(&e.to_string()),
        })?;

        let user_agent = if self.disable_ua {
            None
        } else {
            Some(self.user_agent.unwrap_or_else(default_user_agent))
        };

        Ok(HttpClient {
            inner: Arc::new(HttpClientInner {
                client,
                timeouts: self.timeouts,
                user_agent,
                redirect_limit: self.redirect_limit,
                runtime: Mutex::new(None),
            }),
        })
    }
}

pub(crate) fn default_user_agent() -> String {
    format!("ez-ffmpeg/{}", env!("CARGO_PKG_VERSION"))
}

pub(crate) fn sanitize_transport(msg: &str) -> String {
    // reqwest includes the full URL in many errors. Replace URL-like
    // tokens so scheme/host/path/userinfo never leave the crate.
    let mut out = String::new();
    let mut rest = msg;
    while let Some(idx) = rest.find("://") {
        let prefix = &rest[..idx];
        let scheme_start = prefix
            .rfind(|c: char| !(c.is_ascii_alphabetic() || c == '+' || c == '.' || c == '-'))
            .map(|i| i + 1)
            .unwrap_or(0);
        out.push_str(&prefix[..scheme_start]);
        out.push_str("[url]");
        let after = &rest[idx + 3..];
        let end = after
            .find(|c: char| c.is_whitespace() || matches!(c, '\'' | '"' | ')' | ']' | ',' | ';'))
            .unwrap_or(after.len());
        rest = &after[end..];
        if out.len() > 240 {
            break;
        }
    }
    for ch in rest.chars() {
        if ch == '?' || ch == '#' {
            break;
        }
        out.push(ch);
        if out.len() > 240 {
            break;
        }
    }
    out
}

/// Shared defaults used when [`HttpInput::builder`] creates an exclusive client.
pub(crate) fn exclusive_client(
    timeouts: HttpTimeouts,
    user_agent: Option<String>,
    disable_ua: bool,
) -> Result<HttpClient, HttpInputError> {
    let mut b = HttpClient::builder().timeouts(timeouts);
    if disable_ua {
        b = b.disable_user_agent();
    } else if let Some(ua) = user_agent {
        b = b.user_agent(ua);
    }
    b.build()
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn redirect_limit_zero_survives_build() {
        let client = HttpClient::builder().redirect_limit(0).build().unwrap();
        assert_eq!(
            client.inner.redirect_limit, 0,
            "an explicit zero-redirect policy must not be coerced to 1"
        );
    }

    #[test]
    fn custom_only_without_roots_fails() {
        let err = HttpClient::builder()
            .root_policy(RootPolicy::CustomOnly)
            .build()
            .unwrap_err();
        assert!(matches!(err, HttpInputError::NoTrustAnchors));
    }

    #[test]
    fn invalid_pem_is_rejected() {
        let err = HttpClient::builder()
            .add_root_certificate_pem("not-a-cert")
            .unwrap_err();
        assert!(matches!(err, HttpInputError::InvalidCertificate));
    }

    #[test]
    fn default_ua_uses_crate_version() {
        let ua = default_user_agent();
        assert!(ua.starts_with("ez-ffmpeg/"), "{ua}");
        assert!(!ua.contains("reqwest"), "{ua}");
    }

    #[test]
    fn sanitize_transport_redacts_urls() {
        let raw = "error sending request for url (https://user:hunter2@cdn.example/signed/video.mp4?token=secret): connection reset";
        let clean = sanitize_transport(raw);
        assert!(!clean.contains("cdn.example"), "{clean}");
        assert!(!clean.contains("hunter2"), "{clean}");
        assert!(!clean.contains("token"), "{clean}");
        assert!(!clean.contains("secret"), "{clean}");
        assert!(clean.contains("[url]"), "{clean}");
        assert!(clean.contains("connection reset"), "{clean}");
    }
}
