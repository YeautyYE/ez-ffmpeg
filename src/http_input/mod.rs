//! Experimental single-resource HTTP(S) input via rustls.
//!
//! This module is compiled only with the default-off `http-input` feature.
//! It never hijacks [`crate::Input::from`] URL routing: FFmpeg still handles
//! `https://` URLs when the linked build has an HTTPS protocol. Only the
//! explicit [`HttpInput`](crate::http_input::HttpInput) /
//! [`HttpClient`](crate::http_input::HttpClient) API uses the Rust HTTP stack.
//!
//! # Scope
//!
//! One HTTP(S) response body is one media resource (MP4, MPEG-TS, FLV,
//! Matroska, or a single live connection). HLS and DASH are **not**
//! supported: they are rejected by URL / Content-Type / prefix sniff and by
//! a deny-all `io_open` callback. Full HLS needs a demuxer-specific adapter
//! (FFmpeg 8.1 `hls.c` calls `avio_find_protocol_name` before `io_open`).
//!
//! # Experimental
//!
//! The types in this module are experimental. Errors, builder fields, and
//! reconnect policy may still change.

pub(crate) mod client;
pub(crate) mod config;
pub(crate) mod error;
pub(crate) mod runtime;
pub(crate) mod sniff;
pub(crate) mod stream;
pub(crate) mod urlutil;

pub use client::{HttpClient, HttpClientBuilder};
pub use config::{
    HttpTimeouts, ProxyConfig, ProxyPolicy, ReconnectPolicy, RootPolicy, STOP_POLL_TICK,
};
pub use error::{HttpInputError, ManifestKind};

use crate::core::context::http_avio;
use crate::core::context::input::Input;
use crate::core::context::InterruptState;
use crate::http_input::client::exclusive_client;
use crate::http_input::stream::{wait_reply, RequestSpec, StreamJob};
use crate::http_input::urlutil::parse_input_url;
use crossbeam_channel::bounded;
use std::ffi::CString;
use std::fmt;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, Mutex};
use std::time::Duration;

const CHANNEL_CAP: usize = 8;

/// Custom-IO read callback handed to the shared AVIO bridge: fills the
/// buffer and returns the byte count, `0` at EOF, or a negative `AVERROR`.
pub(crate) type ReadCallback = Box<dyn FnMut(&mut [u8]) -> i32 + Send>;

/// Prepared custom-IO callbacks plus the sanitized filename for FFmpeg.
pub(crate) struct PreparedHttpInput {
    pub read: ReadCallback,
    pub seek: Box<dyn FnMut(i64, i32) -> i64 + Send>,
    pub display_url: CString,
    pub seekable: bool,
    pub io_buffer_size: usize,
    pub failure: Arc<Mutex<Option<HttpInputError>>>,
}

/// Extra state installed on [`Input`] after [`prepare_for_open`].
pub(crate) struct HttpAvioAttach {
    pub display_url: CString,
    pub seekable: bool,
    pub failure: Arc<Mutex<Option<HttpInputError>>>,
}

/// One explicit HTTP(S) media input.
///
/// Convert to [`Input`] with [`From`] and pass it to
/// [`FfmpegContext::builder`](crate::FfmpegContext::builder). Existing
/// `Input` setters (`set_format`, codec options, …) apply after the
/// conversion. Conversion sets `exit_on_error` so a truncated body or
/// `ResourceChanged` fails the job instead of finishing as a short read.
///
/// Share a connection pool with [`HttpClient::input`]. Reconnect is off by
/// default (FFmpeg `reconnect=0`); for seekable VOD resume use
/// [`ReconnectPolicy::seekable_default`].
#[derive(Clone)]
pub struct HttpInput {
    url: reqwest::Url,
    client: Option<HttpClient>,
    extra_headers: Vec<(String, String)>,
    user_agent: Option<String>,
    disable_ua: bool,
    timeouts: HttpTimeouts,
    reconnect: ReconnectPolicy,
    io_buffer_size: usize,
}

impl fmt::Debug for HttpInput {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("HttpInput")
            .field("url", &urlutil::redact_url(&self.url))
            .finish_non_exhaustive()
    }
}

impl HttpInput {
    /// Start a one-shot builder that creates an exclusive [`HttpClient`].
    pub fn builder(url: impl Into<String>) -> HttpInputBuilder {
        HttpInputBuilder::new(url.into())
    }
}

/// Builder for [`HttpInput`].
pub struct HttpInputBuilder {
    url: String,
    client: Option<HttpClient>,
    extra_headers: Vec<(String, String)>,
    user_agent: Option<String>,
    disable_ua: bool,
    timeouts: HttpTimeouts,
    reconnect: ReconnectPolicy,
    io_buffer_size: usize,
}

impl fmt::Debug for HttpInputBuilder {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        let header_names: Vec<&str> = self
            .extra_headers
            .iter()
            .map(|(name, _)| name.as_str())
            .collect();
        let url = parse_input_url(&self.url)
            .map(|u| urlutil::redact_url(&u))
            .unwrap_or_else(|_| "<invalid-url>".into());
        f.debug_struct("HttpInputBuilder")
            .field("url", &url)
            .field("headers", &header_names)
            .finish_non_exhaustive()
    }
}

impl HttpInputBuilder {
    pub(crate) fn new(url: String) -> Self {
        Self {
            url,
            client: None,
            extra_headers: Vec::new(),
            user_agent: None,
            disable_ua: false,
            timeouts: HttpTimeouts::default(),
            reconnect: ReconnectPolicy::default(),
            io_buffer_size: crate::core::context::DEFAULT_CUSTOM_IO_BUFFER_SIZE,
        }
    }

    pub(crate) fn client(mut self, client: HttpClient) -> Self {
        self.client = Some(client);
        self
    }

    /// Extra origin header. Reserved names (`Range`, `Accept-Encoding`, …) fail.
    pub fn header(
        mut self,
        name: impl Into<String>,
        value: impl Into<String>,
    ) -> Result<Self, HttpInputError> {
        let name = name.into();
        let value = value.into();
        config::validate_header(&name, &value)?;
        self.extra_headers.push((name, value));
        Ok(self)
    }

    /// Override User-Agent for this input.
    pub fn user_agent(mut self, ua: impl Into<String>) -> Self {
        self.user_agent = Some(ua.into());
        self.disable_ua = false;
        self
    }

    /// Send no User-Agent on this input.
    pub fn disable_user_agent(mut self) -> Self {
        self.disable_ua = true;
        self.user_agent = None;
        self
    }

    /// Per-input response-header and body-idle timeouts. Zero durations are
    /// rejected. Connect timeout is a client-level setting: exclusive builders
    /// apply it when the hidden client is created; a shared [`HttpClient`]
    /// keeps the connect budget from [`HttpClientBuilder::timeouts`].
    pub fn timeouts(mut self, timeouts: HttpTimeouts) -> Self {
        self.timeouts = timeouts;
        self
    }

    /// Body idle timeout convenience (see [`HttpTimeouts::read_idle`]).
    pub fn read_idle_timeout(mut self, idle: Duration) -> Self {
        self.timeouts.read_idle = Some(idle);
        self
    }

    /// Reconnect policy. Default is off (FFmpeg `reconnect=0`).
    ///
    /// For a seekable file, [`ReconnectPolicy::seekable_default`] enables a
    /// conservative retry budget and still requires an ETag or Last-Modified
    /// unless `require_validator` is set false. For a non-seekable live
    /// stream of unknown length, use [`ReconnectPolicy::streamed_default`]
    /// (a hand-built policy with only `reconnect_streamed` set never
    /// matches a resumable case and is a no-op).
    pub fn reconnect(mut self, policy: ReconnectPolicy) -> Self {
        self.reconnect = policy;
        self
    }

    /// AVIO buffer size (same contract as [`Input::set_io_buffer_size`](crate::Input::set_io_buffer_size)).
    ///
    /// This sizes FFmpeg's read buffer only. The network read-ahead between
    /// the HTTP worker and AVIO is a fixed bounded queue (8 events of at
    /// most 64 KiB each, about 512 KiB) and is not configurable.
    pub fn io_buffer_size(mut self, size: usize) -> Self {
        self.io_buffer_size = size;
        self
    }

    /// Validate configuration. No network I/O.
    pub fn build(self) -> Result<HttpInput, HttpInputError> {
        self.timeouts.validate()?;
        config::validate_header_set(&self.extra_headers)?;
        let url = parse_input_url(&self.url)?;
        if let Some(kind) = sniff::manifest_from_url(url.as_str()) {
            return Err(HttpInputError::ManifestUnsupported { kind });
        }
        if self.io_buffer_size == 0 || self.io_buffer_size > i32::MAX as usize {
            return Err(HttpInputError::InvalidUrl {
                reason: "io_buffer_size must be in 1..=i32::MAX",
            });
        }
        Ok(HttpInput {
            url,
            client: self.client,
            extra_headers: self.extra_headers,
            user_agent: self.user_agent,
            disable_ua: self.disable_ua,
            timeouts: self.timeouts,
            reconnect: self.reconnect,
            io_buffer_size: self.io_buffer_size,
        })
    }
}

impl From<HttpInput> for Input {
    fn from(http: HttpInput) -> Self {
        let io_buffer_size = http.io_buffer_size;
        let mut input = Input::from(String::new());
        input.url = None;
        input.http_input = Some(http);
        input.io_buffer_size = io_buffer_size;
        // TruncatedBody / ResourceChanged map to EIO. FFmpeg's default
        // exit_on_error=0 would otherwise finish the job after demux has
        // started. Callers can still `.set_exit_on_error(false)` after this.
        input.exit_on_error = Some(true);
        input
    }
}

pub(crate) fn attach_http_input(
    input: &mut Input,
    interrupt: &Arc<InterruptState>,
) -> crate::error::Result<()> {
    if let Some(format) = input.format.as_deref() {
        if let Some(kind) = sniff::manifest_from_format(format) {
            return Err(HttpInputError::ManifestUnsupported { kind }.into());
        }
    }
    let http = input
        .http_input
        .take()
        .expect("attach_http_input without http_input");
    let prepared = prepare_for_open(http, interrupt)?;
    input.read_callback = Some(prepared.read);
    input.seek_callback = Some(prepared.seek);
    input.io_buffer_size = prepared.io_buffer_size;
    input.http_avio = Some(HttpAvioAttach {
        display_url: prepared.display_url,
        seekable: prepared.seekable,
        failure: prepared.failure,
    });
    Ok(())
}

pub(crate) fn prepare_for_open(
    input: HttpInput,
    interrupt: &Arc<InterruptState>,
) -> Result<PreparedHttpInput, HttpInputError> {
    let client = match input.client {
        Some(client) => client,
        None => exclusive_client(
            input.timeouts.clone(),
            input.user_agent.clone(),
            input.disable_ua,
        )?,
    };
    let runtime = client.runtime()?;
    let cancel = Arc::new(AtomicBool::new(false));
    let (event_tx, event_rx) = bounded(CHANNEL_CAP);
    let (reply_tx, reply_rx) = std::sync::mpsc::channel();
    let user_agent = if input.disable_ua {
        None
    } else {
        input
            .user_agent
            .clone()
            .or_else(|| client.inner.user_agent.clone())
    };
    let spec = RequestSpec {
        url: input.url.clone(),
        extra_headers: input.extra_headers.clone(),
        user_agent,
        header_timeout: input.timeouts.response_headers,
        read_idle: input.timeouts.read_idle,
        redirect_limit: client.inner.redirect_limit,
        range_start: 0,
        send_range: true,
        if_range: None,
        generation: 0,
    };
    // Shared with every StreamJob of this input: the opening response may
    // omit ETag / Last-Modified while a later 206 window carries one, and a
    // later seek needs that learned validator for its If-Range.
    let learned_validator: Arc<Mutex<Option<String>>> = Arc::new(Mutex::new(None));
    runtime.submit(StreamJob {
        client: client.inner.client.clone(),
        spec: spec.clone(),
        event_tx,
        reply_tx: Mutex::new(Some(reply_tx)),
        cancel: Arc::clone(&cancel),
        reconnect: input.reconnect.clone(),
        prior: None,
        learned_validator: Arc::clone(&learned_validator),
    })?;
    let meta = wait_reply(&reply_rx, interrupt, &cancel)?;
    if let Some(kind) = sniff::sniff_manifest(
        meta.final_url.as_str(),
        meta.content_type.as_deref(),
        &meta.prefix,
    ) {
        cancel.store(true, Ordering::Relaxed);
        return Err(HttpInputError::ManifestUnsupported { kind });
    }
    let mut spec = spec;
    spec.url = meta.final_url.clone();
    spec.if_range = meta.validator.clone();
    stream::drop_cross_origin_secrets(&mut spec, &input.url);
    let cancel = Arc::clone(&cancel);
    let failure = Arc::new(Mutex::new(None));
    let state = Arc::new(Mutex::new(http_avio::new_state(
        event_rx,
        if meta.prefix.is_empty() {
            None
        } else {
            Some(meta.prefix)
        },
        meta.size,
        meta.seekable,
        cancel,
        client,
        runtime,
        spec,
        Arc::clone(interrupt),
        input.reconnect,
        Arc::clone(&failure),
        learned_validator,
    )));
    let read_state = Arc::clone(&state);
    let seek_state = Arc::clone(&state);
    let display = urlutil::sanitized_display_url(&meta.final_url);
    let display_url = CString::new(display)
        .unwrap_or_else(|_| CString::new("https://http-input.invalid/resource").expect("static"));
    Ok(PreparedHttpInput {
        read: Box::new(move |buf| http_avio::read(&read_state, buf)),
        seek: Box::new(move |offset, whence| http_avio::seek(&seek_state, offset, whence)),
        display_url,
        seekable: meta.seekable,
        io_buffer_size: input.io_buffer_size,
        failure,
    })
}

pub(crate) unsafe fn reject_manifest_demuxer(
    ctx: *mut ffmpeg_sys_next::AVFormatContext,
) -> Result<(), HttpInputError> {
    if ctx.is_null() || (*ctx).iformat.is_null() {
        return Ok(());
    }
    let name = std::ffi::CStr::from_ptr((*(*ctx).iformat).name).to_string_lossy();
    if name
        .split(',')
        .any(|part| part == "hls" || part == "applehttp")
    {
        return Err(HttpInputError::ManifestUnsupported {
            kind: ManifestKind::Hls,
        });
    }
    if name.split(',').any(|part| part == "dash") {
        return Err(HttpInputError::ManifestUnsupported {
            kind: ManifestKind::Dash,
        });
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn builder_rejects_hls_url() {
        let err = HttpInput::builder("https://example.com/live.m3u8")
            .build()
            .unwrap_err();
        assert!(
            matches!(
                err,
                HttpInputError::ManifestUnsupported {
                    kind: ManifestKind::Hls
                }
            ),
            "{err}"
        );
        let msg = err.to_string();
        assert!(msg.contains("HLS manifests"), "{msg}");
        assert!(msg.contains("single media stream"), "{msg}");
    }

    #[test]
    fn builder_rejects_dash_url() {
        let err = HttpInput::builder("https://example.com/manifest.mpd")
            .build()
            .unwrap_err();
        assert!(matches!(
            err,
            HttpInputError::ManifestUnsupported {
                kind: ManifestKind::Dash
            }
        ));
        let msg = err.to_string();
        assert!(msg.contains("DASH manifests"), "{msg}");
    }

    #[test]
    fn builder_rejects_userinfo() {
        let err = HttpInput::builder("https://u:p@example.com/v.mp4")
            .build()
            .unwrap_err();
        assert!(matches!(err, HttpInputError::UserinfoForbidden));
    }

    #[test]
    fn builder_rejects_reserved_header() {
        let err = HttpInput::builder("https://example.com/v.mp4")
            .header("Accept-Encoding", "gzip")
            .unwrap_err();
        assert!(matches!(err, HttpInputError::HeaderReserved { .. }));
    }

    #[test]
    fn from_http_input_does_not_set_url() {
        let http = HttpInput::builder("https://example.com/v.mp4")
            .build()
            .unwrap();
        let input = Input::from(http);
        assert!(input.url.is_none());
        assert!(input.http_input.is_some());
        assert!(input.read_callback.is_none());
        assert_eq!(input.exit_on_error, Some(true));
    }

    #[test]
    fn url_from_does_not_install_http_input() {
        let input = Input::from("https://example.com/v.mp4");
        assert_eq!(input.url.as_deref(), Some("https://example.com/v.mp4"));
        assert!(input.http_input.is_none());
        assert!(input.read_callback.is_none());
    }

    #[test]
    fn shared_client_input_inherits_timeouts() {
        let timeouts = HttpTimeouts {
            connect: Duration::from_secs(3),
            response_headers: Duration::from_secs(4),
            read_idle: Some(Duration::from_secs(5)),
        };
        let client = HttpClient::builder().timeouts(timeouts).build().unwrap();
        let http = client.input("https://example.com/v.mp4").build().unwrap();
        assert_eq!(http.timeouts.connect, Duration::from_secs(3));
        assert_eq!(http.timeouts.response_headers, Duration::from_secs(4));
        assert_eq!(http.timeouts.read_idle, Some(Duration::from_secs(5)));
    }
}
