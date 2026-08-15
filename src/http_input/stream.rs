//! Request generation, redirect policy, and body event pump.

use crate::core::context::InterruptState;
use crate::http_input::client::sanitize_transport;
use crate::http_input::config::{ReconnectPolicy, STOP_POLL_TICK};
use crate::http_input::error::HttpInputError;
use crate::http_input::sniff;
use crate::http_input::urlutil::same_origin;
use bytes::Bytes;
use crossbeam_channel::{Receiver, Sender, TrySendError};
use futures_util::StreamExt;
use reqwest::header::{
    HeaderMap, HeaderName, HeaderValue, ACCEPT_ENCODING, AUTHORIZATION, CONTENT_ENCODING,
    CONTENT_LENGTH, CONTENT_RANGE, CONTENT_TYPE, COOKIE, IF_RANGE, LOCATION, RANGE, USER_AGENT,
};
use reqwest::Url;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::mpsc::RecvTimeoutError;
use std::sync::{Arc, Mutex};
use std::time::{Duration, SystemTime};

const INITIAL_BACKOFF: Duration = Duration::from_millis(250);
/// Honest CDNs may window a large object; 40×16-byte tests stay under this.
/// Pathological 16-byte windows on a multi-GB object fail closed.
const MAX_RANGE_CONTINUATIONS: u32 = 4096;
const MAX_RESPONSE_HEADER_BYTES: usize = 64 * 1024;

#[derive(Clone)]
pub(crate) enum BodyEvent {
    Data {
        generation: u64,
        bytes: Bytes,
    },
    Eof {
        generation: u64,
    },
    Error {
        generation: u64,
        error: HttpInputError,
    },
}

#[derive(Clone)]
pub(crate) struct RequestSpec {
    pub url: Url,
    pub extra_headers: Vec<(String, String)>,
    pub user_agent: Option<String>,
    pub header_timeout: Duration,
    pub read_idle: Option<Duration>,
    pub redirect_limit: u32,
    pub range_start: u64,
    pub send_range: bool,
    pub if_range: Option<String>,
    pub generation: u64,
}

/// Identity of an already-opened resource. Seek jobs carry this so the
/// first Range GET is checked like a continuation even when
/// `origin_start == target`.
#[derive(Clone)]
pub(crate) struct ResourceIdentity {
    pub url: Url,
    pub size: Option<u64>,
    pub validator: Option<String>,
}

#[derive(Clone)]
pub(crate) struct OpenMeta {
    pub final_url: Url,
    pub status: u16,
    pub size: Option<u64>,
    pub seekable: bool,
    pub content_type: Option<String>,
    pub validator: Option<String>,
    pub range_end: Option<u64>,
    pub prefix: Bytes,
}

pub(crate) struct StreamJob {
    pub client: reqwest::Client,
    pub spec: RequestSpec,
    pub event_tx: Sender<BodyEvent>,
    pub reply_tx: Mutex<Option<std::sync::mpsc::Sender<Result<OpenMeta, HttpInputError>>>>,
    pub cancel: Arc<AtomicBool>,
    pub reconnect: ReconnectPolicy,
    pub prior: Option<ResourceIdentity>,
    /// Validator (ETag / Last-Modified) learned from any response of this
    /// input, shared with the AVIO state. The first response may omit a
    /// validator while a later 206 window carries one; without this slot a
    /// later seek would build its If-Range from the stale `None` in the
    /// original spec and fail closed under `require_validator`.
    pub learned_validator: Arc<Mutex<Option<String>>>,
}

pub(crate) async fn run_job(job: StreamJob) {
    if let Err(err) = run_job_inner(&job).await {
        reply_err(&job, err.clone());
        let _ = send_event(
            &job.event_tx,
            BodyEvent::Error {
                generation: job.spec.generation,
                error: err,
            },
            &job.cancel,
        )
        .await;
    }
}

async fn run_job_inner(job: &StreamJob) -> Result<(), HttpInputError> {
    let policy = &job.reconnect;
    let mut spec = job.spec.clone();
    let mut offset = spec.range_start;
    let origin_start = job.spec.range_start;
    let mut size: Option<u64> = job.prior.as_ref().and_then(|p| p.size);
    let mut validator = job
        .prior
        .as_ref()
        .and_then(|p| p.validator.clone())
        .or_else(|| spec.if_range.clone());
    // Seek jobs are only started for a seekable AVIO; treat them as
    // already-seekable so the first GET is identity-sensitive.
    let mut seekable = job.prior.is_some();
    let mut replied = false;
    let mut retries = 0u32;
    let mut spent_backoff = Duration::ZERO;
    let mut retried_without_range = false;
    let mut backoff_idx = 0u32;
    let mut range_windows = 0u32;

    loop {
        if job.cancel.load(Ordering::Relaxed) {
            return Err(HttpInputError::Interrupted);
        }

        spec.range_start = offset;
        let continuation = replied || offset != origin_start || job.prior.is_some();
        if continuation && seekable {
            spec.send_range = true;
            spec.if_range = validator.clone();
            if policy.require_validator && validator.is_none() {
                return Err(HttpInputError::ResourceChanged);
            }
        } else if continuation {
            spec.if_range = None;
        }

        let response = match send_follow_redirects(&job.client, &spec, &job.cancel).await {
            Ok(response) => response,
            Err(err) => {
                if !replied
                    && is_pre_body_retryable(&err)
                    && retry_budget_allows(policy, retries, spent_backoff)
                {
                    wait_and_count_retry(
                        policy,
                        &mut retries,
                        &mut backoff_idx,
                        &mut spent_backoff,
                        None,
                        &job.cancel,
                    )
                    .await?;
                    continue;
                }
                return Err(err);
            }
        };

        let status = response.status().as_u16();
        if (status == 400 || status == 416)
            && spec.send_range
            && spec.range_start == 0
            && !retried_without_range
            && !replied
        {
            retried_without_range = true;
            spec.send_range = false;
            continue;
        }
        if status == 416 {
            let unsatisfied = response
                .headers()
                .get(CONTENT_RANGE)
                .and_then(|v| v.to_str().ok())
                .and_then(parse_unsatisfied_content_range);
            if unsatisfied == Some(spec.range_start) || size == Some(spec.range_start) {
                if !has_replied(job) {
                    reply_ok(
                        job,
                        OpenMeta {
                            final_url: spec.url.clone(),
                            status: 416,
                            size: Some(spec.range_start),
                            seekable: true,
                            content_type: None,
                            validator: spec.if_range.clone(),
                            range_end: None,
                            prefix: Bytes::new(),
                        },
                    );
                }
                return finish_clean_eof(job, &spec).await;
            }
            return Err(HttpInputError::RangeNotSatisfiable);
        }
        if policy.enabled
            && policy.retry_http_statuses.contains(&status)
            && retry_budget_allows(policy, retries, spent_backoff)
        {
            let retry_after = retry_after_from(response.headers());
            drop(response);
            wait_and_count_retry(
                policy,
                &mut retries,
                &mut backoff_idx,
                &mut spent_backoff,
                retry_after,
                &job.cancel,
            )
            .await?;
            continue;
        }
        if status != 200 && status != 206 {
            return Err(map_status(status));
        }
        if status == 200 && spec.range_start != 0 {
            return Err(if spec.if_range.is_some() {
                HttpInputError::ResourceChanged
            } else {
                HttpInputError::RangeIgnored
            });
        }
        check_content_encoding(response.headers())?;
        let meta = parse_meta(&response, &spec)?;
        if continuation {
            let expected_url = job.prior.as_ref().map(|p| &p.url).unwrap_or(&spec.url);
            if response.url() != expected_url {
                return Err(HttpInputError::ResourceChanged);
            }
            if let (Some(old), Some(new)) = (size, meta.size) {
                if old != new {
                    return Err(HttpInputError::ResourceChanged);
                }
            }
            if let (Some(old), Some(new)) = (validator.as_ref(), meta.validator.as_ref()) {
                if old != new {
                    return Err(HttpInputError::ResourceChanged);
                }
            }
            if seekable && policy.require_validator && meta.validator.is_none() {
                return Err(HttpInputError::ResourceChanged);
            }
        }

        spec.url = meta.final_url.clone();
        drop_cross_origin_secrets(&mut spec, &job.spec.url);
        if meta.size.is_some() {
            size = meta.size;
        }
        if meta.validator.is_some() {
            validator = meta.validator.clone();
        }
        if let Some(v) = validator.as_ref() {
            let mut slot = job
                .learned_validator
                .lock()
                .unwrap_or_else(|e| e.into_inner());
            *slot = Some(v.clone());
        }
        if meta.seekable {
            seekable = true;
        }

        let range_end_excl = if status == 206 {
            match meta.range_end {
                Some(end) => Some(end.saturating_add(1)),
                None => return Err(HttpInputError::RangeNotSatisfiable),
            }
        } else {
            size
        };
        if let Some(end_excl) = range_end_excl {
            if end_excl <= spec.range_start && status == 206 {
                return Err(HttpInputError::RangeNotSatisfiable);
            }
        }

        let start_of_this = spec.range_start;
        let mut stream = response.bytes_stream();
        match read_and_forward(
            &mut stream,
            job,
            &spec,
            &mut offset,
            range_end_excl,
            !replied,
            meta,
        )
        .await
        {
            Ok(()) => {
                replied = has_replied(job);
                let expected_end = range_end_excl;
                if let Some(end_excl) = expected_end {
                    if offset < end_excl {
                        if can_resume_seekable(policy, seekable, validator.as_deref())
                            && retry_budget_allows(policy, retries, spent_backoff)
                        {
                            wait_and_count_retry(
                                policy,
                                &mut retries,
                                &mut backoff_idx,
                                &mut spent_backoff,
                                None,
                                &job.cancel,
                            )
                            .await?;
                            continue;
                        }
                        if can_resume_streamed(policy, seekable, size)
                            && retry_budget_allows(policy, retries, spent_backoff)
                        {
                            offset = origin_start;
                            spec.send_range = job.spec.send_range;
                            spec.if_range = None;
                            wait_and_count_retry(
                                policy,
                                &mut retries,
                                &mut backoff_idx,
                                &mut spent_backoff,
                                None,
                                &job.cancel,
                            )
                            .await?;
                            continue;
                        }
                        return Err(HttpInputError::TruncatedBody);
                    }
                }

                if status == 206 {
                    match size {
                        Some(total) if offset < total => {
                            // Honest windowing CDNs may need hundreds of
                            // requests for a large object. Zero-progress
                            // fail-closes; the declared total bounds the loop.
                            if offset <= start_of_this {
                                return Err(HttpInputError::RangeNotSatisfiable);
                            }
                            note_range_continuation(&mut range_windows)?;
                            continue;
                        }
                        None => {
                            // RFC 7233 unknown instance-length is not proof
                            // the resource is complete.
                            return Err(HttpInputError::TruncatedBody);
                        }
                        Some(_) => {}
                    }
                }

                if policy.enabled
                    && policy.reconnect_at_eof
                    && retry_budget_allows(policy, retries, spent_backoff)
                {
                    if can_resume_streamed(policy, seekable, size) {
                        offset = origin_start;
                        spec.send_range = job.spec.send_range;
                        spec.if_range = None;
                    } else if !seekable {
                        // Known-length VOD without Accept-Ranges: a second
                        // GET would replay the body from byte 0.
                        return finish_clean_eof(job, &spec).await;
                    }
                    wait_and_count_retry(
                        policy,
                        &mut retries,
                        &mut backoff_idx,
                        &mut spent_backoff,
                        None,
                        &job.cancel,
                    )
                    .await?;
                    continue;
                }
                return finish_clean_eof(job, &spec).await;
            }
            Err(err) if matches!(err, HttpInputError::Interrupted) => return Err(err),
            Err(err) => {
                // Only "FFmpeg already has OpenMeta". A transport/idle error
                // before the first byte must keep reply_first so a reconnect
                // can still call reply_ok; otherwise wait_reply hangs.
                replied = has_replied(job);
                let declared_short = range_end_excl.map(|end| offset < end).unwrap_or(false);
                if can_resume_seekable(policy, seekable, validator.as_deref())
                    && retry_budget_allows(policy, retries, spent_backoff)
                {
                    wait_and_count_retry(
                        policy,
                        &mut retries,
                        &mut backoff_idx,
                        &mut spent_backoff,
                        None,
                        &job.cancel,
                    )
                    .await?;
                    continue;
                }
                if can_resume_streamed(policy, seekable, size)
                    && retry_budget_allows(policy, retries, spent_backoff)
                {
                    offset = origin_start;
                    spec.send_range = job.spec.send_range;
                    spec.if_range = None;
                    wait_and_count_retry(
                        policy,
                        &mut retries,
                        &mut backoff_idx,
                        &mut spent_backoff,
                        None,
                        &job.cancel,
                    )
                    .await?;
                    continue;
                }
                if declared_short && matches!(err, HttpInputError::Transport { .. }) {
                    return Err(HttpInputError::TruncatedBody);
                }
                return Err(err);
            }
        }
    }
}

fn can_resume_seekable(policy: &ReconnectPolicy, seekable: bool, validator: Option<&str>) -> bool {
    if !policy.enabled || !seekable {
        return false;
    }
    if policy.require_validator && validator.is_none() {
        return false;
    }
    true
}

fn can_resume_streamed(policy: &ReconnectPolicy, seekable: bool, size: Option<u64>) -> bool {
    // Live / unknown-length only. Seekable resources use Range resume.
    // A known Content-Length without Accept-Ranges is VOD: restarting from
    // byte 0 would duplicate prefix bytes already given to FFmpeg.
    policy.enabled && policy.reconnect_streamed && !seekable && size.is_none()
}

fn is_pre_body_retryable(err: &HttpInputError) -> bool {
    matches!(
        err,
        HttpInputError::Timeout
            | HttpInputError::ReadIdleTimeout
            | HttpInputError::Transport { .. }
    )
}

fn note_range_continuation(range_windows: &mut u32) -> Result<(), HttpInputError> {
    *range_windows = range_windows.saturating_add(1);
    if *range_windows > MAX_RANGE_CONTINUATIONS {
        Err(HttpInputError::Transport {
            message: "too many HTTP 206 range windows".into(),
        })
    } else {
        Ok(())
    }
}

fn retry_budget_allows(policy: &ReconnectPolicy, retries: u32, spent: Duration) -> bool {
    if !policy.enabled || retries >= policy.max_retries {
        return false;
    }
    if policy.max_total_delay.is_zero() {
        return true;
    }
    spent < policy.max_total_delay
}

fn next_backoff_delay(
    policy: &ReconnectPolicy,
    backoff_idx: u32,
    retry_after: Option<Duration>,
    spent: Duration,
) -> Duration {
    let shift = backoff_idx.min(20);
    let exponential = INITIAL_BACKOFF
        .saturating_mul(1u32 << shift)
        .min(policy.max_delay);
    let chosen = match (policy.respect_retry_after, retry_after) {
        (true, Some(after)) => after.min(policy.max_delay),
        _ => exponential,
    };
    if policy.max_total_delay.is_zero() {
        chosen
    } else {
        chosen.min(policy.max_total_delay.saturating_sub(spent))
    }
}

async fn wait_and_count_retry(
    policy: &ReconnectPolicy,
    retries: &mut u32,
    backoff_idx: &mut u32,
    spent: &mut Duration,
    retry_after: Option<Duration>,
    cancel: &AtomicBool,
) -> Result<(), HttpInputError> {
    let delay = next_backoff_delay(policy, *backoff_idx, retry_after, *spent);
    wait_backoff(delay, cancel).await?;
    *spent = spent.saturating_add(delay);
    *retries = retries.saturating_add(1);
    *backoff_idx = backoff_idx.saturating_add(1);
    Ok(())
}

async fn wait_backoff(delay: Duration, cancel: &AtomicBool) -> Result<(), HttpInputError> {
    if delay.is_zero() {
        return Ok(());
    }
    let start = tokio::time::Instant::now();
    loop {
        if cancel.load(Ordering::Relaxed) {
            return Err(HttpInputError::Interrupted);
        }
        let elapsed = start.elapsed();
        if elapsed >= delay {
            return Ok(());
        }
        let slice = delay.saturating_sub(elapsed).min(STOP_POLL_TICK);
        tokio::select! {
            _ = cancel_watch(cancel) => return Err(HttpInputError::Interrupted),
            _ = tokio::time::sleep(slice) => {}
        }
    }
}

async fn finish_clean_eof(job: &StreamJob, spec: &RequestSpec) -> Result<(), HttpInputError> {
    send_event(
        &job.event_tx,
        BodyEvent::Eof {
            generation: spec.generation,
        },
        &job.cancel,
    )
    .await
}

async fn read_and_forward<S>(
    stream: &mut S,
    job: &StreamJob,
    spec: &RequestSpec,
    offset: &mut u64,
    range_end_excl: Option<u64>,
    reply_first: bool,
    mut meta: OpenMeta,
) -> Result<(), HttpInputError>
where
    S: futures_util::Stream<Item = Result<Bytes, reqwest::Error>> + Unpin,
{
    let mut first = true;
    let mut prefix_buf = Vec::new();
    loop {
        match next_chunk(stream, spec.read_idle, &job.cancel).await? {
            Some(bytes) if bytes.is_empty() => continue,
            Some(bytes) => {
                let bytes = clip_to_range(bytes, *offset, range_end_excl);
                if bytes.is_empty() {
                    if first && reply_first {
                        meta.prefix = Bytes::from(std::mem::take(&mut prefix_buf));
                        reply_ok(job, meta);
                    }
                    return Ok(());
                }
                if first && reply_first {
                    *offset = offset.saturating_add(bytes.len() as u64);
                    prefix_buf.extend_from_slice(&bytes);
                    if sniff::prefix_needs_more(&prefix_buf)
                        && prefix_buf.len() < sniff::SNIFF_LIMIT
                    {
                        continue;
                    }
                    let overflow = if prefix_buf.len() > sniff::SNIFF_LIMIT {
                        Bytes::copy_from_slice(&prefix_buf[sniff::SNIFF_LIMIT..])
                    } else {
                        Bytes::new()
                    };
                    prefix_buf.truncate(sniff::SNIFF_LIMIT);
                    meta.prefix = Bytes::from(std::mem::take(&mut prefix_buf));
                    reply_ok(job, meta.clone());
                    first = false;
                    if !overflow.is_empty() {
                        for piece in split_chunks(overflow) {
                            send_event(
                                &job.event_tx,
                                BodyEvent::Data {
                                    generation: spec.generation,
                                    bytes: piece,
                                },
                                &job.cancel,
                            )
                            .await?;
                        }
                    }
                    if range_end_excl == Some(*offset) {
                        return Ok(());
                    }
                    continue;
                }
                first = false;
                for piece in split_chunks(bytes) {
                    send_event(
                        &job.event_tx,
                        BodyEvent::Data {
                            generation: spec.generation,
                            bytes: piece.clone(),
                        },
                        &job.cancel,
                    )
                    .await?;
                    *offset = offset.saturating_add(piece.len() as u64);
                    if range_end_excl == Some(*offset) {
                        return Ok(());
                    }
                }
            }
            None => {
                if first && reply_first {
                    meta.prefix = Bytes::from(prefix_buf);
                    reply_ok(job, meta);
                }
                return Ok(());
            }
        }
    }
}

fn clip_to_range(bytes: Bytes, offset: u64, range_end_excl: Option<u64>) -> Bytes {
    let Some(end) = range_end_excl else {
        return bytes;
    };
    if offset >= end {
        return Bytes::new();
    }
    let max = (end - offset) as usize;
    if bytes.len() <= max {
        bytes
    } else {
        bytes.slice(..max)
    }
}

fn split_chunks(bytes: Bytes) -> Vec<Bytes> {
    const MAX: usize = 64 * 1024;
    if bytes.len() <= MAX {
        return vec![bytes];
    }
    let mut out = Vec::new();
    let mut offset = 0;
    while offset < bytes.len() {
        let end = (offset + MAX).min(bytes.len());
        out.push(bytes.slice(offset..end));
        offset = end;
    }
    out
}

async fn next_chunk<S>(
    stream: &mut S,
    idle: Option<Duration>,
    cancel: &AtomicBool,
) -> Result<Option<Bytes>, HttpInputError>
where
    S: futures_util::Stream<Item = Result<Bytes, reqwest::Error>> + Unpin,
{
    if cancel.load(Ordering::Relaxed) {
        return Err(HttpInputError::Interrupted);
    }
    let next = stream.next();
    let result = if let Some(idle) = idle {
        tokio::select! {
            _ = cancel_watch(cancel) => return Err(HttpInputError::Interrupted),
            result = tokio::time::timeout(idle, next) => match result {
                Ok(inner) => inner,
                Err(_) => return Err(HttpInputError::ReadIdleTimeout),
            }
        }
    } else {
        tokio::select! {
            _ = cancel_watch(cancel) => return Err(HttpInputError::Interrupted),
            result = next => result,
        }
    };
    match result {
        Some(Ok(bytes)) => Ok(Some(bytes)),
        Some(Err(err)) => Err(map_reqwest(err)),
        None => Ok(None),
    }
}

async fn cancel_watch(cancel: &AtomicBool) {
    loop {
        if cancel.load(Ordering::Relaxed) {
            return;
        }
        tokio::time::sleep(STOP_POLL_TICK).await;
    }
}

async fn send_follow_redirects(
    client: &reqwest::Client,
    spec: &RequestSpec,
    cancel: &AtomicBool,
) -> Result<reqwest::Response, HttpInputError> {
    let mut url = spec.url.clone();
    let mut headers = build_headers(spec)?;
    for _ in 0..=spec.redirect_limit {
        if cancel.load(Ordering::Relaxed) {
            return Err(HttpInputError::Interrupted);
        }
        let send = client.get(url.clone()).headers(headers.clone()).send();
        let response = tokio::select! {
            _ = cancel_watch(cancel) => return Err(HttpInputError::Interrupted),
            result = tokio::time::timeout(spec.header_timeout, send) => match result {
                Ok(Ok(resp)) => resp,
                Ok(Err(err)) => return Err(map_reqwest(err)),
                Err(_) => return Err(HttpInputError::Timeout),
            }
        };
        if response_headers_over_limit(response.headers()) {
            return Err(HttpInputError::Transport {
                message: "response headers exceed 64KiB".into(),
            });
        }
        if !response.status().is_redirection() {
            return Ok(response);
        }
        let loc = response
            .headers()
            .get(LOCATION)
            .and_then(|v| v.to_str().ok())
            .ok_or(HttpInputError::TooManyRedirects)?;
        let next = next_redirect_url(&url, loc)?;
        if !same_origin(&url, &next) {
            strip_cross_origin_headers(&mut headers, spec);
        }
        url = next;
    }
    Err(HttpInputError::TooManyRedirects)
}

fn next_redirect_url(current: &Url, location: &str) -> Result<Url, HttpInputError> {
    let next = current
        .join(location)
        .map_err(|_| HttpInputError::InvalidUrl {
            reason: "redirect Location is not a valid URL",
        })?;
    match next.scheme() {
        "http" | "https" => {}
        _ => {
            return Err(HttpInputError::InvalidUrl {
                reason: "redirect target must be http or https",
            });
        }
    }
    if current.scheme() == "https" && next.scheme() == "http" {
        return Err(HttpInputError::HttpsDowngrade);
    }
    Ok(next)
}

/// Persist the hop-local strip so later Range / reconnect / seek requests
/// built from `spec` do not re-attach origin credentials to the new URL.
pub(crate) fn drop_cross_origin_secrets(spec: &mut RequestSpec, original: &Url) {
    if !same_origin(original, &spec.url) {
        spec.extra_headers.clear();
    }
}

fn strip_cross_origin_headers(headers: &mut HeaderMap, spec: &RequestSpec) {
    headers.remove(AUTHORIZATION);
    headers.remove(COOKIE);
    for (name, _) in &spec.extra_headers {
        if let Ok(header) = HeaderName::from_bytes(name.as_bytes()) {
            headers.remove(header);
        }
    }
}

fn response_headers_over_limit(headers: &HeaderMap) -> bool {
    let mut n = 0usize;
    for (key, value) in headers.iter() {
        n = n
            .saturating_add(key.as_str().len())
            .saturating_add(value.as_bytes().len());
        if n > MAX_RESPONSE_HEADER_BYTES {
            return true;
        }
    }
    false
}

fn build_headers(spec: &RequestSpec) -> Result<HeaderMap, HttpInputError> {
    let mut headers = HeaderMap::new();
    headers.insert(ACCEPT_ENCODING, HeaderValue::from_static("identity"));
    if spec.send_range {
        let value = format!("bytes={}-", spec.range_start);
        headers.insert(
            RANGE,
            HeaderValue::from_str(&value).map_err(|_| HttpInputError::HeaderInvalid {
                name: "Range".into(),
            })?,
        );
    }
    if let Some(ua) = &spec.user_agent {
        headers.insert(
            USER_AGENT,
            HeaderValue::from_str(ua).map_err(|_| HttpInputError::HeaderInvalid {
                name: "User-Agent".into(),
            })?,
        );
    }
    if let Some(validator) = &spec.if_range {
        headers.insert(
            IF_RANGE,
            HeaderValue::from_str(validator).map_err(|_| HttpInputError::HeaderInvalid {
                name: "If-Range".into(),
            })?,
        );
    }
    for (name, value) in &spec.extra_headers {
        let header_name = HeaderName::from_bytes(name.as_bytes())
            .map_err(|_| HttpInputError::HeaderInvalid { name: name.clone() })?;
        let header_value = HeaderValue::from_bytes(value.as_bytes())
            .map_err(|_| HttpInputError::HeaderInvalid { name: name.clone() })?;
        headers.insert(header_name, header_value);
    }
    Ok(headers)
}

fn check_content_encoding(headers: &HeaderMap) -> Result<(), HttpInputError> {
    if let Some(enc) = headers.get(CONTENT_ENCODING) {
        let value = enc.to_str().unwrap_or("");
        if !value.is_empty() && !value.eq_ignore_ascii_case("identity") {
            return Err(HttpInputError::UnsupportedContentEncoding);
        }
    }
    Ok(())
}

fn parse_meta(
    response: &reqwest::Response,
    spec: &RequestSpec,
) -> Result<OpenMeta, HttpInputError> {
    let status = response.status().as_u16();
    let headers = response.headers();
    let mut size = None;
    let mut seekable = false;
    let mut range_end = None;
    if status == 206 {
        let cr = headers
            .get(CONTENT_RANGE)
            .and_then(|v| v.to_str().ok())
            .ok_or(HttpInputError::RangeNotSatisfiable)?;
        let parsed = parse_content_range(cr).ok_or(HttpInputError::RangeNotSatisfiable)?;
        if parsed.start != spec.range_start {
            return Err(HttpInputError::RangeNotSatisfiable);
        }
        size = parsed.total;
        seekable = true;
        range_end = Some(parsed.end);
    } else {
        if spec.send_range && spec.range_start != 0 {
            return Err(HttpInputError::RangeIgnored);
        }
        if let Some(cl) = headers.get(CONTENT_LENGTH).and_then(|v| v.to_str().ok()) {
            size = cl.parse().ok();
        }
        if let Some(ar) = headers.get("accept-ranges").and_then(|v| v.to_str().ok()) {
            if ar.eq_ignore_ascii_case("bytes") {
                seekable = true;
            }
        }
    }
    let content_type = headers
        .get(CONTENT_TYPE)
        .and_then(|v| v.to_str().ok())
        .map(|s| s.to_string());
    Ok(OpenMeta {
        final_url: response.url().clone(),
        status,
        size,
        seekable,
        content_type,
        validator: response_validator(headers),
        range_end,
        prefix: Bytes::new(),
    })
}

fn response_validator(headers: &HeaderMap) -> Option<String> {
    if let Some(etag) = headers.get("etag").and_then(|v| v.to_str().ok()) {
        let trimmed = etag.trim();
        if !trimmed.is_empty() && !trimmed.starts_with("W/") && !trimmed.starts_with("w/") {
            return Some(trimmed.to_string());
        }
    }
    headers
        .get("last-modified")
        .and_then(|v| v.to_str().ok())
        .map(|s| s.trim().to_string())
        .filter(|s| !s.is_empty())
}

pub(crate) struct ContentRange {
    start: u64,
    end: u64,
    total: Option<u64>,
}

/// RFC 7233 unsatisfied-range on 416: `bytes */complete-length`.
fn parse_unsatisfied_content_range(header: &str) -> Option<u64> {
    let rest = header.trim().strip_prefix("bytes ")?;
    let (range, total) = rest.split_once('/')?;
    if range.trim() != "*" || total == "*" {
        return None;
    }
    total.parse().ok()
}

pub(crate) fn parse_content_range(header: &str) -> Option<ContentRange> {
    let header = header.trim();
    let rest = header.strip_prefix("bytes ")?;
    let (range, total) = rest.split_once('/')?;
    let (start, end) = range.split_once('-')?;
    let start = start.parse().ok()?;
    let end = end.parse().ok()?;
    if end < start {
        return None;
    }
    let total = if total == "*" {
        None
    } else {
        Some(total.parse().ok()?)
    };
    if let Some(total) = total {
        if end >= total {
            return None;
        }
    }
    Some(ContentRange { start, end, total })
}

fn map_status(status: u16) -> HttpInputError {
    match status {
        401 => HttpInputError::AuthenticationRequired,
        403 => HttpInputError::AccessDenied,
        407 => HttpInputError::ProxyAuthenticationRequired,
        404 | 410 => HttpInputError::NotFound,
        408 => HttpInputError::Timeout,
        416 => HttpInputError::RangeNotSatisfiable,
        code => HttpInputError::Status { code },
    }
}

fn map_reqwest(err: reqwest::Error) -> HttpInputError {
    if err.is_timeout() {
        return HttpInputError::Timeout;
    }
    // `type_name_of_val` on a `&dyn Error` names the trait object, never the
    // concrete type, so downcast the source chain instead and keep the
    // message substring match for errors that hyper re-wraps as strings.
    let mut source: Option<&(dyn std::error::Error + 'static)> = Some(&err);
    while let Some(item) = source {
        if item.downcast_ref::<rustls::Error>().is_some() {
            return HttpInputError::TlsVerification;
        }
        let rendered = item.to_string();
        if rendered.contains("certificate") || rendered.contains("invalid peer certificate") {
            return HttpInputError::TlsVerification;
        }
        source = item.source();
    }
    if err.is_connect() {
        return HttpInputError::Transport {
            message: "connect failed".into(),
        };
    }
    HttpInputError::Transport {
        message: sanitize_transport(&err.to_string()),
    }
}

async fn send_event(
    tx: &Sender<BodyEvent>,
    event: BodyEvent,
    cancel: &AtomicBool,
) -> Result<(), HttpInputError> {
    loop {
        if cancel.load(Ordering::Relaxed) {
            return Err(HttpInputError::Interrupted);
        }
        match tx.try_send(event.clone()) {
            Ok(()) => return Ok(()),
            Err(TrySendError::Full(_)) => {
                tokio::time::sleep(Duration::from_millis(1)).await;
            }
            Err(TrySendError::Disconnected(_)) => return Err(HttpInputError::Interrupted),
        }
    }
}

fn has_replied(job: &StreamJob) -> bool {
    job.reply_tx
        .lock()
        .unwrap_or_else(|e| e.into_inner())
        .is_none()
}

fn reply_ok(job: &StreamJob, meta: OpenMeta) {
    if let Some(tx) = job
        .reply_tx
        .lock()
        .unwrap_or_else(|e| e.into_inner())
        .take()
    {
        let _ = tx.send(Ok(meta));
    }
}

fn reply_err(job: &StreamJob, err: HttpInputError) {
    if let Some(tx) = job
        .reply_tx
        .lock()
        .unwrap_or_else(|e| e.into_inner())
        .take()
    {
        let _ = tx.send(Err(err));
    }
}

pub(crate) fn wait_reply(
    rx: &std::sync::mpsc::Receiver<Result<OpenMeta, HttpInputError>>,
    interrupt: &InterruptState,
    cancel: &AtomicBool,
) -> Result<OpenMeta, HttpInputError> {
    loop {
        match rx.recv_timeout(STOP_POLL_TICK) {
            Ok(result) => return result,
            Err(RecvTimeoutError::Timeout) => {
                if interrupt.is_input_stopping() {
                    cancel.store(true, Ordering::Relaxed);
                    return Err(HttpInputError::Interrupted);
                }
            }
            Err(RecvTimeoutError::Disconnected) => {
                return Err(HttpInputError::Transport {
                    message: "http worker exited before headers".into(),
                });
            }
        }
    }
}

pub(crate) fn wait_event(
    rx: &Receiver<BodyEvent>,
    interrupt: &InterruptState,
    cancel: &AtomicBool,
) -> Result<BodyEvent, HttpInputError> {
    loop {
        match rx.recv_timeout(STOP_POLL_TICK) {
            Ok(event) => return Ok(event),
            Err(crossbeam_channel::RecvTimeoutError::Timeout) => {
                if interrupt.is_input_stopping() {
                    cancel.store(true, Ordering::Relaxed);
                    return Err(HttpInputError::Interrupted);
                }
            }
            Err(crossbeam_channel::RecvTimeoutError::Disconnected) => {
                return Err(HttpInputError::Transport {
                    message: "http body channel closed".into(),
                });
            }
        }
    }
}

fn retry_after_from(headers: &HeaderMap) -> Option<Duration> {
    let raw = headers.get("retry-after").and_then(|v| v.to_str().ok())?;
    parse_retry_after(raw, SystemTime::now())
}

fn parse_retry_after(value: &str, now: SystemTime) -> Option<Duration> {
    let value = value.trim();
    if value.is_empty() {
        return None;
    }
    if value.bytes().all(|b| b.is_ascii_digit()) {
        return value.parse::<u64>().ok().map(Duration::from_secs);
    }
    parse_imf_fixdate(value).and_then(|when| when.duration_since(now).ok())
}

fn parse_imf_fixdate(s: &str) -> Option<SystemTime> {
    let rest = s.trim().split_once(", ")?.1;
    let mut parts = rest.split_whitespace();
    let day: u64 = parts.next()?.parse().ok()?;
    let month = month_num(parts.next()?)?;
    let year: i32 = parts.next()?.parse().ok()?;
    let time = parts.next()?;
    let tz = parts.next()?;
    if !tz.eq_ignore_ascii_case("GMT") && !tz.eq_ignore_ascii_case("UTC") {
        return None;
    }
    let mut hm = time.split(':');
    let hour: u64 = hm.next()?.parse().ok()?;
    let min: u64 = hm.next()?.parse().ok()?;
    let sec: u64 = hm.next()?.parse().ok()?;
    if !(1..=31).contains(&day) || hour > 23 || min > 59 || sec > 60 {
        return None;
    }
    datetime_to_systemtime(year, month, day, hour, min, sec)
}

fn month_num(name: &str) -> Option<u32> {
    Some(match name {
        "Jan" => 1,
        "Feb" => 2,
        "Mar" => 3,
        "Apr" => 4,
        "May" => 5,
        "Jun" => 6,
        "Jul" => 7,
        "Aug" => 8,
        "Sep" => 9,
        "Oct" => 10,
        "Nov" => 11,
        "Dec" => 12,
        _ => return None,
    })
}

fn is_leap(year: i32) -> bool {
    year % 4 == 0 && (year % 100 != 0 || year % 400 == 0)
}

fn datetime_to_systemtime(
    year: i32,
    month: u32,
    day: u64,
    hour: u64,
    min: u64,
    sec: u64,
) -> Option<SystemTime> {
    if year < 1970 || !(1..=12).contains(&month) {
        return None;
    }
    let mut days: u64 = 0;
    for y in 1970..year {
        days = days.saturating_add(if is_leap(y) { 366 } else { 365 });
    }
    const MD: [u64; 12] = [31, 28, 31, 30, 31, 30, 31, 31, 30, 31, 30, 31];
    for m in 1..month {
        days = days.saturating_add(MD[(m - 1) as usize]);
        if m == 2 && is_leap(year) {
            days = days.saturating_add(1);
        }
    }
    days = days.saturating_add(day.saturating_sub(1));
    let secs = days
        .saturating_mul(86400)
        .saturating_add(hour.saturating_mul(3600))
        .saturating_add(min.saturating_mul(60))
        .saturating_add(sec);
    Some(SystemTime::UNIX_EPOCH + Duration::from_secs(secs))
}

#[cfg(test)]
mod tests {
    use super::*;

    fn empty_learned() -> Arc<Mutex<Option<String>>> {
        Arc::new(Mutex::new(None))
    }

    #[test]
    fn content_range_total_and_unknown() {
        let known = parse_content_range("bytes 0-1023/2048").unwrap();
        assert_eq!(known.start, 0);
        assert_eq!(known.end, 1023);
        assert_eq!(known.total, Some(2048));
        let unknown = parse_content_range("bytes 5-10/*").unwrap();
        assert_eq!(unknown.start, 5);
        assert_eq!(unknown.end, 10);
        assert_eq!(unknown.total, None);
        assert_eq!(parse_unsatisfied_content_range("bytes */2048"), Some(2048));
        assert_eq!(parse_unsatisfied_content_range("bytes */*"), None);
        assert_eq!(parse_unsatisfied_content_range("bytes 0-10/2048"), None);
    }

    #[test]
    fn streamed_reconnect_does_not_restart_seekable_unknown_length() {
        let policy = ReconnectPolicy {
            enabled: true,
            reconnect_streamed: true,
            ..ReconnectPolicy::default()
        };
        assert!(!can_resume_streamed(&policy, true, None));
        assert!(!can_resume_streamed(&policy, true, Some(100)));
        assert!(can_resume_streamed(&policy, false, None));
        assert!(!can_resume_streamed(&policy, false, Some(100)));
    }

    #[test]
    fn streamed_reconnect_does_not_restart_known_length_non_seekable() {
        let policy = ReconnectPolicy {
            enabled: true,
            reconnect_streamed: true,
            reconnect_at_eof: true,
            ..ReconnectPolicy::default()
        };
        assert!(
            !can_resume_streamed(&policy, false, Some(64)),
            "Content-Length VOD without Accept-Ranges must not restart from 0"
        );
        assert!(can_resume_streamed(&policy, false, None));
    }

    #[test]
    fn content_range_rejects_inverted_or_past_total() {
        assert!(parse_content_range("bytes 10-9/20").is_none());
        assert!(parse_content_range("bytes 0-20/20").is_none());
    }

    #[test]
    fn status_mapping() {
        assert!(matches!(
            map_status(401),
            HttpInputError::AuthenticationRequired
        ));
        assert!(matches!(map_status(404), HttpInputError::NotFound));
        assert!(matches!(map_status(403), HttpInputError::AccessDenied));
    }

    #[test]
    fn https_to_http_redirect_is_rejected() {
        let from = Url::parse("https://example.com/v.mp4").unwrap();
        let err = next_redirect_url(&from, "http://example.com/v.mp4").unwrap_err();
        assert!(matches!(err, HttpInputError::HttpsDowngrade));
    }

    #[test]
    fn http_to_https_redirect_is_allowed() {
        let from = Url::parse("http://example.com/v.mp4").unwrap();
        let next = next_redirect_url(&from, "https://example.com/v.mp4").unwrap();
        assert_eq!(next.scheme(), "https");
    }

    #[test]
    fn file_and_ftp_redirects_are_rejected() {
        let from = Url::parse("https://example.com/v.mp4").unwrap();
        for loc in ["file:///tmp/v.mp4", "ftp://example.com/v.mp4"] {
            let err = next_redirect_url(&from, loc).unwrap_err();
            assert!(
                matches!(err, HttpInputError::InvalidUrl { .. }),
                "redirect to {loc} must be InvalidUrl, got {err:?}"
            );
        }
    }

    #[test]
    fn cross_origin_strips_auth_cookie_and_extra_headers() {
        let spec = RequestSpec {
            url: Url::parse("http://example.com/v.mp4").unwrap(),
            extra_headers: vec![("X-Api-Key".into(), "secret".into())],
            user_agent: Some("ez".into()),
            header_timeout: Duration::from_secs(2),
            read_idle: Some(Duration::from_secs(2)),
            redirect_limit: 2,
            range_start: 0,
            send_range: false,
            if_range: None,
            generation: 0,
        };
        let mut headers = HeaderMap::new();
        headers.insert(AUTHORIZATION, HeaderValue::from_static("Bearer x"));
        headers.insert(COOKIE, HeaderValue::from_static("a=b"));
        headers.insert(
            HeaderName::from_static("x-api-key"),
            HeaderValue::from_static("secret"),
        );
        headers.insert(USER_AGENT, HeaderValue::from_static("ez"));
        strip_cross_origin_headers(&mut headers, &spec);
        assert!(headers.get(AUTHORIZATION).is_none());
        assert!(headers.get(COOKIE).is_none());
        assert!(headers.get("x-api-key").is_none());
        assert_eq!(headers.get(USER_AGENT).unwrap(), "ez");
    }

    fn dummy_spec(url: &str) -> RequestSpec {
        RequestSpec {
            url: Url::parse(url).unwrap(),
            extra_headers: vec![("X-Api-Key".into(), "secret".into())],
            user_agent: Some("ez".into()),
            header_timeout: Duration::from_secs(2),
            read_idle: Some(Duration::from_secs(2)),
            redirect_limit: 2,
            range_start: 0,
            send_range: false,
            if_range: None,
            generation: 0,
        }
    }

    #[test]
    fn drop_cross_origin_secrets_clears_follow_up_headers() {
        let original = Url::parse("http://origin.example/v.mp4").unwrap();
        let mut spec = dummy_spec("http://cdn.example/v.mp4");
        drop_cross_origin_secrets(&mut spec, &original);
        assert!(
            spec.extra_headers.is_empty(),
            "cross-origin final URL must forget extra headers"
        );
        let headers = build_headers(&spec).unwrap();
        assert!(headers.get("x-api-key").is_none());
        assert!(headers.get(AUTHORIZATION).is_none());
    }

    #[test]
    fn drop_cross_origin_secrets_keeps_same_origin_headers() {
        let original = Url::parse("http://origin.example/v.mp4").unwrap();
        let mut spec = dummy_spec("http://origin.example/other.mp4");
        drop_cross_origin_secrets(&mut spec, &original);
        assert_eq!(spec.extra_headers.len(), 1);
        let headers = build_headers(&spec).unwrap();
        assert_eq!(headers.get("x-api-key").unwrap(), "secret");
    }

    #[test]
    fn seek_prior_without_validator_is_resource_changed() {
        let runtime = tokio::runtime::Builder::new_current_thread()
            .enable_all()
            .build()
            .unwrap();
        let client = reqwest::Client::builder()
            .redirect(reqwest::redirect::Policy::none())
            .build()
            .unwrap();
        let url = Url::parse("http://127.0.0.1:1/resource.bin").unwrap();
        let (event_tx, _event_rx) = crossbeam_channel::bounded(8);
        let (reply_tx, reply_rx) = std::sync::mpsc::channel();
        let job = StreamJob {
            client,
            spec: RequestSpec {
                url: url.clone(),
                extra_headers: Vec::new(),
                user_agent: None,
                header_timeout: Duration::from_millis(50),
                read_idle: Some(Duration::from_millis(50)),
                redirect_limit: 1,
                range_start: 16,
                send_range: true,
                if_range: None,
                generation: 1,
            },
            event_tx,
            reply_tx: Mutex::new(Some(reply_tx)),
            cancel: Arc::new(AtomicBool::new(false)),
            reconnect: ReconnectPolicy::default(),
            prior: Some(ResourceIdentity {
                url,
                size: Some(64),
                validator: None,
            }),
            learned_validator: empty_learned(),
        };
        runtime.block_on(run_job(job));
        let err = match reply_rx.recv_timeout(Duration::from_secs(2)).unwrap() {
            Err(e) => e,
            Ok(_) => panic!("seek without validator must fail closed"),
        };
        assert!(
            matches!(err, HttpInputError::ResourceChanged),
            "expected ResourceChanged, got {err:?}"
        );
    }

    #[test]
    fn wait_event_stop_sets_cancel() {
        let (_tx, rx) = crossbeam_channel::bounded(1);
        let status = Arc::new(std::sync::atomic::AtomicUsize::new(
            crate::core::scheduler::ffmpeg_scheduler::STATUS_END,
        ));
        let interrupt = InterruptState::new(status);
        let cancel = AtomicBool::new(false);
        let err = match wait_event(&rx, &interrupt, &cancel) {
            Err(e) => e,
            Ok(_) => panic!("stop must interrupt before a body event"),
        };
        assert!(matches!(err, HttpInputError::Interrupted));
        assert!(cancel.load(Ordering::Relaxed));
    }

    #[test]
    fn range_continuation_cap_rejects_the_4097th_window() {
        let mut n = 0u32;
        for _ in 0..4096 {
            note_range_continuation(&mut n).unwrap();
        }
        assert_eq!(n, 4096);
        let err = note_range_continuation(&mut n).unwrap_err();
        assert!(matches!(err, HttpInputError::Transport { .. }));
        assert!(
            format!("{err}").contains("206"),
            "cap error should mention 206 windows: {err}"
        );
    }

    #[test]
    fn retry_after_delta_seconds() {
        let delay = parse_retry_after("3", SystemTime::now()).unwrap();
        assert_eq!(delay, Duration::from_secs(3));
    }

    #[test]
    fn retry_after_http_date() {
        let now = datetime_to_systemtime(1994, 11, 6, 8, 49, 30).unwrap();
        let delay = parse_retry_after("Sun, 06 Nov 1994 08:49:37 GMT", now).unwrap();
        assert_eq!(delay, Duration::from_secs(7));
    }

    #[test]
    fn backoff_sequence_is_250_500_1s() {
        let policy = ReconnectPolicy::seekable_default();
        assert_eq!(
            next_backoff_delay(&policy, 0, None, Duration::ZERO),
            INITIAL_BACKOFF
        );
        assert_eq!(
            next_backoff_delay(&policy, 1, None, Duration::ZERO),
            Duration::from_millis(500)
        );
        assert_eq!(
            next_backoff_delay(&policy, 2, None, Duration::ZERO),
            Duration::from_secs(1)
        );
    }

    #[test]
    fn retry_after_is_capped_by_max_delay() {
        let mut policy = ReconnectPolicy::seekable_default();
        policy.max_delay = Duration::from_secs(2);
        let delay = next_backoff_delay(&policy, 0, Some(Duration::from_secs(30)), Duration::ZERO);
        assert_eq!(delay, Duration::from_secs(2));
    }

    #[test]
    fn truncated_body_maps_to_eio() {
        let errno = HttpInputError::TruncatedBody.to_errno();
        assert_eq!(errno, ffmpeg_sys_next::AVERROR(ffmpeg_sys_next::EIO));
        assert_ne!(errno, ffmpeg_sys_next::AVERROR_EOF);
    }

    #[test]
    fn truncated_content_length_is_not_eof_event() {
        use std::io::{Read, Write};
        use std::net::TcpListener;
        use std::thread;

        let listener = TcpListener::bind("127.0.0.1:0").unwrap();
        let addr = listener.local_addr().unwrap();
        thread::spawn(move || {
            if let Ok((mut stream, _)) = listener.accept() {
                let mut buf = [0u8; 2048];
                let _ = stream.read(&mut buf);
                let header = "HTTP/1.1 200 OK\r\nContent-Type: application/octet-stream\r\nContent-Length: 64\r\nConnection: close\r\n\r\n";
                let _ = stream.write_all(header.as_bytes());
                let _ = stream.write_all(&[0u8; 16]);
            }
        });

        let runtime = tokio::runtime::Builder::new_current_thread()
            .enable_all()
            .build()
            .unwrap();
        let client = reqwest::Client::builder()
            .redirect(reqwest::redirect::Policy::none())
            .build()
            .unwrap();
        let url = Url::parse(&format!("http://{addr}/resource.bin")).unwrap();
        let (event_tx, event_rx) = crossbeam_channel::bounded(8);
        let (reply_tx, reply_rx) = std::sync::mpsc::channel();
        let cancel = Arc::new(AtomicBool::new(false));
        let job = StreamJob {
            client,
            spec: RequestSpec {
                url,
                extra_headers: Vec::new(),
                user_agent: None,
                header_timeout: Duration::from_secs(2),
                read_idle: Some(Duration::from_secs(2)),
                redirect_limit: 2,
                range_start: 0,
                send_range: true,
                if_range: None,
                generation: 0,
            },
            event_tx,
            reply_tx: Mutex::new(Some(reply_tx)),
            cancel,
            reconnect: ReconnectPolicy::default(),
            prior: None,
            learned_validator: empty_learned(),
        };
        runtime.block_on(run_job(job));
        let meta = reply_rx
            .recv_timeout(Duration::from_secs(2))
            .unwrap()
            .unwrap();
        assert_eq!(meta.size, Some(64));
        let mut saw_truncated = false;
        let mut saw_eof = false;
        while let Ok(event) = event_rx.recv_timeout(Duration::from_millis(200)) {
            match event {
                BodyEvent::Eof { .. } => saw_eof = true,
                BodyEvent::Error {
                    error: HttpInputError::TruncatedBody,
                    ..
                } => saw_truncated = true,
                _ => {}
            }
        }
        assert!(
            saw_truncated,
            "truncated Content-Length must emit TruncatedBody"
        );
        assert!(!saw_eof, "truncated Content-Length must not emit clean EOF");
    }

    #[test]
    fn idle_timeout_before_first_byte_still_replies_after_reconnect() {
        use std::io::{Read, Write};
        use std::net::TcpListener;
        use std::thread;

        let listener = TcpListener::bind("127.0.0.1:0").unwrap();
        let addr = listener.local_addr().unwrap();
        thread::spawn(move || {
            for i in 0..2 {
                if let Ok((mut stream, _)) = listener.accept() {
                    let mut buf = [0u8; 2048];
                    let _ = stream.read(&mut buf);
                    if i == 0 {
                        let header = "HTTP/1.1 206 Partial Content\r\nContent-Type: application/octet-stream\r\nContent-Range: bytes 0-63/64\r\nContent-Length: 64\r\nETag: \"v1\"\r\nAccept-Ranges: bytes\r\n\r\n";
                        let _ = stream.write_all(header.as_bytes());
                        thread::sleep(Duration::from_millis(400));
                    } else {
                        let header = "HTTP/1.1 206 Partial Content\r\nContent-Type: application/octet-stream\r\nContent-Range: bytes 0-63/64\r\nContent-Length: 64\r\nETag: \"v1\"\r\nAccept-Ranges: bytes\r\nConnection: close\r\n\r\n";
                        let _ = stream.write_all(header.as_bytes());
                        let _ = stream.write_all(&[7u8; 64]);
                    }
                }
            }
        });

        let runtime = tokio::runtime::Builder::new_current_thread()
            .enable_all()
            .build()
            .unwrap();
        let client = reqwest::Client::builder()
            .redirect(reqwest::redirect::Policy::none())
            .build()
            .unwrap();
        let url = Url::parse(&format!("http://{addr}/resource.bin")).unwrap();
        let (event_tx, _event_rx) = crossbeam_channel::bounded(8);
        let (reply_tx, reply_rx) = std::sync::mpsc::channel();
        let cancel = Arc::new(AtomicBool::new(false));
        let job = StreamJob {
            client,
            spec: RequestSpec {
                url,
                extra_headers: Vec::new(),
                user_agent: None,
                header_timeout: Duration::from_secs(2),
                read_idle: Some(Duration::from_millis(80)),
                redirect_limit: 2,
                range_start: 0,
                send_range: true,
                if_range: None,
                generation: 0,
            },
            event_tx,
            reply_tx: Mutex::new(Some(reply_tx)),
            cancel,
            reconnect: ReconnectPolicy::seekable_default(),
            prior: None,
            learned_validator: empty_learned(),
        };
        runtime.block_on(run_job(job));
        let meta = reply_rx
            .recv_timeout(Duration::from_secs(3))
            .unwrap_or_else(|e| panic!("wait_reply hung after pre-body idle timeout: {e}"))
            .unwrap_or_else(|e| panic!("expected OpenMeta after reconnect, got {e}"));
        assert_eq!(meta.size, Some(64));
        assert_eq!(meta.prefix.len(), 64);
    }

    #[test]
    fn short_206_without_validator_emits_resource_changed() {
        use std::io::{Read, Write};
        use std::net::TcpListener;
        use std::thread;

        let listener = TcpListener::bind("127.0.0.1:0").unwrap();
        let addr = listener.local_addr().unwrap();
        thread::spawn(move || {
            let body: Vec<u8> = (0..64).collect();
            for i in 0..2 {
                if let Ok((mut stream, _)) = listener.accept() {
                    let mut buf = [0u8; 2048];
                    let n = stream.read(&mut buf).unwrap_or(0);
                    let req = String::from_utf8_lossy(&buf[..n]);
                    let start = req
                        .lines()
                        .find_map(|line| {
                            line.to_ascii_lowercase()
                                .strip_prefix("range:")
                                .and_then(|r| r.trim().strip_prefix("bytes="))
                                .and_then(|r| r.trim_end_matches('-').parse::<usize>().ok())
                        })
                        .unwrap_or(0)
                        .min(body.len());
                    let end = if i == 0 {
                        15.min(body.len() - 1)
                    } else {
                        body.len() - 1
                    };
                    let payload = &body[start..=end];
                    let header = format!(
                        "HTTP/1.1 206 Partial Content\r\nContent-Type: application/octet-stream\r\nContent-Range: bytes {start}-{end}/64\r\nContent-Length: {}\r\nAccept-Ranges: bytes\r\nConnection: close\r\n\r\n",
                        payload.len()
                    );
                    let _ = stream.write_all(header.as_bytes());
                    let _ = stream.write_all(payload);
                }
            }
        });

        let runtime = tokio::runtime::Builder::new_current_thread()
            .enable_all()
            .build()
            .unwrap();
        let client = reqwest::Client::builder()
            .redirect(reqwest::redirect::Policy::none())
            .build()
            .unwrap();
        let url = Url::parse(&format!("http://{addr}/resource.bin")).unwrap();
        let (event_tx, event_rx) = crossbeam_channel::bounded(8);
        let (reply_tx, reply_rx) = std::sync::mpsc::channel();
        let cancel = Arc::new(AtomicBool::new(false));
        let job = StreamJob {
            client,
            spec: RequestSpec {
                url,
                extra_headers: Vec::new(),
                user_agent: None,
                header_timeout: Duration::from_secs(2),
                read_idle: Some(Duration::from_secs(2)),
                redirect_limit: 2,
                range_start: 0,
                send_range: true,
                if_range: None,
                generation: 0,
            },
            event_tx,
            reply_tx: Mutex::new(Some(reply_tx)),
            cancel,
            reconnect: ReconnectPolicy::default(),
            prior: None,
            learned_validator: empty_learned(),
        };
        runtime.block_on(run_job(job));
        let _ = reply_rx.recv_timeout(Duration::from_secs(2)).unwrap();
        let mut saw_changed = false;
        while let Ok(event) = event_rx.recv_timeout(Duration::from_millis(200)) {
            if matches!(
                event,
                BodyEvent::Error {
                    error: HttpInputError::ResourceChanged,
                    ..
                }
            ) {
                saw_changed = true;
            }
        }
        assert!(
            saw_changed,
            "short 206 without ETag/Last-Modified must fail closed as ResourceChanged"
        );
    }

    fn drain_events(rx: &Receiver<BodyEvent>) -> Vec<BodyEvent> {
        let mut events = Vec::new();
        while let Ok(event) = rx.recv_timeout(Duration::from_millis(200)) {
            events.push(event);
        }
        events
    }

    fn bind_job(
        range_start: u64,
        reconnect: ReconnectPolicy,
    ) -> (
        std::net::TcpListener,
        StreamJob,
        std::sync::mpsc::Receiver<Result<OpenMeta, HttpInputError>>,
        Receiver<BodyEvent>,
        tokio::runtime::Runtime,
    ) {
        let listener = std::net::TcpListener::bind("127.0.0.1:0").unwrap();
        let addr = listener.local_addr().unwrap();
        let runtime = tokio::runtime::Builder::new_current_thread()
            .enable_all()
            .build()
            .unwrap();
        let client = reqwest::Client::builder()
            .redirect(reqwest::redirect::Policy::none())
            .build()
            .unwrap();
        let url = Url::parse(&format!("http://{addr}/resource.bin")).unwrap();
        let (event_tx, event_rx) = crossbeam_channel::unbounded();
        let (reply_tx, reply_rx) = std::sync::mpsc::channel();
        let job = StreamJob {
            client,
            spec: RequestSpec {
                url,
                extra_headers: Vec::new(),
                user_agent: None,
                header_timeout: Duration::from_secs(2),
                read_idle: Some(Duration::from_secs(2)),
                redirect_limit: 2,
                range_start,
                send_range: true,
                if_range: None,
                generation: 0,
            },
            event_tx,
            reply_tx: Mutex::new(Some(reply_tx)),
            cancel: Arc::new(AtomicBool::new(false)),
            reconnect,
            prior: None,
            learned_validator: empty_learned(),
        };
        (listener, job, reply_rx, event_rx, runtime)
    }

    #[test]
    fn many_short_206_windows_concatenate_past_32() {
        use std::io::{Read, Write};
        use std::thread;

        let window = 16usize;
        let windows = 40usize;
        let body: Vec<u8> = (0..window * windows).map(|i| (i % 251) as u8).collect();
        let (listener, job, reply_rx, event_rx, runtime) = bind_job(0, ReconnectPolicy::default());
        let body_t = body.clone();
        thread::spawn(move || {
            listener.set_nonblocking(false).ok();
            for _ in 0..windows + 4 {
                let Ok((mut stream, _)) = listener.accept() else {
                    break;
                };
                let mut buf = [0u8; 2048];
                let n = stream.read(&mut buf).unwrap_or(0);
                let req = String::from_utf8_lossy(&buf[..n]);
                let start = req
                    .lines()
                    .find_map(|line| {
                        line.to_ascii_lowercase()
                            .strip_prefix("range:")
                            .and_then(|r| r.trim().strip_prefix("bytes="))
                            .and_then(|r| r.trim_end_matches('-').parse::<usize>().ok())
                    })
                    .unwrap_or(0)
                    .min(body_t.len());
                if start >= body_t.len() {
                    let header = format!(
                        "HTTP/1.1 416 Range Not Satisfiable\r\nContent-Range: bytes */{}\r\nContent-Length: 0\r\nConnection: close\r\n\r\n",
                        body_t.len()
                    );
                    let _ = stream.write_all(header.as_bytes());
                    continue;
                }
                let end = (start + window - 1).min(body_t.len() - 1);
                let payload = &body_t[start..=end];
                let header = format!(
                    "HTTP/1.1 206 Partial Content\r\nContent-Type: application/octet-stream\r\nContent-Range: bytes {start}-{end}/{}\r\nContent-Length: {}\r\nETag: \"w40\"\r\nAccept-Ranges: bytes\r\nConnection: close\r\n\r\n",
                    body_t.len(),
                    payload.len()
                );
                let _ = stream.write_all(header.as_bytes());
                let _ = stream.write_all(payload);
            }
        });

        runtime.block_on(run_job(job));
        let meta = reply_rx
            .recv_timeout(Duration::from_secs(5))
            .unwrap()
            .unwrap();
        assert_eq!(meta.size, Some(body.len() as u64));
        let mut copied = meta.prefix.to_vec();
        let mut saw_eof = false;
        let mut err = None;
        for event in drain_events(&event_rx) {
            match event {
                BodyEvent::Data { bytes, .. } => copied.extend_from_slice(&bytes),
                BodyEvent::Eof { .. } => saw_eof = true,
                BodyEvent::Error { error, .. } => err = Some(error),
            }
        }
        assert!(err.is_none(), "honest windows must not error: {err:?}");
        assert!(saw_eof, "completed windows must emit clean EOF");
        assert_eq!(copied, body);
    }

    #[test]
    fn unknown_total_206_is_truncated_not_eof() {
        use std::io::{Read, Write};
        use std::thread;

        let body: Vec<u8> = (0..64).collect();
        let (listener, job, reply_rx, event_rx, runtime) = bind_job(0, ReconnectPolicy::default());
        thread::spawn(move || {
            if let Ok((mut stream, _)) = listener.accept() {
                let mut buf = [0u8; 2048];
                let _ = stream.read(&mut buf);
                let header = "HTTP/1.1 206 Partial Content\r\nContent-Type: application/octet-stream\r\nContent-Range: bytes 0-63/*\r\nContent-Length: 64\r\nETag: \"star\"\r\nAccept-Ranges: bytes\r\nConnection: close\r\n\r\n";
                let _ = stream.write_all(header.as_bytes());
                let _ = stream.write_all(&body);
            }
        });

        runtime.block_on(run_job(job));
        let _ = reply_rx.recv_timeout(Duration::from_secs(2)).unwrap();
        let mut saw_truncated = false;
        let mut saw_eof = false;
        for event in drain_events(&event_rx) {
            match event {
                BodyEvent::Eof { .. } => saw_eof = true,
                BodyEvent::Error {
                    error: HttpInputError::TruncatedBody,
                    ..
                } => saw_truncated = true,
                _ => {}
            }
        }
        assert!(
            saw_truncated,
            "206 with unknown instance-length must emit TruncatedBody"
        );
        assert!(!saw_eof, "unknown-total 206 must not look like clean EOF");
    }

    #[test]
    fn content_encoding_identity_or_missing_passes() {
        let empty = HeaderMap::new();
        assert!(check_content_encoding(&empty).is_ok());
        for value in ["identity", "IDENTITY", ""] {
            let mut headers = HeaderMap::new();
            headers.insert(CONTENT_ENCODING, HeaderValue::from_str(value).unwrap());
            assert!(
                check_content_encoding(&headers).is_ok(),
                "Content-Encoding: {value:?} must pass"
            );
        }
    }

    #[test]
    fn content_encoding_compressed_is_rejected() {
        for value in ["gzip", "deflate", "br", "gzip, identity"] {
            let mut headers = HeaderMap::new();
            headers.insert(CONTENT_ENCODING, HeaderValue::from_str(value).unwrap());
            let err = check_content_encoding(&headers).unwrap_err();
            assert!(
                matches!(err, HttpInputError::UnsupportedContentEncoding),
                "Content-Encoding: {value:?} must be UnsupportedContentEncoding, got {err:?}"
            );
        }
    }

    #[test]
    fn later_response_validator_is_written_to_learned_slot() {
        use std::io::{Read, Write};
        use std::thread;

        let body: Vec<u8> = (0..32).collect();
        let (listener, job, reply_rx, _event_rx, runtime) = bind_job(0, ReconnectPolicy::default());
        let slot = Arc::clone(&job.learned_validator);
        thread::spawn(move || {
            if let Ok((mut stream, _)) = listener.accept() {
                let mut buf = [0u8; 2048];
                let _ = stream.read(&mut buf);
                let header = format!(
                    "HTTP/1.1 200 OK\r\nContent-Type: application/octet-stream\r\nContent-Length: {}\r\nETag: \"learned\"\r\nAccept-Ranges: bytes\r\nConnection: close\r\n\r\n",
                    body.len()
                );
                let _ = stream.write_all(header.as_bytes());
                let _ = stream.write_all(&body);
            }
        });

        assert!(slot.lock().unwrap().is_none(), "slot starts empty");
        runtime.block_on(run_job(job));
        let _ = reply_rx.recv_timeout(Duration::from_secs(2)).unwrap();
        assert_eq!(
            slot.lock().unwrap().as_deref(),
            Some("\"learned\""),
            "validator learned from a response must land in the shared slot"
        );
    }

    #[test]
    fn range_416_at_known_size_replies_eof() {
        use std::io::{Read, Write};
        use std::thread;

        let (listener, job, reply_rx, event_rx, runtime) = bind_job(64, ReconnectPolicy::default());
        thread::spawn(move || {
            if let Ok((mut stream, _)) = listener.accept() {
                let mut buf = [0u8; 2048];
                let n = stream.read(&mut buf).unwrap_or(0);
                let req = String::from_utf8_lossy(&buf[..n]);
                assert!(
                    req.to_ascii_lowercase().contains("range: bytes=64-"),
                    "{req}"
                );
                let header = "HTTP/1.1 416 Range Not Satisfiable\r\nContent-Range: bytes */64\r\nContent-Length: 0\r\nConnection: close\r\n\r\n";
                let _ = stream.write_all(header.as_bytes());
            }
        });

        runtime.block_on(run_job(job));
        let meta = reply_rx
            .recv_timeout(Duration::from_secs(2))
            .unwrap_or_else(|e| panic!("416 at size must reply OpenMeta, hung: {e}"))
            .unwrap_or_else(|e| panic!("416 at size must succeed, got {e}"));
        assert_eq!(meta.status, 416);
        assert_eq!(meta.size, Some(64));
        assert!(meta.prefix.is_empty());
        let mut saw_eof = false;
        let mut err = None;
        for event in drain_events(&event_rx) {
            match event {
                BodyEvent::Eof { .. } => saw_eof = true,
                BodyEvent::Error { error, .. } => err = Some(error),
                _ => {}
            }
        }
        assert!(err.is_none(), "416 at size must not error: {err:?}");
        assert!(saw_eof, "416 at size must emit clean EOF");
    }

    #[test]
    fn first_chunk_overflow_is_split_to_event_cap() {
        use std::io::{Read, Write};
        use std::thread;

        const EVENT_CAP: usize = 64 * 1024;
        let body = vec![0x40; 200 * 1024];
        let (listener, job, reply_rx, event_rx, runtime) = bind_job(0, ReconnectPolicy::default());
        let body_t = body.clone();
        thread::spawn(move || {
            if let Ok((mut stream, _)) = listener.accept() {
                let mut buf = [0u8; 2048];
                let _ = stream.read(&mut buf);
                let header = format!(
                    "HTTP/1.1 200 OK\r\nContent-Type: application/octet-stream\r\nContent-Length: {}\r\nConnection: close\r\n\r\n",
                    body_t.len()
                );
                let _ = stream.write_all(header.as_bytes());
                let _ = stream.write_all(&body_t);
            }
        });

        runtime.block_on(run_job(job));
        let _ = reply_rx
            .recv_timeout(Duration::from_secs(5))
            .unwrap_or_else(|e| panic!("overflow split must reply OpenMeta, hung: {e}"))
            .unwrap_or_else(|e| panic!("overflow split must succeed, got {e}"));
        for event in drain_events(&event_rx) {
            if let BodyEvent::Data { bytes, .. } = event {
                assert!(
                    bytes.len() <= EVENT_CAP,
                    "first-chunk overflow must honor the 64 KiB event cap, got {}",
                    bytes.len()
                );
            }
        }
    }

    #[test]
    fn send_event_yields_to_sibling_tasks() {
        use crossbeam_channel::bounded;

        let rt = tokio::runtime::Builder::new_current_thread()
            .enable_time()
            .build()
            .unwrap();
        rt.block_on(async {
            let (tx, rx) = bounded(1);
            let event = BodyEvent::Data {
                generation: 1,
                bytes: Bytes::from_static(b"x"),
            };
            tx.send(event.clone()).unwrap();
            let cancel = AtomicBool::new(false);
            let ping = async {
                tokio::time::sleep(Duration::from_millis(5)).await;
                true
            };
            let send = send_event(&tx, event, &cancel);
            let pinged = tokio::select! {
                biased;
                _ = send => panic!("full channel must not complete send_event before a sibling task runs"),
                done = ping => done,
            };
            assert!(pinged, "blocked send_event must yield to the current-thread runtime");
            drop(rx);
        });
    }
}
