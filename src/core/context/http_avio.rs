//! FFmpeg-facing HTTP AVIO helpers: read/seek closures and reject-only `io_open`.

use crate::core::context::InterruptState;
use crate::http_input::client::HttpClient;
use crate::http_input::config::ReconnectPolicy;
use crate::http_input::error::HttpInputError;
use crate::http_input::runtime::RuntimeHandle;
use crate::http_input::stream::{
    wait_event, wait_reply, BodyEvent, OpenMeta, RequestSpec, ResourceIdentity, StreamJob,
};
use bytes::Bytes;
use crossbeam_channel::{bounded, Receiver};
use ffmpeg_sys_next::{
    AVFormatContext, AVIOContext, AVERROR, AVSEEK_FORCE, AVSEEK_SIZE, EINVAL, ENOSYS, EPERM,
    SEEK_CUR, SEEK_END, SEEK_SET,
};
use std::ffi::{c_char, c_int};
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, Mutex};

const CHANNEL_CAP: usize = 8;

pub(crate) struct HttpAvioState {
    receiver: Receiver<BodyEvent>,
    remainder: Option<Bytes>,
    position: u64,
    size: Option<u64>,
    seekable: bool,
    generation: u64,
    cancel: Arc<AtomicBool>,
    client: HttpClient,
    runtime: RuntimeHandle,
    spec: RequestSpec,
    interrupt: Arc<InterruptState>,
    reconnect: ReconnectPolicy,
    at_eof: bool,
    failure: Arc<Mutex<Option<HttpInputError>>>,
    /// Validator learned by any StreamJob of this input (shared slot). The
    /// first response may omit an ETag while a later 206 window carries one;
    /// seeks fall back to this slot when `spec.if_range` is still `None`.
    learned_validator: Arc<Mutex<Option<String>>>,
}

impl Drop for HttpAvioState {
    fn drop(&mut self) {
        self.cancel.store(true, Ordering::Relaxed);
    }
}

pub(crate) fn lock_state(state: &Mutex<HttpAvioState>) -> std::sync::MutexGuard<'_, HttpAvioState> {
    state.lock().unwrap_or_else(|e| e.into_inner())
}

pub(crate) fn read(state: &Mutex<HttpAvioState>, buf: &mut [u8]) -> i32 {
    if buf.is_empty() {
        return 0;
    }
    {
        let mut guard = lock_state(state);
        if let Some(rem) = guard.remainder.take() {
            let copied = copy_bytes(&rem, buf);
            if copied < rem.len() {
                guard.remainder = Some(rem.slice(copied..));
            }
            guard.position = guard.position.saturating_add(copied as u64);
            return copied as i32;
        }
        if guard.at_eof {
            return ffmpeg_sys_next::AVERROR_EOF;
        }
    }
    let (rx, interrupt, generation, cancel) = {
        let guard = lock_state(state);
        (
            guard.receiver.clone(),
            Arc::clone(&guard.interrupt),
            guard.generation,
            Arc::clone(&guard.cancel),
        )
    };
    loop {
        match wait_event(&rx, &interrupt, &cancel) {
            Ok(BodyEvent::Data {
                generation: event_gen,
                bytes,
            }) => {
                if event_gen != generation {
                    continue;
                }
                let mut guard = lock_state(state);
                if guard.generation != event_gen {
                    continue;
                }
                let copied = copy_bytes(&bytes, buf);
                if copied < bytes.len() {
                    guard.remainder = Some(bytes.slice(copied..));
                }
                guard.position = guard.position.saturating_add(copied as u64);
                return copied as i32;
            }
            Ok(BodyEvent::Eof {
                generation: event_gen,
            }) => {
                if event_gen != generation {
                    continue;
                }
                lock_state(state).at_eof = true;
                return ffmpeg_sys_next::AVERROR_EOF;
            }
            Ok(BodyEvent::Error {
                generation: event_gen,
                error,
            }) => {
                if event_gen != generation {
                    continue;
                }
                {
                    let guard = lock_state(state);
                    record_failure(&guard.failure, error.clone());
                }
                return error.to_errno();
            }
            Err(err) => {
                if lock_state(state).at_eof {
                    return ffmpeg_sys_next::AVERROR_EOF;
                }
                {
                    let guard = lock_state(state);
                    record_failure(&guard.failure, err.clone());
                }
                return err.to_errno();
            }
        }
    }
}

pub(crate) fn seek(state: &Mutex<HttpAvioState>, offset: i64, whence: i32) -> i64 {
    let whence = whence & !AVSEEK_FORCE;
    if whence == AVSEEK_SIZE {
        let guard = lock_state(state);
        return match guard.size {
            Some(size) if size <= i64::MAX as u64 => size as i64,
            _ => AVERROR(ENOSYS) as i64,
        };
    }
    let target = {
        let guard = lock_state(state);
        if !guard.seekable {
            return AVERROR(ENOSYS) as i64;
        }
        match whence {
            SEEK_SET => offset,
            SEEK_CUR => match i64::try_from(guard.position) {
                Ok(pos) => match pos.checked_add(offset) {
                    Some(target) => target,
                    None => return AVERROR(EINVAL) as i64,
                },
                Err(_) => return AVERROR(EINVAL) as i64,
            },
            SEEK_END => match guard.size {
                Some(size) if size <= i64::MAX as u64 => match (size as i64).checked_add(offset) {
                    Some(target) => target,
                    None => return AVERROR(EINVAL) as i64,
                },
                _ => return AVERROR(ENOSYS) as i64,
            },
            _ => return AVERROR(EINVAL) as i64,
        }
    };
    if target < 0 {
        return AVERROR(EINVAL) as i64;
    }
    let target = target as u64;

    // Known-size EOF: POSIX/file and FFmpeg HTTP treat seek-to-size as a
    // successful end position. Do not issue `Range: bytes={size}-` (416).
    {
        let mut guard = lock_state(state);
        if let Some(size) = guard.size {
            if target > size {
                return AVERROR(EINVAL) as i64;
            }
            if target == size {
                guard.position = target;
                guard.remainder = None;
                guard.at_eof = true;
                guard.generation = guard.generation.wrapping_add(1);
                let old_cancel = Arc::clone(&guard.cancel);
                drop(guard);
                old_cancel.store(true, Ordering::Relaxed);
                return target as i64;
            }
        }
    }

    let (event_tx, event_rx) = bounded(CHANNEL_CAP);
    let cand_cancel = Arc::new(AtomicBool::new(false));
    let (reply_tx, reply_rx) = std::sync::mpsc::channel();
    let (runtime, client, spec, interrupt, old_cancel, reconnect, prior, learned) = {
        let guard = lock_state(state);
        let mut spec = guard.spec.clone();
        spec.range_start = target;
        spec.send_range = true;
        spec.generation = guard.generation.wrapping_add(1);
        if spec.if_range.is_none() {
            // The opening response may have had no validator while a later
            // 206 window carried one; use the learned slot so the seek can
            // still send If-Range and pass `require_validator`.
            spec.if_range = guard
                .learned_validator
                .lock()
                .unwrap_or_else(|e| e.into_inner())
                .clone();
        }
        let prior = Some(ResourceIdentity {
            url: spec.url.clone(),
            size: guard.size,
            validator: spec.if_range.clone(),
        });
        (
            guard.runtime.clone(),
            guard.client.clone(),
            spec,
            Arc::clone(&guard.interrupt),
            Arc::clone(&guard.cancel),
            guard.reconnect.clone(),
            prior,
            Arc::clone(&guard.learned_validator),
        )
    };

    if runtime
        .submit(StreamJob {
            client: client.inner.client.clone(),
            spec: spec.clone(),
            event_tx,
            reply_tx: Mutex::new(Some(reply_tx)),
            cancel: Arc::clone(&cand_cancel),
            reconnect,
            prior,
            learned_validator: learned,
        })
        .is_err()
    {
        cand_cancel.store(true, Ordering::Relaxed);
        return AVERROR(ffmpeg_sys_next::EIO) as i64;
    }

    let meta = match wait_reply(&reply_rx, &interrupt, &cand_cancel) {
        Ok(meta) => meta,
        Err(err) => {
            cand_cancel.store(true, Ordering::Relaxed);
            {
                let guard = lock_state(state);
                record_failure(&guard.failure, err.clone());
            }
            return err.to_errno() as i64;
        }
    };
    if !seek_commit_ok(&meta, target) {
        cand_cancel.store(true, Ordering::Relaxed);
        return HttpInputError::RangeIgnored.to_errno() as i64;
    }

    old_cancel.store(true, Ordering::Relaxed);
    let at_eof = meta.prefix.is_empty() && Some(target) == meta.size;
    let mut guard = lock_state(state);
    guard.receiver = event_rx;
    guard.remainder = if meta.prefix.is_empty() {
        None
    } else {
        Some(meta.prefix)
    };
    guard.position = target;
    if meta.size.is_some() {
        guard.size = meta.size;
    }
    guard.generation = spec.generation;
    guard.cancel = cand_cancel;
    guard.spec = spec;
    guard.at_eof = at_eof;
    target as i64
}

fn seek_commit_ok(meta: &OpenMeta, target: u64) -> bool {
    if meta.status == 206 {
        return true;
    }
    if meta.status == 200 && target == 0 {
        return true;
    }
    // RFC 7233 416 with `bytes */N` at target N is a successful EOF position.
    meta.status == 416 && meta.size == Some(target)
}

fn copy_bytes(src: &Bytes, dst: &mut [u8]) -> usize {
    let n = src.len().min(dst.len());
    dst[..n].copy_from_slice(&src[..n]);
    n
}

fn record_failure(slot: &Mutex<Option<HttpInputError>>, err: HttpInputError) {
    let mut guard = slot.lock().unwrap_or_else(|e| e.into_inner());
    if guard.is_none() {
        *guard = Some(err);
    }
}

// The constructor mirrors HttpAvioState's fields one-to-one; a params struct
// would just duplicate the state struct it builds.
#[allow(clippy::too_many_arguments)]
pub(crate) fn new_state(
    receiver: Receiver<BodyEvent>,
    remainder: Option<Bytes>,
    size: Option<u64>,
    seekable: bool,
    cancel: Arc<AtomicBool>,
    client: HttpClient,
    runtime: RuntimeHandle,
    spec: RequestSpec,
    interrupt: Arc<InterruptState>,
    reconnect: ReconnectPolicy,
    failure: Arc<Mutex<Option<HttpInputError>>>,
    learned_validator: Arc<Mutex<Option<String>>>,
) -> HttpAvioState {
    HttpAvioState {
        receiver,
        remainder,
        position: 0,
        size,
        seekable,
        generation: spec.generation,
        cancel,
        client,
        runtime,
        spec,
        interrupt,
        reconnect,
        at_eof: false,
        failure,
        learned_validator,
    }
}

/// Reject every nested `io_open`. FFmpeg 8.1 HLS still fails earlier when
/// HTTPS is not a registered protocol; this is the last-line contract.
pub(crate) unsafe extern "C" fn reject_nested_io_open(
    format: *mut AVFormatContext,
    _pb: *mut *mut AVIOContext,
    _url: *const c_char,
    _flags: c_int,
    _options: *mut *mut ffmpeg_sys_next::AVDictionary,
) -> c_int {
    if !format.is_null() {
        crate::core::context::record_http_input_failure(
            (*format).pb,
            HttpInputError::NestedResourceUnsupported,
        );
    }
    AVERROR(EPERM)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::http_input::runtime::RuntimeHandle;
    use reqwest::Url;
    use std::sync::atomic::AtomicUsize;
    use std::time::Duration;

    fn dummy_spec() -> RequestSpec {
        RequestSpec {
            url: Url::parse("http://127.0.0.1/resource.bin").unwrap(),
            extra_headers: Vec::new(),
            user_agent: None,
            header_timeout: Duration::from_secs(2),
            read_idle: Some(Duration::from_secs(2)),
            redirect_limit: 2,
            range_start: 0,
            send_range: true,
            if_range: None,
            generation: 0,
        }
    }

    fn dummy_state(size: Option<u64>, seekable: bool) -> Mutex<HttpAvioState> {
        let (_tx, rx) = bounded(CHANNEL_CAP);
        let client = HttpClient::builder().build().unwrap();
        let runtime = RuntimeHandle::start().unwrap();
        Mutex::new(new_state(
            rx,
            None,
            size,
            seekable,
            Arc::new(AtomicBool::new(false)),
            client,
            runtime,
            dummy_spec(),
            Arc::new(InterruptState::new(Arc::new(AtomicUsize::new(0)))),
            ReconnectPolicy::default(),
            Arc::new(Mutex::new(None)),
            Arc::new(Mutex::new(None)),
        ))
    }

    #[test]
    fn seek_to_known_size_is_eof_without_range_request() {
        let state = dummy_state(Some(100), true);
        let ret = seek(&state, 100, SEEK_SET);
        assert_eq!(ret, 100, "seek to size must succeed");
        {
            let guard = lock_state(&state);
            assert!(guard.at_eof);
            assert_eq!(guard.position, 100);
        }
        let n = read(&state, &mut [0u8; 8]);
        assert_eq!(n, ffmpeg_sys_next::AVERROR_EOF);

        let end = seek(&state, 0, SEEK_END);
        assert_eq!(end, 100, "SEEK_END 0 must land on size");
        assert_eq!(read(&state, &mut [0u8; 8]), ffmpeg_sys_next::AVERROR_EOF);

        let past = seek(&state, 101, SEEK_SET);
        assert!(past < 0, "seek past known size must fail, got {past}");
    }

    #[test]
    fn seek_commit_ok_accepts_416_at_size() {
        let meta = OpenMeta {
            final_url: Url::parse("http://127.0.0.1/resource.bin").unwrap(),
            status: 416,
            size: Some(2048),
            seekable: true,
            content_type: None,
            validator: None,
            range_end: None,
            prefix: Bytes::new(),
        };
        assert!(seek_commit_ok(&meta, 2048));
        assert!(!seek_commit_ok(&meta, 2047));
    }

    #[test]
    fn seek_overflow_returns_einval() {
        let state = dummy_state(Some(100), true);
        {
            let mut guard = lock_state(&state);
            guard.position = 50;
        }
        let cur = seek(&state, i64::MAX, SEEK_CUR);
        assert_eq!(
            cur,
            AVERROR(EINVAL) as i64,
            "SEEK_CUR overflow must be EINVAL, got {cur}"
        );
        let end = seek(&state, i64::MAX, SEEK_END);
        assert_eq!(
            end,
            AVERROR(EINVAL) as i64,
            "SEEK_END overflow must be EINVAL, got {end}"
        );
    }
}
