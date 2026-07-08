//! The reusable [`MetadataEventFilter`] and its event-sink plumbing.
//!
//! [`MetadataEventFilter`] is a passthrough [`FrameFilter`]: it reads each
//! frame's `lavfi.*` metadata into [`MetadataEvent`]s, pushes them to an
//! [`EventSink`], and returns the frame unchanged (the `NoopFilter` pattern).
//! Mount it on an output frame pipeline downstream of the detector filters.

use crate::core::analysis::event::{parse_frame_metadata, MetadataEvent, ParseState, Timestamp};
use crate::core::filter::frame_filter::{FrameFilter, FrameFilterError, RequestFrameMode};
use crate::core::filter::frame_filter_context::FrameFilterContext;
use ffmpeg_next::Frame;
use ffmpeg_sys_next::{AVMediaType, AV_NOPTS_VALUE};
use std::sync::mpsc::{SyncSender, TrySendError};
use std::time::Duration;

/// What to do when a bounded sink cannot accept an event immediately.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub enum BackpressurePolicy {
    /// Block the media pipeline until the sink drains. Requires a concurrent
    /// consumer draining the sink, or the pipeline deadlocks.
    Block,
    /// Silently discard the event and keep going.
    Drop,
    /// Abort the run by returning an error from `filter_frame`.
    #[default]
    Error,
}

/// Why an [`EventSink`] rejected an event.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum SinkError {
    /// A bounded sink is momentarily full.
    Full,
    /// The receiving end is gone; further emits are pointless.
    Disconnected,
}

/// A destination for [`MetadataEvent`]s.
///
/// [`try_emit`](EventSink::try_emit) is non-blocking. The default
/// [`emit_blocking`](EventSink::emit_blocking) parks and retries on
/// [`SinkError::Full`]; sinks with a native blocking send (e.g.
/// [`SyncSender`]) should override it.
pub trait EventSink: Send {
    /// Non-blocking emit. Returns [`SinkError::Full`] if a bounded sink is full
    /// or [`SinkError::Disconnected`] if the receiver is gone.
    fn try_emit(&mut self, ev: MetadataEvent) -> Result<(), SinkError>;

    /// Blocking emit: retries until the event is accepted or the sink
    /// disconnects. The default parks briefly between attempts.
    fn emit_blocking(&mut self, ev: MetadataEvent) -> Result<(), SinkError> {
        loop {
            match self.try_emit(ev.clone()) {
                Ok(()) => return Ok(()),
                Err(SinkError::Full) => std::thread::sleep(Duration::from_millis(1)),
                Err(SinkError::Disconnected) => return Err(SinkError::Disconnected),
            }
        }
    }
}

impl EventSink for SyncSender<MetadataEvent> {
    fn try_emit(&mut self, ev: MetadataEvent) -> Result<(), SinkError> {
        match self.try_send(ev) {
            Ok(()) => Ok(()),
            Err(TrySendError::Full(_)) => Err(SinkError::Full),
            Err(TrySendError::Disconnected(_)) => Err(SinkError::Disconnected),
        }
    }

    fn emit_blocking(&mut self, ev: MetadataEvent) -> Result<(), SinkError> {
        self.send(ev).map_err(|_| SinkError::Disconnected)
    }
}

impl<F: FnMut(MetadataEvent) + Send> EventSink for F {
    fn try_emit(&mut self, ev: MetadataEvent) -> Result<(), SinkError> {
        self(ev);
        Ok(())
    }
}

/// A passthrough [`FrameFilter`] that surfaces detector metadata as events.
pub struct MetadataEventFilter {
    media_type: AVMediaType,
    sink: Box<dyn EventSink>,
    policy: BackpressurePolicy,
    ignore_disconnected: bool,
    state: ParseState,
}

impl MetadataEventFilter {
    /// Creates a filter for `media_type` feeding `sink`. Defaults to
    /// [`BackpressurePolicy::Error`] (so a stuck sink never deadlocks the
    /// media worker) and treats [`SinkError::Disconnected`] as fatal.
    pub fn new(media_type: AVMediaType, sink: impl EventSink + 'static) -> Self {
        Self {
            media_type,
            sink: Box::new(sink),
            policy: BackpressurePolicy::Error,
            ignore_disconnected: false,
            state: ParseState::default(),
        }
    }

    /// Sets the backpressure policy.
    pub fn backpressure(mut self, policy: BackpressurePolicy) -> Self {
        self.policy = policy;
        self
    }

    /// When `true`, a disconnected sink stops event delivery but lets the run
    /// continue instead of aborting. [`BackpressurePolicy::Drop`] implies this.
    pub fn ignore_disconnected(mut self, ignore: bool) -> Self {
        self.ignore_disconnected = ignore;
        self
    }

    /// Delivers one event according to the backpressure policy, mapping sink
    /// failures to an error string only when they should abort the run.
    fn dispatch(&mut self, ev: MetadataEvent) -> Result<(), String> {
        let result = match self.policy {
            BackpressurePolicy::Block => self.sink.emit_blocking(ev),
            BackpressurePolicy::Drop => match self.sink.try_emit(ev) {
                Ok(()) | Err(SinkError::Full) => Ok(()),
                Err(SinkError::Disconnected) => Err(SinkError::Disconnected),
            },
            BackpressurePolicy::Error => self.sink.try_emit(ev),
        };
        match result {
            Ok(()) => Ok(()),
            Err(SinkError::Full) => {
                Err("event sink is full (BackpressurePolicy::Error)".to_string())
            }
            Err(SinkError::Disconnected) => {
                if self.ignore_disconnected || self.policy == BackpressurePolicy::Drop {
                    Ok(())
                } else {
                    Err("event sink disconnected".to_string())
                }
            }
        }
    }
}

impl FrameFilter for MetadataEventFilter {
    fn media_type(&self) -> AVMediaType {
        self.media_type
    }

    fn request_frame_mode(&self) -> RequestFrameMode {
        // Pure metadata tap: transforms input, never generates frames (PERF-8).
        RequestFrameMode::Never
    }

    fn filter_frame(
        &mut self,
        frame: Frame,
        _ctx: &FrameFilterContext,
    ) -> Result<Option<Frame>, FrameFilterError> {
        // Props-only / null frames pass straight through (matches SubtitleFilter),
        // and never yield Ok(None), which would starve downstream consumers.
        // SAFETY: the returned raw pointer is null-checked before any deref.
        let p = unsafe { frame.as_ptr() };
        if p.is_null() {
            return Ok(Some(frame));
        }

        // Per-frame r128/cropdetect events need a precise timestamp from the
        // raw frame; metadata-derived events (black/silence/scd) do not. We also
        // derive the frame's END (pts + duration) so end-of-stream regions are
        // closed at the true stream end, not the last frame's start.
        // SAFETY: `p` is non-null here; fields are plain integers.
        let (frame_ts, frame_end_ts) = unsafe {
            let pts = (*p).pts;
            let tb = (*p).time_base;
            if pts == AV_NOPTS_VALUE || tb.den == 0 {
                (None, None)
            } else {
                let tb = (tb.num, tb.den);
                let start = Timestamp::from_pts(pts, tb);
                let dur = (*p).duration;
                let end = if dur > 0 {
                    Timestamp::from_pts(pts.saturating_add(dur), tb)
                } else {
                    start
                };
                (Some(start), Some(end))
            }
        };

        let mut events = Vec::new();
        {
            let md = frame.metadata();
            parse_frame_metadata(&md, frame_ts, self.media_type, &mut events, &mut self.state);
        }
        if let Some(end) = frame_end_ts {
            self.state.record_frame_end(end);
        }
        for ev in events {
            self.dispatch(ev)?;
        }
        Ok(Some(frame)) // passthrough (NoopFilter pattern)
    }

    fn uninit(&mut self, _ctx: &FrameFilterContext) {
        // Best-effort end-of-stream flush: `uninit` returns `()`, so we cannot
        // abort on failure. But under `Block` we still wait for room so the
        // final `R128Summary` / `StreamEnd` (the actual measurement result) is
        // not silently dropped when the bounded channel happens to be full at
        // EOF.
        for ev in self.state.flush(self.media_type) {
            let result = match self.policy {
                BackpressurePolicy::Block => self.sink.emit_blocking(ev),
                _ => self.sink.try_emit(ev),
            };
            if let Err(e) = result {
                log::debug!("MetadataEventFilter: dropped end-of-stream event: {e:?}");
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use ffmpeg_sys_next::AVMediaType::AVMEDIA_TYPE_VIDEO;
    use std::sync::mpsc::sync_channel;

    fn event() -> MetadataEvent {
        MetadataEvent::BlackStart {
            at: Timestamp::from_secs(1.0).unwrap(),
        }
    }

    enum Mode {
        Full,
        Disconnected,
    }

    struct MockSink(Mode);
    impl EventSink for MockSink {
        fn try_emit(&mut self, _ev: MetadataEvent) -> Result<(), SinkError> {
            match self.0 {
                Mode::Full => Err(SinkError::Full),
                Mode::Disconnected => Err(SinkError::Disconnected),
            }
        }
    }

    fn filter(mode: Mode, policy: BackpressurePolicy) -> MetadataEventFilter {
        MetadataEventFilter::new(AVMEDIA_TYPE_VIDEO, MockSink(mode)).backpressure(policy)
    }

    #[test]
    fn sync_sender_reports_full_and_disconnected() {
        let (mut tx, rx) = sync_channel::<MetadataEvent>(1);
        assert_eq!(tx.try_emit(event()), Ok(()));
        assert_eq!(tx.try_emit(event()), Err(SinkError::Full));
        drop(rx);
        assert_eq!(tx.emit_blocking(event()), Err(SinkError::Disconnected));
    }

    #[test]
    fn drop_policy_swallows_full_and_disconnect() {
        assert!(filter(Mode::Full, BackpressurePolicy::Drop)
            .dispatch(event())
            .is_ok());
        assert!(filter(Mode::Disconnected, BackpressurePolicy::Drop)
            .dispatch(event())
            .is_ok());
    }

    #[test]
    fn error_policy_aborts_on_full() {
        assert!(filter(Mode::Full, BackpressurePolicy::Error)
            .dispatch(event())
            .is_err());
    }

    #[test]
    fn disconnected_is_fatal_unless_ignored() {
        assert!(filter(Mode::Disconnected, BackpressurePolicy::Error)
            .dispatch(event())
            .is_err());
        let mut f = MetadataEventFilter::new(AVMEDIA_TYPE_VIDEO, MockSink(Mode::Disconnected))
            .ignore_disconnected(true);
        assert!(f.dispatch(event()).is_ok());
    }
}
