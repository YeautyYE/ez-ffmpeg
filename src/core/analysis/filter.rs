//! The reusable [`MetadataEventFilter`] and its event-sink plumbing.
//!
//! [`MetadataEventFilter`] is a passthrough [`FrameFilter`]: it reads each
//! frame's `lavfi.*` metadata into [`MetadataEvent`]s, optionally runs the
//! native Rust crop scanner on the same frame, pushes events to an
//! [`EventSink`], and returns the frame unchanged (the `NoopFilter` pattern).
//! Mount it on an output frame pipeline downstream of the detector filters.

use crate::core::analysis::crop::{CropDetectionOptions, CropObservation, CropScanner};
use crate::core::analysis::event::{parse_frame_metadata, MetadataEvent, ParseState, Timestamp};
use crate::core::filter::frame_filter::{FrameFilter, FrameFilterError, RequestFrameMode};
use crate::core::filter::frame_filter_context::FrameFilterContext;
use ffmpeg_next::Frame;
use ffmpeg_sys_next::{AVMediaType, AV_NOPTS_VALUE};
use std::panic::{catch_unwind, AssertUnwindSafe};
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

/// A passthrough [`FrameFilter`] that surfaces detector metadata as events
/// and can run native Rust crop detection on the same video frames.
pub struct MetadataEventFilter {
    media_type: AVMediaType,
    sink: Box<dyn EventSink>,
    policy: BackpressurePolicy,
    ignore_disconnected: bool,
    state: ParseState,
    /// Reused per-frame event buffer. Event-carrying frames (every audio
    /// frame under ebur128, every video frame under native crop) would
    /// otherwise allocate and free a fresh `Vec` on each `filter_frame`.
    events_scratch: Vec<MetadataEvent>,
    crop_options: Option<CropDetectionOptions>,
    crop: Option<CropScanner>,
    crop_observer: Option<Box<dyn FnMut(CropObservation) + Send>>,
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
            events_scratch: Vec::new(),
            crop_options: None,
            crop: None,
            crop_observer: None,
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

    /// Enable native crop / letterbox detection on this filter.
    ///
    /// Must be used on a video filter; `init` returns an error if the media
    /// type is not video.
    pub fn with_crop_detection(mut self, options: CropDetectionOptions) -> Self {
        self.crop_options = Some(options);
        self
    }

    /// Receive raw + aligned crop observations (same values as the published
    /// [`MetadataEvent::CropDetect`], plus half-open raw bounds).
    ///
    /// The observer is invoked only after a stable rectangle is published.
    /// Panics in the observer are caught and discarded so they cannot unwind
    /// through the media pipeline.
    pub fn with_crop_observer(
        mut self,
        observer: impl FnMut(CropObservation) + Send + 'static,
    ) -> Self {
        self.crop_observer = Some(Box::new(observer));
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

    fn init(&mut self, ctx: &mut FrameFilterContext) -> Result<(), FrameFilterError> {
        if let Some(options) = self.crop_options.take() {
            if self.media_type != ffmpeg_sys_next::AVMediaType::AVMEDIA_TYPE_VIDEO {
                return Err(
                    "crop detection can only be attached to a video MetadataEventFilter".into(),
                );
            }
            self.crop =
                Some(CropScanner::new(options).map_err(|e| -> FrameFilterError { Box::new(e) })?);
        }
        if self.crop_observer.is_some() && self.crop.is_none() {
            return Err(
                "with_crop_observer requires with_crop_detection on the same filter".into(),
            );
        }
        log::debug!("Initializing filter:{}", ctx.name());
        Ok(())
    }

    fn filter_frame(
        &mut self,
        frame: Frame,
        _ctx: &mut FrameFilterContext,
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

        // Reuse the scratch buffer across frames: take it out of `self` so the
        // dispatch loop below can borrow `self` mutably, then hand it back with
        // its capacity intact. `Drain` removes every element even when the loop
        // stops early on a dispatch error, matching the old drop-on-error.
        let mut events = std::mem::take(&mut self.events_scratch);
        {
            let md = frame.metadata();
            parse_frame_metadata(
                &md,
                frame_ts,
                self.media_type,
                &mut events,
                &mut self.state,
                self.crop.is_some(),
            );
        }
        let scene_changed = events
            .iter()
            .any(|ev| matches!(ev, MetadataEvent::SceneChange { .. }));
        let crop_out = if let Some(scanner) = self.crop.as_mut() {
            scanner
                .process_frame(&frame, frame_ts, scene_changed)
                .map_err(|e| -> FrameFilterError { Box::new(e) })?
        } else {
            None
        };
        if let Some((sug, obs)) = crop_out {
            events.push(MetadataEvent::CropDetect {
                at: obs.at,
                x: sug.x,
                y: sug.y,
                w: sug.w,
                h: sug.h,
            });
            if let Some(observer) = self.crop_observer.as_mut() {
                if let Err(payload) = catch_unwind(AssertUnwindSafe(|| observer(obs))) {
                    crate::core::packet_sink::dispose_panic_payload(payload);
                }
            }
        }
        if let Some(end) = frame_end_ts {
            self.state.record_frame_end(end);
        }
        let result = events.drain(..).try_for_each(|ev| self.dispatch(ev));
        self.events_scratch = events;
        result?;
        Ok(Some(frame)) // passthrough (NoopFilter pattern)
    }

    fn uninit(&mut self, _ctx: &mut FrameFilterContext) {
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
