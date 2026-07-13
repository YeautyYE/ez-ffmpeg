//! Folded analysis report and the streaming event folder.
//!
//! [`fold_event`] folds each [`MetadataEvent`] into a running [`FoldState`] as it
//! arrives — collapsing the stream into ranges and summaries without buffering the
//! per-frame events. [`finalize`] then closes regions left open at end-of-stream,
//! using the detector's `min_duration` config (via [`FoldConfig`]) to drop
//! truncated tails shorter than the configured minimum.

use crate::core::analysis::event::MetadataEvent;

/// A detected black region, in microseconds.
#[derive(Debug, Clone, Copy, PartialEq)]
pub struct BlackRange {
    pub start_us: i64,
    pub end_us: i64,
}

/// A detected silent region, in microseconds. `channel` is the 1-based channel
/// number in `mono` mode, or `None` for combined detection.
#[derive(Debug, Clone, Copy, PartialEq)]
pub struct SilenceRange {
    pub start_us: i64,
    pub end_us: i64,
    pub channel: Option<usize>,
}

/// A detected scene change at `at_us`, with the `scdet` score.
#[derive(Debug, Clone, Copy, PartialEq)]
pub struct SceneChange {
    pub at_us: i64,
    pub score: f64,
}

/// A suggested crop rectangle (last stable `cropdetect` value).
#[derive(Debug, Clone, Copy, PartialEq)]
pub struct CropSuggestion {
    pub x: i32,
    pub y: i32,
    pub w: i32,
    pub h: i32,
}

/// EBU R128 loudness summary. All fields are `Option`: loudness metadata may be
/// missing, and `true_peak` is only present when `peak=true` was requested and
/// the build emitted the keys.
#[derive(Debug, Clone, Copy, PartialEq)]
pub struct LoudnessReport {
    pub integrated: Option<f64>,
    pub lra: Option<f64>,
    pub true_peak: Option<f64>,
}

/// The complete folded result of an [`Analysis`](crate::core::analysis::Analysis) run.
#[derive(Debug, Clone, PartialEq, Default)]
pub struct AnalysisReport {
    pub black: Vec<BlackRange>,
    pub silence: Vec<SilenceRange>,
    pub scenes: Vec<SceneChange>,
    pub crop: Option<CropSuggestion>,
    pub loudness: Option<LoudnessReport>,
}

/// Minimum-duration thresholds (microseconds) used to discard end-of-stream
/// truncated tails. `None` means "no detector of that kind", so no filtering.
#[derive(Debug, Clone, Copy, Default)]
pub(crate) struct FoldConfig {
    pub black_min_duration_us: Option<i64>,
    pub silence_min_duration_us: Option<i64>,
}

/// Running state of a streaming fold: the report accumulated so far plus the
/// cross-event bookkeeping needed to close ranges. Folding each event AS it
/// arrives (rather than buffering every per-frame `MetadataEvent` and folding at
/// the end) keeps analysis memory bounded by the number of DETECTED features, not
/// the media duration — a long live input used to grow the event buffer without
/// bound, since per-frame events the report discards were still retained.
#[derive(Default)]
pub(crate) struct FoldState {
    report: AnalysisReport,
    pending_black: Option<i64>,
    pending_silence: Vec<(Option<usize>, i64)>,
    video_end_us: Option<i64>,
    audio_end_us: Option<i64>,
}

#[cfg(test)]
impl FoldState {
    /// Test-only window into the incrementally folded report: lets sink
    /// tests assert an event is folded the moment it arrives (rather than
    /// buffered until finalize) without consuming the state.
    pub(crate) fn report_so_far(&self) -> &AnalysisReport {
        &self.report
    }
}

/// Folds a single event into the running state. Per-frame events the report does
/// not retain (`R128Frame`) are dropped here instead of being buffered.
pub(crate) fn fold_event(state: &mut FoldState, ev: MetadataEvent) {
    match ev {
        MetadataEvent::BlackStart { at } => state.pending_black = Some(at.time_us),
        MetadataEvent::BlackEnd { at, .. } => {
            if let Some(start) = state.pending_black.take() {
                state.report.black.push(BlackRange {
                    start_us: start,
                    end_us: at.time_us,
                });
            }
        }
        MetadataEvent::SilenceStart { at, channel_number } => {
            state.pending_silence.push((channel_number, at.time_us));
        }
        MetadataEvent::SilenceEnd {
            at, channel_number, ..
        } => {
            if let Some(pos) = state
                .pending_silence
                .iter()
                .position(|(c, _)| *c == channel_number)
            {
                let (_, start) = state.pending_silence.remove(pos);
                state.report.silence.push(SilenceRange {
                    start_us: start,
                    end_us: at.time_us,
                    channel: channel_number,
                });
            }
        }
        MetadataEvent::SceneChange { at, score } => state.report.scenes.push(SceneChange {
            at_us: at.time_us,
            score,
        }),
        MetadataEvent::CropDetect { x, y, w, h, .. } => {
            state.report.crop = Some(CropSuggestion { x, y, w, h });
        }
        MetadataEvent::R128Summary {
            integrated,
            lra,
            true_peak,
        } => {
            state.report.loudness = Some(LoudnessReport {
                integrated,
                lra,
                true_peak,
            });
        }
        MetadataEvent::StreamEnd { at, media } => {
            // Close black regions with the video stream's end and silence
            // regions with the audio stream's end, so mismatched stream
            // durations don't skew the trailing region.
            let slot = if media == ffmpeg_sys_next::AVMediaType::AVMEDIA_TYPE_AUDIO {
                &mut state.audio_end_us
            } else {
                &mut state.video_end_us
            };
            *slot = Some(slot.map_or(at.time_us, |e| e.max(at.time_us)));
        }
        MetadataEvent::R128Frame { .. } => {}
    }
}

/// Closes any still-open ranges at the stream ends and returns the report.
pub(crate) fn finalize(mut state: FoldState, cfg: &FoldConfig) -> AnalysisReport {
    // Close a still-open black region at the video stream's end-of-stream.
    if let (Some(start), Some(end)) = (state.pending_black, state.video_end_us) {
        if keep_tail(start, end, cfg.black_min_duration_us) {
            state.report.black.push(BlackRange {
                start_us: start,
                end_us: end,
            });
        }
    }
    // Close still-open silence regions at the audio stream's end-of-stream.
    if let Some(end) = state.audio_end_us {
        for (channel, start) in state.pending_silence {
            if keep_tail(start, end, cfg.silence_min_duration_us) {
                state.report.silence.push(SilenceRange {
                    start_us: start,
                    end_us: end,
                    channel,
                });
            }
        }
    }

    state.report
}

/// Folds a whole event vector into a report (the streaming `fold_event` +
/// `finalize` applied in sequence). Test-only: the live run folds incrementally to
/// bound memory, so this batch form exists only for the event-list tests below.
#[cfg(test)]
pub(crate) fn fold(events: Vec<MetadataEvent>, cfg: &FoldConfig) -> AnalysisReport {
    let mut state = FoldState::default();
    for ev in events {
        fold_event(&mut state, ev);
    }
    finalize(state, cfg)
}

fn keep_tail(start_us: i64, end_us: i64, min_duration_us: Option<i64>) -> bool {
    let duration = end_us.saturating_sub(start_us);
    duration >= 0 && min_duration_us.is_none_or(|min| duration >= min)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::core::analysis::event::Timestamp;
    use ffmpeg_sys_next::AVMediaType::AVMEDIA_TYPE_VIDEO;

    fn ts(us: i64) -> Timestamp {
        Timestamp {
            time_us: us,
            pts: None,
            time_base: None,
        }
    }

    #[test]
    fn pairs_black_start_end_into_range() {
        let events = vec![
            MetadataEvent::BlackStart { at: ts(1_000_000) },
            MetadataEvent::BlackEnd {
                at: ts(3_000_000),
                duration_us: 2_000_000,
            },
        ];
        let report = fold(events, &FoldConfig::default());
        assert_eq!(
            report.black,
            vec![BlackRange {
                start_us: 1_000_000,
                end_us: 3_000_000
            }]
        );
    }

    // The streaming fold drops per-frame R128Frame events instead of buffering
    // them (the point of the change: analysis memory is bounded by detected
    // features, not frame count). A flood of R128Frame must not affect the report,
    // and folding incrementally via fold_event/finalize must produce the right one.
    #[test]
    fn streaming_fold_drops_per_frame_r128_events() {
        let mut state = FoldState::default();
        for i in 0..10_000 {
            fold_event(
                &mut state,
                MetadataEvent::R128Frame {
                    at: ts(i),
                    momentary: Some(-20.0),
                    short_term: Some(-20.0),
                    integrated: Some(-23.0),
                    lra: Some(1.0),
                    true_peak: Some(-1.0),
                },
            );
        }
        // Real events still fold normally after the flood.
        fold_event(&mut state, MetadataEvent::BlackStart { at: ts(1_000_000) });
        fold_event(
            &mut state,
            MetadataEvent::BlackEnd {
                at: ts(2_000_000),
                duration_us: 1_000_000,
            },
        );
        fold_event(
            &mut state,
            MetadataEvent::R128Summary {
                integrated: Some(-23.0),
                lra: Some(1.0),
                true_peak: Some(-1.0),
            },
        );
        let report = finalize(state, &FoldConfig::default());

        assert!(report.loudness.is_some(), "the summary must be retained");
        assert_eq!(report.black.len(), 1, "the black range must be retained");
        assert!(
            report.scenes.is_empty() && report.silence.is_empty(),
            "per-frame R128 events must not leak into the report"
        );
    }

    #[test]
    fn unpaired_start_closed_at_stream_end() {
        let events = vec![
            MetadataEvent::BlackStart { at: ts(1_000_000) },
            MetadataEvent::StreamEnd {
                media: AVMEDIA_TYPE_VIDEO,
                at: ts(5_000_000),
            },
        ];
        let report = fold(events, &FoldConfig::default());
        assert_eq!(
            report.black,
            vec![BlackRange {
                start_us: 1_000_000,
                end_us: 5_000_000
            }]
        );
    }

    #[test]
    fn short_tail_dropped_when_below_min_duration() {
        let cfg = FoldConfig {
            black_min_duration_us: Some(2_000_000),
            silence_min_duration_us: None,
        };
        // Tail is only 0.5s, below the 2s minimum -> dropped.
        let events = vec![
            MetadataEvent::BlackStart { at: ts(4_500_000) },
            MetadataEvent::StreamEnd {
                media: AVMEDIA_TYPE_VIDEO,
                at: ts(5_000_000),
            },
        ];
        assert!(fold(events, &cfg).black.is_empty());
    }

    #[test]
    fn crop_takes_last_value() {
        let events = vec![
            MetadataEvent::CropDetect {
                at: ts(0),
                x: 0,
                y: 0,
                w: 100,
                h: 100,
            },
            MetadataEvent::CropDetect {
                at: ts(1),
                x: 0,
                y: 10,
                w: 100,
                h: 80,
            },
        ];
        let report = fold(events, &FoldConfig::default());
        assert_eq!(
            report.crop,
            Some(CropSuggestion {
                x: 0,
                y: 10,
                w: 100,
                h: 80
            })
        );
    }
}
