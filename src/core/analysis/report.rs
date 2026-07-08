//! Folded analysis report and the pure event-folding function.
//!
//! [`fold`] collapses the flat [`MetadataEvent`] stream produced by a run into
//! ranges and summaries. It is an online folder that also holds the detector's
//! `min_duration` config (via [`FoldConfig`]) so it can close regions left open
//! at end-of-stream and drop truncated tails shorter than the configured
//! minimum.

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

/// Folds a flat event stream into an [`AnalysisReport`].
///
/// Start/End events are paired into ranges; a start with no matching end is
/// closed at the last-seen stream timestamp ([`MetadataEvent::StreamEnd`]) and
/// kept only if the resulting duration meets the configured minimum.
pub(crate) fn fold(events: Vec<MetadataEvent>, cfg: &FoldConfig) -> AnalysisReport {
    let mut report = AnalysisReport::default();
    let mut pending_black: Option<i64> = None;
    let mut pending_silence: Vec<(Option<usize>, i64)> = Vec::new();
    let mut video_end_us: Option<i64> = None;
    let mut audio_end_us: Option<i64> = None;

    for ev in events {
        match ev {
            MetadataEvent::BlackStart { at } => pending_black = Some(at.time_us),
            MetadataEvent::BlackEnd { at, .. } => {
                if let Some(start) = pending_black.take() {
                    report.black.push(BlackRange {
                        start_us: start,
                        end_us: at.time_us,
                    });
                }
            }
            MetadataEvent::SilenceStart { at, channel_number } => {
                pending_silence.push((channel_number, at.time_us));
            }
            MetadataEvent::SilenceEnd {
                at, channel_number, ..
            } => {
                if let Some(pos) = pending_silence
                    .iter()
                    .position(|(c, _)| *c == channel_number)
                {
                    let (_, start) = pending_silence.remove(pos);
                    report.silence.push(SilenceRange {
                        start_us: start,
                        end_us: at.time_us,
                        channel: channel_number,
                    });
                }
            }
            MetadataEvent::SceneChange { at, score } => report.scenes.push(SceneChange {
                at_us: at.time_us,
                score,
            }),
            MetadataEvent::CropDetect { x, y, w, h, .. } => {
                report.crop = Some(CropSuggestion { x, y, w, h });
            }
            MetadataEvent::R128Summary {
                integrated,
                lra,
                true_peak,
            } => {
                report.loudness = Some(LoudnessReport {
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
                    &mut audio_end_us
                } else {
                    &mut video_end_us
                };
                *slot = Some(slot.map_or(at.time_us, |e| e.max(at.time_us)));
            }
            MetadataEvent::R128Frame { .. } => {}
        }
    }

    // Close a still-open black region at the video stream's end-of-stream.
    if let (Some(start), Some(end)) = (pending_black, video_end_us) {
        if keep_tail(start, end, cfg.black_min_duration_us) {
            report.black.push(BlackRange {
                start_us: start,
                end_us: end,
            });
        }
    }
    // Close still-open silence regions at the audio stream's end-of-stream.
    if let Some(end) = audio_end_us {
        for (channel, start) in pending_silence {
            if keep_tail(start, end, cfg.silence_min_duration_us) {
                report.silence.push(SilenceRange {
                    start_us: start,
                    end_us: end,
                    channel,
                });
            }
        }
    }

    report
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
