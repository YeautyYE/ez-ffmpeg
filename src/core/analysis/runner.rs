//! The one-shot [`Analysis`] builder: configure detectors, run to completion,
//! and get a folded [`AnalysisReport`].
//!
//! `run()` builds an isolation topology (§C2): every detector branch is mapped
//! to its own stream on a single `null` output, each carrying a
//! [`MetadataEventFilter`] that folds events into a shared fold state as they
//! arrive (so per-frame events are never buffered). Audio
//! detectors are split into separate `asplit` branches so `ebur128`'s 100 ms
//! re-chunking never perturbs `silencedetect`.

use crate::core::analysis::detector::{AudioDetector, VideoDetector};
use crate::core::analysis::event::{secs_to_us, MetadataEvent};
use crate::core::analysis::filter::{EventSink, MetadataEventFilter, SinkError};
use crate::core::analysis::report::{finalize, fold_event, AnalysisReport, FoldConfig, FoldState};
use crate::core::filter::frame_pipeline::FramePipeline;
use crate::core::filter::frame_pipeline_builder::FramePipelineBuilder;
use crate::error::Error;
use crate::{FfmpegContext, FfmpegScheduler, Input, Output};
use ffmpeg_sys_next::av_guess_format;
use ffmpeg_sys_next::AVMediaType::{self, AVMEDIA_TYPE_AUDIO, AVMEDIA_TYPE_VIDEO};
use std::ffi::CString;
use std::ptr;
use std::sync::{Arc, Mutex};

/// A one-shot detection/measurement run over a single input.
pub struct Analysis {
    input: Input,
    video: Vec<VideoDetector>,
    audio: Vec<AudioDetector>,
}

/// One mapped detector branch: a filter-graph output label and its media type.
struct Branch {
    media: AVMediaType,
    map: String,
}

impl Analysis {
    /// Starts an analysis over `input` (a path, URL, or anything convertible
    /// into an [`Input`]).
    pub fn new(input: impl Into<Input>) -> Self {
        Self {
            input: input.into(),
            video: Vec::new(),
            audio: Vec::new(),
        }
    }

    /// Adds a video detector. At most one of each kind is allowed per run.
    pub fn video_detector(mut self, detector: VideoDetector) -> Self {
        self.video.push(detector);
        self
    }

    /// Adds an audio detector. At most one of each kind is allowed per run.
    pub fn audio_detector(mut self, detector: AudioDetector) -> Self {
        self.audio.push(detector);
        self
    }

    /// Runs the analysis to completion and folds the events into a report.
    ///
    /// # Errors
    /// - [`Error::InvalidRecipeArg`] if no detectors are configured, a detector
    ///   kind is duplicated, or a required filter / the `null` muxer is missing.
    /// - Any error bubbling up from the underlying FFmpeg run.
    pub fn run(self) -> crate::error::Result<AnalysisReport> {
        self.validate()?;
        self.check_capabilities()?;

        let (filter_desc, branches) = self.plan();
        let cfg = self.fold_config();

        let collector: Arc<Mutex<FoldState>> = Arc::new(Mutex::new(FoldState::default()));
        let pipelines: Vec<FramePipeline> = branches
            .iter()
            .enumerate()
            .map(|(index, branch)| make_pipeline(branch.media, index, collector.clone()))
            .collect();

        let mut output = Output::from("-")
            .set_format("null")
            .set_frame_pipelines(pipelines);
        for branch in &branches {
            output = output.add_stream_map(branch.map.clone());
        }

        let context = FfmpegContext::builder()
            .input(self.input)
            .filter_desc(filter_desc)
            .output(output)
            .build()?;
        FfmpegScheduler::new(context).start()?.wait()?;

        let state = collector
            .lock()
            .map(|mut guard| std::mem::take(&mut *guard))
            .map_err(|_| {
                Error::InvalidRecipeArg(
                    "analysis event collector was poisoned by a panicked pipeline thread"
                        .to_string(),
                )
            })?;
        Ok(finalize(state, &cfg))
    }

    /// Rejects empty and duplicated detector sets (duplicate detectors of the
    /// same kind write indistinguishable `lavfi.*` keys).
    fn validate(&self) -> crate::error::Result<()> {
        if self.video.is_empty() && self.audio.is_empty() {
            return Err(Error::InvalidRecipeArg(
                "Analysis requires at least one detector".to_string(),
            ));
        }
        let mut seen_video = [false; 3];
        for detector in &self.video {
            let idx = match detector {
                VideoDetector::Black { .. } => 0,
                VideoDetector::Scene { .. } => 1,
                VideoDetector::Crop { .. } => 2,
            };
            if seen_video[idx] {
                return Err(Error::InvalidRecipeArg(format!(
                    "duplicate video detector '{}' on the same media",
                    detector.filter_name()
                )));
            }
            seen_video[idx] = true;
        }
        let mut seen_audio = [false; 2];
        for detector in &self.audio {
            let idx = match detector {
                AudioDetector::Silence { .. } => 0,
                AudioDetector::Ebur128 { .. } => 1,
            };
            if seen_audio[idx] {
                return Err(Error::InvalidRecipeArg(format!(
                    "duplicate audio detector '{}' on the same media",
                    detector.filter_name()
                )));
            }
            seen_audio[idx] = true;
        }
        for detector in &self.video {
            detector.validate()?;
        }
        for detector in &self.audio {
            detector.validate()?;
        }
        Ok(())
    }

    /// Verifies the chosen filters, `asplit` (if needed), and the `null` muxer
    /// exist in the linked FFmpeg build. Best-effort — passing here does not
    /// guarantee the graph parses.
    fn check_capabilities(&self) -> crate::error::Result<()> {
        for detector in &self.video {
            require_filter(detector.filter_name())?;
        }
        for detector in &self.audio {
            require_filter(detector.filter_name())?;
        }
        if self.audio.len() >= 2 {
            require_filter("asplit")?;
        }
        require_null_muxer()
    }

    /// Builds the `filter_desc` string and the ordered branch list.
    fn plan(&self) -> (String, Vec<Branch>) {
        let mut desc_parts: Vec<String> = Vec::new();
        let mut branches: Vec<Branch> = Vec::new();

        // Video detectors are all passthrough, so they chain on one branch.
        if !self.video.is_empty() {
            let chain = self
                .video
                .iter()
                .map(|d| d.to_filter())
                .collect::<Vec<_>>()
                .join(",");
            desc_parts.push(format!("[0:v]{chain}[vdet]"));
            branches.push(Branch {
                media: AVMEDIA_TYPE_VIDEO,
                map: "[vdet]".to_string(),
            });
        }

        // Audio detectors must be isolated: split into one branch each.
        match self.audio.len() {
            0 => {}
            1 => {
                desc_parts.push(format!("[0:a]{}[adet0]", self.audio[0].to_filter()));
                branches.push(Branch {
                    media: AVMEDIA_TYPE_AUDIO,
                    map: "[adet0]".to_string(),
                });
            }
            n => {
                let labels: String = (0..n).map(|j| format!("[asplit{j}]")).collect();
                desc_parts.push(format!("[0:a]asplit={n}{labels}"));
                for (j, detector) in self.audio.iter().enumerate() {
                    desc_parts.push(format!("[asplit{j}]{}[adet{j}]", detector.to_filter()));
                    branches.push(Branch {
                        media: AVMEDIA_TYPE_AUDIO,
                        map: format!("[adet{j}]"),
                    });
                }
            }
        }

        (desc_parts.join(";"), branches)
    }

    /// Collects the min-duration thresholds the folder needs to trim tails.
    fn fold_config(&self) -> FoldConfig {
        let mut cfg = FoldConfig::default();
        for detector in &self.video {
            if let VideoDetector::Black { min_duration_s, .. } = detector {
                cfg.black_min_duration_us = secs_to_us(*min_duration_s);
            }
        }
        for detector in &self.audio {
            if let AudioDetector::Silence { min_duration_s, .. } = detector {
                cfg.silence_min_duration_us = secs_to_us(*min_duration_s);
            }
        }
        cfg
    }
}

/// A `Send`-able sink that folds events into the run's shared fold state.
#[derive(Clone)]
struct FoldSink {
    collector: Arc<Mutex<FoldState>>,
}

impl EventSink for FoldSink {
    fn try_emit(&mut self, ev: MetadataEvent) -> Result<(), SinkError> {
        match self.collector.lock() {
            Ok(mut guard) => {
                // Fold on arrival so per-frame events are never buffered.
                fold_event(&mut guard, ev);
                Ok(())
            }
            // A poisoned mutex means a pipeline thread panicked; surface it as a
            // disconnected sink so the run aborts instead of silently dropping.
            Err(_) => Err(SinkError::Disconnected),
        }
    }
}

fn make_pipeline(
    media: AVMediaType,
    stream_index: usize,
    collector: Arc<Mutex<FoldState>>,
) -> FramePipeline {
    let filter = MetadataEventFilter::new(media, FoldSink { collector });
    FramePipelineBuilder::new(media)
        .filter("analysis", Box::new(filter))
        .set_stream_index(stream_index)
        .build()
}

fn require_filter(name: &str) -> crate::error::Result<()> {
    if crate::hwaccel::is_filter_available(name) {
        Ok(())
    } else {
        Err(Error::InvalidRecipeArg(format!(
            "FFmpeg filter '{name}' is not available in this build"
        )))
    }
}

fn require_null_muxer() -> crate::error::Result<()> {
    let name = CString::new("null").expect("literal has no interior NUL");
    // SAFETY: `name` is a valid C string; null filename/mime are accepted.
    let ofmt = unsafe { av_guess_format(name.as_ptr(), ptr::null(), ptr::null()) };
    if ofmt.is_null() {
        Err(Error::InvalidRecipeArg(
            "FFmpeg 'null' muxer is not available in this build".to_string(),
        ))
    } else {
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn sample() -> Analysis {
        Analysis::new("input.mp4")
            .video_detector(VideoDetector::Black {
                min_duration_s: 0.1,
                pixel_th: 0.1,
                picture_th: 0.98,
            })
            .audio_detector(AudioDetector::Silence {
                noise_db: -30.0,
                min_duration_s: 0.5,
                mono: false,
            })
            .audio_detector(AudioDetector::Ebur128 { true_peak: false })
    }

    #[test]
    fn plan_isolates_audio_detectors_into_asplit_branches() {
        let (desc, branches) = sample().plan();
        assert!(desc.contains("[0:v]blackdetect=d=0.1:pix_th=0.1:pic_th=0.98[vdet]"));
        assert!(desc.contains("[0:a]asplit=2[asplit0][asplit1]"));
        assert!(desc.contains("[asplit0]silencedetect=noise=-30dB:d=0.5[adet0]"));
        assert!(desc.contains("[asplit1]ebur128=metadata=1[adet1]"));
        assert_eq!(branches.len(), 3);
        assert_eq!(branches[0].media, AVMEDIA_TYPE_VIDEO);
        assert_eq!(branches[1].media, AVMEDIA_TYPE_AUDIO);
    }

    #[test]
    fn plan_single_audio_detector_has_no_asplit() {
        let (desc, branches) = Analysis::new("input.mp4")
            .audio_detector(AudioDetector::Ebur128 { true_peak: true })
            .plan();
        assert_eq!(desc, "[0:a]ebur128=metadata=1:peak=true[adet0]");
        assert_eq!(branches.len(), 1);
    }

    #[test]
    fn empty_analysis_is_rejected() {
        let result = Analysis::new("input.mp4").validate();
        assert!(matches!(result, Err(Error::InvalidRecipeArg(_))));
    }

    #[test]
    fn duplicate_detector_is_rejected() {
        let result = Analysis::new("input.mp4")
            .video_detector(VideoDetector::Scene {
                threshold_pct: 10.0,
            })
            .video_detector(VideoDetector::Scene {
                threshold_pct: 20.0,
            })
            .validate();
        assert!(matches!(result, Err(Error::InvalidRecipeArg(_))));
    }

    #[test]
    fn fold_config_picks_up_min_durations() {
        let cfg = sample().fold_config();
        assert_eq!(cfg.black_min_duration_us, Some(100_000));
        assert_eq!(cfg.silence_min_duration_us, Some(500_000));
    }
}
