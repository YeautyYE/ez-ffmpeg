//! Native luma crop / letterbox detection.
//!
//! `VideoDetector::Crop` and [`CropDetectionOptions`] scan decoded **progressive**
//! Y planes in Rust. Interlaced frames (`AV_FRAME_FLAG_INTERLACED`) fail the
//! job as [`Error::AnalysisFrame`]; fields are not modeled as a pair of
//! half-height scans. The scanner
//! does not require FFmpeg's GPL `cropdetect` filter. Coordinates are not guaranteed to match
//! FFmpeg bit-for-bit: `round` expands outward so content is never cut,
//! `limit == 0` treats every sample as active, a finite temporal window is
//! used instead of a permanent historical maximum, and consensus is an
//! independent-median of the window (not FFmpeg's historical-max).
//! [`CropDetectionOptions::skip_initial`] drops real video frames before
//! format / hardware / interlace validation and before any luma or scene
//! handling, so a scene cut during skip does not reset crop state and a
//! known-bad leading frame can be stepped over. Flush / props-only markers
//! are triaged first and do not consume the skip budget.
//!
//! Crop events and observations are published only for frames that carry a
//! timestamp: a fully timestamp-less stream still updates crop state but
//! never publishes (mixed-PTS streams publish on the timestamped frames).
//!
//! Users who need the historical `lavfi.cropdetect.*` values can still attach
//! an explicit `cropdetect` filter graph;
//! [`MetadataEventFilter`](crate::core::analysis::filter::MetadataEventFilter)
//! keeps parsing those keys when native crop detection is not configured.
//!
//! Operators can time this scanner against a GPL `ffmpeg` binary (when
//! `ffmpeg -filters` lists `cropdetect`) with the ignored integration test:
//! `cargo test --release --test crop_parity_bench -- --ignored`.

mod luma;
mod scan;
mod stability;

use crate::core::analysis::event::Timestamp;
use crate::core::analysis::report::{AnalysisReport, CropSuggestion};
use crate::error::{Error, Result};
use ffmpeg_next::Frame;
use luma::{LumaAccess, LumaView};
use scan::{legacy_limit, resolve_threshold, scan_boundary_bands, ScanConfig, ThresholdBand};
use stability::Stability;
use std::sync::{Arc, Mutex};
use std::time::Duration;

/// How a luma sample is classified as near-black.
#[derive(Debug, Clone, Copy, PartialEq)]
#[non_exhaustive]
pub enum CropLumaThreshold {
    /// Fraction of the full digital code range, in `0.0..=1.0`.
    ///
    /// `0.0` is a sentinel: every sample is treated as active (full frame).
    Normalized(f32),
    /// Raw luma code after unpacking the stored sample.
    RawCode(u16),
    /// Fraction above nominal black in the declared signal range.
    ///
    /// `0.0` is nominal black itself (16 for limited 8-bit, 64 for limited
    /// 10-bit, 0 for full range). Unlike [`Normalized`](Self::Normalized),
    /// it is **not** the full-frame sentinel.
    AboveNominalBlack(f32),
}

/// Runtime handle for changing the luma threshold between frames.
///
/// Only `limit` is mutable at runtime, matching the public cropdetect command
/// surface. Invalid updates return an error and leave the previous value
/// unchanged.
#[derive(Clone)]
pub struct CropDetectionControl {
    inner: Arc<Mutex<CropLumaThreshold>>,
}

impl std::fmt::Debug for CropDetectionControl {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("CropDetectionControl")
            .field("threshold", &self.threshold())
            .finish()
    }
}

impl CropDetectionControl {
    /// Creates a control holding `initial` after validating it.
    pub fn new(initial: CropLumaThreshold) -> Result<Self> {
        validate_threshold(initial)?;
        Ok(Self {
            inner: Arc::new(Mutex::new(initial)),
        })
    }

    /// Replaces the threshold. On validation failure the previous value is kept.
    pub fn set_threshold(&self, value: CropLumaThreshold) -> Result<()> {
        validate_threshold(value)?;
        let mut guard = self.inner.lock().unwrap_or_else(|e| e.into_inner());
        *guard = value;
        Ok(())
    }

    /// Current threshold.
    pub fn threshold(&self) -> CropLumaThreshold {
        *self.inner.lock().unwrap_or_else(|e| e.into_inner())
    }
}

/// Builder for native crop detection. Fields are private so new knobs can be
/// added without breaking source compatibility.
#[derive(Debug, Clone)]
pub struct CropDetectionOptions {
    threshold: CropLumaThreshold,
    round: u32,
    reset_every: u32,
    skip_initial: u32,
    active_tolerance: f32,
    soft_margin: f32,
    temporal_window: Duration,
    max_border_fraction: f32,
    control: Option<CropDetectionControl>,
}

impl Default for CropDetectionOptions {
    fn default() -> Self {
        Self::new()
    }
}

impl CropDetectionOptions {
    /// Defaults matching `limit=24`, `round=16`, `reset=0`, plus `skip=2`.
    pub fn new() -> Self {
        Self {
            threshold: CropLumaThreshold::Normalized(24.0 / 255.0),
            round: 16,
            reset_every: 0,
            skip_initial: 2,
            active_tolerance: scan::DEFAULT_ACTIVE_TOLERANCE,
            soft_margin: 4.0 / 255.0,
            temporal_window: Duration::from_millis(500),
            max_border_fraction: scan::DEFAULT_MAX_BORDER,
            control: None,
        }
    }

    /// Maps the historical [`crate::analysis::VideoDetector::Crop`] integers.
    pub fn from_legacy(limit: u32, round: u32, reset: u32) -> Self {
        let mut opts = Self::new();
        opts.threshold = legacy_limit(limit);
        opts.round = round;
        opts.reset_every = reset;
        opts.skip_initial = 2;
        opts
    }

    /// Hard luma threshold used to classify near-black samples.
    pub fn threshold(mut self, value: CropLumaThreshold) -> Self {
        self.threshold = value;
        self
    }

    /// Width/height multiple after outward expansion. `0`/`1` skip extra multiples.
    pub fn round(mut self, multiple: u32) -> Self {
        self.round = multiple;
        self
    }

    /// Clear temporal evidence every `frames` evaluated frames (`0` = never).
    /// The current stable rectangle is kept.
    pub fn reset_every(mut self, frames: u32) -> Self {
        self.reset_every = frames;
        self
    }

    /// Skip the first `frames` real video frames. The default is 2.
    ///
    /// Skipped frames do not contribute luma reads, crop state, scene-cut
    /// resets, crop events, or format / hardware / interlace validation.
    /// Flush markers are not counted. Bootstrap still needs three
    /// high-confidence candidates after skip, so with the default the first
    /// crop event is at the earliest the 5th real frame.
    pub fn skip_initial(mut self, frames: u32) -> Self {
        self.skip_initial = frames;
        self
    }

    /// Fraction of a line that may be active and still count as a black bar.
    ///
    /// Values above `0.5` have no further effect: a line whose weighted
    /// activity exceeds half its samples is always classified as content.
    pub fn active_tolerance(mut self, fraction: f32) -> Self {
        self.active_tolerance = fraction;
        self
    }

    /// Extra luma codes above the hard threshold treated as a soft band.
    pub fn soft_margin(mut self, fraction: f32) -> Self {
        self.soft_margin = fraction;
        self
    }

    /// Sliding window of high-confidence candidates used for hysteresis.
    pub fn temporal_window(mut self, duration: Duration) -> Self {
        self.temporal_window = duration;
        self
    }

    /// Maximum fraction of each dimension searched as a border band.
    pub fn max_border_fraction(mut self, fraction: f32) -> Self {
        self.max_border_fraction = fraction;
        self
    }

    /// Share a handle that can change the luma threshold between frames.
    pub fn threshold_control(mut self, control: CropDetectionControl) -> Self {
        self.control = Some(control);
        self
    }

    pub(crate) fn validate(&self) -> Result<()> {
        validate_threshold(self.threshold)?;
        for (v, what) in [
            (self.round, "crop round"),
            (self.reset_every, "crop reset"),
            (self.skip_initial, "crop skip_initial"),
        ] {
            if v > i32::MAX as u32 {
                return Err(Error::InvalidRecipeArg(format!(
                    "{what} must be <= {}, got {v}",
                    i32::MAX
                )));
            }
        }
        if !self.active_tolerance.is_finite() || !(0.0..=1.0).contains(&self.active_tolerance) {
            return Err(Error::InvalidRecipeArg(format!(
                "crop active_tolerance must be in 0.0..=1.0, got {}",
                self.active_tolerance
            )));
        }
        if !self.soft_margin.is_finite() || self.soft_margin < 0.0 {
            return Err(Error::InvalidRecipeArg(format!(
                "crop soft_margin must be finite and >= 0, got {}",
                self.soft_margin
            )));
        }
        if !self.max_border_fraction.is_finite()
            || !(0.05..=0.49).contains(&self.max_border_fraction)
        {
            return Err(Error::InvalidRecipeArg(format!(
                "crop max_border_fraction must be in 0.05..=0.49, got {}",
                self.max_border_fraction
            )));
        }
        Ok(())
    }
}

fn validate_threshold(value: CropLumaThreshold) -> Result<()> {
    match value {
        CropLumaThreshold::Normalized(f) | CropLumaThreshold::AboveNominalBlack(f) => {
            if !f.is_finite() || !(0.0..=1.0).contains(&f) {
                Err(Error::InvalidRecipeArg(format!(
                    "crop luma threshold fraction must be finite in 0.0..=1.0, got {f}"
                )))
            } else {
                Ok(())
            }
        }
        CropLumaThreshold::RawCode(_) => Ok(()),
    }
}

/// Half-open raw bounds, before `round` / chroma expansion.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct CropRawBounds {
    pub left: i32,
    pub top: i32,
    pub right_exclusive: i32,
    pub bottom_exclusive: i32,
}

/// One published crop observation (raw + aligned).
#[derive(Debug, Clone, Copy, PartialEq)]
pub struct CropObservation {
    pub at: Timestamp,
    pub raw: CropRawBounds,
    pub aligned: CropSuggestion,
}

/// [`AnalysisReport`] plus the last raw/aligned crop observation.
#[derive(Debug, Clone, PartialEq)]
pub struct DetailedAnalysisReport {
    pub report: AnalysisReport,
    pub last_crop_observation: Option<CropObservation>,
}

/// Native crop scanner stored on [`crate::analysis::MetadataEventFilter`].
pub(crate) struct CropScanner {
    options: CropDetectionOptions,
    control: CropDetectionControl,
    last_threshold: CropLumaThreshold,
    stability: Stability,
    #[cfg(test)]
    last_probe_count: u32,
}

impl CropScanner {
    pub(crate) fn new(options: CropDetectionOptions) -> Result<Self> {
        options.validate()?;
        let control = match options.control.clone() {
            Some(c) => c,
            None => CropDetectionControl::new(options.threshold)?,
        };
        let initial = control.threshold();
        let window_us = options.temporal_window.as_micros().min(i64::MAX as u128) as i64;
        Ok(Self {
            stability: Stability::new(
                options.skip_initial,
                options.reset_every,
                window_us,
                options.round,
            ),
            options,
            control,
            last_threshold: initial,
            #[cfg(test)]
            last_probe_count: 0,
        })
    }

    pub(crate) fn process_frame(
        &mut self,
        frame: &Frame,
        frame_ts: Option<Timestamp>,
        scene_changed: bool,
    ) -> Result<Option<(CropSuggestion, CropObservation)>> {
        if LumaView::is_passthrough_marker(frame) {
            return Ok(self.publish(frame_ts));
        }

        if self.stability.skip_due() {
            self.stability.consume_skip();
            return Ok(None);
        }

        let luma = match LumaView::try_from_frame(frame)
            .map_err(|e| Error::AnalysisFrame(e.to_string().into_boxed_str()))?
        {
            Some(luma) => luma,
            None => return Ok(self.publish(frame_ts)),
        };

        self.stability.set_geometry(
            luma.frame_width() as i32,
            luma.frame_height() as i32,
            luma.chroma_grid(),
        );

        self.stability.on_evaluated_frame();

        let snapshot = self.control.threshold();
        if snapshot != self.last_threshold {
            self.stability.clear_evidence();
            self.last_threshold = snapshot;
        }

        let full = CropRawBounds {
            left: 0,
            top: 0,
            right_exclusive: luma.frame_width() as i32,
            bottom_exclusive: luma.frame_height() as i32,
        };
        if scene_changed {
            self.stability.reset_scene(full);
        }

        let band = resolve_band(snapshot, &luma, self.options.soft_margin)?;
        let cfg = ScanConfig {
            threshold: band,
            active_tolerance: self.options.active_tolerance,
            max_border_fraction: self.options.max_border_fraction,
        };
        if let Some(candidate) = scan_boundary_bands(&luma, &cfg) {
            if candidate.reliable {
                self.stability
                    .observe(candidate.raw, frame_ts.map(|t| t.time_us));
            }
        }
        #[cfg(test)]
        {
            self.last_probe_count = luma.probe_count();
        }

        if !scene_changed {
            self.stability.maybe_periodic_reset();
        }

        Ok(self.publish(frame_ts))
    }

    fn publish(
        &mut self,
        frame_ts: Option<Timestamp>,
    ) -> Option<(CropSuggestion, CropObservation)> {
        let (_, mut obs) = self.stability.current_aligned()?;
        let ts = frame_ts?;
        obs.at = ts;
        Some((obs.aligned, obs))
    }

    #[cfg(test)]
    pub(crate) fn last_probe_count(&self) -> u32 {
        self.last_probe_count
    }

    #[cfg(test)]
    pub(crate) fn process_luma<L: LumaAccess>(
        &mut self,
        luma: &L,
        time_us: Option<i64>,
        scene_changed: bool,
    ) -> Option<(CropSuggestion, CropObservation)> {
        #[cfg(test)]
        {
            self.last_probe_count = 0;
        }
        if self.stability.skip_due() {
            self.stability.consume_skip();
            return None;
        }
        self.stability.set_geometry(
            luma.frame_width() as i32,
            luma.frame_height() as i32,
            luma.chroma_grid(),
        );
        self.stability.on_evaluated_frame();
        let snapshot = self.control.threshold();
        if snapshot != self.last_threshold {
            self.stability.clear_evidence();
            self.last_threshold = snapshot;
        }
        let full = CropRawBounds {
            left: 0,
            top: 0,
            right_exclusive: luma.frame_width() as i32,
            bottom_exclusive: luma.frame_height() as i32,
        };
        if scene_changed {
            self.stability.reset_scene(full);
        }
        let band = resolve_band(snapshot, luma, self.options.soft_margin).ok()?;
        let cfg = ScanConfig {
            threshold: band,
            active_tolerance: self.options.active_tolerance,
            max_border_fraction: self.options.max_border_fraction,
        };
        if let Some(candidate) = scan_boundary_bands(luma, &cfg) {
            if candidate.reliable {
                self.stability.observe(candidate.raw, time_us);
            }
        }
        #[cfg(test)]
        {
            self.last_probe_count = luma.probe_count();
        }
        if !scene_changed {
            self.stability.maybe_periodic_reset();
        }
        let ts = time_us.map(|us| Timestamp {
            time_us: us,
            pts: Some(us),
            time_base: Some((1, 1_000_000)),
        });
        self.publish(ts)
    }
}

fn resolve_band<L: LumaAccess>(
    spec: CropLumaThreshold,
    luma: &L,
    soft_margin: f32,
) -> Result<ThresholdBand> {
    resolve_threshold(spec, luma.bit_depth(), luma.signal_range(), soft_margin)
        .map_err(Error::InvalidRecipeArg)
}

#[cfg(test)]
mod tests;
