//! Detector definitions and their FFmpeg `filter_desc` string generation.
//!
//! Each variant maps to exactly one FFmpeg detector filter. [`to_filter`] is a
//! pure function (unit-tested) that renders the filter string; the runner
//! assembles those into a filter graph. All detectors are passthrough — they
//! attach `lavfi.*` metadata to frames without dropping any.
//!
//! [`to_filter`]: VideoDetector::to_filter

use crate::error::{Error, Result};

/// A video-domain detector.
#[derive(Debug, Clone, PartialEq)]
pub enum VideoDetector {
    /// `blackdetect`: reports black regions.
    ///
    /// - `min_duration_s`: minimum black duration to report (seconds).
    /// - `pixel_th`: per-pixel blackness threshold, 0.0..1.0.
    /// - `picture_th`: fraction of the picture that must be black, 0.0..1.0.
    Black {
        min_duration_s: f64,
        pixel_th: f64,
        picture_th: f64,
    },
    /// `scdet`: reports scene changes.
    ///
    /// `threshold_pct` is a **percentage in `0.0..=100.0`** (e.g. `10.0` means
    /// 10%, not `0.10`). Runs with `sc_pass=0` so frames/metadata pass through
    /// untouched.
    Scene { threshold_pct: f64 },
    /// `cropdetect`: suggests a crop rectangle.
    ///
    /// - `limit`: luminance threshold below which a pixel is "black".
    /// - `round`: width/height are rounded to a multiple of this.
    /// - `reset`: recompute the crop every N frames (0 = never reset).
    Crop { limit: u32, round: u32, reset: u32 },
}

/// An audio-domain detector.
#[derive(Debug, Clone, PartialEq)]
pub enum AudioDetector {
    /// `silencedetect`: reports silent regions.
    ///
    /// - `noise_db`: noise floor in **dB** (rendered with the required `dB`
    ///   suffix, e.g. `-30dB`).
    /// - `min_duration_s`: minimum silence duration to report (seconds).
    /// - `mono`: when `true`, detect per channel (adds `mono=1`); the parser
    ///   then sees `.N` (1-based) channel suffixes on the metadata keys.
    Silence {
        noise_db: f64,
        min_duration_s: f64,
        mono: bool,
    },
    /// `ebur128 metadata=1`: EBU R128 loudness measurement.
    ///
    /// `true_peak` adds `peak=true` so per-channel true-peak keys are emitted.
    Ebur128 { true_peak: bool },
}

impl VideoDetector {
    /// The bare FFmpeg filter name, for capability checks.
    pub(crate) fn filter_name(&self) -> &'static str {
        match self {
            VideoDetector::Black { .. } => "blackdetect",
            VideoDetector::Scene { .. } => "scdet",
            VideoDetector::Crop { .. } => "cropdetect",
        }
    }

    /// Renders this detector as an FFmpeg filter string.
    pub(crate) fn to_filter(&self) -> String {
        match *self {
            VideoDetector::Black {
                min_duration_s,
                pixel_th,
                picture_th,
            } => format!("blackdetect=d={min_duration_s}:pix_th={pixel_th}:pic_th={picture_th}"),
            VideoDetector::Scene { threshold_pct } => {
                format!("scdet=threshold={threshold_pct}:sc_pass=0")
            }
            VideoDetector::Crop {
                limit,
                round,
                reset,
            } => format!("cropdetect=limit={limit}:round={round}:reset={reset}"),
        }
    }

    /// Rejects values outside each detector's documented range up front, so
    /// they surface as a clean [`Error::InvalidRecipeArg`] instead of an opaque
    /// FFmpeg graph-parse failure.
    pub(crate) fn validate(&self) -> Result<()> {
        let in_range = |v: f64, lo: f64, hi: f64, what: &str| -> Result<()> {
            if v.is_finite() && v >= lo && v <= hi {
                Ok(())
            } else {
                Err(Error::InvalidRecipeArg(format!(
                    "{what} must be in {lo}..={hi}, got {v}"
                )))
            }
        };
        match *self {
            VideoDetector::Black {
                min_duration_s,
                pixel_th,
                picture_th,
            } => {
                if !min_duration_s.is_finite() || min_duration_s < 0.0 {
                    return Err(Error::InvalidRecipeArg(format!(
                        "blackdetect min_duration_s must be finite and >= 0, got {min_duration_s}"
                    )));
                }
                in_range(pixel_th, 0.0, 1.0, "blackdetect pixel_th")?;
                in_range(picture_th, 0.0, 1.0, "blackdetect picture_th")?;
            }
            VideoDetector::Scene { threshold_pct } => {
                in_range(threshold_pct, 0.0, 100.0, "scene threshold_pct")?;
            }
            VideoDetector::Crop {
                limit,
                round,
                reset,
            } => {
                // cropdetect maps these onto FFmpeg int AVOptions; values above
                // i32::MAX would overflow into a late graph error.
                for (v, what) in [
                    (limit, "cropdetect limit"),
                    (round, "cropdetect round"),
                    (reset, "cropdetect reset"),
                ] {
                    if v > i32::MAX as u32 {
                        return Err(Error::InvalidRecipeArg(format!(
                            "{what} must be <= {}, got {v}",
                            i32::MAX
                        )));
                    }
                }
            }
        }
        Ok(())
    }
}

impl AudioDetector {
    /// The bare FFmpeg filter name, for capability checks.
    pub(crate) fn filter_name(&self) -> &'static str {
        match self {
            AudioDetector::Silence { .. } => "silencedetect",
            AudioDetector::Ebur128 { .. } => "ebur128",
        }
    }

    /// Renders this detector as an FFmpeg filter string.
    pub(crate) fn to_filter(&self) -> String {
        match *self {
            AudioDetector::Silence {
                noise_db,
                min_duration_s,
                mono,
            } => {
                let mut s = format!("silencedetect=noise={noise_db}dB:d={min_duration_s}");
                if mono {
                    s.push_str(":mono=1");
                }
                s
            }
            AudioDetector::Ebur128 { true_peak } => {
                if true_peak {
                    "ebur128=metadata=1:peak=true".to_string()
                } else {
                    "ebur128=metadata=1".to_string()
                }
            }
        }
    }

    /// Rejects non-finite (`NaN`/`inf`) values up front.
    pub(crate) fn validate(&self) -> Result<()> {
        if let AudioDetector::Silence {
            noise_db,
            min_duration_s,
            ..
        } = *self
        {
            if !noise_db.is_finite() {
                return Err(Error::InvalidRecipeArg(format!(
                    "silencedetect noise_db must be finite, got {noise_db}"
                )));
            }
            if !min_duration_s.is_finite() || min_duration_s < 0.0 {
                return Err(Error::InvalidRecipeArg(format!(
                    "silencedetect min_duration_s must be finite and >= 0, got {min_duration_s}"
                )));
            }
        }
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn black_filter_string() {
        let d = VideoDetector::Black {
            min_duration_s: 0.1,
            pixel_th: 0.1,
            picture_th: 0.98,
        };
        assert_eq!(d.to_filter(), "blackdetect=d=0.1:pix_th=0.1:pic_th=0.98");
    }

    #[test]
    fn scene_filter_uses_sc_pass_zero() {
        let d = VideoDetector::Scene { threshold_pct: 10.0 };
        assert_eq!(d.to_filter(), "scdet=threshold=10:sc_pass=0");
    }

    #[test]
    fn crop_filter_string() {
        let d = VideoDetector::Crop {
            limit: 24,
            round: 16,
            reset: 0,
        };
        assert_eq!(d.to_filter(), "cropdetect=limit=24:round=16:reset=0");
    }

    #[test]
    fn silence_filter_requires_db_suffix() {
        let d = AudioDetector::Silence {
            noise_db: -30.0,
            min_duration_s: 0.5,
            mono: false,
        };
        assert_eq!(d.to_filter(), "silencedetect=noise=-30dB:d=0.5");
    }

    #[test]
    fn silence_mono_adds_flag() {
        let d = AudioDetector::Silence {
            noise_db: -30.0,
            min_duration_s: 0.5,
            mono: true,
        };
        assert_eq!(d.to_filter(), "silencedetect=noise=-30dB:d=0.5:mono=1");
    }

    #[test]
    fn ebur128_peak_toggle() {
        assert_eq!(
            AudioDetector::Ebur128 { true_peak: false }.to_filter(),
            "ebur128=metadata=1"
        );
        assert_eq!(
            AudioDetector::Ebur128 { true_peak: true }.to_filter(),
            "ebur128=metadata=1:peak=true"
        );
    }

    #[test]
    fn validate_rejects_non_finite() {
        assert!(VideoDetector::Black {
            min_duration_s: f64::NAN,
            pixel_th: 0.1,
            picture_th: 0.98,
        }
        .validate()
        .is_err());
        assert!(VideoDetector::Scene {
            threshold_pct: f64::INFINITY,
        }
        .validate()
        .is_err());
        assert!(AudioDetector::Silence {
            noise_db: f64::NAN,
            min_duration_s: 0.5,
            mono: false,
        }
        .validate()
        .is_err());
        // Out-of-documented-range values are rejected too.
        assert!(VideoDetector::Scene { threshold_pct: 101.0 }.validate().is_err());
        assert!(VideoDetector::Black {
            min_duration_s: -1.0,
            pixel_th: 0.1,
            picture_th: 0.98,
        }
        .validate()
        .is_err());
        assert!(VideoDetector::Black {
            min_duration_s: 0.1,
            pixel_th: 1.5,
            picture_th: 0.98,
        }
        .validate()
        .is_err());
        assert!(VideoDetector::Black {
            min_duration_s: 0.1,
            pixel_th: 0.1,
            picture_th: 0.98,
        }
        .validate()
        .is_ok());
    }
}
