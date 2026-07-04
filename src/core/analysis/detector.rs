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

    /// Rejects non-finite (`NaN`/`inf`) threshold values up front, so they
    /// surface as a clean [`Error::InvalidRecipeArg`] instead of an opaque
    /// FFmpeg graph-parse failure.
    pub(crate) fn validate(&self) -> Result<()> {
        let finite = |v: f64, what: &str| -> Result<()> {
            if v.is_finite() {
                Ok(())
            } else {
                Err(Error::InvalidRecipeArg(format!(
                    "{what} must be a finite number, got {v}"
                )))
            }
        };
        match *self {
            VideoDetector::Black {
                min_duration_s,
                pixel_th,
                picture_th,
            } => {
                finite(min_duration_s, "blackdetect min_duration_s")?;
                finite(pixel_th, "blackdetect pixel_th")?;
                finite(picture_th, "blackdetect picture_th")?;
            }
            VideoDetector::Scene { threshold_pct } => {
                finite(threshold_pct, "scene threshold_pct")?;
            }
            VideoDetector::Crop { .. } => {}
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
            for (v, what) in [
                (noise_db, "silencedetect noise_db"),
                (min_duration_s, "silencedetect min_duration_s"),
            ] {
                if !v.is_finite() {
                    return Err(Error::InvalidRecipeArg(format!(
                        "{what} must be a finite number, got {v}"
                    )));
                }
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
        assert!(VideoDetector::Black {
            min_duration_s: 0.1,
            pixel_th: 0.1,
            picture_th: 0.98,
        }
        .validate()
        .is_ok());
    }
}
