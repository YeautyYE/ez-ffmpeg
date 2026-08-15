//! Stateless boundary-band scan over a [`LumaAccess`] view.
//!
//! No FFmpeg types live here. The scanner samples at most 64 coarse lines per
//! side with at most 256 luma reads each, then refines the black→active
//! transition. Interior probes decide all-black vs vignette vs content.

use super::luma::{ChromaGrid, LumaAccess, SignalRange};
use super::{CropLumaThreshold, CropRawBounds};

pub(crate) const MAX_COARSE_LINES: u32 = 64;
pub(crate) const MAX_SAMPLES_PER_LINE: u32 = 256;
pub(crate) const MIN_CONSECUTIVE_ACTIVE: u32 = 3;
pub(crate) const MAX_INTERIOR_PROBES: u32 = 256;
pub(crate) const DEFAULT_MAX_BORDER: f32 = 0.45;
pub(crate) const DEFAULT_ACTIVE_TOLERANCE: f32 = 0.02;
/// Mean activity above this fraction is a solid content line, not a ramp.
pub(crate) const STRONG_ACTIVITY: f32 = 0.50;
pub(crate) const WEIGHT_SCALE: u32 = 1024;

/// Hard / soft luma band in unpacked code units.
#[derive(Debug, Clone, Copy)]
pub(crate) struct ThresholdBand {
    pub low: u16,
    pub high: u16,
    pub all_active: bool,
}

impl ThresholdBand {
    fn weight(self, code: u16) -> u32 {
        if self.all_active {
            return WEIGHT_SCALE;
        }
        if code <= self.low {
            0
        } else if code >= self.high {
            WEIGHT_SCALE
        } else {
            let span = (self.high - self.low).max(1) as u32;
            (code - self.low) as u32 * WEIGHT_SCALE / span
        }
    }
}

/// Resolve a public threshold spec against a frame's bit depth and range.
pub(crate) fn resolve_threshold(
    spec: CropLumaThreshold,
    bit_depth: u8,
    range: SignalRange,
    soft_margin_frac: f32,
) -> Result<ThresholdBand, String> {
    let max_code = ((1u32 << bit_depth) - 1) as u16;
    let (low, all_active) = match spec {
        CropLumaThreshold::Normalized(f) => {
            if !f.is_finite() || !(0.0..=1.0).contains(&f) {
                return Err(format!(
                    "crop luma threshold must be a finite value in 0.0..=1.0, got {f}"
                ));
            }
            if f == 0.0 {
                (0, true)
            } else {
                (((f * max_code as f32).round() as u16).min(max_code), false)
            }
        }
        CropLumaThreshold::RawCode(code) => (code.min(max_code), false),
        CropLumaThreshold::AboveNominalBlack(f) => {
            if !f.is_finite() || !(0.0..=1.0).contains(&f) {
                return Err(format!(
                    "crop AboveNominalBlack fraction must be finite in 0.0..=1.0, got {f}"
                ));
            }
            let (black, white) = match (range, bit_depth) {
                (SignalRange::Full, _) => (0u16, max_code),
                (SignalRange::Limited, 8) => (16, 235),
                (SignalRange::Limited, _) => (64, 940.min(max_code)),
            };
            let span = white.saturating_sub(black) as f32;
            let code = black.saturating_add((f * span).round() as u16);
            (code.min(max_code), false)
        }
    };
    if all_active {
        return Ok(ThresholdBand {
            low: 0,
            high: 0,
            all_active: true,
        });
    }
    let soft = (soft_margin_frac.max(0.0) * max_code as f32).round() as u16;
    let mut high = low.saturating_add(soft).min(max_code);
    if high <= low {
        high = low.saturating_add(1).min(max_code);
    }
    Ok(ThresholdBand {
        low,
        high,
        all_active: false,
    })
}

/// Map the historical `VideoDetector::Crop { limit }` integer.
pub(crate) fn legacy_limit(limit: u32) -> CropLumaThreshold {
    if limit == 0 {
        CropLumaThreshold::Normalized(0.0)
    } else if limit <= 255 {
        CropLumaThreshold::Normalized(limit as f32 / 255.0)
    } else {
        CropLumaThreshold::RawCode(limit.min(u16::MAX as u32) as u16)
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum LineClass {
    Black,
    /// Above the noise tolerance but not a solid content line (vignette ramp).
    Weak,
    /// Most samples on the line are active (letterbox / pillarbox content).
    Strong,
    Unknown,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum Edge {
    /// Inset from this side, in pixels. `0` means the outer line is already active.
    Inset(u32),
    /// Every sampled line in the border band was black.
    AllBlack,
    /// Gradient / no usable transition.
    Unknown,
}

/// A raw content rectangle plus whether it is safe to publish.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) struct CropCandidate {
    pub raw: CropRawBounds,
    pub reliable: bool,
}

/// Scan config that is constant for a frame.
#[derive(Debug, Clone, Copy)]
pub(crate) struct ScanConfig {
    pub threshold: ThresholdBand,
    pub active_tolerance: f32,
    pub max_border_fraction: f32,
}

impl ScanConfig {
    #[cfg(test)]
    pub(crate) fn standard(threshold: ThresholdBand) -> Self {
        Self {
            threshold,
            active_tolerance: DEFAULT_ACTIVE_TOLERANCE,
            max_border_fraction: DEFAULT_MAX_BORDER,
        }
    }
}

/// Scan the four border bands. Returns `None` when the frame is unreliable
/// (all-black / no contrast). A vignette with an active interior yields a
/// reliable full-frame rectangle (do not crop).
pub(crate) fn scan_boundary_bands<L: LumaAccess>(
    luma: &L,
    cfg: &ScanConfig,
) -> Option<CropCandidate> {
    let w = luma.width();
    let h = luma.height();
    if w == 0 || h == 0 {
        return None;
    }
    let full = CropRawBounds {
        left: luma.origin_x(),
        top: luma.origin_y(),
        right_exclusive: luma.origin_x() + w as i32,
        bottom_exclusive: luma.origin_y() + h as i32,
    };
    if cfg.threshold.all_active {
        return Some(CropCandidate {
            raw: full,
            reliable: true,
        });
    }

    let interior_active = interior_is_active(luma, cfg);

    let top = scan_side(luma, Side::Top, 0, w, 0, h, cfg);
    let bottom = scan_side(luma, Side::Bottom, 0, w, 0, h, cfg);

    let (top_inset, bottom_inset) = match (top, bottom, interior_active) {
        (Edge::Unknown, _, false) | (_, Edge::Unknown, false) => return None,
        (Edge::AllBlack, _, false) | (_, Edge::AllBlack, false) => return None,
        (Edge::Unknown, Edge::Unknown, true) => {
            // No sharp letterbox. Still scan left/right: a pillarbox-only
            // frame makes the full-width top/bottom lines look like a
            // vignette (black sides + active middle).
            (0, 0)
        }
        (t, b, true) => (
            match t {
                Edge::Inset(v) => v,
                // The whole 45% band is black; content sits inside the cap.
                Edge::AllBlack => border_px(h, cfg.max_border_fraction),
                // Gradient / no sharp bar: do not crop this side.
                Edge::Unknown => 0,
            },
            match b {
                Edge::Inset(v) => v,
                Edge::AllBlack => border_px(h, cfg.max_border_fraction),
                Edge::Unknown => 0,
            },
        ),
        (Edge::Inset(t), Edge::Inset(b), false) => (t, b),
    };

    let y0 = top_inset.min(h.saturating_sub(1));
    let y1 = h
        .saturating_sub(bottom_inset)
        .max(y0.saturating_add(1))
        .min(h);

    let left = scan_side(luma, Side::Left, 0, w, y0, y1, cfg);
    let right = scan_side(luma, Side::Right, 0, w, y0, y1, cfg);
    let (left_inset, right_inset) = match (left, right, interior_active) {
        (Edge::Unknown, _, false) | (_, Edge::Unknown, false) => return None,
        (Edge::AllBlack, _, false) | (_, Edge::AllBlack, false) => return None,
        (l, r, _) => (
            match l {
                Edge::Inset(v) => v,
                Edge::AllBlack => border_px(w, cfg.max_border_fraction),
                Edge::Unknown => 0,
            },
            match r {
                Edge::Inset(v) => v,
                Edge::AllBlack => border_px(w, cfg.max_border_fraction),
                Edge::Unknown => 0,
            },
        ),
    };

    let x0 = left_inset.min(w.saturating_sub(1));
    let x1 = w
        .saturating_sub(right_inset)
        .max(x0.saturating_add(1))
        .min(w);

    // Refine top/bottom inside the horizontal span.
    let top = refine_side(luma, Side::Top, x0, x1, 0, h, top_inset, cfg);
    let bottom = refine_side(luma, Side::Bottom, x0, x1, 0, h, bottom_inset, cfg);
    let left = refine_side(luma, Side::Left, 0, w, y0, y1, left_inset, cfg);
    let right = refine_side(luma, Side::Right, 0, w, y0, y1, right_inset, cfg);

    let left_i = inset_or(left, left_inset);
    let right_i = inset_or(right, right_inset);
    let top_i = inset_or(top, top_inset);
    let bottom_i = inset_or(bottom, bottom_inset);

    if left_i.saturating_add(right_i) >= w || top_i.saturating_add(bottom_i) >= h {
        return if interior_active {
            Some(CropCandidate {
                raw: full,
                reliable: true,
            })
        } else {
            None
        };
    }

    let ox = luma.origin_x();
    let oy = luma.origin_y();
    let raw = CropRawBounds {
        left: ox + left_i as i32,
        top: oy + top_i as i32,
        right_exclusive: ox + (w - right_i) as i32,
        bottom_exclusive: oy + (h - bottom_i) as i32,
    };
    Some(CropCandidate {
        raw,
        reliable: interior_active || (left_i + right_i + top_i + bottom_i) > 0,
    })
}

fn inset_or(edge: Edge, fallback: u32) -> u32 {
    match edge {
        Edge::Inset(v) => v,
        Edge::AllBlack | Edge::Unknown => fallback,
    }
}

fn border_px(dim: u32, frac: f32) -> u32 {
    let v = (dim as f32 * frac).floor() as u32;
    v.min(dim.saturating_sub(1))
}

#[derive(Clone, Copy)]
enum Side {
    Top,
    Bottom,
    Left,
    Right,
}

fn scan_side<L: LumaAccess>(
    luma: &L,
    side: Side,
    x0: u32,
    x1: u32,
    y0: u32,
    y1: u32,
    cfg: &ScanConfig,
) -> Edge {
    let (along, inward_dim) = match side {
        Side::Top | Side::Bottom => (x1.saturating_sub(x0), luma.height()),
        Side::Left | Side::Right => (y1.saturating_sub(y0), luma.width()),
    };
    if along == 0 || inward_dim == 0 {
        return Edge::Unknown;
    }
    let border = border_px(inward_dim, cfg.max_border_fraction).max(1);
    let n_coarse = MAX_COARSE_LINES.min(border).max(1);

    let mut last_black: Option<u32> = None;
    let mut first_strong: Option<u32> = None;
    let mut any_strong = false;
    let mut any_weak = false;
    let mut any_black = false;

    for i in 0..n_coarse {
        let inset = i * border / n_coarse;
        match classify_inset(luma, side, inset, x0, x1, y0, y1, i, cfg) {
            LineClass::Black => {
                any_black = true;
                if first_strong.is_none() {
                    last_black = Some(inset);
                }
            }
            LineClass::Weak => {
                any_weak = true;
            }
            LineClass::Strong => {
                any_strong = true;
                if first_strong.is_none() {
                    first_strong = Some(inset);
                }
            }
            LineClass::Unknown => {}
        }
    }

    if !any_strong {
        return if any_weak {
            // Activity creeps in without a solid content edge: vignette.
            Edge::Unknown
        } else if any_black {
            Edge::AllBlack
        } else {
            Edge::Unknown
        };
    }
    if first_strong == Some(0) {
        // The outer line is already solid content: this side is not a bar.
        return Edge::Inset(0);
    }
    // A letterbox jumps from black to strong content in a few coarse steps.
    // A vignette spends many lines in the weak band before anything is strong.
    let coarse_step = (border / n_coarse).max(1);
    if let (Some(black), Some(strong)) = (last_black, first_strong) {
        if strong.saturating_sub(black) > coarse_step.saturating_mul(4) {
            return Edge::Unknown;
        }
    } else if last_black.is_none() {
        // Outer lines were weak/unknown, then became strong further in.
        return Edge::Unknown;
    }
    let start = last_black.unwrap_or(0);
    let end = first_strong.unwrap_or(border).min(border);
    refine_inset(luma, side, start, end, x0, x1, y0, y1, cfg)
}

// The scan window (x0..x1, y0..y1) travels as plain coordinates through the
// whole scan call tree; a params struct here would only add ceremony.
#[allow(clippy::too_many_arguments)]
fn refine_side<L: LumaAccess>(
    luma: &L,
    side: Side,
    x0: u32,
    x1: u32,
    y0: u32,
    y1: u32,
    coarse_inset: u32,
    cfg: &ScanConfig,
) -> Edge {
    let inward_dim = match side {
        Side::Top | Side::Bottom => luma.height(),
        Side::Left | Side::Right => luma.width(),
    };
    let border = border_px(inward_dim, cfg.max_border_fraction).max(1);
    let start = coarse_inset.saturating_sub(8);
    let end = (coarse_inset.saturating_add(8)).min(border);
    refine_inset(luma, side, start, end, x0, x1, y0, y1, cfg)
}

#[allow(clippy::too_many_arguments)] // same coordinate plumbing as refine_side
fn refine_inset<L: LumaAccess>(
    luma: &L,
    side: Side,
    start: u32,
    end: u32,
    x0: u32,
    x1: u32,
    y0: u32,
    y1: u32,
    cfg: &ScanConfig,
) -> Edge {
    let lo = start.min(end);
    let hi = start.max(end);
    let mut run = 0u32;
    let mut found = None;
    for inset in lo..=hi {
        match classify_inset(luma, side, inset, x0, x1, y0, y1, inset, cfg) {
            LineClass::Strong => {
                run += 1;
                if run >= MIN_CONSECUTIVE_ACTIVE && found.is_none() {
                    found = Some(inset.saturating_sub(MIN_CONSECUTIVE_ACTIVE - 1));
                    break;
                }
            }
            _ => run = 0,
        }
    }
    match found {
        Some(v) => Edge::Inset(v),
        None => {
            if lo == 0 {
                // Never found 3 consecutive active lines at the outer edge:
                // if the outer line itself is active, treat as no bar.
                match classify_inset(luma, side, 0, x0, x1, y0, y1, 0, cfg) {
                    LineClass::Strong => Edge::Inset(0),
                    _ => Edge::Unknown,
                }
            } else {
                Edge::Inset(lo)
            }
        }
    }
}

#[allow(clippy::too_many_arguments)] // same coordinate plumbing as refine_side
fn classify_inset<L: LumaAccess>(
    luma: &L,
    side: Side,
    inset: u32,
    x0: u32,
    x1: u32,
    y0: u32,
    y1: u32,
    phase: u32,
    cfg: &ScanConfig,
) -> LineClass {
    match side {
        Side::Top => {
            if inset >= luma.height() {
                return LineClass::Unknown;
            }
            classify_h_line(luma, inset, x0, x1, phase, cfg)
        }
        Side::Bottom => {
            let y = luma.height().saturating_sub(1).saturating_sub(inset);
            classify_h_line(luma, y, x0, x1, phase, cfg)
        }
        Side::Left => {
            if inset >= luma.width() {
                return LineClass::Unknown;
            }
            classify_v_line(luma, inset, y0, y1, phase, cfg)
        }
        Side::Right => {
            let x = luma.width().saturating_sub(1).saturating_sub(inset);
            classify_v_line(luma, x, y0, y1, phase, cfg)
        }
    }
}

fn classify_h_line<L: LumaAccess>(
    luma: &L,
    y: u32,
    x0: u32,
    x1: u32,
    phase: u32,
    cfg: &ScanConfig,
) -> LineClass {
    let len = x1.saturating_sub(x0);
    if len == 0 || y >= luma.height() {
        return LineClass::Unknown;
    }
    let n = MAX_SAMPLES_PER_LINE.min(len);
    classify_samples(n, cfg, |i| {
        luma.sample(x0 + sample_pos(len, n, i, phase), y)
    })
}

fn classify_v_line<L: LumaAccess>(
    luma: &L,
    x: u32,
    y0: u32,
    y1: u32,
    phase: u32,
    cfg: &ScanConfig,
) -> LineClass {
    let len = y1.saturating_sub(y0);
    if len == 0 || x >= luma.width() {
        return LineClass::Unknown;
    }
    let n = MAX_SAMPLES_PER_LINE.min(len);
    classify_samples(n, cfg, |i| {
        luma.sample(x, y0 + sample_pos(len, n, i, phase))
    })
}

fn classify_samples<F: Fn(u32) -> u16>(n: u32, cfg: &ScanConfig, sample: F) -> LineClass {
    if n == 0 {
        return LineClass::Unknown;
    }
    let black_budget = (cfg.active_tolerance.max(0.0) * n as f32 * WEIGHT_SCALE as f32) as u32;
    let strong_budget = (STRONG_ACTIVITY * n as f32 * WEIGHT_SCALE as f32) as u32;
    let mut acc = 0u32;
    for i in 0..n {
        acc += cfg.threshold.weight(sample(i));
        if acc > strong_budget {
            return LineClass::Strong;
        }
    }
    if acc <= black_budget {
        LineClass::Black
    } else {
        LineClass::Weak
    }
}

fn sample_pos(len: u32, n: u32, i: u32, phase: u32) -> u32 {
    if len == 0 || n == 0 {
        return 0;
    }
    let phase = (phase.wrapping_mul(37)) % len;
    let stride = len as u64;
    let pos = (i as u64 * stride / n as u64 + phase as u64) % stride;
    pos as u32
}

fn interior_is_active<L: LumaAccess>(luma: &L, cfg: &ScanConfig) -> bool {
    let w = luma.width();
    let h = luma.height();
    if w < 4 || h < 4 {
        return true;
    }
    let x0 = ((w as f32) * 0.45).floor() as u32;
    let x1 = ((w as f32) * 0.55).ceil() as u32;
    let y0 = ((h as f32) * 0.45).floor() as u32;
    let y1 = ((h as f32) * 0.55).ceil() as u32;
    let x0 = x0.min(w.saturating_sub(1));
    let y0 = y0.min(h.saturating_sub(1));
    let x1 = x1.max(x0.saturating_add(1)).min(w);
    let y1 = y1.max(y0.saturating_add(1)).min(h);
    let nx = 16u32.min(x1 - x0).max(1);
    let ny = 16u32.min(y1 - y0).max(1);
    let mut acc = 0u32;
    let mut n = 0u32;
    for iy in 0..ny {
        let y = y0 + iy * (y1 - y0) / ny;
        for ix in 0..nx {
            if n >= MAX_INTERIOR_PROBES {
                break;
            }
            let x = x0 + ix * (x1 - x0) / nx;
            acc += cfg.threshold.weight(luma.sample(x, y));
            n += 1;
        }
    }
    if n == 0 {
        return false;
    }
    let mean = acc / n;
    mean > (cfg.active_tolerance * WEIGHT_SCALE as f32) as u32
}

/// Expand `raw` outward so width/height meet `round` and chroma siting,
/// never shrinking the content rectangle. Falls back to a full dimension
/// when the constraints cannot be satisfied inside the frame.
pub(crate) fn align_outward(
    raw: CropRawBounds,
    frame_w: i32,
    frame_h: i32,
    round: u32,
    chroma: ChromaGrid,
) -> CropRawBounds {
    let (x, w) = fit_axis(
        raw.left,
        raw.right_exclusive - raw.left,
        frame_w,
        round,
        chroma.x_step(),
    );
    let (y, h) = fit_axis(
        raw.top,
        raw.bottom_exclusive - raw.top,
        frame_h,
        round,
        chroma.y_step(),
    );
    CropRawBounds {
        left: x,
        top: y,
        right_exclusive: x + w,
        bottom_exclusive: y + h,
    }
}

fn fit_axis(raw_pos: i32, raw_len: i32, frame_len: i32, round: u32, grid: i32) -> (i32, i32) {
    if frame_len <= 0 {
        return (0, 0);
    }
    if raw_len <= 0 {
        return (0, frame_len);
    }
    let multiple = if round <= 1 { 1 } else { round as i32 };
    let grid = grid.max(1);

    let mut len = raw_len;
    if multiple > 1 {
        len = div_ceil(len, multiple) * multiple;
    }
    if len > frame_len {
        return (0, frame_len);
    }

    let extra = len - raw_len;
    let mut pos = raw_pos - extra / 2;
    pos = contain_pos(pos, len, raw_pos, raw_len, frame_len);

    // Snap position down to the chroma grid (never move the origin inward).
    let snapped = pos - pos.rem_euclid(grid);
    let grow = pos - snapped;
    let mut pos2 = snapped;
    let mut len2 = len + grow;
    if multiple > 1 {
        len2 = div_ceil(len2, multiple) * multiple;
    }
    if pos2 < 0 || pos2 + len2 > frame_len || !contains(pos2, len2, raw_pos, raw_len) {
        // Try expanding only toward the inside of the frame.
        pos2 = pos - pos.rem_euclid(grid);
        if pos2 < 0 {
            pos2 = 0;
        }
        len2 = (raw_pos + raw_len) - pos2;
        if multiple > 1 {
            len2 = div_ceil(len2, multiple) * multiple;
        }
        if pos2 + len2 > frame_len || !contains(pos2, len2, raw_pos, raw_len) {
            return (0, frame_len);
        }
    }
    (pos2, len2)
}

fn div_ceil(a: i32, b: i32) -> i32 {
    if b <= 0 {
        return a;
    }
    (a + b - 1) / b
}

fn contains(pos: i32, len: i32, raw_pos: i32, raw_len: i32) -> bool {
    pos <= raw_pos && pos + len >= raw_pos + raw_len && pos >= 0
}

fn contain_pos(mut pos: i32, len: i32, raw_pos: i32, raw_len: i32, frame_len: i32) -> i32 {
    if pos < 0 {
        pos = 0;
    }
    if pos + len > frame_len {
        pos = frame_len - len;
    }
    if pos > raw_pos {
        pos = raw_pos;
    }
    if pos + len < raw_pos + raw_len {
        pos = raw_pos + raw_len - len;
    }
    if pos < 0 {
        pos = 0;
    }
    pos
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::core::analysis::crop::luma::{ChromaGrid, PatternLuma, SignalRange};

    fn band_8(limit: u16) -> ThresholdBand {
        resolve_threshold(
            CropLumaThreshold::Normalized(limit as f32 / 255.0),
            8,
            SignalRange::Limited,
            4.0 / 255.0,
        )
        .unwrap()
    }

    fn scan_pat(p: &PatternLuma, limit: u16) -> CropCandidate {
        let cfg = ScanConfig::standard(band_8(limit));
        scan_boundary_bands(p, &cfg).expect("candidate")
    }

    #[test]
    fn above_nominal_black_zero_is_nominal_black_not_full_frame() {
        let limited_8 = resolve_threshold(
            CropLumaThreshold::AboveNominalBlack(0.0),
            8,
            SignalRange::Limited,
            0.0,
        )
        .unwrap();
        assert!(!limited_8.all_active);
        assert_eq!(limited_8.low, 16);

        let limited_10 = resolve_threshold(
            CropLumaThreshold::AboveNominalBlack(0.0),
            10,
            SignalRange::Limited,
            0.0,
        )
        .unwrap();
        assert!(!limited_10.all_active);
        assert_eq!(limited_10.low, 64);

        let full_8 = resolve_threshold(
            CropLumaThreshold::AboveNominalBlack(0.0),
            8,
            SignalRange::Full,
            0.0,
        )
        .unwrap();
        assert!(!full_8.all_active);
        assert_eq!(full_8.low, 0);
    }

    #[test]
    fn limit_zero_is_full_frame_without_reads() {
        let p = PatternLuma::letterbox(320, 240, 40);
        p.reset_probes();
        let cfg = ScanConfig::standard(
            resolve_threshold(
                CropLumaThreshold::Normalized(0.0),
                8,
                SignalRange::Limited,
                4.0 / 255.0,
            )
            .unwrap(),
        );
        let c = scan_boundary_bands(&p, &cfg).unwrap();
        assert!(c.reliable);
        assert_eq!(c.raw.left, 0);
        assert_eq!(c.raw.top, 0);
        assert_eq!(c.raw.right_exclusive, 320);
        assert_eq!(c.raw.bottom_exclusive, 240);
        assert_eq!(p.probe_count(), 0);
    }

    #[test]
    fn letterbox_detects_top_bottom() {
        let p = PatternLuma::letterbox(320, 240, 40);
        let c = scan_pat(&p, 24);
        assert!(c.reliable);
        assert_eq!(c.raw.left, 0);
        assert_eq!(c.raw.right_exclusive, 320);
        assert!((c.raw.top - 40).abs() <= 2, "top={}", c.raw.top);
        assert!(
            (c.raw.bottom_exclusive - 200).abs() <= 2,
            "bottom={}",
            c.raw.bottom_exclusive
        );
    }

    #[test]
    fn pillarbox_detects_left_right() {
        let mut p = PatternLuma::letterbox(320, 240, 0);
        p.left = 48;
        p.right = 48;
        let c = scan_pat(&p, 24);
        assert!(c.reliable);
        assert!((c.raw.left - 48).abs() <= 2, "left={}", c.raw.left);
        assert!(
            (c.raw.right_exclusive - 272).abs() <= 2,
            "right={}",
            c.raw.right_exclusive
        );
        assert_eq!(c.raw.top, 0);
        assert_eq!(c.raw.bottom_exclusive, 240);
    }

    #[test]
    fn wide_pillarbox_still_crops_left_right() {
        // ~30% each side: full-width top/bottom lines are majority-black, so
        // the letterbox pass is Unknown. Left/right must still run.
        let mut p = PatternLuma::letterbox(320, 240, 0);
        p.left = 96;
        p.right = 96;
        let c = scan_pat(&p, 24);
        assert!(c.reliable);
        assert!((c.raw.left - 96).abs() <= 4, "left={}", c.raw.left);
        assert!(
            (c.raw.right_exclusive - 224).abs() <= 4,
            "right={}",
            c.raw.right_exclusive
        );
        assert_eq!(c.raw.top, 0);
        assert_eq!(c.raw.bottom_exclusive, 240);
    }

    #[test]
    fn windowbox_45_percent() {
        let p = PatternLuma::windowbox(320, 240, 0.45);
        let c = scan_pat(&p, 24);
        assert!(c.reliable);
        let w = c.raw.right_exclusive - c.raw.left;
        let h = c.raw.bottom_exclusive - c.raw.top;
        assert!(w <= 64, "w={w}");
        assert!(h <= 48, "h={h}");
        assert!(w >= 16 && h >= 12);
    }

    #[test]
    fn all_black_is_unreliable() {
        let mut p = PatternLuma::letterbox(80, 60, 0);
        p.content_code = 10;
        p.black_code = 10;
        let cfg = ScanConfig::standard(band_8(24));
        assert!(scan_boundary_bands(&p, &cfg).is_none());
    }

    #[test]
    fn vignette_returns_full_frame() {
        let mut p = PatternLuma::letterbox(120, 90, 0);
        p.vignette = true;
        p.black_code = 8;
        p.content_code = 180;
        let c = scan_pat(&p, 24);
        assert!(c.reliable);
        assert_eq!(c.raw.left, 0);
        assert_eq!(c.raw.top, 0);
        assert_eq!(c.raw.right_exclusive, 120);
        assert_eq!(c.raw.bottom_exclusive, 90);
    }

    #[test]
    fn salt_noise_two_percent_still_crops() {
        let mut p = PatternLuma::letterbox(320, 240, 32);
        p.noise_frac = 0.015;
        p.noise_seed = 7;
        let c = scan_pat(&p, 24);
        assert!(c.reliable);
        assert!((c.raw.top - 32).abs() <= 4, "top={}", c.raw.top);
    }

    #[test]
    fn ten_bit_legacy_24_matches_code_96() {
        let p8 = PatternLuma::letterbox(160, 120, 20);
        let p10 = PatternLuma::letterbox(160, 120, 20).with_depth(10);
        let c8 = scan_pat(&p8, 24);
        let cfg10 = ScanConfig::standard(
            resolve_threshold(
                CropLumaThreshold::Normalized(24.0 / 255.0),
                10,
                SignalRange::Limited,
                4.0 / 255.0,
            )
            .unwrap(),
        );
        assert_eq!(cfg10.threshold.low, 96);
        let c10 = scan_boundary_bands(&p10, &cfg10).unwrap();
        assert_eq!(c8.raw, c10.raw);
    }

    #[test]
    fn raw_code_1023_all_black_on_10bit() {
        let p = PatternLuma::letterbox(80, 60, 8).with_depth(10);
        let cfg = ScanConfig::standard(
            resolve_threshold(
                CropLumaThreshold::RawCode(1023),
                10,
                SignalRange::Limited,
                0.0,
            )
            .unwrap(),
        );
        assert_eq!(cfg.threshold.low, 1023);
        assert!(scan_boundary_bands(&p, &cfg).is_none());
    }

    #[test]
    fn normalized_half_scales_with_depth() {
        let b8 = resolve_threshold(
            CropLumaThreshold::Normalized(0.5),
            8,
            SignalRange::Full,
            0.0,
        )
        .unwrap();
        let b10 = resolve_threshold(
            CropLumaThreshold::Normalized(0.5),
            10,
            SignalRange::Full,
            0.0,
        )
        .unwrap();
        assert_eq!(b8.low, 128);
        assert_eq!(b10.low, 512);
    }

    #[test]
    fn legacy_bridge_300_is_raw_not_scaled() {
        match legacy_limit(300) {
            CropLumaThreshold::RawCode(v) => assert_eq!(v, 300),
            other => panic!("{other:?}"),
        }
        let b = resolve_threshold(legacy_limit(300), 10, SignalRange::Limited, 0.0).unwrap();
        assert_eq!(b.low, 300);
    }

    #[test]
    fn legacy_bridge_4096_saturates_to_1023() {
        let b = resolve_threshold(legacy_limit(4096), 10, SignalRange::Limited, 0.0).unwrap();
        assert_eq!(b.low, 1023);
    }

    #[test]
    fn align_round_16_contains_raw() {
        let raw = CropRawBounds {
            left: 13,
            top: 7,
            right_exclusive: 13 + 200,
            bottom_exclusive: 7 + 100,
        };
        let a = align_outward(raw, 320, 240, 16, ChromaGrid::None);
        assert!(a.left <= raw.left && a.top <= raw.top);
        assert!(a.right_exclusive >= raw.right_exclusive);
        assert!(a.bottom_exclusive >= raw.bottom_exclusive);
        let w = a.right_exclusive - a.left;
        let h = a.bottom_exclusive - a.top;
        assert_eq!(w % 16, 0);
        assert_eq!(h % 16, 0);
        assert!(a.left >= 0 && a.top >= 0);
        assert!(a.right_exclusive <= 320 && a.bottom_exclusive <= 240);
    }

    #[test]
    fn align_round_zero_only_chroma() {
        let raw = CropRawBounds {
            left: 5,
            top: 3,
            right_exclusive: 25,
            bottom_exclusive: 21,
        };
        let a = align_outward(raw, 64, 64, 0, ChromaGrid::Yuv420);
        assert_eq!(a.left % 2, 0);
        assert_eq!(a.top % 2, 0);
        assert!(a.left <= 5 && a.top <= 3);
        assert!(a.right_exclusive >= 25 && a.bottom_exclusive >= 21);
    }

    #[test]
    fn align_round_bigger_than_frame_is_full() {
        let raw = CropRawBounds {
            left: 10,
            top: 10,
            right_exclusive: 50,
            bottom_exclusive: 50,
        };
        let a = align_outward(raw, 80, 60, 128, ChromaGrid::None);
        assert_eq!(a.left, 0);
        assert_eq!(a.top, 0);
        assert_eq!(a.right_exclusive, 80);
        assert_eq!(a.bottom_exclusive, 60);
    }

    #[test]
    fn probe_cap_1080p_and_4k() {
        for (w, h, cap) in [(1920u32, 1080u32, 80_000u32), (3840, 2160, 100_000)] {
            let p = PatternLuma::windowbox(w, h, 0.45);
            p.reset_probes();
            let _ = scan_pat(&p, 24);
            assert!(
                p.probe_count() <= cap,
                "{w}x{h} probes={} cap={cap}",
                p.probe_count()
            );
            let p2 = PatternLuma::letterbox(w, h, 0);
            p2.reset_probes();
            let _ = scan_pat(&p2, 24);
            assert!(
                p2.probe_count() <= cap,
                "no-border {w}x{h} probes={}",
                p2.probe_count()
            );
        }
    }
}
