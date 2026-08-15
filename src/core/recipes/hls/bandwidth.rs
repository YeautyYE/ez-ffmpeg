//! RFC 8216 measured peak `BANDWIDTH` from a completed media playlist.

use std::path::{Path, PathBuf};

use crate::error::{Error, Result};

/// Maximum decimal digits accepted in an `EXTINF` duration.
const MAX_EXTINF_SCALE: u32 = 9;

#[derive(Debug, Clone)]
struct PlaylistSegment {
    /// Duration in units of `10^{-scale}` seconds.
    duration: u128,
    bytes: u64,
    uri: String,
}

#[derive(Debug, Clone)]
struct ParsedPlaylist {
    /// `EXT-X-TARGETDURATION` in the same units as segment durations.
    target: u128,
    scale: u32,
    segments: Vec<PlaylistSegment>,
}

/// Measured peak bit rate (bps) for one rendition media playlist.
///
/// Reads only relative segment URIs under `playlist_path`'s directory.
pub(super) fn measured_peak_bps(playlist_path: &Path) -> Result<u64> {
    let text = std::fs::read_to_string(playlist_path).map_err(|e| {
        Error::InvalidRecipeArg(format!(
            "failed to read media playlist '{}': {e}",
            playlist_path.display()
        ))
    })?;
    let dir = playlist_path.parent().ok_or_else(|| {
        Error::InvalidRecipeArg(format!(
            "media playlist '{}' has no parent directory",
            playlist_path.display()
        ))
    })?;
    measured_peak_bps_from_text(&text, dir)
}

pub(super) fn measured_peak_bps_from_text(text: &str, playlist_dir: &Path) -> Result<u64> {
    let parsed = parse_media_playlist(text)?;
    if parsed.segments.is_empty() {
        return Err(Error::InvalidRecipeArg(
            "HLS media playlist contains no segments; cannot measure BANDWIDTH".to_string(),
        ));
    }
    let mut with_bytes = Vec::with_capacity(parsed.segments.len());
    for segment in &parsed.segments {
        let path = resolve_segment_path(playlist_dir, &segment.uri)?;
        let bytes = std::fs::metadata(&path)
            .map_err(|e| {
                Error::InvalidRecipeArg(format!(
                    "failed to stat HLS segment '{}': {e}",
                    path.display()
                ))
            })?
            .len();
        with_bytes.push(PlaylistSegment {
            duration: segment.duration,
            bytes,
            uri: segment.uri.clone(),
        });
    }
    peak_from_segments(&with_bytes, parsed.target, parsed.scale)
}

/// Same algorithm as [`measured_peak_bps_from_text`], with caller-supplied
/// sizes so unit tests do not touch the filesystem.
fn peak_from_segments(segments: &[PlaylistSegment], target: u128, scale: u32) -> Result<u64> {
    if segments.is_empty() {
        return Err(Error::InvalidRecipeArg(
            "HLS media playlist contains no segments; cannot measure BANDWIDTH".to_string(),
        ));
    }

    let mut peak: Option<u128> = None;
    let n = segments.len();
    for i in 0..n {
        let mut sum_dur: u128 = 0;
        let mut sum_bytes: u128 = 0;
        for segment in segments.iter().skip(i) {
            sum_dur = sum_dur.checked_add(segment.duration).ok_or_else(|| {
                Error::InvalidRecipeArg("HLS BANDWIDTH duration sum overflowed".to_string())
            })?;
            sum_bytes = sum_bytes
                .checked_add(u128::from(segment.bytes))
                .ok_or_else(|| {
                    Error::InvalidRecipeArg("HLS BANDWIDTH byte sum overflowed".to_string())
                })?;
            if window_contains(sum_dur, target)? {
                let rate = ceil_bps(sum_bytes, sum_dur, scale)?;
                peak = Some(peak.map_or(rate, |p| p.max(rate)));
            } else if window_exceeds_upper_cap(sum_dur, target)? {
                // Duration only grows along this start. Once past 1.5×
                // TARGETDURATION, no later suffix can re-enter the RFC window.
                break;
            }
        }
    }

    let peak = match peak {
        Some(p) => p,
        None => {
            // No contiguous set fell in the RFC window: conservative
            // single-segment maximum, never the configured-bitrate formula.
            let mut max_rate: u128 = 0;
            for segment in segments {
                let rate = ceil_bps(u128::from(segment.bytes), segment.duration, scale)?;
                max_rate = max_rate.max(rate);
            }
            max_rate
        }
    };

    u64::try_from(peak)
        .map_err(|_| Error::InvalidRecipeArg("HLS measured BANDWIDTH overflowed u64".to_string()))
}

/// `0.5 * target <= sum <= 1.5 * target` using only integer arithmetic:
/// `target <= 2*sum && 2*sum <= 3*target`.
fn window_contains(sum_dur: u128, target: u128) -> Result<bool> {
    let (twice, three_target) = window_bounds(sum_dur, target)?;
    Ok(twice >= target && twice <= three_target)
}

/// `sum > 1.5 * target` (`2*sum > 3*target`). Duration is monotonic along a
/// start, so the inner scan can stop once this is true.
fn window_exceeds_upper_cap(sum_dur: u128, target: u128) -> Result<bool> {
    let (twice, three_target) = window_bounds(sum_dur, target)?;
    Ok(twice > three_target)
}

fn window_bounds(sum_dur: u128, target: u128) -> Result<(u128, u128)> {
    let twice = sum_dur.checked_mul(2).ok_or_else(|| {
        Error::InvalidRecipeArg("HLS BANDWIDTH window comparison overflowed".to_string())
    })?;
    let three_target = target.checked_mul(3).ok_or_else(|| {
        Error::InvalidRecipeArg("HLS BANDWIDTH window comparison overflowed".to_string())
    })?;
    Ok((twice, three_target))
}

/// `ceil(bytes * 8 / duration_seconds)` with duration stored as
/// `duration / 10^scale` seconds: `ceil(bytes * 8 * 10^scale / duration)`.
fn ceil_bps(bytes: u128, duration: u128, scale: u32) -> Result<u128> {
    if duration == 0 {
        return Err(Error::InvalidRecipeArg(
            "HLS segment duration must be positive".to_string(),
        ));
    }
    let scale_factor = pow10(scale)?;
    let numer = bytes
        .checked_mul(8)
        .and_then(|v| v.checked_mul(scale_factor))
        .ok_or_else(|| Error::InvalidRecipeArg("HLS BANDWIDTH bit-rate overflowed".to_string()))?;
    Ok(numer.div_ceil(duration))
}

fn pow10(scale: u32) -> Result<u128> {
    10u128
        .checked_pow(scale)
        .ok_or_else(|| Error::InvalidRecipeArg("HLS duration scale overflowed".to_string()))
}

fn parse_media_playlist(text: &str) -> Result<ParsedPlaylist> {
    let mut target_raw: Option<(u64, u32)> = None;
    let mut pending_extinf: Option<(u64, u32)> = None;
    let mut raw_segments: Vec<((u64, u32), String)> = Vec::new();

    for raw_line in text.lines() {
        let line = raw_line.trim();
        if line.is_empty() {
            continue;
        }
        if let Some(rest) = line.strip_prefix("#EXT-X-TARGETDURATION:") {
            let parsed = parse_decimal(rest.trim(), "EXT-X-TARGETDURATION")?;
            if parsed.0 == 0 {
                return Err(Error::InvalidRecipeArg(
                    "EXT-X-TARGETDURATION must be positive".to_string(),
                ));
            }
            target_raw = Some(parsed);
            continue;
        }
        if let Some(rest) = line.strip_prefix("#EXTINF:") {
            let duration_token = rest.split(',').next().unwrap_or(rest).trim();
            pending_extinf = Some(parse_decimal(duration_token, "EXTINF")?);
            continue;
        }
        if line.starts_with('#') {
            continue;
        }
        let Some(dur) = pending_extinf.take() else {
            return Err(Error::InvalidRecipeArg(format!(
                "HLS playlist URI '{line}' is not preceded by EXTINF"
            )));
        };
        if dur.0 == 0 {
            return Err(Error::InvalidRecipeArg(
                "EXTINF duration must be positive".to_string(),
            ));
        }
        raw_segments.push((dur, line.to_string()));
    }

    if pending_extinf.is_some() {
        return Err(Error::InvalidRecipeArg(
            "EXTINF is not followed by a segment URI".to_string(),
        ));
    }
    let Some(target_raw) = target_raw else {
        return Err(Error::InvalidRecipeArg(
            "HLS media playlist is missing EXT-X-TARGETDURATION".to_string(),
        ));
    };

    let mut scale = target_raw.1;
    for (dur, _) in &raw_segments {
        scale = scale.max(dur.1);
    }

    let target = rescale(target_raw.0, target_raw.1, scale)?;
    let mut segments = Vec::with_capacity(raw_segments.len());
    for (dur, uri) in raw_segments {
        segments.push(PlaylistSegment {
            duration: rescale(dur.0, dur.1, scale)?,
            bytes: 0,
            uri,
        });
    }
    Ok(ParsedPlaylist {
        target,
        scale,
        segments,
    })
}

/// Parse a non-negative decimal with at most 9 fractional digits.
/// Rejects sign, exponent, empty, and extra characters.
fn parse_decimal(token: &str, what: &str) -> Result<(u64, u32)> {
    if token.is_empty()
        || token.contains('e')
        || token.contains('E')
        || token.contains('+')
        || token.contains('-')
        || token.chars().any(|c| c != '.' && !c.is_ascii_digit())
    {
        return Err(Error::InvalidRecipeArg(format!(
            "{what} value '{token}' is not a non-negative decimal"
        )));
    }
    let (int_part, frac_part) = match token.split_once('.') {
        Some((i, f)) => (i, f),
        None => (token, ""),
    };
    if int_part.is_empty() && frac_part.is_empty() {
        return Err(Error::InvalidRecipeArg(format!(
            "{what} value '{token}' is not a non-negative decimal"
        )));
    }
    if frac_part.len() > MAX_EXTINF_SCALE as usize {
        return Err(Error::InvalidRecipeArg(format!(
            "{what} value '{token}' has more than {MAX_EXTINF_SCALE} decimal digits"
        )));
    }
    let int_part = if int_part.is_empty() { "0" } else { int_part };
    let integer: u64 = int_part.parse().map_err(|_| {
        Error::InvalidRecipeArg(format!(
            "{what} value '{token}' is not a non-negative decimal"
        ))
    })?;
    let scale = frac_part.len() as u32;
    let frac: u64 = if frac_part.is_empty() {
        0
    } else {
        frac_part.parse().map_err(|_| {
            Error::InvalidRecipeArg(format!(
                "{what} value '{token}' is not a non-negative decimal"
            ))
        })?
    };
    let factor = 10u64
        .checked_pow(scale)
        .ok_or_else(|| Error::InvalidRecipeArg(format!("{what} value '{token}' overflowed")))?;
    let value = integer
        .checked_mul(factor)
        .and_then(|v| v.checked_add(frac))
        .ok_or_else(|| Error::InvalidRecipeArg(format!("{what} value '{token}' overflowed")))?;
    Ok((value, scale))
}

fn rescale(value: u64, from_scale: u32, to_scale: u32) -> Result<u128> {
    if to_scale < from_scale {
        return Err(Error::InvalidRecipeArg(
            "internal HLS duration rescale underflow".to_string(),
        ));
    }
    let factor = pow10(to_scale - from_scale)?;
    u128::from(value)
        .checked_mul(factor)
        .ok_or_else(|| Error::InvalidRecipeArg("HLS duration rescale overflowed".to_string()))
}

pub(super) fn resolve_segment_path(playlist_dir: &Path, uri: &str) -> Result<PathBuf> {
    if uri.is_empty() {
        return Err(Error::InvalidRecipeArg(
            "HLS segment URI must not be empty".to_string(),
        ));
    }
    if uri.contains("://") || uri.contains(':') {
        return Err(Error::InvalidRecipeArg(format!(
            "HLS segment URI '{uri}' must be a relative path without a scheme"
        )));
    }
    if uri.contains("..") {
        return Err(Error::InvalidRecipeArg(format!(
            "HLS segment URI '{uri}' must not contain '..'"
        )));
    }
    if uri.starts_with('/') || uri.starts_with('\\') || Path::new(uri).is_absolute() {
        return Err(Error::InvalidRecipeArg(format!(
            "HLS segment URI '{uri}' must not be absolute"
        )));
    }
    let path = playlist_dir.join(uri);
    match (playlist_dir.canonicalize(), path.canonicalize()) {
        (Ok(base), Ok(joined)) => {
            if !joined.starts_with(&base) {
                return Err(Error::InvalidRecipeArg(format!(
                    "HLS segment URI '{uri}' resolves outside the rendition directory"
                )));
            }
            Ok(joined)
        }
        _ => {
            // Canonicalize can fail when a path is missing. Fail closed
            // unless lexical normalization still proves containment (`..`
            // in the URI is already rejected above).
            if contained_by_components(playlist_dir, &path) {
                Ok(path)
            } else {
                Err(Error::InvalidRecipeArg(format!(
                    "HLS segment URI '{uri}' could not be verified to stay inside the rendition directory"
                )))
            }
        }
    }
}

/// Lexical containment after dropping `.` and refusing unresolvable `..`.
fn contained_by_components(playlist_dir: &Path, path: &Path) -> bool {
    match (lexical_normalized(playlist_dir), lexical_normalized(path)) {
        (Some(base), Some(joined)) => joined.starts_with(&base),
        _ => false,
    }
}

fn lexical_normalized(path: &Path) -> Option<PathBuf> {
    use std::path::Component;
    let mut out = PathBuf::new();
    for component in path.components() {
        match component {
            Component::CurDir => {}
            Component::ParentDir => match out.components().next_back() {
                Some(Component::Normal(_)) => {
                    let _ = out.pop();
                }
                _ => return None,
            },
            other => out.push(other),
        }
    }
    Some(out)
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::fs;
    use std::io::Write;

    fn seg(duration: u128, bytes: u64) -> PlaylistSegment {
        PlaylistSegment {
            duration,
            bytes,
            uri: "seg.ts".into(),
        }
    }

    #[test]
    fn single_segment_peak() {
        // 1_000_000 bytes over 1.0s at scale 0 → 8_000_000 bps.
        assert_eq!(
            peak_from_segments(&[seg(1, 1_000_000)], 1, 0).unwrap(),
            8_000_000
        );
    }

    #[test]
    fn multi_segment_contiguous_window() {
        // Two 1s segments of 100 and 300 bytes, target=1, scale=0.
        // Windows: [100] → 800 bps, [100+300] duration 2 is outside 0.5–1.5,
        // [300] → 2400 bps. Peak 2400.
        let segs = [seg(1, 100), seg(1, 300)];
        assert_eq!(peak_from_segments(&segs, 1, 0).unwrap(), 2400);
    }

    #[test]
    fn window_includes_half_and_one_and_a_half_target() {
        // scale 1 so target 10 = 1.0s. sum=5 (0.5s) and sum=15 (1.5s) are included.
        let half = [seg(5, 100)];
        assert_eq!(peak_from_segments(&half, 10, 1).unwrap(), 1600); // ceil(100*8*10/5)=1600
        let one_half = [seg(15, 100)];
        assert_eq!(peak_from_segments(&one_half, 10, 1).unwrap(), 534); // ceil(8000/15)=534
    }

    #[test]
    fn window_excludes_just_outside() {
        // sum=4 with target=10 at scale 1 is 0.4s < 0.5s, so no RFC window.
        // Degenerate path: max single-segment rate.
        let segs = [seg(4, 100)];
        assert_eq!(peak_from_segments(&segs, 10, 1).unwrap(), 2000); // ceil(8000/4)
    }

    #[test]
    fn mixed_decimal_scale_unifies() {
        let text = "#EXTM3U\n#EXT-X-TARGETDURATION:6\n#EXTINF:6.0,\na.ts\n#EXTINF:6.000,\nb.ts\n";
        let parsed = parse_media_playlist(text).unwrap();
        assert_eq!(parsed.scale, 3);
        assert_eq!(parsed.target, 6000);
        assert_eq!(parsed.segments[0].duration, 6000);
        assert_eq!(parsed.segments[1].duration, 6000);
    }

    #[test]
    fn rejects_more_than_nine_decimals() {
        let err = parse_decimal("1.1234567890", "EXTINF").unwrap_err();
        assert!(err.to_string().contains("9 decimal"));
    }

    #[test]
    fn rejects_exponent() {
        assert!(parse_decimal("1e3", "EXTINF").is_err());
        assert!(parse_decimal("6E0", "EXTINF").is_err());
    }

    #[test]
    fn empty_playlist_errors() {
        let text = "#EXTM3U\n#EXT-X-TARGETDURATION:6\n";
        let parsed = parse_media_playlist(text).unwrap();
        assert!(parsed.segments.is_empty());
        let err = peak_from_segments(&[], parsed.target, parsed.scale).unwrap_err();
        assert!(err.to_string().contains("no segments"));
    }

    #[test]
    fn ceil_rounds_up() {
        // 1 byte over 3 seconds, scale 0: ceil(8/3)=3.
        assert_eq!(peak_from_segments(&[seg(3, 1)], 3, 0).unwrap(), 3);
    }

    #[test]
    fn rejects_bad_uris() {
        let dir = Path::new("/tmp");
        assert!(resolve_segment_path(dir, "https://evil/seg.ts").is_err());
        assert!(resolve_segment_path(dir, "../seg.ts").is_err());
        assert!(resolve_segment_path(dir, "/abs/seg.ts").is_err());
        assert!(resolve_segment_path(dir, "file:seg.ts").is_err());
        assert!(resolve_segment_path(dir, "").is_err());
        assert!(resolve_segment_path(dir, "seg_00000.ts").is_ok());
    }

    #[test]
    fn missing_playlist_dir_accepts_contained_relative_uri() {
        let dir = Path::new("/this/does/not/exist/ez-ffmpeg-hls-containment-test");
        assert!(!dir.exists());
        let resolved = resolve_segment_path(dir, "seg.ts").expect("lexical containment");
        assert!(resolved.ends_with("seg.ts"));
    }

    #[test]
    fn missing_playlist_dir_still_rejects_parent_escape() {
        let dir = Path::new("/this/does/not/exist/ez-ffmpeg-hls-containment-test");
        assert!(resolve_segment_path(dir, "../seg.ts").is_err());
    }

    #[test]
    fn duration_overflow_errors() {
        let segs = [seg(u128::MAX, 1), seg(1, 1)];
        assert!(peak_from_segments(&segs, 1, 0).is_err());
    }

    /// Exhaustive RFC scan: every contiguous suffix of every start, with no
    /// early break. Used to prove the 1.5×-cap break does not change the peak.
    fn peak_from_segments_exhaustive(
        segments: &[PlaylistSegment],
        target: u128,
        scale: u32,
    ) -> Result<u64> {
        if segments.is_empty() {
            return Err(Error::InvalidRecipeArg(
                "HLS media playlist contains no segments; cannot measure BANDWIDTH".to_string(),
            ));
        }

        let mut peak: Option<u128> = None;
        let n = segments.len();
        for i in 0..n {
            let mut sum_dur: u128 = 0;
            let mut sum_bytes: u128 = 0;
            for segment in segments.iter().skip(i) {
                sum_dur = sum_dur.checked_add(segment.duration).ok_or_else(|| {
                    Error::InvalidRecipeArg("HLS BANDWIDTH duration sum overflowed".to_string())
                })?;
                sum_bytes = sum_bytes
                    .checked_add(u128::from(segment.bytes))
                    .ok_or_else(|| {
                        Error::InvalidRecipeArg("HLS BANDWIDTH byte sum overflowed".to_string())
                    })?;
                if window_contains(sum_dur, target)? {
                    let rate = ceil_bps(sum_bytes, sum_dur, scale)?;
                    peak = Some(peak.map_or(rate, |p| p.max(rate)));
                }
            }
        }

        let peak = match peak {
            Some(p) => p,
            None => {
                let mut max_rate: u128 = 0;
                for segment in segments {
                    let rate = ceil_bps(u128::from(segment.bytes), segment.duration, scale)?;
                    max_rate = max_rate.max(rate);
                }
                max_rate
            }
        };

        u64::try_from(peak).map_err(|_| {
            Error::InvalidRecipeArg("HLS measured BANDWIDTH overflowed u64".to_string())
        })
    }

    #[test]
    fn long_playlist_early_break_matches_exhaustive_peak() {
        // target=6, scale=0 → RFC window is 3..=9 seconds. Forty 1s segments
        // would waste O(n) inner steps per start without the 1.5× break.
        // A late burst (segments 20..=22) is the true peak; a trailing
        // over-cap tail must not change membership.
        let mut segs: Vec<PlaylistSegment> = (0..40).map(|_| seg(1, 100)).collect();
        segs[20] = seg(1, 10_000);
        segs[21] = seg(1, 10_000);
        segs[22] = seg(1, 10_000);
        segs.push(seg(20, 1));

        let early = peak_from_segments(&segs, 6, 0).unwrap();
        let exhaustive = peak_from_segments_exhaustive(&segs, 6, 0).unwrap();
        assert_eq!(early, exhaustive);
        // 3×10_000 bytes over 3s at scale 0 → 80_000 bps.
        assert_eq!(early, 80_000);
        assert!(window_exceeds_upper_cap(10, 6).unwrap());
        assert!(!window_contains(10, 6).unwrap());
    }

    #[test]
    fn filesystem_peak_matches_algorithm() {
        let tmp = std::env::temp_dir().join(format!(
            "ez-ffmpeg-hls-bw-{}-{}",
            std::process::id(),
            std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .unwrap()
                .as_nanos()
        ));
        fs::create_dir_all(&tmp).unwrap();
        let seg_path = tmp.join("seg_00000.ts");
        {
            let mut f = fs::File::create(&seg_path).unwrap();
            f.write_all(&[0u8; 1000]).unwrap();
        }
        let playlist = tmp.join("index.m3u8");
        fs::write(
            &playlist,
            "#EXTM3U\n#EXT-X-TARGETDURATION:1\n#EXTINF:1,\nseg_00000.ts\n#EXT-X-ENDLIST\n",
        )
        .unwrap();
        assert_eq!(measured_peak_bps(&playlist).unwrap(), 8000);
        let _ = fs::remove_file(&seg_path);
        let _ = fs::remove_file(&playlist);
        let _ = fs::remove_dir(&tmp);
    }
}
