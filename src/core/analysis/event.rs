//! Typed detection/measurement events and the `lavfi.*` metadata parser.
//!
//! FFmpeg's detector filters attach their result to each frame as `lavfi.*`
//! metadata (an `AVDictionary`). [`parse_frame_metadata`] reads that dictionary
//! off a decoded/filtered frame and turns the recognised keys into typed
//! [`MetadataEvent`]s. The metadata keys below were cross-checked against the
//! FFmpeg n7.1 sources for `blackdetect`, `silencedetect`, `scdet`,
//! `cropdetect` and `ebur128`.

use ffmpeg_next::DictionaryRef;
use ffmpeg_sys_next::AVMediaType::{self, AVMEDIA_TYPE_AUDIO, AVMEDIA_TYPE_VIDEO};
use ffmpeg_sys_next::{av_rescale_q, AVRational};

// ---- lavfi metadata keys (verified against FFmpeg n7.1) --------------------

const BLACK_START: &str = "lavfi.black_start";
const BLACK_END: &str = "lavfi.black_end";
const SILENCE_START: &str = "lavfi.silence_start";
const SILENCE_END: &str = "lavfi.silence_end";
const SILENCE_DURATION: &str = "lavfi.silence_duration";
const SCD_SCORE: &str = "lavfi.scd.score";
const SCD_TIME: &str = "lavfi.scd.time";
const CROP_X: &str = "lavfi.cropdetect.x";
const CROP_Y: &str = "lavfi.cropdetect.y";
const CROP_W: &str = "lavfi.cropdetect.w";
const CROP_H: &str = "lavfi.cropdetect.h";
const R128_M: &str = "lavfi.r128.M";
const R128_S: &str = "lavfi.r128.S";
const R128_I: &str = "lavfi.r128.I";
const R128_LRA: &str = "lavfi.r128.LRA";
/// Aggregate true-peak key emitted by `ebur128` when `peak=true` is set. Its
/// `SET_META_PEAK` macro (verified against FFmpeg n7.1 `f_ebur128.c`) writes
/// `lavfi.r128.true_peak` — the max across channels — alongside the per-channel
/// `lavfi.r128.true_peaks_chN` keys, so we read the aggregate directly.
const R128_TRUE_PEAK: &str = "lavfi.r128.true_peak";

/// Microseconds per second, used as the rescale target for pts conversion.
const US_PER_SEC: AVRational = AVRational {
    num: 1,
    den: 1_000_000,
};

/// A detection event timestamp, normalised to microseconds.
///
/// Metadata-derived events (`black`, `silence`, `scd`) carry only a seconds
/// string, so [`pts`](Self::pts)/[`time_base`](Self::time_base) are `None`.
/// Per-frame measurement events (`r128`, `cropdetect`) are stamped from the
/// raw frame `pts` + `time_base`, so both are populated.
#[derive(Debug, Clone, Copy, PartialEq)]
pub struct Timestamp {
    /// Authoritative time in microseconds.
    pub time_us: i64,
    /// Raw presentation timestamp, when the event came from a frame pts.
    pub pts: Option<i64>,
    /// Raw `(num, den)` time base, when the event came from a frame pts.
    pub time_base: Option<(i32, i32)>,
}

impl Timestamp {
    /// Builds a timestamp from a seconds value (e.g. a `lavfi.black_start`
    /// string parsed to `f64`).
    ///
    /// Returns `None` for non-finite input (`NaN`/`±inf`); finite values are
    /// **rounded** to the nearest microsecond rather than truncated.
    pub fn from_secs(s: f64) -> Option<Self> {
        if !s.is_finite() {
            return None;
        }
        Some(Self {
            time_us: (s * 1e6).round() as i64,
            pts: None,
            time_base: None,
        })
    }

    /// Builds a timestamp from a raw frame `pts` and `time_base`, converting to
    /// microseconds with `av_rescale_q` for full precision. The caller must
    /// ensure `pts` is valid (not `AV_NOPTS_VALUE`) and `time_base.1 != 0`.
    pub fn from_pts(pts: i64, time_base: (i32, i32)) -> Self {
        let tb = AVRational {
            num: time_base.0,
            den: time_base.1,
        };
        // SAFETY: pure arithmetic FFI on plain integers; no pointers involved.
        let time_us = unsafe { av_rescale_q(pts, tb, US_PER_SEC) };
        Self {
            time_us,
            pts: Some(pts),
            time_base: Some(time_base),
        }
    }

    /// The timestamp as fractional seconds.
    pub fn as_secs_f64(&self) -> f64 {
        self.time_us as f64 / 1e6
    }
}

/// A single typed detection/measurement result decoded from frame metadata.
///
/// All loudness fields are `Option<f64>`: `ebur128` may not have produced a
/// value yet, and `true_peak` is only `Some` when the build emitted the
/// per-channel true-peak keys (i.e. `peak=true` was requested *and* the keys
/// are present).
#[derive(Debug, Clone, PartialEq)]
pub enum MetadataEvent {
    /// A black region began at `at` (`blackdetect`).
    BlackStart { at: Timestamp },
    /// A black region ended at `at`; `duration_us` is `end - start`.
    BlackEnd { at: Timestamp, duration_us: i64 },
    /// A silent region began (`silencedetect`). `channel_number` is the
    /// 1-based channel from the `mono=1` `.N` key suffix, or `None` in
    /// combined mode.
    SilenceStart {
        at: Timestamp,
        channel_number: Option<usize>,
    },
    /// A silent region ended; `duration_us` from `lavfi.silence_duration`.
    SilenceEnd {
        at: Timestamp,
        duration_us: i64,
        channel_number: Option<usize>,
    },
    /// A scene change was detected (`scdet`); emitted only on frames that
    /// carry `lavfi.scd.time`. `score` is the `lavfi.scd.score` value.
    SceneChange { at: Timestamp, score: f64 },
    /// A crop suggestion for this frame (`cropdetect`).
    CropDetect {
        at: Timestamp,
        x: i32,
        y: i32,
        w: i32,
        h: i32,
    },
    /// Per-frame EBU R128 measurement (`ebur128 metadata=1`).
    R128Frame {
        at: Timestamp,
        momentary: Option<f64>,
        short_term: Option<f64>,
        integrated: Option<f64>,
        lra: Option<f64>,
        true_peak: Option<f64>,
    },
    /// Final R128 values (last frame seen), emitted once at end of stream.
    R128Summary {
        integrated: Option<f64>,
        lra: Option<f64>,
        true_peak: Option<f64>,
    },
    /// End-of-stream marker carrying the last frame timestamp, used by the
    /// folder to close regions that never received an explicit end.
    StreamEnd { media: AVMediaType, at: Timestamp },
}

/// Mutable state threaded through [`parse_frame_metadata`] across frames.
///
/// Remembers the open `black`/`silence` starts (so an end event can carry a
/// duration) and the most recent R128 values (for the end-of-stream summary).
#[derive(Default)]
pub(crate) struct ParseState {
    pending_black_start: Option<i64>,
    pending_silence: Vec<(Option<usize>, i64)>,
    last_ts: Option<Timestamp>,
    last_end_ts: Option<Timestamp>,
    last_integrated: Option<f64>,
    last_lra: Option<f64>,
    last_true_peak: Option<f64>,
}

impl ParseState {
    /// End-of-stream events, best-effort: a [`MetadataEvent::StreamEnd`] (if any
    /// frame was seen) and a [`MetadataEvent::R128Summary`] (if R128 data was
    /// seen). Called from the filter's `uninit`.
    pub(crate) fn flush(&mut self, media: AVMediaType) -> Vec<MetadataEvent> {
        let mut out = Vec::new();
        // Close unterminated regions at the true end of stream (the last frame's
        // pts + duration, recorded by the filter) rather than at the last
        // frame's start pts, which would under-report the tail by one frame.
        if let Some(at) = self.last_end_ts.or(self.last_ts) {
            out.push(MetadataEvent::StreamEnd { media, at });
        }
        if self.last_integrated.is_some() || self.last_lra.is_some() || self.last_true_peak.is_some()
        {
            out.push(MetadataEvent::R128Summary {
                integrated: self.last_integrated,
                lra: self.last_lra,
                true_peak: self.last_true_peak,
            });
        }
        out
    }

    /// Records the end timestamp (pts + duration) of the most recent frame, so
    /// [`flush`](Self::flush) can close unterminated black/silence regions at
    /// the true end of stream. Called by the filter, which owns the raw frame.
    pub(crate) fn record_frame_end(&mut self, end: Timestamp) {
        self.last_end_ts = Some(end);
    }
}

/// Parses one frame's `lavfi.*` metadata into typed events.
///
/// `frame_ts` is the frame's pts-derived [`Timestamp`] (or `None` when the
/// frame has no usable pts); it stamps the per-frame `r128`/`cropdetect`
/// events and is also recorded as the latest timestamp for end-of-stream
/// summarisation. `media` selects which detector family to look for.
pub(crate) fn parse_frame_metadata(
    md: &DictionaryRef<'_>,
    frame_ts: Option<Timestamp>,
    media: AVMediaType,
    out: &mut Vec<MetadataEvent>,
    state: &mut ParseState,
) {
    if let Some(ts) = frame_ts {
        state.last_ts = Some(ts);
    }

    match media {
        AVMEDIA_TYPE_VIDEO => {
            parse_black(md, out, state);
            parse_scene(md, out);
            parse_crop(md, frame_ts, out);
        }
        AVMEDIA_TYPE_AUDIO => {
            parse_silence(md, out, state);
            parse_r128(md, frame_ts, out, state);
        }
        _ => {}
    }
}

fn parse_black(md: &DictionaryRef<'_>, out: &mut Vec<MetadataEvent>, state: &mut ParseState) {
    if let Some(at) = md.get(BLACK_START).and_then(parse_secs) {
        state.pending_black_start = Some(at.time_us);
        out.push(MetadataEvent::BlackStart { at });
    }
    if let Some(at) = md.get(BLACK_END).and_then(parse_secs) {
        let duration_us = state
            .pending_black_start
            .take()
            .map_or(0, |start| at.time_us.saturating_sub(start));
        out.push(MetadataEvent::BlackEnd { at, duration_us });
    }
}

fn parse_silence(md: &DictionaryRef<'_>, out: &mut Vec<MetadataEvent>, state: &mut ParseState) {
    // Iterate: `mono=1` emits one `.N`-suffixed key per silent channel, so a
    // single frame can carry several starts/ends.
    for (key, value) in md.iter() {
        if let Some(suffix) = key.strip_prefix(SILENCE_START) {
            let channel_number = parse_channel_suffix(suffix);
            if let Some(at) = parse_secs(value) {
                state.pending_silence.push((channel_number, at.time_us));
                out.push(MetadataEvent::SilenceStart {
                    at,
                    channel_number,
                });
            }
        } else if let Some(suffix) = key.strip_prefix(SILENCE_END) {
            let channel_number = parse_channel_suffix(suffix);
            if let Some(at) = parse_secs(value) {
                let removed = remove_pending(&mut state.pending_silence, channel_number);
                let duration_us = md
                    .get(&format!("{SILENCE_DURATION}{suffix}"))
                    .and_then(parse_f64)
                    .and_then(secs_to_us)
                    .or_else(|| removed.map(|start| at.time_us.saturating_sub(start)))
                    .unwrap_or(0);
                out.push(MetadataEvent::SilenceEnd {
                    at,
                    duration_us,
                    channel_number,
                });
            }
        }
    }
}

fn parse_scene(md: &DictionaryRef<'_>, out: &mut Vec<MetadataEvent>) {
    // `scdet` writes `lavfi.scd.score` on every frame but only writes
    // `lavfi.scd.time` on an actual scene change — key on the latter.
    if let Some(at) = md.get(SCD_TIME).and_then(parse_secs) {
        let score = md.get(SCD_SCORE).and_then(parse_f64).unwrap_or(0.0);
        out.push(MetadataEvent::SceneChange { at, score });
    }
}

fn parse_crop(md: &DictionaryRef<'_>, frame_ts: Option<Timestamp>, out: &mut Vec<MetadataEvent>) {
    let Some(at) = frame_ts else { return };
    if let (Some(x), Some(y), Some(w), Some(h)) = (
        md.get(CROP_X).and_then(parse_i32),
        md.get(CROP_Y).and_then(parse_i32),
        md.get(CROP_W).and_then(parse_i32),
        md.get(CROP_H).and_then(parse_i32),
    ) {
        out.push(MetadataEvent::CropDetect { at, x, y, w, h });
    }
}

fn parse_r128(
    md: &DictionaryRef<'_>,
    frame_ts: Option<Timestamp>,
    out: &mut Vec<MetadataEvent>,
    state: &mut ParseState,
) {
    let Some(at) = frame_ts else { return };
    let momentary = md.get(R128_M).and_then(parse_f64);
    let short_term = md.get(R128_S).and_then(parse_f64);
    let integrated = md.get(R128_I).and_then(parse_f64);
    let lra = md.get(R128_LRA).and_then(parse_f64);
    let true_peak = max_true_peak(md);

    if momentary.is_none()
        && short_term.is_none()
        && integrated.is_none()
        && lra.is_none()
        && true_peak.is_none()
    {
        return;
    }
    if integrated.is_some() {
        state.last_integrated = integrated;
    }
    if lra.is_some() {
        state.last_lra = lra;
    }
    if true_peak.is_some() {
        state.last_true_peak = true_peak;
    }
    out.push(MetadataEvent::R128Frame {
        at,
        momentary,
        short_term,
        integrated,
        lra,
        true_peak,
    });
}

/// The frame's aggregate true peak, or `None` if `ebur128` did not emit
/// true-peak keys (i.e. `peak=true` was not requested).
fn max_true_peak(md: &DictionaryRef<'_>) -> Option<f64> {
    md.get(R128_TRUE_PEAK).and_then(parse_f64)
}

/// Parses a `.N` channel suffix (1-based) into `Some(N)`; `""` yields `None`.
fn parse_channel_suffix(suffix: &str) -> Option<usize> {
    suffix.strip_prefix('.').and_then(|n| n.parse::<usize>().ok())
}

fn remove_pending(pending: &mut Vec<(Option<usize>, i64)>, channel: Option<usize>) -> Option<i64> {
    let pos = pending.iter().position(|(c, _)| *c == channel)?;
    Some(pending.remove(pos).1)
}

fn parse_secs(s: &str) -> Option<Timestamp> {
    s.trim().parse::<f64>().ok().and_then(Timestamp::from_secs)
}

fn parse_f64(s: &str) -> Option<f64> {
    s.trim().parse::<f64>().ok()
}

fn parse_i32(s: &str) -> Option<i32> {
    s.trim().parse::<i32>().ok()
}

/// Converts a finite seconds value to rounded microseconds.
pub(crate) fn secs_to_us(s: f64) -> Option<i64> {
    if s.is_finite() {
        Some((s * 1e6).round() as i64)
    } else {
        None
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use ffmpeg_next::{frame, Dictionary};

    fn md_frame(pairs: &[(&str, &str)]) -> frame::Video {
        let mut dict = Dictionary::new();
        for &(k, v) in pairs {
            dict.set(k, v);
        }
        let mut f = frame::Video::empty();
        f.set_metadata(dict);
        f
    }

    fn parse(pairs: &[(&str, &str)], media: AVMediaType, state: &mut ParseState) -> Vec<MetadataEvent> {
        let f = md_frame(pairs);
        let mut out = Vec::new();
        parse_frame_metadata(&f.metadata(), None, media, &mut out, state);
        out
    }

    #[test]
    fn from_secs_rounds_and_rejects_non_finite() {
        assert_eq!(Timestamp::from_secs(1.5).unwrap().time_us, 1_500_000);
        // 0.0000005 s = 0.5 us -> rounds to 1 us (truncation would give 0).
        assert_eq!(Timestamp::from_secs(0.000_000_5).unwrap().time_us, 1);
        assert!(Timestamp::from_secs(f64::NAN).is_none());
        assert!(Timestamp::from_secs(f64::INFINITY).is_none());
    }

    #[test]
    fn black_start_end_pairs_with_duration() {
        let mut state = ParseState::default();
        let ev = parse(&[("lavfi.black_start", "1.5")], AVMEDIA_TYPE_VIDEO, &mut state);
        assert_eq!(ev, vec![MetadataEvent::BlackStart { at: Timestamp::from_secs(1.5).unwrap() }]);
        let ev = parse(&[("lavfi.black_end", "3.0")], AVMEDIA_TYPE_VIDEO, &mut state);
        assert_eq!(
            ev,
            vec![MetadataEvent::BlackEnd { at: Timestamp::from_secs(3.0).unwrap(), duration_us: 1_500_000 }]
        );
    }

    #[test]
    fn scene_change_only_when_time_present() {
        let mut state = ParseState::default();
        // score alone -> no event
        assert!(parse(&[("lavfi.scd.score", "12.0")], AVMEDIA_TYPE_VIDEO, &mut state).is_empty());
        let ev = parse(
            &[("lavfi.scd.score", "12.0"), ("lavfi.scd.time", "2.0")],
            AVMEDIA_TYPE_VIDEO,
            &mut state,
        );
        assert_eq!(
            ev,
            vec![MetadataEvent::SceneChange { at: Timestamp::from_secs(2.0).unwrap(), score: 12.0 }]
        );
    }

    #[test]
    fn mono_silence_carries_channel_number() {
        let mut state = ParseState::default();
        let ev = parse(&[("lavfi.silence_start.2", "0.5")], AVMEDIA_TYPE_AUDIO, &mut state);
        assert_eq!(
            ev,
            vec![MetadataEvent::SilenceStart { at: Timestamp::from_secs(0.5).unwrap(), channel_number: Some(2) }]
        );
        let ev = parse(
            &[("lavfi.silence_end.2", "1.5"), ("lavfi.silence_duration.2", "1.0")],
            AVMEDIA_TYPE_AUDIO,
            &mut state,
        );
        assert_eq!(
            ev,
            vec![MetadataEvent::SilenceEnd {
                at: Timestamp::from_secs(1.5).unwrap(),
                duration_us: 1_000_000,
                channel_number: Some(2),
            }]
        );
    }

    #[test]
    fn combined_silence_has_no_channel() {
        let mut state = ParseState::default();
        let ev = parse(&[("lavfi.silence_start", "0.5")], AVMEDIA_TYPE_AUDIO, &mut state);
        assert_eq!(
            ev,
            vec![MetadataEvent::SilenceStart { at: Timestamp::from_secs(0.5).unwrap(), channel_number: None }]
        );
    }

    #[test]
    fn r128_true_peak_absent_is_none() {
        let mut state = ParseState::default();
        let ts = Some(Timestamp::from_secs(1.0).unwrap());
        let f = md_frame(&[("lavfi.r128.M", "-20.0"), ("lavfi.r128.I", "-23.0"), ("lavfi.r128.LRA", "5.0")]);
        let mut out = Vec::new();
        parse_frame_metadata(&f.metadata(), ts, AVMEDIA_TYPE_AUDIO, &mut out, &mut state);
        match &out[0] {
            MetadataEvent::R128Frame { true_peak, integrated, .. } => {
                assert_eq!(*true_peak, None);
                assert_eq!(*integrated, Some(-23.0));
            }
            other => panic!("expected R128Frame, got {other:?}"),
        }
    }

    #[test]
    fn r128_true_peak_read_from_aggregate_key() {
        // ebur128's SET_META_PEAK writes the cross-channel max to
        // `lavfi.r128.true_peak` alongside the per-channel `_peaks_chN` keys;
        // we read the aggregate directly.
        let mut state = ParseState::default();
        let ts = Some(Timestamp::from_secs(1.0).unwrap());
        let f = md_frame(&[
            ("lavfi.r128.I", "-23.0"),
            ("lavfi.r128.true_peaks_ch0", "-2.0"),
            ("lavfi.r128.true_peaks_ch1", "-1.5"),
            ("lavfi.r128.true_peak", "-1.5"),
        ]);
        let mut out = Vec::new();
        parse_frame_metadata(&f.metadata(), ts, AVMEDIA_TYPE_AUDIO, &mut out, &mut state);
        match &out[0] {
            MetadataEvent::R128Frame { true_peak, .. } => assert_eq!(*true_peak, Some(-1.5)),
            other => panic!("expected R128Frame, got {other:?}"),
        }
        // flush should now carry the remembered summary values.
        let flushed = state.flush(AVMEDIA_TYPE_AUDIO);
        assert!(flushed.iter().any(|e| matches!(
            e,
            MetadataEvent::R128Summary { integrated: Some(_), true_peak: Some(_), .. }
        )));
    }
}
