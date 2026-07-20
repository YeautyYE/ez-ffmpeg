//! The typed option table: the ONLY place that says which option spellings
//! the subset accepts, in which positional scope, with which arity and value
//! grammar. The parser consults this table token by token; the manifest
//! renders the user-facing support table from it. An option missing here is
//! rejected — first through the known-rejection list (precise reasons), then
//! generically with a nearest-spelling hint.

use super::error::CliError;

/// Positional scopes an option may legally occupy.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum ScopeRule {
    /// Legal anywhere; applies to the run, not a file (`-y`, `-loglevel`).
    Global,
    /// Only between the input and the output path. (No Round-1 option is
    /// input-only; a variant for that scope returns with the Round-2 rows
    /// that need it.)
    OutputOnly,
    /// Legal in both file scopes with position-dependent meaning (`-ss`).
    InputOrOutput,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum Arity {
    Flag,
    Value,
}

/// Value grammar per option. Validation happens at classification time so a
/// bad value is anchored to its exact token.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum ValueRule {
    /// Decimal seconds (`10`, `2.5`). `HH:MM:SS` and negative forms are
    /// explicitly out of the Round-1 grammar.
    Seconds,
    /// Codec name or `copy` (`[A-Za-z0-9_-]+`).
    Codec,
    /// `NNN`, `NNNk`, `NNNM` bitrate spellings.
    Bitrate,
    /// Integer 0..=51.
    Crf,
    /// x264 preset name whitelist.
    Preset,
    /// Positive integer.
    PositiveInt,
    /// Pixel format name (`[a-z0-9_]+`); existence is checked at build time.
    PixFmt,
    /// Exactly `1` (Round 1 supports single-frame output only).
    FramesOne,
    /// A single simple `scale=…` chain: no chains, labels, parentheses or
    /// quoting.
    ScaleFilter,
    /// Basic index stream map: `0`, `0:v`, `0:a`, `0:v:0`, `0:a:1`, `0:1`.
    MapBasic,
    /// Container/demuxer name (`[a-z0-9_]+`).
    FormatName,
    /// Exactly `+faststart`.
    MovflagsFaststart,
    /// Decimal seconds > 0.
    HlsTime,
    /// Exactly `vod`.
    HlsPlaylistVod,
    /// Exactly `0`.
    HlsListSizeZero,
    /// Non-empty segment path; `-`-leading values are rejected as swallowed
    /// options.
    Path,
    /// `-loglevel` value: known level name or integer.
    LogLevel,
}

pub(crate) struct OptSpec {
    /// Canonical spelling as typed on the command line.
    pub(crate) name: &'static str,
    pub(crate) scope: ScopeRule,
    pub(crate) arity: Arity,
    pub(crate) value: Option<ValueRule>,
    /// Recognized but deliberately without an in-process effect.
    pub(crate) noop: bool,
}

const fn flag(name: &'static str, scope: ScopeRule) -> OptSpec {
    OptSpec {
        name,
        scope,
        arity: Arity::Flag,
        value: None,
        noop: false,
    }
}

const fn value(name: &'static str, scope: ScopeRule, rule: ValueRule) -> OptSpec {
    OptSpec {
        name,
        scope,
        arity: Arity::Value,
        value: Some(rule),
        noop: false,
    }
}

const fn noop_flag(name: &'static str) -> OptSpec {
    OptSpec {
        name,
        scope: ScopeRule::Global,
        arity: Arity::Flag,
        value: None,
        noop: true,
    }
}

const fn noop_value(name: &'static str, rule: ValueRule) -> OptSpec {
    OptSpec {
        name,
        scope: ScopeRule::Global,
        arity: Arity::Value,
        value: Some(rule),
        noop: true,
    }
}

/// The Round-1 accept surface. `-i` is handled structurally by the parser and
/// is not a table row.
pub(crate) const OPTION_TABLE: &[OptSpec] = &[
    // Globals.
    flag("-y", ScopeRule::Global),
    noop_flag("-hide_banner"),
    noop_flag("-nostdin"),
    noop_flag("-stats"),
    noop_flag("-nostats"),
    noop_value("-loglevel", ValueRule::LogLevel),
    noop_value("-v", ValueRule::LogLevel),
    // Trims and container selection (position-scoped).
    value("-ss", ScopeRule::InputOrOutput, ValueRule::Seconds),
    value("-t", ScopeRule::InputOrOutput, ValueRule::Seconds),
    value("-to", ScopeRule::InputOrOutput, ValueRule::Seconds),
    value("-f", ScopeRule::InputOrOutput, ValueRule::FormatName),
    // Output stream selection / codecs.
    flag("-vn", ScopeRule::OutputOnly),
    flag("-an", ScopeRule::OutputOnly),
    value("-c:v", ScopeRule::OutputOnly, ValueRule::Codec),
    value("-c:a", ScopeRule::OutputOnly, ValueRule::Codec),
    value("-b:v", ScopeRule::OutputOnly, ValueRule::Bitrate),
    value("-b:a", ScopeRule::OutputOnly, ValueRule::Bitrate),
    value("-crf", ScopeRule::OutputOnly, ValueRule::Crf),
    value("-preset", ScopeRule::OutputOnly, ValueRule::Preset),
    value("-pix_fmt", ScopeRule::OutputOnly, ValueRule::PixFmt),
    value("-ar", ScopeRule::OutputOnly, ValueRule::PositiveInt),
    value("-ac", ScopeRule::OutputOnly, ValueRule::PositiveInt),
    value("-frames:v", ScopeRule::OutputOnly, ValueRule::FramesOne),
    value("-vf", ScopeRule::OutputOnly, ValueRule::ScaleFilter),
    value("-map", ScopeRule::OutputOnly, ValueRule::MapBasic),
    value("-movflags", ScopeRule::OutputOnly, ValueRule::MovflagsFaststart),
    // Single-rendition VOD HLS.
    value("-hls_time", ScopeRule::OutputOnly, ValueRule::HlsTime),
    value("-hls_playlist_type", ScopeRule::OutputOnly, ValueRule::HlsPlaylistVod),
    value("-hls_list_size", ScopeRule::OutputOnly, ValueRule::HlsListSizeZero),
    value("-hls_segment_filename", ScopeRule::OutputOnly, ValueRule::Path),
];

pub(crate) fn lookup(name: &str) -> Option<&'static OptSpec> {
    OPTION_TABLE.iter().find(|spec| spec.name == name)
}

/// Documented rejections: options we recognize precisely enough to explain
/// WHY they are outside the subset. `(spelling, reason, hint)`.
pub(crate) const KNOWN_REJECTIONS: &[(&str, &str, Option<&str>)] = &[
    ("-n", "never-overwrite semantics have no builder equivalent (the crate always creates/truncates outputs); check for the file's existence before running instead", None),
    ("-c", "unsplit -c is ambiguous across media types", Some("did you mean `-c:v` and/or `-c:a`?")),
    ("-codec", "unsplit -codec is ambiguous across media types", Some("did you mean `-c:v` and/or `-c:a`?")),
    ("-vcodec", "alias spellings are outside the subset", Some("did you mean `-c:v`?")),
    ("-acodec", "alias spellings are outside the subset", Some("did you mean `-c:a`?")),
    ("-scodec", "subtitle streams are not in the Round-1 subset", None),
    ("-codec:v", "alias spellings are outside the subset", Some("did you mean `-c:v`?")),
    ("-codec:a", "alias spellings are outside the subset", Some("did you mean `-c:a`?")),
    ("-b", "unsplit -b is ambiguous (the ffmpeg CLI itself says: Please use -b:a or -b:v)", Some("did you mean `-b:v` or `-b:a`?")),
    ("-q", "fixed-quality scale is not in the Round-1 subset", None),
    ("-qscale", "fixed-quality scale is not in the Round-1 subset; the CLI itself calls unsplit -qscale ambiguous", None),
    ("-q:v", "fixed-quality scale is not in the Round-1 subset", None),
    ("-q:a", "fixed-quality scale is not in the Round-1 subset", None),
    ("-fps_mode", "frame sync modes are permanently excluded: the crate models vsync per output, not per stream, so no -fps_mode form can be mapped faithfully", None),
    ("-vsync", "frame sync modes are permanently excluded: the crate models vsync per output, not per stream, so no -vsync form can be mapped faithfully", None),
    ("-filter_complex", "complex filtergraphs are planned for Round 2 (fully labeled graphs only)", None),
    ("-lavfi", "complex filtergraphs are planned for Round 2 (fully labeled graphs only)", None),
    ("-filter:v", "alias spellings are outside the subset", Some("did you mean `-vf`?")),
    ("-af", "audio filters are planned for Round 2 (needs the per-output audio filter API)", None),
    ("-filter:a", "audio filters are planned for Round 2 (needs the per-output audio filter API)", None),
    ("-pass", "two-pass encoding is a documented gap: the stats-file handshake between runs has no ez-ffmpeg equivalent", None),
    ("-passlogfile", "two-pass encoding is a documented gap: the stats-file handshake between runs has no ez-ffmpeg equivalent", None),
    ("-map_metadata", "metadata mapping is planned for Round 2", None),
    ("-metadata", "explicit metadata is planned for Round 2 (implicit metadata copying already matches the CLI default)", None),
    ("-shortest", "-shortest is planned for Round 2 (the builder equivalent exists: Output::set_shortest)", None),
    ("-re", "readrate streaming is planned for Round 2 (the builder equivalent exists: Input::set_readrate)", None),
    ("-readrate", "readrate streaming is planned for Round 2 (the builder equivalent exists: Input::set_readrate)", None),
    ("-stream_loop", "input looping is not in the Round-1 subset (the builder equivalent exists: Input::set_stream_loop)", None),
    ("-r", "output frame rate is not in the Round-1 subset (the builder equivalent exists: Output::set_framerate)", None),
    ("-s", "frame size is not in the Round-1 subset", Some("did you mean `-vf scale=W:H`?")),
    ("-sn", "subtitle streams are not in the Round-1 subset", None),
    ("-dn", "data streams are not in the Round-1 subset", None),
    ("-progress", "progress reporting is a documented gap: the CLI's encoder statistics pipeline has no in-process equivalent", None),
    ("-stats_period", "progress reporting is a documented gap", None),
    ("-t:v", "per-stream indexed/typed variants are permanently excluded: the crate models these options per media type, not per stream", None),
    ("-profile", "encoder profiles are not in the Round-1 subset; the CLI itself calls unsplit -profile ambiguous", None),
    ("-profile:v", "encoder profiles are not in the Round-1 subset", None),
    ("-level", "encoder levels are not in the Round-1 subset", None),
    ("-g", "GOP-size tuning is not in the Round-1 subset", None),
    ("-force_key_frames", "forced keyframes are not in the Round-1 subset (the builder equivalent exists: Output::set_force_key_frames)", None),
    ("-threads", "thread-count tuning is not in the Round-1 subset (the crate already defaults encoders to auto threading like the CLI)", None),
    ("-hwaccel", "hardware acceleration is planned for Round 2 (the builder equivalent exists: Input::set_hwaccel)", None),
    ("-tag:v", "codec tags are a documented gap: the crate does not expose per-stream tags; re-tag with an external remux", None),
    ("-tag:a", "codec tags are a documented gap: the crate does not expose per-stream tags; re-tag with an external remux", None),
    ("-attach", "attachments are not in the Round-1 subset (the builder equivalent exists: Output::add_attachment)", None),
    ("-hls_flags", "only the single-rendition VOD HLS option set is in Round 1 (hls_time, hls_playlist_type vod, hls_list_size 0, hls_segment_filename)", None),
    ("-hls_segment_type", "only the single-rendition VOD HLS option set is in Round 1", None),
    ("-hls_key_info_file", "encrypted HLS is permanently excluded from the subset", None),
    ("-master_pl_name", "multi-rendition HLS is excluded; use the HlsLadder recipe instead", None),
    ("-var_stream_map", "multi-rendition HLS is excluded; use the HlsLadder recipe instead", None),
    ("-segment_time", "the segment muxer is planned for Round 2 (use Output::set_format(\"segment\") with set_format_opt meanwhile)", None),
    ("-ss:v", "per-stream indexed/typed variants are permanently excluded", None),
    ("-frames:a", "only -frames:v 1 (single video frame) is in the Round-1 subset", None),
    ("-frames", "only -frames:v 1 (single video frame) is in the Round-1 subset", None),
    ("-vframes", "legacy alias; only -frames:v 1 is in the Round-1 subset", Some("did you mean `-frames:v 1`?")),
    ("-update", "image2 update mode is applied automatically for -frames:v 1 outputs; the explicit option is not in the subset", None),
];

/// x264 preset names accepted by [`ValueRule::Preset`].
pub(crate) const PRESET_WHITELIST: &[&str] = &[
    "ultrafast",
    "superfast",
    "veryfast",
    "faster",
    "fast",
    "medium",
    "slow",
    "slower",
    "veryslow",
    "placebo",
];

const LOG_LEVELS: &[&str] = &[
    "quiet", "panic", "fatal", "error", "warning", "info", "verbose", "debug", "trace",
];

/// Validates `value` against `rule`. `option` and `index` anchor the error.
pub(crate) fn validate_value(
    rule: ValueRule,
    option: &str,
    value: &str,
    index: usize,
) -> Result<(), CliError> {
    let fail = |reason: String| CliError::UnsupportedValue {
        option: option.to_string(),
        value: value.to_string(),
        index,
        reason,
    };

    match rule {
        ValueRule::Seconds => {
            parse_seconds_us(value).map(|_| ()).map_err(fail)
        }
        ValueRule::Codec => {
            if !value.is_empty()
                && value
                    .chars()
                    .all(|c| c.is_ascii_alphanumeric() || c == '_' || c == '-')
            {
                Ok(())
            } else {
                Err(fail("codec names use [A-Za-z0-9_-] only".to_string()))
            }
        }
        ValueRule::Bitrate => {
            let digits = value.strip_suffix(['k', 'K', 'm', 'M']).unwrap_or(value);
            if !digits.is_empty() && digits.chars().all(|c| c.is_ascii_digit()) {
                Ok(())
            } else {
                Err(fail(
                    "bitrates use the NNN / NNNk / NNNM spellings in this subset".to_string(),
                ))
            }
        }
        ValueRule::Crf => match value.parse::<i64>() {
            Ok(v) if (0..=51).contains(&v) => Ok(()),
            _ => Err(fail("crf must be an integer in 0..=51".to_string())),
        },
        ValueRule::Preset => {
            if PRESET_WHITELIST.contains(&value) {
                Ok(())
            } else {
                Err(fail(format!(
                    "preset must be one of the x264 presets ({})",
                    PRESET_WHITELIST.join(", ")
                )))
            }
        }
        ValueRule::PositiveInt => match value.parse::<i32>() {
            Ok(v) if v > 0 => Ok(()),
            _ => Err(fail("expected a positive integer".to_string())),
        },
        ValueRule::PixFmt => {
            if !value.is_empty()
                && value
                    .chars()
                    .all(|c| c.is_ascii_lowercase() || c.is_ascii_digit() || c == '_')
            {
                Ok(())
            } else {
                Err(fail(
                    "pixel format names use [a-z0-9_] only; validity is checked against the \
                     linked FFmpeg at build time"
                        .to_string(),
                ))
            }
        }
        ValueRule::FramesOne => {
            if value == "1" {
                Ok(())
            } else {
                Err(fail(
                    "Round 1 supports single-frame output only (-frames:v 1)".to_string(),
                ))
            }
        }
        ValueRule::ScaleFilter => validate_scale_filter(value).map_err(fail),
        ValueRule::MapBasic => validate_map_basic(value).map_err(fail),
        ValueRule::FormatName => {
            if !value.is_empty()
                && value
                    .chars()
                    .all(|c| c.is_ascii_lowercase() || c.is_ascii_digit() || c == '_')
            {
                Ok(())
            } else {
                Err(fail("container/demuxer names use [a-z0-9_] only".to_string()))
            }
        }
        ValueRule::MovflagsFaststart => {
            if value == "+faststart" {
                Ok(())
            } else {
                Err(fail(
                    "only the exact `-movflags +faststart` is in the Round-1 subset".to_string(),
                ))
            }
        }
        ValueRule::HlsTime => match parse_seconds_us(value) {
            Ok(us) if us > 0 => Ok(()),
            _ => Err(fail("hls_time must be a positive decimal number of seconds".to_string())),
        },
        ValueRule::HlsPlaylistVod => {
            if value == "vod" {
                Ok(())
            } else {
                Err(fail(
                    "only `-hls_playlist_type vod` (single-rendition VOD) is in the Round-1 \
                     subset"
                        .to_string(),
                ))
            }
        }
        ValueRule::HlsListSizeZero => {
            if value == "0" {
                Ok(())
            } else {
                Err(fail(
                    "only `-hls_list_size 0` (keep every segment, VOD) is in the Round-1 subset"
                        .to_string(),
                ))
            }
        }
        ValueRule::Path => {
            if value.is_empty() {
                Err(fail("expected a non-empty path".to_string()))
            } else {
                Ok(())
            }
        }
        ValueRule::LogLevel => {
            let level = value.rsplit('+').next().unwrap_or(value);
            if LOG_LEVELS.contains(&level) || level.parse::<i32>().is_ok() {
                Ok(())
            } else {
                Err(fail(format!(
                    "unknown log level; expected one of {} or an integer",
                    LOG_LEVELS.join(", ")
                )))
            }
        }
    }
}

/// Decimal seconds -> microseconds. Rejects negatives, `HH:MM:SS`, unit
/// suffixes, and out-of-range values — the Round-1 time grammar is decimal
/// seconds only.
pub(crate) fn parse_seconds_us(value: &str) -> Result<i64, String> {
    if value.contains(':') {
        return Err(
            "the HH:MM:SS form is not in the Round-1 subset; use decimal seconds".to_string(),
        );
    }
    if value.starts_with('-') || value.starts_with('+') {
        return Err("signed times are not in the Round-1 subset".to_string());
    }
    let mut parts = value.split('.');
    let (int_part, frac_part) = (parts.next().unwrap_or(""), parts.next());
    let well_formed = parts.next().is_none()
        && !int_part.is_empty()
        && int_part.chars().all(|c| c.is_ascii_digit())
        && frac_part.is_none_or(|f| !f.is_empty() && f.chars().all(|c| c.is_ascii_digit()));
    if !well_formed {
        return Err("expected decimal seconds (e.g. `10` or `2.5`)".to_string());
    }
    let secs: f64 = value
        .parse()
        .map_err(|_| "expected decimal seconds (e.g. `10` or `2.5`)".to_string())?;
    let us = (secs * 1_000_000.0).round();
    if !us.is_finite() || us < 0.0 || us >= i64::MAX as f64 {
        return Err("time value out of range".to_string());
    }
    Ok(us as i64)
}

fn validate_scale_filter(value: &str) -> Result<(), String> {
    let Some(args) = value.strip_prefix("scale=") else {
        return Err(
            "Round 1 accepts a single simple scale filter only (e.g. `scale=1280:-2`); other \
             filters and chains are planned for Round 2"
                .to_string(),
        );
    };
    if args.is_empty() {
        return Err("scale needs arguments, e.g. `scale=1280:-2`".to_string());
    }
    let simple = args.chars().all(|c| {
        c.is_ascii_alphanumeric() || matches!(c, ':' | '=' | '_' | '.' | '-' | '+' | '*' | '/')
    });
    if !simple {
        return Err(
            "only simple scale arguments are in the subset: no chains (`,`), labels (`[`), \
             semicolons, parentheses or quoting — write expressions like iw/2 without \
             parentheses"
                .to_string(),
        );
    }
    Ok(())
}

fn validate_map_basic(value: &str) -> Result<(), String> {
    const REASON: &str = "Round 1 accepts basic index maps only: `0`, `0:v`, `0:a`, `0:v:0`, \
                          `0:a:1`, `0:1`. Optional (`?`), negative (`-`), label (`[…]`), \
                          program/metadata/disposition selectors and subtitle/data maps are \
                          excluded";
    if value.starts_with('-') {
        return Err(format!("negative mappings are excluded. {REASON}"));
    }
    if value.ends_with('?') {
        return Err(format!(
            "the trailing `?` (optional mapping) is excluded — Output::add_stream_map fails \
             loudly if the stream is missing, which is the explicit replacement. {REASON}"
        ));
    }
    let mut parts = value.split(':');
    let Some(file) = parts.next() else {
        return Err(REASON.to_string());
    };
    if file != "0" {
        if file.chars().all(|c| c.is_ascii_digit()) && !file.is_empty() {
            return Err(format!(
                "map references input #{file}, but the subset is single-input (input #0). \
                 {REASON}"
            ));
        }
        return Err(REASON.to_string());
    }
    match (parts.next(), parts.next(), parts.next()) {
        (None, _, _) => Ok(()),
        (Some(sel), rest, None) => {
            let stream_ok =
                |s: &str| !s.is_empty() && s.chars().all(|c| c.is_ascii_digit());
            match sel {
                "v" | "a" => match rest {
                    None => Ok(()),
                    Some(idx) if stream_ok(idx) => Ok(()),
                    Some(_) => Err(REASON.to_string()),
                },
                s if stream_ok(s) && rest.is_none() => Ok(()),
                "s" | "d" | "t" => Err(format!(
                    "subtitle/data/attachment maps are not in the Round-1 subset. {REASON}"
                )),
                _ => Err(REASON.to_string()),
            }
        }
        _ => Err(REASON.to_string()),
    }
}

/// Nearest supported spelling for "did you mean" hints. Plain Levenshtein
/// over the option table; only close matches (distance ≤ 2) are suggested.
pub(crate) fn nearest_option(name: &str) -> Option<&'static str> {
    let mut best: Option<(usize, &'static str)> = None;
    for spec in OPTION_TABLE {
        let d = levenshtein(name, spec.name);
        if best.is_none_or(|(bd, _)| d < bd) {
            best = Some((d, spec.name));
        }
    }
    match best {
        Some((d, name)) if d <= 2 => Some(name),
        _ => None,
    }
}

fn levenshtein(a: &str, b: &str) -> usize {
    let a: Vec<char> = a.chars().collect();
    let b: Vec<char> = b.chars().collect();
    let mut row: Vec<usize> = (0..=b.len()).collect();
    for (i, ca) in a.iter().enumerate() {
        let mut prev = row[0];
        row[0] = i + 1;
        for (j, cb) in b.iter().enumerate() {
            let cost = if ca == cb { 0 } else { 1 };
            let next = (prev + cost).min(row[j] + 1).min(row[j + 1] + 1);
            prev = row[j + 1];
            row[j + 1] = next;
        }
    }
    row[b.len()]
}

#[cfg(test)]
mod tests {
    use super::*;

    fn check(rule: ValueRule, value: &str) -> Result<(), CliError> {
        validate_value(rule, "-x", value, 0)
    }

    #[test]
    fn seconds_accepts_decimal_only() {
        assert_eq!(parse_seconds_us("10").unwrap(), 10_000_000);
        assert_eq!(parse_seconds_us("2.5").unwrap(), 2_500_000);
        assert_eq!(parse_seconds_us("0").unwrap(), 0);
        for bad in ["00:01:30", "-5", "+5", "10s", "1e3", "", ".", "1.", ".5", "1.2.3"] {
            assert!(parse_seconds_us(bad).is_err(), "expected Err for {bad:?}");
        }
    }

    #[test]
    fn crf_range_enforced() {
        assert!(check(ValueRule::Crf, "0").is_ok());
        assert!(check(ValueRule::Crf, "23").is_ok());
        assert!(check(ValueRule::Crf, "51").is_ok());
        for bad in ["52", "-1", "abc", "23.5"] {
            assert!(check(ValueRule::Crf, bad).is_err(), "expected Err for {bad:?}");
        }
    }

    #[test]
    fn preset_whitelist_enforced() {
        assert!(check(ValueRule::Preset, "fast").is_ok());
        assert!(check(ValueRule::Preset, "veryslow").is_ok());
        assert!(check(ValueRule::Preset, "warpspeed").is_err());
    }

    #[test]
    fn bitrate_spellings() {
        for good in ["192k", "2M", "500000", "2600K"] {
            assert!(check(ValueRule::Bitrate, good).is_ok(), "expected Ok for {good:?}");
        }
        for bad in ["192q", "k", "", "1.5M", "192 k"] {
            assert!(check(ValueRule::Bitrate, bad).is_err(), "expected Err for {bad:?}");
        }
    }

    #[test]
    fn frames_v_must_be_one() {
        assert!(check(ValueRule::FramesOne, "1").is_ok());
        assert!(check(ValueRule::FramesOne, "2").is_err());
        assert!(check(ValueRule::FramesOne, "0").is_err());
    }

    #[test]
    fn scale_filter_simple_forms_only() {
        for good in ["scale=1280:-2", "scale=iw/2:ih/2", "scale=w=640:h=360", "scale=320:240"] {
            assert!(check(ValueRule::ScaleFilter, good).is_ok(), "expected Ok for {good:?}");
        }
        for bad in [
            "scale=1280:-2,crop=64:64",
            "hue=s=0",
            "scale=min(1280\\,iw):-2",
            "[0:v]scale=1:1[v]",
            "scale=",
        ] {
            assert!(check(ValueRule::ScaleFilter, bad).is_err(), "expected Err for {bad:?}");
        }
    }

    #[test]
    fn map_basic_forms() {
        for good in ["0", "0:v", "0:a", "0:v:0", "0:a:1", "0:1"] {
            assert!(check(ValueRule::MapBasic, good).is_ok(), "expected Ok for {good:?}");
        }
        for bad in ["0:a:1?", "-0:v", "[vout]", "1:a", "0:s", "0:m:language:eng", "0:v:0:x", "p:1"] {
            assert!(check(ValueRule::MapBasic, bad).is_err(), "expected Err for {bad:?}");
        }
    }

    #[test]
    fn movflags_exact_faststart_only() {
        assert!(check(ValueRule::MovflagsFaststart, "+faststart").is_ok());
        assert!(check(ValueRule::MovflagsFaststart, "faststart").is_err());
        assert!(check(ValueRule::MovflagsFaststart, "+faststart+frag_keyframe").is_err());
    }

    #[test]
    fn hls_pins() {
        assert!(check(ValueRule::HlsPlaylistVod, "vod").is_ok());
        assert!(check(ValueRule::HlsPlaylistVod, "event").is_err());
        assert!(check(ValueRule::HlsListSizeZero, "0").is_ok());
        assert!(check(ValueRule::HlsListSizeZero, "5").is_err());
        assert!(check(ValueRule::HlsTime, "6").is_ok());
        assert!(check(ValueRule::HlsTime, "0").is_err());
    }

    #[test]
    fn nearest_option_suggests_close_spellings() {
        assert_eq!(nearest_option("-crff"), Some("-crf"));
        assert_eq!(nearest_option("-vff"), Some("-vf"));
        assert_eq!(nearest_option("-completely_unrelated"), None);
    }

    #[test]
    fn known_rejections_do_not_overlap_the_accept_table() {
        for (name, _, _) in KNOWN_REJECTIONS {
            assert!(
                lookup(name).is_none(),
                "{name} is both accepted and rejected"
            );
        }
    }
}
