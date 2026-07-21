//! Positional scoper + option classifier: argv tokens -> [`CliIr`].
//!
//! The walk mirrors the fftools grammar ("options are applied to the next
//! specified file") restricted to the supported canonical layout:
//!
//! ```text
//! [globals|input options] -i INPUT [globals|output options] OUTPUT [globals]
//! ```
//!
//! EVERY token must classify against the option table or the whole command
//! is rejected with a token-anchored diagnostic. An unknown option stops the
//! walk immediately: its arity is unknowable, so every later file boundary
//! would be a guess (fftools knows the arity from its own table; this subset
//! refuses to guess).

use super::error::{CliError, CliScope};
use super::ir::{CliIr, DurationBound, DurationKind, GlobalsIr, InputIr, Noop, OutputIr};
use super::table::{self, lookup, parse_seconds_us, Arity, ScopeRule, KNOWN_REJECTIONS};

/// Parses argv tokens (program name already stripped) into the typed IR.
pub(crate) fn parse(args: &[String]) -> Result<CliIr, CliError> {
    let mut p = Parser {
        globals: GlobalsIr::default(),
        input: None,
        output: None,
        pending_input: InputIr::default(),
        pending_output: OutputIr::default(),
        spans: Vec::new(),
    };

    let mut iter = args.iter().enumerate().peekable();
    while let Some((index, token)) = iter.next() {
        if token == "-i" {
            let Some((_, url)) = iter.next() else {
                return Err(CliError::UnsupportedValue {
                    option: "-i".to_string(),
                    value: String::new(),
                    index,
                    reason: "missing input path after -i".to_string(),
                });
            };
            p.take_input(url, index)?;
            continue;
        }

        if is_option_token(token) {
            let value_slot = match lookup(token) {
                Some(spec) if spec.arity == Arity::Value => {
                    let Some((value_index, value)) = iter.next() else {
                        return Err(CliError::UnsupportedValue {
                            option: token.clone(),
                            value: String::new(),
                            index,
                            reason: format!("missing value after {token}"),
                        });
                    };
                    Some((value.clone(), value_index))
                }
                _ => None,
            };
            p.apply_option(token, value_slot, index)?;
            continue;
        }

        p.take_output(token, index)?;
    }

    p.finish(args.len())
}

/// A token in option position: `-x`, but not a bare `-` (stdin/stdout
/// pseudo-path, handled — rejected — as a path) and not a negative number
/// (never valid where files are expected either; option lookup covers it).
fn is_option_token(token: &str) -> bool {
    token.len() > 1 && token.starts_with('-')
}

struct Parser {
    globals: GlobalsIr,
    input: Option<InputIr>,
    output: Option<OutputIr>,
    pending_input: InputIr,
    pending_output: OutputIr,
    /// First argv index per scope-qualified option key (diagnostic anchors).
    spans: Vec<(String, usize)>,
}

impl Parser {
    fn scope(&self) -> CliScope {
        if self.output.is_some() {
            CliScope::AfterOutput
        } else if self.input.is_none() {
            CliScope::Input
        } else {
            CliScope::Output
        }
    }

    fn take_input(&mut self, url: &str, index: usize) -> Result<(), CliError> {
        // Ordering first: an -i AFTER the output is foremost a layout
        // violation (the single-input message would also be true, but names
        // the lesser problem).
        if self.output.is_some() {
            return Err(CliError::UnsupportedLayout {
                token: "-i".to_string(),
                index,
                reason: "an input after the output file is outside the canonical \
                         inputs-then-outputs layout of the subset"
                    .to_string(),
            });
        }
        if self.input.is_some() {
            return Err(CliError::UnsupportedLayout {
                token: "-i".to_string(),
                index,
                reason: "the current supported subset is single-input; multi-input commands \
                         (overlay, concat, …) are planned for a future release"
                    .to_string(),
            });
        }
        if url == "-" {
            return Err(CliError::UnsupportedLayout {
                token: "-i -".to_string(),
                index,
                reason: "stdin input (`-i -`) is excluded: pipe I/O is process wiring, not part \
                         of the in-process subset"
                    .to_string(),
            });
        }
        let mut input = std::mem::take(&mut self.pending_input);
        input.url = url.to_string();
        self.input = Some(input);
        Ok(())
    }

    fn take_output(&mut self, url: &str, index: usize) -> Result<(), CliError> {
        if self.input.is_none() {
            return Err(CliError::UnsupportedLayout {
                token: url.to_string(),
                index,
                reason: "found an output path before any -i input; the subset accepts the \
                         canonical `[options] -i INPUT [options] OUTPUT` layout"
                    .to_string(),
            });
        }
        if self.output.is_some() {
            return Err(CliError::UnsupportedLayout {
                token: url.to_string(),
                index,
                reason: "the current supported subset is single-output; run one command per \
                         output"
                    .to_string(),
            });
        }
        if url == "-" {
            return Err(CliError::UnsupportedLayout {
                token: "-".to_string(),
                index,
                reason: "stdout output (`-`) is excluded: pipe I/O is process wiring, not part \
                         of the in-process subset"
                    .to_string(),
            });
        }
        let mut output = std::mem::take(&mut self.pending_output);
        output.url = url.to_string();
        self.output = Some(output);
        Ok(())
    }

    fn apply_option(
        &mut self,
        name: &str,
        value: Option<(String, usize)>,
        index: usize,
    ) -> Result<(), CliError> {
        let Some(spec) = lookup(name) else {
            return Err(self.reject_unknown(name, index));
        };

        // Scope admission first, so `-crf 23 -i in.mp4` is "output option in
        // input position", not a mystery later.
        let scope = self.scope();
        match spec.scope {
            ScopeRule::Global => {}
            _ if scope == CliScope::AfterOutput => {
                return Err(CliError::UnsupportedLayout {
                    token: name.to_string(),
                    index,
                    reason: "in ffmpeg grammar an option after the output file would apply to a \
                             FOLLOWING output; the subset is single-output, so only global flags \
                             may follow the output path"
                        .to_string(),
                });
            }
            ScopeRule::OutputOnly if scope != CliScope::Output => {
                return Err(CliError::UnsupportedLayout {
                    token: name.to_string(),
                    index,
                    reason: format!(
                        "{name} applies to the output file, but appears before -i where it \
                         would bind to the input"
                    ),
                });
            }
            _ => {}
        }

        // A bad value is anchored to the VALUE token, not the option that
        // introduced it.
        if let (Some(rule), Some((value, value_index))) = (spec.value, value.as_ref()) {
            table::validate_value(rule, name, value, *value_index)?;
        }

        if spec.selector == table::Selector::NoOp {
            self.globals.noops.push(Noop {
                flag: name.to_string(),
                value: value.map(|(text, _)| text),
            });
            return Ok(());
        }
        if name == "-y" {
            self.globals.overwrite = true;
            return Ok(());
        }

        // Diagnostic anchor: first occurrence of each consumed option — and
        // the manifest's repeat policy, enforced centrally from the table
        // (the per-sink setters keep their own guards only as invariants).
        let span_key = match scope {
            CliScope::Input => format!("in:{name}"),
            _ => format!("out:{name}"),
        };
        match self.spans.iter().find(|(key, _)| key == &span_key) {
            Some((_, first_index)) if spec.repeat == table::Repeat::Once => {
                return Err(CliError::UnsupportedLayout {
                    token: name.to_string(),
                    index,
                    reason: format!(
                        "{name} appears more than once in the same scope (first at token \
                         #{first_index}); the CLI silently keeps the last value, which the \
                         subset refuses to reproduce — the option lands in {}",
                        spec.sink
                    ),
                });
            }
            Some(_) => {}
            None => self.spans.push((span_key, index)),
        }

        let value = value.map(|(text, _)| text).unwrap_or_default();
        match self.scope() {
            CliScope::Input => self.apply_input_option(name, value, index),
            _ => self.apply_output_option(name, value, index),
        }
    }

    fn reject_unknown(&self, name: &str, index: usize) -> CliError {
        // Exact documented rejection first; try the indexed per-stream
        // pattern next; otherwise generic with a nearest-spelling hint.
        for (spelling, reason, hint) in KNOWN_REJECTIONS {
            if name == *spelling {
                return CliError::UnsupportedOption {
                    option: name.to_string(),
                    index,
                    scope: self.scope(),
                    reason: (*reason).to_string(),
                    hint: hint.map(str::to_string),
                };
            }
        }
        if is_indexed_per_stream(name) {
            return CliError::UnsupportedOption {
                option: name.to_string(),
                index,
                scope: self.scope(),
                reason: "per-stream indexed variants (like -b:v:1) are permanently excluded: \
                         the crate models codec/bitrate/filter options per media type, not per \
                         stream index"
                    .to_string(),
                hint: None,
            };
        }
        CliError::UnsupportedOption {
            option: name.to_string(),
            index,
            scope: self.scope(),
            reason: "this option is not in the CLI-compat subset".to_string(),
            hint: table::nearest_option(name).map(|n| format!("did you mean `{n}`?")),
        }
    }

    fn apply_input_option(
        &mut self,
        name: &str,
        value: String,
        index: usize,
    ) -> Result<(), CliError> {
        let input = &mut self.pending_input;
        match name {
            "-ss" => {
                set_once_i64(&mut input.start_time_us, name, &value, index)?;
            }
            "-t" => set_duration(&mut input.duration, DurationKind::Duration, &value, index)?,
            "-to" => set_duration(&mut input.duration, DurationKind::StopAt, &value, index)?,
            "-f" => set_once_str(&mut input.format, name, value, index)?,
            other => unreachable!("input option {other} passed the table but has no sink"),
        }
        Ok(())
    }

    fn apply_output_option(
        &mut self,
        name: &str,
        value: String,
        index: usize,
    ) -> Result<(), CliError> {
        let output = &mut self.pending_output;
        match name {
            "-ss" => {
                set_once_i64(&mut output.start_time_us, name, &value, index)?;
            }
            "-t" => set_duration(&mut output.duration, DurationKind::Duration, &value, index)?,
            "-to" => set_duration(&mut output.duration, DurationKind::StopAt, &value, index)?,
            "-f" => set_once_str(&mut output.format, name, value, index)?,
            "-vn" => output.video_disabled = true,
            "-an" => output.audio_disabled = true,
            "-c:v" => {
                dup_check(
                    output.video_copy || output.video_codec.is_some(),
                    name,
                    index,
                )?;
                if value == "copy" {
                    output.video_copy = true;
                } else {
                    output.video_codec = Some(value);
                }
            }
            "-c:a" => {
                dup_check(
                    output.audio_copy || output.audio_codec.is_some(),
                    name,
                    index,
                )?;
                if value == "copy" {
                    output.audio_copy = true;
                } else {
                    output.audio_codec = Some(value);
                }
            }
            "-b:v" => set_once_str(&mut output.video_bitrate, name, value, index)?,
            "-b:a" => set_once_str(&mut output.audio_bitrate, name, value, index)?,
            "-crf" => set_once_str(&mut output.crf, name, value, index)?,
            "-preset" => set_once_str(&mut output.preset, name, value, index)?,
            "-pix_fmt" => set_once_str(&mut output.pix_fmt, name, value, index)?,
            "-ar" => {
                dup_check(output.audio_sample_rate.is_some(), name, index)?;
                // The table's PositiveInt rule already proved this parses.
                output.audio_sample_rate = Some(value.parse().expect("validated i32"));
            }
            "-ac" => {
                dup_check(output.audio_channels.is_some(), name, index)?;
                output.audio_channels = Some(value.parse().expect("validated i32"));
            }
            "-frames:v" => {
                dup_check(output.frames_v.is_some(), name, index)?;
                output.frames_v = Some(1);
            }
            "-vf" => set_once_str(&mut output.video_filter, name, value, index)?,
            "-map" => {
                output.maps.push(value);
                output.map_indexes.push(index);
            }
            "-movflags" => {
                dup_check(output.movflags_faststart, name, index)?;
                output.movflags_faststart = true;
            }
            "-hls_time" => set_once_str(&mut output.hls_time, name, value, index)?,
            "-hls_playlist_type" => {
                set_once_str(&mut output.hls_playlist_type, name, value, index)?
            }
            "-hls_list_size" => set_once_str(&mut output.hls_list_size, name, value, index)?,
            "-hls_segment_filename" => {
                set_once_str(&mut output.hls_segment_filename, name, value, index)?
            }
            other => unreachable!("output option {other} passed the table but has no sink"),
        }
        Ok(())
    }

    fn finish(self, token_count: usize) -> Result<CliIr, CliError> {
        let Some(input) = self.input else {
            return Err(CliError::UnsupportedLayout {
                token: String::new(),
                index: token_count,
                reason: "no -i input in the command".to_string(),
            });
        };
        let Some(output) = self.output else {
            return Err(CliError::UnsupportedLayout {
                token: String::new(),
                index: token_count,
                reason: "no output path in the command".to_string(),
            });
        };
        if !self.globals.overwrite {
            return Err(CliError::MissingOverwriteFlag);
        }

        let ir = CliIr {
            globals: self.globals,
            input,
            output,
            spans: self.spans,
        };
        check_combinations(&ir)?;
        Ok(ir)
    }
}

/// Cross-option rules that only make sense once the whole command is parsed.
/// Each is a distinct, documented conflict — never a silent drop.
/// Argv anchor for a conflict participant, resolved from the IR's span
/// table. Display names may carry a ` copy` suffix (`-c:v copy`) or the
/// collective `-hls_*`; both resolve to their recorded option keys.
fn display_span(ir: &CliIr, name: &str) -> Option<usize> {
    let base = name.strip_suffix(" copy").unwrap_or(name);
    // Collective names anchor at the EARLIEST participating occurrence, not
    // a fixed priority: `-hls_*` covers the four hls keys, and the synthetic
    // `-c` (from the unqualified-map × per-media-copy conflict) covers
    // whichever of -c:v / -c:a appeared first.
    let candidates: Vec<&str> = match base {
        "-hls_*" => vec![
            "out:-hls_time",
            "out:-hls_playlist_type",
            "out:-hls_list_size",
            "out:-hls_segment_filename",
        ],
        // The synthetic `-c copy`: only the occurrences whose VALUE is
        // actually `copy` participate — `-c:v libx264 -c:a copy` must anchor
        // the -c:a token, not the earlier non-copy -c:v.
        "-c" => {
            let mut keys = Vec::new();
            if ir.output.video_copy {
                keys.push("out:-c:v");
            }
            if ir.output.audio_copy {
                keys.push("out:-c:a");
            }
            keys
        }
        _ => {
            return ir
                .span(&format!("out:{base}"))
                .or_else(|| ir.span(&format!("in:{base}")));
        }
    };
    candidates.iter().filter_map(|key| ir.span(key)).min()
}

fn check_combinations(ir: &CliIr) -> Result<(), CliError> {
    let out = &ir.output;
    let conflict = |first: &str, second: &str, reason: &str| {
        Err(CliError::ConflictingOptions {
            first: first.to_string(),
            second: second.to_string(),
            first_index: display_span(ir, first),
            second_index: display_span(ir, second),
            reason: reason.to_string(),
        })
    };

    if out.video_filter.is_some() {
        if out.video_copy {
            return conflict(
                "-vf",
                "-c:v copy",
                "filtering and streamcopy cannot be used together (the ffmpeg CLI rejects this \
                 pair as well)",
            );
        }
        if out.video_disabled {
            return conflict(
                "-vf",
                "-vn",
                "the filter could never apply: -vn removes video",
            );
        }
        if !out.maps.is_empty() {
            return conflict(
                "-vf",
                "-map",
                "the current supported subset accepts basic maps in filterless commands only; \
                 combining explicit maps with filters needs labeled graphs, which are planned \
                 for a future release",
            );
        }
    }
    if out.video_disabled {
        for (present, name) in [
            (out.video_codec.is_some() || out.video_copy, "-c:v"),
            (out.video_bitrate.is_some(), "-b:v"),
            (out.crf.is_some(), "-crf"),
            (out.preset.is_some(), "-preset"),
            (out.pix_fmt.is_some(), "-pix_fmt"),
            (out.frames_v.is_some(), "-frames:v"),
        ] {
            if present {
                return conflict(
                    "-vn",
                    name,
                    "contradictory: -vn removes the video stream this option configures; the \
                     CLI silently ignores the option, which the subset refuses to reproduce",
                );
            }
        }
    }
    if out.audio_disabled {
        for (present, name) in [
            (out.audio_codec.is_some() || out.audio_copy, "-c:a"),
            (out.audio_bitrate.is_some(), "-b:a"),
            (out.audio_sample_rate.is_some(), "-ar"),
            (out.audio_channels.is_some(), "-ac"),
        ] {
            if present {
                return conflict(
                    "-an",
                    name,
                    "contradictory: -an removes the audio stream this option configures; the \
                     CLI silently ignores the option, which the subset refuses to reproduce",
                );
            }
        }
    }
    if out.video_disabled && out.audio_disabled {
        return conflict("-vn", "-an", "the output would have no streams at all");
    }

    // -crf/-preset are whitelisted for the libx264 path only: on any other
    // encoder they would fall through to AVOption lookup with
    // build-dependent results.
    let x264 = out.video_codec.as_deref() == Some("libx264");
    if out.crf.is_some() && !x264 {
        return conflict(
            "-crf",
            "-c:v",
            "the current supported subset whitelists -crf for `-c:v libx264` only; on other \
             encoders crf is an encoder-private AVOption with build-dependent meaning",
        );
    }
    if out.preset.is_some() && !x264 {
        return conflict(
            "-preset",
            "-c:v",
            "the current supported subset whitelists -preset for `-c:v libx264` only; on \
             other encoders preset is an encoder-private AVOption with build-dependent meaning",
        );
    }

    // Copy excludes the re-encode knobs for the same media type.
    if out.video_copy {
        for (present, name) in [
            (out.video_bitrate.is_some(), "-b:v"),
            (out.pix_fmt.is_some(), "-pix_fmt"),
            (out.frames_v.is_some(), "-frames:v"),
        ] {
            if present {
                return conflict(
                    "-c:v copy",
                    name,
                    "stream copy does not decode or encode, so this option could not take \
                     effect",
                );
            }
        }
    }
    if out.audio_copy {
        for (present, name) in [
            (out.audio_bitrate.is_some(), "-b:a"),
            (out.audio_sample_rate.is_some(), "-ar"),
            (out.audio_channels.is_some(), "-ac"),
        ] {
            if present {
                return conflict(
                    "-c:a copy",
                    name,
                    "stream copy does not decode or encode, so this option could not take \
                     effect",
                );
            }
        }
    }

    // Map interactions. A media-qualified map (`0:v…`/`0:a…`) can be paired
    // with that media type's copy flag statically; an unqualified index map
    // (`0`, `0:1`) cannot — the CLI would resolve the stream's type by
    // probing, which classification must not depend on.
    for (position, map) in out.maps.iter().enumerate() {
        // Anchor the OFFENDING occurrence, not the first map: the span table
        // only keeps the first `-map`, so per-occurrence indexes ride the IR.
        let map_conflict = |second: &str, reason: &str| {
            Err(CliError::ConflictingOptions {
                first: "-map".to_string(),
                second: second.to_string(),
                first_index: out.map_indexes.get(position).copied(),
                second_index: display_span(ir, second),
                reason: reason.to_string(),
            })
        };
        let media = map.split(':').nth(1).and_then(|s| s.chars().next());
        let qualified = matches!(media, Some('v') | Some('a'));
        if !qualified && (out.video_copy || out.audio_copy) {
            return map_conflict(
                "-c copy",
                "an index-only map cannot be classified against per-media copy without \
                 probing the input; use media-qualified maps (0:v… / 0:a…)",
            );
        }
        if media == Some('v') && out.video_disabled {
            return map_conflict(
                "-vn",
                "contradictory: the map selects a video stream that -vn removes",
            );
        }
        if media == Some('a') && out.audio_disabled {
            return map_conflict(
                "-an",
                "contradictory: the map selects an audio stream that -an removes",
            );
        }
    }

    // HLS keys only make sense together and only on an hls output.
    let hls_keys = out.hls_time.is_some()
        || out.hls_playlist_type.is_some()
        || out.hls_list_size.is_some()
        || out.hls_segment_filename.is_some();
    if hls_keys && out.format.as_deref() != Some("hls") {
        return conflict(
            "-hls_*",
            "-f",
            "hls muxer options require an explicit `-f hls` in the subset (extension-based \
             muxer guessing is not relied on for verified runs)",
        );
    }
    Ok(())
}

fn is_indexed_per_stream(name: &str) -> bool {
    // -opt:v:0 / -opt:a:1 / -opt:0 …: a per-stream index after a media
    // selector (or directly after the option).
    let mut parts = name.trim_start_matches('-').split(':');
    let _opt = parts.next();
    match (parts.next(), parts.next(), parts.next()) {
        (Some(sel), Some(idx), None) => {
            matches!(sel, "v" | "a" | "s" | "d" | "t")
                && !idx.is_empty()
                && idx.chars().all(|c| c.is_ascii_digit())
        }
        (Some(idx), None, None) => !idx.is_empty() && idx.chars().all(|c| c.is_ascii_digit()),
        _ => false,
    }
}

fn set_once_str(
    slot: &mut Option<String>,
    name: &str,
    value: String,
    index: usize,
) -> Result<(), CliError> {
    dup_check(slot.is_some(), name, index)?;
    *slot = Some(value);
    Ok(())
}

fn set_once_i64(
    slot: &mut Option<i64>,
    name: &str,
    value: &str,
    index: usize,
) -> Result<(), CliError> {
    dup_check(slot.is_some(), name, index)?;
    // The Seconds rule validated the token; parse cannot fail here.
    *slot = Some(parse_seconds_us(value).expect("validated seconds"));
    Ok(())
}

fn set_duration(
    slot: &mut Option<DurationBound>,
    kind: DurationKind,
    value: &str,
    index: usize,
) -> Result<(), CliError> {
    if let Some(existing) = slot {
        let (first, second) = match (existing.kind, kind) {
            (DurationKind::Duration, DurationKind::StopAt) => ("-t", "-to"),
            (DurationKind::StopAt, DurationKind::Duration) => ("-to", "-t"),
            (DurationKind::Duration, DurationKind::Duration) => {
                return dup_check(true, "-t", index)
            }
            (DurationKind::StopAt, DurationKind::StopAt) => return dup_check(true, "-to", index),
        };
        return Err(CliError::ConflictingOptions {
            first: first.to_string(),
            second: second.to_string(),
            first_index: Some(existing.token_index),
            second_index: Some(index),
            reason: "-t and -to in the SAME scope: the CLI resolves this pair with a \
                     precedence rule (-t wins); the subset rejects the pair instead of \
                     silently dropping one (cross-scope pairs remain legal)"
                .to_string(),
        });
    }
    *slot = Some(DurationBound {
        kind,
        us: parse_seconds_us(value).expect("validated seconds"),
        token_index: index,
    });
    Ok(())
}

fn dup_check(already: bool, name: &str, index: usize) -> Result<(), CliError> {
    if already {
        return Err(CliError::UnsupportedLayout {
            token: name.to_string(),
            index,
            reason: format!(
                "{name} appears more than once in the same scope; the CLI silently keeps the \
                 last value, which the subset refuses to reproduce"
            ),
        });
    }
    Ok(())
}
