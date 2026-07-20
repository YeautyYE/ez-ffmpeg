//! Code emitter: [`LoweredJob`] -> a complete, compile-ready Rust program.
//!
//! The emitter consumes the SAME lowered plan the runtime path applies to
//! the builder (`LoweredJob::into_context`) — field for field, in the same
//! order — so generated code and in-process execution cannot drift. Every
//! emitted call exists in the crate's public API; a checked-in emitted
//! program is compiled as a real example (`examples/cli_emitted_transcode.rs`)
//! and pinned byte-for-byte by a unit test below.

use super::lower::LoweredJob;
use super::manifest::{ShapeStatus, DIALECT, MANIFEST_REVISION};

/// Renders the program. `command` is the original argv (for the header
/// comment); `status` decides between the verified header and the
/// unverified-scaffolding banner.
pub(crate) fn emit(job: &LoweredJob, command: &[String], status: &ShapeStatus) -> String {
    let plain_input = job.input.format.is_none()
        && job.input.start_time_us.is_none()
        && job.input.recording_time_us.is_none()
        && job.input.stop_time_us.is_none();

    let mut out = String::new();
    header(&mut out, command, status, plain_input);

    // Recognized no-op globals: named instead of silently swallowed.
    for noop in &job.noops {
        match &noop.value {
            Some(value) => out.push_str(&format!(
                "// {} {}: not applicable in-process (no-op)\n",
                noop.flag, value
            )),
            None => out.push_str(&format!(
                "// {}: not applicable in-process (no-op)\n",
                noop.flag
            )),
        }
    }
    if !job.noops.is_empty() {
        out.push('\n');
    }

    out.push_str("fn main() -> Result<(), Box<dyn std::error::Error>> {\n");
    out.push_str("    FfmpegContext::builder()\n");

    if plain_input {
        line(&mut out, 2, &format!(".input({})", lit(&job.input.url)));
    } else {
        line(&mut out, 2, ".input(");
        line(&mut out, 3, &format!("Input::from({})", lit(&job.input.url)));
        if let Some(format) = &job.input.format {
            line(&mut out, 4, &format!(".set_format({}) // -f {format}", lit(format)));
        }
        if let Some(us) = job.input.start_time_us {
            line(
                &mut out,
                4,
                &format!(".set_start_time_us({}) // -ss (input side, seconds -> microseconds)", num(us)),
            );
        }
        if let Some(us) = job.input.recording_time_us {
            line(&mut out, 4, &format!(".set_recording_time_us({}) // -t (input side)", num(us)));
        }
        if let Some(us) = job.input.stop_time_us {
            line(&mut out, 4, &format!(".set_stop_time_us({}) // -to (input side)", num(us)));
        }
        line(&mut out, 2, ")");
    }

    line(&mut out, 2, ".output(");
    line(&mut out, 3, &format!("Output::from({})", lit(&job.output.url)));
    let o = &job.output;
    if let Some(format) = &o.format {
        line(&mut out, 4, &format!(".set_format({}) // -f {format}", lit(format)));
    }
    if o.video_disable {
        line(&mut out, 4, ".disable_video() // -vn");
    }
    if o.audio_disable {
        line(&mut out, 4, ".disable_audio() // -an");
    }
    if let Some(codec) = &o.video_codec {
        line(&mut out, 4, &format!(".set_video_codec({}) // -c:v {codec}", lit(codec)));
    }
    if let Some(codec) = &o.audio_codec {
        line(&mut out, 4, &format!(".set_audio_codec({}) // -c:a {codec}", lit(codec)));
    }
    if let Some(bitrate) = &o.video_bitrate {
        line(&mut out, 4, &format!(".set_video_bitrate({}) // -b:v {bitrate}", lit(bitrate)));
    }
    if let Some(bitrate) = &o.audio_bitrate {
        line(&mut out, 4, &format!(".set_audio_bitrate({}) // -b:a {bitrate}", lit(bitrate)));
    }
    for (key, value) in &o.video_codec_opts {
        line(
            &mut out,
            4,
            &format!(".set_video_codec_opt({}, {}) // -{key} {value}", lit(key), lit(value)),
        );
    }
    for (key, value) in &o.format_opts {
        line(
            &mut out,
            4,
            &format!(".set_format_opt({}, {}) // -{key} {value}", lit(key), lit(value)),
        );
    }
    if let Some(pix_fmt) = &o.pix_fmt {
        line(&mut out, 4, &format!(".set_pix_fmt({}) // -pix_fmt {pix_fmt}", lit(pix_fmt)));
    }
    if let Some(rate) = o.audio_sample_rate {
        line(&mut out, 4, &format!(".set_audio_sample_rate({rate}) // -ar {rate}"));
    }
    if let Some(channels) = o.audio_channels {
        line(&mut out, 4, &format!(".set_audio_channels({channels}) // -ac {channels}"));
    }
    if let Some(frames) = o.max_video_frames {
        line(
            &mut out,
            4,
            &format!(".set_max_video_frames({frames}) // -frames:v {frames} (image2 update mode is applied automatically)"),
        );
    }
    if let Some(filter) = &o.video_filter {
        line(&mut out, 4, &format!(".set_video_filter({}) // -vf {filter}", lit(filter)));
    }
    for (map, copy) in &o.stream_maps {
        if *copy {
            line(
                &mut out,
                4,
                &format!(".add_stream_map_with_copy({}) // -map {map} + copy", lit(map)),
            );
        } else {
            line(&mut out, 4, &format!(".add_stream_map({}) // -map {map}", lit(map)));
        }
    }
    if let Some(us) = o.start_time_us {
        line(
            &mut out,
            4,
            &format!(".set_start_time_us({}) // -ss (output side: decode, then discard)", num(us)),
        );
    }
    if let Some(us) = o.recording_time_us {
        line(&mut out, 4, &format!(".set_recording_time_us({}) // -t", num(us)));
    }
    if let Some(us) = o.stop_time_us {
        line(&mut out, 4, &format!(".set_stop_time_us({}) // -to", num(us)));
    }
    line(&mut out, 2, ")");
    line(&mut out, 2, ".build()?");
    line(&mut out, 2, ".start()?");
    line(&mut out, 2, ".wait()?;");
    out.push_str("    Ok(())\n}\n");
    out
}

fn header(out: &mut String, command: &[String], status: &ShapeStatus, plain_input: bool) {
    out.push_str("// Generated from an ffmpeg command by the ez-ffmpeg CLI-compat emitter.\n");
    out.push_str(&format!("// command: ffmpeg {}\n", requote(command)));
    out.push_str(&format!(
        "// dialect: {DIALECT}; manifest: r{MANIFEST_REVISION}; crate: ez-ffmpeg {}; cargo features: none required\n",
        env!("CARGO_PKG_VERSION")
    ));
    match status {
        ShapeStatus::Verified(id) => {
            match super::manifest::shape(id) {
                Some(shape) => out.push_str(&format!(
                    "// status: verified shape {id} ({}) — backed by the semantic golden \
                     `{}` against the ffmpeg CLI\n",
                    shape.summary, shape.golden
                )),
                None => out.push_str(&format!(
                    "// status: verified shape {id} — backed by a semantic golden against the ffmpeg CLI\n"
                )),
            }
        }
        ShapeStatus::Unverified => {
            out.push_str(
                "// status: UNVERIFIED SCAFFOLDING — this command shape has no semantic golden\n\
                 // in the compatibility manifest. The code below compiles against the ez-ffmpeg\n\
                 // builder API, but its behavior has NOT been checked against the ffmpeg CLI and\n\
                 // must not be treated as a faithful translation. Review every call before use;\n\
                 // in-process execution (from_cli / from_cli_args) refuses this shape.\n",
            );
        }
    }
    out.push('\n');
    // Import exactly what the program uses: a plain `.input("url")` never
    // names the Input type, and the emitted file must compile warning-free.
    if plain_input {
        out.push_str("use ez_ffmpeg::{FfmpegContext, Output};\n\n");
    } else {
        out.push_str("use ez_ffmpeg::{FfmpegContext, Input, Output};\n\n");
    }
}

/// One indented builder line.
fn line(out: &mut String, level: usize, text: &str) {
    for _ in 0..level {
        out.push_str("    ");
    }
    out.push_str(text);
    out.push('\n');
}

/// A Rust string literal for `s`. `{:?}` escapes quotes, backslashes and
/// control characters and passes unicode through — exactly a valid literal.
fn lit(s: &str) -> String {
    format!("{s:?}")
}

/// Microsecond literals with `_` thousands separators, matching the crate's
/// documented `10_000_000` style.
fn num(us: i64) -> String {
    let digits = us.to_string();
    let mut grouped = String::new();
    for (i, ch) in digits.chars().enumerate() {
        if i > 0 && (digits.len() - i) % 3 == 0 {
            grouped.push('_');
        }
        grouped.push(ch);
    }
    grouped
}

/// Reassembles the argv into a copy-pasteable POSIX command line: tokens with
/// whitespace or quoting characters are single-quoted (embedded single quotes
/// via the `'\''` idiom).
fn requote(command: &[String]) -> String {
    command
        .iter()
        .map(|token| {
            let simple = !token.is_empty()
                && token.chars().all(|c| {
                    c.is_ascii_alphanumeric()
                        || matches!(c, '-' | '_' | '.' | '/' | ':' | '=' | '+' | '%' | ',' | '@')
                });
            if simple {
                token.clone()
            } else {
                format!("'{}'", token.replace('\'', r"'\''"))
            }
        })
        .collect::<Vec<_>>()
        .join(" ")
}

#[cfg(test)]
mod tests {
    use super::super::{parse::parse, tokenize::tokenize};
    use super::*;
    use crate::core::cli::lower::lower;
    use crate::core::cli::manifest::classify;

    fn emit_cmd(cmd: &str) -> String {
        let args = tokenize(cmd).unwrap();
        let ir = parse(&args).unwrap_or_else(|e| panic!("{cmd} should parse: {e}"));
        let status = classify(&ir);
        emit(&lower(&ir), &args, &status)
    }

    #[test]
    fn num_groups_digits() {
        assert_eq!(num(0), "0");
        assert_eq!(num(500), "500");
        assert_eq!(num(10_000_000), "10_000_000");
        assert_eq!(num(2_500_000), "2_500_000");
        assert_eq!(num(100), "100");
        assert_eq!(num(1000), "1_000");
    }

    #[test]
    fn lit_escapes_quotes_and_keeps_unicode() {
        assert_eq!(lit(r#"a"b"#), r#""a\"b""#);
        assert_eq!(lit("视频.mp4"), "\"视频.mp4\"");
    }

    #[test]
    fn requote_quotes_tokens_with_spaces() {
        let args = vec!["-i".to_string(), "my movie.mp4".to_string()];
        assert_eq!(requote(&args), "-i 'my movie.mp4'");
    }

    #[test]
    fn verified_emit_carries_dialect_and_shape() {
        let code = emit_cmd("ffmpeg -i in.mkv -c:v libx264 -crf 23 -preset fast -c:a aac -y out.mp4");
        assert!(code.contains("// status: verified shape V1"));
        assert!(code.contains("dialect: ffmpeg 7.1 command line"));
        assert!(code.contains("manifest: r1"));
        assert!(code.contains(".set_video_codec(\"libx264\") // -c:v libx264"));
        assert!(code.contains(".set_video_codec_opt(\"crf\", \"23\") // -crf 23"));
        assert!(code.contains(".set_video_codec_opt(\"preset\", \"fast\") // -preset fast"));
        assert!(code.contains(".set_audio_codec(\"aac\") // -c:a aac"));
        assert!(!code.contains("UNVERIFIED"));
    }

    #[test]
    fn clip_emit_scopes_trims_correctly() {
        let code =
            emit_cmd("ffmpeg -ss 10 -i in.mp4 -t 20 -c:v libx264 -crf 23 -c:a aac -y clip.mp4");
        assert!(code.contains("Input::from(\"in.mp4\")"));
        assert!(code.contains(".set_start_time_us(10_000_000) // -ss (input side"));
        assert!(code.contains(".set_recording_time_us(20_000_000) // -t"));
        // The output must NOT carry the input's -ss.
        assert!(!code.contains("// -ss (output side"));
    }

    #[test]
    fn thumbnail_emit_uses_generic_path() {
        let code = emit_cmd("ffmpeg -ss 5 -i in.mp4 -an -c:v mjpeg -frames:v 1 -y thumb.jpg");
        assert!(code.contains(".disable_audio() // -an"));
        assert!(code.contains(".set_video_codec(\"mjpeg\")"));
        assert!(code.contains(".set_max_video_frames(1)"));
        assert!(code.contains("image2 update mode is applied automatically"));
    }

    #[test]
    fn scale_emit_uses_per_output_filter() {
        let code = emit_cmd(
            "ffmpeg -i in.mp4 -vf scale=1280:-2 -c:v libx264 -crf 23 -preset fast -c:a aac -y scaled.mp4",
        );
        assert!(code.contains(".set_video_filter(\"scale=1280:-2\") // -vf scale=1280:-2"));
    }

    #[test]
    fn hls_emit_maps_every_muxer_option() {
        let code = emit_cmd(
            "ffmpeg -i in.mp4 -c:v libx264 -crf 23 -c:a aac -f hls -hls_time 6 -hls_playlist_type vod -hls_list_size 0 -hls_segment_filename 'seg_%03d.ts' -y out.m3u8",
        );
        assert!(code.contains(".set_format(\"hls\") // -f hls"));
        assert!(code.contains(".set_format_opt(\"hls_time\", \"6\")"));
        assert!(code.contains(".set_format_opt(\"hls_playlist_type\", \"vod\")"));
        assert!(code.contains(".set_format_opt(\"hls_list_size\", \"0\")"));
        assert!(code.contains(".set_format_opt(\"hls_segment_filename\", \"seg_%03d.ts\")"));
    }

    #[test]
    fn audio_extract_emit() {
        let code = emit_cmd("ffmpeg -i in.mp4 -vn -c:a aac -b:a 192k -y out.m4a");
        assert!(code.contains(".disable_video() // -vn"));
        assert!(code.contains(".set_audio_bitrate(\"192k\") // -b:a 192k"));
    }

    #[test]
    fn unverified_emit_carries_the_scaffolding_banner() {
        // Parses (all tokens classify) but matches no golden-backed shape.
        let code = emit_cmd("ffmpeg -i in.mp4 -c:v mpeg4 -y out.avi");
        assert!(code.contains("UNVERIFIED SCAFFOLDING"));
        assert!(code.contains("refuses this shape"));
        // The scaffolding must never claim equivalence.
        assert!(!code.to_lowercase().contains("equivalent"));
    }

    #[test]
    fn emitted_code_quotes_paths_with_spaces_and_unicode() {
        let code = emit_cmd("ffmpeg -i '我的 视频.mp4' -c:v mpeg4 -y '导出 v1.avi'");
        assert!(code.contains(".input(\"我的 视频.mp4\")"));
        assert!(code.contains("Output::from(\"导出 v1.avi\")"));
        assert!(code.contains("// command: ffmpeg -i '我的 视频.mp4'"));
    }

    #[test]
    fn faststart_remux_emit_is_unverified_with_explicit_copies() {
        let code = emit_cmd(
            "ffmpeg -i in.mp4 -c:v copy -c:a copy -movflags +faststart -y faststart.mp4",
        );
        assert!(code.contains("UNVERIFIED SCAFFOLDING"));
        assert!(code.contains(".set_video_codec(\"copy\") // -c:v copy"));
        assert!(code.contains(".set_audio_codec(\"copy\") // -c:a copy"));
        assert!(code.contains(".set_format_opt(\"movflags\", \"+faststart\")"));
    }
}
