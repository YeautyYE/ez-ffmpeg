//! Classification corpus: real-world commands swept through the full public
//! pipeline, each pinned to its EXACT disposition.
//!
//! Three dispositions exist — and only three:
//! - `runs_verified`: classification passes and the shape is golden-backed;
//!   `from_cli` proceeds past every gate (proven here by reaching the
//!   pipeline-build stage, which fails on the deliberately nonexistent
//!   fixture path — a `CliError::Build` is the success signal; real
//!   execution is `tests/cli_goldens.rs`' job);
//! - `emit_only`: parses completely, but no golden covers the shape;
//!   `from_cli` refuses with `NotVerified`, the emitter labels its output
//!   "unverified scaffolding";
//! - `rejected`: some token fails to classify; run AND emit fail with the
//!   same typed, token-anchored diagnostic.
//!
//! A common command silently doing the wrong thing is a release-blocking
//! bug; a common command clearly rejected with the right reason is CORRECT
//! behavior. This file is the executable form of that rule.

use super::error::CliError;
use super::{emit_rust_code, from_cli};

/// The command must pass classification, shape verification and the profile
/// gate. It then fails at pipeline build (nonexistent input) — precisely the
/// proof that no CLI-layer gate stopped it.
#[track_caller]
fn runs_verified(cmd: &str) {
    match from_cli(cmd) {
        Ok(_) => panic!("corpus fixtures must not exist, yet {cmd:?} built"),
        Err(CliError::Build(_)) => {}
        Err(other) => panic!("{cmd:?} should reach the build stage, got: {other}"),
    }
    let code = emit_rust_code(cmd).unwrap();
    assert!(
        code.contains("// status: verified shape"),
        "verified command must emit verified code: {cmd:?}"
    );
}

#[track_caller]
fn emit_only(cmd: &str) {
    match from_cli(cmd) {
        Ok(_) => panic!("{cmd:?} must not execute"),
        Err(CliError::NotVerified { .. }) => {}
        Err(other) => panic!("{cmd:?} should be NotVerified, got: {other}"),
    }
    let code = emit_rust_code(cmd).unwrap();
    assert!(
        code.contains("UNVERIFIED SCAFFOLDING"),
        "emit-only command must carry the scaffolding banner: {cmd:?}"
    );
}

/// `expect` names the variant; `fragment` must appear in the Display output
/// (the reason a user actually reads).
#[track_caller]
fn rejected(cmd: &str, expect: &str, fragment: &str) {
    let err = match from_cli(cmd) {
        Ok(_) => panic!("{cmd:?} must be rejected"),
        Err(err) => err,
    };
    let variant = match &err {
        CliError::Tokenize { .. } => "Tokenize",
        CliError::UnsupportedOption { .. } => "UnsupportedOption",
        CliError::UnsupportedValue { .. } => "UnsupportedValue",
        CliError::UnsupportedLayout { .. } => "UnsupportedLayout",
        CliError::ConflictingOptions { .. } => "ConflictingOptions",
        CliError::MissingOverwriteFlag => "MissingOverwriteFlag",
        CliError::NotVerified { .. } => "NotVerified",
        CliError::UnverifiedRuntimeProfile { .. } => "UnverifiedRuntimeProfile",
        CliError::Build(_) => "Build",
        _ => "other",
    };
    assert_eq!(
        variant, expect,
        "{cmd:?} rejected with the wrong variant: {err}"
    );
    let display = err.to_string();
    assert!(
        display.contains(fragment),
        "{cmd:?}: diagnostic should mention {fragment:?}, got:\n{display}"
    );
    // Emit must fail identically — rejection is a property of the command,
    // not of the consumer.
    let emit_err = emit_rust_code(cmd).unwrap_err();
    assert_eq!(err.to_string(), emit_err.to_string());
}

// ---------------------------------------------------------------------------
// The six verified commands (R6 slice) — the exact spellings of the goldens.
// ---------------------------------------------------------------------------

#[test]
fn corpus_v1_transcode() {
    runs_verified(
        "ffmpeg -i no_such_fixture.mkv -c:v libx264 -crf 23 -preset fast -c:a aac -y out.mp4",
    );
}

#[test]
fn corpus_v2_clip() {
    runs_verified(
        "ffmpeg -ss 10 -i no_such_fixture.mp4 -t 20 -c:v libx264 -crf 23 -c:a aac -y clip.mp4",
    );
}

#[test]
fn corpus_v3_audio_extract() {
    runs_verified("ffmpeg -i no_such_fixture.mp4 -vn -c:a aac -b:a 192k -y out.m4a");
}

#[test]
fn corpus_v4_thumbnail() {
    runs_verified("ffmpeg -ss 5 -i no_such_fixture.mp4 -an -c:v mjpeg -frames:v 1 -y thumb.jpg");
}

#[test]
fn corpus_v5_scale() {
    runs_verified(
        "ffmpeg -i no_such_fixture.mp4 -vf scale=1280:-2 -c:v libx264 -crf 23 -preset fast -c:a aac -y scaled.mp4",
    );
}

#[test]
fn corpus_v6_hls() {
    runs_verified(
        "ffmpeg -i no_such_fixture.mp4 -c:v libx264 -crf 23 -c:a aac -f hls -hls_time 6 -hls_playlist_type vod -hls_list_size 0 -hls_segment_filename 'seg_%03d.ts' -y out.m3u8",
    );
}

#[test]
fn corpus_verified_with_trailing_y_and_noops() {
    runs_verified(
        "ffmpeg -hide_banner -loglevel error -stats -i no_such_fixture.mkv -c:v libx264 -crf 23 -preset fast -c:a aac out.mp4 -y",
    );
}

// ---------------------------------------------------------------------------
// Cookbook recipes (the 15-entry CLI-to-API article), as pasted.
// ---------------------------------------------------------------------------

#[test]
fn corpus_cookbook_1_transcode_without_y() {
    // The article's spelling has no -y: the CLI would prompt; we reject.
    rejected(
        "ffmpeg -i input.mkv -c:v libx264 -crf 23 -preset fast -c:a aac output.mp4",
        "MissingOverwriteFlag",
        "requires an explicit -y",
    );
}

#[test]
fn corpus_cookbook_2_input_trim() {
    // Input-side -ss AND -t: parses, but no golden covers the shape.
    emit_only("ffmpeg -ss 10 -t 5 -i input.mp4 -y clip.mp4");
}

#[test]
fn corpus_cookbook_3_unsplit_c_copy() {
    rejected(
        "ffmpeg -i input.mp4 -c copy -movflags +faststart -y faststart.mp4",
        "UnsupportedOption",
        "did you mean `-c:v` and/or `-c:a`?",
    );
}

#[test]
fn corpus_cookbook_3_respelled_faststart_remux() {
    // The manifest-admitted spelling: explicit per-media copy. Emit-only —
    // faststart left the verified six (R6), and stays a Round-1 candidate.
    emit_only("ffmpeg -i input.mp4 -c:v copy -c:a copy -movflags +faststart -y faststart.mp4");
}

#[test]
fn corpus_cookbook_4_scale_with_audio_copy() {
    emit_only("ffmpeg -i input.mp4 -vf scale=1280:-2 -c:a copy -y resized.mp4");
}

#[test]
fn corpus_cookbook_5_thumbnail_recipe_shape() {
    // The article's thumbnail spelling (scale + -frames:v, no -an/-c:v):
    // parses, not the golden V4 shape.
    emit_only("ffmpeg -ss 3 -i input.mp4 -frames:v 1 -vf scale=320:-2 -y thumb.jpg");
}

#[test]
fn corpus_cookbook_6_fps_extraction() {
    rejected(
        "ffmpeg -i input.mp4 -vf fps=1 -y frame_%04d.jpg",
        "UnsupportedValue",
        "single simple scale filter",
    );
}

#[test]
fn corpus_cookbook_7_overlay_multi_input() {
    rejected(
        "ffmpeg -i input.mp4 -i logo.png -filter_complex overlay=10:10 -y watermarked.mp4",
        "UnsupportedLayout",
        "single-input",
    );
}

#[test]
fn corpus_cookbook_8_audio_copy_extract() {
    emit_only("ffmpeg -i input.mp4 -vn -c:a copy -y audio.aac");
}

#[test]
fn corpus_cookbook_8b_segment_muxer() {
    rejected(
        "ffmpeg -i input.mp4 -f segment -segment_time 10 -y chunk_%03d.wav",
        "UnsupportedOption",
        "segment muxer is planned for Round 2",
    );
}

#[test]
fn corpus_cookbook_9_whisper_pcm() {
    emit_only("ffmpeg -i input.mp4 -ar 16000 -ac 1 -c:a pcm_s16le -y audio.wav");
}

#[test]
fn corpus_cookbook_10_concat_filter() {
    rejected(
        "ffmpeg -i part1.mp4 -i part2.mp4 -i part3.mp4 -filter_complex concat=n=3:v=1:a=1 -y joined.mp4",
        "UnsupportedLayout",
        "single-input",
    );
}

#[test]
fn corpus_cookbook_11_gif_palette() {
    rejected(
        "ffmpeg -i input.mp4 -vf 'fps=12,scale=480:-1:flags=lanczos,split[a][b];[a]palettegen[p];[b][p]paletteuse' -y output.gif",
        "UnsupportedValue",
        "single simple scale filter",
    );
}

#[test]
fn corpus_cookbook_12_hls_without_the_full_option_set() {
    // The article's HLS spelling lacks -crf/-hls_list_size/-hls_segment_filename:
    // parses, but only the exact V6 set is golden-backed.
    emit_only(
        "ffmpeg -i input.mp4 -c:v libx264 -c:a aac -f hls -hls_time 6 -hls_playlist_type vod -y playlist.m3u8",
    );
}

#[test]
fn corpus_cookbook_13_rtmp_readrate() {
    rejected(
        "ffmpeg -re -i input.mp4 -c:v libx264 -c:a aac -f flv -y rtmp://localhost/live/stream",
        "UnsupportedOption",
        "readrate streaming is planned for Round 2",
    );
}

#[test]
fn corpus_cookbook_14_pasted_ffprobe_line() {
    // An ffprobe line is not an ffmpeg command; the leading token is not
    // stripped and lands in output position before any -i.
    rejected(
        "ffprobe -v error -show_format -show_streams input.mp4",
        "UnsupportedLayout",
        "before any -i input",
    );
}

#[test]
fn corpus_cookbook_15_two_pass() {
    rejected(
        "ffmpeg -y -i input.mp4 -c:v libx264 -b:v 2600k -pass 1 -an -f null /dev/null",
        "UnsupportedOption",
        "two-pass encoding",
    );
}

// ---------------------------------------------------------------------------
// Common patterns the evaluation catalogs (typed rejections).
// ---------------------------------------------------------------------------

#[test]
fn corpus_subtitle_burn_in() {
    rejected(
        "ffmpeg -i input.mp4 -vf subtitles=subs.srt -y out.mp4",
        "UnsupportedValue",
        "single simple scale filter",
    );
}

#[test]
fn corpus_shortest() {
    rejected(
        "ffmpeg -i input.mp4 -shortest -y out.mp4",
        "UnsupportedOption",
        "Output::set_shortest",
    );
}

#[test]
fn corpus_loudnorm_audio_filter() {
    rejected(
        "ffmpeg -i input.mp4 -af loudnorm -y out.mp4",
        "UnsupportedOption",
        "per-output audio filter API",
    );
}

#[test]
fn corpus_map_metadata() {
    rejected(
        "ffmpeg -i input.mp4 -map_metadata 0 -y out.mp4",
        "UnsupportedOption",
        "metadata mapping is planned for Round 2",
    );
}

#[test]
fn corpus_fps_mode_and_vsync_all_forms() {
    rejected(
        "ffmpeg -i input.mp4 -fps_mode cfr -y out.mp4",
        "UnsupportedOption",
        "permanently excluded",
    );
    rejected(
        "ffmpeg -i input.mp4 -vsync 0 -y out.mp4",
        "UnsupportedOption",
        "permanently excluded",
    );
    rejected(
        "ffmpeg -i input.mp4 -fps_mode:v cfr -y out.mp4",
        "UnsupportedOption",
        "not in the CLI-compat subset",
    );
}

#[test]
fn corpus_hardware_accel() {
    rejected(
        "ffmpeg -hwaccel cuda -i input.mp4 -y out.mp4",
        "UnsupportedOption",
        "Round 2",
    );
}

#[test]
fn corpus_frame_rate_and_size() {
    rejected(
        "ffmpeg -i input.mp4 -r 30 -y out.mp4",
        "UnsupportedOption",
        "Round-1",
    );
    rejected(
        "ffmpeg -i input.mp4 -s 1280x720 -y out.mp4",
        "UnsupportedOption",
        "did you mean `-vf scale=W:H`?",
    );
}

#[test]
fn corpus_quality_scale_and_unsplit_bitrate() {
    rejected(
        "ffmpeg -i input.mp4 -q:v 2 -y out.mp4",
        "UnsupportedOption",
        "fixed-quality",
    );
    rejected(
        "ffmpeg -i input.mp4 -b 2M -y out.mp4",
        "UnsupportedOption",
        "did you mean `-b:v` or `-b:a`?",
    );
}

#[test]
fn corpus_threads_and_progress() {
    rejected(
        "ffmpeg -i in.mp4 -threads 4 -y out.mp4",
        "UnsupportedOption",
        "auto threading",
    );
    rejected(
        "ffmpeg -i in.mp4 -progress p.txt -y out.mp4",
        "UnsupportedOption",
        "documented gap",
    );
}

#[test]
fn corpus_codec_tags_and_never_overwrite() {
    rejected(
        "ffmpeg -i in.mp4 -tag:v hvc1 -y out.mp4",
        "UnsupportedOption",
        "documented gap",
    );
    rejected(
        "ffmpeg -n -i in.mp4 -y out.mp4",
        "UnsupportedOption",
        "never-overwrite",
    );
}

#[test]
fn corpus_legacy_aliases_get_hints() {
    rejected(
        "ffmpeg -i in.mp4 -vcodec libx264 -y out.mp4",
        "UnsupportedOption",
        "did you mean `-c:v`?",
    );
    rejected(
        "ffmpeg -i in.mp4 -acodec aac -y out.mp4",
        "UnsupportedOption",
        "did you mean `-c:a`?",
    );
    rejected(
        "ffmpeg -i in.mp4 -vframes 1 -y out.jpg",
        "UnsupportedOption",
        "did you mean `-frames:v 1`?",
    );
}

// ---------------------------------------------------------------------------
// Map grammar.
// ---------------------------------------------------------------------------

#[test]
fn corpus_map_optional_suffix() {
    rejected(
        "ffmpeg -i in.mp4 -map 0:a:1? -y out.mp4",
        "UnsupportedValue",
        "fails loudly if the stream is missing",
    );
}

#[test]
fn corpus_map_negative() {
    rejected(
        "ffmpeg -i in.mp4 -map -0:a -y out.mp4",
        "UnsupportedValue",
        "negative mappings",
    );
}

#[test]
fn corpus_map_label() {
    rejected(
        "ffmpeg -i in.mp4 -map '[vout]' -y out.mp4",
        "UnsupportedValue",
        "basic index maps",
    );
}

#[test]
fn corpus_map_second_input_index() {
    rejected(
        "ffmpeg -i in.mp4 -map 1:a -y out.mp4",
        "UnsupportedValue",
        "single-input",
    );
}

#[test]
fn corpus_map_subtitles() {
    rejected(
        "ffmpeg -i in.mkv -map 0:s -y out.mkv",
        "UnsupportedValue",
        "subtitle/data",
    );
}

#[test]
fn corpus_map_metadata_selector() {
    rejected(
        "ffmpeg -i in.mp4 -map 0:m:language:eng -y out.mp4",
        "UnsupportedValue",
        "basic index maps",
    );
}

#[test]
fn corpus_basic_maps_parse_in_filterless_commands() {
    emit_only("ffmpeg -i in.mp4 -map 0:v:0 -map 0:a:0 -c:v libx264 -c:a aac -y out.mp4");
}

#[test]
fn corpus_media_qualified_copy_maps_parse() {
    emit_only("ffmpeg -i in.mp4 -map 0:a:0 -c:a copy -y audio.m4a");
}

#[test]
fn corpus_bare_map_with_copy_is_untypeable() {
    rejected(
        "ffmpeg -i in.mp4 -map 0 -c:v copy -c:a aac -y out.mp4",
        "ConflictingOptions",
        "media-qualified maps",
    );
}

#[test]
fn corpus_map_against_disabled_stream() {
    rejected(
        "ffmpeg -i in.mp4 -map 0:a -an -c:v libx264 -y out.mp4",
        "ConflictingOptions",
        "-an removes",
    );
}

#[test]
fn corpus_map_with_filter() {
    rejected(
        "ffmpeg -i in.mp4 -map 0:v -vf scale=640:360 -c:v libx264 -y out.mp4",
        "ConflictingOptions",
        "labeled graphs",
    );
}

// ---------------------------------------------------------------------------
// Per-stream indexed variants and complex graphs.
// ---------------------------------------------------------------------------

#[test]
fn corpus_indexed_per_stream_variants() {
    rejected(
        "ffmpeg -i in.mp4 -b:v:1 500k -y out.mp4",
        "UnsupportedOption",
        "per-stream indexed",
    );
    rejected(
        "ffmpeg -i in.mp4 -crf:v:0 23 -y out.mp4",
        "UnsupportedOption",
        "per-stream indexed",
    );
    rejected(
        "ffmpeg -i in.mp4 -c:v:0 libx264 -y out.mp4",
        "UnsupportedOption",
        "per-stream indexed",
    );
}

#[test]
fn corpus_filter_complex_single_input() {
    rejected(
        "ffmpeg -i in.mp4 -filter_complex '[0:v]hue=s=0[v]' -map '[v]' -y out.mp4",
        "UnsupportedOption",
        "Round 2",
    );
}

// ---------------------------------------------------------------------------
// Trim grammar: scope layering.
// ---------------------------------------------------------------------------

#[test]
fn corpus_t_and_to_same_input_scope() {
    rejected(
        "ffmpeg -ss 1 -t 5 -to 8 -i in.mp4 -y out.mp4",
        "ConflictingOptions",
        "SAME scope",
    );
}

#[test]
fn corpus_t_and_to_same_output_scope() {
    rejected(
        "ffmpeg -i in.mp4 -t 5 -to 8 -y out.mp4",
        "ConflictingOptions",
        "SAME scope",
    );
}

#[test]
fn corpus_cross_scope_trim_pair_is_legal() {
    emit_only("ffmpeg -t 5 -i in.mp4 -to 8 -y out.mp4");
}

#[test]
fn corpus_output_side_seek() {
    emit_only(
        "ffmpeg -i in.mp4 -ss 10 -t 20 -c:v libx264 -crf 23 -preset fast -c:a aac -y out.mp4",
    );
}

#[test]
fn corpus_time_value_grammar() {
    rejected(
        "ffmpeg -ss 00:01:30 -i in.mp4 -y out.mp4",
        "UnsupportedValue",
        "decimal seconds",
    );
    rejected(
        "ffmpeg -ss -5 -i in.mp4 -y out.mp4",
        "UnsupportedValue",
        "signed times",
    );
    rejected(
        "ffmpeg -i in.mp4 -t 10s -y out.mp4",
        "UnsupportedValue",
        "decimal seconds",
    );
}

// ---------------------------------------------------------------------------
// Value whitelists.
// ---------------------------------------------------------------------------

#[test]
fn corpus_crf_range() {
    rejected(
        "ffmpeg -i in.mp4 -c:v libx264 -crf 99 -y out.mp4",
        "UnsupportedValue",
        "0..=51",
    );
}

#[test]
fn corpus_preset_whitelist() {
    rejected(
        "ffmpeg -i in.mp4 -c:v libx264 -preset turbo -y out.mp4",
        "UnsupportedValue",
        "x264 presets",
    );
}

#[test]
fn corpus_crf_requires_x264() {
    rejected(
        "ffmpeg -i in.mp4 -c:v mpeg4 -crf 23 -y out.mp4",
        "ConflictingOptions",
        "libx264",
    );
    rejected(
        "ffmpeg -i in.mp4 -crf 23 -y out.mp4",
        "ConflictingOptions",
        "libx264",
    );
}

#[test]
fn corpus_hls_pins() {
    rejected(
        "ffmpeg -i in.mp4 -c:v libx264 -crf 23 -c:a aac -f hls -hls_time 6 -hls_playlist_type event -hls_list_size 0 -hls_segment_filename s_%03d.ts -y out.m3u8",
        "UnsupportedValue",
        "vod",
    );
    rejected(
        "ffmpeg -i in.mp4 -c:v libx264 -crf 23 -c:a aac -f hls -hls_time 6 -hls_playlist_type vod -hls_list_size 5 -hls_segment_filename s_%03d.ts -y out.m3u8",
        "UnsupportedValue",
        "keep every segment",
    );
    rejected(
        "ffmpeg -i in.mp4 -c:v libx264 -crf 23 -c:a aac -f hls -hls_time 6 -hls_playlist_type vod -hls_list_size 0 -hls_flags delete_segments -hls_segment_filename s_%03d.ts -y out.m3u8",
        "UnsupportedOption",
        "single-rendition VOD HLS",
    );
}

#[test]
fn corpus_hls_options_without_f_hls() {
    rejected(
        "ffmpeg -i in.mp4 -c:v libx264 -crf 23 -c:a aac -hls_time 6 -y out.m3u8",
        "ConflictingOptions",
        "-f hls",
    );
}

#[test]
fn corpus_movflags_exact_form_only() {
    rejected(
        "ffmpeg -i in.mp4 -c:v copy -c:a copy -movflags +faststart+frag_keyframe -y out.mp4",
        "UnsupportedValue",
        "+faststart",
    );
}

#[test]
fn corpus_frames_v_only_one() {
    rejected(
        "ffmpeg -i in.mp4 -an -c:v mjpeg -frames:v 5 -y thumbs_%02d.jpg",
        "UnsupportedValue",
        "single-frame",
    );
}

// ---------------------------------------------------------------------------
// Layout and structure.
// ---------------------------------------------------------------------------

#[test]
fn corpus_multi_output() {
    rejected(
        "ffmpeg -i in.mp4 -c:v libx264 -y out1.mp4 out2.mp4",
        "UnsupportedLayout",
        "single-output",
    );
}

#[test]
fn corpus_interleaved_input_after_output() {
    rejected(
        "ffmpeg -i a.mp4 -y out.mp4 -i b.mp4",
        "UnsupportedLayout",
        "inputs-then-outputs",
    );
}

#[test]
fn corpus_output_option_after_output_path() {
    rejected(
        "ffmpeg -i in.mp4 -y out.mp4 -c:v libx264",
        "UnsupportedLayout",
        "FOLLOWING output",
    );
}

#[test]
fn corpus_output_option_in_input_position() {
    rejected(
        "ffmpeg -crf 23 -i in.mp4 -y out.mp4",
        "UnsupportedLayout",
        "before -i",
    );
}

#[test]
fn corpus_missing_files() {
    rejected("ffmpeg -y -i in.mp4", "UnsupportedLayout", "no output path");
    rejected("ffmpeg -y out.mp4", "UnsupportedLayout", "before any -i");
    rejected("ffmpeg -i in.mp4 -c:v", "UnsupportedValue", "missing value");
}

#[test]
fn corpus_stdin_stdout_pipes() {
    rejected("ffmpeg -i - -y out.mp4", "UnsupportedLayout", "stdin");
    rejected(
        "ffmpeg -i in.mp4 -f mpegts -y -",
        "UnsupportedLayout",
        "stdout",
    );
}

#[test]
fn corpus_duplicate_option() {
    rejected(
        "ffmpeg -i in.mp4 -c:v libx264 -crf 23 -crf 30 -y out.mp4",
        "UnsupportedLayout",
        "more than once",
    );
}

#[test]
fn corpus_double_dash_is_an_unknown_option() {
    rejected(
        "ffmpeg -i in.mp4 -- -y out.mp4",
        "UnsupportedOption",
        "not in the CLI-compat subset",
    );
}

#[test]
fn corpus_unknown_option_with_hint() {
    rejected(
        "ffmpeg -i in.mp4 -vff scale=1:1 -y out.mp4",
        "UnsupportedOption",
        "did you mean `-vf`?",
    );
}

#[test]
fn corpus_unknown_option_without_hint() {
    rejected(
        "ffmpeg -i in.mp4 -totally_unknown_thing 1 -y out.mp4",
        "UnsupportedOption",
        "open an issue",
    );
}

// ---------------------------------------------------------------------------
// Contradictions.
// ---------------------------------------------------------------------------

#[test]
fn corpus_disabled_stream_contradictions() {
    rejected(
        "ffmpeg -i in.mp4 -an -c:a aac -y out.mp4",
        "ConflictingOptions",
        "-an removes",
    );
    rejected(
        "ffmpeg -i in.mp4 -vn -crf 23 -y out.m4a",
        "ConflictingOptions",
        "-vn removes",
    );
    rejected(
        "ffmpeg -i in.mp4 -vn -an -y out.mp4",
        "ConflictingOptions",
        "no streams",
    );
}

#[test]
fn corpus_filter_with_copy_or_vn() {
    rejected(
        "ffmpeg -i in.mp4 -vf scale=640:360 -c:v copy -y out.mp4",
        "ConflictingOptions",
        "streamcopy",
    );
    rejected(
        "ffmpeg -i in.mp4 -vf scale=640:360 -vn -y out.m4a",
        "ConflictingOptions",
        "removes video",
    );
}

#[test]
fn corpus_copy_with_reencode_knobs() {
    rejected(
        "ffmpeg -i in.mp4 -c:v copy -pix_fmt yuv420p -y out.mp4",
        "ConflictingOptions",
        "does not decode",
    );
    rejected(
        "ffmpeg -i in.mp4 -c:a copy -ar 44100 -y out.m4a",
        "ConflictingOptions",
        "does not decode",
    );
}

// ---------------------------------------------------------------------------
// Shell-level constructs arriving through the string form.
// ---------------------------------------------------------------------------

#[test]
fn corpus_shell_redirect_and_pipe() {
    rejected(
        "ffmpeg -i in.mp4 -y out.mp4 2>&1",
        "Tokenize",
        "shell operator",
    );
    rejected(
        "ffmpeg -i in.mp4 -y out.mp4 | cat",
        "Tokenize",
        "shell operator",
    );
}

#[test]
fn corpus_shell_variable() {
    rejected("ffmpeg -i $SRC -y out.mp4", "Tokenize", "expansion");
}

#[test]
fn corpus_empty_command() {
    rejected("", "UnsupportedLayout", "no -i input");
    rejected("ffmpeg", "UnsupportedLayout", "no -i input");
}

// ---------------------------------------------------------------------------
// Additional accepted-but-unverified spellings (Round-1 surface).
// ---------------------------------------------------------------------------

#[test]
fn corpus_bitrate_ladder_spelling() {
    emit_only("ffmpeg -i in.mp4 -c:v libx264 -b:v 2M -c:a aac -b:a 128k -y out.mp4");
}

#[test]
fn corpus_pix_fmt() {
    emit_only("ffmpeg -i in.mp4 -c:v libx264 -pix_fmt yuv420p -y out.mp4");
}

#[test]
fn corpus_input_format_override() {
    emit_only("ffmpeg -f lavfi -i testsrc2=size=320x240:rate=30 -c:v libx264 -y out.mp4");
}

#[test]
fn corpus_default_codecs_remux() {
    emit_only("ffmpeg -i in.mov -y out.mp4");
}

// ---------------------------------------------------------------------------
// No-panic property sweep (the fuzz obligation, deterministic form).
// ---------------------------------------------------------------------------

/// Every generated string must classify (Ok or a typed Err) — the tokenizer
/// and scoper must never panic, whatever byte salad arrives. The generator
/// is a seeded LCG over an alphabet chosen to stress quoting, escapes,
/// continuations, multi-byte characters and option-shaped fragments.
#[test]
fn corpus_no_panic_property_sweep() {
    const ALPHABET: &[&str] = &[
        " ", "-", "i", "c", ":", "v", "'", "\"", "\\", "\n", "^", "$", "0", "9", ".", "=", "映",
        "🎬", "\t", "\r", "%", "?", "*", "[", "]", "y", "-i", "-y", "-c:v", "scale", "map", "~",
        "#",
    ];
    let mut state: u64 = 0x243F_6A88_85A3_08D3;
    let mut next = move || {
        state = state
            .wrapping_mul(6364136223846793005)
            .wrapping_add(1442695040888963407);
        (state >> 33) as usize
    };
    for _ in 0..500 {
        let len = next() % 24;
        let mut cmd = String::new();
        for _ in 0..len {
            cmd.push_str(ALPHABET[next() % ALPHABET.len()]);
        }
        // The property is totality, not acceptance.
        let _ = from_cli(&cmd).map(|_| ());
        let _ = emit_rust_code(&cmd);
    }
}

/// argv-form totality: raw token vectors (no tokenizer in front) with hostile
/// shapes must classify without panicking.
#[test]
fn corpus_no_panic_argv_sweep() {
    let hostile: &[&[&str]] = &[
        &[],
        &[""],
        &["-"],
        &["--"],
        &["-i"],
        &["-i", ""],
        &["-i", "-i"],
        &["-ss"],
        &["-ss", "-ss", "-i", "x", "-y", "y.mp4"],
        &["-c:v", "-c:a", "-i", "x", "-y", "y.mp4"],
        &["🎬", "-y"],
        &["-i", "🎬", "-y", "映画.mp4"],
        &["-map", "", "-i", "x", "-y", "y.mp4"],
        &["-vf", "", "-i", "x", "-y", "y.mp4"],
        &["-y", "-y", "-i", "x", "out.mp4"],
    ];
    for args in hostile {
        let _ = super::from_cli_args(args).map(|_| ());
        let _ = super::emit_rust_code_from_args(args);
    }
}
