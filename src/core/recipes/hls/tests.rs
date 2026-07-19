use std::collections::HashMap;

use super::*;

fn ladder() -> HlsLadder {
    HlsLadder::new("input.mp4", "out")
        .rendition(1280, 720, "2800k")
        .rendition(640, 360, "800k")
}

// ---- build_split_desc ------------------------------------------------

#[test]
fn split_desc_single_rendition() {
    let renditions = vec![Rendition::new(640, 360, "800k")];
    assert_eq!(
        build_split_desc(&renditions),
        "[0:v]split=1[s0];[s0]scale=640:360,setsar=1,format=yuv420p[v0]"
    );
}

#[test]
fn split_desc_three_renditions() {
    let renditions = vec![
        Rendition::new(1920, 1080, "5000k"),
        Rendition::new(1280, 720, "2800k"),
        Rendition::new(640, 360, "800k"),
    ];
    assert_eq!(
        build_split_desc(&renditions),
        "[0:v]split=3[s0][s1][s2];\
             [s0]scale=1920:1080,setsar=1,format=yuv420p[v0];\
             [s1]scale=1280:720,setsar=1,format=yuv420p[v1];\
             [s2]scale=640:360,setsar=1,format=yuv420p[v2]"
    );
}

#[test]
fn split_desc_every_branch_has_yuv420p() {
    let renditions = vec![
        Rendition::new(1280, 720, "2800k"),
        Rendition::new(640, 360, "800k"),
    ];
    let desc = build_split_desc(&renditions);
    assert_eq!(desc.matches("format=yuv420p").count(), 2);
}

// ---- compute_gop_frames ----------------------------------------------

#[test]
fn gop_exact_integer_rates() {
    // 30fps * 6s = 180, 25fps * 6s = 150 — exact, no rounding.
    assert_eq!(compute_gop_frames(30, 1, 6_000_000), 180);
    assert_eq!(compute_gop_frames(25, 1, 6_000_000), 150);
}

#[test]
fn gop_ntsc_rational_not_broken_by_round() {
    // 30000/1001 ≈ 29.97fps; * 6s ≈ 179.82 -> ceil = 180.
    assert_eq!(compute_gop_frames(30000, 1001, 6_000_000), 180);
    // 24000/1001 ≈ 23.976fps; * 1s -> ceil = 24.
    assert_eq!(compute_gop_frames(24000, 1001, 1_000_000), 24);
}

#[test]
fn gop_uses_ceil_not_round() {
    // 7/3 ≈ 2.333fps * 1s: ceil = 3, round would give 2. Proves ceil().
    assert_eq!(compute_gop_frames(7, 3, 1_000_000), 3);
}

#[test]
fn gop_degenerate_inputs_clamp_to_one() {
    assert_eq!(compute_gop_frames(0, 1, 6_000_000), 1);
    assert_eq!(compute_gop_frames(30, 0, 6_000_000), 1);
    assert_eq!(compute_gop_frames(30, 1, 0), 1);
}

// ---- bitrate parsing + bandwidth -------------------------------------

#[test]
fn parse_bitrate_suffixes() {
    assert_eq!(parse_bitrate_bps("800000").unwrap(), 800_000);
    assert_eq!(parse_bitrate_bps("800k").unwrap(), 800_000);
    assert_eq!(parse_bitrate_bps("800K").unwrap(), 800_000);
    assert_eq!(parse_bitrate_bps("5M").unwrap(), 5_000_000);
    assert_eq!(parse_bitrate_bps("1.5M").unwrap(), 1_500_000);
}

#[test]
fn parse_bitrate_rejects_garbage() {
    assert!(parse_bitrate_bps("").is_err());
    assert!(parse_bitrate_bps("abc").is_err());
    assert!(parse_bitrate_bps("0").is_err());
    assert!(parse_bitrate_bps("-5M").is_err());
}

#[test]
fn bandwidth_folds_audio_and_overhead() {
    // (5_000_000 + 128_000) * 1.1 = 5_640_800.
    assert_eq!(compute_bandwidth(5_000_000, Some(128_000)), 5_640_800);
    // No audio: 5_000_000 * 1.1 = 5_500_000.
    assert_eq!(compute_bandwidth(5_000_000, None), 5_500_000);
}

// ---- master variants (BANDWIDTH / RESOLUTION / uri) ------------------

#[test]
fn master_variants_carry_resolution_and_relative_uri() {
    let variants = ladder().build_master_variants(true).unwrap();
    assert_eq!(variants.len(), 2);

    let v720 = &variants[0];
    assert_eq!((v720.width, v720.height), (1280, 720));
    assert_eq!(v720.uri, "720p/index.m3u8");
    // (2_800_000 + 128_000) * 1.1 = 3_220_800.
    assert_eq!(v720.bandwidth, 3_220_800);
    assert!(v720.codecs.is_none());
}

#[test]
fn master_variants_omit_audio_when_absent() {
    let variants = ladder().build_master_variants(false).unwrap();
    // 800k video only: 800_000 * 1.1 = 880_000.
    assert_eq!(variants[1].bandwidth, 880_000);
}

#[test]
fn master_variants_custom_name_becomes_uri() {
    let variants = HlsLadder::new("input.mp4", "out")
        .rendition_named(Rendition::new(640, 360, "800k").with_name("low"))
        .build_master_variants(false)
        .unwrap();
    assert_eq!(variants[0].uri, "low/index.m3u8");
}

// ---- validation ------------------------------------------------------

#[test]
fn validate_accepts_default_ladder() {
    assert!(ladder().validate().is_ok());
}

#[test]
fn validate_rejects_empty_ladder() {
    let l = HlsLadder::new("input.mp4", "out");
    assert!(matches!(l.validate(), Err(Error::InvalidRecipeArg(_))));
}

#[test]
fn validate_rejects_odd_dimensions() {
    let l = HlsLadder::new("input.mp4", "out").rendition(641, 360, "800k");
    assert!(l.validate().is_err());
}

#[test]
fn validate_rejects_non_multiple_segment_gop() {
    // 6s segment, 4s gop -> ratio 1.5, not integer.
    let l = ladder().segment_duration(6.0).gop_seconds(4.0);
    assert!(l.validate().is_err());
    // 6s / 2s = 3 -> OK.
    assert!(ladder()
        .segment_duration(6.0)
        .gop_seconds(2.0)
        .validate()
        .is_ok());
}

#[test]
fn validate_rejects_bad_names() {
    for bad in ["../escape", "a/b", "..", "/abs"] {
        let l = HlsLadder::new("input.mp4", "out")
            .rendition_named(Rendition::new(640, 360, "800k").with_name(bad));
        assert!(l.validate().is_err(), "name '{bad}' should be rejected");
    }
}

#[test]
fn validate_rejects_duplicate_names() {
    // Both default to "360p".
    let l = HlsLadder::new("input.mp4", "out")
        .rendition(640, 360, "800k")
        .rendition(480, 360, "600k");
    assert!(l.validate().is_err());
}

#[test]
fn validate_rejects_nonpositive_fps_override() {
    assert!(ladder().fps(0, 1).validate().is_err());
    assert!(ladder().fps(30, 0).validate().is_err());
    assert!(ladder().fps(30, 1).validate().is_ok());
}

// ---- fps resolution (override path, no I/O) --------------------------

#[test]
fn resolve_fps_uses_override() {
    assert_eq!(
        ladder().fps(30000, 1001).resolve_fps().unwrap(),
        (30000, 1001)
    );
}

#[test]
fn compute_bandwidth_saturates_on_overflow() {
    // Hostile bitrates saturate to u64::MAX rather than wrapping or
    // compressing below the input, which would mis-select the variant.
    assert_eq!(compute_bandwidth(u64::MAX, Some(128_000)), u64::MAX);
}

#[test]
fn path_segment_rejects_traversal_and_control_chars() {
    assert!(validate_path_segment("../../etc", "x").is_err());
    assert!(validate_path_segment("a\nBANDWIDTH=9", "x").is_err());
    assert!(validate_path_segment("a/b", "x").is_err());
    assert!(validate_path_segment("#low", "x").is_err()); // would break the URI line
    assert!(validate_path_segment("master.m3u8", "x").is_ok());
    assert!(validate_path_segment("720p", "x").is_ok());
}

#[test]
fn validate_rejects_codecs_injection_and_master_collision() {
    assert!(ladder().codecs("avc1.640028\"\n#EXT").validate().is_err());
    // "720p" is the default dir name of the 1280x720 rendition.
    assert!(ladder().master("720p").validate().is_err());
    assert!(ladder().validate().is_ok());
}

// ---- segment type wiring (rendition_output) ---------------------------

/// Joins path components the same way `build_context` does, so the expected
/// strings stay correct on every platform's separator.
fn joined(parts: &[&str]) -> String {
    let path: PathBuf = parts.iter().collect();
    path.to_str().unwrap().to_string()
}

fn opts(pairs: &[(&str, &str)]) -> HashMap<String, String> {
    pairs
        .iter()
        .map(|(k, v)| (k.to_string(), v.to_string()))
        .collect()
}

#[test]
fn segment_type_defaults_to_mpegts() {
    assert_eq!(HlsSegmentType::default(), HlsSegmentType::MpegTs);
    assert_eq!(ladder().segment_type, HlsSegmentType::MpegTs);
}

/// Freezes the 0.13.x MPEG-TS wiring: the exact `format_opts` set (in
/// particular NO `hls_segment_type` key), the exact codec options, the `.ts`
/// segment template, and the playlist URL. Adding a segment type must never
/// disturb the default path — this is the regression lock for that contract.
#[test]
fn mpegts_rendition_output_is_frozen() {
    let l = ladder();
    let output = l.rendition_output(0, &l.renditions[0], "180", "6").unwrap();

    assert_eq!(
        output.url.as_deref(),
        Some(joined(&["out", "720p", "index.m3u8"]).as_str())
    );
    assert_eq!(output.format.as_deref(), Some("hls"));
    assert_eq!(output.video_codec.as_deref(), Some("libx264"));
    assert_eq!(output.audio_codec.as_deref(), Some("aac"));
    assert_eq!(output.pix_fmt.as_deref(), Some("yuv420p"));

    // Both stream maps in order: this rendition's scaled video pad, then the
    // optional shared first audio stream. Dropping the audio map (`0:a:0?`)
    // would silently change output content, so it is frozen here.
    let maps: Vec<(&str, bool)> = output
        .stream_map_specs
        .iter()
        .map(|spec| (spec.linklabel.as_str(), spec.copy))
        .collect();
    assert_eq!(maps, vec![("[v0]", false), ("0:a:0?", false)]);

    assert_eq!(
        output.format_opts,
        Some(opts(&[
            ("hls_time", "6"),
            ("hls_playlist_type", "vod"),
            (
                "hls_segment_filename",
                joined(&["out", "720p", "seg_%05d.ts"]).as_str()
            ),
        ]))
    );

    // `b` is where set_video_bitrate/set_audio_bitrate land.
    assert_eq!(
        output.video_codec_opts,
        Some(opts(&[
            ("b", "2800k"),
            ("g", "180"),
            ("keyint_min", "180"),
            ("sc_threshold", "0"),
            ("maxrate", "2800k"),
            ("bufsize", "5600000"),
            ("x264-params", "scenecut=0:open-gop=0"),
        ]))
    );
    assert_eq!(output.audio_codec_opts, Some(opts(&[("b", "128k")])));
}

#[test]
fn fmp4_rendition_output_wires_fmp4_options() {
    let l = ladder().segment_type(HlsSegmentType::Fmp4);
    let output = l.rendition_output(0, &l.renditions[0], "180", "6").unwrap();

    assert_eq!(
        output.format_opts,
        Some(opts(&[
            ("hls_time", "6"),
            ("hls_playlist_type", "vod"),
            (
                "hls_segment_filename",
                joined(&["out", "720p", "seg_%05d.m4s"]).as_str()
            ),
            ("hls_segment_type", "fmp4"),
            ("hls_fmp4_init_filename", "init.mp4"),
        ]))
    );
    // Segment type only touches muxer options, never the codec wiring.
    assert_eq!(output.video_codec.as_deref(), Some("libx264"));
    assert!(output
        .video_codec_opts
        .as_ref()
        .is_some_and(|codec_opts| codec_opts["g"] == "180"));
}

// ---- master playlist version -------------------------------------------

#[test]
fn master_version_follows_segment_type() {
    assert_eq!(HlsSegmentType::MpegTs.master_playlist_version(), 3);
    assert_eq!(HlsSegmentType::Fmp4.master_playlist_version(), 7);
}

/// Freezes the exact master playlist bytes a default (MPEG-TS) ladder
/// writes, matching the pre-fMP4 releases: version 3 and ascending-bandwidth
/// variants.
#[test]
fn mpegts_master_text_is_frozen() {
    let l = ladder();
    let variants = l.build_master_variants(true).unwrap();
    let text = generate_master_playlist(&variants, l.segment_type.master_playlist_version());
    assert_eq!(
        text,
        "#EXTM3U\n\
         #EXT-X-VERSION:3\n\
         #EXT-X-STREAM-INF:BANDWIDTH=1020800,RESOLUTION=640x360\n\
         360p/index.m3u8\n\
         #EXT-X-STREAM-INF:BANDWIDTH=3220800,RESOLUTION=1280x720\n\
         720p/index.m3u8\n"
    );
}

#[test]
fn fmp4_master_text_declares_version_7() {
    let l = ladder().segment_type(HlsSegmentType::Fmp4);
    let variants = l.build_master_variants(true).unwrap();
    let text = generate_master_playlist(&variants, l.segment_type.master_playlist_version());
    assert!(text.starts_with("#EXTM3U\n#EXT-X-VERSION:7\n"));
}

// Regression (Windows lane): the hls muxer locates the fMP4 init segment
// with a forward-slash-only strrchr on the playlist path, so `\`-separated
// option strings dropped init.mp4 outside the rendition directory. Paths
// handed to FFmpeg must come out of path_to_utf8 with `/` separators.
#[cfg(windows)]
#[test]
fn path_to_utf8_normalizes_separators_for_ffmpeg() {
    // Relative, drive-letter, and UNC paths all normalize to forward
    // slashes (Windows file APIs and FFmpeg accept them); verbatim paths
    // are prefix-sensitive and must pass through untouched.
    let p = std::path::Path::new(r"out\360p\index.m3u8");
    assert_eq!(path_to_utf8(p).unwrap(), "out/360p/index.m3u8");
    let p = std::path::Path::new(r"C:\hls\360p\index.m3u8");
    assert_eq!(path_to_utf8(p).unwrap(), "C:/hls/360p/index.m3u8");
    let p = std::path::Path::new(r"\\server\share\360p\index.m3u8");
    assert_eq!(path_to_utf8(p).unwrap(), "//server/share/360p/index.m3u8");
    let p = std::path::Path::new(r"\\?\C:\hls\360p\index.m3u8");
    assert_eq!(path_to_utf8(p).unwrap(), r"\\?\C:\hls\360p\index.m3u8");
}
