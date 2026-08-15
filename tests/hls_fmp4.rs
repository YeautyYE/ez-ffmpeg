//! fMP4 HLS ladder integration coverage: structure-level assertions (init
//! segment on disk, `.m4s` media segments, `EXT-X-MAP` in the media
//! playlists, master `EXT-X-VERSION`), plus a decode-back pass through
//! FFmpeg's own hls demuxer.
//!
//! Every scenario encodes with the native `mpeg4` encoder so the suite runs
//! on FFmpeg builds without libx264 (a default, external-lib-free FFmpeg
//! configure has no libx264). These tests assert container structure and a
//! decode round-trip only; they do not exercise browser/player playback
//! (hls.js, Safari).

mod common;

use std::path::{Path, PathBuf};

use common::tmp_path_in;
use ez_ffmpeg::error::{Error, OpenOutputError};
use ez_ffmpeg::recipes::{HlsLadder, HlsSegmentType};
use ez_ffmpeg::stream_info::{find_video_stream_info, StreamInfo};
use ez_ffmpeg::{FfmpegContext, Output};

const SUBDIR: &str = "ez_ffmpeg_hls_fmp4";

/// Repo fixture: h264+aac, 320x240, 10 fps CFR, 5 s — probe-able frame rate
/// and a real audio stream, so the ladder's `0:a:0?` map and the master's
/// audio bandwidth path are both exercised.
fn fixture() -> String {
    let path = Path::new(env!("CARGO_MANIFEST_DIR")).join("test.mp4");
    assert!(path.exists(), "missing repo fixture test.mp4");
    path.to_string_lossy().into_owned()
}

/// Two-rendition ladder over the fixture with 1 s segments (10-frame GOP at
/// the fixture's 10 fps), encoded by the always-available `mpeg4` encoder.
fn ladder_into(out_dir: &str) -> HlsLadder {
    HlsLadder::new(fixture(), out_dir)
        .rendition(320, 240, "300k")
        .rendition(160, 120, "150k")
        .segment_duration(1.0)
        .video_codec("mpeg4")
}

/// Runs `ladder` on a watchdog thread: a hang is a test failure naming
/// `scenario`, not a suite timeout.
fn run_ladder(ladder: HlsLadder, secs: u64, scenario: &str) -> ez_ffmpeg::error::Result<()> {
    let (tx, rx) = std::sync::mpsc::channel();
    std::thread::spawn(move || {
        let _ = tx.send(ladder.run());
    });
    match rx.recv_timeout(std::time::Duration::from_secs(secs)) {
        Ok(result) => result,
        Err(std::sync::mpsc::RecvTimeoutError::Timeout) => {
            panic!("scenario `{scenario}` did not finish within {secs}s (hang)")
        }
        Err(std::sync::mpsc::RecvTimeoutError::Disconnected) => {
            panic!("scenario `{scenario}`: run() thread panicked before reporting")
        }
    }
}

/// Files in `dir` (non-recursive) whose extension is `ext`.
fn files_with_ext(dir: &Path, ext: &str) -> Vec<PathBuf> {
    let mut files: Vec<PathBuf> = std::fs::read_dir(dir)
        .unwrap_or_else(|e| panic!("cannot read {}: {e}", dir.display()))
        .map(|entry| entry.unwrap().path())
        .filter(|p| p.extension().and_then(|e| e.to_str()) == Some(ext))
        .collect();
    files.sort();
    files
}

/// Media URIs (non-tag, non-empty lines) of an m3u8 playlist.
fn media_uris(playlist: &str) -> Vec<&str> {
    playlist
        .lines()
        .map(str::trim)
        .filter(|line| !line.is_empty() && !line.starts_with('#'))
        .collect()
}

#[test]
fn fmp4_ladder_writes_init_segments_m4s_and_version_7_master() {
    let out_dir = tmp_path_in(SUBDIR, "fmp4_out");
    let _ = std::fs::remove_dir_all(&out_dir);

    run_ladder(
        ladder_into(&out_dir).segment_type(HlsSegmentType::Fmp4),
        120,
        "fmp4 ladder",
    )
    .expect("fMP4 ladder must run to completion");

    let master = std::fs::read_to_string(Path::new(&out_dir).join("master.m3u8"))
        .expect("master.m3u8 must be written");
    assert!(
        master.contains("#EXT-X-VERSION:7"),
        "fMP4 master must declare version 7:\n{master}"
    );
    assert!(master.contains("240p/index.m3u8") && master.contains("120p/index.m3u8"));

    for name in ["240p", "120p"] {
        let dir = Path::new(&out_dir).join(name);

        let init = dir.join("init.mp4");
        assert!(init.is_file(), "{name}: init.mp4 must exist");
        assert!(
            std::fs::metadata(&init).unwrap().len() > 0,
            "{name}: init.mp4 must be non-empty"
        );

        let m4s = files_with_ext(&dir, "m4s");
        assert!(
            m4s.len() >= 2,
            "{name}: expected multiple .m4s media segments, got {m4s:?}"
        );
        let ts = files_with_ext(&dir, "ts");
        assert!(
            ts.is_empty(),
            "{name}: fMP4 ladder must not write .ts segments: {ts:?}"
        );

        let playlist = std::fs::read_to_string(dir.join("index.m3u8")).unwrap();
        assert!(
            playlist.contains("#EXT-X-MAP:URI=\"init.mp4\""),
            "{name}: media playlist must announce the init segment:\n{playlist}"
        );
        assert!(
            playlist.contains("#EXT-X-ENDLIST"),
            "{name}: VOD playlist must be closed"
        );

        // Playlist and directory agree exactly: the set of URI basenames
        // equals the set of .m4s files on disk (no missing, extra, or
        // duplicate references).
        let uris = media_uris(&playlist);
        assert!(uris.iter().all(|uri| uri.ends_with(".m4s")));
        let uri_set: std::collections::BTreeSet<&str> = uris.iter().copied().collect();
        assert_eq!(uri_set.len(), uris.len(), "{name}: duplicate segment URI");
        let file_set: std::collections::BTreeSet<String> = m4s
            .iter()
            .map(|p| p.file_name().unwrap().to_str().unwrap().to_string())
            .collect();
        let uri_names: std::collections::BTreeSet<String> =
            uri_set.iter().map(|s| s.to_string()).collect();
        assert_eq!(
            uri_names, file_set,
            "{name}: playlist URIs {uri_names:?} do not match .m4s files {file_set:?}"
        );
    }
}

/// The produced rendition must be consumable by FFmpeg's own hls demuxer:
/// probe the media playlist (init segment + first segments must parse) and
/// then decode the whole rendition to the `null` muxer (end-to-end proof the
/// segments decode, not just parse).
#[test]
fn fmp4_rendition_probes_and_decodes_back() {
    let out_dir = tmp_path_in(SUBDIR, "fmp4_decode_out");
    let _ = std::fs::remove_dir_all(&out_dir);

    run_ladder(
        ladder_into(&out_dir).segment_type(HlsSegmentType::Fmp4),
        120,
        "fmp4 decode-back ladder",
    )
    .expect("fMP4 ladder must run to completion");

    let playlist = Path::new(&out_dir).join("240p").join("index.m3u8");
    let playlist = playlist.to_str().unwrap();

    let info = find_video_stream_info(playlist)
        .expect("probing the fMP4 rendition playlist must succeed")
        .expect("rendition playlist must expose a video stream");
    match info {
        StreamInfo::Video { width, height, .. } => {
            assert_eq!((width, height), (320, 240), "probed rendition dimensions");
        }
        other => panic!("expected a video stream, got {other:?}"),
    }

    // Decode back through the hls demuxer into the null muxer (AVFMT_NOFILE:
    // nothing is written). A truncated or mis-indexed segment fails here.
    let ctx = FfmpegContext::builder()
        .input(playlist)
        .output(Output::from("decoded.null").set_format("null"))
        .build()
        .expect("decode-back context must build");
    let (tx, rx) = std::sync::mpsc::channel();
    std::thread::spawn(move || {
        let _ = tx.send(ctx.start().and_then(|scheduler| scheduler.wait()));
    });
    match rx.recv_timeout(std::time::Duration::from_secs(120)) {
        Ok(result) => result.expect("decoding the fMP4 rendition back must succeed"),
        Err(_) => panic!("decode-back of the fMP4 rendition hung"),
    }
}

/// Regression lock for the default path: an untouched (MPEG-TS) ladder keeps
/// the 0.13.x on-disk layout — `.ts` segments, no init segment, no
/// `EXT-X-MAP`, master version 3.
#[test]
fn mpegts_default_ladder_layout_is_unchanged() {
    let out_dir = tmp_path_in(SUBDIR, "ts_out");
    let _ = std::fs::remove_dir_all(&out_dir);

    run_ladder(ladder_into(&out_dir), 120, "ts ladder").expect("TS ladder must run");

    let master = std::fs::read_to_string(Path::new(&out_dir).join("master.m3u8")).unwrap();
    assert!(
        master.contains("#EXT-X-VERSION:3"),
        "default master must stay version 3:\n{master}"
    );
    assert!(!master.contains("#EXT-X-VERSION:7"));

    for name in ["240p", "120p"] {
        let dir = Path::new(&out_dir).join(name);

        let ts = files_with_ext(&dir, "ts");
        assert!(
            ts.len() >= 2,
            "{name}: expected multiple .ts segments, got {ts:?}"
        );
        assert!(
            files_with_ext(&dir, "m4s").is_empty(),
            "{name}: TS ladder must not write .m4s segments"
        );
        assert!(
            !dir.join("init.mp4").exists(),
            "{name}: TS ladder must not write an init segment"
        );

        let playlist = std::fs::read_to_string(dir.join("index.m3u8")).unwrap();
        assert!(
            !playlist.contains("EXT-X-MAP"),
            "{name}: TS playlist must not carry EXT-X-MAP:\n{playlist}"
        );
        assert!(playlist.contains("#EXT-X-ENDLIST"));
        assert!(media_uris(&playlist).iter().all(|uri| uri.ends_with(".ts")));
    }
}

// ---------------------------------------------------------------------------
// OpenH264 segment-alignment evidence (skip-guarded).
// ---------------------------------------------------------------------------

/// Payload bytes of one 188-byte MPEG-TS packet (adaptation field skipped),
/// plus its PID and payload_unit_start_indicator.
fn ts_packet_payload(packet: &[u8]) -> Option<(u16, bool, &[u8])> {
    assert_eq!(packet[0], 0x47, "lost MPEG-TS sync");
    let pid = (u16::from(packet[1] & 0x1F) << 8) | u16::from(packet[2]);
    let pusi = packet[1] & 0x40 != 0;
    let afc = (packet[3] >> 4) & 0x3;
    if afc & 0x1 == 0 {
        return None; // no payload
    }
    let start = if afc & 0x2 != 0 {
        4 + 1 + usize::from(packet[4])
    } else {
        4
    };
    (start <= packet.len()).then(|| (pid, pusi, &packet[start..]))
}

/// Extracts the H.264 elementary stream of a single-program MPEG-TS segment:
/// PAT (PID 0) names the PMT, the PMT names the stream_type 0x1B (H.264)
/// PID, and that PID's payloads are concatenated with each PES header
/// stripped, so the result scans as a plain Annex B byte stream.
fn ts_h264_elementary_stream(segment: &[u8]) -> Vec<u8> {
    assert_eq!(segment.len() % 188, 0, "segment is not packet-aligned");
    let packets: Vec<&[u8]> = segment.chunks_exact(188).collect();

    let find_table = |wanted_pid: u16| -> Vec<u8> {
        for packet in &packets {
            if let Some((pid, pusi, payload)) = ts_packet_payload(packet) {
                if pid == wanted_pid && pusi {
                    // pointer_field precedes the section on a unit start.
                    let skip = 1 + usize::from(payload[0]);
                    return payload[skip..].to_vec();
                }
            }
        }
        panic!("no table found on PID {wanted_pid}");
    };

    // PAT: first program loop entry with a non-zero program number.
    let pat = find_table(0);
    let section_len = usize::from(u16::from(pat[1] & 0x0F) << 8 | u16::from(pat[2]));
    let mut pmt_pid = None;
    let mut i = 8; // table header (3) + fixed section fields (5)
    while i + 4 <= 3 + section_len - 4 {
        let program = u16::from(pat[i]) << 8 | u16::from(pat[i + 1]);
        let pid = (u16::from(pat[i + 2] & 0x1F) << 8) | u16::from(pat[i + 3]);
        if program != 0 {
            pmt_pid = Some(pid);
            break;
        }
        i += 4;
    }
    let pmt = find_table(pmt_pid.expect("PAT names no program"));

    // PMT: elementary-stream loop, looking for stream_type 0x1B (H.264).
    let section_len = usize::from(u16::from(pmt[1] & 0x0F) << 8 | u16::from(pmt[2]));
    let program_info_len = usize::from(u16::from(pmt[10] & 0x0F) << 8 | u16::from(pmt[11]));
    let mut video_pid = None;
    let mut i = 12 + program_info_len;
    while i + 5 <= 3 + section_len - 4 {
        let stream_type = pmt[i];
        let pid = (u16::from(pmt[i + 1] & 0x1F) << 8) | u16::from(pmt[i + 2]);
        let es_info_len = usize::from(u16::from(pmt[i + 3] & 0x0F) << 8 | u16::from(pmt[i + 4]));
        if stream_type == 0x1B {
            video_pid = Some(pid);
            break;
        }
        i += 5 + es_info_len;
    }
    let video_pid = video_pid.expect("PMT carries no H.264 (stream_type 0x1B) stream");

    let mut es = Vec::new();
    for packet in &packets {
        if let Some((pid, pusi, payload)) = ts_packet_payload(packet) {
            if pid != video_pid {
                continue;
            }
            if pusi && payload.len() > 9 && payload[..3] == [0, 0, 1] {
                // PES header: 6 fixed bytes, 3 optional-header bytes, then
                // PES_header_data_length bytes of stuffing/PTS.
                let header_len = 9 + usize::from(payload[8]);
                es.extend_from_slice(&payload[header_len.min(payload.len())..]);
            } else {
                es.extend_from_slice(payload);
            }
        }
    }
    es
}

/// nal_unit_type of the first VCL NAL (slice type 1 or 5) in an Annex B
/// stream: 5 means the stream starts on an IDR.
fn first_vcl_nal_type(es: &[u8]) -> Option<u8> {
    let mut i = 0;
    while i + 3 < es.len() {
        if es[i] == 0 && es[i + 1] == 0 && es[i + 2] == 1 {
            let nal_type = es[i + 3] & 0x1F;
            if nal_type == 1 || nal_type == 5 {
                return Some(nal_type);
            }
            i += 3;
        } else {
            i += 1;
        }
    }
    None
}

/// Runtime segment-alignment evidence for the openh264 prescription: on an
/// FFmpeg build that carries `libopenh264` (the LGPL-compatible H.264
/// encoder in the recipe's auto-admitted set), a ladder pinned to it must
/// produce playlists whose every media segment begins with an IDR — the
/// periodic forced keyframes are what align segment boundaries, because
/// openh264 ignores the generic `g`/`keyint_min` options. Skips (with a
/// note) when the encoder is not in the build; the option-set half of the
/// evidence runs unconditionally in `src/core/recipes/hls/tests.rs`.
#[test]
fn openh264_ladder_segments_start_on_idr() {
    if !ez_ffmpeg::capabilities::is_encoder_available("libopenh264") {
        eprintln!("skipping: libopenh264 not available in this FFmpeg build");
        return;
    }
    let out_dir = tmp_path_in(SUBDIR, "openh264_out");
    let _ = std::fs::remove_dir_all(&out_dir);

    // One tiny rendition over the 5s/10fps fixture with 1s segments keeps
    // the encode short while still producing several segment boundaries.
    run_ladder(
        HlsLadder::new(fixture(), &out_dir)
            .rendition(160, 120, "150k")
            .segment_duration(1.0)
            .video_codec("libopenh264"),
        120,
        "openh264 ladder",
    )
    .expect("openh264 ladder must run to completion");

    assert!(
        Path::new(&out_dir).join("master.m3u8").is_file(),
        "master playlist must be written"
    );
    let dir = Path::new(&out_dir).join("120p");
    let playlist = std::fs::read_to_string(dir.join("index.m3u8"))
        .expect("rendition playlist must be written");
    assert!(playlist.contains("#EXT-X-ENDLIST"));
    let uris = media_uris(&playlist);
    assert!(
        uris.len() >= 2,
        "1s segments over the 5s fixture must yield multiple segments, got {uris:?}"
    );

    for (index, uri) in uris.iter().enumerate() {
        let segment = std::fs::read(dir.join(uri))
            .unwrap_or_else(|e| panic!("segment {uri} unreadable: {e}"));
        let es = ts_h264_elementary_stream(&segment);
        assert_eq!(
            first_vcl_nal_type(&es),
            Some(5),
            "segment {index} ({uri}) must start with an IDR slice"
        );
    }
}

#[test]
fn scratch_validate_ts_helpers_with_videotoolbox() {
    if !ez_ffmpeg::capabilities::is_encoder_available("h264_videotoolbox") {
        eprintln!("skipping: h264_videotoolbox not available");
        return;
    }
    let out_dir = tmp_path_in(SUBDIR, "vt_scratch_out");
    let _ = std::fs::remove_dir_all(&out_dir);
    run_ladder(
        HlsLadder::new(fixture(), &out_dir)
            .rendition(160, 120, "150k")
            .segment_duration(1.0)
            .video_codec("h264_videotoolbox"),
        120,
        "vt scratch ladder",
    )
    .expect("vt ladder must run");
    let dir = Path::new(&out_dir).join("120p");
    let playlist = std::fs::read_to_string(dir.join("index.m3u8")).unwrap();
    let uris = media_uris(&playlist);
    assert!(uris.len() >= 2, "got {uris:?}");
    for (index, uri) in uris.iter().enumerate() {
        let segment = std::fs::read(dir.join(uri)).unwrap();
        let es = ts_h264_elementary_stream(&segment);
        assert_eq!(
            first_vcl_nal_type(&es),
            Some(5),
            "segment {index} ({uri}) must start with an IDR slice"
        );
    }
}

/// Fail-on-revert lock for the create-directories-after-build ordering: a
/// ladder whose video encoder is unavailable must fail `build_context()`
/// (with a `NAME`d error, exercising the diagnostics change too) and must not
/// leave a half-created output directory tree behind.
#[test]
fn failed_build_leaves_no_directory_debris() {
    let out_dir = tmp_path_in(SUBDIR, "no_debris_out");
    let _ = std::fs::remove_dir_all(&out_dir);

    let result = HlsLadder::new(fixture(), &out_dir)
        .rendition(320, 240, "300k")
        .segment_duration(1.0)
        .fps(10, 1)
        .video_codec("definitely_not_a_real_encoder")
        .build_context();

    match result {
        Err(Error::OpenOutput(OpenOutputError::EncoderUnavailable { name })) => {
            assert_eq!(name, "definitely_not_a_real_encoder");
        }
        Err(other) => panic!("expected EncoderUnavailable naming the codec, got {other:?}"),
        Ok(_) => panic!("build_context must fail for an unavailable encoder"),
    }
    assert!(
        !Path::new(&out_dir).exists(),
        "a failed build_context() must not create the output directory tree"
    );
}
