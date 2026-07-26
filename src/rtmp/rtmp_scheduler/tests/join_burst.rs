//! Join burst tests: packet flags, byte budget and GOP selection.

use super::super::join::{
    build_join_burst, gop_wire_size, join_replay_prefix_bytes, select_replay_start,
    CONT_HEADER_MAX, JOIN_REPLAY_BUDGET_BYTES, MSG_HEADER_MAX,
};
use super::*;
use rml_rtmp::sessions::ServerSessionResult;

// ---- H8: join burst (flags, budget trim, current-GOP inclusion) ----

#[test]
fn join_burst_flags_every_packet_like_the_live_path() {
    let mut channel = MediaChannel::new(10);
    channel.metadata = Some(Rc::new(StreamMetadata {
        video_width: None,
        video_height: None,
        video_codec_id: Some(7), // AVC
        video_frame_rate: None,
        video_bitrate_kbps: None,
        audio_codec_id: Some(10), // AAC
        audio_bitrate_kbps: None,
        audio_sample_rate: None,
        audio_channels: None,
        audio_is_stereo: None,
        encoder: None,
    }));
    channel.video_sequence_header = Some(Bytes::from_static(VIDEO_SEQ));
    channel.audio_sequence_header = Some(Bytes::from_static(AUDIO_SEQ));

    // Frozen GOP: keyframe + audio + delta; the second keyframe freezes
    // it and opens the current GOP.
    channel
        .gops
        .save_frame_data(video_frame(0, Bytes::from_static(KEYFRAME)), true);
    channel
        .gops
        .save_frame_data(audio_frame(10, Bytes::from_static(AUDIO_FRAME)), false);
    channel
        .gops
        .save_frame_data(video_frame(33, Bytes::from_static(DELTA)), false);
    channel
        .gops
        .save_frame_data(video_frame(66, Bytes::from_static(KEYFRAME)), true);

    let mut client = make_watching_client(7, "live", 1);
    let mut out = Vec::new();
    build_join_burst(&channel, &mut client, 7, 1, 0, &mut out);

    for result in &out {
        if let ServerResult::OutboundPacket {
            target_connection_id,
            ..
        } = result
        {
            assert_eq!(*target_connection_id, 7);
        }
    }
    assert_eq!(
        burst_flags(&out),
        vec![
            (false, false, false), // metadata
            (false, true, true),   // video sequence header
            (false, true, false),  // audio sequence header
            (true, false, true),   // frozen GOP keyframe
            (false, false, false), // frozen GOP audio
            (false, false, true),  // frozen GOP delta
            (true, false, true),   // current GOP keyframe
        ],
        "every replayed packet must carry the flags the live path computes"
    );
    assert!(
        client.has_received_video_keyframe,
        "replaying a flagged keyframe must open the keyframe gate"
    );
}

#[test]
fn join_burst_trims_whole_oldest_gops_to_the_byte_budget() {
    let mut channel = MediaChannel::new(10);
    channel.video_sequence_header = Some(Bytes::from_static(VIDEO_SEQ));
    channel.audio_sequence_header = Some(Bytes::from_static(AUDIO_SEQ));

    // Three ~400KiB single-keyframe GOPs (marker at data[2]) plus a small
    // current GOP. Newest-first the 960KiB budget admits current + GOP3 +
    // GOP2 (~800KiB) but not GOP1.
    let make_keyframe = |marker: u8| {
        let mut data = vec![0u8; 400 * 1024];
        data[0] = 0x17;
        data[1] = 0x01;
        data[2] = marker;
        Bytes::from(data)
    };
    for (i, marker) in [1u8, 2, 3].into_iter().enumerate() {
        channel
            .gops
            .save_frame_data(video_frame(i as u32 * 100, make_keyframe(marker)), true);
    }
    // The fourth keyframe freezes GOP3 and becomes the (small) current GOP.
    channel
        .gops
        .save_frame_data(video_frame(300, Bytes::from_static(KEYFRAME)), true);

    let mut client = make_watching_client(7, "live", 1);
    let mut out = Vec::new();
    build_join_burst(&channel, &mut client, 7, 1, 0, &mut out);

    // vseq + aseq + GOP2 + GOP3 + current — GOP1 trimmed as a whole.
    assert_eq!(out.len(), 5, "the burst must trim the oldest GOP entirely");
    assert_eq!(
        burst_flags(&out)[..2],
        [(false, true, true), (false, true, false)],
        "sequence headers are sent outside the budget trim"
    );
    assert!(
        out.iter().all(|p| !packet_contains(p, &[0x17, 0x01, 1])),
        "no fragment of the trimmed GOP1 may be replayed"
    );
    assert!(
        packet_contains(&out[2], &[0x17, 0x01, 2]),
        "the replay must start at GOP2's keyframe"
    );
    assert!(packet_contains(&out[3], &[0x17, 0x01, 3]));
    assert!(client.has_received_video_keyframe);
}

#[test]
fn join_burst_with_only_oversized_gops_sends_headers_only() {
    let mut channel = MediaChannel::new(10);
    channel.video_sequence_header = Some(Bytes::from_static(VIDEO_SEQ));
    channel.audio_sequence_header = Some(Bytes::from_static(AUDIO_SEQ));

    // One frozen GOP and a current GOP, each alone above the budget: no
    // segment fits, so the burst degrades to the sequence headers.
    let huge = {
        let mut data = vec![0u8; JOIN_REPLAY_BUDGET_BYTES + 1];
        data[0] = 0x17;
        data[1] = 0x01;
        Bytes::from(data)
    };
    channel
        .gops
        .save_frame_data(video_frame(0, huge.clone()), true);
    channel.gops.save_frame_data(video_frame(100, huge), true);

    let mut client = make_watching_client(7, "live", 1);
    let mut out = Vec::new();
    build_join_burst(&channel, &mut client, 7, 1, 0, &mut out);

    assert_eq!(
        burst_flags(&out),
        vec![(false, true, true), (false, true, false)],
        "an oversized cache must degrade to sequence headers only"
    );
    assert!(
        !client.has_received_video_keyframe,
        "no keyframe was replayed, so live deltas must stay gated until a live keyframe"
    );
}

// H8.a regression: the replay used to iterate frozen GOPs only. A joiner
// then received live deltas referencing the open GOP's keyframe it never got —
// a smeared picture until the next keyframe.
#[test]
fn join_burst_includes_the_open_current_gop_as_the_last_segment() {
    const CURRENT_KEYFRAME: &[u8] = &[0x17, 0x01, 0xB2, 0x00, 0x00];
    const CURRENT_DELTA: &[u8] = &[0x27, 0x01, 0xB3, 0x00, 0x00];

    let mut channel = MediaChannel::new(10);
    channel.video_sequence_header = Some(Bytes::from_static(VIDEO_SEQ));

    // Frozen GOP: keyframe + delta. Open GOP: a second keyframe + delta
    // not yet frozen by any later keyframe.
    channel
        .gops
        .save_frame_data(video_frame(0, Bytes::from_static(KEYFRAME)), true);
    channel
        .gops
        .save_frame_data(video_frame(33, Bytes::from_static(DELTA)), false);
    channel
        .gops
        .save_frame_data(video_frame(66, Bytes::from_static(CURRENT_KEYFRAME)), true);
    channel
        .gops
        .save_frame_data(video_frame(99, Bytes::from_static(CURRENT_DELTA)), false);

    let mut client = make_watching_client(7, "live", 1);
    let mut out = Vec::new();
    build_join_burst(&channel, &mut client, 7, 1, 0, &mut out);

    // vseq + frozen (keyframe, delta) + current (keyframe, delta).
    assert_eq!(
        out.len(),
        5,
        "the open GOP must be replayed after the frozen ones"
    );
    assert!(
        packet_contains(&out[3], CURRENT_KEYFRAME),
        "the open GOP's keyframe must be replayed — live deltas reference it"
    );
    assert!(packet_contains(&out[4], CURRENT_DELTA));
    assert_eq!(
        burst_flags(&out)[3..],
        [(true, false, true), (false, false, true)]
    );
}

#[test]
fn keyframeless_current_gop_replays_audio_only_and_keeps_the_gate_closed() {
    let mut channel = MediaChannel::new(10);
    channel.video_sequence_header = Some(Bytes::from_static(VIDEO_SEQ));
    channel.audio_sequence_header = Some(Bytes::from_static(AUDIO_SEQ));

    // Publish started mid-GOP: the open GOP holds deltas and audio, no
    // keyframe, and nothing is frozen yet.
    channel
        .gops
        .save_frame_data(video_frame(0, Bytes::from_static(DELTA)), false);
    channel
        .gops
        .save_frame_data(audio_frame(10, Bytes::from_static(AUDIO_FRAME)), false);
    channel
        .gops
        .save_frame_data(video_frame(33, Bytes::from_static(DELTA)), false);
    channel
        .gops
        .save_frame_data(audio_frame(43, Bytes::from_static(AUDIO_FRAME)), false);

    let mut client = make_watching_client(7, "live", 1);
    let mut out = Vec::new();
    build_join_burst(&channel, &mut client, 7, 1, 0, &mut out);

    assert_eq!(
        burst_flags(&out),
        vec![
            (false, true, true),   // video sequence header
            (false, true, false),  // audio sequence header
            (false, false, false), // audio
            (false, false, false), // audio
        ],
        "undecodable pre-keyframe deltas must be skipped while audio still flows"
    );
    assert!(
        !client.has_received_video_keyframe,
        "a keyframeless replay must not open the gate"
    );
}

#[test]
fn select_replay_start_picks_the_longest_fitting_suffix() {
    let cases: &[(&[usize], usize, usize)] = &[
        (&[], 100, 0),    // nothing cached
        (&[10], 100, 0),  // everything fits
        (&[100], 100, 0), // exactly the budget fits (<=)
        (&[101], 100, 1), // a single oversized segment -> none
        (&[100, 200, 300], 600, 0),
        (&[100, 200, 300], 599, 1),
        (&[100, 200, 300], 500, 1),
        (&[100, 200, 300], 499, 2),
        (&[100, 200, 300], 300, 2),
        (&[100, 200, 300], 299, 3),
        (&[0, 0, 0], 0, 0), // zero-size segments always fit
        // Overflowing older segments must fail closed — keep the newest
        // fitting suffix, never wrap around and admit everything.
        (&[usize::MAX, 100], usize::MAX, 1),
        (&[usize::MAX - 50, 100], usize::MAX, 1),
    ];
    for &(sizes, budget, expected) in cases {
        assert_eq!(
            select_replay_start(sizes, budget),
            expected,
            "sizes={sizes:?} budget={budget}"
        );
    }
}

// ---- F1: the replay budget must count real wire bytes, not raw payload ----

/// gop_wire_size adds the RTMP chunk framing on top of the raw payload.
#[test]
fn gop_wire_size_adds_per_frame_and_continuation_framing() {
    // Empty GOP: no payload, no frames -> no framing.
    assert_eq!(gop_wire_size(0, 0), 0);
    // One 100-byte frame -> payload + one type-0 header + one chunk's cont.
    assert_eq!(
        gop_wire_size(100, 1),
        100 + MSG_HEADER_MAX + CONT_HEADER_MAX
    );
    // Many small frames: the per-frame header dominates the payload.
    // 1000 bytes spans a single 4096-byte chunk (one continuation header).
    assert_eq!(
        gop_wire_size(1000, 100),
        1000 + 100 * MSG_HEADER_MAX + CONT_HEADER_MAX
    );
    // A payload spanning several chunks accrues one cont header per chunk.
    let payload = OUTBOUND_CHUNK_SIZE * 3 + 1;
    assert_eq!(
        gop_wire_size(payload, 1),
        payload + MSG_HEADER_MAX + 4 * CONT_HEADER_MAX
    );
}

/// A metadata packet near the 64 KiB headroom must reduce the GOP budget so
/// a GOP that fits the raw-payload budget is trimmed, keeping the real
/// serialized burst under the Warning threshold.
#[test]
fn join_burst_oversized_metadata_trims_gops_below_the_warning_threshold() {
    const GOP_MARKER: &[u8] = &[0x17, 0x01, 0xC1];
    let warn = crate::rtmp::write_queue::QUEUE_WARN_BYTES;

    let mut channel = MediaChannel::new(10);
    // A large-but-legal encoder string (AMF0 UTF8 tops out at 65535 bytes).
    channel.metadata = Some(Rc::new(StreamMetadata {
        video_width: None,
        video_height: None,
        video_codec_id: Some(7),
        video_frame_rate: None,
        video_bitrate_kbps: None,
        audio_codec_id: None,
        audio_bitrate_kbps: None,
        audio_sample_rate: None,
        audio_channels: None,
        audio_is_stereo: None,
        encoder: Some("x".repeat(60_000)),
    }));
    channel.video_sequence_header = Some(Bytes::from_static(VIDEO_SEQ));
    channel.audio_sequence_header = Some(Bytes::from_static(AUDIO_SEQ));

    // A single ~950 KiB one-keyframe GOP: it fits the raw 960 KiB budget, but not
    // once ~60 KiB of metadata is subtracted.
    let mut keyframe = vec![0u8; 950 * 1024];
    keyframe[0] = 0x17;
    keyframe[1] = 0x01;
    keyframe[2] = 0xC1;
    channel
        .gops
        .save_frame_data(video_frame(0, Bytes::from(keyframe)), true);

    let mut client = make_watching_client(7, "live", 1);
    let mut out = Vec::new();
    build_join_burst(&channel, &mut client, 7, 1, 0, &mut out);

    assert!(
        !out.iter().any(|p| packet_contains(p, GOP_MARKER)),
        "the oversized-metadata prefix must trim the GOP that the raw budget would have kept"
    );
    assert!(
        serialized_burst_len(&out) <= warn,
        "the real serialized burst must stay within the Warning threshold ({} <= {})",
        serialized_burst_len(&out),
        warn
    );
}

/// A high-frame-count GOP whose payload alone fits must still be trimmed
/// once per-frame chunk framing is counted.
#[test]
fn join_burst_many_small_frames_framing_trims_older_gops() {
    const OLD_KEYFRAME: &[u8] = &[0x17, 0x01, 0xD1];
    const NEW_KEYFRAME: &[u8] = &[0x17, 0x01, 0xD2];
    let warn = crate::rtmp::write_queue::QUEUE_WARN_BYTES;

    let mut channel = MediaChannel::new(10);
    channel.video_sequence_header = Some(Bytes::from_static(VIDEO_SEQ));
    channel.audio_sequence_header = Some(Bytes::from_static(AUDIO_SEQ));

    // Two GOPs of 3000 x 150-byte frames each. Payload sum (~880 KiB) fits
    // the 960 KiB budget, but the per-frame framing (3000 x 18 bytes/GOP)
    // pushes the pair over it, so the older GOP is trimmed as a whole.
    let keyframe = |marker: u8| {
        let mut d = vec![0u8; 150];
        d[0] = 0x17;
        d[1] = 0x01;
        d[2] = marker;
        Bytes::from(d)
    };
    let delta = || {
        let mut d = vec![0u8; 150];
        d[0] = 0x27;
        d[1] = 0x01;
        Bytes::from(d)
    };
    channel
        .gops
        .save_frame_data(video_frame(0, keyframe(0xD1)), true);
    for i in 0..2999u32 {
        channel
            .gops
            .save_frame_data(video_frame(i + 1, delta()), false);
    }
    // The second keyframe freezes the first GOP and opens the second.
    channel
        .gops
        .save_frame_data(video_frame(3000, keyframe(0xD2)), true);
    for i in 0..2999u32 {
        channel
            .gops
            .save_frame_data(video_frame(3001 + i, delta()), false);
    }

    let mut client = make_watching_client(7, "live", 1);
    let mut out = Vec::new();
    build_join_burst(&channel, &mut client, 7, 1, 0, &mut out);

    assert!(
        out.iter().any(|p| packet_contains(p, NEW_KEYFRAME)),
        "the newest GOP must be replayed"
    );
    assert!(
        !out.iter().any(|p| packet_contains(p, OLD_KEYFRAME)),
        "framing must trim the older GOP even though its payload alone fits"
    );
    assert!(
        serialized_burst_len(&out) <= warn,
        "the real serialized burst must stay within the Warning threshold"
    );
}

/// An oversized sequence header exhausts the budget by itself: the prefix
/// subtraction saturates to zero and no delta GOP is replayed — no panic,
/// no underflow.
#[test]
fn join_burst_oversized_sequence_header_replays_zero_gops() {
    const GOP_MARKER: &[u8] = &[0x17, 0x01, 0xE1];

    let mut channel = MediaChannel::new(10);
    // A 2 MiB sequence header (adversarial but legal), far over the budget.
    let mut header = vec![0u8; 2 * 1024 * 1024];
    header[0] = 0x17;
    header[1] = 0x00;
    channel.video_sequence_header = Some(Bytes::from(header));

    let mut keyframe = vec![0u8; 4096];
    keyframe[0] = 0x17;
    keyframe[1] = 0x01;
    keyframe[2] = 0xE1;
    channel
        .gops
        .save_frame_data(video_frame(0, Bytes::from(keyframe)), true);

    let mut client = make_watching_client(7, "live", 1);
    let mut out = Vec::new();
    // Must not panic on the budget subtraction (saturating to zero).
    build_join_burst(&channel, &mut client, 7, 1, 0, &mut out);

    assert!(
        !out.iter().any(|p| packet_contains(p, GOP_MARKER)),
        "a prefix that exhausts the budget must replay zero GOPs"
    );
    assert!(
        !client.has_received_video_keyframe,
        "no keyframe was replayed, so the keyframe gate must stay closed"
    );
}

/// F1: the play-accept control packets enqueued ahead of the burst count
/// against the GOP budget. A ~64 KiB accept prefix (an oversized stream key
/// echoed by NetStream.Play.Start) must trim a GOP the raw budget would
/// keep, so the real enqueued bytes (accept prefix + burst) stay under the
/// Warning threshold and no delta frame is dropped by backpressure.
#[test]
fn join_burst_counts_the_accept_prefix_against_the_gop_budget() {
    const GOP_MARKER: &[u8] = &[0x17, 0x01, 0xF1];
    let warn = crate::rtmp::write_queue::QUEUE_WARN_BYTES;

    // A single ~950 KiB one-keyframe GOP: it fits the raw 960 KiB budget on its own.
    let make_channel = || {
        let mut channel = MediaChannel::new(10);
        channel.video_sequence_header = Some(Bytes::from_static(VIDEO_SEQ));
        let mut keyframe = vec![0u8; 950 * 1024];
        keyframe[0] = 0x17;
        keyframe[1] = 0x01;
        keyframe[2] = 0xF1;
        channel
            .gops
            .save_frame_data(video_frame(0, Bytes::from(keyframe)), true);
        channel
    };

    // Sanity: with no accept prefix the GOP fits and is replayed.
    let channel = make_channel();
    let mut client = make_watching_client(7, "live", 1);
    let mut out = Vec::new();
    build_join_burst(&channel, &mut client, 7, 1, 0, &mut out);
    assert!(
        out.iter().any(|p| packet_contains(p, GOP_MARKER)),
        "the GOP fits the budget when no accept prefix is charged"
    );

    // With a ~64 KiB accept prefix the same GOP no longer fits and is
    // trimmed as a whole, degrading the burst to the sequence header.
    let channel = make_channel();
    let accept_prefix = 64 * 1024;
    let mut client = make_watching_client(7, "live", 1);
    let mut out = Vec::new();
    build_join_burst(&channel, &mut client, 7, 1, accept_prefix, &mut out);
    assert!(
        !out.iter().any(|p| packet_contains(p, GOP_MARKER)),
        "the accept prefix must trim the GOP the raw budget would have kept"
    );
    assert!(
        accept_prefix + serialized_burst_len(&out) <= warn,
        "accept prefix + burst must stay within the Warning threshold ({} + {} <= {})",
        accept_prefix,
        serialized_burst_len(&out),
        warn
    );
}

// the join-replay budget subtracts backlog + same-batch
// prefix + accept packets, every add saturating. The same-batch prefix is now
// a pre-accumulated scalar (see the incremental scan test below).
#[test]
fn join_replay_prefix_sums_backlog_prefix_and_accept_bytes() {
    let accept = vec![ServerSessionResult::OutboundResponse(Packet {
        bytes: vec![0u8; 40],
        can_be_dropped: false,
    })];
    // backlog 500 + same-batch prefix 350 + accept 40.
    assert_eq!(join_replay_prefix_bytes(500, 350, &accept), 500 + 350 + 40);
    // No accept packets → backlog + prefix only.
    assert_eq!(join_replay_prefix_bytes(0, 350, &[]), 350);
    // Nothing queued ahead → zero.
    assert_eq!(join_replay_prefix_bytes(0, 0, &[]), 0);
    // Every add saturates: a pathological backlog cannot wrap.
    assert_eq!(
        join_replay_prefix_bytes(usize::MAX, 350, &accept),
        usize::MAX
    );
}

// repeated plays in one batch must fold the same-batch prefix
// INCREMENTALLY (each server_results entry visited once) rather than rescan
// the growing vec per play (quadratic — a reachable reactor stall).
// advance_serving_prefix advances a cursor and a running total.
#[test]
fn serving_prefix_scan_is_incremental_and_targeted() {
    let mut scheduler = RtmpScheduler::new(10);
    let target = 7usize;
    let other = 9usize;
    let outbound = |conn: usize, n: usize| ServerResult::OutboundPacket {
        target_connection_id: conn,
        bytes: Bytes::from(vec![0u8; n]),
        can_be_dropped: false,
        is_keyframe: false,
        is_sequence_header: false,
        is_video: false,
    };
    // First play folds the createStream prefix — only target packets count
    // (100 + 50); the other watcher's 999 is ignored.
    let mut results = vec![
        outbound(target, 100),
        outbound(other, 999),
        outbound(target, 50),
    ];
    assert_eq!(scheduler.advance_serving_prefix(&results, target), 150);
    assert_eq!(
        scheduler.serving_prefix_scan_pos, 3,
        "all three entries consumed exactly once"
    );
    // The play appended its accept+burst to the target; the NEXT play sees it
    // without rescanning the earlier entries.
    results.push(outbound(target, 200));
    assert_eq!(scheduler.advance_serving_prefix(&results, target), 350);
    assert_eq!(
        scheduler.serving_prefix_scan_pos, 4,
        "only the newly-appended entry is consumed"
    );
    // No new entries → unchanged, cursor stable (idempotent).
    assert_eq!(scheduler.advance_serving_prefix(&results, target), 350);
    assert_eq!(scheduler.serving_prefix_scan_pos, 4);
}
