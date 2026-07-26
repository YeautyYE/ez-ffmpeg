//! Publisher abort, oversized-sequence-header ingest bound and
//! publisher-end channel-reset tests.

use super::super::join::build_join_burst;
use super::*;

// a publisher torn down by a fatal ingest error (an oversized sequence
// header rejected on the untrusted byte path) must still finalize its watchers,
// or they are orphaned in Watching with a stale keyframe gate when a new
// publisher reclaims the key. `abort_publisher_watchers` ends each watcher
// (Disconnect) WITHOUT re-feeding the just-errored publisher session.
#[test]
fn abort_publisher_watchers_disconnects_every_watcher() {
    let mut scheduler = RtmpScheduler::new(10);
    let stream_key = "live".to_string();
    let publisher_connection_id = 1;
    let watcher_connection_id = 2;

    assert!(scheduler.new_channel(stream_key.clone(), publisher_connection_id));
    // Register the watcher and move it into Watching on the channel (the accept
    // fails on the fresh synthetic session, but Watching + membership are set
    // before that early return — the exact orphan state F2 must finalize).
    let _ = scheduler.bytes_received(watcher_connection_id, &[]);
    let mut sink = Vec::new();
    scheduler.handle_play_requested(
        watcher_connection_id,
        1,
        "app".to_string(),
        stream_key.clone(),
        1,
        &mut sink,
    );
    assert_eq!(
        scheduler
            .channels
            .get(&stream_key)
            .unwrap()
            .watching_client_ids
            .len(),
        1,
        "watcher must be registered before the abort"
    );

    let results = scheduler.abort_publisher_watchers(publisher_connection_id);
    assert!(
        results.iter().any(|r| matches!(
            r,
            ServerResult::DisconnectConnection { connection_id }
                if *connection_id == watcher_connection_id
        )),
        "the aborted publisher's watcher must receive a Disconnect, not be orphaned"
    );
}

// ---- F2: bound the cacheable sequence-header size at ingest ----

/// The size-gate predicate: only a sequence header (not a large ordinary
/// keyframe) over the cap is flagged, for both audio and video.
#[test]
fn oversized_sequence_header_size_gate_predicate() {
    let over = |first: u8, second: u8| {
        let mut d = vec![0u8; MAX_CACHEABLE_SEQUENCE_HEADER_BYTES + 1];
        d[0] = first;
        d[1] = second;
        Bytes::from(d)
    };

    // Normal small headers: never flagged.
    assert!(!is_oversized_sequence_header(
        &Bytes::from_static(VIDEO_SEQ),
        ReceivedDataType::Video
    ));
    assert!(!is_oversized_sequence_header(
        &Bytes::from_static(AUDIO_SEQ),
        ReceivedDataType::Audio
    ));
    // Over-cap sequence headers: flagged for both media types.
    assert!(is_oversized_sequence_header(
        &over(0x17, 0x00),
        ReceivedDataType::Video
    ));
    assert!(is_oversized_sequence_header(
        &over(0xaf, 0x00),
        ReceivedDataType::Audio
    ));
    // A huge NON-header video frame (keyframe 0x17 0x01) is left to the
    // write queue's backpressure policy, not gated here.
    assert!(!is_oversized_sequence_header(
        &over(0x17, 0x01),
        ReceivedDataType::Video
    ));
    // Exactly at the cap is still cacheable (strict `>`).
    let mut at_cap = vec![0u8; MAX_CACHEABLE_SEQUENCE_HEADER_BYTES];
    at_cap[0] = 0x17;
    at_cap[1] = 0x00;
    assert!(!is_oversized_sequence_header(
        &Bytes::from(at_cap),
        ReceivedDataType::Video
    ));
}

// the event-level predicate shared by the per-event ingest gate and the
// publisher-batch pre-scan. Flags only oversized media sequence headers.
#[test]
fn oversized_sequence_header_error_flags_only_oversized_media() {
    let media = |video: bool, first: u8, second: u8, len: usize| {
        let mut d = vec![0u8; len];
        if len >= 2 {
            d[0] = first;
            d[1] = second;
        }
        let data = Bytes::from(d);
        let timestamp = RtmpTimestamp { value: 0 };
        if video {
            ServerSessionEvent::VideoDataReceived {
                app_name: "a".to_string(),
                stream_key: "k".to_string(),
                data,
                timestamp,
            }
        } else {
            ServerSessionEvent::AudioDataReceived {
                app_name: "a".to_string(),
                stream_key: "k".to_string(),
                data,
                timestamp,
            }
        }
    };
    let over = MAX_CACHEABLE_SEQUENCE_HEADER_BYTES + 1;
    // Oversized video (0x17 0x00) and audio (0xaf 0x00) sequence headers → fatal.
    assert!(oversized_sequence_header_error(&media(true, 0x17, 0x00, over)).is_some());
    assert!(oversized_sequence_header_error(&media(false, 0xaf, 0x00, over)).is_some());
    // A same-size NON-sequence-header video keyframe (0x17 0x01) is not gated.
    assert!(oversized_sequence_header_error(&media(true, 0x17, 0x01, over)).is_none());
    // A small sequence header is fine.
    assert!(oversized_sequence_header_error(&media(true, 0x17, 0x00, 64)).is_none());
    // A non-media event is never gated.
    assert!(
        oversized_sequence_header_error(&ServerSessionEvent::PublishStreamFinished {
            app_name: "a".to_string(),
            stream_key: "k".to_string(),
        })
        .is_none()
    );
}

/// An over-cap sequence header on the untrusted socket ingest path must be
/// refused: the publisher is aborted (Err → the reactor's misbehaving-
/// publisher removal) and the header is NEVER cached, so a late joiner's
/// replay can never carry a header that trips the queue-critical threshold
/// and disconnects it.
#[test]
fn oversized_sequence_header_rejected_at_ingest_not_cached() {
    let mut scheduler = RtmpScheduler::new(10);
    let publisher_conn = 100;
    assert!(scheduler.new_channel("live".to_string(), publisher_conn));

    let mut header = vec![0u8; MAX_CACHEABLE_SEQUENCE_HEADER_BYTES + 1];
    header[0] = 0x17;
    header[1] = 0x00;
    let mut results = Vec::new();
    let outcome = scheduler.handle_raised_event(
        usize::MAX,
        ServerSessionEvent::VideoDataReceived {
            app_name: "app".to_string(),
            stream_key: "live".to_string(),
            data: Bytes::from(header),
            timestamp: RtmpTimestamp { value: 1 },
        },
        &mut results,
    );

    assert!(
        matches!(outcome, Err(SchedulerError::OversizedSequenceHeader { .. })),
        "an over-cap sequence header must abort the publisher at ingest"
    );
    assert!(
        results.is_empty(),
        "a rejected header must not be cached or fanned out"
    );
    assert!(
        scheduler.channel_video_sequence_header("live").is_none(),
        "an over-cap sequence header must never enter the cache"
    );
    assert_eq!(
        scheduler.channel_frozen_gop_count("live"),
        0,
        "an over-cap header must not be saved into the GOP cache either"
    );
}

// ---- F4: a codec-config change must not replay old-config GOPs ----

/// After a video sequence-header change, cached GOPs from the old config are
/// dropped: a joiner arriving after the change never receives an
/// old-config keyframe.
#[test]
fn codec_config_change_clears_stale_gops_from_the_replay() {
    const VIDEO_SEQ_A: &[u8] = &[0x17, 0x00, 0x00, 0x00, 0x00, 0x01, 0x64];
    const VIDEO_SEQ_B: &[u8] = &[0x17, 0x00, 0x00, 0x00, 0x00, 0x01, 0x2a];
    const KEYFRAME_A1: &[u8] = &[0x17, 0x01, 0xa1, 0x00, 0x00];
    const KEYFRAME_A2: &[u8] = &[0x17, 0x01, 0xa2, 0x00, 0x00];
    const KEYFRAME_B: &[u8] = &[0x17, 0x01, 0xb1, 0x00, 0x00];

    let mut scheduler = RtmpScheduler::new(10);
    let publisher_conn = 100;
    assert!(scheduler.new_channel("live".to_string(), publisher_conn));

    // Old config: header A then two keyframes, so a GOP-A is frozen and one is open.
    feed_media(&mut scheduler, publisher_conn, 0x09, 0, VIDEO_SEQ_A);
    feed_media(&mut scheduler, publisher_conn, 0x09, 33, KEYFRAME_A1);
    feed_media(&mut scheduler, publisher_conn, 0x09, 66, KEYFRAME_A2);
    assert!(
        scheduler.channels.get("live").unwrap().gops.frozen_count() >= 1,
        "a GOP-A must be frozen before the config change"
    );

    // Config change: a DIFFERENT header B, then a new-config keyframe.
    feed_media(&mut scheduler, publisher_conn, 0x09, 99, VIDEO_SEQ_B);
    feed_media(&mut scheduler, publisher_conn, 0x09, 132, KEYFRAME_B);

    let channel = scheduler.channels.get("live").unwrap();
    assert_eq!(
        channel.video_sequence_header.as_deref(),
        Some(VIDEO_SEQ_B),
        "the latest header must be cached"
    );

    let mut client = make_watching_client(7, "live", 1);
    let mut out = Vec::new();
    build_join_burst(channel, &mut client, 7, 1, 0, &mut out);

    assert!(
        out.iter().any(|p| packet_contains(p, KEYFRAME_B)),
        "the new-config keyframe must be replayable to a joiner"
    );
    assert!(
        !out.iter().any(|p| packet_contains(p, &[0x17, 0x01, 0xa1])),
        "the old-config keyframe A1 must have been cleared on the header change"
    );
    assert!(
        !out.iter().any(|p| packet_contains(p, &[0x17, 0x01, 0xa2])),
        "the old-config keyframe A2 must have been cleared on the header change"
    );
}

/// A byte-identical sequence-header resend (keepalive) must NOT clear the
/// cache — otherwise every duplicate header would wipe the replay history.
#[test]
fn identical_sequence_header_resend_keeps_the_gop_cache() {
    const KEYFRAME_1: &[u8] = &[0x17, 0x01, 0x51, 0x00, 0x00];
    const KEYFRAME_2: &[u8] = &[0x17, 0x01, 0x52, 0x00, 0x00];

    let mut scheduler = RtmpScheduler::new(10);
    let publisher_conn = 100;
    assert!(scheduler.new_channel("live".to_string(), publisher_conn));

    feed_media(&mut scheduler, publisher_conn, 0x09, 0, VIDEO_SEQ);
    feed_media(&mut scheduler, publisher_conn, 0x09, 33, KEYFRAME_1);
    feed_media(&mut scheduler, publisher_conn, 0x09, 66, KEYFRAME_2);
    let frozen_before = scheduler.channels.get("live").unwrap().gops.frozen_count();
    assert!(frozen_before >= 1);

    // Re-send the SAME header: a keepalive, not a config change.
    feed_media(&mut scheduler, publisher_conn, 0x09, 99, VIDEO_SEQ);

    let channel = scheduler.channels.get("live").unwrap();
    assert_eq!(
        channel.gops.frozen_count(),
        frozen_before,
        "an identical header resend must not clear the GOP cache"
    );
    let mut client = make_watching_client(7, "live", 1);
    let mut out = Vec::new();
    build_join_burst(channel, &mut client, 7, 1, 0, &mut out);
    assert!(
        out.iter().any(|p| packet_contains(p, &[0x17, 0x01, 0x51])),
        "the cached keyframe must survive an identical header resend"
    );
}

/// F4: when publisher A ends while a slow watcher lingers (the channel
/// survives), a NEW publisher B that reclaims the same key with the SAME
/// sequence header must not replay A's cached GOPs. The clear-on-change gate
/// cannot fire for an identical header, so publisher-end must reset the full
/// channel state (headers, timestamps, GOP cache) — a fresh joiner then sees
/// only B's session.
#[test]
fn publisher_end_resets_channel_so_same_key_reuse_replays_only_the_new_session() {
    const KEYFRAME_A: &[u8] = &[0x17, 0x01, 0xa1, 0x00, 0x00];
    const KEYFRAME_B: &[u8] = &[0x17, 0x01, 0xb1, 0x00, 0x00];

    let mut scheduler = RtmpScheduler::new(10);
    let publisher_a = 100;
    let publisher_b = 101;
    let lingering_watcher = 2;

    // Publisher A publishes header H and keyframe A while a watcher is attached
    // (so the channel outlives A's departure).
    assert!(scheduler.new_channel("live".to_string(), publisher_a));
    play(&mut scheduler, lingering_watcher, "live");
    feed_media(&mut scheduler, publisher_a, 0x09, 0, VIDEO_SEQ);
    feed_media(&mut scheduler, publisher_a, 0x09, 33, KEYFRAME_A);
    assert_eq!(
        scheduler.channel_video_sequence_header("live").as_deref(),
        Some(VIDEO_SEQ),
        "publisher A's header must be cached before it ends"
    );

    // Publisher A ends. The lingering watcher keeps the channel alive, but
    // its publisher-scoped state must be fully reset.
    scheduler.notify_publisher_closed(publisher_a);
    assert!(
        scheduler.channels.contains_key("live"),
        "the lingering watcher must keep the channel alive"
    );
    assert!(
        scheduler.channel_video_sequence_header("live").is_none(),
        "publisher-end must clear the cached sequence header"
    );

    // Publisher B reclaims the same key with the SAME header and its own keyframe.
    assert!(scheduler.new_channel("live".to_string(), publisher_b));
    feed_media(&mut scheduler, publisher_b, 0x09, 0, VIDEO_SEQ);
    feed_media(&mut scheduler, publisher_b, 0x09, 33, KEYFRAME_B);

    // A fresh joiner must receive ONLY publisher B's keyframe, never A's.
    let channel = scheduler.channels.get("live").unwrap();
    let mut joiner = make_watching_client(7, "live", 1);
    let mut out = Vec::new();
    build_join_burst(channel, &mut joiner, 7, 1, 0, &mut out);
    assert!(
        out.iter().any(|p| packet_contains(p, KEYFRAME_B)),
        "the new session's keyframe must be replayable"
    );
    assert!(
        !out.iter().any(|p| packet_contains(p, KEYFRAME_A)),
        "the previous session's keyframe must never leak into the new session"
    );
}
