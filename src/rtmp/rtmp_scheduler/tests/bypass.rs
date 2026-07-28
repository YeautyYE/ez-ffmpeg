//! In-process media bypass (PERF-5a) tests.

use super::*;

// PERF-5a: the in-process media bypass (publish_media_received) must feed
// the exact same channel machinery the serialize path reaches, so a live
// watcher observes sequence-header-first, keyframe-first, correctly gated and
// interleaved audio/video.
#[test]
fn in_process_bypass_delivers_seq_header_then_gated_keyframe_then_interleaved() {
    let mut scheduler = RtmpScheduler::new(10);
    let publisher_conn = 100;
    let watcher_conn = 2;

    assert!(scheduler.new_channel("live".to_string(), publisher_conn));
    play(&mut scheduler, watcher_conn, "live");
    let watcher_client_id = *scheduler
        .connection_to_client_map
        .get(&watcher_conn)
        .unwrap();

    // Sequence headers pass the gate even before any keyframe.
    let results = feed_media(&mut scheduler, publisher_conn, 0x09, 0, VIDEO_SEQ);
    assert_eq!(
        results.len(),
        1,
        "video sequence header must reach the watcher"
    );
    assert!(matches!(
        &results[0],
        ServerResult::OutboundPacket {
            is_sequence_header: true,
            is_video: true,
            ..
        }
    ));

    let results = feed_media(&mut scheduler, publisher_conn, 0x08, 0, AUDIO_SEQ);
    assert_eq!(
        results.len(),
        1,
        "audio sequence header must reach the watcher"
    );
    assert!(matches!(
        &results[0],
        ServerResult::OutboundPacket {
            is_sequence_header: true,
            is_video: false,
            ..
        }
    ));

    // A delta frame and audio before the first keyframe are withheld (gate).
    assert!(
        feed_media(&mut scheduler, publisher_conn, 0x09, 33, DELTA).is_empty(),
        "delta before the keyframe must be withheld"
    );
    assert!(
        feed_media(&mut scheduler, publisher_conn, 0x08, 33, AUDIO_FRAME).is_empty(),
        "audio before the first keyframe must be withheld"
    );
    assert!(
        !scheduler
            .clients
            .get(watcher_client_id)
            .unwrap()
            .has_received_video_keyframe
    );

    // The keyframe opens the gate and is delivered with is_keyframe set.
    let results = feed_media(&mut scheduler, publisher_conn, 0x09, 66, KEYFRAME);
    assert_eq!(results.len(), 1, "the keyframe must be delivered");
    assert!(matches!(
        &results[0],
        ServerResult::OutboundPacket {
            is_keyframe: true,
            is_video: true,
            ..
        }
    ));
    assert!(
        scheduler
            .clients
            .get(watcher_client_id)
            .unwrap()
            .has_received_video_keyframe
    );

    // After the keyframe, audio and delta video interleave through to the watcher.
    let results = feed_media(&mut scheduler, publisher_conn, 0x08, 70, AUDIO_FRAME);
    assert_eq!(results.len(), 1, "audio flows after the keyframe");
    assert!(matches!(
        &results[0],
        ServerResult::OutboundPacket {
            is_video: false,
            is_keyframe: false,
            ..
        }
    ));

    let results = feed_media(&mut scheduler, publisher_conn, 0x09, 99, DELTA);
    assert_eq!(results.len(), 1, "delta flows after the keyframe");
    assert!(matches!(
        &results[0],
        ServerResult::OutboundPacket {
            is_video: true,
            is_keyframe: false,
            ..
        }
    ));
}

// PERF-5a: bypassed media must populate the same cached sequence headers,
// header timestamps and GOP cache the serialize path would, so the
// existing late-watcher replay path has identical state to work from.
#[test]
fn in_process_bypass_populates_seq_headers_and_gop_cache_for_replay() {
    let mut scheduler = RtmpScheduler::new(10);
    let publisher_conn = 100;

    assert!(scheduler.new_channel("live".to_string(), publisher_conn));

    // Publish a full GOP via the bypass before any watcher joins.
    feed_media(&mut scheduler, publisher_conn, 0x09, 0, VIDEO_SEQ);
    feed_media(&mut scheduler, publisher_conn, 0x08, 0, AUDIO_SEQ);
    feed_media(&mut scheduler, publisher_conn, 0x09, 33, KEYFRAME);
    feed_media(&mut scheduler, publisher_conn, 0x08, 34, AUDIO_FRAME);
    feed_media(&mut scheduler, publisher_conn, 0x09, 66, DELTA);
    // A second keyframe freezes the first GOP into the replay cache.
    feed_media(&mut scheduler, publisher_conn, 0x09, 99, KEYFRAME);

    // The bypass must have cached the sequence headers and their timestamps
    // (byte-identical to the serialize path), and frozen the completed GOP.
    // This is exactly the state handle_play_requested replays to late
    // joiners, so preserving it proves metadata/seq-header/GOP semantics.
    let channel = scheduler.channels.get("live").unwrap();
    assert_eq!(channel.video_sequence_header.as_deref(), Some(VIDEO_SEQ));
    assert_eq!(channel.video_timestamp, RtmpTimestamp { value: 0 });
    assert_eq!(channel.audio_sequence_header.as_deref(), Some(AUDIO_SEQ));
    assert_eq!(channel.audio_timestamp, RtmpTimestamp { value: 0 });
    assert!(
        channel.gops.frozen_count() >= 1,
        "the completed GOP must be frozen for replay"
    );
}

// Defense-in-depth for the fanout path: even if a stale membership leaks
// into a channel's watcher set, frames must not follow it.
#[test]
fn fanout_skips_watchers_whose_action_points_at_another_stream() {
    let mut scheduler = RtmpScheduler::new(10);
    let publisher_conn = 100;
    let watcher_conn = 2;

    assert!(scheduler.new_channel("stream_a".to_string(), publisher_conn));
    play(&mut scheduler, watcher_conn, "stream_a");
    let client_id = *scheduler
        .connection_to_client_map
        .get(&watcher_conn)
        .unwrap();

    // Simulate the stale membership this fix prevents: the client's
    // action moved to another stream (a real channel, with its own
    // handle) but its id was left in A's set.
    let (stream_b_handle, _) = scheduler.channels.get_or_create("stream_b", 10);
    scheduler.clients.get_mut(client_id).unwrap().current_action = ClientAction::Watching {
        stream_key: "stream_b".to_string(),
        stream_id: 1,
        channel: stream_b_handle,
    };

    let results = feed_video(&mut scheduler, "stream_a", 0, KEYFRAME);
    assert!(
        results.is_empty(),
        "frames must not be delivered through a stale watcher membership"
    );
    assert!(
        !scheduler
            .clients
            .get(client_id)
            .unwrap()
            .has_received_video_keyframe,
        "a mismatched channel must not open the client's keyframe gate"
    );
}
