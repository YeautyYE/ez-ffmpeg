//! Audio-only gating, keyframe gating and live distribution tests.

use super::super::fanout::should_send_to_watcher;
use super::super::join::gop_contains_video_keyframe;
use super::*;

// GitHub: audio-only RTMP streams were silent for subscribers because audio
// delivery was gated on a video keyframe that never arrives.
#[test]
fn audio_only_stream_delivers_audio_without_a_video_keyframe() {
    // Metadata declared the stream audio-only.
    let channel_is_audio_only = true;
    // A normal (non-sequence-header) audio packet, client has no keyframe.
    assert!(
        should_send_to_watcher(
            ReceivedDataType::Audio,
            /* has_received_video_keyframe */ false,
            /* is_keyframe */ false,
            /* is_sequence_header */ false,
            channel_is_audio_only,
        ),
        "audio-only stream must deliver audio frames to a fresh subscriber"
    );
}

#[test]
fn av_stream_still_gates_audio_on_a_video_keyframe() {
    // A/V (or not-yet-known) channel: not positively audio-only.
    let channel_is_audio_only = false;
    // Before the client has a keyframe, a plain audio packet is withheld
    // (unchanged behaviour, keeps A/V start in sync — this also covers the
    // start-up window before any video packet has arrived).
    assert!(
        !should_send_to_watcher(
            ReceivedDataType::Audio,
            false,
            false,
            false,
            channel_is_audio_only,
        ),
        "A/V stream must still withhold audio until the client has a keyframe"
    );
    // Once the client has a keyframe, audio flows.
    assert!(should_send_to_watcher(
        ReceivedDataType::Audio,
        true,
        false,
        false,
        channel_is_audio_only,
    ));
    // An audio sequence header is always delivered (needed to decode).
    assert!(should_send_to_watcher(
        ReceivedDataType::Audio,
        false,
        false,
        /* is_sequence_header */ true,
        channel_is_audio_only,
    ));
}

// Full-path regression guard: with audio-only metadata, a watcher receives
// audio even though no video keyframe ever arrives. Complements the
// extracted-gate unit tests by exercising the metadata detection at the call
// site in `handle_audio_video_data_received`.
#[test]
fn audio_only_metadata_lets_watcher_receive_audio_before_any_keyframe() {
    let mut scheduler = RtmpScheduler::new(10);
    let stream_key = "audio_only".to_string();
    let publisher_connection_id = 1;
    let watcher_connection_id = 2;

    scheduler.new_channel(stream_key.clone(), publisher_connection_id);
    let _ = scheduler.bytes_received(watcher_connection_id, &[]);
    let mut results = Vec::new();
    scheduler.handle_play_requested(
        watcher_connection_id,
        1,
        "app".to_string(),
        stream_key.clone(),
        1,
        &mut results,
    );

    // Publisher declares an audio-only stream: an audio codec, no video codec.
    let metadata = StreamMetadata {
        video_width: None,
        video_height: None,
        video_codec_id: None,
        video_frame_rate: None,
        video_bitrate_kbps: None,
        audio_codec_id: Some(10), // AAC
        audio_bitrate_kbps: None,
        audio_sample_rate: Some(44100),
        audio_channels: Some(2),
        audio_is_stereo: Some(true),
        encoder: None,
    };
    scheduler.handle_metadata_received(
        "app".to_string(),
        stream_key.clone(),
        metadata,
        &mut Vec::new(),
    );

    // A plain audio packet with no preceding video keyframe must reach the watcher.
    let mut server_results = Vec::new();
    let audio_data = Bytes::from(vec![0xAF, 0x01, 0xDD, 0xEE]);
    scheduler.handle_audio_video_data_received(
        &stream_key,
        RtmpTimestamp { value: 50 },
        audio_data,
        ReceivedDataType::Audio,
        &mut server_results,
    );

    assert_eq!(
        server_results.len(),
        1,
        "audio-only stream (declared via metadata) must deliver audio without a video keyframe"
    );
}

#[test]
fn video_gating_is_unchanged() {
    // channel_is_audio_only never affects the video path.
    for audio_only in [false, true] {
        // Video is withheld until the client has a keyframe...
        assert!(!should_send_to_watcher(
            ReceivedDataType::Video,
            false,
            false,
            false,
            audio_only,
        ));
        // ...but the keyframe itself (and sequence headers) always pass.
        assert!(should_send_to_watcher(
            ReceivedDataType::Video,
            false,
            /* is_keyframe */ true,
            false,
            audio_only,
        ));
        assert!(should_send_to_watcher(
            ReceivedDataType::Video,
            true,
            false,
            false,
            audio_only,
        ));
    }
}

#[test]
fn test_audio_video_distribution_to_watchers() {
    let mut scheduler = RtmpScheduler::new(10);
    let stream_key = "test_stream".to_string();
    let publisher_connection_id = 1;
    let watcher_connection_id = 2;

    // Step 1: Create channel with publisher
    let result = scheduler.new_channel(stream_key.clone(), publisher_connection_id);
    assert!(result, "Channel creation should succeed");

    // Step 2: Create watcher connection and have it request play
    let _ = scheduler.bytes_received(watcher_connection_id, &[]);
    let mut server_results = Vec::new();
    scheduler.handle_play_requested(
        watcher_connection_id,
        1, // request_id
        "test_app".to_string(),
        stream_key.clone(),
        1, // stream_id
        &mut server_results,
    );

    // Verify watcher is in channel
    let channel = scheduler.channels.get(&stream_key).unwrap();
    assert_eq!(channel.watching_client_ids.len(), 1);

    // Step 3: Send video keyframe (0x17 = AVC keyframe, 0x01 = NALU)
    server_results.clear();
    let keyframe_data = Bytes::from(vec![0x17, 0x01, 0x00, 0x00, 0x00, 0xAA, 0xBB]);
    let timestamp = RtmpTimestamp { value: 1000 };
    scheduler.handle_audio_video_data_received(
        &stream_key,
        timestamp,
        keyframe_data,
        ReceivedDataType::Video,
        &mut server_results,
    );

    // Verify watcher received the keyframe
    assert_eq!(server_results.len(), 1, "Watcher should receive keyframe");
    match &server_results[0] {
        ServerResult::OutboundPacket {
            target_connection_id,
            is_keyframe,
            is_video,
            ..
        } => {
            assert_eq!(*target_connection_id, watcher_connection_id);
            assert!(*is_keyframe, "Should be marked as keyframe");
            assert!(*is_video, "Should be marked as video");
        }
        _ => panic!("Expected OutboundPacket"),
    }

    // Step 4: Send video non-keyframe (0x27 = AVC inter-frame)
    server_results.clear();
    let non_keyframe_data = Bytes::from(vec![0x27, 0x01, 0x00, 0x00, 0x00, 0xCC]);
    scheduler.handle_audio_video_data_received(
        &stream_key,
        RtmpTimestamp { value: 1033 },
        non_keyframe_data,
        ReceivedDataType::Video,
        &mut server_results,
    );

    // Watcher should receive non-keyframe (after having received keyframe)
    assert_eq!(
        server_results.len(),
        1,
        "Watcher should receive non-keyframe"
    );
    match &server_results[0] {
        ServerResult::OutboundPacket {
            is_keyframe,
            is_video,
            ..
        } => {
            assert!(!*is_keyframe, "Should not be marked as keyframe");
            assert!(*is_video, "Should be marked as video");
        }
        _ => panic!("Expected OutboundPacket"),
    }

    // Step 5: Send audio data (0xAF = AAC audio)
    server_results.clear();
    let audio_data = Bytes::from(vec![0xAF, 0x01, 0xDD, 0xEE]);
    scheduler.handle_audio_video_data_received(
        &stream_key,
        RtmpTimestamp { value: 1040 },
        audio_data,
        ReceivedDataType::Audio,
        &mut server_results,
    );

    // Watcher should receive audio (after having received video keyframe)
    assert_eq!(server_results.len(), 1, "Watcher should receive audio");
    match &server_results[0] {
        ServerResult::OutboundPacket {
            is_video,
            is_keyframe,
            ..
        } => {
            assert!(!*is_video, "Should be marked as audio");
            assert!(!*is_keyframe, "Audio should not be keyframe");
        }
        _ => panic!("Expected OutboundPacket"),
    }
}

#[test]
fn test_multiple_watchers_distribution() {
    let mut scheduler = RtmpScheduler::new(10);
    let stream_key = "test_stream".to_string();
    let publisher_connection_id = 1;
    let watcher1_connection_id = 2;
    let watcher2_connection_id = 3;
    let watcher3_connection_id = 4;

    // Create channel with publisher
    scheduler.new_channel(stream_key.clone(), publisher_connection_id);

    // Create multiple watchers
    for (watcher_id, request_id) in [
        (watcher1_connection_id, 1u32),
        (watcher2_connection_id, 2u32),
        (watcher3_connection_id, 3u32),
    ] {
        let _ = scheduler.bytes_received(watcher_id, &[]);
        let mut results = Vec::new();
        scheduler.handle_play_requested(
            watcher_id,
            request_id,
            "app".to_string(),
            stream_key.clone(),
            1,
            &mut results,
        );
    }

    // Verify all watchers are in channel
    let channel = scheduler.channels.get(&stream_key).unwrap();
    assert_eq!(channel.watching_client_ids.len(), 3);

    // Send video keyframe
    let mut server_results = Vec::new();
    let keyframe_data = Bytes::from(vec![0x17, 0x01, 0x00, 0x00, 0x00]);
    scheduler.handle_audio_video_data_received(
        &stream_key,
        RtmpTimestamp { value: 0 },
        keyframe_data,
        ReceivedDataType::Video,
        &mut server_results,
    );

    // All 3 watchers should receive the packet
    assert_eq!(
        server_results.len(),
        3,
        "All watchers should receive keyframe"
    );

    // Verify each watcher received the packet
    let target_ids: HashSet<_> = server_results
        .iter()
        .filter_map(|r| match r {
            ServerResult::OutboundPacket {
                target_connection_id,
                ..
            } => Some(*target_connection_id),
            _ => None,
        })
        .collect();

    assert!(target_ids.contains(&watcher1_connection_id));
    assert!(target_ids.contains(&watcher2_connection_id));
    assert!(target_ids.contains(&watcher3_connection_id));
}

#[test]
fn test_watcher_receives_sequence_header_first() {
    let mut scheduler = RtmpScheduler::new(10);
    let stream_key = "test_stream".to_string();
    let publisher_connection_id = 1;
    let early_watcher_id = 2;

    // Create channel with publisher
    scheduler.new_channel(stream_key.clone(), publisher_connection_id);

    // First watcher joins
    let _ = scheduler.bytes_received(early_watcher_id, &[]);
    let mut results = Vec::new();
    scheduler.handle_play_requested(
        early_watcher_id,
        1,
        "app".to_string(),
        stream_key.clone(),
        1,
        &mut results,
    );

    // Send video sequence header (0x17 = AVC keyframe, 0x00 = sequence header)
    let mut server_results = Vec::new();
    let sequence_header = Bytes::from(vec![0x17, 0x00, 0x00, 0x00, 0x00, 0x01, 0x64]);
    scheduler.handle_audio_video_data_received(
        &stream_key,
        RtmpTimestamp { value: 0 },
        sequence_header.clone(),
        ReceivedDataType::Video,
        &mut server_results,
    );

    // Watcher should receive sequence header
    assert_eq!(server_results.len(), 1);
    match &server_results[0] {
        ServerResult::OutboundPacket {
            is_sequence_header, ..
        } => {
            assert!(*is_sequence_header, "Should be marked as sequence header");
        }
        _ => panic!("Expected OutboundPacket"),
    }

    // Verify sequence header is cached in channel
    let channel = scheduler.channels.get(&stream_key).unwrap();
    assert!(channel.video_sequence_header.is_some());
    assert_eq!(
        channel.video_sequence_header.as_ref().unwrap(),
        &sequence_header
    );
}

#[test]
fn test_watcher_without_keyframe_skips_non_keyframe() {
    let mut scheduler = RtmpScheduler::new(10);
    let stream_key = "test_stream".to_string();
    let publisher_connection_id = 1;
    let watcher_connection_id = 2;

    // Create channel and watcher
    scheduler.new_channel(stream_key.clone(), publisher_connection_id);
    let _ = scheduler.bytes_received(watcher_connection_id, &[]);
    let mut results = Vec::new();
    scheduler.handle_play_requested(
        watcher_connection_id,
        1,
        "app".to_string(),
        stream_key.clone(),
        1,
        &mut results,
    );

    // Send non-keyframe video BEFORE any keyframe (0x27 = inter-frame)
    let mut server_results = Vec::new();
    let non_keyframe = Bytes::from(vec![0x27, 0x01, 0x00, 0x00, 0x00]);
    scheduler.handle_audio_video_data_received(
        &stream_key,
        RtmpTimestamp { value: 100 },
        non_keyframe,
        ReceivedDataType::Video,
        &mut server_results,
    );

    // Watcher should NOT receive non-keyframe (hasn't received keyframe yet)
    assert!(
        server_results.is_empty(),
        "Watcher should skip non-keyframe before receiving keyframe"
    );

    // Now send keyframe
    let keyframe = Bytes::from(vec![0x17, 0x01, 0x00, 0x00, 0x00]);
    scheduler.handle_audio_video_data_received(
        &stream_key,
        RtmpTimestamp { value: 200 },
        keyframe,
        ReceivedDataType::Video,
        &mut server_results,
    );

    // Now watcher should receive keyframe
    assert_eq!(server_results.len(), 1, "Watcher should receive keyframe");
}

#[test]
fn test_audio_skipped_before_video_keyframe() {
    let mut scheduler = RtmpScheduler::new(10);
    let stream_key = "test_stream".to_string();
    let publisher_connection_id = 1;
    let watcher_connection_id = 2;

    // Create channel and watcher
    scheduler.new_channel(stream_key.clone(), publisher_connection_id);
    let _ = scheduler.bytes_received(watcher_connection_id, &[]);
    let mut results = Vec::new();
    scheduler.handle_play_requested(
        watcher_connection_id,
        1,
        "app".to_string(),
        stream_key.clone(),
        1,
        &mut results,
    );

    // Send audio BEFORE video keyframe
    let mut server_results = Vec::new();
    let audio_data = Bytes::from(vec![0xAF, 0x01, 0xDD, 0xEE]);
    scheduler.handle_audio_video_data_received(
        &stream_key,
        RtmpTimestamp { value: 50 },
        audio_data.clone(),
        ReceivedDataType::Audio,
        &mut server_results,
    );

    // Watcher should NOT receive audio (hasn't received video keyframe yet)
    assert!(
        server_results.is_empty(),
        "Watcher should skip audio before video keyframe"
    );

    // Send video keyframe
    let keyframe = Bytes::from(vec![0x17, 0x01, 0x00, 0x00, 0x00]);
    scheduler.handle_audio_video_data_received(
        &stream_key,
        RtmpTimestamp { value: 100 },
        keyframe,
        ReceivedDataType::Video,
        &mut server_results,
    );

    // Now send audio again
    server_results.clear();
    scheduler.handle_audio_video_data_received(
        &stream_key,
        RtmpTimestamp { value: 150 },
        audio_data,
        ReceivedDataType::Audio,
        &mut server_results,
    );

    // Now watcher should receive audio
    assert_eq!(
        server_results.len(),
        1,
        "Watcher should receive audio after video keyframe"
    );
}

#[test]
fn test_audio_sequence_header_sent_before_keyframe() {
    let mut scheduler = RtmpScheduler::new(10);
    let stream_key = "test_stream".to_string();
    let publisher_connection_id = 1;
    let watcher_connection_id = 2;

    // Create channel and watcher
    scheduler.new_channel(stream_key.clone(), publisher_connection_id);
    let _ = scheduler.bytes_received(watcher_connection_id, &[]);
    let mut results = Vec::new();
    scheduler.handle_play_requested(
        watcher_connection_id,
        1,
        "app".to_string(),
        stream_key.clone(),
        1,
        &mut results,
    );

    // Send audio sequence header (0xAF = AAC, 0x00 = sequence header)
    let mut server_results = Vec::new();
    let audio_seq_header = Bytes::from(vec![0xAF, 0x00, 0x12, 0x10]);
    scheduler.handle_audio_video_data_received(
        &stream_key,
        RtmpTimestamp { value: 0 },
        audio_seq_header.clone(),
        ReceivedDataType::Audio,
        &mut server_results,
    );

    // Audio sequence header SHOULD be sent even before video keyframe
    assert_eq!(
        server_results.len(),
        1,
        "Audio sequence header should be sent before video keyframe"
    );
    match &server_results[0] {
        ServerResult::OutboundPacket {
            is_sequence_header,
            is_video,
            ..
        } => {
            assert!(*is_sequence_header);
            assert!(!*is_video);
        }
        _ => panic!("Expected OutboundPacket"),
    }

    // Verify audio sequence header is cached
    let channel = scheduler.channels.get(&stream_key).unwrap();
    assert!(channel.audio_sequence_header.is_some());
}

#[test]
fn test_watcher_close_during_stream() {
    let mut scheduler = RtmpScheduler::new(10);
    let stream_key = "test_stream".to_string();
    let publisher_connection_id = 1;
    let watcher1_id = 2;
    let watcher2_id = 3;

    // Create channel with publisher
    scheduler.new_channel(stream_key.clone(), publisher_connection_id);

    // Create two watchers
    for watcher_id in [watcher1_id, watcher2_id] {
        let _ = scheduler.bytes_received(watcher_id, &[]);
        let mut results = Vec::new();
        scheduler.handle_play_requested(
            watcher_id,
            1,
            "app".to_string(),
            stream_key.clone(),
            1,
            &mut results,
        );
    }

    // Verify both watchers in channel
    assert_eq!(
        scheduler
            .channels
            .get(&stream_key)
            .unwrap()
            .watching_client_ids
            .len(),
        2
    );

    // Send initial keyframe to both
    let mut server_results = Vec::new();
    let keyframe = Bytes::from(vec![0x17, 0x01, 0x00, 0x00, 0x00]);
    scheduler.handle_audio_video_data_received(
        &stream_key,
        RtmpTimestamp { value: 0 },
        keyframe,
        ReceivedDataType::Video,
        &mut server_results,
    );
    assert_eq!(
        server_results.len(),
        2,
        "Both watchers should receive keyframe"
    );

    // Watcher 1 disconnects
    scheduler.notify_connection_closed(watcher1_id);

    // Verify watcher1 removed from channel
    let channel = scheduler.channels.get(&stream_key).unwrap();
    assert_eq!(channel.watching_client_ids.len(), 1);

    // Send another frame - only watcher2 should receive it
    server_results.clear();
    let frame = Bytes::from(vec![0x27, 0x01, 0x00, 0x00, 0x00]);
    scheduler.handle_audio_video_data_received(
        &stream_key,
        RtmpTimestamp { value: 33 },
        frame,
        ReceivedDataType::Video,
        &mut server_results,
    );

    assert_eq!(
        server_results.len(),
        1,
        "Only remaining watcher should receive frame"
    );
    match &server_results[0] {
        ServerResult::OutboundPacket {
            target_connection_id,
            ..
        } => {
            assert_eq!(*target_connection_id, watcher2_id);
        }
        _ => panic!("Expected OutboundPacket"),
    }
}

// The first GOP is frozen before the first keyframe and
// carries only the AVC sequence header (0x17 0x00) + pre-roll audio. GOP
// replay must not mark a watcher as having a keyframe from such a GOP, or
// should_send_to_watcher would forward undecodable delta frames to it.
#[test]
fn keyframeless_gop_does_not_flip_the_replay_keyframe_gate() {
    let seq_header = FrameData::Video {
        timestamp: RtmpTimestamp { value: 0 },
        data: Bytes::from_static(&[0x17, 0x00, 0x00, 0x00, 0x00]),
    };
    let pre_roll_audio = FrameData::Audio {
        timestamp: RtmpTimestamp { value: 0 },
        data: Bytes::from_static(&[0xaf, 0x01, 0x21]),
    };
    // A GOP with only the sequence header and audio (no keyframe) must NOT flip.
    assert!(
        !gop_contains_video_keyframe(&[seq_header.clone(), pre_roll_audio.clone()]),
        "a sequence-header-only GOP must not flip the keyframe gate"
    );

    // A normal GOP starts with a flagged keyframe (0x17 0x01, AVC NALU
    // packet) and must flip it.
    let keyframe = FrameData::Video {
        timestamp: RtmpTimestamp { value: 33 },
        data: Bytes::from_static(&[0x17, 0x01, 0x00, 0x00, 0x00]),
    };
    let delta = FrameData::Video {
        timestamp: RtmpTimestamp { value: 66 },
        data: Bytes::from_static(&[0x27, 0x01, 0x00, 0x00, 0x00]),
    };
    assert!(
        gop_contains_video_keyframe(&[keyframe, delta.clone(), pre_roll_audio.clone()]),
        "a GOP carrying a flagged keyframe must flip the keyframe gate"
    );

    // Mid-GOP publish start: the first frozen GOP can be delta frames only
    // (no keyframe, no sequence header). It must not flip the gate — the replay
    // loop skips its undecodable video and forwards only audio.
    assert!(
        !gop_contains_video_keyframe(&[delta.clone(), delta, pre_roll_audio]),
        "a delta-only GOP (mid-GOP start) must not flip the keyframe gate"
    );

    // AVC end-of-sequence (0x17 0x02) carries the keyframe frame-type but
    // is not a NALU packet (no picture data); it must not flip the gate
    // either.
    let end_of_seq = FrameData::Video {
        timestamp: RtmpTimestamp { value: 99 },
        data: Bytes::from_static(&[0x17, 0x02, 0x00, 0x00, 0x00]),
    };
    assert!(
        !gop_contains_video_keyframe(&[end_of_seq]),
        "an AVC end-of-sequence tag must not flip the keyframe gate"
    );
}
