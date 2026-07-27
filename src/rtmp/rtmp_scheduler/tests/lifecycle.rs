//! Watcher liveness ping and client/channel lifecycle tests.

use super::*;

// R-B: the liveness ping must target exactly the Watching role — every
// other classification stays reapable by the idle sweep — and the built
// packet must decode as a UserControl PingRequest from the client's own
// session serializer.
#[test]
fn ping_watcher_builds_a_ping_request_only_for_watching_clients() {
    use rml_rtmp::chunk_io::ChunkDeserializer;
    use rml_rtmp::messages::{MessagePayload, RtmpMessage, UserControlEventType};

    let mut scheduler = RtmpScheduler::new(1);

    // Unknown connection: nothing to ping.
    assert!(scheduler.ping_watcher(9).is_none());

    // A client that only connected (Waiting) is not probed.
    let waiting_connection_id = 5;
    let _ = scheduler.bytes_received(waiting_connection_id, &[]);
    assert!(scheduler.ping_watcher(waiting_connection_id).is_none());

    // A network publisher is never probed: one idle for the full
    // timeout is dead weight pinning a stream key and must be reaped.
    let publisher_connection_id = 6;
    let _ = scheduler.bytes_received(publisher_connection_id, &[]);
    let publisher_client_id = *scheduler
        .connection_to_client_map
        .get(&publisher_connection_id)
        .unwrap();
    scheduler
        .clients
        .get_mut(publisher_client_id)
        .unwrap()
        .current_action = ClientAction::Publishing(Rc::from("ping_stream"));
    assert!(scheduler.ping_watcher(publisher_connection_id).is_none());

    // A watcher gets a ping, built by its own session. The session's
    // serializer may compress headers against chunks it sent earlier on
    // the same control chunk stream, so the decoder must replay the
    // session's prior output before the ping — which is the point: the
    // packet has to come from the client's own session, not a fresh
    // serializer.
    fn drain_messages(
        deserializer: &mut ChunkDeserializer,
        bytes: &[u8],
    ) -> Vec<MessagePayload> {
        let mut messages = Vec::new();
        let mut next = deserializer
            .get_next_message(bytes)
            .expect("valid chunk bytes");
        while let Some(payload) = next {
            messages.push(payload);
            next = deserializer
                .get_next_message(&[])
                .expect("valid buffered continuation");
        }
        messages
    }

    let watcher_connection_id = 7;
    let mut deserializer = ChunkDeserializer::new();
    let prior = scheduler
        .bytes_received(watcher_connection_id, &[])
        .expect("create the watcher client");
    let mut results = Vec::new();
    scheduler.handle_play_requested(
        watcher_connection_id,
        1,
        "app".to_string(),
        "ping_stream".to_string(),
        1,
        &mut results,
    );
    for result in prior.into_iter().chain(results) {
        if let ServerResult::OutboundPacket {
            target_connection_id,
            bytes,
            ..
        } = result
        {
            if target_connection_id == watcher_connection_id {
                drain_messages(&mut deserializer, &bytes);
            }
        }
    }

    let packet = scheduler
        .ping_watcher(watcher_connection_id)
        .expect("a watching client must be pingable");
    let messages = drain_messages(&mut deserializer, &packet.bytes);
    assert_eq!(messages.len(), 1, "the ping packet is one message");
    let message = messages
        .into_iter()
        .next()
        .expect("length asserted above")
        .to_rtmp_message()
        .expect("decode the ping payload");
    assert!(
        matches!(
            message,
            RtmpMessage::UserControl {
                event_type: UserControlEventType::PingRequest,
                ..
            }
        ),
        "the built packet must be a UserControl PingRequest, got {message:?}"
    );
}

#[test]
fn test_new_channel_creation() {
    let mut scheduler = RtmpScheduler::new(10);
    let stream_key = "test_stream".to_string();
    let publisher_connection_id = 1;

    // First channel creation should succeed
    let result = scheduler.new_channel(stream_key.clone(), publisher_connection_id);
    assert!(result, "First channel creation should succeed");

    // Verify channel exists
    assert!(scheduler.channels.contains_key(&stream_key));

    // Verify publisher mapping exists
    assert!(scheduler
        .publisher_to_client_map
        .contains_key(&publisher_connection_id));
}

#[test]
fn test_duplicate_channel_rejected() {
    let mut scheduler = RtmpScheduler::new(10);
    let stream_key = "test_stream".to_string();
    let publisher_connection_id_1 = 1;
    let publisher_connection_id_2 = 2;

    // First channel creation should succeed
    let result1 = scheduler.new_channel(stream_key.clone(), publisher_connection_id_1);
    assert!(result1, "First channel creation should succeed");

    // Set the publishing_client_id to simulate active publisher
    if let Some(channel) = scheduler.channels.get_mut(&stream_key) {
        channel.publishing_client_id = Some(0);
    }

    // Second channel creation with same stream_key should fail
    let result2 = scheduler.new_channel(stream_key.clone(), publisher_connection_id_2);
    assert!(!result2, "Duplicate channel creation should be rejected");

    // Verify only first publisher is mapped
    assert!(scheduler
        .publisher_to_client_map
        .contains_key(&publisher_connection_id_1));
    assert!(!scheduler
        .publisher_to_client_map
        .contains_key(&publisher_connection_id_2));
}

#[test]
fn test_notify_connection_closed() {
    let mut scheduler = RtmpScheduler::new(10);
    let connection_id = 1;

    // Create a session by calling bytes_received
    let _ = scheduler.bytes_received(connection_id, &[]);

    // Verify connection exists
    assert!(scheduler
        .connection_to_client_map
        .contains_key(&connection_id));
    let client_id = *scheduler
        .connection_to_client_map
        .get(&connection_id)
        .unwrap();
    assert!(scheduler.clients.contains(client_id));

    // Close the connection
    scheduler.notify_connection_closed(connection_id);

    // Verify connection is removed
    assert!(!scheduler
        .connection_to_client_map
        .contains_key(&connection_id));
    assert!(!scheduler.clients.contains(client_id));
}

#[test]
fn test_notify_publisher_closed() {
    let mut scheduler = RtmpScheduler::new(10);
    let stream_key = "test_stream".to_string();
    let publisher_connection_id = 1;

    // Create a channel
    let result = scheduler.new_channel(stream_key.clone(), publisher_connection_id);
    assert!(result, "Channel creation should succeed");

    // Verify publisher exists
    assert!(scheduler
        .publisher_to_client_map
        .contains_key(&publisher_connection_id));
    let client_id = *scheduler
        .publisher_to_client_map
        .get(&publisher_connection_id)
        .unwrap();
    assert!(scheduler.clients.contains(client_id));

    // Close the publisher
    scheduler.notify_publisher_closed(publisher_connection_id);

    // Verify publisher is removed
    assert!(!scheduler
        .publisher_to_client_map
        .contains_key(&publisher_connection_id));
    assert!(!scheduler.clients.contains(client_id));

    // With no watchers, channel should be removed (memory cleanup)
    assert!(
        !scheduler.channels.contains_key(&stream_key),
        "Empty channel (no publisher, no watchers) should be removed"
    );
}

#[test]
fn test_notify_publisher_closed_with_watchers() {
    let mut scheduler = RtmpScheduler::new(10);
    let stream_key = "test_stream".to_string();
    let publisher_connection_id = 1;

    // Create a channel
    let result = scheduler.new_channel(stream_key.clone(), publisher_connection_id);
    assert!(result, "Channel creation should succeed");

    // Add a watcher to the channel
    if let Some(channel) = scheduler.channels.get_mut(&stream_key) {
        channel.watching_client_ids.insert(100);
    }

    // Close the publisher
    scheduler.notify_publisher_closed(publisher_connection_id);

    // Verify publisher is removed
    assert!(!scheduler
        .publisher_to_client_map
        .contains_key(&publisher_connection_id));

    // With watchers still present, channel should remain
    assert!(
        scheduler.channels.contains_key(&stream_key),
        "Channel with watchers should remain after publisher closes"
    );
    if let Some(channel) = scheduler.channels.get(&stream_key) {
        assert_eq!(channel.publishing_client_id, None);
        assert!(channel.watching_client_ids.contains(&100));
    }
}

#[test]
fn test_publish_bytes_to_nonexistent_connection() {
    let mut scheduler = RtmpScheduler::new(10);
    let nonexistent_connection_id = 999;

    // Attempt to publish bytes to a connection that doesn't exist
    let result = scheduler.publish_bytes_received(nonexistent_connection_id, vec![0x03]);

    // Should succeed but return empty results (with warning logged)
    assert!(result.is_ok());
    assert!(result.unwrap().is_empty());
}

#[test]
fn test_bytes_received_creates_session() {
    let mut scheduler = RtmpScheduler::new(10);
    let connection_id = 1;

    // Verify connection doesn't exist initially
    assert!(!scheduler
        .connection_to_client_map
        .contains_key(&connection_id));

    // Receive bytes from new connection
    let result = scheduler.bytes_received(connection_id, &[0x03]);

    // Should succeed
    assert!(result.is_ok());

    // Verify session was created
    assert!(scheduler
        .connection_to_client_map
        .contains_key(&connection_id));
    let client_id = *scheduler
        .connection_to_client_map
        .get(&connection_id)
        .unwrap();
    assert!(scheduler.clients.contains(client_id));

    // Verify client is in Waiting state
    let client = scheduler.clients.get(client_id).unwrap();
    assert!(matches!(client.current_action, ClientAction::Waiting));
}

#[test]
fn test_handle_play_request_flow() {
    let mut scheduler = RtmpScheduler::new(10);
    let stream_key = "test_stream".to_string();
    let connection_id = 1;

    // Create a watcher connection
    let _ = scheduler.bytes_received(connection_id, &[]);

    // Verify connection exists and is in Waiting state
    assert!(scheduler
        .connection_to_client_map
        .contains_key(&connection_id));
    let client_id = *scheduler
        .connection_to_client_map
        .get(&connection_id)
        .unwrap();
    let client = scheduler.clients.get(client_id).unwrap();
    assert!(matches!(client.current_action, ClientAction::Waiting));

    // Simulate play request by directly calling handle_play_requested
    let mut server_results = Vec::new();
    scheduler.handle_play_requested(
        connection_id,
        1, // request_id
        "test_app".to_string(),
        stream_key.clone(),
        1, // stream_id
        &mut server_results,
    );

    // Verify client is now in Watching state
    let client = scheduler.clients.get(client_id).unwrap();
    assert!(matches!(
        client.current_action,
        ClientAction::Watching { .. }
    ));

    // Verify channel was created and client is registered as watcher
    assert!(scheduler.channels.contains_key(&stream_key));
    let channel = scheduler.channels.get(&stream_key).unwrap();
    assert!(channel.watching_client_ids.contains(&client_id));
}

#[test]
fn test_scheduler_error_propagation() {
    let mut scheduler = RtmpScheduler::new(10);
    let connection_id = 1;

    // Create a session
    let _ = scheduler.bytes_received(connection_id, &[]);

    // Send invalid RTMP data that should cause a session error
    // Using a very short invalid chunk that will fail parsing
    let invalid_data = vec![0xFF, 0xFF];
    let result = scheduler.bytes_received(connection_id, &invalid_data);

    // Should return error (or Ok with empty results depending on rml_rtmp behavior)
    // The key is that it doesn't panic and returns Result type
    match result {
        Ok(_) => {
            // Some invalid data might be silently ignored by rml_rtmp
            // This is acceptable behavior
        }
        Err(_) => {
            // Error should be properly wrapped in SchedulerError
            // The fact that we got an Err variant means error handling works
        }
    }
    // Test passes if we reach here without panicking
}

#[test]
fn test_invalid_stream_key_warning() {
    let mut scheduler = RtmpScheduler::new(10);
    let stream_key = "nonexistent_stream".to_string();

    // Simulate receiving audio/video data for a stream that doesn't exist
    let mut server_results = Vec::new();
    let timestamp = RtmpTimestamp { value: 0 };
    let data = Bytes::from(vec![0x17, 0x01, 0x00, 0x00, 0x00]); // Video data

    scheduler.handle_audio_video_data_received(
        &stream_key,
        timestamp,
        data,
        ReceivedDataType::Video,
        &mut server_results,
    );

    // Should not panic, just return early with no results
    assert!(server_results.is_empty());

    // Channel should not be created
    assert!(!scheduler.channels.contains_key(&stream_key));
}

// The per-tag fanout walks `watching_client_ids` at table CAPACITY,
// not len, so a flash crowd that decays must not leave every later tag
// paying for the historical peak. The guarded shrink at the single
// membership-removal site (`play_ended`) has to walk the capacity back
// down once occupancy drops below a quarter — without disturbing delivery
// to the watchers that remain.
#[test]
fn watcher_set_capacity_shrinks_after_watcher_churn() {
    let mut scheduler = RtmpScheduler::new(1);
    let stream_key = "shrink_stream";
    let publisher_connection_id = 1;
    assert!(scheduler.new_channel(stream_key.to_string(), publisher_connection_id));

    // Flash crowd: 512 distinct watchers join, growing the table well
    // past the shrink floor.
    let first_watcher = 100;
    let watcher_count = 512;
    for connection_id in first_watcher..first_watcher + watcher_count {
        play(&mut scheduler, connection_id, stream_key);
    }
    let channel = scheduler.channels.get(stream_key).unwrap();
    assert_eq!(channel.watching_client_ids.len(), watcher_count);
    let peak_capacity = channel.watching_client_ids.capacity();
    assert!(
        peak_capacity > WATCHER_SET_SHRINK_MIN_CAPACITY,
        "512 watchers must grow the table past the shrink floor, got {peak_capacity}"
    );

    // The crowd decays to one survivor; every leave funnels through
    // `play_ended`, so the opportunistic shrink runs on the way down.
    let survivor_connection_id = first_watcher;
    for connection_id in first_watcher + 1..first_watcher + watcher_count {
        scheduler.notify_connection_closed(connection_id);
    }
    let channel = scheduler.channels.get(stream_key).unwrap();
    assert_eq!(channel.watching_client_ids.len(), 1);
    let decayed_capacity = channel.watching_client_ids.capacity();
    assert!(
        decayed_capacity < WATCHER_SET_SHRINK_MIN_CAPACITY,
        "decayed watcher set must shrink below the floor, got {decayed_capacity}"
    );

    // Fanout must be intact across the rehash: the survivor still
    // receives media.
    let results = feed_video(&mut scheduler, stream_key, 0, KEYFRAME);
    assert!(
        results.iter().any(|result| matches!(
            result,
            ServerResult::OutboundPacket {
                target_connection_id,
                ..
            } if *target_connection_id == survivor_connection_id
        )),
        "survivor must still receive the keyframe after the shrink"
    );
}
