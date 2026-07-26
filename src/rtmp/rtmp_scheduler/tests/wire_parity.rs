//! Byte-exact wire parity between the shared fanout path and the retired
//! per-session serialization path.

use super::super::join::build_join_burst;
use super::*;

// ======================================================================
// Shared-fanout wire goldens
// ======================================================================
//
// Three tiers pin the shared serialization against the retired
// per-session path:
// - steady state: byte-for-byte identity with a per-watcher reference
//   serializer (droppable history forces type 0 on both paths), across
//   extended timestamps, multi-chunk payloads, A/V interleave and
//   multiple message_stream_id groups;
// - join adjacency: a burst (per-burst throwaway serializer, allowed to
//   header-compress internally) followed by shared live packets must
//   round-trip rml's ChunkDeserializer with exact message semantics —
//   equivalence here is wire-level, not byte-level, by design;
// - repeat play: burst -> shared live -> second play -> second burst ->
//   live must stay decodable end to end (the stale-previous-headers
//   hole a session-serializer burst would reopen).

// Steady-state golden: under droppable history the shared product must
// be byte-identical to the per-session product, across an extended
// timestamp (> 0xFFFFFF), payloads crossing the 4096-byte chunk size
// (type-3 continuations, including extended-timestamp continuations),
// interleaved audio/video, and two message_stream_id groups.
#[test]
fn shared_fanout_steady_state_bytes_match_the_per_session_path() {
    let mut scheduler = RtmpScheduler::new(10);
    assert!(scheduler.new_channel("golden".to_string(), 100));

    // Two watchers in the stream-id-1 group, one in the stream-id-3
    // group.
    play_with_stream_id(&mut scheduler, 2, "golden", 1);
    play_with_stream_id(&mut scheduler, 3, "golden", 1);
    play_with_stream_id(&mut scheduler, 4, "golden", 3);

    // (data_type, timestamp, payload). The keyframe leads so every
    // watcher's gate opens on message one; message four crosses the
    // chunk size; message five carries an extended timestamp AND
    // crosses the chunk size, so its continuations re-emit the
    // extended timestamp.
    let schedule: Vec<(ReceivedDataType, u32, Bytes)> = vec![
        (
            ReceivedDataType::Video,
            1000,
            media_payload(0x17, 0x01, 0xA1, 32),
        ),
        (
            ReceivedDataType::Audio,
            1010,
            media_payload(0xAF, 0x01, 0xB1, 24),
        ),
        (
            ReceivedDataType::Video,
            1043,
            media_payload(0x27, 0x01, 0xA2, 10_000),
        ),
        (
            ReceivedDataType::Audio,
            1050,
            media_payload(0xAF, 0x01, 0xB2, 512),
        ),
        (
            ReceivedDataType::Video,
            0x0100_0010,
            media_payload(0x27, 0x01, 0xA3, 5_000),
        ),
        (
            ReceivedDataType::Audio,
            0x0100_0020,
            media_payload(0xAF, 0x01, 0xB3, 100),
        ),
    ];

    let mut fanned: Vec<Vec<ServerResult>> = Vec::new();
    for (data_type, timestamp, payload) in &schedule {
        fanned.push(feed(
            &mut scheduler,
            "golden",
            *data_type,
            *timestamp,
            payload,
        ));
    }

    // Reference: one retired-path serializer per watcher.
    let mut reference_group1 = reference_serializer();
    let mut reference_group3 = reference_serializer();
    for (index, (data_type, timestamp, payload)) in schedule.iter().enumerate() {
        let expected_g1 = reference_media_bytes(
            &mut reference_group1,
            *data_type,
            1,
            payload,
            *timestamp,
            true,
        );
        let expected_g3 = reference_media_bytes(
            &mut reference_group3,
            *data_type,
            3,
            payload,
            *timestamp,
            true,
        );
        for watcher_conn in [2usize, 3] {
            let packets = watcher_packets(&fanned[index], watcher_conn);
            assert_eq!(
                packets.len(),
                1,
                "watcher {watcher_conn} must receive message {index}"
            );
            assert!(packets[0].1, "live media must stay droppable");
            assert_same_bytes(
                &packets[0].0,
                &expected_g1,
                &format!("message {index} for group-1 watcher {watcher_conn}"),
            );
        }
        let packets = watcher_packets(&fanned[index], 4);
        assert_eq!(packets.len(), 1, "watcher 4 must receive message {index}");
        assert_same_bytes(
            &packets[0].0,
            &expected_g3,
            &format!("message {index} for the group-3 watcher"),
        );
        assert_ne!(
            expected_g1, expected_g3,
            "distinct stream ids must serialize distinct type-0 headers"
        );
    }

    // The group-1 watchers share ONE serialization: identical contents
    // by the assertions above, and the same refcounted buffer by
    // construction (`Bytes::clone` per watcher).
    let first = watcher_packets(&fanned[0], 2);
    let second = watcher_packets(&fanned[0], 3);
    assert_eq!(first[0].0, second[0].0);
}

// Join-adjacency golden: the (throwaway-serialized, possibly internally
// compressed) burst followed by shared type-0 live packets must parse
// through rml's own deserializer with exact message semantics. This
// tier is deliberately NOT a byte comparison: after a non-droppable
// burst the retired path could legally compress the first live packet,
// the shared path always emits type 0 — both are wire-correct.
#[test]
fn join_burst_and_adjacent_shared_live_parse_wire_level() {
    let mut scheduler = RtmpScheduler::new(10);
    assert!(scheduler.new_channel("adjacent".to_string(), 100));

    let vseq = media_payload(0x17, 0x00, 0xC1, 16);
    let aseq = media_payload(0xAF, 0x00, 0xC2, 4);
    let k1 = media_payload(0x17, 0x01, 0xD1, 64);
    let d1 = media_payload(0x27, 0x01, 0xD2, 48);
    let a1 = media_payload(0xAF, 0x01, 0xD3, 16);
    let d2 = media_payload(0x27, 0x01, 0xD4, 5_000);
    let k2 = media_payload(0x17, 0x01, 0xD5, 64);
    let a2 = media_payload(0xAF, 0x01, 0xD6, 16);
    let d3 = media_payload(0x27, 0x01, 0xD7, 48);

    // Cached history before the watcher joins.
    feed(&mut scheduler, "adjacent", ReceivedDataType::Video, 0, &vseq);
    feed(&mut scheduler, "adjacent", ReceivedDataType::Audio, 5, &aseq);
    feed(&mut scheduler, "adjacent", ReceivedDataType::Video, 100, &k1);
    feed(&mut scheduler, "adjacent", ReceivedDataType::Video, 133, &d1);
    feed(&mut scheduler, "adjacent", ReceivedDataType::Audio, 140, &a1);
    feed(&mut scheduler, "adjacent", ReceivedDataType::Video, 166, &d2);

    // Join: membership via the play path, burst via build_join_burst
    // (the unit-test idiom — accept_request needs a full client byte
    // exchange these tests don't run).
    play_with_stream_id(&mut scheduler, 2, "adjacent", 1);
    let client_id = *scheduler.connection_to_client_map.get(&2).unwrap();
    let mut burst = Vec::new();
    build_join_burst(
        scheduler.channels.get("adjacent").unwrap(),
        scheduler.clients.get_mut(client_id).unwrap(),
        2,
        1,
        0,
        &mut burst,
    );

    // Adjacent live media through the shared path.
    let mut live = Vec::new();
    live.extend(feed(
        &mut scheduler,
        "adjacent",
        ReceivedDataType::Video,
        200,
        &k2,
    ));
    live.extend(feed(
        &mut scheduler,
        "adjacent",
        ReceivedDataType::Audio,
        210,
        &a2,
    ));
    live.extend(feed(
        &mut scheduler,
        "adjacent",
        ReceivedDataType::Video,
        233,
        &d3,
    ));

    // The watcher's byte stream: burst then live, as its queue would
    // send them.
    let mut wire = Vec::new();
    for (bytes, _) in watcher_packets(&burst, 2)
        .into_iter()
        .chain(watcher_packets(&live, 2))
    {
        wire.extend_from_slice(&bytes);
    }

    let mut deserializer = ChunkDeserializer::new();
    deserializer
        .set_max_chunk_size(OUTBOUND_CHUNK_SIZE)
        .expect("pin the inbound chunk size");
    let messages = collect_messages(&mut deserializer, &wire);

    let expected: Vec<(u8, u32, &Bytes)> = vec![
        // Unconditional header prefix.
        (9, 0, &vseq),
        (8, 5, &aseq),
        // GOP segment replay: the pre-keyframe segment skips its video
        // (the cached vseq) but replays its audio — the aseq reaches a
        // joiner twice, as header and as cached frame, which is
        // wire-legal (the decoder just reapplies the config).
        (8, 5, &aseq),
        (9, 100, &k1),
        (9, 133, &d1),
        (8, 140, &a1),
        (9, 166, &d2),
        // Adjacent shared live.
        (9, 200, &k2),
        (8, 210, &a2),
        (9, 233, &d3),
    ];
    assert_eq!(
        messages.len(),
        expected.len(),
        "every burst and live message must survive the round-trip"
    );
    for (index, (message, (type_id, timestamp, payload))) in
        messages.iter().zip(expected.iter()).enumerate()
    {
        assert_eq!(message.type_id, *type_id, "message {index} type");
        assert_eq!(
            message.timestamp.value, *timestamp,
            "message {index} timestamp"
        );
        assert_eq!(message.message_stream_id, 1, "message {index} stream id");
        assert_eq!(&&message.data, payload, "message {index} payload");
    }
}

// Repeat-play golden — the hole the throwaway burst serializer closes:
// burst -> shared live -> second play -> second burst -> live, all on
// one connection, must stay decodable with exact timestamps. Under a
// session-serializer burst, the second burst would delta-compress
// against csid 4/5 history from the FIRST burst while the peer's csid
// state has long been replaced by the shared type-0 live stream —
// decoding garbage timestamps exactly where this asserts equality.
#[test]
fn second_play_after_shared_live_stays_wire_decodable() {
    let mut scheduler = RtmpScheduler::new(10);
    assert!(scheduler.new_channel("replay".to_string(), 100));

    let vseq = media_payload(0x17, 0x00, 0xE1, 16);
    let k1 = media_payload(0x17, 0x01, 0xE2, 64);
    let d1 = media_payload(0x27, 0x01, 0xE3, 48);
    let k2 = media_payload(0x17, 0x01, 0xE4, 64);
    let d2 = media_payload(0x27, 0x01, 0xE5, 48);
    let d3 = media_payload(0x27, 0x01, 0xE6, 48);

    feed(&mut scheduler, "replay", ReceivedDataType::Video, 0, &vseq);
    feed(&mut scheduler, "replay", ReceivedDataType::Video, 100, &k1);
    feed(&mut scheduler, "replay", ReceivedDataType::Video, 133, &d1);

    play_with_stream_id(&mut scheduler, 2, "replay", 1);
    let client_id = *scheduler.connection_to_client_map.get(&2).unwrap();
    let mut burst1 = Vec::new();
    build_join_burst(
        scheduler.channels.get("replay").unwrap(),
        scheduler.clients.get_mut(client_id).unwrap(),
        2,
        1,
        0,
        &mut burst1,
    );

    let mut live1 = Vec::new();
    live1.extend(feed(
        &mut scheduler,
        "replay",
        ReceivedDataType::Video,
        200,
        &k2,
    ));
    live1.extend(feed(
        &mut scheduler,
        "replay",
        ReceivedDataType::Video,
        233,
        &d2,
    ));

    // Second play on the SAME connection (same key: membership is kept,
    // the keyframe gate resets, and a real client gets a fresh burst).
    play_with_stream_id(&mut scheduler, 2, "replay", 1);
    let mut burst2 = Vec::new();
    build_join_burst(
        scheduler.channels.get("replay").unwrap(),
        scheduler.clients.get_mut(client_id).unwrap(),
        2,
        1,
        0,
        &mut burst2,
    );

    // The second burst must open csid 4 with a FULL (type 0) header —
    // the directly observable effect of the per-burst throwaway
    // serializer (a session serializer carrying first-burst history
    // would emit a compressed format here).
    let burst2_first_video = watcher_packets(&burst2, 2)
        .into_iter()
        .map(|(bytes, _)| bytes)
        .find(|bytes| !bytes.is_empty() && (bytes[0] & 0x3F) == 4)
        .expect("the second burst replays video on csid 4");
    assert_eq!(
        burst2_first_video[0] & 0xC0,
        0,
        "the second burst's first csid-4 chunk must be type 0"
    );

    let mut live2 = Vec::new();
    live2.extend(feed(
        &mut scheduler,
        "replay",
        ReceivedDataType::Video,
        266,
        &d3,
    ));

    let mut wire = Vec::new();
    for (bytes, _) in watcher_packets(&burst1, 2)
        .into_iter()
        .chain(watcher_packets(&live1, 2))
        .chain(watcher_packets(&burst2, 2))
        .chain(watcher_packets(&live2, 2))
    {
        wire.extend_from_slice(&bytes);
    }

    let mut deserializer = ChunkDeserializer::new();
    deserializer
        .set_max_chunk_size(OUTBOUND_CHUNK_SIZE)
        .expect("pin the inbound chunk size");
    let messages = collect_messages(&mut deserializer, &wire);

    let expected: Vec<(u8, u32, &Bytes)> = vec![
        // burst 1: cached header, then the open GOP (k1, d1)
        (9, 0, &vseq),
        (9, 100, &k1),
        (9, 133, &d1),
        // live 1
        (9, 200, &k2),
        (9, 233, &d2),
        // burst 2: header again, then the cache as of the second play
        (9, 0, &vseq),
        (9, 100, &k1),
        (9, 133, &d1),
        (9, 200, &k2),
        (9, 233, &d2),
        // live 2
        (9, 266, &d3),
    ];
    assert_eq!(
        messages.len(),
        expected.len(),
        "every message across both plays must survive the round-trip"
    );
    for (index, (message, (type_id, timestamp, payload))) in
        messages.iter().zip(expected.iter()).enumerate()
    {
        assert_eq!(message.type_id, *type_id, "message {index} type");
        assert_eq!(
            message.timestamp.value, *timestamp,
            "message {index} timestamp"
        );
        assert_eq!(&&message.data, payload, "message {index} payload");
    }
}

// Publisher-generation swap (the ABA rule): after publishing_ended and
// a new publisher under the same key, the fanout bytes must still be
// byte-identical to a pristine reference serializer — no csid header
// state may leak across generations into the shared serializer.
#[test]
fn publisher_generation_swap_keeps_fanout_bytes_pristine() {
    let mut scheduler = RtmpScheduler::new(10);
    assert!(scheduler.new_channel("aba".to_string(), 100));
    play_with_stream_id(&mut scheduler, 2, "aba", 1);

    // Generation 1 traffic seeds the shared serializer's history.
    let k_gen1 = media_payload(0x17, 0x01, 0xF1, 64);
    feed(&mut scheduler, "aba", ReceivedDataType::Video, 500, &k_gen1);

    // Give the test teeth: ordinary live history is all droppable and
    // forces type 0 by itself, so a deleted swap would go unnoticed.
    // Plant one NON-droppable csid-4 entry (same stream id) directly in
    // the generation-1 shared serializer — compressible history that
    // only a swapped-in fresh serializer cannot inherit. Without the
    // swap, generation 2's first video below would delta-compress
    // against this and the byte comparison fails.
    let planted = media_payload(0x27, 0x01, 0xF9, 32);
    serialize_media(
        &mut scheduler.channels.get_mut("aba").unwrap().fanout_serializer,
        ReceivedDataType::Video,
        1,
        planted,
        RtmpTimestamp { value: 600 },
        false,
    )
    .expect("plant non-droppable csid-4 history");

    // Publisher goes away; the lingering watcher keeps the channel.
    scheduler.publishing_ended("aba");
    assert!(scheduler.channels.contains_key("aba"));

    // Generation 2 reclaims the key.
    assert!(scheduler.new_channel("aba".to_string(), 101));
    let k_gen2 = media_payload(0x17, 0x01, 0xF2, 64);
    let results = feed(&mut scheduler, "aba", ReceivedDataType::Video, 40, &k_gen2);

    let packets = watcher_packets(&results, 2);
    assert_eq!(packets.len(), 1, "the lingering watcher gets gen-2 video");
    let expected = reference_media_bytes(
        &mut reference_serializer(),
        ReceivedDataType::Video,
        1,
        &k_gen2,
        40,
        true,
    );
    assert_same_bytes(&packets[0].0, &expected, "generation-2 first fanout packet");
}
