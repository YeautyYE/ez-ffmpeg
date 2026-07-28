//! Shared fixtures and helpers for the scheduler unit tests; the tests
//! themselves live in the child modules, split by subject.

mod bypass;
mod fanout;
mod join_burst;
mod lifecycle;
mod oversized;
mod switching;
mod wire_parity;

use super::*;
use crate::rtmp::gop::FrameData;
use rml_rtmp::sessions::ServerSessionConfig;

fn play(scheduler: &mut RtmpScheduler, connection_id: usize, stream_key: &str) {
    let _ = scheduler.bytes_received(connection_id, &[]);
    let mut results = Vec::new();
    scheduler.handle_play_requested(
        connection_id,
        1,
        "app".to_string(),
        stream_key.to_string(),
        1,
        &mut results,
    );
}

fn feed_video(
    scheduler: &mut RtmpScheduler,
    stream_key: &str,
    timestamp: u32,
    data: &'static [u8],
) -> Vec<ServerResult> {
    let mut results = Vec::new();
    scheduler.handle_audio_video_data_received(
        stream_key,
        RtmpTimestamp { value: timestamp },
        Bytes::from_static(data),
        ReceivedDataType::Video,
        &mut results,
    );
    results
}

const KEYFRAME: &[u8] = &[0x17, 0x01, 0x00, 0x00, 0x00];
const DELTA: &[u8] = &[0x27, 0x01, 0x00, 0x00, 0x00];

fn feed_media(
    scheduler: &mut RtmpScheduler,
    publisher_conn: usize,
    tag_type: u8,
    timestamp: u32,
    data: &'static [u8],
) -> Vec<ServerResult> {
    let mut results = Vec::new();
    scheduler.publish_media_received(
        publisher_conn,
        tag_type,
        RtmpTimestamp { value: timestamp },
        Bytes::from_static(data),
        &mut results,
    );
    results
}

const VIDEO_SEQ: &[u8] = &[0x17, 0x00, 0x00, 0x00, 0x00, 0x01, 0x64];
const AUDIO_SEQ: &[u8] = &[0xaf, 0x00, 0x12, 0x10];
const AUDIO_FRAME: &[u8] = &[0xaf, 0x01, 0xDD, 0xEE];

fn make_watching_client(connection_id: usize, stream_key: &str, stream_id: u32) -> Client {
    let (session, _) = ServerSession::new(ServerSessionConfig::new()).expect("server session");
    Client {
        session,
        connection_id,
        current_action: ClientAction::Watching {
            stream_key: stream_key.to_string(),
            stream_id,
            // Detached fixture: these clients are handed straight to
            // `build_join_burst`, which never consults the action, so a
            // never-issued handle (generation 0) is fine.
            channel: ChannelHandle {
                index: 0,
                generation: 0,
            },
        },
        has_received_video_keyframe: false,
    }
}

fn video_frame(timestamp: u32, data: Bytes) -> FrameData {
    FrameData::Video {
        timestamp: RtmpTimestamp { value: timestamp },
        data,
    }
}

fn audio_frame(timestamp: u32, data: Bytes) -> FrameData {
    FrameData::Audio {
        timestamp: RtmpTimestamp { value: timestamp },
        data,
    }
}

/// (is_keyframe, is_sequence_header, is_video) per burst packet; panics on
/// any non-packet result so tests fail loudly on unexpected disconnects.
fn burst_flags(out: &[ServerResult]) -> Vec<(bool, bool, bool)> {
    out.iter()
        .map(|result| match result {
            ServerResult::OutboundPacket {
                is_keyframe,
                is_sequence_header,
                is_video,
                ..
            } => (*is_keyframe, *is_sequence_header, *is_video),
            other => panic!("expected OutboundPacket, got {:?}", other),
        })
        .collect()
}

/// Whether a burst packet's serialized chunk bytes carry `needle`.
/// Frame payloads start inside the first RTMP chunk, so a marker placed
/// in a frame's opening bytes is contiguous in the packet.
fn packet_contains(result: &ServerResult, needle: &[u8]) -> bool {
    match result {
        ServerResult::OutboundPacket { bytes, .. } => {
            bytes.windows(needle.len()).any(|w| w == needle)
        }
        _ => false,
    }
}

/// Total serialized wire bytes of a burst — the real number the write queue
/// would enqueue. Used to prove a burst stays under the Warning threshold.
fn serialized_burst_len(out: &[ServerResult]) -> usize {
    out.iter()
        .map(|result| match result {
            ServerResult::OutboundPacket { bytes, .. } => bytes.len(),
            _ => 0,
        })
        .sum()
}

/// The retired per-session path's shape: a fresh serializer pinned to
/// the 4096-byte outbound chunk size, driven exactly like rml's
/// `send_video_data`/`send_audio_data`. In droppable steady state the
/// old path's csid 4/5 history held only droppable entries, which force
/// type 0 exactly like a fresh serializer's first chunk, so this
/// reference reproduces the old path's bytes from the first message on.
fn reference_serializer() -> ChunkSerializer {
    let mut serializer = ChunkSerializer::new();
    serializer
        .set_max_chunk_size(OUTBOUND_CHUNK_SIZE as u32, RtmpTimestamp { value: 0 })
        .expect("4096 is a valid chunk size");
    serializer
}

fn reference_media_bytes(
    serializer: &mut ChunkSerializer,
    data_type: ReceivedDataType,
    stream_id: u32,
    data: &Bytes,
    timestamp: u32,
    can_be_dropped: bool,
) -> Vec<u8> {
    let message = match data_type {
        ReceivedDataType::Audio => RtmpMessage::AudioData { data: data.clone() },
        ReceivedDataType::Video => RtmpMessage::VideoData { data: data.clone() },
    };
    let payload = message
        .into_message_payload(RtmpTimestamp { value: timestamp }, stream_id)
        .expect("payload conversion");
    serializer
        .serialize(&payload, false, can_be_dropped)
        .expect("reference serialize")
        .bytes
}

/// The `(bytes, can_be_dropped)` of every OutboundPacket bound for
/// `connection_id`, in emission order.
fn watcher_packets(results: &[ServerResult], connection_id: usize) -> Vec<(Bytes, bool)> {
    results
        .iter()
        .filter_map(|result| match result {
            ServerResult::OutboundPacket {
                target_connection_id,
                bytes,
                can_be_dropped,
                ..
            } if *target_connection_id == connection_id => Some((bytes.clone(), *can_be_dropped)),
            _ => None,
        })
        .collect()
}

fn play_with_stream_id(
    scheduler: &mut RtmpScheduler,
    connection_id: usize,
    stream_key: &str,
    stream_id: u32,
) {
    let _ = scheduler.bytes_received(connection_id, &[]);
    let mut results = Vec::new();
    scheduler.handle_play_requested(
        connection_id,
        1,
        "app".to_string(),
        stream_key.to_string(),
        stream_id,
        &mut results,
    );
}

fn feed(
    scheduler: &mut RtmpScheduler,
    stream_key: &str,
    data_type: ReceivedDataType,
    timestamp: u32,
    data: &Bytes,
) -> Vec<ServerResult> {
    let mut results = Vec::new();
    scheduler.handle_audio_video_data_received(
        stream_key,
        RtmpTimestamp { value: timestamp },
        data.clone(),
        data_type,
        &mut results,
    );
    results
}

/// Byte-level comparison with a concise failure report (lengths and the
/// first divergent offset instead of a multi-KiB dump).
fn assert_same_bytes(actual: &[u8], expected: &[u8], what: &str) {
    if actual == expected {
        return;
    }
    let first_diff = actual
        .iter()
        .zip(expected.iter())
        .position(|(a, b)| a != b)
        .unwrap_or_else(|| actual.len().min(expected.len()));
    panic!(
        "{what}: wire bytes diverge (actual {} B, expected {} B, first diff at offset {})",
        actual.len(),
        expected.len(),
        first_diff
    );
}

fn collect_messages(deserializer: &mut ChunkDeserializer, bytes: &[u8]) -> Vec<MessagePayload> {
    let mut messages = Vec::new();
    let mut next = deserializer.get_next_message(bytes).expect("valid chunks");
    while let Some(payload) = next {
        messages.push(payload);
        next = deserializer
            .get_next_message(&[])
            .expect("valid buffered continuation");
    }
    messages
}

use rml_rtmp::chunk_io::ChunkDeserializer;
use rml_rtmp::messages::{MessagePayload, UserControlEventType};

fn media_payload(first: u8, second: u8, marker: u8, len: usize) -> Bytes {
    let mut v = vec![marker; len.max(3)];
    v[0] = first;
    v[1] = second;
    Bytes::from(v)
}

/// The socket-ingest entry appends into a caller-owned results buffer; the
/// reactor reuses ONE buffer for every batch, clearing it in between. Pin the
/// append+clear contract: a reused clear-between-batches buffer must yield
/// exactly the per-batch results of fresh per-batch Vecs. Packets are compared
/// at wire level (type, stream id, payload) because control-packet chunk
/// headers carry each session's own epoch-relative timestamp, which races the
/// wall clock across two sessions.
#[test]
fn reused_results_buffer_batches_match_fresh_vecs() {
    let ping_request = |echo: u32| {
        let payload = RtmpMessage::UserControl {
            event_type: UserControlEventType::PingRequest,
            stream_id: None,
            buffer_length: None,
            timestamp: Some(RtmpTimestamp { value: echo }),
        }
        .into_message_payload(RtmpTimestamp { value: 0 }, 0)
        .expect("ping payload");
        ChunkSerializer::new()
            .serialize(&payload, false, false)
            .expect("ping chunk")
            .bytes
    };
    // Session creation (the initial control burst), then two pings with
    // distinct echo timestamps: any leakage of one batch's entries into the
    // next would break the per-batch equality below.
    let batches = [Vec::new(), ping_request(1234), ping_request(5678)];

    let mut baseline = RtmpScheduler::new(10);
    let mut twin = RtmpScheduler::new(10);
    let mut reused = Vec::new();
    let mut baseline_parser = ChunkDeserializer::new();
    let mut twin_parser = ChunkDeserializer::new();
    for (index, batch) in batches.iter().enumerate() {
        let fresh = baseline.bytes_received(1, batch).expect("fresh-Vec batch");
        reused.clear();
        twin.bytes_received_with_backlog(1, batch, 0, &mut reused)
            .expect("reused-buffer batch");

        assert_eq!(
            burst_flags(&fresh),
            burst_flags(&reused),
            "batch {index}: result flags diverge"
        );
        let fresh_packets = watcher_packets(&fresh, 1);
        let reused_packets = watcher_packets(&reused, 1);
        assert_eq!(
            fresh_packets.len(),
            fresh.len(),
            "batch {index}: every result targets connection 1"
        );
        assert!(
            !fresh_packets.is_empty(),
            "batch {index} must produce packets for this pin to bite"
        );
        assert_eq!(
            fresh_packets.iter().map(|(_, d)| *d).collect::<Vec<_>>(),
            reused_packets.iter().map(|(_, d)| *d).collect::<Vec<_>>(),
            "batch {index}: droppable flags diverge"
        );

        let mut fresh_wire = Vec::new();
        for (bytes, _) in &fresh_packets {
            fresh_wire.extend_from_slice(bytes);
        }
        let mut reused_wire = Vec::new();
        for (bytes, _) in &reused_packets {
            reused_wire.extend_from_slice(bytes);
        }
        let fresh_messages = collect_messages(&mut baseline_parser, &fresh_wire);
        let reused_messages = collect_messages(&mut twin_parser, &reused_wire);
        assert_eq!(
            fresh_messages.len(),
            reused_messages.len(),
            "batch {index}: message counts diverge"
        );
        for (i, (a, b)) in fresh_messages
            .iter()
            .zip(reused_messages.iter())
            .enumerate()
        {
            assert_eq!(a.type_id, b.type_id, "batch {index} message {i}: type");
            assert_eq!(
                a.message_stream_id, b.message_stream_id,
                "batch {index} message {i}: stream id"
            );
            assert_eq!(a.data, b.data, "batch {index} message {i}: payload");
        }
    }
}
