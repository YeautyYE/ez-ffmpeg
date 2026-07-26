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
    scheduler.publish_media_received(
        publisher_conn,
        tag_type,
        RtmpTimestamp { value: timestamp },
        Bytes::from_static(data),
    )
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
            } if *target_connection_id == connection_id => {
                Some((bytes.clone(), *can_be_dropped))
            }
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
use rml_rtmp::messages::MessagePayload;

fn media_payload(first: u8, second: u8, marker: u8, len: usize) -> Bytes {
    let mut v = vec![marker; len.max(3)];
    v[0] = first;
    v[1] = second;
    Bytes::from(v)
}
