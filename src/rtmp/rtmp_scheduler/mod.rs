//! The embedded RTMP server's single-threaded scheduler: session, channel
//! and watcher bookkeeping driven by the reactor thread.
//!
//! The type definitions and the shared serialization helpers live in this
//! module root so the submodules (and their tests) keep private-field
//! access; the behavior is split by topic:
//! * `entry` — reactor-facing entry points and connection lifecycle;
//! * `events` — session-result routing, control handlers and channel GC;
//! * `fanout` — the shared live A/V fanout and the keyframe gate;
//! * `join` — everything a late joiner receives (budget and burst build).

mod entry;
mod events;
mod fanout;
mod join;

#[cfg(test)]
mod tests;

use crate::flv::flv_tag_body::{is_audio_sequence_header, is_video_sequence_header};
use crate::rtmp::gop::Gops;
use bytes::Bytes;
use rml_rtmp::chunk_io::{ChunkSerializationError, ChunkSerializer, Packet};
use rml_rtmp::messages::{MessageSerializationError, RtmpMessage};
use rml_rtmp::sessions::StreamMetadata;
use rml_rtmp::sessions::{ServerSession, ServerSessionError, ServerSessionEvent};
use rml_rtmp::time::RtmpTimestamp;
use slab::Slab;
use std::collections::{HashMap, HashSet};
use std::rc::Rc;
use thiserror::Error;

/// Error type for RTMP scheduler operations
#[derive(Error, Debug)]
pub(super) enum SchedulerError {
    /// Error from RTMP session handling
    #[error("RTMP session error: {0}")]
    Session(#[from] ServerSessionError),

    /// A publisher sent a sequence header larger than the cacheable limit. A
    /// real AVC/AAC config is well under 1 KiB, so this is protocol abuse, not
    /// normal traffic. Returned from the ingest path to terminate the abusing
    /// publisher via the reactor's existing scheduler-error removal path.
    #[error("oversized sequence header: {size} bytes exceeds the {limit}-byte cacheable limit")]
    OversizedSequenceHeader { size: usize, limit: usize },
}

/// The server's negotiated OUTBOUND chunk size, in bytes. `new_channel` builds
/// every session from `ServerSessionConfig::new()`, whose `chunk_size` is 4096;
/// `ServerSession::new` pins the outbound serializer to it once at construction
/// (`set_max_chunk_size`) and the server never renegotiates the outbound
/// direction afterwards. A payload larger than this is split into continuation
/// chunks, each carrying its own (small) chunk header on the wire.
const OUTBOUND_CHUNK_SIZE: usize = 4096;

/// Largest sequence header (AVC/AAC codec config) the channel will cache and
/// replay. A real config is well under 1 KiB; 256 KiB is generous while staying
/// far below `write_queue::QUEUE_MAX_BYTES` (4 MiB). A header at or above the
/// queue-critical threshold, cached and then replayed in a join burst, would be
/// rejected by `WriteQueue::enqueue` and disconnect every late joiner — so an
/// oversized header is refused at ingest instead of poisoning the cache.
const MAX_CACHEABLE_SEQUENCE_HEADER_BYTES: usize = 256 * 1024;

/// Watcher-set tables at or below this capacity are never shrunk: iterating
/// them is already a handful of hashbrown control-group loads per tag, so
/// small audiences pay zero shrink bookkeeping.
const WATCHER_SET_SHRINK_MIN_CAPACITY: usize = 64;

enum ClientAction {
    Waiting,
    // Publishing to a stream key. `Rc<str>` because the in-process media path
    // clones the key once per audio/video tag (to end the borrow of `clients`
    // before mutating `channels`); a refcount bump there beats a per-tag String
    // allocation. The scheduler lives on the single reactor thread and already
    // holds `Rc<StreamMetadata>`, so `Rc` adds no new threading constraint.
    Publishing(Rc<str>),
    Watching { stream_key: String, stream_id: u32 },
}

#[derive(Clone, Copy)]
enum ReceivedDataType {
    Audio,
    Video,
}

struct Client {
    session: ServerSession,
    current_action: ClientAction,
    connection_id: usize,
    has_received_video_keyframe: bool,
}

impl Client {
    fn get_active_stream_id(&self) -> Option<u32> {
        match self.current_action {
            ClientAction::Waiting => None,
            ClientAction::Publishing(_) => None,
            ClientAction::Watching {
                stream_key: _,
                stream_id,
            } => Some(stream_id),
        }
    }
}

struct MediaChannel {
    publishing_client_id: Option<usize>,
    watching_client_ids: HashSet<usize>,
    metadata: Option<Rc<StreamMetadata>>,
    video_sequence_header: Option<Bytes>,
    video_timestamp: RtmpTimestamp,
    audio_sequence_header: Option<Bytes>,
    audio_timestamp: RtmpTimestamp,
    gops: Gops,
    /// The channel's shared live-media serializer: each fanned-out A/V
    /// message is serialized ONCE per (channel, `message_stream_id`) group
    /// here and the wire bytes are refcount-cloned per watcher, instead of
    /// re-running `ChunkSerializer::serialize` per watcher (an O(W x bytes)
    /// alloc+memcpy on the reactor thread).
    ///
    /// Wire-level soundness: everything through this serializer is media
    /// with `can_be_dropped = true`, and rml forces the chunk AFTER a
    /// droppable packet back to type 0 (serializer.rs, droppable branch of
    /// `add_chunk`), so every chunk it ever emits carries a full
    /// self-contained header — a first chunk (no previous header) is
    /// equally type 0. Video owns csid 4 and audio csid 5 exclusively
    /// (`get_csid_for_message_type`), and the per-session serializers no
    /// longer emit those csids once media is shared (the join burst uses a
    /// per-burst throwaway, see `build_join_burst`), so no header-
    /// compression chain ever spans two serializers.
    fanout_serializer: ChunkSerializer,
}

impl MediaChannel {
    fn new(gop_limit: usize) -> MediaChannel {
        Self {
            publishing_client_id: None,
            watching_client_ids: Default::default(),
            metadata: None,
            video_sequence_header: None,
            video_timestamp: RtmpTimestamp { value: 0 },
            audio_sequence_header: None,
            audio_timestamp: RtmpTimestamp { value: 0 },
            gops: Gops::new(gop_limit),
            fanout_serializer: new_media_serializer(),
        }
    }

    /// Check if channel should be removed (no publisher and no watchers)
    fn should_remove(&self) -> bool {
        self.publishing_client_id.is_none() && self.watching_client_ids.is_empty()
    }

    /// Opportunistic high-water shrink of the watcher set, called from the
    /// single membership-removal site (`play_ended`). std's `HashSet`
    /// iteration cost tracks table CAPACITY, not `len`, and the per-tag
    /// fanout walks this set once per media message — without a shrink, a
    /// flash crowd that decays leaves every later tag paying for the peak
    /// audience for as long as the channel lives. Quarter-occupancy trigger
    /// with a `len * 2` target: each shrink at least halves the table (a
    /// full decay costs O(peak) total, amortized O(1) per leave) and the 2x
    /// headroom keeps a modest rejoin from immediately regrowing it.
    fn shrink_watchers_if_sparse(&mut self) {
        let watchers = &mut self.watching_client_ids;
        if watchers.capacity() > WATCHER_SET_SHRINK_MIN_CAPACITY
            && watchers.len() < watchers.capacity() / 4
        {
            watchers.shrink_to(watchers.len() * 2);
        }
    }
}

#[derive(Debug)]
pub(super) enum ServerResult {
    DisconnectConnection {
        connection_id: usize,
    },
    OutboundPacket {
        target_connection_id: usize,
        /// Serialized RTMP chunk bytes. `Bytes` end-to-end so the shared
        /// fanout path hands every watcher a refcount clone of ONE
        /// serialization; per-session packets convert their `Vec` with the
        /// zero-copy `Bytes::from`.
        bytes: Bytes,
        /// rml's `Packet::can_be_dropped`, forwarded to the write queue's
        /// shedding policy.
        can_be_dropped: bool,
        is_keyframe: bool,
        is_sequence_header: bool,
        is_video: bool,
    },
}

impl ServerResult {
    /// An `OutboundPacket` from a per-session rml [`Packet`] (control
    /// responses, burst frames): moves the packet's `Vec` into `Bytes`
    /// without copying.
    fn outbound(
        target_connection_id: usize,
        packet: Packet,
        is_keyframe: bool,
        is_sequence_header: bool,
        is_video: bool,
    ) -> ServerResult {
        ServerResult::OutboundPacket {
            target_connection_id,
            bytes: Bytes::from(packet.bytes),
            can_be_dropped: packet.can_be_dropped,
            is_keyframe,
            is_sequence_header,
            is_video,
        }
    }
}

/// A media-path `ChunkSerializer` pinned to the server's outbound chunk
/// size: the shared per-channel live serializer and the per-burst throwaway
/// both use this. `ServerSession::new` already announced 4096 to every peer
/// (its construction-time SetChunkSize), and the outbound size is never
/// renegotiated, so the announcement packet this pinning produces is
/// redundant on the wire and deliberately discarded — the serializer only
/// needs the internal split size to match what peers were told.
fn new_media_serializer() -> ChunkSerializer {
    let mut serializer = ChunkSerializer::new();
    // Failure is impossible for the constant 4096 (the only error is a size
    // over i32::MAX); a silent fallback to the 128-byte default would split
    // chunks at a size the peers were never told, so refuse to continue.
    serializer
        .set_max_chunk_size(OUTBOUND_CHUNK_SIZE as u32, RtmpTimestamp { value: 0 })
        .expect("4096 is a valid outbound chunk size");
    serializer
}

/// Why a media serialize failed: the payload -> message conversion (cannot
/// happen for A/V data, which converts by identity) or the chunk
/// serialization itself (payload over the 16 MiB message cap).
#[derive(Debug)]
enum MediaSerializeError {
    Message(#[allow(dead_code)] MessageSerializationError),
    Chunk(#[allow(dead_code)] ChunkSerializationError),
}

/// Serialize one A/V message the way rml's `send_video_data` /
/// `send_audio_data` do, but on a caller-supplied serializer: the shared
/// channel serializer for the live fanout (droppable), or a per-burst
/// throwaway for the join replay (non-droppable).
fn serialize_media(
    serializer: &mut ChunkSerializer,
    data_type: ReceivedDataType,
    stream_id: u32,
    data: Bytes,
    timestamp: RtmpTimestamp,
    can_be_dropped: bool,
) -> Result<Packet, MediaSerializeError> {
    let message = match data_type {
        ReceivedDataType::Audio => RtmpMessage::AudioData { data },
        ReceivedDataType::Video => RtmpMessage::VideoData { data },
    };
    let payload = message
        .into_message_payload(timestamp, stream_id)
        .map_err(MediaSerializeError::Message)?;
    serializer
        .serialize(&payload, false, can_be_dropped)
        .map_err(MediaSerializeError::Chunk)
}

pub(super) struct RtmpScheduler {
    clients: Slab<Client>,
    connection_to_client_map: HashMap<usize, usize>,
    publisher_to_client_map: HashMap<usize, usize>,
    channels: HashMap<String, MediaChannel>,
    gop_limit: usize,
    /// Write-queue backlog of the connection whose input is currently being
    /// serviced, supplied by the reactor at the top of `bytes_received_with_backlog`.
    /// A `play` seeds its join-replay budget with this so a connection that still
    /// has undrained bytes (e.g. a rapid stream switch) does not overfill its queue
    /// into the frame-dropping Warning band. Zero outside a serviced `bytes_received`
    /// (handlers driven directly by tests see no phantom backlog).
    serving_connection_backlog_bytes: usize,
    /// Amortized-O(1) cursor for the same-batch join-replay prefix. All `play`s
    /// in one input batch share the serviced connection, so instead of rescanning
    /// the growing `server_results` per play (quadratic on a batch stuffed with
    /// repeated `play`s — a reachable reactor-stall), `advance_serving_prefix`
    /// folds only the entries added since the previous play into a running total.
    /// `serving_prefix_scan_pos` is how far it has scanned; `serving_prefix_bytes`
    /// is the accumulated bytes targeting the serviced connection. Both reset per
    /// batch.
    serving_prefix_scan_pos: usize,
    serving_prefix_bytes: usize,
}

/// Whether `data` is a sequence header (of `data_type`) too large to cache.
/// Only sequence headers are cached and replayed in the join burst, so only
/// they carry the "poison the cache and disconnect every late joiner" risk; a
/// large ordinary keyframe is left to the write queue's backpressure policy.
/// See [`MAX_CACHEABLE_SEQUENCE_HEADER_BYTES`].
fn is_oversized_sequence_header(data: &Bytes, data_type: ReceivedDataType) -> bool {
    let is_sequence_header = match data_type {
        ReceivedDataType::Video => is_video_sequence_header(data),
        ReceivedDataType::Audio => is_audio_sequence_header(data),
    };
    is_sequence_header && data.len() > MAX_CACHEABLE_SEQUENCE_HEADER_BYTES
}

/// The fatal `OversizedSequenceHeader` error for a raised media event whose
/// sequence-header data exceeds the cache cap, or `None`. Shared by the
/// per-event ingest gate in `handle_raised_event` and the publisher-batch
/// PRE-scan in `publish_bytes_received`, so both reject identically: the pre-scan
/// stops a fatal header from being applied only AFTER an earlier event in the
/// same batch (e.g. a `PublishStreamFinished`) already produced watcher side
/// effects that the abort would then discard, stranding the watcher's finish
/// status and forcing a double-finalize.
fn oversized_sequence_header_error(event: &ServerSessionEvent) -> Option<SchedulerError> {
    let (data, data_type) = match event {
        ServerSessionEvent::VideoDataReceived { data, .. } => (data, ReceivedDataType::Video),
        ServerSessionEvent::AudioDataReceived { data, .. } => (data, ReceivedDataType::Audio),
        _ => return None,
    };
    if is_oversized_sequence_header(data, data_type) {
        Some(SchedulerError::OversizedSequenceHeader {
            size: data.len(),
            limit: MAX_CACHEABLE_SEQUENCE_HEADER_BYTES,
        })
    } else {
        None
    }
}
