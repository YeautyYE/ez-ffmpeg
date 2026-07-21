use crate::flv::flv_tag_body::{
    is_audio_sequence_header, is_video_keyframe, is_video_sequence_header,
};
use crate::rtmp::gop::{FrameData, Gops};
use crate::rtmp::write_queue::QUEUE_WARN_BYTES;
use bytes::Bytes;
use log::{debug, warn};
use rml_rtmp::chunk_io::Packet;
use rml_rtmp::sessions::StreamMetadata;
use rml_rtmp::sessions::{
    ServerSession, ServerSessionConfig, ServerSessionError, ServerSessionEvent, ServerSessionResult,
};
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

/// Headroom subtracted from the write queue's Warning threshold when
/// budgeting a join burst. `WriteQueue::enqueue` classifies each item on the
/// pre-push total, so a burst that stays under Warning is accepted entirely
/// in the Normal band; the headroom additionally absorbs live frames that
/// land on the fresh queue in the same reactor round as the burst.
const JOIN_REPLAY_HEADROOM: usize = 64 * 1024;

/// Byte budget for the GOP segments of one join burst (sequence headers are
/// sent outside it). A joining watcher's queue is empty, so a burst within
/// this budget suffers zero policy drops. Without the cap, a large replay
/// pushes the queue into the Warning/High bands where correctly-flagged
/// delta frames — most of the burst — are dropped, and the watcher decodes
/// garbage from the surviving fragments.
const JOIN_REPLAY_BUDGET_BYTES: usize = QUEUE_WARN_BYTES - JOIN_REPLAY_HEADROOM;

/// The server's negotiated OUTBOUND chunk size, in bytes. `new_channel` builds
/// every session from `ServerSessionConfig::new()`, whose `chunk_size` is 4096;
/// `ServerSession::new` pins the outbound serializer to it once at construction
/// (`set_max_chunk_size`) and the server never renegotiates the outbound
/// direction afterwards. A payload larger than this is split into continuation
/// chunks, each carrying its own (small) chunk header on the wire.
const OUTBOUND_CHUNK_SIZE: usize = 4096;

/// Upper bound on a type-0 RTMP chunk header: 3-byte basic header + 11-byte
/// message header + 4-byte extended timestamp. Every media frame is serialized
/// as its own packet, and droppable A/V frames are forced back to a type-0
/// header by the serializer, so budget one full message header per frame.
const MSG_HEADER_MAX: usize = 18;

/// Upper bound on a type-3 continuation chunk header (basic header only). Added
/// once per outbound chunk a payload is split across. The join budget caps the
/// replayed payload well under 1 MiB, so the rare extended-timestamp bytes a
/// continuation may also carry stay far inside the 64 KiB headroom.
const CONT_HEADER_MAX: usize = 3;

/// Conservative upper bound on the serialized wire size of one GOP: its raw
/// payload plus the RTMP chunk framing the write queue actually enqueues. The
/// replay budget must count wire bytes, not raw payload — a burst of many small
/// frames (per-frame headers) or a large metadata/sequence-header prefix can
/// otherwise push the real queue past the Warning threshold, where correctly
/// flagged delta frames are dropped and the joiner decodes garbage.
///
/// `frame_count * MSG_HEADER_MAX` covers the per-frame message header;
/// `ceil(payload / OUTBOUND_CHUNK_SIZE) * CONT_HEADER_MAX` covers continuation
/// headers for payloads split across chunks. Saturating arithmetic keeps an
/// absurd/corrupt size from wrapping (the caller's `select_replay_start` then
/// fails that segment closed).
fn gop_wire_size(payload: usize, frame_count: usize) -> usize {
    let per_frame = frame_count.saturating_mul(MSG_HEADER_MAX);
    let continuation = payload
        .div_ceil(OUTBOUND_CHUNK_SIZE)
        .saturating_mul(CONT_HEADER_MAX);
    payload
        .saturating_add(per_frame)
        .saturating_add(continuation)
}

/// Largest sequence header (AVC/AAC codec config) the channel will cache and
/// replay. A real config is well under 1 KiB; 256 KiB is generous while staying
/// far below `write_queue::QUEUE_MAX_BYTES` (4 MiB). A header at or above the
/// queue-critical threshold, cached and then replayed in a join burst, would be
/// rejected by `WriteQueue::enqueue` and disconnect every late joiner — so an
/// oversized header is refused at ingest instead of poisoning the cache.
const MAX_CACHEABLE_SEQUENCE_HEADER_BYTES: usize = 256 * 1024;

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
        }
    }

    /// Check if channel should be removed (no publisher and no watchers)
    fn should_remove(&self) -> bool {
        self.publishing_client_id.is_none() && self.watching_client_ids.is_empty()
    }
}

#[derive(Debug)]
pub(super) enum ServerResult {
    DisconnectConnection {
        connection_id: usize,
    },
    OutboundPacket {
        target_connection_id: usize,
        packet: Packet,
        is_keyframe: bool,
        is_sequence_header: bool,
        is_video: bool,
    },
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

impl RtmpScheduler {
    pub(crate) fn new_channel(
        &mut self,
        stream_key: String,
        publisher_connection_id: usize,
    ) -> bool {
        match self.channels.get(&stream_key) {
            None => (),
            Some(channel) => match channel.publishing_client_id {
                None => (),
                Some(_) => {
                    warn!("Stream key '{}' already being published to", stream_key);
                    return false;
                }
            },
        }

        let config = ServerSessionConfig::new();
        let (session, _initial_session_results) = match ServerSession::new(config) {
            Ok(results) => results,
            Err(e) => {
                warn!("Rtmp error creating new server session: {}", e);
                return false;
            }
        };

        let client = Client {
            session,
            connection_id: publisher_connection_id,
            current_action: ClientAction::Publishing(Rc::from(stream_key.as_str())),
            has_received_video_keyframe: false,
        };

        let client_id = Some(self.clients.insert(client));
        self.publisher_to_client_map
            .insert(publisher_connection_id, client_id.unwrap());

        // Get or create channel and set publisher ownership
        let channel = self
            .channels
            .entry(stream_key)
            .or_insert_with(|| MediaChannel::new(self.gop_limit));
        channel.publishing_client_id = client_id;

        true
    }
}

impl RtmpScheduler {
    pub(super) fn new(gop_limit: usize) -> RtmpScheduler {
        RtmpScheduler {
            clients: Slab::with_capacity(1024),
            connection_to_client_map: HashMap::with_capacity(1024),
            publisher_to_client_map: HashMap::with_capacity(32),
            channels: HashMap::new(),
            gop_limit,
            serving_connection_backlog_bytes: 0,
            serving_prefix_scan_pos: 0,
            serving_prefix_bytes: 0,
        }
    }

    pub fn publish_bytes_received(
        &mut self,
        publisher_connection_id: usize,
        bytes: Vec<u8>,
    ) -> Result<Vec<ServerResult>, SchedulerError> {
        let mut server_results = Vec::new();

        if !self
            .publisher_to_client_map
            .contains_key(&publisher_connection_id)
        {
            warn!(
                "Publishing event for non-existent connection_id: {}",
                publisher_connection_id
            );
            return Ok(server_results);
        }

        let publisher_results = {
            let client_id = self
                .publisher_to_client_map
                .get(&publisher_connection_id)
                .unwrap();
            let client = self.clients.get_mut(*client_id).unwrap();
            let publisher_results: Vec<ServerSessionResult> =
                match client.session.handle_input(&bytes) {
                    Ok(results) => results,
                    Err(error) => return Err(error.into()),
                };
            publisher_results
        };

        // pre-scan the whole batch for a fatal oversized sequence header
        // BEFORE processing any event. Otherwise an earlier PublishStreamFinished
        // in the same batch would finalize its watchers, then the later fatal `?`
        // would discard those results — stranding the watcher's finish status and
        // forcing abort_publisher_watchers to double-finalize an already-Completed
        // watcher session. Rejecting up front keeps every watcher side effect out
        // of a batch that is going to abort (server_results stays empty here).
        for result in &publisher_results {
            if let ServerSessionResult::RaisedEvent(event) = result {
                if let Some(err) = oversized_sequence_header_error(event) {
                    return Err(err);
                }
            }
        }

        for result in publisher_results {
            match result {
                ServerSessionResult::OutboundResponse(_packet) => {
                    // debug!("Publisher can't receive data");
                }
                ServerSessionResult::RaisedEvent(event) => match event {
                    ServerSessionEvent::ClientChunkSizeChanged { .. }
                    | ServerSessionEvent::StreamMetadataChanged { .. }
                    | ServerSessionEvent::AudioDataReceived { .. }
                    | ServerSessionEvent::VideoDataReceived { .. }
                    | ServerSessionEvent::AcknowledgementReceived { .. }
                    | ServerSessionEvent::PingResponseReceived { .. }
                    | ServerSessionEvent::PublishStreamFinished { .. } => {
                        // `?` routes an oversized-sequence-header abort out of
                        // publish_bytes_received; the reactor then removes this
                        // misbehaving publisher (the existing abort path).
                        self.handle_raised_event(usize::MAX, event, &mut server_results)?;
                    }
                    ServerSessionEvent::ConnectionRequested {
                        request_id,
                        app_name: _,
                    } => {
                        let client_id = self
                            .publisher_to_client_map
                            .get(&publisher_connection_id)
                            .unwrap();
                        let client = self.clients.get_mut(*client_id).unwrap();
                        if let Err(e) = client.session.accept_request(request_id) {
                            warn!(
                                "Failed to accept connection request {}: {:?}",
                                request_id, e
                            );
                        }
                    }
                    ServerSessionEvent::PublishStreamRequested {
                        request_id,
                        app_name: _,
                        stream_key,
                        mode: _,
                    } => {
                        let client_id = self
                            .publisher_to_client_map
                            .get(&publisher_connection_id)
                            .unwrap();
                        let client = self.clients.get_mut(*client_id).unwrap();
                        if let Err(e) = client.session.accept_request(request_id) {
                            warn!(
                                "Failed to accept publish request {} for stream '{}': {:?}",
                                request_id, stream_key, e
                            );
                        }
                    }
                    _ => {
                        debug!("Publisher received unexpected event: {:?}", event);
                    }
                },

                x => warn!("Server result received: {:?}", x),
            }
        }

        Ok(server_results)
    }

    /// Direct in-process media ingest (PERF-5a serialize-bypass).
    ///
    /// An in-process publisher hands an already-parsed FLV audio/video tag
    /// straight to the channel machinery, skipping the serialize→channel→
    /// deserialize round-trip the socket path needs. The `(timestamp, data)`
    /// pair is byte-identical to what `flv_tag_to_message_payload` +
    /// `ChunkSerializer` + `handle_input` would reconstruct for the same tag,
    /// so this converges on the very same `handle_audio_video_data_received`
    /// the serialize path reaches — the scheduler observes an identical
    /// `FrameData` sequence (metadata / sequence headers / keyframe gate /
    /// GOP cache semantics are all unchanged).
    ///
    /// Only tag types `0x08` (audio) and `0x09` (video) are delivered here;
    /// metadata (`0x12`) and control messages stay on the byte path because
    /// they require AMF parsing / session state.
    pub(super) fn publish_media_received(
        &mut self,
        publisher_connection_id: usize,
        tag_type: u8,
        timestamp: RtmpTimestamp,
        data: Bytes,
    ) -> Vec<ServerResult> {
        let mut server_results = Vec::new();

        let data_type = match tag_type {
            0x08 => ReceivedDataType::Audio,
            0x09 => ReceivedDataType::Video,
            other => {
                // Only audio/video tags are bypassed; anything else is a
                // caller bug (metadata and control must stay on the byte path).
                warn!("In-process media bypass received unexpected FLV tag type {other:#04x}");
                return server_results;
            }
        };

        let client_id = match self.publisher_to_client_map.get(&publisher_connection_id) {
            Some(client_id) => *client_id,
            None => {
                warn!(
                    "In-process media for non-existent publisher connection_id: {}",
                    publisher_connection_id
                );
                return server_results;
            }
        };

        let stream_key = match self.clients.get(client_id) {
            Some(client) => match &client.current_action {
                ClientAction::Publishing(stream_key) => stream_key.clone(),
                _ => {
                    warn!(
                        "In-process media for a publisher not in the Publishing state: {}",
                        publisher_connection_id
                    );
                    return server_results;
                }
            },
            None => return server_results,
        };

        // The oversized-sequence-header bound (F2) lives on the untrusted socket
        // ingest path (`handle_raised_event`, driven by rml_rtmp deserialization
        // of remote bytes). This bypass carries only in-process FFmpeg muxer
        // output — a trusted source that never emits an oversized header — so it
        // needs no gate here (and, returning no Result, could not terminate the
        // feed anyway).
        self.handle_audio_video_data_received(
            &stream_key,
            timestamp,
            data,
            data_type,
            &mut server_results,
        );

        server_results
    }

    // The production reactor always supplies a real write-queue backlog via
    // bytes_received_with_backlog; only unit tests drive the scheduler bare.
    #[cfg_attr(not(test), allow(dead_code))]
    pub(super) fn bytes_received(
        &mut self,
        connection_id: usize,
        bytes: &[u8],
    ) -> Result<Vec<ServerResult>, SchedulerError> {
        self.bytes_received_with_backlog(connection_id, bytes, 0)
    }

    /// Like `bytes_received`, but told the connection's current write-queue
    /// backlog so a `play` handled in this batch can budget its join-replay burst
    /// against the bytes already queued ahead of it (see
    /// `serving_connection_backlog_bytes`). The reactor supplies the real value;
    /// the plain `bytes_received` wrapper passes 0.
    pub(super) fn bytes_received_with_backlog(
        &mut self,
        connection_id: usize,
        bytes: &[u8],
        connection_backlog_bytes: usize,
    ) -> Result<Vec<ServerResult>, SchedulerError> {
        self.serving_connection_backlog_bytes = connection_backlog_bytes;
        // Reset the same-batch join-replay prefix cursor for this input batch.
        self.serving_prefix_scan_pos = 0;
        self.serving_prefix_bytes = 0;
        let mut server_results = Vec::new();

        if !self.connection_to_client_map.contains_key(&connection_id) {
            let config = ServerSessionConfig::new();
            let (session, initial_session_results) = match ServerSession::new(config) {
                Ok(results) => results,
                Err(error) => return Err(error.into()),
            };

            self.handle_session_results(
                connection_id,
                initial_session_results,
                &mut server_results,
            );
            let client = Client {
                session,
                connection_id,
                current_action: ClientAction::Waiting,
                has_received_video_keyframe: false,
            };

            let client_id = Some(self.clients.insert(client));
            self.connection_to_client_map
                .insert(connection_id, client_id.unwrap());
        }

        let client_results: Vec<ServerSessionResult>;
        {
            let client_id = self.connection_to_client_map.get(&connection_id).unwrap();
            let client = self.clients.get_mut(*client_id).unwrap();
            client_results = match client.session.handle_input(bytes) {
                Ok(results) => results,
                Err(error) => return Err(error.into()),
            };
        }

        self.handle_session_results(connection_id, client_results, &mut server_results);
        Ok(server_results)
    }

    pub(super) fn notify_connection_closed(&mut self, connection_id: usize) {
        match self.connection_to_client_map.remove(&connection_id) {
            None => (),
            Some(client_id) => {
                let client = self.clients.remove(client_id);
                match client.current_action {
                    ClientAction::Watching {
                        stream_key,
                        stream_id: _,
                    } => self.play_ended(client_id, stream_key),
                    ClientAction::Waiting => (),
                    _ => {}
                }
            }
        }
    }

    pub(super) fn notify_publisher_closed(&mut self, publisher_connection_id: usize) {
        match self
            .publisher_to_client_map
            .remove(&publisher_connection_id)
        {
            None => (),
            Some(client_id) => {
                let client = self.clients.remove(client_id);
                match client.current_action {
                    ClientAction::Publishing(stream_key) => self.publishing_ended(&stream_key),
                    _ => {}
                }
            }
        }
    }

    /// Finalize the watchers of a publisher that must be torn down due to a fatal
    /// protocol error (e.g. an oversized sequence header rejected at ingest).
    /// Unlike a graceful `deleteStream`, this does NOT re-feed the publisher's
    /// session (which just errored on the untrusted byte path); it ends every
    /// watcher of the publisher's channel exactly as `handle_publish_finished`
    /// does — a final `finish_playing` status plus a Disconnect — so no watcher is
    /// left orphaned in `Watching` with a stale keyframe gate when a new publisher
    /// later reclaims the same stream key. The caller must still invoke
    /// `notify_publisher_closed` afterward to release the publisher-scoped state.
    pub(super) fn abort_publisher_watchers(
        &mut self,
        publisher_connection_id: usize,
    ) -> Vec<ServerResult> {
        let mut server_results = Vec::new();
        let stream_key = match self
            .publisher_to_client_map
            .get(&publisher_connection_id)
            .and_then(|client_id| self.clients.get(*client_id))
        {
            Some(client) => match &client.current_action {
                ClientAction::Publishing(stream_key) => stream_key.clone(),
                _ => return server_results,
            },
            None => return server_results,
        };
        self.handle_publish_finished(String::new(), &stream_key, &mut server_results);
        server_results
    }

    fn handle_session_results(
        &mut self,
        executed_connection_id: usize,
        session_results: Vec<ServerSessionResult>,
        server_results: &mut Vec<ServerResult>,
    ) {
        for result in session_results {
            match result {
                ServerSessionResult::OutboundResponse(packet) => {
                    // Control message, not audio/video data
                    server_results.push(ServerResult::OutboundPacket {
                        target_connection_id: executed_connection_id,
                        packet,
                        is_keyframe: false,
                        is_sequence_header: false,
                        is_video: false,
                    })
                }

                ServerSessionResult::RaisedEvent(event) => {
                    // A media event never reaches this watcher-side path, but if
                    // one ever did and tripped the oversized-header gate, drop
                    // that connection rather than swallow the abort.
                    if let Err(e) =
                        self.handle_raised_event(executed_connection_id, event, server_results)
                    {
                        debug!(
                            "Rtmp connection {} aborted during event handling: {}",
                            executed_connection_id, e
                        );
                        server_results.push(ServerResult::DisconnectConnection {
                            connection_id: executed_connection_id,
                        });
                    }
                }

                x => debug!("Server result received: {:?}", x),
            }
        }
    }

    fn handle_raised_event(
        &mut self,
        executed_connection_id: usize,
        event: ServerSessionEvent,
        server_results: &mut Vec<ServerResult>,
    ) -> Result<(), SchedulerError> {
        // Ingest bound (F2): reject a fatal oversized sequence header BEFORE
        // dispatching the event, so no watcher side effect is applied for a batch
        // that is going to abort. The publisher byte path additionally pre-scans
        // the whole batch, so this is also its belt-and-braces gate.
        if let Some(err) = oversized_sequence_header_error(&event) {
            return Err(err);
        }
        match event {
            ServerSessionEvent::ConnectionRequested {
                request_id,
                app_name,
            } => {
                self.handle_connection_requested(
                    executed_connection_id,
                    request_id,
                    app_name,
                    server_results,
                );
            }

            ServerSessionEvent::PublishStreamRequested {
                request_id,
                app_name,
                stream_key,
                mode: _,
            } => {
                self.handle_publish_requested(
                    executed_connection_id,
                    request_id,
                    app_name,
                    stream_key,
                    server_results,
                );
            }

            ServerSessionEvent::PublishStreamFinished {
                app_name,
                stream_key,
            } => {
                self.handle_publish_finished(app_name, &stream_key, server_results);
            }

            ServerSessionEvent::PlayStreamRequested {
                request_id,
                app_name,
                stream_key,
                start_at: _,
                duration: _,
                reset: _,
                stream_id,
            } => {
                self.handle_play_requested(
                    executed_connection_id,
                    request_id,
                    app_name,
                    stream_key,
                    stream_id,
                    server_results,
                );
            }

            ServerSessionEvent::PlayStreamFinished {
                app_name,
                stream_key,
            } => {
                self.handle_play_finished(executed_connection_id, app_name, stream_key);
            }

            ServerSessionEvent::StreamMetadataChanged {
                app_name,
                stream_key,
                metadata,
            } => {
                self.handle_metadata_received(app_name, stream_key, metadata, server_results);
            }

            ServerSessionEvent::VideoDataReceived {
                app_name: _,
                stream_key,
                data,
                timestamp,
            } => {
                // The oversized-video-sequence-header ingest bound (F2) — caching
                // one would push every late joiner's burst past the 4 MiB
                // queue-critical threshold — is enforced at the top of this
                // function via oversized_sequence_header_error.
                self.handle_audio_video_data_received(
                    &stream_key,
                    timestamp,
                    data,
                    ReceivedDataType::Video,
                    server_results,
                );
            }

            ServerSessionEvent::AudioDataReceived {
                app_name: _,
                stream_key,
                data,
                timestamp,
            } => {
                // Same ingest bound for an oversized audio sequence header,
                // enforced at the top via oversized_sequence_header_error.
                self.handle_audio_video_data_received(
                    &stream_key,
                    timestamp,
                    data,
                    ReceivedDataType::Audio,
                    server_results,
                );
            }

            _ => debug!(
                "Rtmp event raised by connection {executed_connection_id}: {:?}",
                event
            ),
        }
        Ok(())
    }

    fn handle_connection_requested(
        &mut self,
        requested_connection_id: usize,
        request_id: u32,
        app_name: String,
        server_results: &mut Vec<ServerResult>,
    ) {
        debug!(
            "Rtmp connection {requested_connection_id} requested connection to app '{app_name}'"
        );

        let accept_result;
        {
            let client_id = self
                .connection_to_client_map
                .get(&requested_connection_id)
                .unwrap();
            let client = self.clients.get_mut(*client_id).unwrap();
            accept_result = client.session.accept_request(request_id);
        }

        match accept_result {
            Err(error) => {
                debug!(
                    "Rtmp client error occurred accepting connection request: {:?}",
                    error
                );
                server_results.push(ServerResult::DisconnectConnection {
                    connection_id: requested_connection_id,
                })
            }

            Ok(results) => {
                self.handle_session_results(requested_connection_id, results, server_results);
            }
        }
    }

    fn handle_publish_requested(
        &mut self,
        requested_connection_id: usize,
        _request_id: u32,
        _app_name: String,
        _stream_key: String,
        server_results: &mut Vec<ServerResult>,
    ) {
        warn!("Rtmp publish requested, but socket-based push is not supported.");
        server_results.push(ServerResult::DisconnectConnection {
            connection_id: requested_connection_id,
        });
    }

    fn handle_publish_finished(
        &mut self,
        app_name: String,
        stream_key: &str,
        server_results: &mut Vec<ServerResult>,
    ) {
        debug!("Rtmp publish finished on app '{app_name}' and stream key '{stream_key}'");

        let channel = match self.channels.get(stream_key) {
            Some(channel) => channel,
            None => return,
        };

        for client_id in &channel.watching_client_ids {
            let client = match self.clients.get_mut(*client_id) {
                Some(client) => client,
                None => continue,
            };
            let active_stream_id = match client.get_active_stream_id() {
                Some(stream_id) => stream_id,
                None => continue,
            };

            match client.session.finish_playing(active_stream_id) {
                Ok(packet) => {
                    // Control message, not audio/video data
                    server_results.push(ServerResult::OutboundPacket {
                        target_connection_id: client.connection_id,
                        packet,
                        is_keyframe: false,
                        is_sequence_header: false,
                        is_video: false,
                    });
                }
                Err(error) => {
                    warn!(
                        "Error sending stream end to client on connection id {}: {:?}",
                        client.connection_id, error
                    );
                }
            }
            server_results.push(ServerResult::DisconnectConnection {
                connection_id: client.connection_id,
            });
        }
    }

    /// Fold the `server_results` entries added since the previous `play` in this
    /// batch into the running same-batch prefix for `target`, advancing the scan
    /// cursor. Every entry is visited at most once across all plays in a batch
    /// (all of which share the serviced connection), so N repeated plays cost
    /// O(N) total rather than the O(N^2) of rescanning the whole vec each time.
    /// Saturating so a pathological batch cannot wrap the accumulator.
    fn advance_serving_prefix(&mut self, server_results: &[ServerResult], target: usize) -> usize {
        while self.serving_prefix_scan_pos < server_results.len() {
            if let ServerResult::OutboundPacket {
                target_connection_id,
                packet,
                ..
            } = &server_results[self.serving_prefix_scan_pos]
            {
                if *target_connection_id == target {
                    self.serving_prefix_bytes =
                        self.serving_prefix_bytes.saturating_add(packet.bytes.len());
                }
            }
            self.serving_prefix_scan_pos += 1;
        }
        self.serving_prefix_bytes
    }

    fn handle_play_requested(
        &mut self,
        requested_connection_id: usize,
        request_id: u32,
        app_name: String,
        stream_key: String,
        stream_id: u32,
        server_results: &mut Vec<ServerResult>,
    ) {
        debug!("Rtmp play requested on app '{app_name}' and stream key '{stream_key}'");

        // A connection that switches streams with a second `play` must leave
        // the previous channel's watcher set before its action is overwritten:
        // otherwise the old channel's fanout keeps delivering frames to it,
        // and the old stream's live IDR could wrongly re-open the keyframe
        // gate for the new stream.
        let previous_watch = {
            let client_id = self
                .connection_to_client_map
                .get(&requested_connection_id)
                .unwrap();
            let client = self.clients.get(*client_id).unwrap();
            match &client.current_action {
                ClientAction::Watching {
                    stream_key: old_stream_key,
                    ..
                } if *old_stream_key != stream_key => Some((*client_id, old_stream_key.clone())),
                _ => None,
            }
        };
        if let Some((client_id, old_stream_key)) = previous_watch {
            // Removes the membership and GCs the old channel if it is now
            // empty and unpublished (same idiom as a connection close).
            self.play_ended(client_id, old_stream_key);
        }

        // Two phases: (1) register the watcher and accept the request while
        // the client/channel borrows are alive, building the media burst as
        // pre-flagged ServerResults; (2) run the accept's control results
        // through handle_session_results (which needs &mut self) and only
        // then append the burst, preserving control-before-media ordering.
        // The old single-phase shape funneled the burst through the control
        // path, which stamped every replayed frame is_video:false — the write
        // queue then applied the wrong backpressure tier to the entire replay.
        // Captured before the borrow block (both touch `self`, which the
        // client/channel borrows below would conflict with):
        // - the connection's current write-queue backlog, and
        // - the same-batch prefix already targeting this watcher, folded in
        //   amortized-O(1) (only entries added since the previous play) so a batch
        //   of repeated plays stays linear rather than rescanning quadratically.
        let serving_backlog = self.serving_connection_backlog_bytes;
        let same_batch_prefix =
            self.advance_serving_prefix(server_results.as_slice(), requested_connection_id);
        let accept_result;
        let mut join_burst = Vec::new();
        {
            let client_id = self
                .connection_to_client_map
                .get(&requested_connection_id)
                .unwrap();
            let client = self.clients.get_mut(*client_id).unwrap();
            client.current_action = ClientAction::Watching {
                stream_key: stream_key.clone(),
                stream_id,
            };
            // Reset the keyframe gate for this play request: has_received_video_keyframe
            // is persistent client state, so a connection that previously watched
            // another stream (and saw its IDR) must not carry that True over and let
            // should_send_to_watcher forward this stream's delta frames before an IDR
            // is replayed or arrives live. Replay of a GOP with
            // a real IDR, or a later live IDR, re-sets it.
            client.has_received_video_keyframe = false;

            let channel = self
                .channels
                .entry(stream_key.clone())
                .or_insert_with(|| MediaChannel::new(self.gop_limit));

            channel.watching_client_ids.insert(*client_id);
            accept_result = client.session.accept_request(request_id);
            if let Ok(ref accept_results) = accept_result {
                // The play-accept control packets (Stream Begin,
                // NetStream.Play.Reset/Start, |RtmpSampleAccess, ...) are
                // enqueued to THIS watcher's queue BEFORE the burst. Play.Start
                // echoes the stream key, and a legal key can be ~65 KiB, so one
                // accept packet alone can rival the 64 KiB headroom. Charge
                // their real serialized size against the join budget so a
                // near-full replay plus a fat accept prefix stays under the
                // Warning threshold instead of dropping delta frames.
                // Bytes already queued ahead of the replay burst: the connection's
                // existing backlog, the packets emitted earlier in THIS input batch
                // that target this watcher (e.g. a createStream flood just before the
                // play), and this play's own accept-control packets. Charging all
                // three keeps a near-full GOP under the Warning threshold instead of
                // shedding delta frames.
                let accept_prefix_bytes =
                    join_replay_prefix_bytes(serving_backlog, same_batch_prefix, accept_results);
                build_join_burst(
                    channel,
                    client,
                    requested_connection_id,
                    stream_id,
                    accept_prefix_bytes,
                    &mut join_burst,
                );
            }
        }

        match accept_result {
            Err(error) => {
                debug!(
                    "Rtmp client error occurred accepting playback request: {:?}",
                    error
                );
                server_results.push(ServerResult::DisconnectConnection {
                    connection_id: requested_connection_id,
                });

                return;
            }

            Ok(results) => {
                self.handle_session_results(requested_connection_id, results, server_results);
                // A burst that failed mid-build ends with its own
                // DisconnectConnection, so appending it verbatim keeps the
                // error recovery of the old inline path.
                server_results.extend(join_burst);
            }
        }
    }

    /// A watcher stopped its play (`closeStream`/`deleteStream`) without
    /// dropping the connection. Leave the watched channel so its fanout stops
    /// targeting this client, reset the play state, and GC the channel if it
    /// is now empty and unpublished. Connection close only cleans the current
    /// action, so an unhandled finish would leak the membership forever.
    ///
    /// Matched by `stream_key` alone: `ServerSessionEvent::PlayStreamFinished`
    /// carries no `stream_id` (rml_rtmp 0.8 only reports `app_name` +
    /// `stream_key`), and this scheduler models a single active play per
    /// connection via one `current_action`. A same-key replay therefore
    /// collapses to one membership, so stopping the connection's current play
    /// when its key finishes is the only representable — and correct —
    /// behaviour for the model. Multiple concurrent plays of the same key on
    /// one connection are not modelled and cannot be disambiguated here.
    fn handle_play_finished(
        &mut self,
        finished_connection_id: usize,
        app_name: String,
        stream_key: String,
    ) {
        debug!("Rtmp play finished on app '{app_name}' and stream key '{stream_key}'");

        let client_id = match self.connection_to_client_map.get(&finished_connection_id) {
            Some(client_id) => *client_id,
            None => return,
        };
        let client = match self.clients.get_mut(client_id) {
            Some(client) => client,
            None => return,
        };

        let is_watching_finished_stream = matches!(
            &client.current_action,
            ClientAction::Watching {
                stream_key: watched_stream_key,
                ..
            } if *watched_stream_key == stream_key
        );
        if !is_watching_finished_stream {
            debug!(
                "Rtmp connection {finished_connection_id} finished playing '{stream_key}' \
                 which it is not currently watching; ignoring"
            );
            return;
        }

        client.current_action = ClientAction::Waiting;
        client.has_received_video_keyframe = false;
        self.play_ended(client_id, stream_key);
    }

    fn handle_metadata_received(
        &mut self,
        app_name: String,
        stream_key: String,
        metadata: StreamMetadata,
        server_results: &mut Vec<ServerResult>,
    ) {
        debug!("Rtmp new metadata received for app '{app_name}' and stream key '{stream_key}'");
        let channel = match self.channels.get_mut(&stream_key) {
            Some(channel) => channel,
            None => return,
        };

        let metadata = Rc::new(metadata);
        channel.metadata = Some(metadata.clone());

        // Send the metadata to all current watchers
        for client_id in &channel.watching_client_ids {
            let client = match self.clients.get_mut(*client_id) {
                Some(client) => client,
                None => continue,
            };

            let active_stream_id = match client.get_active_stream_id() {
                Some(stream_id) => stream_id,
                None => continue,
            };

            match client.session.send_metadata(active_stream_id, &metadata) {
                Ok(packet) => {
                    // Metadata message, not audio/video frame data
                    server_results.push(ServerResult::OutboundPacket {
                        target_connection_id: client.connection_id,
                        packet,
                        is_keyframe: false,
                        is_sequence_header: false,
                        is_video: false,
                    });
                }

                Err(error) => {
                    debug!(
                        "Rtmp error sending metadata to client on connection id {}: {:?}",
                        client.connection_id, error
                    );
                    server_results.push(ServerResult::DisconnectConnection {
                        connection_id: client.connection_id,
                    });
                }
            }
        }
    }

    fn handle_audio_video_data_received(
        &mut self,
        stream_key: &str,
        timestamp: RtmpTimestamp,
        data: Bytes,
        data_type: ReceivedDataType,
        server_results: &mut Vec<ServerResult>,
    ) {
        let channel = match self.channels.get_mut(stream_key) {
            Some(channel) => channel,
            None => return,
        };

        // Pre-compute flags once to avoid repeated calls in hot path
        let is_video = matches!(data_type, ReceivedDataType::Video);
        let (is_keyframe, is_sequence_header) = if is_video {
            (is_video_keyframe(&data), is_video_sequence_header(&data))
        } else {
            (false, is_audio_sequence_header(&data))
        };

        // If this is an audio or video sequence header we need to save it, so it can be
        // distributed to any late coming watchers
        match data_type {
            ReceivedDataType::Video => {
                if is_sequence_header {
                    // A codec-config change (a video sequence header whose bytes
                    // differ from the cached one) invalidates every cached GOP:
                    // they were encoded under the OLD header, and a late joiner
                    // that gets the NEW header followed by those OLD-config
                    // frames decodes garbage. Drop the stale history so replay
                    // restarts from the new-config GOPs. A byte-identical resend
                    // (the common keepalive case) leaves the cache untouched.
                    if channel
                        .video_sequence_header
                        .as_ref()
                        .is_some_and(|cached| cached != &data)
                    {
                        channel.gops.clear();
                    }
                    channel.video_sequence_header = Some(data.clone());
                    channel.video_timestamp = timestamp;
                }
                channel.gops.save_frame_data(
                    crate::rtmp::gop::FrameData::Video {
                        timestamp,
                        data: data.clone(),
                    },
                    is_keyframe,
                );
            }

            ReceivedDataType::Audio => {
                if is_sequence_header {
                    // Same reasoning as the video path. Audio frames share this
                    // channel's single GOP cache, so an audio codec change also
                    // clears the cached video GOPs. That is safe (a joiner just
                    // replays fewer frames and catches up on the next live IDR)
                    // and audio-config changes are rare; correctness over a
                    // marginally longer catch-up.
                    if channel
                        .audio_sequence_header
                        .as_ref()
                        .is_some_and(|cached| cached != &data)
                    {
                        channel.gops.clear();
                    }
                    channel.audio_sequence_header = Some(data.clone());
                    channel.audio_timestamp = timestamp;
                }
                channel.gops.save_frame_data(
                    crate::rtmp::gop::FrameData::Audio {
                        timestamp,
                        data: data.clone(),
                    },
                    false,
                );
            }
        }

        // Detect an audio-only stream from the publisher's metadata: it declares
        // an audio codec but no video codec. Computed once per received packet so
        // the per-watcher gate can deliver audio for such streams instead of
        // waiting for a video keyframe that never comes (see
        // `should_send_to_watcher`).
        let channel_is_audio_only = channel
            .metadata
            .as_ref()
            .is_some_and(|m| m.audio_codec_id.is_some() && m.video_codec_id.is_none());

        for client_id in &channel.watching_client_ids {
            let client = match self.clients.get_mut(*client_id) {
                Some(client) => client,
                None => continue,
            };

            // Defense-in-depth: a watcher whose current action points at a
            // different stream must not receive this channel's frames, even
            // if its id is still (incorrectly) present in this watcher set.
            let active_stream_id = match &client.current_action {
                ClientAction::Watching {
                    stream_key: watched_stream_key,
                    stream_id,
                } => {
                    if *watched_stream_key != stream_key {
                        debug!(
                            "Rtmp client {} is watching '{}'; skipping frame delivery \
                             from channel '{}'",
                            client_id, watched_stream_key, stream_key
                        );
                        continue;
                    }
                    *stream_id
                }
                _ => continue,
            };

            let should_send_to_client = should_send_to_watcher(
                data_type,
                client.has_received_video_keyframe,
                is_keyframe,
                is_sequence_header,
                channel_is_audio_only,
            );

            if !should_send_to_client {
                continue;
            }

            let send_result = match data_type {
                ReceivedDataType::Audio => {
                    client
                        .session
                        .send_audio_data(active_stream_id, data.clone(), timestamp, true)
                }
                ReceivedDataType::Video => {
                    if is_keyframe {
                        client.has_received_video_keyframe = true;
                    }

                    client
                        .session
                        .send_video_data(active_stream_id, data.clone(), timestamp, true)
                }
            };

            match send_result {
                Ok(packet) => {
                    server_results.push(ServerResult::OutboundPacket {
                        target_connection_id: client.connection_id,
                        packet,
                        is_keyframe,
                        is_sequence_header,
                        is_video,
                    });
                }

                Err(error) => {
                    let data_type_str = if is_video { "video" } else { "audio" };
                    debug!(
                        "Rtmp error sending {} data to client on connection id {}: {:?}",
                        data_type_str, client.connection_id, error
                    );
                    server_results.push(ServerResult::DisconnectConnection {
                        connection_id: client.connection_id,
                    });
                }
            }
        }
    }

    fn publishing_ended(&mut self, stream_key: &str) {
        let should_remove = if let Some(channel) = self.channels.get_mut(stream_key) {
            // Reset the FULL publisher-scoped state, not just metadata. During
            // the close linger the channel outlives its publisher (lingering
            // watchers keep it), yet `stream_keys` is released at once, so a NEW
            // publisher can reclaim the same key immediately. If it reuses the
            // same sequence header, the clear-on-change in
            // `handle_audio_video_data_received` never fires — without this, a
            // fresh joiner would replay the PREVIOUS session's cached GOPs and
            // the new publisher's first IDR would freeze the old open GOP,
            // mixing two sessions. Watchers already mid-delivery are unaffected:
            // `build_join_burst` snapshots at join time, so a cleared cache only
            // means a FUTURE joiner replays fewer frames.
            channel.publishing_client_id = None;
            channel.metadata = None;
            channel.video_sequence_header = None;
            channel.audio_sequence_header = None;
            channel.video_timestamp = RtmpTimestamp { value: 0 };
            channel.audio_timestamp = RtmpTimestamp { value: 0 };
            channel.gops.clear();
            channel.should_remove()
        } else {
            return;
        };
        if should_remove {
            self.channels.remove(stream_key);
        }
    }

    fn play_ended(&mut self, client_id: usize, stream_key: String) {
        let should_remove = if let Some(channel) = self.channels.get_mut(&stream_key) {
            channel.watching_client_ids.remove(&client_id);
            channel.should_remove()
        } else {
            return;
        };
        if should_remove {
            self.channels.remove(&stream_key);
        }
    }

    /// The cached video sequence header for a channel, if any (test only).
    #[cfg(test)]
    pub(crate) fn channel_video_sequence_header(&self, stream_key: &str) -> Option<Bytes> {
        self.channels
            .get(stream_key)
            .and_then(|c| c.video_sequence_header.clone())
    }

    /// The cached audio sequence header for a channel, if any (test only).
    #[cfg(test)]
    pub(crate) fn channel_audio_sequence_header(&self, stream_key: &str) -> Option<Bytes> {
        self.channels
            .get(stream_key)
            .and_then(|c| c.audio_sequence_header.clone())
    }

    /// The number of frozen GOPs cached for a channel (test only).
    #[cfg(test)]
    pub(crate) fn channel_frozen_gop_count(&self, stream_key: &str) -> usize {
        self.channels
            .get(stream_key)
            .map(|c| c.gops.frozen_count())
            .unwrap_or(0)
    }
}

/// Decide whether a freshly received media packet should be forwarded to a
/// watching client right now.
///
/// Video is gated on the client already having a keyframe to start decoding
/// from (the packet itself passes if it *is* that keyframe or a sequence
/// header). Audio is normally gated the same way so that A/V playback starts in
/// sync — but that coupling is only correct when the stream actually carries
/// video. An audio-only stream never produces a video keyframe, so gating its
/// audio on one leaves every subscriber permanently silent.
///
/// We only relax audio delivery when the publisher's metadata positively
/// declares the stream audio-only (`channel_is_audio_only`); absent that
/// signal we keep gating on a keyframe, which preserves A/V start-up sync even
/// during the window before the first video packet arrives. (An audio-only
/// stream that publishes no metadata at all is the one residual case still
/// gated — rare, since real audio-only publishers send `onMetaData`.)
fn should_send_to_watcher(
    data_type: ReceivedDataType,
    has_received_video_keyframe: bool,
    is_keyframe: bool,
    is_sequence_header: bool,
    channel_is_audio_only: bool,
) -> bool {
    match data_type {
        ReceivedDataType::Video => has_received_video_keyframe || is_sequence_header || is_keyframe,
        ReceivedDataType::Audio => {
            has_received_video_keyframe || is_sequence_header || channel_is_audio_only
        }
    }
}

/// The index of the first GOP segment to replay to a joining watcher: the
/// longest suffix of `sizes` whose byte total stays within `budget`. Whole
/// segments only — a GOP entered mid-way hands the watcher delta frames whose
/// IDR was trimmed away, which decodes as a smeared picture.
///
/// The accumulation uses `checked_add` so a corrupt or absurd segment size
/// fails closed (older segments dropped) instead of wrapping around and
/// admitting the entire cache.
fn select_replay_start(sizes: &[usize], budget: usize) -> usize {
    let mut total: usize = 0;
    let mut start = sizes.len();
    for (i, &size) in sizes.iter().enumerate().rev() {
        match total.checked_add(size) {
            Some(sum) if sum <= budget => {
                total = sum;
                start = i;
            }
            _ => break,
        }
    }
    start
}

/// Build the media burst a joining watcher receives, pushing fully-flagged
/// [`ServerResult::OutboundPacket`]s into `out` (and a
/// [`ServerResult::DisconnectConnection`], stopping early, if a session send
/// fails — the same recovery the live fanout uses).
///
/// Order: metadata, video sequence header, audio sequence header, then the
/// GOP segments selected by [`select_replay_start`] oldest -> newest, with
/// the open (current) GOP as the final segment. The current GOP must be
/// included: the live delta frames the watcher receives right after joining
/// reference the current GOP's IDR, so skipping it smears the picture until
/// the next keyframe arrives.
///
/// The sequence headers are sent outside (before) the budget trim,
/// unconditionally: they are required to decode anything at all and are
/// enqueued with `is_sequence_header: true`, which the write queue never
/// drops. Every media frame carries the flags the live path would compute
/// for it, so downstream backpressure applies the same keep/drop policy to
/// replayed frames as to live ones (previously the whole burst was
/// mislabelled as non-video and slipped through the wrong policy tier).
///
/// Saturating sum of the bytes already committed to a joining watcher's write
/// queue AHEAD of the replay burst, which the join-replay budget must subtract so
/// the burst stays under the frame-dropping Warning threshold:
/// - `connection_backlog`: bytes already queued on the connection (e.g. an
///   undrained prior play's replay on a rapid stream switch),
/// - `same_batch_prefix`: bytes of packets emitted earlier in THIS input batch
///   that already target the watcher (e.g. a flood of `createStream` responses,
///   or prior repeated plays' bursts) — accumulated incrementally by
///   `advance_serving_prefix`, so this helper stays O(1) instead of rescanning,
/// - this play's own accept-control packets (`OutboundResponse`s).
/// Every add saturates so a pathological batch cannot wrap the total.
fn join_replay_prefix_bytes(
    connection_backlog: usize,
    same_batch_prefix: usize,
    accept_results: &[ServerSessionResult],
) -> usize {
    let accept_packet_bytes = accept_results
        .iter()
        .fold(0usize, |acc, result| match result {
            ServerSessionResult::OutboundResponse(packet) => acc.saturating_add(packet.bytes.len()),
            _ => acc,
        });
    connection_backlog
        .saturating_add(same_batch_prefix)
        .saturating_add(accept_packet_bytes)
}

/// A watcher must receive a real IDR before any delta frame it is expected
/// to decode. Only the first replayed segment can be keyframeless — either
/// the pre-first-IDR headers+audio, or (mid-GOP publish start) a run of
/// deltas with no IDR. Replay only audio from such a segment and do not flip
/// the keyframe gate until a segment that actually carries an IDR is reached;
/// from then on every frame is decodable (normal GOPs start with their IDR).
fn build_join_burst(
    channel: &MediaChannel,
    client: &mut Client,
    connection_id: usize,
    stream_id: u32,
    accept_prefix_bytes: usize,
    out: &mut Vec<ServerResult>,
) {
    // Real serialized wire bytes of everything enqueued to the joiner's queue
    // ahead of the replayed GOPs. `accept_prefix_bytes` is the play-accept
    // control burst already enqueued before this call (F1); the metadata and
    // sequence headers below add to it. All of it is sent OUTSIDE the GOP
    // replay budget but still occupies the joiner's write queue, so the GOP
    // budget is reduced by their true size. Without this, a large accept prefix
    // (a ~65 KiB stream key echoed by NetStream.Play.Start), a large metadata
    // (encoder string up to ~64 KiB), or a large sequence header could sit on
    // top of a full-budget GOP burst and push the real queue past the Warning
    // threshold, dropping delta frames.
    let mut prefix_wire_bytes = accept_prefix_bytes;

    // If the channel already has existing metadata, send that to the new
    // client so they have up to date info.
    if let Some(ref metadata) = channel.metadata {
        match client.session.send_metadata(stream_id, metadata) {
            Ok(packet) => {
                prefix_wire_bytes = prefix_wire_bytes.saturating_add(packet.bytes.len());
                out.push(ServerResult::OutboundPacket {
                    target_connection_id: connection_id,
                    packet,
                    is_keyframe: false,
                    is_sequence_header: false,
                    is_video: false,
                });
            }
            Err(error) => {
                debug!(
                    "Rtmp client error occurred sending existing metadata to new client: {:?}",
                    error
                );
                out.push(ServerResult::DisconnectConnection { connection_id });
                return;
            }
        }
    }

    if let Some(ref data) = channel.video_sequence_header {
        match client.session.send_video_data(
            stream_id,
            data.clone(),
            channel.video_timestamp,
            false,
        ) {
            Ok(packet) => {
                prefix_wire_bytes = prefix_wire_bytes.saturating_add(packet.bytes.len());
                out.push(ServerResult::OutboundPacket {
                    target_connection_id: connection_id,
                    packet,
                    is_keyframe: false,
                    is_sequence_header: true,
                    is_video: true,
                });
            }
            Err(error) => {
                debug!(
                    "Rtmp client error occurred sending video header to new client: {:?}",
                    error
                );
                out.push(ServerResult::DisconnectConnection { connection_id });
                return;
            }
        }
    }

    if let Some(ref data) = channel.audio_sequence_header {
        match client.session.send_audio_data(
            stream_id,
            data.clone(),
            channel.audio_timestamp,
            false,
        ) {
            Ok(packet) => {
                prefix_wire_bytes = prefix_wire_bytes.saturating_add(packet.bytes.len());
                out.push(ServerResult::OutboundPacket {
                    target_connection_id: connection_id,
                    packet,
                    is_keyframe: false,
                    is_sequence_header: true,
                    is_video: false,
                });
            }
            Err(error) => {
                debug!(
                    "Rtmp client error occurred sending audio header to new client: {:?}",
                    error
                );
                out.push(ServerResult::DisconnectConnection { connection_id });
                return;
            }
        }
    }

    // FrozenGop clone is O(1) (Arc refcount). The segment list is the frozen
    // GOPs oldest -> newest plus the open GOP as the final segment. Each size
    // is the segment's real wire size (payload + RTMP chunk framing), and the
    // budget is the base budget minus the prefix already enqueued above. If the
    // prefix alone meets/exceeds the budget (an oversized metadata or sequence
    // header) the budget saturates to zero, so no delta GOP is replayed — the
    // headers still went out, they are just the whole burst.
    let budget = JOIN_REPLAY_BUDGET_BYTES.saturating_sub(prefix_wire_bytes);
    let frozen: Vec<_> = channel.gops.get_frozen_gops().collect();
    let current_frames = channel.gops.current_frames();
    let mut sizes: Vec<usize> = frozen
        .iter()
        .map(|gop| gop_wire_size(gop.byte_size(), gop.frame_count()))
        .collect();
    sizes.push(gop_wire_size(
        channel.gops.current_byte_size(),
        current_frames.len(),
    ));
    let start = select_replay_start(&sizes, budget);

    let mut replayed_idr = false;
    for segment_index in start..sizes.len() {
        let frames = if segment_index < frozen.len() {
            frozen[segment_index].frames()
        } else {
            current_frames
        };
        if !replayed_idr && gop_contains_video_keyframe(frames) {
            replayed_idr = true;
            client.has_received_video_keyframe = true;
        }
        for frame_data in frames {
            match frame_data {
                FrameData::Video { timestamp, data } => {
                    // Skip undecodable pre-IDR video; the AVC sequence header
                    // is already sent separately above.
                    if !replayed_idr {
                        continue;
                    }
                    let is_keyframe = is_video_keyframe(data);
                    let is_sequence_header = is_video_sequence_header(data);
                    match client
                        .session
                        .send_video_data(stream_id, data.clone(), *timestamp, false)
                    {
                        Ok(packet) => out.push(ServerResult::OutboundPacket {
                            target_connection_id: connection_id,
                            packet,
                            is_keyframe,
                            is_sequence_header,
                            is_video: true,
                        }),
                        Err(error) => {
                            debug!(
                                "Rtmp client error occurred sending video data to new client: {:?}",
                                error
                            );
                            out.push(ServerResult::DisconnectConnection { connection_id });
                            return;
                        }
                    }
                }
                FrameData::Audio { timestamp, data } => {
                    let is_sequence_header = is_audio_sequence_header(data);
                    match client
                        .session
                        .send_audio_data(stream_id, data.clone(), *timestamp, false)
                    {
                        Ok(packet) => out.push(ServerResult::OutboundPacket {
                            target_connection_id: connection_id,
                            packet,
                            is_keyframe: false,
                            is_sequence_header,
                            is_video: false,
                        }),
                        Err(error) => {
                            debug!(
                                "Rtmp client error occurred sending audio data to new client: {:?}",
                                error
                            );
                            out.push(ServerResult::DisconnectConnection { connection_id });
                            return;
                        }
                    }
                }
            }
        }
    }
}

/// Whether replaying `frames` gives a watcher a decodable starting point, i.e.
/// the GOP contains a real IDR (not merely the AVC sequence header or audio).
/// Used to gate has_received_video_keyframe during GOP replay: a GOP frozen
/// before the first IDR must not flip the gate.
fn gop_contains_video_keyframe(frames: &[FrameData]) -> bool {
    frames.iter().any(|f| match f {
        FrameData::Video { data, .. } => is_video_keyframe(data),
        FrameData::Audio { .. } => false,
    })
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

#[cfg(test)]
mod tests {
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

    // The first GOP is frozen before the first IDR and
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
        // A GOP with only the sequence header and audio (no IDR) must NOT flip.
        assert!(
            !gop_contains_video_keyframe(&[seq_header.clone(), pre_roll_audio.clone()]),
            "a sequence-header-only GOP must not flip the keyframe gate"
        );

        // A real GOP starts with an IDR (0x17 0x01, AVC NALU) and must flip it.
        let idr = FrameData::Video {
            timestamp: RtmpTimestamp { value: 33 },
            data: Bytes::from_static(&[0x17, 0x01, 0x00, 0x00, 0x00]),
        };
        let delta = FrameData::Video {
            timestamp: RtmpTimestamp { value: 66 },
            data: Bytes::from_static(&[0x27, 0x01, 0x00, 0x00, 0x00]),
        };
        assert!(
            gop_contains_video_keyframe(&[idr, delta.clone(), pre_roll_audio.clone()]),
            "a GOP containing a real IDR must flip the keyframe gate"
        );

        // Mid-GOP publish start: the first frozen GOP can be delta frames only
        // (no IDR, no sequence header). It must not flip the gate — the replay
        // loop skips its undecodable video and forwards only audio.
        assert!(
            !gop_contains_video_keyframe(&[delta.clone(), delta, pre_roll_audio]),
            "a delta-only GOP (mid-GOP start) must not flip the keyframe gate"
        );

        // AVC end-of-sequence (0x17 0x02) is a keyframe frame-type but not a
        // decodable IDR; it must not flip the gate either.
        let end_of_seq = FrameData::Video {
            timestamp: RtmpTimestamp { value: 99 },
            data: Bytes::from_static(&[0x17, 0x02, 0x00, 0x00, 0x00]),
        };
        assert!(
            !gop_contains_video_keyframe(&[end_of_seq]),
            "an AVC end-of-sequence tag must not flip the keyframe gate"
        );
    }

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

    const IDR: &[u8] = &[0x17, 0x01, 0x00, 0x00, 0x00];
    const DELTA: &[u8] = &[0x27, 0x01, 0x00, 0x00, 0x00];

    // Pre-existing multi-play bug:
    // a client that plays stream A and then plays stream B on the same
    // connection must leave A's watcher set, stop receiving A's frames, and
    // have its keyframe gate reset so B's deltas are withheld until B's IDR.
    #[test]
    fn switching_streams_leaves_the_old_channel_and_regates_on_the_new_idr() {
        let mut scheduler = RtmpScheduler::new(10);
        let publisher_a_conn = 100;
        let watcher_conn = 2;

        assert!(scheduler.new_channel("stream_a".to_string(), publisher_a_conn));
        play(&mut scheduler, watcher_conn, "stream_a");
        let client_id = *scheduler
            .connection_to_client_map
            .get(&watcher_conn)
            .unwrap();
        assert!(scheduler
            .channels
            .get("stream_a")
            .unwrap()
            .watching_client_ids
            .contains(&client_id));

        // A's live IDR reaches the watcher and opens its keyframe gate.
        let results = feed_video(&mut scheduler, "stream_a", 0, IDR);
        assert_eq!(results.len(), 1, "watcher should receive A's IDR");
        assert!(
            scheduler
                .clients
                .get(client_id)
                .unwrap()
                .has_received_video_keyframe
        );

        // The same connection now plays stream B.
        let mut results = Vec::new();
        scheduler.handle_play_requested(
            watcher_conn,
            2,
            "app".to_string(),
            "stream_b".to_string(),
            1,
            &mut results,
        );

        assert!(
            !scheduler
                .channels
                .get("stream_a")
                .unwrap()
                .watching_client_ids
                .contains(&client_id),
            "switching to B must remove the client from A's watcher set"
        );
        assert!(
            scheduler
                .channels
                .get("stream_b")
                .unwrap()
                .watching_client_ids
                .contains(&client_id),
            "the client must be a member of B's watcher set"
        );

        // The keyframe gate must have been reset by the new play request.
        assert!(
            !scheduler
                .clients
                .get(client_id)
                .unwrap()
                .has_received_video_keyframe,
            "a new play request must reset the keyframe gate"
        );

        // A's frames are no longer delivered to the switched client, and A's
        // IDR must not re-open the gate for B.
        let results = feed_video(&mut scheduler, "stream_a", 33, IDR);
        assert!(
            results.is_empty(),
            "A's frames must not reach a client that switched to B"
        );
        assert!(
            !scheduler
                .clients
                .get(client_id)
                .unwrap()
                .has_received_video_keyframe,
            "A's live IDR must not open the gate of a client watching B"
        );

        // B's delta frames are withheld until B's own IDR arrives.
        let results = feed_video(&mut scheduler, "stream_b", 40, DELTA);
        assert!(
            results.is_empty(),
            "B's deltas must be withheld until B's IDR"
        );
        let results = feed_video(&mut scheduler, "stream_b", 66, IDR);
        assert_eq!(results.len(), 1, "B's IDR must be delivered");
        let results = feed_video(&mut scheduler, "stream_b", 100, DELTA);
        assert_eq!(results.len(), 1, "B's deltas flow after B's IDR");
    }

    #[test]
    fn switching_streams_gcs_the_empty_unpublished_old_channel() {
        let mut scheduler = RtmpScheduler::new(10);
        let watcher_conn = 2;

        // Play "stream_a" with no publisher: the channel is created on demand.
        play(&mut scheduler, watcher_conn, "stream_a");
        assert!(scheduler.channels.contains_key("stream_a"));

        // Switching to "stream_b" leaves "stream_a" empty and unpublished, so
        // the channel must be garbage collected.
        let mut results = Vec::new();
        scheduler.handle_play_requested(
            watcher_conn,
            2,
            "app".to_string(),
            "stream_b".to_string(),
            1,
            &mut results,
        );
        assert!(
            !scheduler.channels.contains_key("stream_a"),
            "an empty, unpublished channel must be removed on stream switch"
        );
        assert!(scheduler.channels.contains_key("stream_b"));
    }

    #[test]
    fn replaying_the_same_stream_keeps_the_watcher_membership() {
        let mut scheduler = RtmpScheduler::new(10);
        let publisher_conn = 100;
        let watcher_conn = 2;

        assert!(scheduler.new_channel("stream_a".to_string(), publisher_conn));
        play(&mut scheduler, watcher_conn, "stream_a");
        let client_id = *scheduler
            .connection_to_client_map
            .get(&watcher_conn)
            .unwrap();

        // A second play request for the same stream key must not drop the
        // membership (and must not GC the channel).
        let mut results = Vec::new();
        scheduler.handle_play_requested(
            watcher_conn,
            2,
            "app".to_string(),
            "stream_a".to_string(),
            1,
            &mut results,
        );
        assert!(scheduler
            .channels
            .get("stream_a")
            .unwrap()
            .watching_client_ids
            .contains(&client_id));
    }

    // Pre-existing bug: PlayStreamFinished used to fall into the scheduler's
    // catch-all arm, leaking the watcher membership and the keyframe gate.
    #[test]
    fn play_stream_finished_leaves_the_channel_and_resets_the_play_state() {
        let mut scheduler = RtmpScheduler::new(10);
        let publisher_conn = 100;
        let watcher_conn = 2;

        assert!(scheduler.new_channel("stream_a".to_string(), publisher_conn));
        play(&mut scheduler, watcher_conn, "stream_a");
        let client_id = *scheduler
            .connection_to_client_map
            .get(&watcher_conn)
            .unwrap();

        let results = feed_video(&mut scheduler, "stream_a", 0, IDR);
        assert_eq!(results.len(), 1, "watcher should receive the IDR");
        assert!(
            scheduler
                .clients
                .get(client_id)
                .unwrap()
                .has_received_video_keyframe
        );

        // Deliver the finish through the real scheduler event path to prove
        // it no longer lands in the `_ => debug!` catch-all.
        let mut results = Vec::new();
        let _ = scheduler.handle_raised_event(
            watcher_conn,
            ServerSessionEvent::PlayStreamFinished {
                app_name: "app".to_string(),
                stream_key: "stream_a".to_string(),
            },
            &mut results,
        );

        let channel = scheduler.channels.get("stream_a").unwrap();
        assert!(
            !channel.watching_client_ids.contains(&client_id),
            "PlayStreamFinished must remove the watcher membership"
        );
        let client = scheduler.clients.get(client_id).unwrap();
        assert!(
            matches!(client.current_action, ClientAction::Waiting),
            "PlayStreamFinished must reset the action to Waiting"
        );
        assert!(
            !client.has_received_video_keyframe,
            "PlayStreamFinished must reset the keyframe gate"
        );

        // The published channel itself must survive, and its frames must no
        // longer be delivered to the finished client.
        let results = feed_video(&mut scheduler, "stream_a", 33, IDR);
        assert!(results.is_empty(), "no frames after the play finished");
    }

    #[test]
    fn play_stream_finished_gcs_the_empty_unpublished_channel() {
        let mut scheduler = RtmpScheduler::new(10);
        let watcher_conn = 2;

        play(&mut scheduler, watcher_conn, "stream_a");
        assert!(scheduler.channels.contains_key("stream_a"));

        let mut results = Vec::new();
        let _ = scheduler.handle_raised_event(
            watcher_conn,
            ServerSessionEvent::PlayStreamFinished {
                app_name: "app".to_string(),
                stream_key: "stream_a".to_string(),
            },
            &mut results,
        );

        assert!(
            !scheduler.channels.contains_key("stream_a"),
            "an empty, unpublished channel must be removed when the play finishes"
        );
    }

    // A same-key replay collapses to one membership (single current_action),
    // so a later PlayStreamFinished for that key stops the connection's current
    // play. PlayStreamFinished carries no stream_id, so this key-only match is
    // the only representable behaviour; pin it against regressions.
    #[test]
    fn play_stream_finished_after_a_same_stream_replay_tears_down_the_current_play() {
        let mut scheduler = RtmpScheduler::new(10);
        let publisher_conn = 100;
        let watcher_conn = 2;

        assert!(scheduler.new_channel("stream_a".to_string(), publisher_conn));
        play(&mut scheduler, watcher_conn, "stream_a");
        let client_id = *scheduler
            .connection_to_client_map
            .get(&watcher_conn)
            .unwrap();

        // Replay the SAME stream on a new stream_id (2). Membership is retained.
        let mut results = Vec::new();
        scheduler.handle_play_requested(
            watcher_conn,
            2,
            "app".to_string(),
            "stream_a".to_string(),
            2,
            &mut results,
        );
        assert!(scheduler
            .channels
            .get("stream_a")
            .unwrap()
            .watching_client_ids
            .contains(&client_id));
        assert!(matches!(
            scheduler.clients.get(client_id).unwrap().current_action,
            ClientAction::Watching { stream_id: 2, .. }
        ));

        // A finish for "stream_a" stops the connection's (single) current play:
        // membership removed and action reset. The channel is NOT GC'd because
        // the publisher still holds it.
        let mut results = Vec::new();
        let _ = scheduler.handle_raised_event(
            watcher_conn,
            ServerSessionEvent::PlayStreamFinished {
                app_name: "app".to_string(),
                stream_key: "stream_a".to_string(),
            },
            &mut results,
        );
        assert!(!scheduler
            .channels
            .get("stream_a")
            .unwrap()
            .watching_client_ids
            .contains(&client_id));
        assert!(matches!(
            scheduler.clients.get(client_id).unwrap().current_action,
            ClientAction::Waiting
        ));
        // Publisher keeps the channel alive.
        assert!(scheduler.channels.contains_key("stream_a"));
    }

    #[test]
    fn play_stream_finished_for_another_stream_is_ignored() {
        let mut scheduler = RtmpScheduler::new(10);
        let watcher_conn = 2;

        play(&mut scheduler, watcher_conn, "stream_b");
        let client_id = *scheduler
            .connection_to_client_map
            .get(&watcher_conn)
            .unwrap();

        // A finish for a stream the client is not watching must not disturb
        // the current play.
        let mut results = Vec::new();
        let _ = scheduler.handle_raised_event(
            watcher_conn,
            ServerSessionEvent::PlayStreamFinished {
                app_name: "app".to_string(),
                stream_key: "stream_a".to_string(),
            },
            &mut results,
        );

        let client = scheduler.clients.get(client_id).unwrap();
        assert!(matches!(
            client.current_action,
            ClientAction::Watching { ref stream_key, .. } if stream_key == "stream_b"
        ));
        assert!(scheduler
            .channels
            .get("stream_b")
            .unwrap()
            .watching_client_ids
            .contains(&client_id));
    }

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

    // PERF-5a: the in-process media bypass (publish_media_received) must feed
    // the exact same channel machinery the serialize path reaches, so a live
    // watcher observes sequence-header-first, IDR-first, correctly gated and
    // interleaved audio/video.
    #[test]
    fn in_process_bypass_delivers_seq_header_then_gated_idr_then_interleaved() {
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

        // A delta frame and audio before the first IDR are withheld (gate).
        assert!(
            feed_media(&mut scheduler, publisher_conn, 0x09, 33, DELTA).is_empty(),
            "delta before the IDR must be withheld"
        );
        assert!(
            feed_media(&mut scheduler, publisher_conn, 0x08, 33, AUDIO_FRAME).is_empty(),
            "audio before the first IDR must be withheld"
        );
        assert!(
            !scheduler
                .clients
                .get(watcher_client_id)
                .unwrap()
                .has_received_video_keyframe
        );

        // The IDR opens the gate and is delivered as a keyframe.
        let results = feed_media(&mut scheduler, publisher_conn, 0x09, 66, IDR);
        assert_eq!(results.len(), 1, "the IDR must be delivered");
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

        // After the IDR, audio and delta video interleave through to the watcher.
        let results = feed_media(&mut scheduler, publisher_conn, 0x08, 70, AUDIO_FRAME);
        assert_eq!(results.len(), 1, "audio flows after the IDR");
        assert!(matches!(
            &results[0],
            ServerResult::OutboundPacket {
                is_video: false,
                is_keyframe: false,
                ..
            }
        ));

        let results = feed_media(&mut scheduler, publisher_conn, 0x09, 99, DELTA);
        assert_eq!(results.len(), 1, "delta flows after the IDR");
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
        feed_media(&mut scheduler, publisher_conn, 0x09, 33, IDR);
        feed_media(&mut scheduler, publisher_conn, 0x08, 34, AUDIO_FRAME);
        feed_media(&mut scheduler, publisher_conn, 0x09, 66, DELTA);
        // A second IDR freezes the first GOP into the replay cache.
        feed_media(&mut scheduler, publisher_conn, 0x09, 99, IDR);

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
        // action moved to another stream but its id was left in A's set.
        scheduler.clients.get_mut(client_id).unwrap().current_action = ClientAction::Watching {
            stream_key: "stream_b".to_string(),
            stream_id: 1,
        };

        let results = feed_video(&mut scheduler, "stream_a", 0, IDR);
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

    // ---- H8: join burst (flags, budget trim, current-GOP inclusion) ----

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
            ServerResult::OutboundPacket { packet, .. } => {
                packet.bytes.windows(needle.len()).any(|w| w == needle)
            }
            _ => false,
        }
    }

    /// Total serialized wire bytes of a burst — the real number the write queue
    /// would enqueue. Used to prove a burst stays under the Warning threshold.
    fn serialized_burst_len(out: &[ServerResult]) -> usize {
        out.iter()
            .map(|result| match result {
                ServerResult::OutboundPacket { packet, .. } => packet.bytes.len(),
                _ => 0,
            })
            .sum()
    }

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

        // Frozen GOP: IDR + audio + delta; the second IDR freezes it and
        // opens the current GOP.
        channel
            .gops
            .save_frame_data(video_frame(0, Bytes::from_static(IDR)), true);
        channel
            .gops
            .save_frame_data(audio_frame(10, Bytes::from_static(AUDIO_FRAME)), false);
        channel
            .gops
            .save_frame_data(video_frame(33, Bytes::from_static(DELTA)), false);
        channel
            .gops
            .save_frame_data(video_frame(66, Bytes::from_static(IDR)), true);

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
                (true, false, true),   // frozen GOP IDR
                (false, false, false), // frozen GOP audio
                (false, false, true),  // frozen GOP delta
                (true, false, true),   // current GOP IDR
            ],
            "every replayed packet must carry the flags the live path computes"
        );
        assert!(
            client.has_received_video_keyframe,
            "replaying a real IDR must open the keyframe gate"
        );
    }

    #[test]
    fn join_burst_trims_whole_oldest_gops_to_the_byte_budget() {
        let mut channel = MediaChannel::new(10);
        channel.video_sequence_header = Some(Bytes::from_static(VIDEO_SEQ));
        channel.audio_sequence_header = Some(Bytes::from_static(AUDIO_SEQ));

        // Three ~400KiB single-IDR GOPs (marker at data[2]) plus a small
        // current GOP. Newest-first the 960KiB budget admits current + GOP3 +
        // GOP2 (~800KiB) but not GOP1.
        let make_idr = |marker: u8| {
            let mut data = vec![0u8; 400 * 1024];
            data[0] = 0x17;
            data[1] = 0x01;
            data[2] = marker;
            Bytes::from(data)
        };
        for (i, marker) in [1u8, 2, 3].into_iter().enumerate() {
            channel
                .gops
                .save_frame_data(video_frame(i as u32 * 100, make_idr(marker)), true);
        }
        // The fourth IDR freezes GOP3 and becomes the (small) current GOP.
        channel
            .gops
            .save_frame_data(video_frame(300, Bytes::from_static(IDR)), true);

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
            "the replay must start at GOP2's IDR"
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
            "no IDR was replayed, so live deltas must stay gated until a live IDR"
        );
    }

    // H8.a regression: the replay used to iterate frozen GOPs only. A joiner
    // then received live deltas referencing the open GOP's IDR it never got —
    // a smeared picture until the next keyframe.
    #[test]
    fn join_burst_includes_the_open_current_gop_as_the_last_segment() {
        const CURRENT_IDR: &[u8] = &[0x17, 0x01, 0xB2, 0x00, 0x00];
        const CURRENT_DELTA: &[u8] = &[0x27, 0x01, 0xB3, 0x00, 0x00];

        let mut channel = MediaChannel::new(10);
        channel.video_sequence_header = Some(Bytes::from_static(VIDEO_SEQ));

        // Frozen GOP: IDR + delta. Open GOP: a second IDR + delta not yet
        // frozen by any later keyframe.
        channel
            .gops
            .save_frame_data(video_frame(0, Bytes::from_static(IDR)), true);
        channel
            .gops
            .save_frame_data(video_frame(33, Bytes::from_static(DELTA)), false);
        channel
            .gops
            .save_frame_data(video_frame(66, Bytes::from_static(CURRENT_IDR)), true);
        channel
            .gops
            .save_frame_data(video_frame(99, Bytes::from_static(CURRENT_DELTA)), false);

        let mut client = make_watching_client(7, "live", 1);
        let mut out = Vec::new();
        build_join_burst(&channel, &mut client, 7, 1, 0, &mut out);

        // vseq + frozen (IDR, delta) + current (IDR, delta).
        assert_eq!(
            out.len(),
            5,
            "the open GOP must be replayed after the frozen ones"
        );
        assert!(
            packet_contains(&out[3], CURRENT_IDR),
            "the open GOP's IDR must be replayed — live deltas reference it"
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
        // IDR, and nothing is frozen yet.
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
            "undecodable pre-IDR deltas must be skipped while audio still flows"
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

        // A single ~950 KiB IDR GOP: it fits the raw 960 KiB budget, but not
        // once ~60 KiB of metadata is subtracted.
        let mut idr = vec![0u8; 950 * 1024];
        idr[0] = 0x17;
        idr[1] = 0x01;
        idr[2] = 0xC1;
        channel
            .gops
            .save_frame_data(video_frame(0, Bytes::from(idr)), true);

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
        const OLD_IDR: &[u8] = &[0x17, 0x01, 0xD1];
        const NEW_IDR: &[u8] = &[0x17, 0x01, 0xD2];
        let warn = crate::rtmp::write_queue::QUEUE_WARN_BYTES;

        let mut channel = MediaChannel::new(10);
        channel.video_sequence_header = Some(Bytes::from_static(VIDEO_SEQ));
        channel.audio_sequence_header = Some(Bytes::from_static(AUDIO_SEQ));

        // Two GOPs of 3000 x 150-byte frames each. Payload sum (~880 KiB) fits
        // the 960 KiB budget, but the per-frame framing (3000 x 18 bytes/GOP)
        // pushes the pair over it, so the older GOP is trimmed as a whole.
        let idr = |marker: u8| {
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
            .save_frame_data(video_frame(0, idr(0xD1)), true);
        for i in 0..2999u32 {
            channel
                .gops
                .save_frame_data(video_frame(i + 1, delta()), false);
        }
        // The second IDR freezes the first GOP and opens the second.
        channel
            .gops
            .save_frame_data(video_frame(3000, idr(0xD2)), true);
        for i in 0..2999u32 {
            channel
                .gops
                .save_frame_data(video_frame(3001 + i, delta()), false);
        }

        let mut client = make_watching_client(7, "live", 1);
        let mut out = Vec::new();
        build_join_burst(&channel, &mut client, 7, 1, 0, &mut out);

        assert!(
            out.iter().any(|p| packet_contains(p, NEW_IDR)),
            "the newest GOP must be replayed"
        );
        assert!(
            !out.iter().any(|p| packet_contains(p, OLD_IDR)),
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

        let mut idr = vec![0u8; 4096];
        idr[0] = 0x17;
        idr[1] = 0x01;
        idr[2] = 0xE1;
        channel
            .gops
            .save_frame_data(video_frame(0, Bytes::from(idr)), true);

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
            "no IDR was replayed, so the keyframe gate must stay closed"
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

        // A single ~950 KiB IDR GOP: it fits the raw 960 KiB budget on its own.
        let make_channel = || {
            let mut channel = MediaChannel::new(10);
            channel.video_sequence_header = Some(Bytes::from_static(VIDEO_SEQ));
            let mut idr = vec![0u8; 950 * 1024];
            idr[0] = 0x17;
            idr[1] = 0x01;
            idr[2] = 0xF1;
            channel
                .gops
                .save_frame_data(video_frame(0, Bytes::from(idr)), true);
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
            packet: Packet {
                bytes: vec![0u8; n],
                can_be_dropped: false,
            },
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
    /// dropped: a joiner arriving after the change never receives an old IDR.
    #[test]
    fn codec_config_change_clears_stale_gops_from_the_replay() {
        const VIDEO_SEQ_A: &[u8] = &[0x17, 0x00, 0x00, 0x00, 0x00, 0x01, 0x64];
        const VIDEO_SEQ_B: &[u8] = &[0x17, 0x00, 0x00, 0x00, 0x00, 0x01, 0x2a];
        const IDR_A1: &[u8] = &[0x17, 0x01, 0xa1, 0x00, 0x00];
        const IDR_A2: &[u8] = &[0x17, 0x01, 0xa2, 0x00, 0x00];
        const IDR_B: &[u8] = &[0x17, 0x01, 0xb1, 0x00, 0x00];

        let mut scheduler = RtmpScheduler::new(10);
        let publisher_conn = 100;
        assert!(scheduler.new_channel("live".to_string(), publisher_conn));

        // Old config: header A then two IDRs, so a GOP-A is frozen and one is open.
        feed_media(&mut scheduler, publisher_conn, 0x09, 0, VIDEO_SEQ_A);
        feed_media(&mut scheduler, publisher_conn, 0x09, 33, IDR_A1);
        feed_media(&mut scheduler, publisher_conn, 0x09, 66, IDR_A2);
        assert!(
            scheduler.channels.get("live").unwrap().gops.frozen_count() >= 1,
            "a GOP-A must be frozen before the config change"
        );

        // Config change: a DIFFERENT header B, then a new-config IDR.
        feed_media(&mut scheduler, publisher_conn, 0x09, 99, VIDEO_SEQ_B);
        feed_media(&mut scheduler, publisher_conn, 0x09, 132, IDR_B);

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
            out.iter().any(|p| packet_contains(p, IDR_B)),
            "the new-config IDR must be replayable to a joiner"
        );
        assert!(
            !out.iter().any(|p| packet_contains(p, &[0x17, 0x01, 0xa1])),
            "the old-config IDR A1 must have been cleared on the header change"
        );
        assert!(
            !out.iter().any(|p| packet_contains(p, &[0x17, 0x01, 0xa2])),
            "the old-config IDR A2 must have been cleared on the header change"
        );
    }

    /// A byte-identical sequence-header resend (keepalive) must NOT clear the
    /// cache — otherwise every duplicate header would wipe the replay history.
    #[test]
    fn identical_sequence_header_resend_keeps_the_gop_cache() {
        const IDR_1: &[u8] = &[0x17, 0x01, 0x51, 0x00, 0x00];
        const IDR_2: &[u8] = &[0x17, 0x01, 0x52, 0x00, 0x00];

        let mut scheduler = RtmpScheduler::new(10);
        let publisher_conn = 100;
        assert!(scheduler.new_channel("live".to_string(), publisher_conn));

        feed_media(&mut scheduler, publisher_conn, 0x09, 0, VIDEO_SEQ);
        feed_media(&mut scheduler, publisher_conn, 0x09, 33, IDR_1);
        feed_media(&mut scheduler, publisher_conn, 0x09, 66, IDR_2);
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
            "the cached IDR must survive an identical header resend"
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
        const IDR_A: &[u8] = &[0x17, 0x01, 0xa1, 0x00, 0x00];
        const IDR_B: &[u8] = &[0x17, 0x01, 0xb1, 0x00, 0x00];

        let mut scheduler = RtmpScheduler::new(10);
        let publisher_a = 100;
        let publisher_b = 101;
        let lingering_watcher = 2;

        // Publisher A publishes header H and IDR-A while a watcher is attached
        // (so the channel outlives A's departure).
        assert!(scheduler.new_channel("live".to_string(), publisher_a));
        play(&mut scheduler, lingering_watcher, "live");
        feed_media(&mut scheduler, publisher_a, 0x09, 0, VIDEO_SEQ);
        feed_media(&mut scheduler, publisher_a, 0x09, 33, IDR_A);
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

        // Publisher B reclaims the same key with the SAME header and its own IDR.
        assert!(scheduler.new_channel("live".to_string(), publisher_b));
        feed_media(&mut scheduler, publisher_b, 0x09, 0, VIDEO_SEQ);
        feed_media(&mut scheduler, publisher_b, 0x09, 33, IDR_B);

        // A fresh joiner must receive ONLY publisher B's IDR, never A's.
        let channel = scheduler.channels.get("live").unwrap();
        let mut joiner = make_watching_client(7, "live", 1);
        let mut out = Vec::new();
        build_join_burst(channel, &mut joiner, 7, 1, 0, &mut out);
        assert!(
            out.iter().any(|p| packet_contains(p, IDR_B)),
            "the new session's IDR must be replayable"
        );
        assert!(
            !out.iter().any(|p| packet_contains(p, IDR_A)),
            "the previous session's IDR must never leak into the new session"
        );
    }
}
