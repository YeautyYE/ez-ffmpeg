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
use crate::rtmp::gop::{FrameData, Gops};

/// Error type for RTMP scheduler operations
#[derive(Error, Debug)]
pub(super) enum SchedulerError {
    /// Error from RTMP session handling
    #[error("RTMP session error: {0}")]
    Session(#[from] ServerSessionError),
}

enum ClientAction {
    Waiting,
    Publishing(String), // Publishing to a stream key
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
            current_action: ClientAction::Publishing(stream_key.clone()),
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
            let publisher_results: Vec<ServerSessionResult> = match client.session.handle_input(&bytes) {
                Ok(results) => results,
                Err(error) => return Err(error.into()),
            };
            publisher_results
        };

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
                        self.handle_raised_event(usize::MAX, event, &mut server_results);
                    }
                    ServerSessionEvent::ConnectionRequested {request_id, app_name: _} => {
                        let client_id = self
                            .publisher_to_client_map
                            .get(&publisher_connection_id)
                            .unwrap();
                        let client = self.clients.get_mut(*client_id).unwrap();
                        if let Err(e) = client.session.accept_request(request_id) {
                            warn!("Failed to accept connection request {}: {:?}", request_id, e);
                        }
                    }
                    ServerSessionEvent::PublishStreamRequested {request_id, app_name: _, stream_key, mode: _} => {
                        let client_id = self
                            .publisher_to_client_map
                            .get(&publisher_connection_id)
                            .unwrap();
                        let client = self.clients.get_mut(*client_id).unwrap();
                        if let Err(e) = client.session.accept_request(request_id) {
                            warn!("Failed to accept publish request {} for stream '{}': {:?}", request_id, stream_key, e);
                        }
                    }
                    _ => {
                        debug!("Publisher received unexpected event: {:?}", event);
                    }
                }

                x => warn!("Server result received: {:?}", x),
            }
        }

        Ok(server_results)
    }

    pub(super) fn bytes_received(
        &mut self,
        connection_id: usize,
        bytes: &[u8],
    ) -> Result<Vec<ServerResult>, SchedulerError> {
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
                    ClientAction::Publishing(stream_key) => self.publishing_ended(stream_key),
                    _ => {}
                }
            }
        }
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
                    self.handle_raised_event(executed_connection_id, event, server_results)
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
    ) {
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
                self.handle_publish_finished(
                    app_name,
                    stream_key,
                    server_results,
                );
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
                self.handle_audio_video_data_received(
                    stream_key,
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
                self.handle_audio_video_data_received(
                    stream_key,
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
        stream_key: String,
        server_results: &mut Vec<ServerResult>,
    ) {
        debug!("Rtmp publish finished on app '{app_name}' and stream key '{stream_key}'");

        let channel = match self.channels.get(&stream_key) {
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

        let accept_result;
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
                .or_insert(MediaChannel::new(self.gop_limit));

            channel.watching_client_ids.insert(*client_id);
            accept_result = match client.session.accept_request(request_id) {
                Err(error) => Err(error),
                Ok(mut results) => {
                    // If the channel already has existing metadata, send that to the new client
                    // so they have up to date info
                    match channel.metadata {
                        None => (),
                        Some(ref metadata) => {
                            let packet = match client.session.send_metadata(stream_id, &metadata) {
                                Ok(packet) => packet,
                                Err(error) => {
                                    debug!("Rtmp client error occurred sending existing metadata to new client: {:?}", error);
                                    server_results.push(ServerResult::DisconnectConnection {
                                        connection_id: requested_connection_id,
                                    });

                                    return;
                                }
                            };

                            results.push(ServerSessionResult::OutboundResponse(packet));
                        }
                    }

                    // If the channel already has sequence headers, send them
                    match channel.video_sequence_header {
                        None => (),
                        Some(ref data) => {
                            let packet = match client.session.send_video_data(
                                stream_id,
                                data.clone(),
                                channel.video_timestamp,
                                false,
                            ) {
                                Ok(packet) => packet,
                                Err(error) => {
                                    debug!("Rtmp client error occurred sending video header to new client: {:?}", error);
                                    server_results.push(ServerResult::DisconnectConnection {
                                        connection_id: requested_connection_id,
                                    });

                                    return;
                                }
                            };

                            results.push(ServerSessionResult::OutboundResponse(packet));
                        }
                    }

                    match channel.audio_sequence_header {
                        None => (),
                        Some(ref data) => {
                            let packet = match client.session.send_audio_data(
                                stream_id,
                                data.clone(),
                                channel.audio_timestamp,
                                false,
                            ) {
                                Ok(packet) => packet,
                                Err(error) => {
                                    debug!("Rtmp client error occurred sending audio header to new client: {:?}", error);
                                    server_results.push(ServerResult::DisconnectConnection {
                                        connection_id: requested_connection_id,
                                    });

                                    return;
                                }
                            };

                            results.push(ServerSessionResult::OutboundResponse(packet));
                        }
                    }

                    // Use zero-copy API to get frozen GOPs
                    // FrozenGop clone is O(1), only increments Arc reference count
                    //
                    // A watcher must receive a real IDR before any delta frame it
                    // is expected to decode. Only the first frozen GOP can be
                    // keyframeless — either the pre-first-IDR headers+audio, or
                    // (mid-GOP publish start) a run of deltas with no IDR. Replay
                    // only audio from such a GOP and do not flip the gate until a
                    // GOP that actually carries an IDR is reached; from then on
                    // every frame is decodable (normal GOPs start with their IDR).
                    let mut replayed_idr = false;
                    for frozen_gop in channel.gops.get_frozen_gops() {
                        let frames = frozen_gop.frames();
                        if !replayed_idr && gop_contains_video_keyframe(frames) {
                            replayed_idr = true;
                            client.has_received_video_keyframe = true;
                        }
                        for frame_data in frames {
                            match frame_data {
                                FrameData::Video { timestamp, data } => {
                                    // Skip undecodable pre-IDR video; the AVC
                                    // sequence header is already sent separately
                                    // before this loop.
                                    if !replayed_idr {
                                        continue;
                                    }
                                    let packet = match client.session.send_video_data(
                                        stream_id,
                                        data.clone(),
                                        *timestamp,
                                        false,
                                    ) {
                                        Ok(packet) => packet,
                                        Err(error) => {
                                            debug!("Rtmp client error occurred sending video data to new client: {:?}", error);
                                            server_results.push(ServerResult::DisconnectConnection {
                                                connection_id: requested_connection_id,
                                            });

                                            return;
                                        }
                                    };
                                    results.push(ServerSessionResult::OutboundResponse(packet));
                                }
                                FrameData::Audio { timestamp, data } => {
                                    let packet = match client.session.send_audio_data(
                                        stream_id,
                                        data.clone(),
                                        *timestamp,
                                        false,
                                    ) {
                                        Ok(packet) => packet,
                                        Err(error) => {
                                            debug!("Rtmp client error occurred sending audio data to new client: {:?}", error);
                                            server_results.push(ServerResult::DisconnectConnection {
                                                connection_id: requested_connection_id,
                                            });

                                            return;
                                        }
                                    };
                                    results.push(ServerSessionResult::OutboundResponse(packet));
                                }
                            }
                        }
                    }
                    Ok(results)
                }
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
            }
        }
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
        stream_key: String,
        timestamp: RtmpTimestamp,
        data: Bytes,
        data_type: ReceivedDataType,
        server_results: &mut Vec<ServerResult>,
    ) {
        let channel = match self.channels.get_mut(&stream_key) {
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
                    channel.video_sequence_header = Some(data.clone());
                    channel.video_timestamp = timestamp;
                }
                channel.gops.save_frame_data(crate::rtmp::gop::FrameData::Video { timestamp, data: data.clone() }, is_keyframe);
            }

            ReceivedDataType::Audio => {
                if is_sequence_header {
                    channel.audio_sequence_header = Some(data.clone());
                    channel.audio_timestamp = timestamp;
                }
                channel.gops.save_frame_data(crate::rtmp::gop::FrameData::Audio { timestamp, data: data.clone() }, false);
            }
        }

        // Detect an audio-only stream from the publisher's metadata: it declares
        // an audio codec but no video codec. Computed once per received packet so
        // the per-watcher gate can deliver audio for such streams instead of
        // waiting for a video keyframe that never comes (see
        // `should_send_to_watcher`).
        let channel_is_audio_only = channel.metadata.as_ref().is_some_and(|m| {
            m.audio_codec_id.is_some() && m.video_codec_id.is_none()
        });

        for client_id in &channel.watching_client_ids {
            let client = match self.clients.get_mut(*client_id) {
                Some(client) => client,
                None => continue,
            };

            let active_stream_id = match client.get_active_stream_id() {
                Some(stream_id) => stream_id,
                None => continue,
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
                ReceivedDataType::Audio => client.session.send_audio_data(
                    active_stream_id,
                    data.clone(),
                    timestamp,
                    true,
                ),
                ReceivedDataType::Video => {
                    if is_keyframe {
                        client.has_received_video_keyframe = true;
                    }

                    client.session.send_video_data(
                        active_stream_id,
                        data.clone(),
                        timestamp,
                        true,
                    )
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

    fn publishing_ended(&mut self, stream_key: String) {
        let should_remove = if let Some(channel) = self.channels.get_mut(&stream_key) {
            channel.publishing_client_id = None;
            channel.metadata = None;
            channel.should_remove()
        } else {
            return;
        };
        if should_remove {
            self.channels.remove(&stream_key);
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
        ReceivedDataType::Video => {
            has_received_video_keyframe || is_sequence_header || is_keyframe
        }
        ReceivedDataType::Audio => {
            has_received_video_keyframe || is_sequence_header || channel_is_audio_only
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

fn is_video_sequence_header(data: &Bytes) -> bool {
    // This is assuming h264.
    data.len() >= 2 && data[0] == 0x17 && data[1] == 0x00
}

fn is_audio_sequence_header(data: &Bytes) -> bool {
    // This is assuming aac.
    data.len() >= 2 && data[0] == 0xaf && data[1] == 0x00
}

fn is_video_keyframe(data: &Bytes) -> bool {
    // Assuming h264. 0x17 = keyframe frame-type + AVC codec; AVCPacketType 0x01
    // is a NALU (the actual IDR). Require == 0x01 rather than != 0x00 so the
    // sequence header (0x00) AND the AVC end-of-sequence marker (0x02) are both
    // excluded — only a decodable IDR frame may flip the keyframe gate.
    data.len() >= 2 && data[0] == 0x17 && data[1] == 0x01
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
            stream_key.clone(),
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
        assert!(scheduler.publisher_to_client_map.contains_key(&publisher_connection_id));
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
        assert!(scheduler.publisher_to_client_map.contains_key(&publisher_connection_id_1));
        assert!(!scheduler.publisher_to_client_map.contains_key(&publisher_connection_id_2));
    }

    #[test]
    fn test_notify_connection_closed() {
        let mut scheduler = RtmpScheduler::new(10);
        let connection_id = 1;

        // Create a session by calling bytes_received
        let _ = scheduler.bytes_received(connection_id, &[]);

        // Verify connection exists
        assert!(scheduler.connection_to_client_map.contains_key(&connection_id));
        let client_id = *scheduler.connection_to_client_map.get(&connection_id).unwrap();
        assert!(scheduler.clients.contains(client_id));

        // Close the connection
        scheduler.notify_connection_closed(connection_id);

        // Verify connection is removed
        assert!(!scheduler.connection_to_client_map.contains_key(&connection_id));
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
        assert!(scheduler.publisher_to_client_map.contains_key(&publisher_connection_id));
        let client_id = *scheduler.publisher_to_client_map.get(&publisher_connection_id).unwrap();
        assert!(scheduler.clients.contains(client_id));

        // Close the publisher
        scheduler.notify_publisher_closed(publisher_connection_id);

        // Verify publisher is removed
        assert!(!scheduler.publisher_to_client_map.contains_key(&publisher_connection_id));
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
        assert!(!scheduler.publisher_to_client_map.contains_key(&publisher_connection_id));

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
        assert!(!scheduler.connection_to_client_map.contains_key(&connection_id));

        // Receive bytes from new connection
        let result = scheduler.bytes_received(connection_id, &[0x03]);

        // Should succeed
        assert!(result.is_ok());

        // Verify session was created
        assert!(scheduler.connection_to_client_map.contains_key(&connection_id));
        let client_id = *scheduler.connection_to_client_map.get(&connection_id).unwrap();
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
        assert!(scheduler.connection_to_client_map.contains_key(&connection_id));
        let client_id = *scheduler.connection_to_client_map.get(&connection_id).unwrap();
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
        assert!(matches!(client.current_action, ClientAction::Watching { .. }));

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
            stream_key.clone(),
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
            stream_key.clone(),
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
            stream_key.clone(),
            RtmpTimestamp { value: 1033 },
            non_keyframe_data,
            ReceivedDataType::Video,
            &mut server_results,
        );

        // Watcher should receive non-keyframe (after having received keyframe)
        assert_eq!(server_results.len(), 1, "Watcher should receive non-keyframe");
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
            stream_key.clone(),
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
            stream_key.clone(),
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
            stream_key.clone(),
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
        assert_eq!(channel.video_sequence_header.as_ref().unwrap(), &sequence_header);
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
            stream_key.clone(),
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
            stream_key.clone(),
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
            stream_key.clone(),
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
            stream_key.clone(),
            RtmpTimestamp { value: 100 },
            keyframe,
            ReceivedDataType::Video,
            &mut server_results,
        );

        // Now send audio again
        server_results.clear();
        scheduler.handle_audio_video_data_received(
            stream_key.clone(),
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
            stream_key.clone(),
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
        assert_eq!(scheduler.channels.get(&stream_key).unwrap().watching_client_ids.len(), 2);

        // Send initial keyframe to both
        let mut server_results = Vec::new();
        let keyframe = Bytes::from(vec![0x17, 0x01, 0x00, 0x00, 0x00]);
        scheduler.handle_audio_video_data_received(
            stream_key.clone(),
            RtmpTimestamp { value: 0 },
            keyframe,
            ReceivedDataType::Video,
            &mut server_results,
        );
        assert_eq!(server_results.len(), 2, "Both watchers should receive keyframe");

        // Watcher 1 disconnects
        scheduler.notify_connection_closed(watcher1_id);

        // Verify watcher1 removed from channel
        let channel = scheduler.channels.get(&stream_key).unwrap();
        assert_eq!(channel.watching_client_ids.len(), 1);

        // Send another frame - only watcher2 should receive it
        server_results.clear();
        let frame = Bytes::from(vec![0x27, 0x01, 0x00, 0x00, 0x00]);
        scheduler.handle_audio_video_data_received(
            stream_key.clone(),
            RtmpTimestamp { value: 33 },
            frame,
            ReceivedDataType::Video,
            &mut server_results,
        );

        assert_eq!(server_results.len(), 1, "Only remaining watcher should receive frame");
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
}

