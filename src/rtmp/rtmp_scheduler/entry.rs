//! Reactor-facing entry points: session construction, socket-byte and
//! in-process media ingest, the watcher liveness ping and the connection
//! lifecycle notifications.

use super::{
    oversized_sequence_header_error, ChannelTable, Client, ClientAction, ReceivedDataType,
    RtmpScheduler, SchedulerError, ServerResult,
};
use bytes::Bytes;
use log::{debug, warn};
use rml_rtmp::chunk_io::Packet;
use rml_rtmp::sessions::{
    ServerSession, ServerSessionConfig, ServerSessionEvent, ServerSessionResult,
};
use rml_rtmp::time::RtmpTimestamp;
use slab::Slab;
use std::collections::HashMap;

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

        // Resolve (or create) the channel slot first: the publisher's
        // action stores the pre-resolved handle the per-tag media path
        // rides, and the slot's shared key allocation backs the action's
        // `Rc<str>`. A slot that already exists (created by early
        // watchers, or surviving a previous publisher through lingering
        // watchers) keeps its handle, so those watchers stay wired.
        let (channel_handle, slot) = self.channels.get_or_create(&stream_key, self.gop_limit);
        let client = Client {
            session,
            connection_id: publisher_connection_id,
            current_action: ClientAction::Publishing {
                stream_key: slot.stream_key.clone(),
                channel: channel_handle,
            },
            has_received_video_keyframe: false,
        };

        let client_id = self.clients.insert(client);
        self.publisher_to_client_map
            .insert(publisher_connection_id, client_id);
        slot.channel.publishing_client_id = Some(client_id);

        true
    }
}

impl RtmpScheduler {
    pub(in crate::rtmp) fn new(gop_limit: usize) -> RtmpScheduler {
        RtmpScheduler {
            clients: Slab::with_capacity(1024),
            connection_to_client_map: HashMap::with_capacity(1024),
            publisher_to_client_map: HashMap::with_capacity(32),
            channels: ChannelTable::new(),
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
        server_results: &mut Vec<ServerResult>,
    ) -> Result<(), SchedulerError> {
        // Single lookup: the map hit both gates this call and names the
        // client (the old contains_key + get pair walked the map twice).
        let Some(&client_id) = self.publisher_to_client_map.get(&publisher_connection_id) else {
            warn!(
                "Publishing event for non-existent connection_id: {}",
                publisher_connection_id
            );
            return Ok(());
        };

        let publisher_results = {
            let Some(client) = self.clients.get_mut(client_id) else {
                warn!(
                    "Publishing client {} not found for connection_id: {}",
                    client_id, publisher_connection_id
                );
                return Ok(());
            };
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
        // of a batch that is going to abort (nothing is appended to server_results here).
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
                        self.handle_raised_event(usize::MAX, event, server_results)?;
                    }
                    ServerSessionEvent::ConnectionRequested {
                        request_id,
                        app_name: _,
                    } => {
                        let Some(client) = self
                            .publisher_to_client_map
                            .get(&publisher_connection_id)
                            .and_then(|client_id| self.clients.get_mut(*client_id))
                        else {
                            warn!(
                                "Connection request {} for non-existent publisher connection_id: {}",
                                request_id, publisher_connection_id
                            );
                            continue;
                        };
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
                        let Some(client) = self
                            .publisher_to_client_map
                            .get(&publisher_connection_id)
                            .and_then(|client_id| self.clients.get_mut(*client_id))
                        else {
                            warn!(
                                "Publish request {} for stream '{}' on non-existent publisher connection_id: {}",
                                request_id, stream_key, publisher_connection_id
                            );
                            continue;
                        };
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

        Ok(())
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
    ///
    /// Results are appended to `server_results`; the caller owns clearing and
    /// draining the buffer.
    pub(in crate::rtmp) fn publish_media_received(
        &mut self,
        publisher_connection_id: usize,
        tag_type: u8,
        timestamp: RtmpTimestamp,
        data: Bytes,
        server_results: &mut Vec<ServerResult>,
    ) {
        let data_type = match tag_type {
            0x08 => ReceivedDataType::Audio,
            0x09 => ReceivedDataType::Video,
            other => {
                // Only audio/video tags are bypassed; anything else is a
                // caller bug (metadata and control must stay on the byte path).
                warn!("In-process media bypass received unexpected FLV tag type {other:#04x}");
                return;
            }
        };

        let client_id = match self.publisher_to_client_map.get(&publisher_connection_id) {
            Some(client_id) => *client_id,
            None => {
                warn!(
                    "In-process media for non-existent publisher connection_id: {}",
                    publisher_connection_id
                );
                return;
            }
        };

        // The publisher's channel was resolved to a handle once, at attach
        // time (`new_channel`); copying it out ends the `clients` borrow
        // before the fanout mutates `channels`, and the per-tag path
        // touches no string at all.
        let channel_handle = match self.clients.get(client_id) {
            Some(client) => match &client.current_action {
                ClientAction::Publishing { channel, .. } => *channel,
                _ => {
                    warn!(
                        "In-process media for a publisher not in the Publishing state: {}",
                        publisher_connection_id
                    );
                    return;
                }
            },
            None => return,
        };

        // The oversized-sequence-header bound (F2) lives on the untrusted socket
        // ingest path (`handle_raised_event`, driven by rml_rtmp deserialization
        // of remote bytes). This bypass carries only in-process FFmpeg muxer
        // output — a trusted source that never emits an oversized header — so it
        // needs no gate here (and, returning no Result, could not terminate the
        // feed anyway).
        self.distribute_media(channel_handle, timestamp, data, data_type, server_results);
    }

    // The production reactor always supplies a real write-queue backlog via
    // bytes_received_with_backlog; only unit tests drive the scheduler bare.
    #[cfg_attr(not(test), allow(dead_code))]
    pub(in crate::rtmp) fn bytes_received(
        &mut self,
        connection_id: usize,
        bytes: &[u8],
    ) -> Result<Vec<ServerResult>, SchedulerError> {
        let mut server_results = Vec::new();
        self.bytes_received_with_backlog(connection_id, bytes, 0, &mut server_results)?;
        Ok(server_results)
    }

    /// Like `bytes_received`, but told the connection's current write-queue
    /// backlog so a `play` handled in this batch can budget its join-replay burst
    /// against the bytes already queued ahead of it (see
    /// `serving_connection_backlog_bytes`). The reactor supplies the real value;
    /// the plain `bytes_received` wrapper passes 0.
    ///
    /// Results are APPENDED to `server_results`; the caller must pass a cleared
    /// buffer (the reactor reuses one across batches). The same-batch
    /// join-replay prefix cursor counts entries from index 0, so pre-existing
    /// entries would inflate the join-burst prefix accounting.
    pub(in crate::rtmp) fn bytes_received_with_backlog(
        &mut self,
        connection_id: usize,
        bytes: &[u8],
        connection_backlog_bytes: usize,
        server_results: &mut Vec<ServerResult>,
    ) -> Result<(), SchedulerError> {
        debug_assert!(
            server_results.is_empty(),
            "results buffer must be cleared before each batch: the same-batch \
             join-replay prefix cursor counts entries from index 0"
        );
        self.serving_connection_backlog_bytes = connection_backlog_bytes;
        // Reset the same-batch join-replay prefix cursor for this input batch.
        self.serving_prefix_scan_pos = 0;
        self.serving_prefix_bytes = 0;

        // Single lookup, copied out at once: handle_session_results below
        // needs &mut self, so a held map borrow would not compile — and the
        // old contains_key + get pair paid the hash walk twice per batch.
        let client_id = match self.connection_to_client_map.get(&connection_id).copied() {
            Some(id) => id,
            None => {
                let config = ServerSessionConfig::new();
                let (session, initial_session_results) = match ServerSession::new(config) {
                    Ok(results) => results,
                    Err(error) => return Err(error.into()),
                };

                self.handle_session_results(connection_id, initial_session_results, server_results);
                let client = Client {
                    session,
                    connection_id,
                    current_action: ClientAction::Waiting,
                    has_received_video_keyframe: false,
                };

                let client_id = self.clients.insert(client);
                self.connection_to_client_map
                    .insert(connection_id, client_id);
                client_id
            }
        };

        let client_results: Vec<ServerSessionResult>;
        {
            let client = self.clients.get_mut(client_id).unwrap();
            client_results = match client.session.handle_input(bytes) {
                Ok(results) => results,
                Err(error) => return Err(error.into()),
            };
        }

        self.handle_session_results(connection_id, client_results, server_results);
        Ok(())
    }

    /// Build a liveness ping (RTMP User Control `PingRequest`) for
    /// `connection_id` if it is a client currently watching a channel.
    ///
    /// Watchers are the only role that can sit legitimately idle on a
    /// healthy connection: with no publisher on their channel nothing is
    /// ever written to them, and after `play` they have nothing left to
    /// say, so without a server-side ping the reactor's idle sweep reaps
    /// them. Every other classification returns `None` — a publisher idle
    /// for the full timeout is dead weight pinning a stream key and must
    /// still be reaped — as do unknown connections and a session that
    /// fails to serialize the request. The packet must come from the
    /// client's own session: it owns the serializer whose chunk-stream
    /// state the peer is tracking.
    pub(in crate::rtmp) fn ping_watcher(&mut self, connection_id: usize) -> Option<Packet> {
        let client_id = self.connection_to_client_map.get(&connection_id)?;
        let client = self.clients.get_mut(*client_id)?;
        if !matches!(client.current_action, ClientAction::Watching { .. }) {
            return None;
        }
        match client.session.send_ping_request() {
            Ok((packet, _sent_at)) => Some(packet),
            Err(e) => {
                warn!("Failed to build a ping request for connection {connection_id}: {e:?}");
                None
            }
        }
    }

    /// Test-only staging: register `connection_id` as a client watching
    /// `stream_key`, through the same registration path a real `play` runs
    /// (client creation plus `handle_play_requested`; the session's accept
    /// round-trip would need a full client byte exchange, which watcher-
    /// classification tests do not require). Exists because reactor-level
    /// tests cannot reach this module's private handlers.
    #[cfg(test)]
    pub(in crate::rtmp) fn register_watcher_for_test(
        &mut self,
        connection_id: usize,
        stream_key: &str,
    ) {
        let _ = self.bytes_received(connection_id, &[]);
        let mut server_results = Vec::new();
        self.handle_play_requested(
            connection_id,
            1,
            "test-app".to_string(),
            stream_key.to_string(),
            1,
            &mut server_results,
        );
    }

    pub(in crate::rtmp) fn notify_connection_closed(&mut self, connection_id: usize) {
        match self.connection_to_client_map.remove(&connection_id) {
            None => (),
            Some(client_id) => {
                let client = self.clients.remove(client_id);
                match client.current_action {
                    ClientAction::Watching { stream_key, .. } => {
                        self.play_ended(client_id, stream_key)
                    }
                    ClientAction::Waiting => (),
                    _ => {}
                }
            }
        }
    }

    pub(in crate::rtmp) fn notify_publisher_closed(&mut self, publisher_connection_id: usize) {
        match self
            .publisher_to_client_map
            .remove(&publisher_connection_id)
        {
            None => (),
            Some(client_id) => {
                let client = self.clients.remove(client_id);
                match client.current_action {
                    ClientAction::Publishing { stream_key, .. } => {
                        self.publishing_ended(&stream_key)
                    }
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
    pub(in crate::rtmp) fn abort_publisher_watchers(
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
                ClientAction::Publishing { stream_key, .. } => stream_key.clone(),
                _ => return server_results,
            },
            None => return server_results,
        };
        self.handle_publish_finished(String::new(), &stream_key, &mut server_results);
        server_results
    }
}
