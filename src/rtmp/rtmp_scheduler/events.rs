//! Session-result routing, the control-path event handlers and the
//! channel teardown/GC that runs when publishing or playing ends.

use super::{
    new_media_serializer, oversized_sequence_header_error, ClientAction, ReceivedDataType,
    RtmpScheduler, SchedulerError, ServerResult,
};
#[cfg(test)]
use bytes::Bytes;
use log::{debug, warn};
use rml_rtmp::sessions::{ServerSessionEvent, ServerSessionResult, StreamMetadata};
use rml_rtmp::time::RtmpTimestamp;
use std::rc::Rc;

impl RtmpScheduler {
    pub(super) fn handle_session_results(
        &mut self,
        executed_connection_id: usize,
        session_results: Vec<ServerSessionResult>,
        server_results: &mut Vec<ServerResult>,
    ) {
        for result in session_results {
            match result {
                ServerSessionResult::OutboundResponse(packet) => {
                    // Control message, not audio/video data
                    server_results.push(ServerResult::outbound(
                        executed_connection_id,
                        packet,
                        false,
                        false,
                        false,
                    ))
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

    pub(super) fn handle_raised_event(
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

    pub(super) fn handle_publish_finished(
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
                    server_results.push(ServerResult::outbound(
                        client.connection_id,
                        packet,
                        false,
                        false,
                        false,
                    ));
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

    pub(super) fn handle_metadata_received(
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
                    server_results.push(ServerResult::outbound(
                        client.connection_id,
                        packet,
                        false,
                        false,
                        false,
                    ));
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

    pub(super) fn publishing_ended(&mut self, stream_key: &str) {
        let should_remove = if let Some(channel) = self.channels.get_mut(stream_key) {
            // Reset the FULL publisher-scoped state, not just metadata. During
            // the close linger the channel outlives its publisher (lingering
            // watchers keep it), yet `stream_keys` is released at once, so a NEW
            // publisher can reclaim the same key immediately. If it reuses the
            // same sequence header, the clear-on-change in
            // `handle_audio_video_data_received` never fires — without this, a
            // fresh joiner would replay the PREVIOUS session's cached GOPs and
            // the new publisher's first keyframe would freeze the old open GOP,
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
            // The shared serializer is publisher-scoped state too: a channel
            // that outlives its publisher (lingering watchers) can be
            // reclaimed by a NEW publisher under the same key, and the next
            // generation must not inherit the previous generation's per-csid
            // header history. Today that history is provably inert (every
            // entry is droppable, forcing type 0 forever), but a fresh
            // serializer per publisher generation removes the entire class
            // of cross-generation leakage rather than leaning on that
            // invariant.
            channel.fanout_serializer = new_media_serializer();
            channel.should_remove()
        } else {
            return;
        };
        if should_remove {
            self.channels.remove(stream_key);
        }
    }

    pub(super) fn play_ended(&mut self, client_id: usize, stream_key: String) {
        let should_remove = if let Some(channel) = self.channels.get_mut(&stream_key) {
            channel.watching_client_ids.remove(&client_id);
            channel.shrink_watchers_if_sparse();
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
