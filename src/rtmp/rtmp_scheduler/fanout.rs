//! The shared live A/V fanout and the per-watcher keyframe gate: each
//! frame is serialized once per distinct active message stream ID among
//! the gate-passing watchers, with `serialized_groups` caching SUCCESSFUL
//! serializations only — the cached bytes are refcount-cloned to every
//! further watcher of the group, while a failed serialization caches
//! nothing and is re-attempted (and fails deterministically) per watcher
//! of that group, disconnecting each. Metadata fanout deliberately stays
//! per-session (`events`): metadata rides csid 3, whose header-compression
//! chain belongs to the per-session serializers — see the
//! `MediaChannel::fanout_serializer` doc.

use super::join::evict_unreplayable_frozen;
use super::{
    serialize_media, ChannelHandle, ChannelSlot, ClientAction, MediaChannel, ReceivedDataType,
    RtmpScheduler, ServerResult,
};
use crate::flv::flv_tag_body::{
    is_audio_sequence_header, is_video_keyframe, is_video_sequence_header,
};
use bytes::Bytes;
use log::debug;
use rml_rtmp::time::RtmpTimestamp;

impl RtmpScheduler {
    /// String-keyed entry to the fanout, used by the socket-byte ingest
    /// path (whose media events carry the stream key by name) and the
    /// tests: one map lookup resolves the key to its current
    /// [`ChannelHandle`], then [`RtmpScheduler::distribute_media`] does the
    /// work. An unknown key returns with no output, exactly as before. The
    /// in-process bypass skips this resolution entirely by carrying the
    /// publisher's pre-resolved handle.
    pub(super) fn handle_audio_video_data_received(
        &mut self,
        stream_key: &str,
        timestamp: RtmpTimestamp,
        data: Bytes,
        data_type: ReceivedDataType,
        server_results: &mut Vec<ServerResult>,
    ) {
        let Some(channel_handle) = self.channels.handle_by_key(stream_key) else {
            return;
        };
        self.distribute_media(channel_handle, timestamp, data, data_type, server_results);
    }

    /// The per-tag fanout core, reached with a pre-resolved
    /// [`ChannelHandle`]: one generation-checked slab access replaces the
    /// per-tag stream-key hash, and the per-watcher membership guard
    /// compares handles instead of key strings. A stale handle (its
    /// channel was removed — and the slot possibly reused — after the
    /// handle was issued) resolves to `None` and returns silently, the
    /// same observable behavior as an unknown stream key on the string
    /// path above.
    pub(super) fn distribute_media(
        &mut self,
        channel_handle: ChannelHandle,
        timestamp: RtmpTimestamp,
        data: Bytes,
        data_type: ReceivedDataType,
        server_results: &mut Vec<ServerResult>,
    ) {
        let slot = match self.channels.slot_by_handle_mut(channel_handle) {
            Some(slot) => slot,
            None => return,
        };
        // Disjoint slot borrows: the key is read for diagnostics only
        // while the channel is driven mutably.
        let ChannelSlot {
            stream_key,
            channel,
            ..
        } = slot;

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
                // A keyframe save is the only event that grows the frozen
                // history (audio saves and deltas never freeze); frozen GOPs
                // beyond the join-replay budget can never reach a joiner, so
                // drop them at the freeze transition.
                if is_keyframe {
                    evict_unreplayable_frozen(&mut channel.gops);
                }
            }

            ReceivedDataType::Audio => {
                if is_sequence_header {
                    // Same reasoning as the video path. Audio frames share this
                    // channel's single GOP cache, so an audio codec change also
                    // clears the cached video GOPs. That is safe (a joiner just
                    // replays fewer frames and catches up on the next live keyframe)
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

        // Disjoint field borrows: the watcher set is iterated while the
        // shared serializer is driven mutably below.
        let MediaChannel {
            watching_client_ids,
            metadata,
            fanout_serializer,
            ..
        } = channel;

        // Detect an audio-only stream from the publisher's metadata: it declares
        // an audio codec but no video codec. Computed once per received packet so
        // the per-watcher gate can deliver audio for such streams instead of
        // waiting for a video keyframe that never comes (see
        // `should_send_to_watcher`).
        let channel_is_audio_only = metadata
            .as_ref()
            .is_some_and(|m| m.audio_codec_id.is_some() && m.video_codec_id.is_none());

        // Shared fanout serialization: this message's wire bytes per
        // `message_stream_id` group, serialized on the channel serializer
        // the FIRST time a watcher of that group passes the gate; every
        // other watcher of the group gets a `Bytes` refcount clone. Only a
        // SUCCESSFUL serialization is cached — the error arm below caches
        // nothing, so each remaining watcher of the group re-attempts (and
        // deterministically re-fails) its own serialization on the way to
        // its disconnect. The type-0 header embeds the stream id (the only
        // per-watcher-variable field), hence the grouping; watchers sharing
        // a stream id share one entry, so the common case stays a single
        // serialization.
        let mut serialized_groups: Vec<(u32, Bytes)> = Vec::new();

        for client_id in watching_client_ids.iter() {
            let client = match self.clients.get_mut(*client_id) {
                Some(client) => client,
                None => continue,
            };

            // Defense-in-depth: a watcher whose current action points at a
            // different channel must not receive this channel's frames,
            // even if its id is still (incorrectly) present in this
            // watcher set. The handle comparison is the old stream-key
            // comparison one notch stricter: handles also differ across
            // same-key channel incarnations (remove + recreate), where a
            // corrupt leftover membership must not resume delivery from
            // the new incarnation.
            let active_stream_id = match &client.current_action {
                ClientAction::Watching {
                    stream_key: watched_stream_key,
                    stream_id,
                    channel: watched_channel,
                } => {
                    if *watched_channel != channel_handle {
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

            // The keyframe gate trusts the container flag: is_keyframe
            // means the encoder flagged this packet as a keyframe (FLV
            // frame type 1, AVC NALU packet type 0x01). The NAL payload
            // is not inspected, so a mis-flagged packet passes the
            // gate. FFmpeg's flvdec (libavformat/flvdec.c derives
            // AV_PKT_FLAG_KEY from the same frame-type nibble) and
            // mainstream RTMP servers place the same trust in the
            // container flag, which keeps this per-packet path free of
            // NAL parsing.
            if is_video && is_keyframe {
                client.has_received_video_keyframe = true;
            }

            let wire_bytes = match serialized_groups
                .iter()
                .find(|(stream_id, _)| *stream_id == active_stream_id)
            {
                Some((_, bytes)) => bytes.clone(),
                None => match serialize_media(
                    fanout_serializer,
                    data_type,
                    active_stream_id,
                    data.clone(),
                    timestamp,
                    true,
                ) {
                    Ok(packet) => {
                        let bytes = Bytes::from(packet.bytes);
                        serialized_groups.push((active_stream_id, bytes.clone()));
                        bytes
                    }
                    Err(error) => {
                        // Deterministic for the (message, group) pair, so
                        // every watcher of the group lands here — the same
                        // per-watcher disconnect the per-session path
                        // produced when its send failed.
                        let data_type_str = if is_video { "video" } else { "audio" };
                        debug!(
                            "Rtmp error serializing {} data for client on connection id {}: {:?}",
                            data_type_str, client.connection_id, error
                        );
                        server_results.push(ServerResult::DisconnectConnection {
                            connection_id: client.connection_id,
                        });
                        continue;
                    }
                },
            };

            server_results.push(ServerResult::OutboundPacket {
                target_connection_id: client.connection_id,
                bytes: wire_bytes,
                can_be_dropped: true,
                is_keyframe,
                is_sequence_header,
                is_video,
            });
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
pub(super) fn should_send_to_watcher(
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
