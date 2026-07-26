//! Everything a late joiner receives: the join-replay byte budget, the
//! replay-start selection over cached GOP segments and the burst build,
//! plus the `play` request handling that drives them.

use super::{
    new_media_serializer, serialize_media, Client, ClientAction, MediaChannel,
    ReceivedDataType, RtmpScheduler, ServerResult, OUTBOUND_CHUNK_SIZE,
};
use crate::flv::flv_tag_body::{
    is_audio_sequence_header, is_video_keyframe, is_video_sequence_header,
};
use crate::rtmp::gop::FrameData;
use crate::rtmp::write_queue::QUEUE_WARN_BYTES;
use log::debug;
use rml_rtmp::sessions::ServerSessionResult;

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
pub(super) const JOIN_REPLAY_BUDGET_BYTES: usize = QUEUE_WARN_BYTES - JOIN_REPLAY_HEADROOM;

/// Upper bound on a type-0 RTMP chunk header: 3-byte basic header + 11-byte
/// message header + 4-byte extended timestamp. Every media frame is serialized
/// as its own packet, and droppable A/V frames are forced back to a type-0
/// header by the serializer, so budget one full message header per frame.
pub(super) const MSG_HEADER_MAX: usize = 18;

/// Upper bound on a type-3 continuation chunk header (basic header only). Added
/// once per outbound chunk a payload is split across. The join budget caps the
/// replayed payload well under 1 MiB, so the rare extended-timestamp bytes a
/// continuation may also carry stay far inside the 64 KiB headroom.
pub(super) const CONT_HEADER_MAX: usize = 3;

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
pub(super) fn gop_wire_size(payload: usize, frame_count: usize) -> usize {
    let per_frame = frame_count.saturating_mul(MSG_HEADER_MAX);
    let continuation = payload
        .div_ceil(OUTBOUND_CHUNK_SIZE)
        .saturating_mul(CONT_HEADER_MAX);
    payload
        .saturating_add(per_frame)
        .saturating_add(continuation)
}

impl RtmpScheduler {
    /// Fold the `server_results` entries added since the previous `play` in this
    /// batch into the running same-batch prefix for `target`, advancing the scan
    /// cursor. Every entry is visited at most once across all plays in a batch
    /// (all of which share the serviced connection), so N repeated plays cost
    /// O(N) total rather than the O(N^2) of rescanning the whole vec each time.
    /// Saturating so a pathological batch cannot wrap the accumulator.
    pub(super) fn advance_serving_prefix(&mut self, server_results: &[ServerResult], target: usize) -> usize {
        while self.serving_prefix_scan_pos < server_results.len() {
            if let ServerResult::OutboundPacket {
                target_connection_id,
                bytes,
                ..
            } = &server_results[self.serving_prefix_scan_pos]
            {
                if *target_connection_id == target {
                    self.serving_prefix_bytes =
                        self.serving_prefix_bytes.saturating_add(bytes.len());
                }
            }
            self.serving_prefix_scan_pos += 1;
        }
        self.serving_prefix_bytes
    }

    pub(super) fn handle_play_requested(
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
        // and the old stream's next live keyframe could wrongly re-open the
        // keyframe gate for the new stream.
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
            // another stream (and saw its keyframe) must not carry that True over and
            // let should_send_to_watcher forward this stream's delta frames before a
            // keyframe is replayed or arrives live. Replay of a GOP carrying a
            // keyframe, or a later live keyframe, re-sets it.
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
}

/// The index of the first GOP segment to replay to a joining watcher: the
/// longest suffix of `sizes` whose byte total stays within `budget`. Whole
/// segments only — a GOP entered mid-way hands the watcher delta frames whose
/// opening keyframe was trimmed away, which decodes as a smeared picture.
///
/// The accumulation uses `checked_add` so a corrupt or absurd segment size
/// fails closed (older segments dropped) instead of wrapping around and
/// admitting the entire cache.
pub(super) fn select_replay_start(sizes: &[usize], budget: usize) -> usize {
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
/// reference the current GOP's opening keyframe, so skipping it smears the
/// picture until the next keyframe arrives.
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
pub(super) fn join_replay_prefix_bytes(
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

/// A watcher must receive a video keyframe before any delta frame it is
/// expected to decode. Only the first replayed segment can be keyframeless —
/// either the pre-first-keyframe headers+audio, or (mid-GOP publish start) a
/// run of deltas with no keyframe. Replay only audio from such a segment and
/// do not flip the keyframe gate until a segment carrying a flagged keyframe
/// is reached; from then on every replayed video frame follows its GOP's
/// keyframe (GOPs freeze on keyframe boundaries).
pub(super) fn build_join_burst(
    channel: &MediaChannel,
    client: &mut Client,
    connection_id: usize,
    stream_id: u32,
    accept_prefix_bytes: usize,
    out: &mut Vec<ServerResult>,
) {
    // Every media frame of THIS burst is serialized on a one-shot throwaway
    // serializer, not the client's session serializer. The burst frames are
    // non-droppable, and serializing them on the session serializer would
    // leave compressible (non-droppable) entries in its csid 4/5 header
    // history — history the shared live path then never advances. A later
    // `play` on the same connection would serialize its second burst
    // against that stale history and could emit delta headers relative to
    // a frame sent an entire live session ago, while the peer's csid state
    // has long been replaced by the shared type-0 stream: wire-level
    // timestamp corruption. A fresh serializer per burst starts its csid
    // 4/5 chains with full type-0 headers, and any compression WITHIN the
    // burst resolves against frames the peer consumes in order right
    // before — self-contained by construction, for every play of this
    // connection, forever.
    //
    // The metadata packet stays on the session serializer: metadata rides
    // csid 3 (Amf0Data), a chunk stream the shared media path never emits,
    // so its header-compression chain lives entirely inside the session
    // serializer and stays consistent at the peer.
    let mut burst_serializer = new_media_serializer();

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
                out.push(ServerResult::outbound(
                    connection_id,
                    packet,
                    false,
                    false,
                    false,
                ));
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
        match serialize_media(
            &mut burst_serializer,
            ReceivedDataType::Video,
            stream_id,
            data.clone(),
            channel.video_timestamp,
            false,
        ) {
            Ok(packet) => {
                prefix_wire_bytes = prefix_wire_bytes.saturating_add(packet.bytes.len());
                out.push(ServerResult::outbound(
                    connection_id,
                    packet,
                    false,
                    true,
                    true,
                ));
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
        match serialize_media(
            &mut burst_serializer,
            ReceivedDataType::Audio,
            stream_id,
            data.clone(),
            channel.audio_timestamp,
            false,
        ) {
            Ok(packet) => {
                prefix_wire_bytes = prefix_wire_bytes.saturating_add(packet.bytes.len());
                out.push(ServerResult::outbound(
                    connection_id,
                    packet,
                    false,
                    true,
                    false,
                ));
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

    let mut replayed_keyframe = false;
    for segment_index in start..sizes.len() {
        let frames = if segment_index < frozen.len() {
            frozen[segment_index].frames()
        } else {
            current_frames
        };
        if !replayed_keyframe && gop_contains_video_keyframe(frames) {
            replayed_keyframe = true;
            client.has_received_video_keyframe = true;
        }
        for frame_data in frames {
            match frame_data {
                FrameData::Video { timestamp, data } => {
                    // Skip pre-keyframe video (deltas whose reference frames
                    // were never sent); the AVC sequence header is already
                    // sent separately above.
                    if !replayed_keyframe {
                        continue;
                    }
                    let is_keyframe = is_video_keyframe(data);
                    let is_sequence_header = is_video_sequence_header(data);
                    match serialize_media(
                        &mut burst_serializer,
                        ReceivedDataType::Video,
                        stream_id,
                        data.clone(),
                        *timestamp,
                        false,
                    ) {
                        Ok(packet) => out.push(ServerResult::outbound(
                            connection_id,
                            packet,
                            is_keyframe,
                            is_sequence_header,
                            true,
                        )),
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
                    match serialize_media(
                        &mut burst_serializer,
                        ReceivedDataType::Audio,
                        stream_id,
                        data.clone(),
                        *timestamp,
                        false,
                    ) {
                        Ok(packet) => out.push(ServerResult::outbound(
                            connection_id,
                            packet,
                            false,
                            is_sequence_header,
                            false,
                        )),
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

/// Whether replaying `frames` hands a watcher a video keyframe: a frame the
/// encoder flagged as one (FLV frame type 1, AVC NALU packet type 0x01 — the
/// NAL payload is not inspected, see [`is_video_keyframe`]), not merely the
/// AVC sequence header or audio. Used to gate has_received_video_keyframe
/// during GOP replay: a GOP frozen before the first keyframe must not flip
/// the gate.
pub(super) fn gop_contains_video_keyframe(frames: &[FrameData]) -> bool {
    frames.iter().any(|f| match f {
        FrameData::Video { data, .. } => is_video_keyframe(data),
        FrameData::Audio { .. } => false,
    })
}
