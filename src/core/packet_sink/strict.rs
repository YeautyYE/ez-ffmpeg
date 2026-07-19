//! Strict-tier packet-sink runtime: configuration collection at the header
//! slot, per-packet validation and delivery at the write slot, terminal
//! events at the trailer slot.
//!
//! The processing order at the write slot is fixed (progress accounting via
//! `update_last_dts` already ran in the worker loop, on the original
//! timeline, before this code sees the packet):
//!
//! 1. S8 configuration-change detection (side data vs. the seeded baseline);
//! 2. S7 timestamp validation: reject missing timestamps, then shift onto the
//!    shared origin, then enforce `pts >= dts`, strictly monotonic dts, and
//!    no duplicate pts;
//! 3. payload normalization to a 4-byte length-prefixed access unit (Annex-B
//!    packets are rewritten; already length-prefixed packets are validated in
//!    place) and the in-band parameter-set policy;
//! 4. `is_key` determination (IDR presence, not the raw key flag);
//! 5. duration (encoder value passed through, derived when absent, error when
//!    underivable);
//! 6. `on_packet` delivery; a negative return breaks the worker loop through
//!    the same path a failed container write would take.
//!
//! Every violation is a typed [`PacketSinkError`], stashed here and published
//! as the job error by the worker; the write slot itself only speaks the
//! muxer's `i32` convention (a sentinel that is never `AVERROR_EOF`, so a
//! failing callback can never masquerade as a healthy end of stream).

use super::avcc;
use super::{PacketSink, PacketStreamInfo, PacketView};
use crate::core::context::PacketBox;
use crate::error::PacketSinkError;
use ffmpeg_sys_next::AVMediaType::{AVMEDIA_TYPE_AUDIO, AVMEDIA_TYPE_VIDEO};
use ffmpeg_sys_next::AVPacketSideDataType::{AV_PKT_DATA_NEW_EXTRADATA, AV_PKT_DATA_PARAM_CHANGE};
use ffmpeg_next::packet::Ref;
use ffmpeg_sys_next::{
    av_get_audio_frame_duration2, av_rescale_q, av_rescale_q_rnd, avcodec_get_name,
    AVCodecParameters, AVFormatContext, AVRational, AVRounding, AVERROR_EXTERNAL, AV_NOPTS_VALUE,
};
use std::ffi::CStr;

/// Codec-specific handling of one output stream.
enum StreamKind {
    /// H.264 in the strict tier. `annexb_packets` is decided once, from the
    /// extradata form: Annex-B extradata means Annex-B packets (rewritten per
    /// packet), avcC extradata means already length-prefixed packets
    /// (validated in place). The forms never mix within one encoder.
    H264 { annexb_packets: bool },
    /// AAC raw frames; the AudioSpecificConfig travels in the stream info.
    Aac,
}

/// S8 baseline: the canonical stream configuration seeded before delivery.
struct ConfigBaseline {
    /// Canonical parameter sets (video) — wrapper-independent fingerprint.
    parameter_sets: Option<avcc::ParameterSets>,
    /// Raw extradata bytes (audio ASC).
    extradata: Vec<u8>,
    /// Typed projections compared against `AV_PKT_DATA_PARAM_CHANGE`.
    width: i32,
    height: i32,
    sample_rate: i32,
}

/// Per-stream runtime state.
struct StreamState {
    kind: StreamKind,
    time_base: AVRational,
    frame_rate: Option<AVRational>,
    baseline: ConfigBaseline,
    /// Origin shift for this stream, in its own time base; set for every
    /// stream the moment the first packet of the job is delivered.
    applied_offset: Option<i64>,
    /// Last delivered dts (shifted timeline), for strict monotonicity.
    last_dts: Option<i64>,
    /// Shifted pts values still above the dts watermark. Since `pts >= dts`
    /// holds and dts strictly increases, any pts at or below the current dts
    /// can never repeat — pruning keeps this at the encoder's reorder depth.
    pending_pts: Vec<i64>,
}

/// Strict-tier worker state. Built at the header slot (before any callback),
/// moved into the mux worker, consumed at the trailer slot.
pub(crate) struct PacketSinkWorker {
    sink: PacketSink,
    infos: Vec<PacketStreamInfo>,
    streams: Vec<StreamState>,
    /// Shared origin: `(dts, time_base)` of the first delivered packet.
    origin: Option<(i64, AVRational)>,
    /// First delivery-path error; cloned into the job result and handed to
    /// `on_error` at the terminal slot.
    pending_error: Option<PacketSinkError>,
    /// Reused Annex-B -> AVCC conversion buffer.
    scratch: Vec<u8>,
    stream_info_delivered: bool,
}

impl PacketSinkWorker {
    /// Collects and validates the strict-tier stream configuration from the
    /// (never-written) output context. Runs at the header slot: after every
    /// encoder finalized its parameters, before any callback. Any error here
    /// fails the job **without a single callback having run**.
    ///
    /// # Safety
    /// - `out_fmt_ctx` must be a valid output context with at least
    ///   `stream_count` streams whose `codecpar` were finalized by their
    ///   encoders (`avcodec_parameters_from_context` already ran).
    pub(crate) unsafe fn collect(
        out_fmt_ctx: *mut AVFormatContext,
        stream_count: usize,
        sink: PacketSink,
    ) -> Result<Self, PacketSinkError> {
        // Tier dispatch: only Strict exists; new tiers add arms here.
        let super::PacketSinkTier::Strict = sink.tier;

        let mut infos = Vec::with_capacity(stream_count);
        let mut streams = Vec::with_capacity(stream_count);
        for stream_index in 0..stream_count {
            let st = *(*out_fmt_ctx).streams.add(stream_index);
            let codecpar = (*st).codecpar;
            let media_type = (*codecpar).codec_type;
            let codec_name = CStr::from_ptr(avcodec_get_name((*codecpar).codec_id))
                .to_string_lossy()
                .into_owned();
            let time_base = (*st).time_base;
            let extradata = extradata_bytes(codecpar)
                .ok_or(PacketSinkError::MissingExtradata { stream_index })?;

            let (kind, delivered_extradata, baseline, frame_rate) = match media_type {
                AVMEDIA_TYPE_VIDEO => {
                    // The encoder whitelist was enforced at build time; this
                    // guards the codec id itself (h264 only in v1).
                    if (*codecpar).codec_id != ffmpeg_sys_next::AVCodecID::AV_CODEC_ID_H264 {
                        return Err(PacketSinkError::UnsupportedStream { kind: "non-H.264 video" });
                    }
                    let annexb_packets = extradata.first() != Some(&1);
                    let sets = avcc::parse_parameter_sets(&extradata).map_err(|reason| {
                        PacketSinkError::InvalidExtradata {
                            stream_index,
                            reason,
                        }
                    })?;
                    // Deliver the configuration as avcC: pass a pre-existing
                    // avcC through after validation (FFmpeg does not rewrite
                    // one), synthesize it from Annex-B parameter sets
                    // otherwise. The strict-tier structural checks (version 1,
                    // >=1 SPS/PPS, 4-byte lengths) hold on both paths.
                    let delivered = if annexb_packets {
                        avcc::build_avcc(&sets).map_err(|reason| {
                            PacketSinkError::InvalidExtradata {
                                stream_index,
                                reason,
                            }
                        })?
                    } else {
                        extradata.clone()
                    };
                    let fr = (*st).avg_frame_rate;
                    let frame_rate = (fr.num > 0 && fr.den > 0).then_some(fr);
                    (
                        StreamKind::H264 { annexb_packets },
                        delivered,
                        ConfigBaseline {
                            parameter_sets: Some(sets),
                            extradata,
                            width: (*codecpar).width,
                            height: (*codecpar).height,
                            sample_rate: 0,
                        },
                        frame_rate,
                    )
                }
                AVMEDIA_TYPE_AUDIO => {
                    if (*codecpar).codec_id != ffmpeg_sys_next::AVCodecID::AV_CODEC_ID_AAC {
                        return Err(PacketSinkError::UnsupportedStream { kind: "non-AAC audio" });
                    }
                    (
                        StreamKind::Aac,
                        extradata.clone(),
                        ConfigBaseline {
                            parameter_sets: None,
                            extradata,
                            width: 0,
                            height: 0,
                            sample_rate: (*codecpar).sample_rate,
                        },
                        None,
                    )
                }
                _ => {
                    return Err(PacketSinkError::UnsupportedStream {
                        kind: "non-audio/video",
                    })
                }
            };

            infos.push(PacketStreamInfo {
                stream_index,
                media_type,
                codec_name,
                time_base,
                extradata: delivered_extradata,
                width: (*codecpar).width,
                height: (*codecpar).height,
                frame_rate,
                sample_rate: (*codecpar).sample_rate,
                channels: (*codecpar).ch_layout.nb_channels,
            });
            streams.push(StreamState {
                kind,
                time_base,
                frame_rate,
                baseline,
                applied_offset: None,
                last_dts: None,
                pending_pts: Vec::new(),
            });
        }

        Ok(Self {
            sink,
            infos,
            streams,
            origin: None,
            pending_error: None,
            scratch: Vec::new(),
            stream_info_delivered: false,
        })
    }

    /// Invokes `on_stream_info` exactly once, on the worker thread, before
    /// any packet. Returns `0` or the sentinel error code.
    pub(crate) fn deliver_stream_info(&mut self) -> i32 {
        debug_assert!(!self.stream_info_delivered, "on_stream_info delivered twice");
        self.stream_info_delivered = true;
        let code = (self.sink.on_stream_info)(&self.infos);
        if code < 0 {
            self.pending_error = Some(PacketSinkError::StreamInfoCallbackFailed { code });
            return AVERROR_EXTERNAL;
        }
        0
    }

    /// The first delivery-path error, for publication as the job error.
    pub(crate) fn pending_error_cloned(&self) -> Option<PacketSinkError> {
        self.pending_error.clone()
    }

    /// Validates and delivers one packet at the write slot. Returns `0` on
    /// success or `AVERROR_EXTERNAL` (never `AVERROR_EOF`) with the typed
    /// error stashed.
    ///
    /// # Safety
    /// - `out_fmt_ctx` must be the live output context this worker was
    ///   collected from, and `packet_box.packet_data.output_stream_index`
    ///   must be a valid stream index of it (same contract as `write_packet`).
    /// - `packet_box.packet` must wrap a live packet.
    pub(crate) unsafe fn process_and_deliver(
        &mut self,
        out_fmt_ctx: *const AVFormatContext,
        packet_box: &mut PacketBox,
    ) -> i32 {
        match self.process(out_fmt_ctx, packet_box) {
            Ok(()) => 0,
            Err(e) => {
                if self.pending_error.is_none() {
                    self.pending_error = Some(e);
                }
                AVERROR_EXTERNAL
            }
        }
    }

    unsafe fn process(
        &mut self,
        out_fmt_ctx: *const AVFormatContext,
        packet_box: &mut PacketBox,
    ) -> Result<(), PacketSinkError> {
        let stream_index = packet_box.packet_data.output_stream_index as usize;
        debug_assert!(stream_index < self.streams.len());
        let pkt = packet_box.packet.as_ptr();

        // 1. S8: mid-stream configuration change detection.
        check_side_data(&self.streams[stream_index], stream_index, pkt)?;

        // 2. S7: timestamps. Missing values are rejected before anything else
        // (the origin cannot anchor on AV_NOPTS_VALUE).
        let orig_pts = (*pkt).pts;
        let orig_dts = (*pkt).dts;
        if orig_dts == AV_NOPTS_VALUE {
            return Err(PacketSinkError::MissingTimestamp {
                stream_index,
                which: "dts",
            });
        }
        if orig_pts == AV_NOPTS_VALUE {
            return Err(PacketSinkError::MissingTimestamp {
                stream_index,
                which: "pts",
            });
        }

        // Anchor the shared origin on the first delivered packet and derive
        // every stream's offset from it (delivery order == arrival order).
        if self.origin.is_none() {
            let tb0 = (*pkt).time_base;
            self.origin = Some((orig_dts, tb0));
            for (i, stream) in self.streams.iter_mut().enumerate() {
                let offset = av_rescale_q_rnd(
                    orig_dts,
                    tb0,
                    stream.time_base,
                    AVRounding::AV_ROUND_NEAR_INF,
                );
                if offset == i64::MIN {
                    return Err(PacketSinkError::TimestampOverflow { stream_index: i });
                }
                stream.applied_offset = Some(offset);
            }
        }
        let state = &mut self.streams[stream_index];
        let applied_offset = state
            .applied_offset
            .expect("origin anchored above for every stream");
        let pts = orig_pts
            .checked_sub(applied_offset)
            .ok_or(PacketSinkError::TimestampOverflow { stream_index })?;
        let dts = orig_dts
            .checked_sub(applied_offset)
            .ok_or(PacketSinkError::TimestampOverflow { stream_index })?;

        if pts < dts {
            return Err(PacketSinkError::PtsBeforeDts {
                stream_index,
                pts,
                dts,
            });
        }
        if let Some(prev) = state.last_dts {
            if dts <= prev {
                return Err(PacketSinkError::NonMonotonicDts {
                    stream_index,
                    prev,
                    current: dts,
                });
            }
        }
        // Duplicate-pts window: `pts >= dts` plus strictly increasing dts
        // means any recorded pts at or below the new dts can never repeat.
        state.pending_pts.retain(|&p| p > dts);
        if state.pending_pts.contains(&pts) {
            return Err(PacketSinkError::DuplicatePts { stream_index, pts });
        }
        state.pending_pts.push(pts);
        state.last_dts = Some(dts);

        // 3.-4. Payload normalization + is_key.
        let size = (*pkt).size;
        let data_ptr = (*pkt).data;
        if data_ptr.is_null() || size <= 0 {
            return Err(PacketSinkError::MalformedPacket {
                stream_index,
                reason: "empty payload (a packet must carry one complete frame)".to_string(),
            });
        }
        let payload = std::slice::from_raw_parts(data_ptr, size as usize);
        let malformed = |reason: String| PacketSinkError::MalformedPacket {
            stream_index,
            reason,
        };
        let (is_key, data): (bool, &[u8]) = match &state.kind {
            StreamKind::H264 { annexb_packets } => {
                let nals = if *annexb_packets {
                    avcc::split_annexb(payload).map_err(malformed)?
                } else {
                    avcc::length_prefixed_nals(payload).map_err(malformed)?
                };
                let scan = avcc::au_scan(&nals);
                if scan.has_parameter_set {
                    check_inband_parameter_sets(state, stream_index, &nals)?;
                    // Equal to the baseline is still not deliverable: the
                    // strict tier keeps configuration out-of-band.
                    return Err(PacketSinkError::InBandParameterSets { stream_index });
                }
                let data: &[u8] = if *annexb_packets {
                    avcc::write_length_prefixed(&nals, &mut self.scratch)
                        .map_err(|reason| PacketSinkError::MalformedPacket {
                            stream_index,
                            reason,
                        })?;
                    &self.scratch
                } else {
                    payload
                };
                (scan.has_idr, data)
            }
            // Every AAC frame is a random access point; the raw frame passes
            // through unchanged.
            StreamKind::Aac => (true, payload),
        };

        // 5. Duration: pass the encoder's through; derive when absent; a
        // value that stays underivable is an error, never a silent zero.
        let state = &self.streams[stream_index];
        let mut duration = (*pkt).duration;
        if duration < 0 {
            return Err(PacketSinkError::MissingDuration { stream_index });
        }
        if duration == 0 {
            duration = match &state.kind {
                StreamKind::H264 { .. } => match state.frame_rate {
                    // One CFR frame interval, in stream time-base ticks.
                    Some(fr) => av_rescale_q(
                        1,
                        AVRational {
                            num: fr.den,
                            den: fr.num,
                        },
                        state.time_base,
                    ),
                    None => 0,
                },
                StreamKind::Aac => {
                    let codecpar: *mut AVCodecParameters =
                        (**(*out_fmt_ctx).streams.add(stream_index)).codecpar;
                    let samples = av_get_audio_frame_duration2(codecpar, size);
                    let sample_rate = (*codecpar).sample_rate;
                    if samples > 0 && sample_rate > 0 {
                        av_rescale_q(
                            samples as i64,
                            AVRational {
                                num: 1,
                                den: sample_rate,
                            },
                            state.time_base,
                        )
                    } else {
                        0
                    }
                }
            };
        }
        if duration <= 0 {
            return Err(PacketSinkError::MissingDuration { stream_index });
        }

        // 6. Deliver. The borrowed view (including `data`) dies with the call.
        let view = PacketView {
            stream_index,
            pts,
            dts,
            duration,
            time_base: state.time_base,
            is_key,
            applied_offset,
            data,
        };
        let code = (self.sink.on_packet)(&view);
        if code < 0 {
            return Err(PacketSinkError::PacketCallbackFailed { stream_index, code });
        }
        Ok(())
    }

    /// Terminal slot (where the muxer would write its trailer). Fires
    /// `on_end` only through the strong gate; fires `on_error` for a stashed
    /// delivery-path error; stays silent for cancellation and failures
    /// elsewhere in the pipeline (the job result carries those).
    pub(crate) fn finish(&mut self, all_streams_terminal: bool, ret: i32, aborted: bool, task_error: bool) {
        if aborted {
            return;
        }
        if let Some(e) = self.pending_error.take() {
            (self.sink.on_error)(&e);
            return;
        }
        if ret >= 0 && all_streams_terminal && !task_error {
            (self.sink.on_end)();
        }
    }
}

/// Owned copy of a stream's extradata, `None` when absent/empty.
unsafe fn extradata_bytes(codecpar: *const AVCodecParameters) -> Option<Vec<u8>> {
    let ptr = (*codecpar).extradata;
    let size = (*codecpar).extradata_size;
    if ptr.is_null() || size <= 0 {
        return None;
    }
    Some(std::slice::from_raw_parts(ptr, size as usize).to_vec())
}

/// S8: compare packet side data against the seeded baseline. Redundant
/// (value-equal) announcements pass; any true change is a typed error.
unsafe fn check_side_data(
    state: &StreamState,
    stream_index: usize,
    pkt: *const ffmpeg_sys_next::AVPacket,
) -> Result<(), PacketSinkError> {
    for i in 0..(*pkt).side_data_elems {
        let sd = (*pkt).side_data.add(i as usize);
        let sd_type = (*sd).type_;
        if sd_type == AV_PKT_DATA_NEW_EXTRADATA {
            let bytes = std::slice::from_raw_parts((*sd).data, (*sd).size);
            match (&state.kind, &state.baseline.parameter_sets) {
                (StreamKind::H264 { .. }, Some(baseline_sets)) => {
                    // Normalize before comparing: the same parameter sets can
                    // arrive as Annex-B, avcC or raw side data; wrapper bytes
                    // would misreport an unchanged configuration.
                    let sets = avcc::parse_parameter_sets(bytes).map_err(|reason| {
                        PacketSinkError::ConfigChange {
                            stream_index,
                            what: format!("unparseable NEW_EXTRADATA ({reason})"),
                        }
                    })?;
                    if &sets != baseline_sets {
                        return Err(PacketSinkError::ConfigChange {
                            stream_index,
                            what: "NEW_EXTRADATA carries different parameter sets".to_string(),
                        });
                    }
                }
                _ => {
                    if bytes != state.baseline.extradata.as_slice() {
                        return Err(PacketSinkError::ConfigChange {
                            stream_index,
                            what: "NEW_EXTRADATA differs from the stream configuration"
                                .to_string(),
                        });
                    }
                }
            }
        } else if sd_type == AV_PKT_DATA_PARAM_CHANGE {
            check_param_change(
                state,
                stream_index,
                std::slice::from_raw_parts((*sd).data, (*sd).size),
            )?;
        }
    }
    Ok(())
}

/// Layout check + typed-field projection compare for `AV_PKT_DATA_PARAM_CHANGE`
/// (little-endian `u32 flags`, then the fields the flags select — the side
/// data carries no parameter sets, so only its typed fields are compared).
fn check_param_change(
    state: &StreamState,
    stream_index: usize,
    data: &[u8],
) -> Result<(), PacketSinkError> {
    const SAMPLE_RATE_FLAG: u32 =
        ffmpeg_sys_next::AVSideDataParamChangeFlags::AV_SIDE_DATA_PARAM_CHANGE_SAMPLE_RATE as u32;
    const DIMENSIONS_FLAG: u32 =
        ffmpeg_sys_next::AVSideDataParamChangeFlags::AV_SIDE_DATA_PARAM_CHANGE_DIMENSIONS as u32;

    let malformed = |reason: &str| PacketSinkError::MalformedPacket {
        stream_index,
        reason: format!("PARAM_CHANGE side data: {reason}"),
    };
    let mut read_i32 = {
        let mut pos = 0usize;
        move |data: &[u8]| -> Result<i32, PacketSinkError> {
            let bytes: [u8; 4] = data
                .get(pos..pos + 4)
                .and_then(|s| s.try_into().ok())
                .ok_or_else(|| malformed("truncated"))?;
            pos += 4;
            Ok(i32::from_le_bytes(bytes))
        }
    };

    let flags = read_i32(data)? as u32;
    if flags & !(SAMPLE_RATE_FLAG | DIMENSIONS_FLAG) != 0 {
        return Err(PacketSinkError::ConfigChange {
            stream_index,
            what: format!("PARAM_CHANGE with unrecognized flags {flags:#x}"),
        });
    }
    if flags & SAMPLE_RATE_FLAG != 0 {
        let sample_rate = read_i32(data)?;
        if sample_rate != state.baseline.sample_rate {
            return Err(PacketSinkError::ConfigChange {
                stream_index,
                what: format!(
                    "sample rate changed {} -> {sample_rate}",
                    state.baseline.sample_rate
                ),
            });
        }
    }
    if flags & DIMENSIONS_FLAG != 0 {
        let width = read_i32(data)?;
        let height = read_i32(data)?;
        if width != state.baseline.width || height != state.baseline.height {
            return Err(PacketSinkError::ConfigChange {
                stream_index,
                what: format!(
                    "dimensions changed {}x{} -> {width}x{height}",
                    state.baseline.width, state.baseline.height
                ),
            });
        }
    }
    Ok(())
}

/// In-band SPS/PPS: sets differing from the baseline are a configuration
/// change; value-equal sets fall through to the strict-tier in-band rejection
/// at the caller.
fn check_inband_parameter_sets(
    state: &StreamState,
    stream_index: usize,
    nals: &[&[u8]],
) -> Result<(), PacketSinkError> {
    let Some(baseline) = &state.baseline.parameter_sets else {
        return Ok(());
    };
    for nal in nals {
        let matches_baseline = match nal[0] & 0x1F {
            avcc::NAL_SPS => baseline.sps.iter().any(|s| s.as_slice() == *nal),
            avcc::NAL_PPS => baseline.pps.iter().any(|p| p.as_slice() == *nal),
            _ => continue,
        };
        if !matches_baseline {
            return Err(PacketSinkError::ConfigChange {
                stream_index,
                what: "in-band SPS/PPS differ from the stream configuration".to_string(),
            });
        }
    }
    Ok(())
}
