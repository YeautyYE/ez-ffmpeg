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

#[cfg(test)]
mod tests {
    use super::*;
    use crate::core::context::PacketData;
    use crate::core::packet_sink::{PacketSink, PacketSinkEvent};
    use ffmpeg_next::packet::Mut;
    use ffmpeg_sys_next::{
        av_guess_format, av_mallocz, av_packet_new_side_data, avformat_alloc_output_context2,
        avformat_free_context, avformat_new_stream, AVCodecID, AVMediaType,
        AV_INPUT_BUFFER_PADDING_SIZE,
    };
    use std::ffi::CString;
    use std::ptr::{null, null_mut};
    use std::sync::{Arc, Mutex};

    const SPS: &[u8] = &[0x67, 66, 0xC0, 0x1E, 0xAC, 0xD9, 0x40];
    const PPS: &[u8] = &[0x68, 0xCE, 0x3C, 0x80];
    const OTHER_SPS: &[u8] = &[0x67, 66, 0xC0, 0x28, 0xAC, 0xD9, 0x41];

    fn annexb_config() -> Vec<u8> {
        let mut v = vec![0, 0, 0, 1];
        v.extend_from_slice(SPS);
        v.extend_from_slice(&[0, 0, 1]);
        v.extend_from_slice(PPS);
        v
    }

    /// An Annex-B IDR access unit (one slice NAL).
    fn idr_au() -> Vec<u8> {
        vec![0, 0, 0, 1, 0x65, 0x88, 0x84, 0x21, 0xFF]
    }

    /// An Annex-B non-IDR access unit.
    fn p_au() -> Vec<u8> {
        vec![0, 0, 0, 1, 0x41, 0x9A, 0x21, 0x03]
    }

    /// Owns a dummy output context whose streams carry synthesized codecpar.
    struct TestCtx {
        ctx: *mut ffmpeg_sys_next::AVFormatContext,
    }

    impl TestCtx {
        fn new() -> Self {
            unsafe {
                let name = CString::new("mp4").unwrap();
                let fmt = av_guess_format(name.as_ptr(), null(), null());
                assert!(!fmt.is_null());
                let mut ctx = null_mut();
                let ret = avformat_alloc_output_context2(&mut ctx, fmt, null(), null());
                assert!(ret >= 0 && !ctx.is_null());
                Self { ctx }
            }
        }

        unsafe fn add_stream(
            &self,
            media_type: AVMediaType,
            codec_id: AVCodecID,
            extradata: Option<&[u8]>,
            time_base: AVRational,
        ) -> usize {
            let st = avformat_new_stream(self.ctx, null());
            assert!(!st.is_null());
            let par = (*st).codecpar;
            (*par).codec_type = media_type;
            (*par).codec_id = codec_id;
            if media_type == AVMediaType::AVMEDIA_TYPE_VIDEO {
                (*par).width = 320;
                (*par).height = 240;
                (*st).avg_frame_rate = AVRational { num: 25, den: 1 };
            } else {
                (*par).sample_rate = 44100;
                (*par).ch_layout.nb_channels = 2;
                // Real audio encoders publish their frame size (AAC: 1024
                // samples); the duration derivation relies on it.
                (*par).frame_size = 1024;
            }
            if let Some(ed) = extradata {
                let buf =
                    av_mallocz(ed.len() + AV_INPUT_BUFFER_PADDING_SIZE as usize) as *mut u8;
                std::ptr::copy_nonoverlapping(ed.as_ptr(), buf, ed.len());
                (*par).extradata = buf;
                (*par).extradata_size = ed.len() as i32;
            }
            (*st).time_base = time_base;
            ((*self.ctx).nb_streams - 1) as usize
        }

        fn stream_count(&self) -> usize {
            unsafe { (*self.ctx).nb_streams as usize }
        }
    }

    impl Drop for TestCtx {
        fn drop(&mut self) {
            unsafe { avformat_free_context(self.ctx) }
        }
    }

    fn video_ctx() -> TestCtx {
        let ctx = TestCtx::new();
        unsafe {
            ctx.add_stream(
                AVMediaType::AVMEDIA_TYPE_VIDEO,
                AVCodecID::AV_CODEC_ID_H264,
                Some(&annexb_config()),
                AVRational { num: 1, den: 25 },
            );
        }
        ctx
    }

    /// A sink that logs every event; returns the sink and the log.
    fn recording_sink() -> (PacketSink, Arc<Mutex<Vec<PacketSinkEvent>>>) {
        let events: Arc<Mutex<Vec<PacketSinkEvent>>> = Arc::new(Mutex::new(Vec::new()));
        let (info_log, pkt_log, end_log, err_log) = (
            events.clone(),
            events.clone(),
            events.clone(),
            events.clone(),
        );
        let sink = PacketSink::builder()
            .on_stream_info(move |infos| {
                info_log
                    .lock()
                    .unwrap()
                    .push(PacketSinkEvent::StreamInfo(infos.to_vec()));
                0
            })
            .on_packet(move |view| {
                pkt_log.lock().unwrap().push(PacketSinkEvent::Packet(
                    crate::core::packet_sink::SinkPacket {
                        stream_index: view.stream_index(),
                        pts: view.pts(),
                        dts: view.dts(),
                        duration: view.duration(),
                        time_base: view.time_base(),
                        is_key: view.is_key(),
                        applied_offset: view.applied_offset(),
                        data: view.data().to_vec(),
                    },
                ));
                0
            })
            .on_end(move || end_log.lock().unwrap().push(PacketSinkEvent::End))
            .on_error(move |e| {
                err_log
                    .lock()
                    .unwrap()
                    .push(PacketSinkEvent::Error(e.clone()))
            })
            .build();
        (sink, events)
    }

    fn packet(data: &[u8], pts: i64, dts: i64, tb: AVRational, stream_index: i32) -> PacketBox {
        let mut packet = ffmpeg_next::Packet::copy(data);
        unsafe {
            let p = packet.as_mut_ptr();
            (*p).pts = pts;
            (*p).dts = dts;
            (*p).time_base = tb;
            (*p).stream_index = stream_index;
            (*p).duration = 0;
        }
        PacketBox {
            packet,
            packet_data: PacketData {
                dts_est: 0,
                codec_type: AVMediaType::AVMEDIA_TYPE_VIDEO,
                output_stream_index: stream_index,
                is_copy: false,
            },
        }
    }

    fn collect(ctx: &TestCtx, sink: PacketSink) -> Result<PacketSinkWorker, PacketSinkError> {
        unsafe { PacketSinkWorker::collect(ctx.ctx, ctx.stream_count(), sink) }
    }

    fn tb25() -> AVRational {
        AVRational { num: 1, den: 25 }
    }

    #[test]
    fn missing_extradata_fails_before_any_callback() {
        let ctx = TestCtx::new();
        unsafe {
            ctx.add_stream(
                AVMediaType::AVMEDIA_TYPE_VIDEO,
                AVCodecID::AV_CODEC_ID_H264,
                None,
                tb25(),
            );
        }
        let (sink, events) = recording_sink();
        let err = match collect(&ctx, sink) {
            Err(e) => e,
            Ok(_) => panic!("collect must fail without extradata"),
        };
        assert!(matches!(
            err,
            PacketSinkError::MissingExtradata { stream_index: 0 }
        ));
        assert!(
            events.lock().unwrap().is_empty(),
            "a configuration failure must precede every callback"
        );
    }

    #[test]
    fn collect_normalizes_annexb_extradata_to_avcc() {
        let ctx = video_ctx();
        let (sink, _events) = recording_sink();
        let worker = collect(&ctx, sink).unwrap();
        let info = &worker.infos[0];
        let avcc = info.extradata();
        assert_eq!(avcc[0], 1);
        assert_eq!(avcc[4] & 0x03, 3, "4-byte NAL length prefixes");
        assert!(avcc[5] & 0x1F >= 1, "at least one SPS");
        assert_eq!(info.codec_name(), "h264");
        assert_eq!(info.frame_rate(), Some(AVRational { num: 25, den: 1 }));
    }

    #[test]
    fn happy_path_delivers_avcc_au_and_derives_duration() {
        let ctx = video_ctx();
        let (sink, events) = recording_sink();
        let mut worker = collect(&ctx, sink).unwrap();
        assert_eq!(worker.deliver_stream_info(), 0);
        let mut pb = packet(&idr_au(), 4, 4, tb25(), 0);
        assert_eq!(
            unsafe { worker.process_and_deliver(ctx.ctx, &mut pb) },
            0
        );
        let events = events.lock().unwrap();
        assert!(matches!(events[0], PacketSinkEvent::StreamInfo(_)));
        let PacketSinkEvent::Packet(p) = &events[1] else {
            panic!("expected a packet event");
        };
        // Anchor stream: first delivered dts is 0, offset carries the shift.
        assert_eq!(p.dts(), 0);
        assert_eq!(p.pts(), 0);
        assert_eq!(p.applied_offset(), 4);
        // duration 0 was derived from the 25 fps frame rate: one tick at 1/25.
        assert_eq!(p.duration(), 1);
        assert!(p.is_key());
        // Payload is the length-prefixed rewrite of the Annex-B AU.
        assert_eq!(p.data()[..4], [0, 0, 0, 5]);
        assert_eq!(p.data()[4], 0x65);
    }

    #[test]
    fn non_idr_slice_is_not_key() {
        let ctx = video_ctx();
        let (sink, events) = recording_sink();
        let mut worker = collect(&ctx, sink).unwrap();
        let mut pb = packet(&p_au(), 0, 0, tb25(), 0);
        assert_eq!(unsafe { worker.process_and_deliver(ctx.ctx, &mut pb) }, 0);
        let events = events.lock().unwrap();
        let PacketSinkEvent::Packet(p) = &events[0] else {
            panic!("expected a packet event");
        };
        assert!(!p.is_key());
    }

    #[test]
    fn s7_rejects_missing_and_disordered_timestamps() {
        let ctx = video_ctx();
        let (sink, _events) = recording_sink();
        let mut worker = collect(&ctx, sink).unwrap();

        let mut nopts = packet(&idr_au(), ffmpeg_sys_next::AV_NOPTS_VALUE, 0, tb25(), 0);
        assert_eq!(
            unsafe { worker.process_and_deliver(ctx.ctx, &mut nopts) },
            AVERROR_EXTERNAL
        );
        assert!(matches!(
            worker.pending_error_cloned(),
            Some(PacketSinkError::MissingTimestamp { which: "pts", .. })
        ));

        // Fresh worker: pts earlier than dts.
        let (sink, _events) = recording_sink();
        let mut worker = collect(&ctx, sink).unwrap();
        let mut bad = packet(&idr_au(), 1, 2, tb25(), 0);
        assert_eq!(
            unsafe { worker.process_and_deliver(ctx.ctx, &mut bad) },
            AVERROR_EXTERNAL
        );
        assert!(matches!(
            worker.pending_error_cloned(),
            Some(PacketSinkError::PtsBeforeDts { .. })
        ));

        // Fresh worker: dts must strictly increase.
        let (sink, _events) = recording_sink();
        let mut worker = collect(&ctx, sink).unwrap();
        let mut first = packet(&idr_au(), 0, 0, tb25(), 0);
        assert_eq!(unsafe { worker.process_and_deliver(ctx.ctx, &mut first) }, 0);
        let mut stale = packet(&p_au(), 5, 0, tb25(), 0);
        assert_eq!(
            unsafe { worker.process_and_deliver(ctx.ctx, &mut stale) },
            AVERROR_EXTERNAL
        );
        assert!(matches!(
            worker.pending_error_cloned(),
            Some(PacketSinkError::NonMonotonicDts { .. })
        ));

        // Fresh worker: duplicate pts within the reorder window.
        let (sink, _events) = recording_sink();
        let mut worker = collect(&ctx, sink).unwrap();
        let mut a = packet(&idr_au(), 3, 0, tb25(), 0);
        assert_eq!(unsafe { worker.process_and_deliver(ctx.ctx, &mut a) }, 0);
        let mut b = packet(&p_au(), 3, 1, tb25(), 0);
        assert_eq!(
            unsafe { worker.process_and_deliver(ctx.ctx, &mut b) },
            AVERROR_EXTERNAL
        );
        assert!(matches!(
            worker.pending_error_cloned(),
            Some(PacketSinkError::DuplicatePts { pts: 3, .. })
        ));
    }

    #[test]
    fn duration_underivable_without_a_frame_rate_errors() {
        let ctx = TestCtx::new();
        unsafe {
            let i = ctx.add_stream(
                AVMediaType::AVMEDIA_TYPE_VIDEO,
                AVCodecID::AV_CODEC_ID_H264,
                Some(&annexb_config()),
                tb25(),
            );
            let st = *(*ctx.ctx).streams.add(i);
            (*st).avg_frame_rate = AVRational { num: 0, den: 0 };
        }
        let (sink, _events) = recording_sink();
        let mut worker = collect(&ctx, sink).unwrap();
        let mut pb = packet(&idr_au(), 0, 0, tb25(), 0);
        assert_eq!(
            unsafe { worker.process_and_deliver(ctx.ctx, &mut pb) },
            AVERROR_EXTERNAL
        );
        assert!(matches!(
            worker.pending_error_cloned(),
            Some(PacketSinkError::MissingDuration { .. })
        ));
    }

    #[test]
    fn s8_new_extradata_redundant_passes_and_change_errors() {
        let ctx = video_ctx();
        let (sink, _events) = recording_sink();
        let mut worker = collect(&ctx, sink).unwrap();

        // Redundant announcement (byte-identical Annex-B config): passes.
        let mut same = packet(&idr_au(), 0, 0, tb25(), 0);
        unsafe {
            let config = annexb_config();
            let sd = av_packet_new_side_data(
                same.packet.as_mut_ptr(),
                AV_PKT_DATA_NEW_EXTRADATA,
                config.len(),
            );
            assert!(!sd.is_null());
            std::ptr::copy_nonoverlapping(config.as_ptr(), sd, config.len());
        }
        assert_eq!(unsafe { worker.process_and_deliver(ctx.ctx, &mut same) }, 0);

        // A different SPS is a mid-stream configuration change.
        let mut changed = packet(&p_au(), 1, 1, tb25(), 0);
        unsafe {
            let mut config = vec![0, 0, 0, 1];
            config.extend_from_slice(OTHER_SPS);
            config.extend_from_slice(&[0, 0, 1]);
            config.extend_from_slice(PPS);
            let sd = av_packet_new_side_data(
                changed.packet.as_mut_ptr(),
                AV_PKT_DATA_NEW_EXTRADATA,
                config.len(),
            );
            assert!(!sd.is_null());
            std::ptr::copy_nonoverlapping(config.as_ptr(), sd, config.len());
        }
        assert_eq!(
            unsafe { worker.process_and_deliver(ctx.ctx, &mut changed) },
            AVERROR_EXTERNAL
        );
        assert!(matches!(
            worker.pending_error_cloned(),
            Some(PacketSinkError::ConfigChange { .. })
        ));
    }

    #[test]
    fn s8_param_change_projection_compares_typed_fields() {
        let ctx = video_ctx();

        // Equal dimensions: redundant, passes.
        let (sink, _events) = recording_sink();
        let mut worker = collect(&ctx, sink).unwrap();
        let mut same = packet(&idr_au(), 0, 0, tb25(), 0);
        unsafe {
            let mut payload = 8u32.to_le_bytes().to_vec(); // DIMENSIONS flag
            payload.extend_from_slice(&320i32.to_le_bytes());
            payload.extend_from_slice(&240i32.to_le_bytes());
            let sd = av_packet_new_side_data(
                same.packet.as_mut_ptr(),
                AV_PKT_DATA_PARAM_CHANGE,
                payload.len(),
            );
            assert!(!sd.is_null());
            std::ptr::copy_nonoverlapping(payload.as_ptr(), sd, payload.len());
        }
        assert_eq!(unsafe { worker.process_and_deliver(ctx.ctx, &mut same) }, 0);

        // A resolution change is rejected typed.
        let (sink, _events) = recording_sink();
        let mut worker = collect(&ctx, sink).unwrap();
        let mut changed = packet(&idr_au(), 0, 0, tb25(), 0);
        unsafe {
            let mut payload = 8u32.to_le_bytes().to_vec();
            payload.extend_from_slice(&640i32.to_le_bytes());
            payload.extend_from_slice(&480i32.to_le_bytes());
            let sd = av_packet_new_side_data(
                changed.packet.as_mut_ptr(),
                AV_PKT_DATA_PARAM_CHANGE,
                payload.len(),
            );
            assert!(!sd.is_null());
            std::ptr::copy_nonoverlapping(payload.as_ptr(), sd, payload.len());
        }
        assert_eq!(
            unsafe { worker.process_and_deliver(ctx.ctx, &mut changed) },
            AVERROR_EXTERNAL
        );
        assert!(matches!(
            worker.pending_error_cloned(),
            Some(PacketSinkError::ConfigChange { .. })
        ));
    }

    #[test]
    fn inband_parameter_sets_are_rejected() {
        let ctx = video_ctx();

        // Equal to the baseline: still out-of-band-only in the strict tier.
        let (sink, _events) = recording_sink();
        let mut worker = collect(&ctx, sink).unwrap();
        let mut au = annexb_config();
        au.extend_from_slice(&[0, 0, 1, 0x65, 0x88, 0x84]);
        let mut pb = packet(&au, 0, 0, tb25(), 0);
        assert_eq!(
            unsafe { worker.process_and_deliver(ctx.ctx, &mut pb) },
            AVERROR_EXTERNAL
        );
        assert!(matches!(
            worker.pending_error_cloned(),
            Some(PacketSinkError::InBandParameterSets { .. })
        ));

        // Different in-band sets are a configuration change.
        let (sink, _events) = recording_sink();
        let mut worker = collect(&ctx, sink).unwrap();
        let mut au = vec![0, 0, 0, 1];
        au.extend_from_slice(OTHER_SPS);
        au.extend_from_slice(&[0, 0, 1, 0x65, 0x88, 0x84]);
        let mut pb = packet(&au, 0, 0, tb25(), 0);
        assert_eq!(
            unsafe { worker.process_and_deliver(ctx.ctx, &mut pb) },
            AVERROR_EXTERNAL
        );
        assert!(matches!(
            worker.pending_error_cloned(),
            Some(PacketSinkError::ConfigChange { .. })
        ));
    }

    #[test]
    fn shared_origin_spans_streams_with_true_relative_offsets() {
        let ctx = TestCtx::new();
        unsafe {
            ctx.add_stream(
                AVMediaType::AVMEDIA_TYPE_VIDEO,
                AVCodecID::AV_CODEC_ID_H264,
                Some(&annexb_config()),
                tb25(),
            );
            ctx.add_stream(
                AVMediaType::AVMEDIA_TYPE_AUDIO,
                AVCodecID::AV_CODEC_ID_AAC,
                Some(&[0x12, 0x10]),
                AVRational { num: 1, den: 44100 },
            );
        }
        let (sink, events) = recording_sink();
        let mut worker = collect(&ctx, sink).unwrap();

        // First delivered packet: video dts 5 (at 1/25) anchors the origin.
        let mut v = packet(&idr_au(), 5, 5, tb25(), 0);
        assert_eq!(unsafe { worker.process_and_deliver(ctx.ctx, &mut v) }, 0);
        // Audio at original dts 0 keeps its true relative offset: negative.
        let mut a = packet(&[0x21, 0x10, 0x04], 0, 0, AVRational { num: 1, den: 44100 }, 1);
        a.packet_data.codec_type = AVMediaType::AVMEDIA_TYPE_AUDIO;
        assert_eq!(unsafe { worker.process_and_deliver(ctx.ctx, &mut a) }, 0);

        let events = events.lock().unwrap();
        let packets: Vec<_> = events
            .iter()
            .filter_map(|e| match e {
                PacketSinkEvent::Packet(p) => Some(p),
                _ => None,
            })
            .collect();
        assert_eq!(packets.len(), 2);
        assert_eq!(packets[0].stream_index(), 0);
        assert_eq!(packets[0].dts(), 0, "anchor stream starts at zero");
        assert_eq!(packets[0].applied_offset(), 5);
        // 5 ticks at 1/25 s = 0.2 s = 8820 ticks at 1/44100.
        assert_eq!(packets[1].applied_offset(), 8820);
        assert_eq!(packets[1].dts(), -8820, "earlier stream keeps its offset");
        assert!(packets[1].is_key(), "audio packets are always key");
        // AAC duration was derived from the codec frame size (1024 samples).
        assert_eq!(packets[1].duration(), 1024);
    }

    #[test]
    fn failing_packet_callback_stops_with_typed_error_and_no_on_end() {
        let ctx = video_ctx();
        let events: Arc<Mutex<Vec<PacketSinkEvent>>> = Arc::new(Mutex::new(Vec::new()));
        let (end_log, err_log) = (events.clone(), events.clone());
        let sink = PacketSink::builder()
            .on_packet(|_| -7)
            .on_end(move || end_log.lock().unwrap().push(PacketSinkEvent::End))
            .on_error(move |e| {
                err_log
                    .lock()
                    .unwrap()
                    .push(PacketSinkEvent::Error(e.clone()))
            })
            .build();
        let mut worker = collect(&ctx, sink).unwrap();
        let mut pb = packet(&idr_au(), 0, 0, tb25(), 0);
        let ret = unsafe { worker.process_and_deliver(ctx.ctx, &mut pb) };
        assert_eq!(ret, AVERROR_EXTERNAL);
        assert_ne!(ret, ffmpeg_sys_next::AVERROR_EOF);
        assert!(matches!(
            worker.pending_error_cloned(),
            Some(PacketSinkError::PacketCallbackFailed { code: -7, .. })
        ));

        // Terminal slot: the stashed error fires on_error, never on_end —
        // even if every stream were terminal.
        worker.finish(true, AVERROR_EXTERNAL, false, true);
        let events = events.lock().unwrap();
        assert_eq!(events.len(), 1);
        assert!(matches!(events[0], PacketSinkEvent::Error(_)));
    }

    #[test]
    fn finish_gate_matrix() {
        let ctx = video_ctx();
        let run = |all_done: bool, ret: i32, aborted: bool, task_error: bool| {
            let (sink, events) = recording_sink();
            let mut worker = collect(&ctx, sink).unwrap();
            worker.finish(all_done, ret, aborted, task_error);
            let events = events.lock().unwrap();
            events
                .iter()
                .filter(|e| matches!(e, PacketSinkEvent::End))
                .count()
        };
        assert_eq!(run(true, 0, false, false), 1, "healthy completion");
        assert_eq!(run(false, 0, false, false), 0, "streams not terminal");
        assert_eq!(run(true, -5, false, false), 0, "delivery error");
        assert_eq!(run(true, 0, true, false), 0, "abort");
        assert_eq!(run(true, 0, false, true), 0, "task error elsewhere");
    }
}
