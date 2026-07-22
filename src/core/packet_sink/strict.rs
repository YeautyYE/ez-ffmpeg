//! Strict-tier orchestrator: configuration collection at the header slot,
//! per-packet validation and delivery at the write slot, terminal events at
//! the trailer slot.
//!
//! This file owns control flow and phase state only; the mechanics live in
//! sibling modules — [`super::timeline`] (shared origin + per-stream S7
//! validation), [`super::codec`] (AVC/AAC configuration and payload
//! normalization over the streaming walkers in [`super::nal_framing`]), and
//! [`super::side_data`] (checked side-data iteration).
//!
//! The processing order at the write slot is fixed (progress accounting via
//! `update_last_dts` already ran in the worker loop, on the original
//! timeline, before this code sees the packet):
//!
//! 1. S8 configuration-change detection (side data vs. the seeded baseline)
//!    — a config change reports as such even when timestamps also broke;
//! 2. packet/stream time-base equality (the anchor math depends on it);
//! 3. S7 timestamp validation: reject missing timestamps, then shift onto the
//!    shared origin, then enforce `pts >= dts`, strictly monotonic dts, and
//!    no duplicate pts;
//! 4. payload normalization to a 4-byte length-prefixed access unit (Annex-B
//!    packets are rewritten; already length-prefixed packets are validated in
//!    place) and the in-band parameter-set policy;
//! 5. `is_key` determination (IDR presence, not the raw key flag);
//! 6. duration (encoder value passed through, derived when absent, error when
//!    underivable);
//! 7. `on_packet` delivery; a negative return breaks the worker loop through
//!    the same path a failed container write would take.
//!
//! Every violation is a typed [`PacketSinkError`], stashed here and published
//! as the job error by the worker; the write slot itself only speaks the
//! muxer's `i32` convention (a sentinel that is never `AVERROR_EOF`, so a
//! failing callback can never masquerade as a healthy end of stream).

use super::codec::{aac::AacRuntime, avc::AvcRuntime, CodecRuntime};
use super::side_data;
use super::timeline::{StreamTimeline, Timeline};
use super::{
    AudioPacketConfig, CallbackFailureKind, PacketCallbackError, PacketSink, PacketStreamInfo,
    PacketView, VideoPacketConfig,
};
use crate::core::context::PacketBox;
use crate::error::PacketSinkError;
use ffmpeg_next::packet::Ref;
use ffmpeg_sys_next::AVMediaType::{AVMEDIA_TYPE_AUDIO, AVMEDIA_TYPE_VIDEO};
use ffmpeg_sys_next::AVPacketSideDataType::{AV_PKT_DATA_NEW_EXTRADATA, AV_PKT_DATA_PARAM_CHANGE};
use ffmpeg_sys_next::{
    av_channel_layout_describe, av_get_audio_frame_duration2, av_rescale_q, AVCodecParameters,
    AVFormatContext, AVRational, AVERROR_EXTERNAL, AV_NOPTS_VALUE,
};
use std::sync::atomic::AtomicUsize;
use std::sync::Arc;

/// Typed projections compared against `AV_PKT_DATA_PARAM_CHANGE` (which
/// carries no parameter sets — only these fields).
struct ParamProjection {
    width: i32,
    height: i32,
    sample_rate: i32,
}

/// Per-stream runtime.
struct StreamRuntime {
    codec: CodecRuntime,
    time_base: AVRational,
    /// Duration substituted for durationless packets, in stream time-base
    /// ticks; 0 when underivable (such a packet then fails typed). Both
    /// derivations are stream-invariant — one CFR frame interval over the
    /// fixed frame rate for video, the fixed codec frame size over the
    /// sample rate for audio — so they are computed once at collection
    /// instead of per packet.
    derived_duration: i64,
    projection: ParamProjection,
    timeline: StreamTimeline,
}

/// Delivery phase: explicit, one-way transitions instead of correlated flags.
enum Phase {
    /// Configuration collected; `on_stream_info` has not run yet. No packet
    /// may be processed in this phase.
    Collected,
    /// Stream info delivered; packets flow.
    Running,
    /// Terminal callback dispatched (or deliberately suppressed); nothing may
    /// fire twice.
    Finished,
}

/// Why `process` stopped: a typed delivery error, or a cooperative
/// cancellation (a blocking channel send observed the job stopping — the
/// counterpart of the worker loop's own `is_stopping` exit, never an error).
enum ProcessFailure {
    Sink(PacketSinkError),
    Cancelled,
    /// Delivery truncated by a job failure recorded elsewhere: stop
    /// delivering, stash nothing — the terminal reads the recorded job
    /// error and reports `JobFailed`.
    JobStopped,
}

impl From<PacketSinkError> for ProcessFailure {
    fn from(e: PacketSinkError) -> Self {
        ProcessFailure::Sink(e)
    }
}

/// Maps a consumer callback error onto the delivery outcome.
fn callback_failure(
    error: PacketCallbackError,
    stream_index: Option<usize>,
) -> ProcessFailure {
    match error.kind {
        CallbackFailureKind::Cancelled => ProcessFailure::Cancelled,
        CallbackFailureKind::JobStopped => ProcessFailure::JobStopped,
        CallbackFailureKind::Disconnected => {
            ProcessFailure::Sink(PacketSinkError::ChannelDisconnected)
        }
        CallbackFailureKind::Failure => ProcessFailure::Sink(match stream_index {
            Some(stream_index) => PacketSinkError::PacketCallbackFailed {
                stream_index,
                error,
            },
            None => PacketSinkError::StreamInfoCallbackFailed { error },
        }),
    }
}

/// Strict-tier worker state. Built at the header slot (before any callback),
/// moved into the mux worker, consumed at the trailer slot.
pub(crate) struct PacketSinkWorker {
    sink: PacketSink,
    infos: Vec<PacketStreamInfo>,
    streams: Vec<StreamRuntime>,
    /// Stream time bases in index order (the anchor transition rescales into
    /// every one of them at once).
    stream_time_bases: Vec<AVRational>,
    /// Shared origin state machine.
    timeline: Timeline,
    /// First delivery-path error; cloned into the job result and handed to
    /// `on_delivery_error` at the terminal slot.
    pending_error: Option<PacketSinkError>,
    /// Reused Annex-B -> AVCC conversion buffer (high-water sized).
    scratch: Vec<u8>,
    phase: Phase,
    /// A blocking channel send observed the job stopping and bailed out
    /// cooperatively: delivery stopped, but it is NOT an error.
    cancelled: bool,
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
        scheduler_status: &Arc<AtomicUsize>,
        scheduler_result: &Arc<std::sync::Mutex<Option<crate::error::Result<()>>>>,
    ) -> Result<Self, Box<(PacketSink, PacketSinkError)>> {
        // Tier dispatch: only Strict exists; new tiers add arms here.
        let super::PacketSinkTier::Strict = sink.tier;

        // Wire the owned-channel job observer: a blocked bounded send can
        // now bail out when the job stops AND classify whether it stopped
        // by explicit cancellation or by a recorded failure (nothing is
        // wired for plain callback sinks).
        if let Some(slot) = &sink.cancellation {
            let _ = slot.set(super::JobStopObservables {
                status: scheduler_status.clone(),
                result: scheduler_result.clone(),
            });
        }

        let mut infos = Vec::with_capacity(stream_count);
        let mut streams = Vec::with_capacity(stream_count);
        for stream_index in 0..stream_count {
            let st = *(*out_fmt_ctx).streams.add(stream_index);
            let codecpar = (*st).codecpar;
            let media_type = (*codecpar).codec_type;
            let time_base = (*st).time_base;
            // Required S7 precondition: every advertised time base must be a
            // valid positive rational BEFORE any callback observes it (it
            // anchors rescaling and labels every delivered timestamp).
            if time_base.num <= 0 || time_base.den <= 0 {
                return Err(Box::new((
                    sink,
                    PacketSinkError::InvalidTimeBase {
                        stream_index,
                        num: time_base.num,
                        den: time_base.den,
                    },
                )));
            }
            let extradata = match extradata_bytes(codecpar) {
                Some(bytes) => bytes,
                None => {
                    return Err(Box::new((
                        sink,
                        PacketSinkError::MissingExtradata { stream_index },
                    )))
                }
            };

            let (codec, info, derived_duration) = match media_type {
                AVMEDIA_TYPE_VIDEO => {
                    // The encoder whitelist was enforced at build time; this
                    // guards the codec id itself (h264 only in v1).
                    if (*codecpar).codec_id != ffmpeg_sys_next::AVCodecID::AV_CODEC_ID_H264 {
                        return Err(Box::new((
                            sink,
                            PacketSinkError::UnsupportedStream {
                                kind: "non-H.264 video",
                            },
                        )));
                    }
                    let (runtime, delivered, projection) =
                        match AvcRuntime::from_extradata(&extradata, stream_index) {
                            Ok(parts) => parts,
                            Err(e) => return Err(Box::new((sink, e))),
                        };
                    let fr = (*st).avg_frame_rate;
                    let frame_rate = (fr.num > 0 && fr.den > 0).then_some(fr);
                    let sar = (*codecpar).sample_aspect_ratio;
                    // Single source: the codec string AND the typed
                    // profile/compatibility/level fields all come from the
                    // same derived projection the S8 baseline compares.
                    let info = PacketStreamInfo::Video(VideoPacketConfig {
                        stream_index,
                        codec_id: (*codecpar).codec_id,
                        codec_string: projection.codec_string(),
                        profile: projection.profile,
                        compatibility: projection.compatibility,
                        level: projection.level,
                        codec_config: delivered,
                        time_base,
                        width: (*codecpar).width,
                        height: (*codecpar).height,
                        sample_aspect_ratio: (sar.num > 0 && sar.den > 0).then_some(sar),
                        frame_rate,
                    });
                    // One CFR frame interval in stream time-base ticks; 0
                    // when no frame rate is advertised (a durationless
                    // packet then fails typed).
                    let derived_duration = match frame_rate {
                        Some(fr) => av_rescale_q(
                            1,
                            AVRational {
                                num: fr.den,
                                den: fr.num,
                            },
                            time_base,
                        ),
                        None => 0,
                    };
                    (CodecRuntime::Avc(runtime), info, derived_duration)
                }
                AVMEDIA_TYPE_AUDIO => {
                    if (*codecpar).codec_id != ffmpeg_sys_next::AVCodecID::AV_CODEC_ID_AAC {
                        return Err(Box::new((
                            sink,
                            PacketSinkError::UnsupportedStream {
                                kind: "non-AAC audio",
                            },
                        )));
                    }
                    let runtime = match AacRuntime::from_extradata(&extradata, stream_index) {
                        Ok(runtime) => runtime,
                        Err(e) => return Err(Box::new((sink, e))),
                    };
                    let channels = (*codecpar).ch_layout.nb_channels;
                    // With channelConfiguration 0 the ASC's embedded
                    // program_config_element is the configuration's only
                    // channel declaration; the advertised count must agree
                    // with it, or the delivered metadata (`channels`) and
                    // the delivered codec_config (the ASC consumers hand to
                    // decoders) would contradict each other. Table-signaled
                    // layouts are exempt: SBR/PS decoding legitimately
                    // doubles a mono channelConfiguration (HE-AAC v2), so
                    // table count and advertised count may differ.
                    if let Some(pce_channels) = runtime.pce_channel_count() {
                        if i64::from(pce_channels) != i64::from(channels) {
                            return Err(Box::new((
                                sink,
                                PacketSinkError::InvalidExtradata {
                                    stream_index,
                                    reason: format!(
                                        "AudioSpecificConfig program_config_element declares \
                                         {pce_channels} channels but the stream advertises \
                                         {channels}"
                                    ),
                                },
                            )));
                        }
                    }
                    let info = PacketStreamInfo::Audio(AudioPacketConfig {
                        stream_index,
                        codec_id: (*codecpar).codec_id,
                        codec_string: runtime.codec_string(),
                        codec_config: extradata.clone(),
                        time_base,
                        sample_rate: (*codecpar).sample_rate,
                        channels,
                        channel_layout: describe_channel_layout(codecpar),
                    });
                    // The AAC frame duration is stream-invariant:
                    // av_get_audio_frame_duration2 reads only codecpar (the
                    // fixed codec frame size) for AAC and treats the byte
                    // count purely as a nonzero gate, so a unit stand-in
                    // derives the same value as any real payload (empty
                    // payloads are rejected before duration handling).
                    let samples = av_get_audio_frame_duration2(codecpar, 1);
                    let sample_rate = (*codecpar).sample_rate;
                    let derived_duration = if samples > 0 && sample_rate > 0 {
                        av_rescale_q(
                            samples as i64,
                            AVRational {
                                num: 1,
                                den: sample_rate,
                            },
                            time_base,
                        )
                    } else {
                        0
                    };
                    (CodecRuntime::Aac(runtime), info, derived_duration)
                }
                _ => {
                    return Err(Box::new((
                        sink,
                        PacketSinkError::UnsupportedStream {
                            kind: "non-audio/video",
                        },
                    )))
                }
            };

            infos.push(info);
            streams.push(StreamRuntime {
                codec,
                time_base,
                derived_duration,
                projection: ParamProjection {
                    width: (*codecpar).width,
                    height: (*codecpar).height,
                    sample_rate: (*codecpar).sample_rate,
                },
                timeline: StreamTimeline::new(),
            });
        }

        let stream_time_bases = streams.iter().map(|s| s.time_base).collect();
        Ok(Self {
            sink,
            infos,
            streams,
            stream_time_bases,
            timeline: Timeline::new(stream_count),
            pending_error: None,
            scratch: Vec::new(),
            phase: Phase::Collected,
            cancelled: false,
        })
    }

    /// Invokes `on_stream_info` exactly once, on the worker thread, before
    /// any packet: the `Collected -> Running` phase transition. A repeated
    /// call is an idempotent no-op (returns `0` without re-invoking the
    /// callback), as is a call after the terminal phase.
    pub(crate) fn deliver_stream_info(&mut self) -> i32 {
        if !matches!(self.phase, Phase::Collected) {
            return 0;
        }
        self.phase = Phase::Running;
        if let Err(cb) = self.sink.dispatch_stream_info(&self.infos) {
            match callback_failure(cb, None) {
                ProcessFailure::Cancelled => self.cancelled = true,
                ProcessFailure::JobStopped => {}
                ProcessFailure::Sink(e) => {
                    if self.pending_error.is_none() {
                        self.pending_error = Some(e);
                    }
                }
            }
            return AVERROR_EXTERNAL;
        }
        0
    }

    /// Whether delivery stopped via cooperative cancellation with no error
    /// recorded (the worker then exits like a plain stop, publishing
    /// nothing).
    pub(crate) fn cancelled_cleanly(&self) -> bool {
        self.cancelled && self.pending_error.is_none()
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
    /// - `packet_box.packet` must wrap a live packet, and
    ///   `packet_box.packet_data.output_stream_index` must be a valid stream
    ///   index of the output this worker was collected from (same contract
    ///   as `write_packet`).
    pub(crate) unsafe fn process_and_deliver(&mut self, packet_box: &mut PacketBox) -> i32 {
        match self.process(packet_box) {
            Ok(()) => 0,
            Err(ProcessFailure::Cancelled) => {
                self.cancelled = true;
                AVERROR_EXTERNAL
            }
            // Truncated by a failure elsewhere: neither cancelled nor a
            // sink error — the terminal reads the recorded job error.
            Err(ProcessFailure::JobStopped) => AVERROR_EXTERNAL,
            Err(ProcessFailure::Sink(e)) => {
                if self.pending_error.is_none() {
                    self.pending_error = Some(e);
                }
                AVERROR_EXTERNAL
            }
        }
    }

    /// # Safety
    /// - Same contract as [`Self::process_and_deliver`] (its only caller).
    unsafe fn process(&mut self, packet_box: &mut PacketBox) -> Result<(), ProcessFailure> {
        let stream_index = packet_box.packet_data.output_stream_index as usize;
        debug_assert!(stream_index < self.streams.len());
        // Real phase guard (not debug-only): a packet may only be processed
        // while Running — after the one-time stream info, before the
        // terminal slot. Anything else is an internal sequencing violation
        // and fails typed instead of silently delivering out of contract.
        if !matches!(self.phase, Phase::Running) {
            return Err(ProcessFailure::from(PacketSinkError::PhaseViolation {
                stream_index,
            }));
        }
        let pkt = packet_box.packet.as_ptr();

        // 1. S8 FIRST (consensus order): mid-stream configuration change
        // detection over checked side-data iteration — a packet carrying
        // both configuration-change evidence and a timestamp/time-base
        // fault must report the configuration change.
        for entry in side_data::entries(pkt) {
            let (sd_type, bytes) = entry.map_err(|reason| {
                ProcessFailure::from(PacketSinkError::MalformedPacket {
                    stream_index,
                    reason,
                })
            })?;
            if sd_type == AV_PKT_DATA_NEW_EXTRADATA {
                self.streams[stream_index]
                    .codec
                    .check_new_extradata(bytes, stream_index)?;
            } else if sd_type == AV_PKT_DATA_PARAM_CHANGE {
                check_param_change(
                    &self.streams[stream_index].projection,
                    stream_index,
                    bytes,
                )?;
            }
        }

        // 2. S7: time bases and timestamps. The packet must be labeled in
        // the stream's advertised time base — the anchor is computed from
        // packet values while delivery is labeled with the stream time base,
        // so a mismatch would silently corrupt the shared-origin math.
        let stream_tb = self.streams[stream_index].time_base;
        {
            let pkt_tb = (*pkt).time_base;
            if pkt_tb.num != stream_tb.num || pkt_tb.den != stream_tb.den {
                return Err(ProcessFailure::from(
                    PacketSinkError::PacketTimeBaseMismatch {
                        stream_index,
                        packet_num: pkt_tb.num,
                        packet_den: pkt_tb.den,
                        stream_num: stream_tb.num,
                        stream_den: stream_tb.den,
                    },
                ));
            }
        }
        // Missing values are rejected before anything else (the origin
        // cannot anchor on AV_NOPTS_VALUE).
        let orig_pts = (*pkt).pts;
        let orig_dts = (*pkt).dts;
        if orig_dts == AV_NOPTS_VALUE {
            return Err(ProcessFailure::from(PacketSinkError::MissingTimestamp {
                stream_index,
                which: "dts",
            }));
        }
        if orig_pts == AV_NOPTS_VALUE {
            return Err(ProcessFailure::from(PacketSinkError::MissingTimestamp {
                stream_index,
                which: "pts",
            }));
        }

        // Anchor the shared origin on the first delivered packet (delivery
        // order == arrival order); the transition derives every stream's
        // offset at once, all-or-nothing.
        self.timeline
            .ensure_anchored(orig_dts, stream_tb, &self.stream_time_bases)?;
        let applied_offset = self.timeline.offset(stream_index);
        let pts = orig_pts
            .checked_sub(applied_offset)
            .ok_or(PacketSinkError::TimestampOverflow { stream_index })
            .map_err(ProcessFailure::from)?;
        let dts = orig_dts
            .checked_sub(applied_offset)
            .ok_or(PacketSinkError::TimestampOverflow { stream_index })
            .map_err(ProcessFailure::from)?;
        self.streams[stream_index]
            .timeline
            .observe(stream_index, pts, dts)?;

        // 4.-5. Payload normalization + is_key.
        let size = (*pkt).size;
        let data_ptr = (*pkt).data;
        if data_ptr.is_null() || size <= 0 {
            return Err(ProcessFailure::from(PacketSinkError::MalformedPacket {
                stream_index,
                reason: "empty payload (a packet must carry one complete frame)".to_string(),
            }));
        }
        let payload = std::slice::from_raw_parts(data_ptr, size as usize);
        let stream = &self.streams[stream_index];
        let (is_key, data): (bool, &[u8]) = match &stream.codec {
            CodecRuntime::Avc(avc) => avc.normalize_au(payload, &mut self.scratch, stream_index)?,
            // Every AAC frame is a random access point; the raw frame passes
            // through unchanged.
            CodecRuntime::Aac(_) => (true, payload),
        };

        // 6. Duration: pass the encoder's through; substitute this stream's
        // collection-derived interval when absent; a value that stays
        // underivable is an error, never a silent zero.
        let mut duration = (*pkt).duration;
        if duration < 0 {
            return Err(ProcessFailure::from(PacketSinkError::MissingDuration {
                stream_index,
            }));
        }
        if duration == 0 {
            duration = stream.derived_duration;
        }
        if duration <= 0 {
            return Err(ProcessFailure::from(PacketSinkError::MissingDuration {
                stream_index,
            }));
        }

        // 7. Deliver. The borrowed view (including `data`) dies with the call.
        let view = PacketView {
            stream_index,
            pts,
            dts,
            duration,
            time_base: stream_tb,
            is_key,
            applied_offset,
            data,
        };
        if let Err(cb) = self.sink.dispatch_packet(&view) {
            return Err(callback_failure(cb, Some(stream_index)));
        }
        Ok(())
    }

    /// Terminal slot (where the muxer would write its trailer). One-way phase
    /// transition — a second call is a no-op, so no terminal event can ever
    /// fire twice. Fires `on_end` only through the strong gate; fires
    /// `on_delivery_error` for a stashed delivery-path error, or as
    /// [`PacketSinkError::JobFailed`] when the job failed elsewhere (whether
    /// or not that failure truncated this sink's delivery — `wait()` keeps
    /// the original error); stays silent for aborts and clean cancellation.
    pub(crate) fn finish(
        &mut self,
        all_streams_terminal: bool,
        ret: i32,
        aborted: bool,
        job_error: Option<String>,
    ) {
        if matches!(self.phase, Phase::Finished) {
            return;
        }
        self.phase = Phase::Finished;
        if aborted {
            return;
        }
        if let Some(e) = self.pending_error.take() {
            self.sink.dispatch_delivery_error(&e);
            return;
        }
        if self.cancelled {
            // A delivery interrupted by cancellation stays silent (the
            // documented stop()/abort() contract) even when some other
            // worker recorded an error for the same shutdown.
            return;
        }
        if let Some(message) = job_error {
            // The JOB failed (a sibling output, an upstream task) — whether
            // this sink had drained fully or the failure truncated its
            // delivery: success must not be promised, and the consumer
            // learns why. wait() keeps the original error.
            self.sink
                .dispatch_delivery_error(&PacketSinkError::JobFailed { message });
            return;
        }
        if ret >= 0 && all_streams_terminal {
            self.sink.dispatch_end();
        }
    }
}

/// FFmpeg's textual channel-layout description (e.g. "stereo"), empty when
/// the layout cannot be described.
///
/// # Safety
/// - `codecpar` must be a valid, non-null `AVCodecParameters` with an
///   initialized `ch_layout`, alive for the call.
unsafe fn describe_channel_layout(codecpar: *const AVCodecParameters) -> String {
    let mut buf = [0u8; 128];
    let n = av_channel_layout_describe(
        &(*codecpar).ch_layout,
        buf.as_mut_ptr() as *mut libc::c_char,
        buf.len(),
    );
    if n > 0 {
        match std::ffi::CStr::from_bytes_until_nul(&buf) {
            Ok(c) => c.to_string_lossy().into_owned(),
            Err(_) => String::new(),
        }
    } else {
        String::new()
    }
}

/// Owned copy of a stream's extradata, `None` when absent/empty.
///
/// # Safety
/// - `codecpar` must be a valid, non-null `AVCodecParameters` whose
///   `extradata`/`extradata_size` pair is consistent (as populated by
///   `avcodec_parameters_from_context`), alive for the call.
unsafe fn extradata_bytes(codecpar: *const AVCodecParameters) -> Option<Vec<u8>> {
    let ptr = (*codecpar).extradata;
    let size = (*codecpar).extradata_size;
    if ptr.is_null() || size <= 0 {
        return None;
    }
    Some(std::slice::from_raw_parts(ptr, size as usize).to_vec())
}

/// Layout check + typed-field projection compare for `AV_PKT_DATA_PARAM_CHANGE`
/// (little-endian `u32 flags`, then the fields the flags select — the side
/// data carries no parameter sets, so only its typed fields are compared).
/// The payload must be EXACTLY as long as its flags require, and the fields
/// must pass FFmpeg-style range checks.
fn check_param_change(
    projection: &ParamProjection,
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
                .ok_or_else(|| PacketSinkError::MalformedPacket {
                    stream_index,
                    reason: "PARAM_CHANGE side data: truncated".to_string(),
                })?;
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
    // Exact-length validation per flag combination: the payload is 4 bytes of
    // flags plus exactly the fields the flags select. Trailing garbage (or a
    // flags=0 announcement padded with junk) is a malformed announcement, not
    // a redundant one.
    let expected_len = 4
        + if flags & SAMPLE_RATE_FLAG != 0 { 4 } else { 0 }
        + if flags & DIMENSIONS_FLAG != 0 { 8 } else { 0 };
    if data.len() != expected_len {
        return Err(malformed(&format!(
            "payload is {} bytes, flags {flags:#x} require exactly {expected_len}",
            data.len()
        )));
    }
    if flags & SAMPLE_RATE_FLAG != 0 {
        let sample_rate = read_i32(data)?;
        // FFmpeg's own param-change handler rejects non-positive rates.
        if sample_rate <= 0 {
            return Err(malformed(&format!("sample rate {sample_rate} out of range")));
        }
        if sample_rate != projection.sample_rate {
            return Err(PacketSinkError::ConfigChange {
                stream_index,
                what: format!(
                    "sample rate changed {} -> {sample_rate}",
                    projection.sample_rate
                ),
            });
        }
    }
    if flags & DIMENSIONS_FLAG != 0 {
        let width = read_i32(data)?;
        let height = read_i32(data)?;
        // av_image_check_size bound: positive and (w+128)*(h+128) within
        // INT_MAX/8 addressable pixels.
        let plausible = width > 0
            && height > 0
            && (width as i64 + 128) * (height as i64 + 128) < (i32::MAX as i64) / 8;
        if !plausible {
            return Err(malformed(&format!(
                "dimensions {width}x{height} out of range"
            )));
        }
        if width != projection.width || height != projection.height {
            return Err(PacketSinkError::ConfigChange {
                stream_index,
                what: format!(
                    "dimensions changed {}x{} -> {width}x{height}",
                    projection.width, projection.height
                ),
            });
        }
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::core::context::PacketData;
    use crate::core::packet_sink::{EncodedPacket, PacketSink, PacketSinkEvent};
    use ffmpeg_next::packet::Mut;
    use ffmpeg_sys_next::{
        av_guess_format, av_mallocz, av_packet_new_side_data, avformat_alloc_output_context2,
        avformat_free_context, avformat_new_stream, AVCodecID, AVMediaType,
        AV_INPUT_BUFFER_PADDING_SIZE,
    };
    use std::ffi::CString;
    use std::ptr::{null, null_mut};
    use std::sync::{Arc, Mutex};

    // Encoder-produced parameter sets (x264 via the ffmpeg CLI): Constrained
    // Baseline (profile_idc 66, compatibility 0xC0), level_idc 30, coding the
    // same 320x240 yuv420p shape the test streams declare in codecpar.
    const SPS: &[u8] = &[
        0x67, 0x42, 0xC0, 0x1E, 0xD9, 0x01, 0x41, 0xFB, 0x01, 0x10, 0x00, 0x00, 0x03, 0x00, 0x10,
        0x00, 0x00, 0x03, 0x03, 0x20, 0xF1, 0x62, 0xE4, 0x80,
    ];
    const PPS: &[u8] = &[0x68, 0xCB, 0x83, 0xCB, 0x20];
    // Same encoder and coded shape at level 4.0: level_idc (SPS byte 3) is
    // 0x28, so the derived codec projection differs from `SPS`.
    const OTHER_SPS: &[u8] = &[
        0x67, 0x42, 0xC0, 0x28, 0xD9, 0x01, 0x41, 0xFB, 0x01, 0x10, 0x00, 0x00, 0x03, 0x00, 0x10,
        0x00, 0x00, 0x03, 0x03, 0x20, 0xF1, 0x83, 0x24, 0x80,
    ];

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
        let sink = PacketSink::builder(move |view: &PacketView<'_>| {
            pkt_log
                .lock()
                .unwrap()
                .push(PacketSinkEvent::Packet(EncodedPacket {
                    stream_index: view.stream_index(),
                    pts: view.pts(),
                    dts: view.dts(),
                    duration: view.duration(),
                    time_base: view.time_base(),
                    is_key: view.is_key(),
                    applied_offset: view.applied_offset(),
                    data: view.data().to_vec(),
                }));
            Ok(())
        })
        .on_stream_info(move |infos| {
            info_log
                .lock()
                .unwrap()
                .push(PacketSinkEvent::StreamInfo(infos.to_vec()));
            Ok(())
        })
        .on_end(move || end_log.lock().unwrap().push(PacketSinkEvent::End))
        .on_delivery_error(move |e| {
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
        let status = Arc::new(AtomicUsize::new(0));
        let result = Arc::new(std::sync::Mutex::new(None));
        unsafe { PacketSinkWorker::collect(ctx.ctx, ctx.stream_count(), sink, &status, &result) }
            .map_err(|boxed| (*boxed).1)
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
        // avcC bytes 1..4 are SPS bytes 1..4 verbatim.
        assert_eq!(&avcc[1..4], &SPS[1..4]);
        // Bytes 4 and 5 carry all-ones reserved bits around the length size
        // and the SPS count (ff_isom_write_avcc emits 0xFF and 0xE0 | count);
        // the assertions pin the full bytes, not a masked view.
        assert_eq!(avcc[4], 0xFF, "reserved ones + 4-byte NAL length prefixes");
        assert_eq!(avcc[5] & 0xE0, 0xE0, "byte 5 reserved bits must be ones");
        assert!(avcc[5] & 0x1F >= 1, "at least one SPS");
        let video = info.video().expect("typed video configuration");
        assert_eq!(video.codec_id(), AVCodecID::AV_CODEC_ID_H264);
        // The typed header fields mirror the fixture SPS: profile_idc 66
        // with constraint flags 0xC0 (Constrained Baseline), level_idc 30
        // (level 3.0) — and the RFC 6381 string is their hex projection.
        assert_eq!(video.profile(), 66);
        assert_eq!(video.compatibility(), 0xC0);
        assert_eq!(video.level(), 30);
        assert_eq!(video.codec_string(), "avc1.42C01E");
        assert_eq!(video.frame_rate(), Some(AVRational { num: 25, den: 1 }));
        assert_eq!((video.width(), video.height()), (320, 240));
    }

    #[test]
    fn happy_path_delivers_avcc_au_and_derives_duration() {
        let ctx = video_ctx();
        let (sink, events) = recording_sink();
        let mut worker = collect(&ctx, sink).unwrap();
        assert_eq!(worker.deliver_stream_info(), 0);
        // Documented idempotence: a repeated call is a no-op that does not
        // re-invoke the callback (the single Info event below proves it).
        assert_eq!(worker.deliver_stream_info(), 0);
        let mut pb = packet(&idr_au(), 4, 4, tb25(), 0);
        assert_eq!(
            unsafe { worker.process_and_deliver(&mut pb) },
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
        assert_eq!(worker.deliver_stream_info(), 0);
        let mut pb = packet(&p_au(), 0, 0, tb25(), 0);
        assert_eq!(unsafe { worker.process_and_deliver(&mut pb) }, 0);
        let events = events.lock().unwrap();
        let PacketSinkEvent::Packet(p) = &events[1] else {
            panic!("expected a packet event after the stream info");
        };
        assert!(!p.is_key());
    }

    #[test]
    fn s7_rejects_missing_and_disordered_timestamps() {
        let ctx = video_ctx();
        let (sink, _events) = recording_sink();
        let mut worker = collect(&ctx, sink).unwrap();
        assert_eq!(worker.deliver_stream_info(), 0);

        let mut nopts = packet(&idr_au(), ffmpeg_sys_next::AV_NOPTS_VALUE, 0, tb25(), 0);
        assert_eq!(
            unsafe { worker.process_and_deliver(&mut nopts) },
            AVERROR_EXTERNAL
        );
        assert!(matches!(
            worker.pending_error_cloned(),
            Some(PacketSinkError::MissingTimestamp { which: "pts", .. })
        ));

        // Fresh worker: pts earlier than dts.
        let (sink, _events) = recording_sink();
        let mut worker = collect(&ctx, sink).unwrap();
        assert_eq!(worker.deliver_stream_info(), 0);
        let mut bad = packet(&idr_au(), 1, 2, tb25(), 0);
        assert_eq!(
            unsafe { worker.process_and_deliver(&mut bad) },
            AVERROR_EXTERNAL
        );
        assert!(matches!(
            worker.pending_error_cloned(),
            Some(PacketSinkError::PtsBeforeDts { .. })
        ));

        // Fresh worker: dts must strictly increase.
        let (sink, _events) = recording_sink();
        let mut worker = collect(&ctx, sink).unwrap();
        assert_eq!(worker.deliver_stream_info(), 0);
        let mut first = packet(&idr_au(), 0, 0, tb25(), 0);
        assert_eq!(unsafe { worker.process_and_deliver(&mut first) }, 0);
        let mut stale = packet(&p_au(), 5, 0, tb25(), 0);
        assert_eq!(
            unsafe { worker.process_and_deliver(&mut stale) },
            AVERROR_EXTERNAL
        );
        assert!(matches!(
            worker.pending_error_cloned(),
            Some(PacketSinkError::NonMonotonicDts { .. })
        ));

        // Fresh worker: duplicate pts within the reorder window.
        let (sink, _events) = recording_sink();
        let mut worker = collect(&ctx, sink).unwrap();
        assert_eq!(worker.deliver_stream_info(), 0);
        let mut a = packet(&idr_au(), 3, 0, tb25(), 0);
        assert_eq!(unsafe { worker.process_and_deliver(&mut a) }, 0);
        let mut b = packet(&p_au(), 3, 1, tb25(), 0);
        assert_eq!(
            unsafe { worker.process_and_deliver(&mut b) },
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
        assert_eq!(worker.deliver_stream_info(), 0);
        let mut pb = packet(&idr_au(), 0, 0, tb25(), 0);
        assert_eq!(
            unsafe { worker.process_and_deliver(&mut pb) },
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
        assert_eq!(worker.deliver_stream_info(), 0);

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
        assert_eq!(unsafe { worker.process_and_deliver(&mut same) }, 0);

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
            unsafe { worker.process_and_deliver(&mut changed) },
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
        assert_eq!(worker.deliver_stream_info(), 0);
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
        assert_eq!(unsafe { worker.process_and_deliver(&mut same) }, 0);

        // A resolution change is rejected typed.
        let (sink, _events) = recording_sink();
        let mut worker = collect(&ctx, sink).unwrap();
        assert_eq!(worker.deliver_stream_info(), 0);
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
            unsafe { worker.process_and_deliver(&mut changed) },
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
        assert_eq!(worker.deliver_stream_info(), 0);
        let mut au = annexb_config();
        au.extend_from_slice(&[0, 0, 1, 0x65, 0x88, 0x84]);
        let mut pb = packet(&au, 0, 0, tb25(), 0);
        assert_eq!(
            unsafe { worker.process_and_deliver(&mut pb) },
            AVERROR_EXTERNAL
        );
        assert!(matches!(
            worker.pending_error_cloned(),
            Some(PacketSinkError::InBandParameterSets { .. })
        ));

        // Different in-band sets are a configuration change.
        let (sink, _events) = recording_sink();
        let mut worker = collect(&ctx, sink).unwrap();
        assert_eq!(worker.deliver_stream_info(), 0);
        let mut au = vec![0, 0, 0, 1];
        au.extend_from_slice(OTHER_SPS);
        au.extend_from_slice(&[0, 0, 1, 0x65, 0x88, 0x84]);
        let mut pb = packet(&au, 0, 0, tb25(), 0);
        assert_eq!(
            unsafe { worker.process_and_deliver(&mut pb) },
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
        assert_eq!(worker.deliver_stream_info(), 0);

        // First delivered packet: video dts 5 (at 1/25) anchors the origin.
        let mut v = packet(&idr_au(), 5, 5, tb25(), 0);
        assert_eq!(unsafe { worker.process_and_deliver(&mut v) }, 0);
        // Audio at original dts 0 keeps its true relative offset: negative.
        let mut a = packet(&[0x21, 0x10, 0x04], 0, 0, AVRational { num: 1, den: 44100 }, 1);
        a.packet_data.codec_type = AVMediaType::AVMEDIA_TYPE_AUDIO;
        assert_eq!(unsafe { worker.process_and_deliver(&mut a) }, 0);

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

    /// AudioSpecificConfig produced by FFmpeg's native AAC encoder with
    /// forced PCE signaling (channelConfiguration 0, quad layout: one
    /// front CPE + one back CPE = 4 channels):
    ///   ffmpeg -f lavfi -i anullsrc=channel_layout=quad:sample_rate=48000 \
    ///          -c:a aac -aac_pce 1 -bitexact -frames:a 3 out.mp4
    const PCE_QUAD_ASC: [u8; 16] = [
        0x11, 0x80, 0x04, 0xC4, 0x04, 0x00, 0x21, 0x10, 0x04, 0x4C, 0x61, 0x76, 0x63, 0x56, 0xE5,
        0x00,
    ];

    #[test]
    fn pce_channel_count_must_match_the_advertised_metadata() {
        // The default test stream advertises 2 channels; the PCE declares
        // 4 — contradictory metadata must fail before any callback.
        let ctx = TestCtx::new();
        unsafe {
            ctx.add_stream(
                AVMediaType::AVMEDIA_TYPE_AUDIO,
                AVCodecID::AV_CODEC_ID_AAC,
                Some(&PCE_QUAD_ASC),
                AVRational { num: 1, den: 48000 },
            );
        }
        let (sink, events) = recording_sink();
        match collect(&ctx, sink) {
            Err(PacketSinkError::InvalidExtradata {
                stream_index: 0,
                reason,
            }) => {
                assert!(
                    reason.contains("program_config_element")
                        && reason.contains('4')
                        && reason.contains('2'),
                    "got {reason:?}"
                );
            }
            Err(other) => panic!("expected InvalidExtradata, got {other:?}"),
            Ok(_) => panic!("collect must reject a PCE/metadata channel mismatch"),
        }
        assert!(
            events.lock().unwrap().is_empty(),
            "a configuration failure must precede every callback"
        );
    }

    #[test]
    fn pce_channel_count_agreeing_with_the_metadata_collects() {
        let ctx = TestCtx::new();
        unsafe {
            let idx = ctx.add_stream(
                AVMediaType::AVMEDIA_TYPE_AUDIO,
                AVCodecID::AV_CODEC_ID_AAC,
                Some(&PCE_QUAD_ASC),
                AVRational { num: 1, den: 48000 },
            );
            let st = *(*ctx.ctx).streams.add(idx);
            (*(*st).codecpar).ch_layout.nb_channels = 4;
        }
        let (sink, _events) = recording_sink();
        let worker = collect(&ctx, sink).unwrap();
        let audio = worker.infos[0].audio().expect("typed audio configuration");
        assert_eq!(audio.channels(), 4);
        assert_eq!(audio.codec_string(), "mp4a.40.2");
    }

    #[test]
    fn failing_packet_callback_stops_with_typed_error_and_no_on_end() {
        let ctx = video_ctx();
        let events: Arc<Mutex<Vec<PacketSinkEvent>>> = Arc::new(Mutex::new(Vec::new()));
        let (end_log, err_log) = (events.clone(), events.clone());
        let sink = PacketSink::builder(|_: &PacketView<'_>| {
            Err(PacketCallbackError::new("test consumer failure"))
        })
        .on_end(move || end_log.lock().unwrap().push(PacketSinkEvent::End))
        .on_delivery_error(move |e| {
            err_log
                .lock()
                .unwrap()
                .push(PacketSinkEvent::Error(e.clone()))
        })
        .build();
        let mut worker = collect(&ctx, sink).unwrap();
        assert_eq!(worker.deliver_stream_info(), 0);
        let mut pb = packet(&idr_au(), 0, 0, tb25(), 0);
        let ret = unsafe { worker.process_and_deliver(&mut pb) };
        assert_eq!(ret, AVERROR_EXTERNAL);
        assert_ne!(ret, ffmpeg_sys_next::AVERROR_EOF);
        match worker.pending_error_cloned() {
            Some(PacketSinkError::PacketCallbackFailed {
                stream_index: 0,
                error,
            }) => assert_eq!(error.to_string(), "test consumer failure"),
            other => panic!("expected the typed callback failure, got {other:?}"),
        }

        // Terminal slot: the stashed error fires on_delivery_error, never
        // on_end — even if every stream were terminal.
        worker.finish(true, AVERROR_EXTERNAL, false, None);
        let events = events.lock().unwrap();
        assert_eq!(events.len(), 1);
        assert!(matches!(events[0], PacketSinkEvent::Error(_)));
    }

    /// The three independent review counterexamples for the duplicate-PTS
    /// boundary: a recorded pts equal to the NEW packet's dts must still be
    /// caught (membership is checked before pruning).
    #[test]
    fn duplicate_pts_boundary_counterexamples() {
        for (first, second) in [((1, 0), (1, 1)), ((2, 0), (2, 2)), ((3, 0), (3, 3))] {
            let ctx = video_ctx();
            let (sink, _events) = recording_sink();
            let mut worker = collect(&ctx, sink).unwrap();
        assert_eq!(worker.deliver_stream_info(), 0);
            let mut a = packet(&idr_au(), first.0, first.1, tb25(), 0);
            assert_eq!(unsafe { worker.process_and_deliver(&mut a) }, 0);
            let mut b = packet(&p_au(), second.0, second.1, tb25(), 0);
            assert_eq!(
                unsafe { worker.process_and_deliver(&mut b) },
                AVERROR_EXTERNAL,
                "({},{}) then ({},{}) must be rejected",
                first.0,
                first.1,
                second.0,
                second.1
            );
            assert!(matches!(
                worker.pending_error_cloned(),
                Some(PacketSinkError::DuplicatePts { .. })
            ));
        }
    }

    #[test]
    fn invalid_stream_time_base_fails_before_any_callback() {
        let ctx = TestCtx::new();
        unsafe {
            ctx.add_stream(
                AVMediaType::AVMEDIA_TYPE_VIDEO,
                AVCodecID::AV_CODEC_ID_H264,
                Some(&annexb_config()),
                AVRational { num: 0, den: 25 },
            );
        }
        let (sink, events) = recording_sink();
        let err = match collect(&ctx, sink) {
            Err(e) => e,
            Ok(_) => panic!("collect must reject a zero-numerator time base"),
        };
        assert!(matches!(
            err,
            PacketSinkError::InvalidTimeBase {
                stream_index: 0,
                num: 0,
                den: 25
            }
        ));
        assert!(events.lock().unwrap().is_empty());
    }

    #[test]
    fn packet_time_base_mismatch_is_rejected() {
        let ctx = video_ctx();
        let (sink, _events) = recording_sink();
        let mut worker = collect(&ctx, sink).unwrap();
        assert_eq!(worker.deliver_stream_info(), 0);
        let mut pb = packet(&idr_au(), 0, 0, AVRational { num: 1, den: 30 }, 0);
        assert_eq!(
            unsafe { worker.process_and_deliver(&mut pb) },
            AVERROR_EXTERNAL
        );
        assert!(matches!(
            worker.pending_error_cloned(),
            Some(PacketSinkError::PacketTimeBaseMismatch {
                packet_den: 30,
                stream_den: 25,
                ..
            })
        ));
    }

    /// PARAM_CHANGE payloads must be EXACTLY as long as their flags require:
    /// flags=0 plus junk, and a valid projection with trailing bytes, are
    /// malformed announcements — not redundant ones.
    #[test]
    fn param_change_exact_length_and_ranges() {
        let ctx = video_ctx();
        let cases: Vec<(Vec<u8>, &str)> = vec![
            // flags = 0 followed by junk.
            {
                let mut v = 0u32.to_le_bytes().to_vec();
                v.push(0xAB);
                (v, "flags=0 with trailing junk")
            },
            // Valid equal dimensions followed by one trailing byte.
            {
                let mut v = 8u32.to_le_bytes().to_vec();
                v.extend_from_slice(&320i32.to_le_bytes());
                v.extend_from_slice(&240i32.to_le_bytes());
                v.push(0);
                (v, "valid projection with trailing byte")
            },
            // Out-of-range dimensions (zero width).
            {
                let mut v = 8u32.to_le_bytes().to_vec();
                v.extend_from_slice(&0i32.to_le_bytes());
                v.extend_from_slice(&240i32.to_le_bytes());
                (v, "zero width")
            },
        ];
        for (payload, what) in cases {
            let (sink, _events) = recording_sink();
            let mut worker = collect(&ctx, sink).unwrap();
        assert_eq!(worker.deliver_stream_info(), 0);
            let mut pb = packet(&idr_au(), 0, 0, tb25(), 0);
            unsafe {
                let sd = av_packet_new_side_data(
                    pb.packet.as_mut_ptr(),
                    AV_PKT_DATA_PARAM_CHANGE,
                    payload.len(),
                );
                assert!(!sd.is_null());
                std::ptr::copy_nonoverlapping(payload.as_ptr(), sd, payload.len());
            }
            assert_eq!(
                unsafe { worker.process_and_deliver(&mut pb) },
                AVERROR_EXTERNAL,
                "{what} must be rejected"
            );
            assert!(matches!(
                worker.pending_error_cloned(),
                Some(PacketSinkError::MalformedPacket { .. })
            ));
        }
    }

    /// Composite S8 baseline: reordering the same SPS/PPS identities is not
    /// a configuration change — UNLESS the reorder changes the DERIVED codec
    /// projection consumers were told (profile/compatibility/level from the
    /// first SPS, the same source as on_stream_info). Both directions pinned.
    #[test]
    fn reorder_is_config_change_only_when_the_projection_changes() {
        // Second SPS with an IDENTICAL projection (bytes 1..4) but a
        // different tail (same encoder, Constrained Baseline level 3.0,
        // coding 640x480): reorder must pass.
        const SAME_PROJ_SPS: &[u8] = &[
            0x67, 0x42, 0xC0, 0x1E, 0xD9, 0x00, 0xA0, 0x3D, 0xB0, 0x11, 0x00, 0x00, 0x03, 0x00,
            0x01, 0x00, 0x00, 0x03, 0x00, 0x32, 0x0F, 0x16, 0x2E, 0x48,
        ];
        let mut config = vec![0, 0, 0, 1];
        config.extend_from_slice(SPS);
        config.extend_from_slice(&[0, 0, 1]);
        config.extend_from_slice(SAME_PROJ_SPS);
        config.extend_from_slice(&[0, 0, 1]);
        config.extend_from_slice(PPS);
        let ctx = TestCtx::new();
        unsafe {
            ctx.add_stream(
                AVMediaType::AVMEDIA_TYPE_VIDEO,
                AVCodecID::AV_CODEC_ID_H264,
                Some(&config),
                tb25(),
            );
        }
        let (sink, _events) = recording_sink();
        let mut worker = collect(&ctx, sink).unwrap();
        assert_eq!(worker.deliver_stream_info(), 0);
        let mut swapped = vec![0, 0, 0, 1];
        swapped.extend_from_slice(SAME_PROJ_SPS);
        swapped.extend_from_slice(&[0, 0, 1]);
        swapped.extend_from_slice(SPS);
        swapped.extend_from_slice(&[0, 0, 1]);
        swapped.extend_from_slice(PPS);
        let mut pb = packet(&idr_au(), 0, 0, tb25(), 0);
        unsafe {
            let sd = av_packet_new_side_data(
                pb.packet.as_mut_ptr(),
                AV_PKT_DATA_NEW_EXTRADATA,
                swapped.len(),
            );
            assert!(!sd.is_null());
            std::ptr::copy_nonoverlapping(swapped.as_ptr(), sd, swapped.len());
        }
        assert_eq!(
            unsafe { worker.process_and_deliver(&mut pb) },
            0,
            "reordered identical sets with an unchanged projection must pass"
        );

        // Same identities, but the reorder changes the first SPS's level
        // byte — the derived projection consumers were told changes, so it
        // IS a configuration change even though the canonical sets match.
        let mut config = vec![0, 0, 0, 1];
        config.extend_from_slice(SPS);
        config.extend_from_slice(&[0, 0, 1]);
        config.extend_from_slice(OTHER_SPS);
        config.extend_from_slice(&[0, 0, 1]);
        config.extend_from_slice(PPS);
        let ctx = TestCtx::new();
        unsafe {
            ctx.add_stream(
                AVMediaType::AVMEDIA_TYPE_VIDEO,
                AVCodecID::AV_CODEC_ID_H264,
                Some(&config),
                tb25(),
            );
        }
        let (sink, _events) = recording_sink();
        let mut worker = collect(&ctx, sink).unwrap();
        assert_eq!(worker.deliver_stream_info(), 0);
        let mut swapped = vec![0, 0, 0, 1];
        swapped.extend_from_slice(OTHER_SPS);
        swapped.extend_from_slice(&[0, 0, 1]);
        swapped.extend_from_slice(SPS);
        swapped.extend_from_slice(&[0, 0, 1]);
        swapped.extend_from_slice(PPS);
        let mut pb = packet(&idr_au(), 0, 0, tb25(), 0);
        unsafe {
            let sd = av_packet_new_side_data(
                pb.packet.as_mut_ptr(),
                AV_PKT_DATA_NEW_EXTRADATA,
                swapped.len(),
            );
            assert!(!sd.is_null());
            std::ptr::copy_nonoverlapping(swapped.as_ptr(), sd, swapped.len());
        }
        assert_eq!(
            unsafe { worker.process_and_deliver(&mut pb) },
            AVERROR_EXTERNAL,
            "a reorder that changes the derived projection must be rejected"
        );
        assert!(matches!(
            worker.pending_error_cloned(),
            Some(PacketSinkError::ConfigChange { .. })
        ));
    }

    /// The real (not debug-only) phase guard: a packet before stream-info
    /// readiness is a typed sequencing violation and delivers nothing.
    #[test]
    fn packets_before_stream_info_are_a_phase_violation() {
        let ctx = video_ctx();
        let (sink, events) = recording_sink();
        let mut worker = collect(&ctx, sink).unwrap();
        let mut pb = packet(&idr_au(), 0, 0, tb25(), 0);
        assert_eq!(
            unsafe { worker.process_and_deliver(&mut pb) },
            AVERROR_EXTERNAL
        );
        assert!(matches!(
            worker.pending_error_cloned(),
            Some(PacketSinkError::PhaseViolation { stream_index: 0 })
        ));
        assert!(
            events.lock().unwrap().is_empty(),
            "nothing may be delivered out of phase"
        );
    }

    #[test]
    fn finish_gate_matrix() {
        let ctx = video_ctx();
        let run = |all_done: bool, ret: i32, aborted: bool, job_error: Option<String>| {
            let (sink, events) = recording_sink();
            let mut worker = collect(&ctx, sink).unwrap();
        assert_eq!(worker.deliver_stream_info(), 0);
            worker.finish(all_done, ret, aborted, job_error);
            // Finish is a one-way phase transition: a second call must be a
            // no-op and can never fire a second terminal event.
            worker.finish(true, 0, false, None);
            let events = events.lock().unwrap();
            let ends = events
                .iter()
                .filter(|e| matches!(e, PacketSinkEvent::End))
                .count();
            let errors = events
                .iter()
                .filter(|e| matches!(e, PacketSinkEvent::Error(_)))
                .count();
            (ends, errors)
        };
        assert_eq!(run(true, 0, false, None), (1, 0), "healthy completion");
        assert_eq!(run(false, 0, false, None), (0, 0), "streams not terminal");
        assert_eq!(run(true, -5, false, None), (0, 0), "delivery error");
        assert_eq!(run(true, 0, true, None), (0, 0), "abort");
        assert_eq!(
            run(true, 0, false, Some("sibling failed".to_string())),
            (0, 1),
            "a job error after clean drain reports on_delivery_error, never on_end"
        );
    }

    /// Cancellation precedence when a sibling failure races the shutdown:
    /// this sink observes a stopping status that carries NO recorded error
    /// (an explicit stop()) and cancels its blocked send cooperatively; a
    /// sibling's error is recorded only AFTER that observation. The terminal
    /// must stay silent — neither `on_end` nor `on_delivery_error` — while
    /// the recorded error is left untouched for `wait()` to report.
    ///
    /// Scope: the scheduler-level interleaving (a live stop() racing a live
    /// sibling failure across worker threads) cannot be forced
    /// deterministically, so this pins the worker-visible half of the race
    /// end to end — the cooperative-cancel classification at the parked
    /// send, the silent terminal, and the preserved job result that wait()
    /// surfaces.
    #[test]
    fn cancelled_delivery_stays_silent_when_sibling_error_lands_later() {
        use crate::core::scheduler::ffmpeg_scheduler::{STATUS_END, STATUS_RUN};
        use std::sync::atomic::Ordering;

        let ctx = video_ctx();
        let (sink, rx) = PacketSink::channel(std::num::NonZeroUsize::new(1).unwrap());
        let status = Arc::new(AtomicUsize::new(STATUS_RUN));
        let result: Arc<Mutex<Option<crate::error::Result<()>>>> = Arc::new(Mutex::new(None));
        let mut worker = unsafe {
            PacketSinkWorker::collect(ctx.ctx, ctx.stream_count(), sink, &status, &result)
        }
        .map_err(|boxed| (*boxed).1)
        .unwrap();
        // The stream-info event fills the capacity-1 channel; nothing drains.
        assert_eq!(worker.deliver_stream_info(), 0);
        // stop(): a stopping status published with NO recorded error.
        status.store(STATUS_END, Ordering::Release);
        // The parked send observes the stop, finds no recorded error, and
        // classifies the exit as cooperative cancellation.
        let mut pb = packet(&idr_au(), 0, 0, tb25(), 0);
        assert_eq!(
            unsafe { worker.process_and_deliver(&mut pb) },
            AVERROR_EXTERNAL
        );
        assert!(worker.cancelled_cleanly());
        // The sibling's failure lands only now, after the classification.
        *result.lock().unwrap() = Some(Err(crate::error::Error::WorkerPanicked(
            "muxer1:mpegts".to_string(),
        )));
        // Terminal with the job error visible (what the mux worker reads
        // after full settlement): cancellation still wins.
        worker.finish(false, AVERROR_EXTERNAL, false, Some("muxer1:mpegts".to_string()));
        drop(worker);
        let (mut ends, mut errors) = (0, 0);
        while let Ok(event) = rx.try_recv() {
            match event {
                PacketSinkEvent::End => ends += 1,
                PacketSinkEvent::Error(_) => errors += 1,
                _ => {}
            }
        }
        assert_eq!(
            (ends, errors),
            (0, 0),
            "a cooperatively cancelled delivery must stay silent even when a \
             sibling error is recorded before the terminal"
        );
        // The silent terminal left the sibling error in place: this mutex is
        // exactly what wait() surfaces as the job result.
        assert!(matches!(
            result.lock().unwrap().as_ref(),
            Some(Err(crate::error::Error::WorkerPanicked(_)))
        ));
    }
}
