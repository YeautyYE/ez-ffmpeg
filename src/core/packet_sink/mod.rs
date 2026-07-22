//! Encoded-packet output: consume encoder packets directly, without a muxer.
//!
//! A packet sink is the fourth quadrant of the crate's I/O matrix (decoded
//! frames out = frame export, PCM out = sample export, frames in =
//! [`VideoWriter`](crate::VideoWriter), **encoded packets out = packet sink**).
//! Instead of muxing packets into container bytes, the job hands each encoded
//! packet to a consumer, normalized for WebCodecs-style use.
//!
//! # Strict tier (v1)
//!
//! The construction paths on [`PacketSink`] build a **strict-tier** sink —
//! [`PacketView`], [`PacketStreamInfo`] and the callback bundle are the
//! strict-tier contract, aligned with WebCodecs `"avc"` / AAC consumption:
//!
//! * **H.264 video** is delivered as avcC-configured, 4-byte length-prefixed,
//!   access-unit-complete packets. The encoder whitelist is `libx264` only —
//!   the delivery contract assumes one packet == one access unit, which is
//!   established for libx264 and not verified for other encoders. Any other
//!   video encoder fails the build with a typed error.
//! * **AAC audio** is delivered as raw AAC frames; the stream configuration
//!   carries the AudioSpecificConfig.
//! * Anything else (subtitles, data streams, stream copy, bitstream filters)
//!   is rejected up front with a typed [`PacketSinkError`].
//!
//! Future tiers (generic passthrough, HEVC, Annex-B) will introduce their own
//! construction paths and view/config types; everything here is
//! `#[non_exhaustive]` so that growth is additive.
//!
//! # Callback order
//!
//! All callbacks run **serially on the one delivery (mux worker) thread** —
//! never concurrently, never reentrantly — in this order:
//!
//! 1. `on_stream_info` — at most once, after every encoder finalized its
//!    parameters and **before any packet**. The video configuration is
//!    already a valid avcC record here.
//! 2. `on_packet` — zero or more times.
//! 3. `on_end` **or** `on_delivery_error` — at most one of them, at most
//!    once:
//!    * `on_end` fires only when every output stream reached a recognized
//!      terminal state (natural encoder EOF, or configured truncation such as
//!      `set_recording_time_us` / `set_shortest`), everything was delivered,
//!      and the whole job settled without an error: the delivery thread
//!      first waits for every other job worker to finish (including
//!      container outputs' teardown), then decides on one fresh
//!      status/result read — the linearization point. Sibling packet-sink
//!      workers are the one exception to that wait: they are only
//!      guaranteed settled by then (errors recorded, encoders joined,
//!      contexts freed) — their terminal callbacks and capture drops may
//!      still be running concurrently. An `abort()` that lands after the
//!      status read is indistinguishable from one after the callback.
//!    * `on_delivery_error` fires when delivery stopped because of a
//!      strict-tier violation or a failing callback, or when the job failed
//!      elsewhere — whether that failure landed after this sink delivered
//!      everything or truncated its delivery. Cancellation is silent only
//!      when it interrupts delivery: a `stop()` that lands after this sink
//!      fully drained still delivers `on_end`. Cancellation also takes
//!      precedence over a failure it races with: a sink that observes the
//!      termination `stop()`/`abort()` published and cancels its delivery
//!      cooperatively before a sibling's error is recorded stays silent —
//!      no `on_delivery_error`. The late error is still recorded
//!      first-error-wins as the job result, and the `stop()` call that
//!      drove the race returns it once every worker has settled; after
//!      `abort()`, which returns nothing, it goes unobserved.
//!
//! # Timestamp and ordering
//!
//! Timestamps are per-stream: within one stream, dts is strictly increasing
//! and `pts >= dts`. **No cross-stream interleaving order is promised** —
//! audio and video packets arrive in worker order, and a consumer must route
//! by [`PacketView::stream_index`] rather than assume global ordering. All
//! streams share one time origin (see [`PacketView::applied_offset`]). A
//! packet that violates the strict contract (including a mid-stream
//! configuration change) fails the job typed and is **never delivered**.
//!
//! # Failure and panic
//!
//! The scheduler result returned by `wait()`/`stop()` is **authoritative**;
//! terminal callbacks are a convenience with deliberately narrower coverage.
//! In these cases **no terminal sink callback fires at all**:
//!
//! * initial configuration failure (missing/malformed extradata, whitelist
//!   violations) — the job fails before any callback runs;
//! * cancellation (`stop()` with packets still in flight, `abort()`);
//! * a panicking DELIVERY callback (`on_stream_info`, `on_packet`) — the job
//!   fails with a worker-panic error and no further sink callback is
//!   invoked.
//!
//! Single carve-out — the post-settlement region: once the job has settled
//! and the terminal decision is made, everything that remains on the
//! delivery thread is user code (the terminal callback itself, then the
//! destruction of the consumer's captures at the defined teardown point).
//! A panic ANYWHERE in that region — `on_end`, `on_delivery_error`, or a
//! capture's `Drop` — is caught, logged at error level, and does NOT change
//! the already-settled job result (a delivered or decided `on_end` still
//! yields `wait() == Ok`, and a failing job keeps its original error).
//!
//! That containment is **per callback box** (per handler box for
//! [`PacketSinkHandler`](crate::packet_sink::PacketSinkHandler)). A panic
//! thrown by a callback, or by ONE
//! destructor — a captured value's, a stashed error source's, or a
//! `panic_any` payload's — is contained, and the crate keeps every such
//! unwind single: each box is destroyed under its own catch, and the
//! stashed delivery error stays in the worker's custody while
//! `on_delivery_error` borrows it. The boundary is Rust's own unwind
//! semantics: when one capture's destructor panics, the remaining captures
//! OF THAT SAME BOX are dropped by the unwind itself — an erased box
//! destroys its captures as one indivisible drop-glue call that nothing
//! outside the box can decompose — so a SECOND panicking destructor there
//! is a panic-during-unwind process abort, exactly as in any Rust struct
//! whose field destructors both panic. Keep the destructors of values
//! captured together panic-free relative to one another.
//!
//! # Backpressure: callbacks block the pipeline
//!
//! **The callbacks run on the delivery thread. A slow `on_packet` blocks that
//! thread, the bounded packet queue behind it fills, and the encoders stall —
//! exactly the backpressure a slow container write exerts today.** No packet
//! is ever silently dropped. If you need decoupling, copy the borrowed data
//! out (it is only valid during the callback) and queue it yourself, or use
//! [`PacketSink::channel`], which does that copy for you and blocks the
//! pipeline only while its bounded channel is full. The channel's blocking
//! send observes job cancellation, so `stop()` terminates even with a full,
//! undrained channel.
//!
//! # Example
//!
//! ```rust,no_run
//! use ez_ffmpeg::packet_sink::PacketSink;
//! use ez_ffmpeg::{FfmpegContext, Output};
//!
//! fn main() -> Result<(), Box<dyn std::error::Error>> {
//!     let sink = PacketSink::builder(|packet| {
//!         println!(
//!             "stream {} pts {} ({} bytes)",
//!             packet.stream_index(),
//!             packet.pts(),
//!             packet.data().len()
//!         );
//!         Ok(())
//!     })
//!     .on_end(|| println!("done"))
//!     .build();
//!
//!     FfmpegContext::builder()
//!         .input("input.mp4")
//!         .output(Output::from(sink).set_video_codec("libx264"))
//!         .build()?
//!         .start()?
//!         .wait()?;
//!     Ok(())
//! }
//! ```

pub use crate::error::PacketSinkError;
use crate::core::scheduler::ffmpeg_scheduler::{is_stopping, FfmpegScheduler, Running};
use crate::core::scheduler::owned_run_iter::OwnedRunIter;
use ffmpeg_sys_next::{AVCodecID, AVMediaType, AVRational};
use std::num::NonZeroUsize;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::{Arc, OnceLock};
use std::time::Duration;

pub(crate) mod codec;
pub(crate) mod nal_framing;
pub(crate) mod side_data;
pub(crate) mod strict;
pub(crate) mod timeline;

/// Delivery tier of a packet sink. Only [`Strict`](PacketSinkTier::Strict)
/// exists in v1; the enum is `#[non_exhaustive]` so later tiers (generic
/// passthrough, HEVC, Annex-B) are additive.
///
/// The strict construction paths ([`PacketSink::builder`],
/// [`PacketSink::from_handler`], [`PacketSink::channel`]) do NOT take a tier:
/// they are strict-tier by definition, because their callback bundle is typed
/// to the strict [`PacketView`]/[`PacketStreamInfo`] contract (mandatory
/// `i64` timestamps and durations). A future tier arrives as its own
/// constructor with its own view/config/callback types — never by routing a
/// different tier through the strict bundle.
#[non_exhaustive]
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub enum PacketSinkTier {
    /// WebCodecs-aligned strict tier: avcC H.264 (libx264) + AAC.
    #[default]
    Strict,
}

/// Why a callback rejected delivery. Carries a message and an optional
/// source error, both preserved on the job result via
/// [`PacketSinkError::PacketCallbackFailed`].
#[derive(Debug, Clone)]
pub struct PacketCallbackError {
    message: String,
    source: Option<Arc<dyn std::error::Error + Send + Sync + 'static>>,
    pub(crate) kind: CallbackFailureKind,
}

/// Internal classification of a callback failure.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum CallbackFailureKind {
    /// A consumer-reported failure: the job stops with a typed error.
    Failure,
    /// The owned-channel receiver is gone: the job stops with
    /// [`PacketSinkError::ChannelDisconnected`].
    Disconnected,
    /// The job is already stopping WITHOUT a recorded error (explicit
    /// `stop()`/`abort()`) and a blocking send bailed out cooperatively:
    /// NOT an error (mirrors the worker's stop observation).
    Cancelled,
    /// The job is stopping because some worker recorded a FAILURE while a
    /// blocking send was parked: delivery is truncated by that job failure
    /// (the terminal reports it as `JobFailed`), not cancelled.
    JobStopped,
}

impl PacketCallbackError {
    /// A failure described by a message.
    pub fn new(message: impl Into<String>) -> Self {
        Self {
            message: message.into(),
            source: None,
            kind: CallbackFailureKind::Failure,
        }
    }

    /// A failure wrapping a source error (preserved on the job result).
    pub fn with_source(
        message: impl Into<String>,
        source: impl std::error::Error + Send + Sync + 'static,
    ) -> Self {
        Self {
            message: message.into(),
            source: Some(Arc::new(source)),
            kind: CallbackFailureKind::Failure,
        }
    }

    pub(crate) fn disconnected() -> Self {
        Self {
            message: "packet-sink channel receiver dropped".to_string(),
            source: None,
            kind: CallbackFailureKind::Disconnected,
        }
    }

    pub(crate) fn job_stopped() -> Self {
        Self {
            message: "job failed elsewhere; blocking send abandoned".to_string(),
            source: None,
            kind: CallbackFailureKind::JobStopped,
        }
    }

    pub(crate) fn cancelled() -> Self {
        Self {
            message: "job stopping; blocking send cancelled".to_string(),
            source: None,
            kind: CallbackFailureKind::Cancelled,
        }
    }
}

impl std::fmt::Display for PacketCallbackError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str(&self.message)
    }
}

impl std::error::Error for PacketCallbackError {
    fn source(&self) -> Option<&(dyn std::error::Error + 'static)> {
        self.source
            .as_ref()
            .map(|s| s.as_ref() as &(dyn std::error::Error + 'static))
    }
}

/// What every fallible sink callback returns: `Ok(())` continues delivery, an
/// error stops the job with a typed, source-preserving [`PacketSinkError`].
pub type PacketCallbackResult = Result<(), PacketCallbackError>;

/// A single stateful packet consumer. All methods run serially on the one
/// delivery thread (never concurrently, never reentrantly), so `&mut self`
/// state needs no locking. This is the strict-tier handler shape; see the
/// [module docs](self) for the callback order and backpressure contract.
///
/// Teardown panic containment is per handler box: a panic from a method,
/// or from ONE of the handler's fields' destructors, is contained; two
/// fields whose destructors both panic compose into a panic-during-unwind
/// process abort, as in any Rust struct — see "Failure and panic" in the
/// [module docs](self).
pub trait PacketSinkHandler: Send + 'static {
    /// One-time stream configuration, before any packet.
    fn on_stream_info(&mut self, _streams: &[PacketStreamInfo]) -> PacketCallbackResult {
        Ok(())
    }

    /// One delivered packet; the borrowed view is valid only for this call.
    fn on_packet(&mut self, packet: &PacketView<'_>) -> PacketCallbackResult;

    /// Terminal success (see the module docs for the exact gate). A panic
    /// here is contained and cannot change the settled job result.
    fn on_end(&mut self) {}

    /// Terminal failure. For delivery-path errors (strict-tier violations,
    /// failing callbacks) the same error is also the job result. When the
    /// JOB failed elsewhere (after this sink drained or truncating its
    /// delivery), the callback receives a synthesized
    /// [`PacketSinkError::JobFailed`] summarizing that failure, while
    /// `wait()`/`stop()` keep the original error.
    fn on_delivery_error(&mut self, _error: &PacketSinkError) {}
}

/// Per-stream video configuration delivered via `on_stream_info` —
/// everything a WebCodecs `VideoDecoder` / fMP4 packager needs, precomputed.
#[non_exhaustive]
#[derive(Debug, Clone)]
pub struct VideoPacketConfig {
    pub(crate) stream_index: usize,
    pub(crate) codec_id: AVCodecID,
    pub(crate) codec_string: String,
    pub(crate) profile: u8,
    pub(crate) compatibility: u8,
    pub(crate) level: u8,
    pub(crate) codec_config: Vec<u8>,
    pub(crate) time_base: AVRational,
    pub(crate) width: i32,
    pub(crate) height: i32,
    pub(crate) sample_aspect_ratio: Option<AVRational>,
    pub(crate) frame_rate: Option<AVRational>,
}

impl VideoPacketConfig {
    /// Output stream index; matches [`PacketView::stream_index`].
    pub fn stream_index(&self) -> usize {
        self.stream_index
    }

    /// FFmpeg codec id (`AV_CODEC_ID_H264` in the strict tier).
    pub fn codec_id(&self) -> AVCodecID {
        self.codec_id
    }

    /// RFC 6381 codec string (`"avc1.PPCCLL"`), suitable as the WebCodecs
    /// `codec` value.
    pub fn codec_string(&self) -> &str {
        &self.codec_string
    }

    /// H.264 `profile_idc` (the avcC `AVCProfileIndication`; e.g. 66 =
    /// Baseline, 77 = Main, 100 = High). Same source as
    /// [`codec_string`](Self::codec_string).
    pub fn profile(&self) -> u8 {
        self.profile
    }

    /// The avcC `profile_compatibility` byte (constraint-set flags).
    pub fn compatibility(&self) -> u8 {
        self.compatibility
    }

    /// H.264 `level_idc` (the avcC `AVCLevelIndication`; e.g. 30 = level
    /// 3.0, 0x1F = level 3.1).
    pub fn level(&self) -> u8 {
        self.level
    }

    /// The `AVCDecoderConfigurationRecord` (avcC), suitable as the WebCodecs
    /// `description`.
    pub fn codec_config(&self) -> &[u8] {
        &self.codec_config
    }

    /// FFmpeg-oriented alias of [`codec_config`](Self::codec_config).
    pub fn extradata(&self) -> &[u8] {
        &self.codec_config
    }

    /// Time base every timestamp of this stream is expressed in (the encoder
    /// time base, passed through verbatim).
    pub fn time_base(&self) -> AVRational {
        self.time_base
    }

    /// Coded width in pixels.
    pub fn width(&self) -> i32 {
        self.width
    }

    /// Coded height in pixels.
    pub fn height(&self) -> i32 {
        self.height
    }

    /// Sample aspect ratio, when known.
    pub fn sample_aspect_ratio(&self) -> Option<AVRational> {
        self.sample_aspect_ratio
    }

    /// Nominal frame rate. `None` when the pipeline did not pin one (VFR
    /// sources, and CFR jobs without an explicit output rate).
    pub fn frame_rate(&self) -> Option<AVRational> {
        self.frame_rate
    }
}

/// Per-stream audio configuration delivered via `on_stream_info`.
#[non_exhaustive]
#[derive(Debug, Clone)]
pub struct AudioPacketConfig {
    pub(crate) stream_index: usize,
    pub(crate) codec_id: AVCodecID,
    pub(crate) codec_string: String,
    pub(crate) codec_config: Vec<u8>,
    pub(crate) time_base: AVRational,
    pub(crate) sample_rate: i32,
    pub(crate) channels: i32,
    pub(crate) channel_layout: String,
}

impl AudioPacketConfig {
    /// Output stream index; matches [`PacketView::stream_index`].
    pub fn stream_index(&self) -> usize {
        self.stream_index
    }

    /// FFmpeg codec id (`AV_CODEC_ID_AAC` in the strict tier).
    pub fn codec_id(&self) -> AVCodecID {
        self.codec_id
    }

    /// RFC 6381 codec string (`"mp4a.40.X"`, X = audio object type).
    pub fn codec_string(&self) -> &str {
        &self.codec_string
    }

    /// The `AudioSpecificConfig`, suitable as the WebCodecs `description`.
    pub fn codec_config(&self) -> &[u8] {
        &self.codec_config
    }

    /// FFmpeg-oriented alias of [`codec_config`](Self::codec_config).
    pub fn extradata(&self) -> &[u8] {
        &self.codec_config
    }

    /// Time base every timestamp of this stream is expressed in.
    pub fn time_base(&self) -> AVRational {
        self.time_base
    }

    /// Sample rate in Hz.
    pub fn sample_rate(&self) -> i32 {
        self.sample_rate
    }

    /// Channel count.
    pub fn channels(&self) -> i32 {
        self.channels
    }

    /// FFmpeg channel-layout description (e.g. `"stereo"`, `"5.1"`).
    pub fn channel_layout(&self) -> &str {
        &self.channel_layout
    }
}

/// Per-stream configuration delivered once via `on_stream_info`, typed by
/// media kind (mirrors the crate's `StreamInfo` shape).
#[non_exhaustive]
#[derive(Debug, Clone)]
pub enum PacketStreamInfo {
    /// H.264 stream configuration: avcC record, RFC 6381 codec string,
    /// profile/level, dimensions, time base, frame rate.
    Video(VideoPacketConfig),
    /// AAC stream configuration: AudioSpecificConfig, RFC 6381 codec string,
    /// time base, sample rate, channel layout.
    Audio(AudioPacketConfig),
}

impl PacketStreamInfo {
    /// Output stream index; matches [`PacketView::stream_index`].
    pub fn stream_index(&self) -> usize {
        match self {
            PacketStreamInfo::Video(v) => v.stream_index,
            PacketStreamInfo::Audio(a) => a.stream_index,
        }
    }

    /// Media type of the stream.
    pub fn media_type(&self) -> AVMediaType {
        match self {
            PacketStreamInfo::Video(_) => AVMediaType::AVMEDIA_TYPE_VIDEO,
            PacketStreamInfo::Audio(_) => AVMediaType::AVMEDIA_TYPE_AUDIO,
        }
    }

    /// FFmpeg codec id.
    pub fn codec_id(&self) -> AVCodecID {
        match self {
            PacketStreamInfo::Video(v) => v.codec_id,
            PacketStreamInfo::Audio(a) => a.codec_id,
        }
    }

    /// RFC 6381 codec string (`"avc1.PPCCLL"` / `"mp4a.40.X"`).
    pub fn codec_string(&self) -> &str {
        match self {
            PacketStreamInfo::Video(v) => &v.codec_string,
            PacketStreamInfo::Audio(a) => &a.codec_string,
        }
    }

    /// Codec configuration record (avcC / AudioSpecificConfig).
    pub fn codec_config(&self) -> &[u8] {
        match self {
            PacketStreamInfo::Video(v) => &v.codec_config,
            PacketStreamInfo::Audio(a) => &a.codec_config,
        }
    }

    /// FFmpeg-oriented alias of [`codec_config`](Self::codec_config).
    pub fn extradata(&self) -> &[u8] {
        self.codec_config()
    }

    /// Time base every timestamp of this stream is expressed in.
    pub fn time_base(&self) -> AVRational {
        match self {
            PacketStreamInfo::Video(v) => v.time_base,
            PacketStreamInfo::Audio(a) => a.time_base,
        }
    }

    /// The video configuration, when this is a video stream.
    pub fn video(&self) -> Option<&VideoPacketConfig> {
        match self {
            PacketStreamInfo::Video(v) => Some(v),
            _ => None,
        }
    }

    /// The audio configuration, when this is an audio stream.
    pub fn audio(&self) -> Option<&AudioPacketConfig> {
        match self {
            PacketStreamInfo::Audio(a) => Some(a),
            _ => None,
        }
    }
}

/// Converts stream ticks to microseconds (exact rescale, round-nearest).
fn ticks_to_us(ticks: i64, time_base: AVRational) -> i64 {
    // SAFETY: pure integer arithmetic; every stream time base was validated
    // positive at collection, and the target rational is a constant.
    unsafe {
        ffmpeg_sys_next::av_rescale_q(
            ticks,
            time_base,
            AVRational {
                num: 1,
                den: 1_000_000,
            },
        )
    }
}

/// Borrowed view of one delivered packet (strict tier).
///
/// The view — including [`data`](Self::data) — is valid **only during the
/// `on_packet` callback**; the underlying packet is recycled as soon as the
/// callback returns. Copy out what you keep.
#[non_exhaustive]
#[derive(Debug)]
pub struct PacketView<'a> {
    pub(crate) stream_index: usize,
    pub(crate) pts: i64,
    pub(crate) dts: i64,
    pub(crate) duration: i64,
    pub(crate) time_base: AVRational,
    pub(crate) is_key: bool,
    pub(crate) applied_offset: i64,
    pub(crate) data: &'a [u8],
}

impl<'a> PacketView<'a> {
    /// Output stream index (matches the `on_stream_info` entries).
    pub fn stream_index(&self) -> usize {
        self.stream_index
    }

    /// Presentation timestamp in [`time_base`](Self::time_base) units, on the
    /// shared zero-based timeline (see
    /// [`applied_offset`](Self::applied_offset)).
    pub fn pts(&self) -> i64 {
        self.pts
    }

    /// Decode timestamp in [`time_base`](Self::time_base) units, strictly
    /// increasing per stream. May be negative on non-anchor streams (a stream
    /// whose timeline starts earlier than the anchor stream keeps its true
    /// relative offset) and, with B-frames, ahead of `pts` reordering.
    pub fn dts(&self) -> i64 {
        self.dts
    }

    /// Packet duration in [`time_base`](Self::time_base) units. Always
    /// positive in the strict tier: the encoder's duration is passed through;
    /// when absent it is derived (video: one CFR frame interval; audio: the
    /// codec frame size). A packet whose duration cannot be derived fails the
    /// job before delivery — this field is never a guess of zero.
    pub fn duration(&self) -> i64 {
        self.duration
    }

    /// Time base of this stream (identical to the stream's
    /// [`PacketStreamInfo::time_base`]).
    pub fn time_base(&self) -> AVRational {
        self.time_base
    }

    /// [`pts`](Self::pts) in microseconds (exact rescale of the ticks).
    pub fn pts_us(&self) -> i64 {
        ticks_to_us(self.pts, self.time_base)
    }

    /// [`dts`](Self::dts) in microseconds.
    pub fn dts_us(&self) -> i64 {
        ticks_to_us(self.dts, self.time_base)
    }

    /// [`duration`](Self::duration) in microseconds.
    pub fn duration_us(&self) -> i64 {
        ticks_to_us(self.duration, self.time_base)
    }

    /// [`applied_offset`](Self::applied_offset) in microseconds.
    pub fn applied_offset_us(&self) -> i64 {
        ticks_to_us(self.applied_offset, self.time_base)
    }

    /// Whether this packet is a fresh-decoder-safe random access point.
    ///
    /// For H.264 this is true **iff the access unit contains an IDR NAL
    /// unit** — deliberately not the encoder's raw `AV_PKT_FLAG_KEY`: with
    /// open-GOP encoding, encoders flag non-IDR recovery points as key
    /// frames, and feeding such a packet to a fresh decoder (the WebCodecs
    /// `"key"` contract) is not safe. Audio packets are always key.
    pub fn is_key(&self) -> bool {
        self.is_key
    }

    /// The per-stream offset that was subtracted from `pts`/`dts` to move
    /// this stream onto the shared zero-based timeline, in this stream's
    /// [`time_base`](Self::time_base) units.
    ///
    /// All streams share a single origin: the `(dts, time_base)` of the first
    /// delivered packet of the job. The anchor stream therefore starts at
    /// dts 0; other streams keep their true audio/video offset (which may be
    /// negative). `original_ts = delivered_ts + applied_offset` recovers the
    /// encoder timeline exactly (cross-time-base rounding is at most one tick
    /// per stream).
    pub fn applied_offset(&self) -> i64 {
        self.applied_offset
    }

    /// The packet payload. H.264: one complete access unit, 4-byte
    /// length-prefixed (AVCC), parameter sets carried out-of-band in the
    /// stream configuration. AAC: one raw AAC frame.
    pub fn data(&self) -> &'a [u8] {
        self.data
    }
}

pub(crate) type StreamInfoFn =
    Box<dyn FnMut(&[PacketStreamInfo]) -> PacketCallbackResult + Send>;
pub(crate) type PacketFn =
    Box<dyn for<'a> FnMut(&PacketView<'a>) -> PacketCallbackResult + Send>;
pub(crate) type EndFn = Box<dyn FnMut() + Send>;
pub(crate) type DeliveryErrorFn = Box<dyn FnMut(&PacketSinkError) + Send>;

/// How the sink dispatches callbacks: four independent closures, or one
/// stateful handler. Either way every call runs serially on the delivery
/// thread.
enum SinkDispatch {
    Closures {
        on_stream_info: Option<StreamInfoFn>,
        on_packet: PacketFn,
        on_end: Option<EndFn>,
        on_delivery_error: Option<DeliveryErrorFn>,
    },
    Handler(Box<dyn PacketSinkHandler>),
}

/// What the owned-channel adapter observes about the job while a bounded
/// send is blocked: the scheduler status (has the job stopped?) and the
/// scheduler result (did it stop because some worker FAILED?). Published by
/// the worker at collection time.
pub(crate) struct JobStopObservables {
    pub(crate) status: Arc<AtomicUsize>,
    pub(crate) result: Arc<std::sync::Mutex<Option<crate::error::Result<()>>>>,
}

/// Slot the owned-channel adapter uses to observe job cancellation: see
/// [`JobStopObservables`]. One slot is allocated per [`PacketSink::channel`]
/// call and shared by the sink's callbacks and its receiver, so the Arc's
/// pointer identity doubles as the pair's run token: the muxer keeps a clone
/// and [`PacketSinkReceiver::into_events`] matches its own clone against the
/// scheduler's job to reject a cross-wired scheduler.
pub(crate) type CancellationSlot = Arc<OnceLock<JobStopObservables>>;

/// The consumer bundle handed to `Output::from(sink)` /
/// [`Output::new_by_packet_sink`](crate::Output::new_by_packet_sink).
///
/// Build one with [`PacketSink::builder`] (closures),
/// [`PacketSink::from_handler`] (one stateful consumer) or
/// [`PacketSink::channel`] (owned events over a bounded channel). All
/// construction paths produce a **strict-tier** sink; see the
/// [module docs](self) for the callback order and the **blocking
/// backpressure** contract.
pub struct PacketSink {
    pub(crate) tier: PacketSinkTier,
    dispatch: SinkDispatch,
    /// `Some` only for channel-adapter sinks (see [`CancellationSlot`]).
    pub(crate) cancellation: Option<CancellationSlot>,
}

impl std::fmt::Debug for PacketSink {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("PacketSink")
            .field("tier", &self.tier)
            .finish_non_exhaustive()
    }
}

impl PacketSink {
    /// The delivery tier this sink was built for.
    ///
    /// Every v1 construction path produces [`PacketSinkTier::Strict`], so
    /// today this always returns `Strict`; the accessor exists so consumers
    /// that route or log sinks can branch on the tier once additional tiers
    /// land, instead of inferring it from which constructor was used.
    pub fn tier(&self) -> PacketSinkTier {
        self.tier
    }

    /// Starts building a strict-tier sink around the required packet
    /// consumer. `on_stream_info`, `on_end` and `on_delivery_error` are
    /// optional extras on the returned builder — but a sink cannot exist
    /// without a packet consumer (a job that encodes into nothing is a
    /// configuration mistake, not a default; use [`PacketSink::discard`]
    /// when discarding is genuinely intended).
    ///
    /// Teardown panic containment is per callback box: a panic from the
    /// closure, or from ONE captured value's destructor, is contained; two
    /// captures of this same closure whose destructors both panic compose
    /// into a panic-during-unwind process abort, as in any Rust struct —
    /// see "Failure and panic" in the [module docs](self).
    pub fn builder<F>(on_packet: F) -> PacketSinkBuilder
    where
        F: for<'a> FnMut(&PacketView<'a>) -> PacketCallbackResult + Send + 'static,
    {
        PacketSinkBuilder {
            tier: PacketSinkTier::Strict,
            on_stream_info: None,
            on_packet: Box::new(on_packet),
            on_end: None,
            on_delivery_error: None,
        }
    }

    /// A sink that deliberately discards every packet (accepting them all).
    /// Exists so intent is explicit — mainly for validation-only jobs and
    /// tests.
    pub fn discard() -> PacketSink {
        PacketSink::builder(|_| Ok(())).build()
    }

    /// Builds a strict-tier sink around one stateful [`PacketSinkHandler`] —
    /// the natural shape for consumers whose stream-info/packet/terminal
    /// handling shares state (packagers, senders); callbacks are serial, so
    /// the handler needs no locking.
    ///
    /// Teardown panic containment is per handler box: a panic from a
    /// handler method, or from ONE of the handler's fields' destructors, is
    /// contained; two fields of this same handler whose destructors both
    /// panic compose into a panic-during-unwind process abort, as in any
    /// Rust struct — see "Failure and panic" in the [module docs](self).
    pub fn from_handler<H: PacketSinkHandler>(handler: H) -> PacketSink {
        PacketSink {
            tier: PacketSinkTier::Strict,
            dispatch: SinkDispatch::Handler(Box::new(handler)),
            cancellation: None,
        }
    }

    /// Builds a strict-tier sink that forwards everything over a **bounded**
    /// channel of owned events, for consumers that want packets on their own
    /// thread.
    ///
    /// Every payload is copied once into an owned [`EncodedPacket`] (one
    /// additional adapter copy on top of any Annex-B normalization). The
    /// channel preserves the callback contract: when it is full, the sending
    /// callback **blocks the pipeline** until the consumer catches up — no
    /// packet is dropped. **Drain the receiver concurrently** (its own
    /// thread, or [`PacketSinkReceiver::into_events`]); draining only after
    /// `wait()` deadlocks as soon as the channel fills, because `wait()`
    /// needs the blocked worker to finish. The blocking send observes job
    /// cancellation, so `stop()`/`abort()` (or a job failing elsewhere)
    /// terminates even with a full, undrained channel. Dropping the receiver
    /// cancels the job with [`PacketSinkError::ChannelDisconnected`].
    ///
    /// Terminal `End`/`Error` events are delivered best-effort: when the
    /// channel is full and the consumer never drains, they are dropped —
    /// sender disconnection (`Disconnected` on the receiver) is the
    /// authoritative end-of-events signal either way.
    pub fn channel(capacity: NonZeroUsize) -> (PacketSink, PacketSinkReceiver) {
        let (tx, rx) = crossbeam_channel::bounded::<PacketSinkEvent>(capacity.get());
        let cancellation: CancellationSlot = Arc::new(OnceLock::new());
        let info_tx = tx.clone();
        let info_cancel = cancellation.clone();
        let pkt_tx = tx.clone();
        let pkt_cancel = cancellation.clone();
        let end_tx = tx.clone();
        let err_tx = tx;
        let mut sink = PacketSink::builder(move |packet: &PacketView<'_>| {
            send_with_cancellation(
                &pkt_tx,
                &pkt_cancel,
                PacketSinkEvent::Packet(EncodedPacket::from_view(packet)),
            )
        })
        .on_stream_info(move |infos: &[PacketStreamInfo]| {
            send_with_cancellation(
                &info_tx,
                &info_cancel,
                PacketSinkEvent::StreamInfo(infos.to_vec()),
            )
        })
        .on_end(move || {
            // Best-effort terminal event: the job is already in its terminal
            // state here (a cancellation-aware blocking send would be
            // indistinguishable from try_send), and sender disconnection is
            // the authoritative signal.
            let _ = end_tx.try_send(PacketSinkEvent::End);
        })
        .on_delivery_error(move |e: &PacketSinkError| {
            let _ = err_tx.try_send(PacketSinkEvent::Error(e.clone()));
        })
        .build();
        sink.cancellation = Some(cancellation.clone());
        (
            sink,
            PacketSinkReceiver {
                inner: rx,
                token: cancellation,
            },
        )
    }

    // ---- crate-internal dispatch (serial, delivery thread only) ----

    pub(crate) fn dispatch_stream_info(
        &mut self,
        infos: &[PacketStreamInfo],
    ) -> PacketCallbackResult {
        match &mut self.dispatch {
            SinkDispatch::Closures { on_stream_info, .. } => match on_stream_info {
                Some(f) => f(infos),
                None => Ok(()),
            },
            SinkDispatch::Handler(h) => h.on_stream_info(infos),
        }
    }

    pub(crate) fn dispatch_packet(&mut self, packet: &PacketView<'_>) -> PacketCallbackResult {
        match &mut self.dispatch {
            SinkDispatch::Closures { on_packet, .. } => on_packet(packet),
            SinkDispatch::Handler(h) => h.on_packet(packet),
        }
    }

    pub(crate) fn dispatch_end(&mut self) {
        match &mut self.dispatch {
            SinkDispatch::Closures { on_end, .. } => {
                if let Some(f) = on_end {
                    f()
                }
            }
            SinkDispatch::Handler(h) => h.on_end(),
        }
    }

    pub(crate) fn dispatch_delivery_error(&mut self, error: &PacketSinkError) {
        match &mut self.dispatch {
            SinkDispatch::Closures {
                on_delivery_error, ..
            } => {
                if let Some(f) = on_delivery_error {
                    f(error)
                }
            }
            SinkDispatch::Handler(h) => h.on_delivery_error(error),
        }
    }

    /// Consumes the sink, dropping every user callback box under its OWN
    /// panic containment. The derived drop glue runs the boxes as one
    /// chain: after a first capture destructor panics, the REMAINING boxes
    /// are destroyed by that unwind itself, where a second panicking
    /// destructor aborts the process — so one `catch_unwind` around a plain
    /// `drop` of the whole aggregate contains only the first bomb. Dropping
    /// each box under its own catch keeps every unwind single. Returns true
    /// when any destructor panicked (each caught payload is disposed
    /// through [`dispose_panic_payload`], never re-dropped raw).
    pub(crate) fn dispose_contained(self) -> bool {
        let Self {
            tier: _,
            dispatch,
            cancellation,
        } = self;
        let mut panicked = false;
        match dispatch {
            SinkDispatch::Closures {
                on_stream_info,
                on_packet,
                on_end,
                on_delivery_error,
            } => {
                // Field declaration order — the order the derived drop glue
                // would have used.
                if let Some(f) = on_stream_info {
                    panicked |= drop_contained(f);
                }
                panicked |= drop_contained(on_packet);
                if let Some(f) = on_end {
                    panicked |= drop_contained(f);
                }
                if let Some(f) = on_delivery_error {
                    panicked |= drop_contained(f);
                }
            }
            SinkDispatch::Handler(handler) => {
                panicked |= drop_contained(handler);
            }
        }
        // The cancellation slot is crate data, but the shared job result
        // behind it can hold arbitrary error types; the (normally non-final)
        // Arc release is contained for the same price as the boxes.
        if let Some(slot) = cancellation {
            panicked |= drop_contained(slot);
        }
        panicked
    }
}

/// Drops `value` under its own panic containment. Returns true when the
/// destructor panicked; the caught payload is disposed, not re-dropped raw.
fn drop_contained<T>(value: T) -> bool {
    match std::panic::catch_unwind(std::panic::AssertUnwindSafe(move || drop(value))) {
        Ok(()) => false,
        Err(payload) => {
            dispose_panic_payload(payload);
            true
        }
    }
}

/// Disposes a caught panic payload without letting the payload's own
/// destructor start a second, uncontained unwind at the discard site.
///
/// `panic_any` lets panicking user code throw an ARBITRARY payload, and
/// nothing forbids one whose `Drop` panics again — with yet another such
/// payload. Discarding a `catch_unwind` error via `.is_err()` / `let _` /
/// a wildcard therefore runs an uncontained user destructor exactly where
/// the containment believed the panic was over. Each drop attempt here runs
/// under its own catch, following replacement payloads a bounded number of
/// times; a chain still panicking after the last attempt is deliberately
/// LEAKED via `mem::forget`. That trade is intentional: a bounded leak on
/// an adversarial path is recoverable, while re-throwing would unwind
/// frames that may still own user state — and a destructor panic during
/// that unwind escalates to a process abort.
///
/// The containment boundary, here and in every catch this module owns, is
/// per BOX: a panic thrown by a callback, or by ONE destructor (a
/// capture's, a stashed error source's, or this payload's), is contained.
/// A box whose own captured fields' destructors panic DURING that unwind —
/// two bombs inside one erased `Box<dyn ..>` — aborts the process by
/// Rust's panic-during-unwind rule before any catch regains control: the
/// box destroys its captures as one indivisible drop-glue call that no
/// code outside the box can decompose. That is identical to any Rust code
/// path (a plain struct with two panicking field destructors aborts the
/// same way), so it is documented as the boundary, not worked around.
pub(crate) fn dispose_panic_payload(payload: Box<dyn std::any::Any + Send>) {
    let mut payload = payload;
    for _ in 0..4 {
        match std::panic::catch_unwind(std::panic::AssertUnwindSafe(move || drop(payload))) {
            Ok(()) => return,
            Err(next) => payload = next,
        }
    }
    std::mem::forget(payload);
}

/// Cancellation-aware bounded send: blocks (in bounded slices) while the
/// channel is full, but bails out cooperatively once the job is stopping — so
/// `stop()`/`abort()` (or a failure elsewhere) terminates even with a full,
/// undrained channel — and reports a dropped receiver as a typed
/// disconnection.
fn send_with_cancellation(
    tx: &crossbeam_channel::Sender<PacketSinkEvent>,
    cancellation: &CancellationSlot,
    event: PacketSinkEvent,
) -> PacketCallbackResult {
    // Fast path: `send_timeout` computes a wall-clock deadline up front on
    // every call — pure overhead while the channel has capacity (the common
    // case). Only a full channel proceeds to the deadline-based slices.
    let mut event = match tx.try_send(event) {
        Ok(()) => return Ok(()),
        Err(crossbeam_channel::TrySendError::Disconnected(_)) => {
            return Err(PacketCallbackError::disconnected());
        }
        Err(crossbeam_channel::TrySendError::Full(back)) => back,
    };
    loop {
        match tx.send_timeout(event, Duration::from_millis(50)) {
            Ok(()) => return Ok(()),
            Err(crossbeam_channel::SendTimeoutError::Timeout(back)) => {
                event = back;
                if let Some(observables) = cancellation.get() {
                    if is_stopping(observables.status.load(Ordering::Acquire)) {
                        // Classify WHY the job is stopping. A natural
                        // (all-muxers-done) STATUS_END cannot exist while
                        // this sink is still delivering — the sink is itself
                        // one of those muxers — so a stopping status here is
                        // either explicit stop()/abort() (no error recorded:
                        // stay silent as cancellation) or a failure-driven
                        // shutdown. Failures record their error BEFORE
                        // publishing the terminal status, so the recorded
                        // result is already visible on this path and the
                        // terminal can report the truncation as JobFailed.
                        let failed = observables
                            .result
                            .lock()
                            .unwrap_or_else(std::sync::PoisonError::into_inner)
                            .as_ref()
                            .is_some_and(|result| result.is_err());
                        return Err(if failed {
                            PacketCallbackError::job_stopped()
                        } else {
                            PacketCallbackError::cancelled()
                        });
                    }
                }
            }
            Err(crossbeam_channel::SendTimeoutError::Disconnected(_)) => {
                return Err(PacketCallbackError::disconnected());
            }
        }
    }
}

/// Builder for a strict-tier [`PacketSink`]; created by
/// [`PacketSink::builder`] with the required packet consumer.
pub struct PacketSinkBuilder {
    tier: PacketSinkTier,
    on_stream_info: Option<StreamInfoFn>,
    on_packet: PacketFn,
    on_end: Option<EndFn>,
    on_delivery_error: Option<DeliveryErrorFn>,
}

impl PacketSinkBuilder {
    /// One-time stream configuration callback, invoked before any packet.
    /// Return `Ok(())` to accept; an error fails the job before any packet is
    /// delivered. Teardown panic containment is per callback box — see
    /// "Failure and panic" in the [module docs](self).
    pub fn on_stream_info<F>(mut self, f: F) -> Self
    where
        F: FnMut(&[PacketStreamInfo]) -> PacketCallbackResult + Send + 'static,
    {
        self.on_stream_info = Some(Box::new(f));
        self
    }

    /// Terminal success callback; see the [module docs](self) for the exact
    /// gate. Never invoked after an error or lost packets. Cancellation
    /// suppresses it only when it interrupts delivery: a `stop()` that lands
    /// after this sink fully drained still delivers `on_end`. A panic here
    /// is contained per callback box and cannot change the settled job
    /// result — see "Failure and panic" in the [module docs](self) for the
    /// exact boundary.
    pub fn on_end<F>(mut self, f: F) -> Self
    where
        F: FnMut() + Send + 'static,
    {
        self.on_end = Some(Box::new(f));
        self
    }

    /// Terminal failure callback. For delivery-path errors (strict-tier
    /// violations, failing callbacks) the same error is also returned by
    /// `wait()`/`stop()`; when the JOB failed elsewhere (after this sink
    /// drained or truncating its delivery), the callback receives a
    /// synthesized [`PacketSinkError::JobFailed`] summarizing that failure
    /// while the job keeps its original error. Not invoked for cancellation
    /// or initial configuration failures — see "Failure and panic" in the
    /// [module docs](self).
    ///
    /// The borrowed error stays in the worker's custody for the whole call
    /// (a panic here cannot run the error source's destructor mid-unwind),
    /// and the panic is contained per callback box — see "Failure and
    /// panic" in the [module docs](self) for the exact boundary.
    pub fn on_delivery_error<F>(mut self, f: F) -> Self
    where
        F: FnMut(&PacketSinkError) + Send + 'static,
    {
        self.on_delivery_error = Some(Box::new(f));
        self
    }

    /// Finalizes the sink.
    pub fn build(self) -> PacketSink {
        PacketSink {
            tier: self.tier,
            dispatch: SinkDispatch::Closures {
                on_stream_info: self.on_stream_info,
                on_packet: self.on_packet,
                on_end: self.on_end,
                on_delivery_error: self.on_delivery_error,
            },
            cancellation: None,
        }
    }
}

/// Owned copy of one delivered packet, produced by [`PacketSink::channel`].
#[non_exhaustive]
#[derive(Debug, Clone)]
pub struct EncodedPacket {
    pub(crate) stream_index: usize,
    pub(crate) pts: i64,
    pub(crate) dts: i64,
    pub(crate) duration: i64,
    pub(crate) time_base: AVRational,
    pub(crate) is_key: bool,
    pub(crate) applied_offset: i64,
    pub(crate) data: Vec<u8>,
}

impl EncodedPacket {
    fn from_view(view: &PacketView<'_>) -> Self {
        Self {
            stream_index: view.stream_index,
            pts: view.pts,
            dts: view.dts,
            duration: view.duration,
            time_base: view.time_base,
            is_key: view.is_key,
            applied_offset: view.applied_offset,
            data: view.data.to_vec(),
        }
    }

    /// Output stream index.
    pub fn stream_index(&self) -> usize {
        self.stream_index
    }

    /// Presentation timestamp; see [`PacketView::pts`].
    pub fn pts(&self) -> i64 {
        self.pts
    }

    /// Decode timestamp; see [`PacketView::dts`].
    pub fn dts(&self) -> i64 {
        self.dts
    }

    /// Packet duration; see [`PacketView::duration`].
    pub fn duration(&self) -> i64 {
        self.duration
    }

    /// Stream time base.
    pub fn time_base(&self) -> AVRational {
        self.time_base
    }

    /// [`pts`](Self::pts) in microseconds.
    pub fn pts_us(&self) -> i64 {
        ticks_to_us(self.pts, self.time_base)
    }

    /// [`dts`](Self::dts) in microseconds.
    pub fn dts_us(&self) -> i64 {
        ticks_to_us(self.dts, self.time_base)
    }

    /// [`duration`](Self::duration) in microseconds.
    pub fn duration_us(&self) -> i64 {
        ticks_to_us(self.duration, self.time_base)
    }

    /// [`applied_offset`](Self::applied_offset) in microseconds.
    pub fn applied_offset_us(&self) -> i64 {
        ticks_to_us(self.applied_offset, self.time_base)
    }

    /// Fresh-decoder-safe random access point; see [`PacketView::is_key`].
    pub fn is_key(&self) -> bool {
        self.is_key
    }

    /// Applied origin shift; see [`PacketView::applied_offset`].
    pub fn applied_offset(&self) -> i64 {
        self.applied_offset
    }

    /// The owned packet payload.
    pub fn data(&self) -> &[u8] {
        &self.data
    }

    /// Consumes the packet, returning the payload.
    pub fn into_data(self) -> Vec<u8> {
        self.data
    }
}

/// One event delivered over a [`PacketSink::channel`] adapter, mirroring the
/// callback order: `StreamInfo`, then `Packet`s, then at most one terminal
/// `End`/`Error` (terminal events are best-effort under a stalled consumer;
/// sender disconnection is authoritative).
#[non_exhaustive]
#[derive(Debug, Clone)]
pub enum PacketSinkEvent {
    /// The one-time stream configuration, one entry per output stream.
    StreamInfo(Vec<PacketStreamInfo>),
    /// One delivered packet, copied into an owned payload.
    Packet(EncodedPacket),
    /// Terminal success (best-effort; see the enum docs).
    End,
    /// A delivery-path error, or [`PacketSinkError::JobFailed`] when the job
    /// failed elsewhere (`wait()` keeps the original error). Best-effort like
    /// [`End`](Self::End).
    Error(PacketSinkError),
}

/// Why [`PacketSinkReceiver::recv`] returned no event.
#[non_exhaustive]
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum PacketRecvError {
    /// The sending side is gone (job finished or failed; all events
    /// consumed).
    Disconnected,
}

impl std::fmt::Display for PacketRecvError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str("packet-sink channel disconnected")
    }
}

impl std::error::Error for PacketRecvError {}

/// Why [`PacketSinkReceiver::try_recv`] returned no event.
#[non_exhaustive]
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum PacketTryRecvError {
    /// No event is currently queued.
    Empty,
    /// The sending side is gone.
    Disconnected,
}

impl std::fmt::Display for PacketTryRecvError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            PacketTryRecvError::Empty => f.write_str("no packet-sink event queued"),
            PacketTryRecvError::Disconnected => f.write_str("packet-sink channel disconnected"),
        }
    }
}

impl std::error::Error for PacketTryRecvError {}

/// Why [`PacketSinkReceiver::recv_timeout`] returned no event.
#[non_exhaustive]
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum PacketRecvTimeoutError {
    /// No event arrived within the timeout.
    Timeout,
    /// The sending side is gone.
    Disconnected,
}

impl std::fmt::Display for PacketRecvTimeoutError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            PacketRecvTimeoutError::Timeout => {
                f.write_str("timed out waiting for a packet-sink event")
            }
            PacketRecvTimeoutError::Disconnected => {
                f.write_str("packet-sink channel disconnected")
            }
        }
    }
}

impl std::error::Error for PacketRecvTimeoutError {}

/// Error from [`PacketSinkReceiver::into_events`]: the scheduler passed in
/// is not the one running this receiver's sink.
///
/// Each [`PacketSink::channel`] call shares an identity token between the
/// sink and its receiver, and `into_events` requires the scheduler whose job
/// contains that sink. Accepting an arbitrary scheduler would silently
/// cross-wire two runs: iterate receiver A's events while joining — and, on
/// early drop, aborting — job B. Both handles are returned unchanged so the
/// caller can pair them correctly (the scheduler's job keeps running).
pub struct PacketEventsPairingError {
    /// The receiver, returned unchanged.
    pub receiver: PacketSinkReceiver,
    /// The scheduler, returned unchanged; its job is unaffected.
    pub scheduler: FfmpegScheduler<Running>,
}

impl std::fmt::Debug for PacketEventsPairingError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("PacketEventsPairingError")
            .finish_non_exhaustive()
    }
}

impl std::fmt::Display for PacketEventsPairingError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str(
            "packet-sink receiver paired with a scheduler that is not running its sink",
        )
    }
}

impl std::error::Error for PacketEventsPairingError {}

/// Receiving side of a [`PacketSink::channel`] adapter.
///
/// Drain it concurrently with the running job (its own thread, or
/// [`into_events`](Self::into_events)); dropping the receiver cancels the
/// job — the next delivery fails typed instead of blocking forever.
pub struct PacketSinkReceiver {
    inner: crossbeam_channel::Receiver<PacketSinkEvent>,
    /// Identity of the `PacketSink::channel` call that produced this
    /// receiver: the same `CancellationSlot` Arc the paired sink carries.
    /// [`into_events`](Self::into_events) matches it by pointer identity
    /// against the scheduler's job.
    token: CancellationSlot,
}

impl PacketSinkReceiver {
    /// Blocks until the next event; [`PacketRecvError::Disconnected`] once
    /// the sending side is gone and all events were consumed.
    pub fn recv(&self) -> Result<PacketSinkEvent, PacketRecvError> {
        self.inner.recv().map_err(|_| PacketRecvError::Disconnected)
    }

    /// Non-blocking receive, distinguishing an empty channel from a
    /// disconnected one.
    pub fn try_recv(&self) -> Result<PacketSinkEvent, PacketTryRecvError> {
        self.inner.try_recv().map_err(|e| match e {
            crossbeam_channel::TryRecvError::Empty => PacketTryRecvError::Empty,
            crossbeam_channel::TryRecvError::Disconnected => PacketTryRecvError::Disconnected,
        })
    }

    /// Receive with a timeout, distinguishing a timeout from disconnection.
    pub fn recv_timeout(
        &self,
        timeout: Duration,
    ) -> Result<PacketSinkEvent, PacketRecvTimeoutError> {
        self.inner.recv_timeout(timeout).map_err(|e| match e {
            crossbeam_channel::RecvTimeoutError::Timeout => PacketRecvTimeoutError::Timeout,
            crossbeam_channel::RecvTimeoutError::Disconnected => {
                PacketRecvTimeoutError::Disconnected
            }
        })
    }

    /// Blocking iterator over events until the sender disconnects.
    pub fn iter(&self) -> impl Iterator<Item = PacketSinkEvent> + '_ {
        self.inner.iter()
    }

    /// Consumes the receiver and the running scheduler into a single
    /// owned-run iterator (the frame-export `FrameIter` shape): events stream
    /// out as they arrive, the scheduler is joined exactly once when the
    /// channel drains, and a job error surfaces as one terminal `Err`.
    /// Dropping the iterator mid-run releases the receiver FIRST (unblocking
    /// a worker parked in the channel send), then aborts the job.
    ///
    /// # Errors
    ///
    /// [`PacketEventsPairingError`] when `scheduler` is not the one running
    /// this receiver's sink. The pairing is checked by identity — the token
    /// shared by the sink/receiver pair from [`PacketSink::channel`] must
    /// belong to the scheduler's job — because a cross-wired iterator would
    /// silently stream one run's events while reporting (and, on drop,
    /// aborting) another run's outcome. The error returns both handles
    /// unchanged so they can be re-paired.
    // The Err variant carries the scheduler back to the caller, so it is as
    // large as the Ok variant (which owns the same scheduler inside the
    // iterator); boxing it would not shrink the Result.
    #[allow(clippy::result_large_err)]
    pub fn into_events(
        self,
        scheduler: FfmpegScheduler<Running>,
    ) -> Result<PacketEventIter, PacketEventsPairingError> {
        if !scheduler.runs_packet_sink(&self.token) {
            return Err(PacketEventsPairingError {
                receiver: self,
                scheduler,
            });
        }
        Ok(PacketEventIter {
            inner: OwnedRunIter::new(self.inner, scheduler, std::convert::identity),
        })
    }
}

/// An owned-run iterator over [`PacketSinkEvent`]s; see
/// [`PacketSinkReceiver::into_events`].
pub struct PacketEventIter {
    inner: OwnedRunIter<PacketSinkEvent>,
}

impl std::fmt::Debug for PacketEventIter {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("PacketEventIter").finish_non_exhaustive()
    }
}

impl Iterator for PacketEventIter {
    type Item = Result<PacketSinkEvent, crate::error::Error>;

    fn next(&mut self) -> Option<Self::Item> {
        self.inner.next()
    }
}

impl std::iter::FusedIterator for PacketEventIter {}

/// Explicit muxing policy a packet-sink output pins for encoder setup.
///
/// A packet sink still allocates a real (never-written) output context so the
/// existing stream/parameter plumbing works unchanged, but nothing may be
/// implicitly decided by which container that happens to be: the flags the
/// encoders and the vsync selection observe are synthesized from this policy
/// and overwrite the container's snapshot.
pub(crate) struct PacketSinkPolicy {
    /// Set `AV_CODEC_FLAG_GLOBAL_HEADER` on the encoders, so codec
    /// configuration (SPS/PPS, AudioSpecificConfig) materializes as
    /// out-of-band extradata at encoder open — the strict tier requires it
    /// before the first callback.
    pub(crate) global_header: bool,
    /// Advertise variable-fps semantics to the vsync selection (`false` in
    /// the strict tier: CFR-style timestamps, like mp4).
    pub(crate) variable_fps: bool,
    /// Advertise a timestamp-free sink to the vsync selection (`false`:
    /// timestamps are the product).
    pub(crate) no_timestamps: bool,
}

impl PacketSinkPolicy {
    pub(crate) fn for_tier(tier: PacketSinkTier) -> Self {
        match tier {
            PacketSinkTier::Strict => Self {
                global_header: true,
                variable_fps: false,
                no_timestamps: false,
            },
        }
    }

    /// The `AVOutputFormat.flags` projection of this policy, stored as the
    /// muxer's `oformat_flags` snapshot (what `enc_init` and the vsync
    /// selection read).
    pub(crate) fn oformat_flags(&self) -> i32 {
        let mut flags = 0;
        if self.global_header {
            flags |= ffmpeg_sys_next::AVFMT_GLOBALHEADER;
        }
        if self.variable_fps {
            flags |= ffmpeg_sys_next::AVFMT_VARIABLE_FPS;
        }
        if self.no_timestamps {
            flags |= ffmpeg_sys_next::AVFMT_NOTIMESTAMPS;
        }
        flags
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn test_view(data: &[u8]) -> PacketView<'_> {
        PacketView {
            stream_index: 0,
            pts: 10,
            dts: 5,
            duration: 1,
            time_base: AVRational { num: 1, den: 25 },
            is_key: false,
            applied_offset: 3,
            data,
        }
    }

    #[test]
    fn builder_requires_a_packet_consumer_and_discard_is_explicit() {
        let mut sink = PacketSink::builder(|_| Ok(())).build();
        assert_eq!(sink.tier, PacketSinkTier::Strict);
        assert!(sink.dispatch_stream_info(&[]).is_ok());
        let payload = [0u8, 0, 0, 1, 0x65];
        assert!(sink.dispatch_packet(&test_view(&payload)).is_ok());
        sink.dispatch_end();
        sink.dispatch_delivery_error(&PacketSinkError::NoStreams);

        let mut discard = PacketSink::discard();
        assert!(discard.dispatch_packet(&test_view(&payload)).is_ok());
    }

    /// The public tier accessor is the only way consumers can observe a
    /// sink's tier; every v1 construction path must report `Strict`, and the
    /// enum default must agree so builders can rely on it.
    #[test]
    fn every_construction_path_reports_the_strict_tier() {
        assert_eq!(PacketSinkTier::default(), PacketSinkTier::Strict);
        assert_eq!(
            PacketSink::builder(|_| Ok(())).build().tier(),
            PacketSinkTier::Strict
        );
        assert_eq!(PacketSink::discard().tier(), PacketSinkTier::Strict);

        struct Accepting;
        impl PacketSinkHandler for Accepting {
            fn on_packet(&mut self, _packet: &PacketView<'_>) -> PacketCallbackResult {
                Ok(())
            }
        }
        assert_eq!(
            PacketSink::from_handler(Accepting).tier(),
            PacketSinkTier::Strict
        );

        let (sink, _receiver) = PacketSink::channel(NonZeroUsize::new(1).unwrap());
        assert_eq!(sink.tier(), PacketSinkTier::Strict);
    }

    #[test]
    fn handler_receives_serial_callbacks_with_shared_state() {
        struct Counting {
            packets: usize,
        }
        impl PacketSinkHandler for Counting {
            fn on_packet(&mut self, _packet: &PacketView<'_>) -> PacketCallbackResult {
                self.packets += 1;
                if self.packets > 1 {
                    Err(PacketCallbackError::new("enough"))
                } else {
                    Ok(())
                }
            }
        }
        let mut sink = PacketSink::from_handler(Counting { packets: 0 });
        let payload = [0u8, 0, 0, 1, 0x65];
        assert!(sink.dispatch_packet(&test_view(&payload)).is_ok());
        let err = sink
            .dispatch_packet(&test_view(&payload))
            .expect_err("handler state must persist across calls");
        assert_eq!(err.kind, CallbackFailureKind::Failure);
        assert_eq!(err.to_string(), "enough");
    }

    /// A `panic_any` payload whose own `Drop` panics (with another such
    /// payload) must be consumed without the disposal itself throwing —
    /// both for chains within the attempt bound and for chains beyond it
    /// (the remainder is leaked by design, never re-thrown).
    #[test]
    fn panic_payload_chains_are_disposed_without_escaping() {
        struct ChainBomb(u32);
        impl Drop for ChainBomb {
            fn drop(&mut self) {
                if self.0 > 0 {
                    std::panic::panic_any(ChainBomb(self.0 - 1));
                }
            }
        }
        // Depth 3: attempts 1-3 each panic with the next link, attempt 4
        // drops the final link cleanly.
        dispose_panic_payload(Box::new(ChainBomb(3)));
        // Deeper than the attempt bound: the helper must still return.
        dispose_panic_payload(Box::new(ChainBomb(64)));
    }

    /// Every callback box must be destroyed even when SEVERAL capture
    /// destructors panic. One catch around a plain drop of the aggregate
    /// contains only the first bomb — the remaining boxes are then
    /// destroyed by the unwind itself, where the second bomb aborts the
    /// process. Reaching the assertions at all is the point.
    #[test]
    fn dispose_contained_destroys_every_box_across_multiple_drop_panics() {
        use std::sync::atomic::AtomicBool;

        struct DropBomb(Arc<AtomicBool>);
        impl Drop for DropBomb {
            fn drop(&mut self) {
                self.0.store(true, Ordering::Release);
                panic!("injected capture-destructor panic");
            }
        }

        let flags: Vec<Arc<AtomicBool>> = (0..3).map(|_| Arc::new(AtomicBool::new(false))).collect();
        let (b0, b1, b2) = (
            DropBomb(flags[0].clone()),
            DropBomb(flags[1].clone()),
            DropBomb(flags[2].clone()),
        );
        let sink = PacketSink::builder(move |_pkt| {
            let _hold = &b0;
            Ok(())
        })
        .on_end(move || {
            let _hold = &b1;
        })
        .on_delivery_error(move |_e| {
            let _hold = &b2;
        })
        .build();
        assert!(
            sink.dispose_contained(),
            "three panicking capture destructors must be reported"
        );
        for (i, flag) in flags.iter().enumerate() {
            assert!(
                flag.load(Ordering::Acquire),
                "callback box {i} was never destroyed"
            );
        }

        struct BombHandler(Arc<AtomicBool>);
        impl Drop for BombHandler {
            fn drop(&mut self) {
                self.0.store(true, Ordering::Release);
                panic!("injected handler-destructor panic");
            }
        }
        impl PacketSinkHandler for BombHandler {
            fn on_packet(&mut self, _packet: &PacketView<'_>) -> PacketCallbackResult {
                Ok(())
            }
        }
        let destroyed = Arc::new(AtomicBool::new(false));
        assert!(PacketSink::from_handler(BombHandler(destroyed.clone())).dispose_contained());
        assert!(destroyed.load(Ordering::Acquire));

        // Benign sinks report no panic.
        assert!(!PacketSink::builder(|_pkt| Ok(())).build().dispose_contained());
    }

    #[test]
    fn callback_error_preserves_its_source() {
        let io = std::io::Error::new(std::io::ErrorKind::BrokenPipe, "peer gone");
        let err = PacketCallbackError::with_source("send failed", io);
        assert_eq!(err.to_string(), "send failed");
        let source = std::error::Error::source(&err).expect("source preserved");
        assert!(source.to_string().contains("peer gone"));
    }

    #[test]
    fn channel_adapter_forwards_events_in_order() {
        let (mut sink, rx) = PacketSink::channel(NonZeroUsize::new(8).unwrap());
        assert!(sink.dispatch_stream_info(&[]).is_ok());
        let payload = [0u8, 0, 0, 1, 0x65];
        assert!(sink.dispatch_packet(&test_view(&payload)).is_ok());
        sink.dispatch_end();
        match rx.recv().unwrap() {
            PacketSinkEvent::StreamInfo(v) => assert!(v.is_empty()),
            other => panic!("expected StreamInfo, got {other:?}"),
        }
        match rx.recv().unwrap() {
            PacketSinkEvent::Packet(p) => {
                assert_eq!(p.pts(), 10);
                assert_eq!(p.dts(), 5);
                assert_eq!(p.applied_offset(), 3);
                assert_eq!(p.data(), &payload);
                assert!(!p.is_key());
                // Tick conveniences: 10 ticks at 1/25 s = 400_000 us.
                assert_eq!(p.pts_us(), 400_000);
                assert_eq!(p.duration_us(), 40_000);
            }
            other => panic!("expected Packet, got {other:?}"),
        }
        assert!(matches!(rx.recv().unwrap(), PacketSinkEvent::End));
        drop(sink);
        assert!(matches!(rx.recv(), Err(PacketRecvError::Disconnected)));
    }

    #[test]
    fn dropped_receiver_turns_sends_into_typed_disconnection() {
        let (mut sink, rx) = PacketSink::channel(NonZeroUsize::new(1).unwrap());
        drop(rx);
        let payload = [0u8, 0, 0, 1, 0x65];
        let err = sink
            .dispatch_packet(&test_view(&payload))
            .expect_err("send into a dropped receiver must fail");
        assert_eq!(err.kind, CallbackFailureKind::Disconnected);
    }

    /// The review probe: a blocked bounded send with a live, undrained
    /// receiver must observe the job stopping and bail out promptly —
    /// classified as clean cancellation when NO job error is recorded.
    #[test]
    fn blocked_channel_send_observes_cancellation() {
        let (mut sink, rx) = PacketSink::channel(NonZeroUsize::new(1).unwrap());
        // Simulate the worker wiring: publish the job observables.
        let status = Arc::new(AtomicUsize::new(
            crate::core::scheduler::ffmpeg_scheduler::STATUS_RUN,
        ));
        let result = Arc::new(std::sync::Mutex::new(None));
        sink.cancellation
            .as_ref()
            .expect("channel sinks carry a cancellation slot")
            .set(JobStopObservables {
                status: status.clone(),
                result,
            })
            .ok();
        // Fill the capacity-1 channel; the receiver never drains.
        let payload = [0u8, 0, 0, 1, 0x65];
        assert!(sink.dispatch_packet(&test_view(&payload)).is_ok());
        // Flip to stopping from another thread; the blocked send must
        // observe it and bail out with the cancellation kind.
        let flip = status.clone();
        let flipper = std::thread::spawn(move || {
            std::thread::sleep(Duration::from_millis(120));
            flip.store(
                crate::core::scheduler::ffmpeg_scheduler::STATUS_END,
                Ordering::Release,
            );
        });
        let start = std::time::Instant::now();
        let err = sink
            .dispatch_packet(&test_view(&payload))
            .expect_err("blocked send must cancel");
        assert_eq!(err.kind, CallbackFailureKind::Cancelled);
        assert!(
            start.elapsed() < Duration::from_secs(5),
            "cancellation must be prompt"
        );
        flipper.join().unwrap();
        drop(rx);
    }

    /// A stopping status WITH a recorded job error is a failure-driven
    /// shutdown, not cancellation: the blocked send must classify it as
    /// `JobStopped` so the terminal reports `JobFailed` instead of staying
    /// silent.
    #[test]
    fn blocked_channel_send_classifies_failure_driven_stop() {
        let (mut sink, rx) = PacketSink::channel(NonZeroUsize::new(1).unwrap());
        let status = Arc::new(AtomicUsize::new(
            crate::core::scheduler::ffmpeg_scheduler::STATUS_RUN,
        ));
        let result: Arc<std::sync::Mutex<Option<crate::error::Result<()>>>> =
            Arc::new(std::sync::Mutex::new(None));
        sink.cancellation
            .as_ref()
            .expect("channel sinks carry a cancellation slot")
            .set(JobStopObservables {
                status: status.clone(),
                result: result.clone(),
            })
            .ok();
        let payload = [0u8, 0, 0, 1, 0x65];
        assert!(sink.dispatch_packet(&test_view(&payload)).is_ok());
        // Record the error BEFORE publishing the stopping status — the
        // order every failure path guarantees.
        let flipper = std::thread::spawn(move || {
            std::thread::sleep(Duration::from_millis(120));
            *result.lock().unwrap() = Some(Err(crate::error::Error::WorkerPanicked(
                "muxer1:mpegts".to_string(),
            )));
            status.store(
                crate::core::scheduler::ffmpeg_scheduler::STATUS_END,
                Ordering::Release,
            );
        });
        let err = sink
            .dispatch_packet(&test_view(&payload))
            .expect_err("blocked send must abandon on a failed job");
        assert_eq!(err.kind, CallbackFailureKind::JobStopped);
        flipper.join().unwrap();
        drop(rx);
    }

    #[test]
    fn recv_variants_distinguish_empty_timeout_disconnected() {
        let (sink, rx) = PacketSink::channel(NonZeroUsize::new(1).unwrap());
        assert_eq!(rx.try_recv().unwrap_err(), PacketTryRecvError::Empty);
        assert_eq!(
            rx.recv_timeout(Duration::from_millis(10)).unwrap_err(),
            PacketRecvTimeoutError::Timeout
        );
        drop(sink);
        assert_eq!(rx.try_recv().unwrap_err(), PacketTryRecvError::Disconnected);
        assert_eq!(
            rx.recv_timeout(Duration::from_millis(10)).unwrap_err(),
            PacketRecvTimeoutError::Disconnected
        );
    }
}
