//! Encoded-packet output: consume encoder packets directly, without a muxer.
//!
//! A packet sink is the fourth quadrant of the crate's I/O matrix (decoded
//! frames out = frame export, PCM out = sample export, frames in =
//! [`VideoWriter`](crate::VideoWriter), **encoded packets out = packet sink**).
//! Instead of muxing packets into container bytes, the job hands each encoded
//! packet to user callbacks, normalized for WebCodecs-style consumers.
//!
//! # Strict tier (v1)
//!
//! The only tier implemented today is [`PacketSinkTier::Strict`], aligned with
//! WebCodecs `"avc"` / AAC consumption:
//!
//! * **H.264 video** is delivered as avcC-configured, 4-byte length-prefixed,
//!   access-unit-complete packets. The encoder whitelist is `libx264` only —
//!   the delivery contract assumes one packet == one access unit, which is
//!   established for libx264 and not verified for other encoders. Any other
//!   video encoder fails the build with a typed error.
//! * **AAC audio** is delivered as raw AAC frames; the stream configuration
//!   carries the AudioSpecificConfig.
//! * Anything else (subtitles, data streams, stream copy, bitstream filters)
//!   is rejected up front with a typed [`PacketSinkError`](crate::error::PacketSinkError).
//!
//! Future tiers (generic passthrough, HEVC, Annex-B) extend
//! [`PacketSinkTier`]; the enum and the delivered structs are
//! `#[non_exhaustive]` so they can grow without breaking changes.
//!
//! # Callback order
//!
//! All callbacks run **serially on the muxing worker thread**, in this exact
//! order:
//!
//! 1. `on_stream_info` — exactly once, after every encoder finalized its
//!    parameters and **before any packet**. The video configuration is already
//!    a valid avcC record here.
//! 2. `on_packet` — zero or more times, per-stream dts-monotonic.
//! 3. `on_end` **or** `on_error` — at most one of them, exactly once:
//!    * `on_end` fires only when every output stream reached a recognized
//!      terminal state (natural encoder EOF, or configured truncation such as
//!      `set_recording_time_us` / `set_shortest`), everything was delivered,
//!      and no error occurred.
//!    * `on_error` fires when delivery stopped because of a strict-tier
//!      violation or a failing callback.
//!    * Neither fires when the job is cancelled (`stop()` with packets still
//!      in flight, `abort()`) or when an unrelated pipeline task fails — the
//!      job result returned by `wait()`/`stop()` carries the outcome.
//!
//! # Backpressure: callbacks block the pipeline
//!
//! **The callbacks run on the muxing worker thread. A slow `on_packet` blocks
//! that thread, the bounded packet queue behind it fills, and the encoders
//! stall — exactly the backpressure a slow container write exerts today.**
//! No packet is ever silently dropped. If you need decoupling, copy the
//! borrowed data out (it is only valid during the callback) and queue it
//! yourself, or use [`PacketSink::channel`], which does that copy for you and
//! blocks the pipeline only when its bounded channel is full.
//!
//! # Example
//!
//! ```rust,ignore
//! use ez_ffmpeg::packet_sink::PacketSink;
//! use ez_ffmpeg::{FfmpegContext, Output};
//!
//! let sink = PacketSink::builder()
//!     .on_stream_info(|infos| {
//!         println!("streams: {}", infos.len());
//!         0
//!     })
//!     .on_packet(|pkt| {
//!         println!("stream {} pts {} ({} bytes)", pkt.stream_index(), pkt.pts(), pkt.data().len());
//!         0
//!     })
//!     .on_end(|| println!("done"))
//!     .build();
//!
//! FfmpegContext::builder()
//!     .input("input.mp4")
//!     .output(Output::new_by_packet_sink(sink).set_video_codec("libx264"))
//!     .build()?
//!     .start()?
//!     .wait()?;
//! ```

use crate::error::PacketSinkError;
use ffmpeg_sys_next::{AVMediaType, AVRational};

pub(crate) mod codec;
pub(crate) mod nal_framing;
pub(crate) mod side_data;
pub(crate) mod strict;
pub(crate) mod timeline;

/// Delivery tier of a packet sink. Only [`Strict`](PacketSinkTier::Strict)
/// exists in v1; the enum is `#[non_exhaustive]` so later tiers (generic
/// passthrough, HEVC, Annex-B) are additive.
#[non_exhaustive]
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub enum PacketSinkTier {
    /// WebCodecs-aligned strict tier: avcC H.264 (libx264) + AAC.
    #[default]
    Strict,
}

/// Per-stream configuration delivered once via `on_stream_info`, before any
/// packet.
#[non_exhaustive]
#[derive(Debug, Clone)]
pub struct PacketStreamInfo {
    pub(crate) stream_index: usize,
    pub(crate) media_type: AVMediaType,
    pub(crate) codec_name: String,
    pub(crate) time_base: AVRational,
    pub(crate) extradata: Vec<u8>,
    pub(crate) width: i32,
    pub(crate) height: i32,
    pub(crate) frame_rate: Option<AVRational>,
    pub(crate) sample_rate: i32,
    pub(crate) channels: i32,
}

impl PacketStreamInfo {
    /// Output stream index; matches [`PacketView::stream_index`].
    pub fn stream_index(&self) -> usize {
        self.stream_index
    }

    /// Media type of the stream (video or audio in the strict tier).
    pub fn media_type(&self) -> AVMediaType {
        self.media_type
    }

    /// FFmpeg codec name (`"h264"`, `"aac"`).
    pub fn codec_name(&self) -> &str {
        &self.codec_name
    }

    /// Time base every timestamp of this stream is expressed in. This is the
    /// encoder time base, passed through verbatim — no muxer rescaling.
    pub fn time_base(&self) -> AVRational {
        self.time_base
    }

    /// Codec configuration record: a valid `AVCDecoderConfigurationRecord`
    /// (avcC) for H.264, the `AudioSpecificConfig` for AAC. Suitable as the
    /// WebCodecs `description`.
    pub fn extradata(&self) -> &[u8] {
        &self.extradata
    }

    /// Video width in pixels (0 for audio).
    pub fn width(&self) -> i32 {
        self.width
    }

    /// Video height in pixels (0 for audio).
    pub fn height(&self) -> i32 {
        self.height
    }

    /// Video frame rate, when known (`None` for audio and for VFR sources
    /// whose rate could not be established).
    pub fn frame_rate(&self) -> Option<AVRational> {
        self.frame_rate
    }

    /// Audio sample rate in Hz (0 for video).
    pub fn sample_rate(&self) -> i32 {
        self.sample_rate
    }

    /// Audio channel count (0 for video).
    pub fn channels(&self) -> i32 {
        self.channels
    }
}

/// Borrowed view of one delivered packet.
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
    /// shared zero-based timeline (see [`applied_offset`](Self::applied_offset)).
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
    /// length-prefixed (AVCC), parameter sets carried out-of-band in
    /// [`PacketStreamInfo::extradata`]. AAC: one raw AAC frame.
    pub fn data(&self) -> &'a [u8] {
        self.data
    }
}

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

pub(crate) type StreamInfoFn = Box<dyn FnMut(&[PacketStreamInfo]) -> i32 + Send>;
pub(crate) type PacketFn = Box<dyn for<'a> FnMut(&PacketView<'a>) -> i32 + Send>;
pub(crate) type EndFn = Box<dyn FnMut() + Send>;
pub(crate) type ErrorFn = Box<dyn FnMut(&PacketSinkError) + Send>;

/// The callback bundle handed to [`Output::new_by_packet_sink`](crate::Output::new_by_packet_sink).
///
/// Build one with [`PacketSink::builder`], or use [`PacketSink::channel`] for
/// a ready-made owned/channel consumer. See the [module docs](self) for the
/// callback order and the **blocking backpressure** contract.
pub struct PacketSink {
    pub(crate) tier: PacketSinkTier,
    pub(crate) on_stream_info: StreamInfoFn,
    pub(crate) on_packet: PacketFn,
    pub(crate) on_end: EndFn,
    pub(crate) on_error: ErrorFn,
}

impl std::fmt::Debug for PacketSink {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("PacketSink")
            .field("tier", &self.tier)
            .finish_non_exhaustive()
    }
}

impl PacketSink {
    /// Starts building a packet sink. Unset callbacks default to accepting
    /// no-ops.
    pub fn builder() -> PacketSinkBuilder {
        PacketSinkBuilder::default()
    }

    /// Builds a strict-tier sink that forwards everything over a **bounded**
    /// channel of owned events, for consumers that want packets on their own
    /// thread.
    ///
    /// Every payload is copied once into an owned [`SinkPacket`]. The channel
    /// preserves the callback contract: when it is full, the sending callback
    /// **blocks the pipeline** until the consumer catches up (no packet is
    /// dropped). Dropping the [`PacketSinkReceiver`] cancels the job with a
    /// typed error on the next delivery.
    ///
    /// `capacity` is clamped to at least 1.
    pub fn channel(capacity: usize) -> (PacketSink, PacketSinkReceiver) {
        let (tx, rx) = crossbeam_channel::bounded::<PacketSinkEvent>(capacity.max(1));
        let info_tx = tx.clone();
        let pkt_tx = tx.clone();
        let end_tx = tx.clone();
        let err_tx = tx;
        let sink = PacketSink::builder()
            .on_stream_info(move |infos: &[PacketStreamInfo]| {
                match info_tx.send(PacketSinkEvent::StreamInfo(infos.to_vec())) {
                    Ok(()) => 0,
                    Err(_) => ffmpeg_sys_next::AVERROR(ffmpeg_sys_next::EIO),
                }
            })
            .on_packet(move |view: &PacketView<'_>| {
                match pkt_tx.send(PacketSinkEvent::Packet(SinkPacket::from_view(view))) {
                    Ok(()) => 0,
                    Err(_) => ffmpeg_sys_next::AVERROR(ffmpeg_sys_next::EIO),
                }
            })
            .on_end(move || {
                let _ = end_tx.send(PacketSinkEvent::End);
            })
            .on_error(move |e: &PacketSinkError| {
                let _ = err_tx.send(PacketSinkEvent::Error(e.clone()));
            })
            .build();
        (sink, PacketSinkReceiver { inner: rx })
    }
}

/// Builder for [`PacketSink`]. Every callback is optional; unset ones default
/// to accepting no-ops.
#[derive(Default)]
pub struct PacketSinkBuilder {
    tier: PacketSinkTier,
    on_stream_info: Option<StreamInfoFn>,
    on_packet: Option<PacketFn>,
    on_end: Option<EndFn>,
    on_error: Option<ErrorFn>,
}

impl PacketSinkBuilder {
    /// Selects the delivery tier. Defaults to [`PacketSinkTier::Strict`],
    /// the only tier in v1.
    pub fn tier(mut self, tier: PacketSinkTier) -> Self {
        self.tier = tier;
        self
    }

    /// One-time stream configuration callback, invoked before any packet.
    /// Return `0` to accept; a negative value fails the job before any packet
    /// is delivered.
    pub fn on_stream_info<F>(mut self, f: F) -> Self
    where
        F: FnMut(&[PacketStreamInfo]) -> i32 + Send + 'static,
    {
        self.on_stream_info = Some(Box::new(f));
        self
    }

    /// Per-packet callback. The [`PacketView`] (including its payload) is
    /// valid only for the duration of the call. Return `0` to continue; a
    /// negative value stops the job with
    /// [`PacketSinkError::PacketCallbackFailed`](crate::error::PacketSinkError::PacketCallbackFailed)
    /// (any negative value — including `AVERROR_EOF` — is a failure, never a
    /// clean end). **This callback blocks the encoding pipeline while it
    /// runs.**
    pub fn on_packet<F>(mut self, f: F) -> Self
    where
        F: for<'a> FnMut(&PacketView<'a>) -> i32 + Send + 'static,
    {
        self.on_packet = Some(Box::new(f));
        self
    }

    /// Terminal success callback; see the [module docs](self) for the exact
    /// gate. Never invoked after an error, a cancelled job, or lost packets.
    pub fn on_end<F>(mut self, f: F) -> Self
    where
        F: FnMut() + Send + 'static,
    {
        self.on_end = Some(Box::new(f));
        self
    }

    /// Terminal failure callback for delivery-path errors (strict-tier
    /// violations, failing callbacks). The same error is also returned by
    /// `wait()`/`stop()`. Not invoked for cancellation or failures elsewhere
    /// in the pipeline.
    pub fn on_error<F>(mut self, f: F) -> Self
    where
        F: FnMut(&PacketSinkError) + Send + 'static,
    {
        self.on_error = Some(Box::new(f));
        self
    }

    /// Finalizes the sink.
    pub fn build(self) -> PacketSink {
        PacketSink {
            tier: self.tier,
            on_stream_info: self.on_stream_info.unwrap_or_else(|| Box::new(|_| 0)),
            on_packet: self.on_packet.unwrap_or_else(|| Box::new(|_| 0)),
            on_end: self.on_end.unwrap_or_else(|| Box::new(|| {})),
            on_error: self.on_error.unwrap_or_else(|| Box::new(|_| {})),
        }
    }
}

/// Owned copy of one delivered packet, produced by [`PacketSink::channel`].
#[non_exhaustive]
#[derive(Debug, Clone)]
pub struct SinkPacket {
    pub(crate) stream_index: usize,
    pub(crate) pts: i64,
    pub(crate) dts: i64,
    pub(crate) duration: i64,
    pub(crate) time_base: AVRational,
    pub(crate) is_key: bool,
    pub(crate) applied_offset: i64,
    pub(crate) data: Vec<u8>,
}

impl SinkPacket {
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
/// `End`/`Error`.
#[non_exhaustive]
#[derive(Debug, Clone)]
pub enum PacketSinkEvent {
    /// The one-time stream configuration.
    StreamInfo(Vec<PacketStreamInfo>),
    /// One delivered packet.
    Packet(SinkPacket),
    /// Terminal success.
    End,
    /// Terminal delivery-path failure.
    Error(PacketSinkError),
}

/// Receiving side of a [`PacketSink::channel`] adapter.
///
/// Dropping the receiver cancels the job: the next delivery fails with a
/// typed error instead of blocking forever.
pub struct PacketSinkReceiver {
    inner: crossbeam_channel::Receiver<PacketSinkEvent>,
}

impl PacketSinkReceiver {
    /// Blocks until the next event, or `None` once the sending side is gone
    /// (the job finished or failed and the terminal event was consumed).
    pub fn recv(&self) -> Option<PacketSinkEvent> {
        self.inner.recv().ok()
    }

    /// Non-blocking receive.
    pub fn try_recv(&self) -> Option<PacketSinkEvent> {
        self.inner.try_recv().ok()
    }

    /// Receive with a timeout; `None` on timeout or disconnect.
    pub fn recv_timeout(&self, timeout: std::time::Duration) -> Option<PacketSinkEvent> {
        self.inner.recv_timeout(timeout).ok()
    }

    /// Blocking iterator over events until the sender disconnects.
    pub fn iter(&self) -> impl Iterator<Item = PacketSinkEvent> + '_ {
        self.inner.iter()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn builder_defaults_are_accepting_noops() {
        let mut sink = PacketSink::builder().build();
        assert_eq!(sink.tier, PacketSinkTier::Strict);
        assert_eq!((sink.on_stream_info)(&[]), 0);
        let view = PacketView {
            stream_index: 0,
            pts: 0,
            dts: 0,
            duration: 1,
            time_base: AVRational { num: 1, den: 25 },
            is_key: true,
            applied_offset: 0,
            data: &[0, 0, 0, 1, 0x65],
        };
        assert_eq!((sink.on_packet)(&view), 0);
        (sink.on_end)();
        (sink.on_error)(&crate::error::PacketSinkError::NoStreams);
    }

    #[test]
    fn channel_adapter_forwards_events_in_order() {
        let (mut sink, rx) = PacketSink::channel(8);
        let infos = vec![];
        assert_eq!((sink.on_stream_info)(&infos), 0);
        let payload = [0u8, 0, 0, 1, 0x65];
        let view = PacketView {
            stream_index: 0,
            pts: 10,
            dts: 5,
            duration: 1,
            time_base: AVRational { num: 1, den: 25 },
            is_key: false,
            applied_offset: 3,
            data: &payload,
        };
        assert_eq!((sink.on_packet)(&view), 0);
        (sink.on_end)();
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
            }
            other => panic!("expected Packet, got {other:?}"),
        }
        assert!(matches!(rx.recv().unwrap(), PacketSinkEvent::End));
    }

    #[test]
    fn dropped_receiver_turns_sends_into_errors() {
        let (mut sink, rx) = PacketSink::channel(1);
        drop(rx);
        let view = PacketView {
            stream_index: 0,
            pts: 0,
            dts: 0,
            duration: 1,
            time_base: AVRational { num: 1, den: 25 },
            is_key: true,
            applied_offset: 0,
            data: &[0, 0, 0, 1, 0x65],
        };
        assert!((sink.on_packet)(&view) < 0);
    }
}
