//! Route synchronized audio+video packets into a transport packager stub.
//!
//! One [`PacketSinkHandler`] receives BOTH streams of an A/V encode and
//! routes each packet to its per-track state — the shape of an fMP4
//! segmenter or an RTP/SRT sender session. It demonstrates the contracts a
//! transport consumer relies on:
//!
//! * `on_stream_info` describes every track once (avcC + `avc1.PPCCLL` for
//!   video, ASC + `mp4a.40.X` for audio) before any packet;
//! * packets carry a per-stream `stream_index` for routing — **no cross-
//!   stream interleaving order is promised**, so the packager tracks each
//!   stream independently;
//! * all streams share one time origin: the anchor stream starts at dts 0
//!   and `applied_offset` recovers each stream's original encoder timeline
//!   (`original = delivered + applied_offset`), preserving true A/V offsets;
//! * per stream, dts is strictly increasing and every duration is positive —
//!   validated by the strict tier before delivery.
//!
//! Requires an FFmpeg build with libx264 (prints a notice and exits
//! otherwise).
//!
//! Run it:
//!
//! ```sh
//! cargo run --example packet_sink_av_transport
//! ```

use ez_ffmpeg::packet_sink::{
    PacketCallbackResult, PacketSink, PacketSinkHandler, PacketStreamInfo, PacketView,
};
use ez_ffmpeg::{FfmpegContext, Input, Output};
use std::collections::BTreeMap;

/// Per-track accounting a real packager would replace with segment/RTP state.
#[derive(Default)]
struct Track {
    label: String,
    packets: usize,
    bytes: usize,
    sync_samples: usize,
    first_delivered_dts_us: Option<i64>,
    first_source_dts_us: Option<i64>,
    last_end_us: i64,
}

/// The A/V packager stub: routes by stream index, keeps per-track timing.
#[derive(Default)]
struct StubPackager {
    tracks: BTreeMap<usize, Track>,
}

impl PacketSinkHandler for StubPackager {
    fn on_stream_info(&mut self, streams: &[PacketStreamInfo]) -> PacketCallbackResult {
        for info in streams {
            let label = match info {
                PacketStreamInfo::Video(v) => format!(
                    "video {} {}x{} (config {} bytes)",
                    v.codec_string(),
                    v.width(),
                    v.height(),
                    v.codec_config().len()
                ),
                PacketStreamInfo::Audio(a) => format!(
                    "audio {} {} Hz {} (config {} bytes)",
                    a.codec_string(),
                    a.sample_rate(),
                    a.channel_layout(),
                    a.codec_config().len()
                ),
                other => format!("unsupported track {other:?}"),
            };
            println!("add_track({}): {label}", info.stream_index());
            self.tracks.insert(
                info.stream_index(),
                Track {
                    label,
                    ..Track::default()
                },
            );
        }
        Ok(())
    }

    fn on_packet(&mut self, packet: &PacketView<'_>) -> PacketCallbackResult {
        let track = self
            .tracks
            .get_mut(&packet.stream_index())
            .expect("packet for an announced track");
        track.packets += 1;
        track.bytes += packet.data().len();
        track.sync_samples += usize::from(packet.is_key());
        // Delivered timeline: shared zero-based origin across tracks.
        track
            .first_delivered_dts_us
            .get_or_insert(packet.dts_us());
        // Source timeline: recovered exactly via the applied offset — this is
        // what a packager stamps into container/track timescales.
        track
            .first_source_dts_us
            .get_or_insert(packet.dts_us() + packet.applied_offset_us());
        track.last_end_us = packet.pts_us() + packet.duration_us();
        Ok(())
    }

    fn on_end(&mut self) {
        println!("all tracks finalized:");
        for (index, track) in &self.tracks {
            println!(
                "  track {index} [{}]: {} packets, {} bytes, {} sync, delivered dts0 {} us (source {} us), end {} us",
                track.label,
                track.packets,
                track.bytes,
                track.sync_samples,
                track.first_delivered_dts_us.unwrap_or(0),
                track.first_source_dts_us.unwrap_or(0),
                track.last_end_us
            );
        }
    }

    fn on_delivery_error(&mut self, error: &ez_ffmpeg::packet_sink::PacketSinkError) {
        eprintln!("packaging failed: {error}");
    }
}

fn main() -> Result<(), Box<dyn std::error::Error>> {
    let has_x264 = ez_ffmpeg::codec::get_encoders()
        .iter()
        .any(|e| e.codec_name == "libx264");
    if !has_x264 {
        eprintln!("This example needs an FFmpeg build with libx264.");
        return Ok(());
    }

    let sink = PacketSink::from_handler(StubPackager::default());
    FfmpegContext::builder()
        .input(Input::from("testsrc=size=640x360:rate=25:duration=2").set_format("lavfi"))
        .input(Input::from("sine=frequency=440:duration=2").set_format("lavfi"))
        .output(
            Output::from(sink)
                .add_stream_map("0:v")
                .add_stream_map("1:a")
                .set_video_codec("libx264")
                .set_video_codec_opt("preset", "ultrafast")
                .set_audio_codec("aac"),
        )
        .build()?
        .start()?
        .wait()?;
    Ok(())
}
