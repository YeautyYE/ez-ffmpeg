//! Map encoded H.264 access units onto a WebCodecs-style decoder feed — no
//! container, no I/O, bounded memory.
//!
//! A packet sink delivers what a `VideoDecoder` (or an RTP packetizer)
//! actually consumes:
//!
//! * one stream configuration with a ready-made RFC 6381 codec string
//!   (`avc1.PPCCLL`) and the avcC record (the WebCodecs `description`);
//! * per access unit: AVCC payload (4-byte NAL length prefixes),
//!   microsecond timestamp/duration, and an IDR-accurate key/delta flag.
//!
//! The consumer is a [`PacketSinkHandler`]: one stateful object receives
//! every callback serially on the delivery thread — no `Arc<Mutex<_>>`
//! ceremony — and feeds mapped chunks to a stub "decoder" that keeps only
//! counters and the latest chunk, so memory stays bounded no matter how long
//! the stream runs.
//!
//! Requires an FFmpeg build with libx264 (the strict tier's v1 whitelist).
//!
//! Run it:
//!
//! ```sh
//! cargo run --example packet_sink_avc
//! ```

use ez_ffmpeg::packet_sink::{
    PacketCallbackError, PacketCallbackResult, PacketSink, PacketSinkHandler, PacketStreamInfo,
    PacketView,
};
use ez_ffmpeg::{FfmpegContext, Input, Output};

/// The WebCodecs `VideoDecoderConfig` fields this pipeline produces.
struct DecoderConfig {
    codec: String,
    description: Vec<u8>,
    coded_width: i32,
    coded_height: i32,
}

/// One WebCodecs `EncodedVideoChunk`-shaped sample (scalar fields only —
/// a real feed would also copy the payload out of the borrowed view).
struct Chunk {
    timestamp_us: i64,
    duration_us: i64,
    key: bool,
    byte_length: usize,
}

/// Stand-in for a real `VideoDecoder`: swallow chunks, keep counters and the
/// last chunk only — bounded memory by construction.
#[derive(Default)]
struct StubDecoder {
    chunks: usize,
    key_chunks: usize,
    total_bytes: usize,
    last_chunk: Option<Chunk>,
}

impl StubDecoder {
    fn configure(&mut self, config: DecoderConfig) {
        println!(
            "decoder.configure(codec: \"{}\", description: {} bytes, codedWidth: {}, codedHeight: {})",
            config.codec,
            config.description.len(),
            config.coded_width,
            config.coded_height
        );
    }

    fn decode(&mut self, chunk: Chunk) {
        self.chunks += 1;
        self.key_chunks += usize::from(chunk.key);
        self.total_bytes += chunk.byte_length;
        self.last_chunk = Some(chunk);
    }
}

/// The packet-sink consumer: maps stream info and packets onto the stub
/// decoder. All methods run serially on the delivery thread, so plain
/// `&mut self` state suffices.
struct WebCodecsFeed {
    decoder: StubDecoder,
}

impl PacketSinkHandler for WebCodecsFeed {
    fn on_stream_info(&mut self, streams: &[PacketStreamInfo]) -> PacketCallbackResult {
        let video = streams
            .iter()
            .find_map(PacketStreamInfo::video)
            .ok_or_else(|| PacketCallbackError::new("no video stream configured"))?;
        self.decoder.configure(DecoderConfig {
            codec: video.codec_string().to_owned(),
            description: video.codec_config().to_vec(),
            coded_width: video.width(),
            coded_height: video.height(),
        });
        if let Some(sar) = video.sample_aspect_ratio() {
            println!("display aspect hint: SAR {}/{}", sar.num, sar.den);
        }
        Ok(())
    }

    fn on_packet(&mut self, packet: &PacketView<'_>) -> PacketCallbackResult {
        // NOTE: this runs on the delivery thread and blocks the encoding
        // pipeline while it runs; the borrowed view dies with this call, so
        // anything kept must be copied out.
        self.decoder.decode(Chunk {
            timestamp_us: packet.pts_us(),
            duration_us: packet.duration_us(),
            key: packet.is_key(),
            byte_length: packet.data().len(),
        });
        Ok(())
    }

    fn on_end(&mut self) {
        println!(
            "delivery finished cleanly: {} chunks ({} key, {} bytes total)",
            self.decoder.chunks, self.decoder.key_chunks, self.decoder.total_bytes
        );
        if let Some(last) = &self.decoder.last_chunk {
            println!(
                "last chunk: timestamp {} us, duration {} us, {}",
                last.timestamp_us,
                last.duration_us,
                if last.key { "key" } else { "delta" }
            );
        }
    }

    fn on_delivery_error(&mut self, error: &ez_ffmpeg::packet_sink::PacketSinkError) {
        eprintln!("delivery failed: {error}");
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

    // Two seconds of synthetic 25 fps video, straight into the handler.
    let sink = PacketSink::from_handler(WebCodecsFeed {
        decoder: StubDecoder::default(),
    });
    FfmpegContext::builder()
        .input(Input::from("testsrc=size=640x360:rate=25:duration=2").set_format("lavfi"))
        .output(
            Output::from(sink)
                .set_video_codec("libx264")
                .set_video_codec_opt("preset", "ultrafast"),
        )
        .build()?
        .start()?
        .wait()?;
    Ok(())
}
