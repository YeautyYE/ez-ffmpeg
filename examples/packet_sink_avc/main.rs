//! Collect encoded H.264 access units in memory — no container, no I/O.
//!
//! A packet sink is the packet-domain counterpart of the byte-domain write
//! callback: instead of muxed container bytes, the callbacks receive each
//! encoded packet as a WebCodecs-style access unit (avcC layout, 4-byte NAL
//! length prefixes) plus a one-time stream configuration whose `extradata`
//! is a ready-to-use `AVCDecoderConfigurationRecord`. Feed the pair to a
//! `VideoDecoder`, an RTP packetizer, or your own transport — no demuxing
//! round-trip required.
//!
//! Requires an FFmpeg build with libx264 (the strict tier's v1 whitelist).
//!
//! Run it:
//!
//! ```sh
//! cargo run --example packet_sink_avc
//! ```

use ez_ffmpeg::packet_sink::PacketSink;
use ez_ffmpeg::{FfmpegContext, Input, Output};
use std::sync::{Arc, Mutex};

/// One collected access unit.
struct CollectedAu {
    pts: i64,
    dts: i64,
    duration: i64,
    is_key: bool,
    data: Vec<u8>,
}

fn main() -> Result<(), Box<dyn std::error::Error>> {
    let has_x264 = ez_ffmpeg::codec::get_encoders()
        .iter()
        .any(|e| e.codec_name == "libx264");
    if !has_x264 {
        eprintln!("This example needs an FFmpeg build with libx264.");
        return Ok(());
    }

    // Shared collection the callbacks write into. The borrowed `PacketView`
    // is only valid during the callback, so everything kept is copied out.
    let config: Arc<Mutex<Vec<u8>>> = Arc::new(Mutex::new(Vec::new()));
    let aus: Arc<Mutex<Vec<CollectedAu>>> = Arc::new(Mutex::new(Vec::new()));
    let time_base = Arc::new(Mutex::new((1, 25)));

    let sink = {
        let (config, aus, time_base) = (config.clone(), aus.clone(), time_base.clone());
        PacketSink::builder()
            .on_stream_info(move |infos| {
                // Exactly once, before any packet: the avcC decoder
                // configuration (WebCodecs `description`).
                let info = &infos[0];
                *config.lock().unwrap() = info.extradata().to_vec();
                *time_base.lock().unwrap() = (info.time_base().num, info.time_base().den);
                0
            })
            .on_packet(move |pkt| {
                // NOTE: this callback runs on the delivery thread and blocks
                // the encoding pipeline while it runs — copy out and return.
                aus.lock().unwrap().push(CollectedAu {
                    pts: pkt.pts(),
                    dts: pkt.dts(),
                    duration: pkt.duration(),
                    is_key: pkt.is_key(),
                    data: pkt.data().to_vec(),
                });
                0
            })
            .on_end(|| println!("delivery finished cleanly"))
            .on_error(|e| eprintln!("delivery failed: {e}"))
            .build()
    };

    // Two seconds of synthetic 25 fps video, straight into the sink.
    FfmpegContext::builder()
        .input(Input::from("testsrc=size=640x360:rate=25:duration=2").set_format("lavfi"))
        .output(
            Output::new_by_packet_sink(sink)
                .set_video_codec("libx264")
                .set_video_codec_opt("preset", "ultrafast"),
        )
        .build()?
        .start()?
        .wait()?;

    let config = config.lock().unwrap();
    let aus = aus.lock().unwrap();
    let (tb_num, tb_den) = *time_base.lock().unwrap();
    let total_bytes: usize = aus.iter().map(|au| au.data.len()).sum();
    let key_count = aus.iter().filter(|au| au.is_key).count();

    println!(
        "avcC configuration: {} bytes (version {}, {} SPS)",
        config.len(),
        config[0],
        config[5] & 0x1F
    );
    println!(
        "collected {} access units ({} bytes total, {} key), time base {}/{}",
        aus.len(),
        total_bytes,
        key_count,
        tb_num,
        tb_den
    );
    for au in aus.iter().take(3) {
        println!(
            "  au: pts={} dts={} duration={} key={} first NAL length={}",
            au.pts,
            au.dts,
            au.duration,
            au.is_key,
            u32::from_be_bytes([au.data[0], au.data[1], au.data[2], au.data[3]])
        );
    }
    Ok(())
}
