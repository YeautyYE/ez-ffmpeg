//! Drain owned AAC packets on a consumer thread while the job runs.
//!
//! The owned-channel adapter copies each delivered packet into an
//! [`EncodedPacket`] and hands it over a **bounded** channel. The consumer
//! MUST drain concurrently: a full channel blocks the encoding pipeline
//! (bounded backpressure — nothing is dropped), so draining only after
//! `wait()` would deadlock. This example shows the correct order — start the
//! job, drain on a second thread, then join the job and the consumer.
//!
//! Uses the native AAC encoder, so it runs on FFmpeg builds without GPL
//! components (no libx264 needed).
//!
//! Run it:
//!
//! ```sh
//! cargo run --example packet_sink_aac_channel
//! ```

use ez_ffmpeg::packet_sink::{PacketSink, PacketSinkEvent};
use ez_ffmpeg::{FfmpegContext, Input, Output};
use std::num::NonZeroUsize;

fn main() -> Result<(), Box<dyn std::error::Error>> {
    // A deliberately small channel: backpressure engages quickly, and the
    // concurrent consumer keeps the pipeline moving anyway.
    let (sink, receiver) = PacketSink::channel(NonZeroUsize::new(8).unwrap());

    let scheduler = FfmpegContext::builder()
        .input(Input::from("sine=frequency=440:duration=2").set_format("lavfi"))
        .output(Output::from(sink).set_audio_codec("aac"))
        .build()?
        .start()?;

    // Consumer thread: drain events while the job runs.
    let consumer = std::thread::spawn(move || -> Result<(), String> {
        let mut frames = 0usize;
        let mut bytes = 0usize;
        for event in receiver.iter() {
            match event {
                PacketSinkEvent::StreamInfo(streams) => {
                    let audio = streams[0].audio().expect("aac stream");
                    println!(
                        "config: codec \"{}\", ASC {} bytes, {} Hz, {} ({} ch)",
                        audio.codec_string(),
                        audio.codec_config().len(),
                        audio.sample_rate(),
                        audio.channel_layout(),
                        audio.channels()
                    );
                }
                PacketSinkEvent::Packet(packet) => {
                    frames += 1;
                    bytes += packet.data().len();
                    if frames <= 3 {
                        println!(
                            "  frame {frames}: pts {} us, duration {} us, {} bytes",
                            packet.pts_us(),
                            packet.duration_us(),
                            packet.data().len()
                        );
                    }
                }
                PacketSinkEvent::End => {
                    println!("delivery finished cleanly: {frames} AAC frames, {bytes} bytes");
                    break;
                }
                PacketSinkEvent::Error(error) => return Err(error.to_string()),
                // The event set can grow; unknown events are ignorable here.
                _ => {}
            }
        }
        Ok(())
    });

    // Join order matters: the job first (the worker needs the consumer to
    // keep draining while it finishes), then the consumer.
    let job = scheduler.wait();
    let consumption = consumer.join().expect("consumer thread panicked");
    job?;
    consumption.map_err(|e| -> Box<dyn std::error::Error> { e.into() })?;
    Ok(())
}
