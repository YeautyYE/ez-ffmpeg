# ez-ffmpeg Example: Packet Sink (AVC Access Units)

## Functionality

Encodes synthetic video with libx264 and collects the **encoded packets**
directly — no container is written and no bytes are demuxed back. The packet
sink delivers:

- one `on_stream_info` callback carrying a valid avcC
  (`AVCDecoderConfigurationRecord`) — the WebCodecs `description`;
- one `on_packet` callback per access unit, in AVCC layout (4-byte NAL length
  prefixes), with zero-based timestamps, a positive duration and an
  IDR-accurate `is_key` flag;
- a terminal `on_end` once every stream finished cleanly.

This replaces the mux-then-demux round-trip when feeding a `VideoDecoder`,
an RTP/SRT packetizer, or any consumer that wants codec packets instead of a
container byte stream. Callbacks run on the delivery thread and block the
pipeline while they run — copy data out and return (see the `packet_sink`
module docs for the backpressure contract).

## How to Run

1. Use an FFmpeg build that includes libx264 (the strict tier's v1 encoder
   whitelist); the example prints a notice and exits otherwise.
2. From the repository root, run `cargo run --example packet_sink_avc`.
3. The program prints the avcC size and a summary of the collected access
   units.
