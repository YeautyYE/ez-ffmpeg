# ez-ffmpeg Example: Packet Sink (WebCodecs-Style AVC Feed)

## Functionality

Encodes synthetic video with libx264 and maps the **encoded packets** straight
onto a WebCodecs-style decoder feed — no container is written and no bytes are
demuxed back:

- `on_stream_info` supplies a ready-made `VideoDecoderConfig`: the RFC 6381
  codec string (`avc1.PPCCLL`), the avcC record (`description`), coded
  dimensions and the sample aspect ratio;
- `on_packet` supplies `EncodedVideoChunk`-shaped samples: AVCC payload
  (4-byte NAL length prefixes), microsecond timestamp/duration via the `*_us`
  conveniences, and an IDR-accurate key/delta flag;
- `on_end` reports a clean finish.

The consumer is a single stateful `PacketSinkHandler` — callbacks run serially
on the delivery thread, so the handler needs no locking — and the stub decoder
keeps only counters plus the latest chunk, so memory stays bounded for streams
of any length. Callbacks block the encoding pipeline while they run; see the
`packet_sink` module docs for the backpressure contract.

## How to Run

1. Use an FFmpeg build that includes libx264 (the strict tier's v1 encoder
   whitelist); the example prints a notice and exits otherwise.
2. From the repository root, run `cargo run --example packet_sink_avc`.
3. The program prints the decoder configuration and a delivery summary.
