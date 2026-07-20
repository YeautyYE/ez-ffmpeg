# ez-ffmpeg Example: Packet Sink (A/V Transport Packager Stub)

## Functionality

Encodes synchronized video (libx264) and audio (AAC) into one packet sink and
routes both streams through a single stateful `PacketSinkHandler` — the shape
of an fMP4 segmenter or an RTP/SRT sender session:

- `on_stream_info` announces every track once, with consumer-ready
  configuration (`avc1.PPCCLL` + avcC, `mp4a.40.X` + AudioSpecificConfig,
  dimensions, sample rate, channel layout);
- packets are routed by `stream_index` — no cross-stream interleaving order
  is promised, so each track keeps independent timing state;
- all tracks share one zero-based time origin; `applied_offset` recovers each
  stream's original encoder timeline, so true A/V offsets are preserved in
  the packaged output;
- per track: strictly increasing dts, positive durations, IDR-accurate sync
  flags — validated by the strict tier before delivery.

## How to Run

1. Use an FFmpeg build that includes libx264; the example prints a notice and
   exits otherwise.
2. From the repository root, run `cargo run --example packet_sink_av_transport`.
3. The program prints the announced tracks and the per-track packaging
   summary.
