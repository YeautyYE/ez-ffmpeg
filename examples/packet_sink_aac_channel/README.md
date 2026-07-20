# ez-ffmpeg Example: Packet Sink (AAC over an Owned Channel)

## Functionality

Encodes a sine tone with the native AAC encoder and consumes the **owned**
packet events on a separate thread while the job runs:

- `StreamInfo` carries the `mp4a.40.X` codec string, the
  `AudioSpecificConfig`, sample rate and channel layout;
- each `Packet` is an owned `EncodedPacket` (raw AAC frame plus tick and
  microsecond timing);
- `End` closes the stream of events.

The channel is bounded: when it fills, the delivery thread blocks until the
consumer catches up — which is why the consumer drains **concurrently** and
the main thread joins the job before the consumer. Draining only after
`wait()` would deadlock as soon as the channel fills.

Runs on FFmpeg builds without GPL components (no libx264 required).

## How to Run

1. From the repository root, run `cargo run --example packet_sink_aac_channel`.
2. The program prints the audio configuration, the first frames, and a
   delivery summary.
