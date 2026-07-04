# detect_streaming

The streaming, lower-level side of the analysis API: attach a
`MetadataEventFilter` to an output frame pipeline and receive `MetadataEvent`s
live on a channel *while the transcode runs* — as opposed to the one-shot
`Analysis` runner in the `media_analysis` example, which decodes once and
returns a folded report.

```bash
cargo run --example detect_streaming
```

Expects a `test.mp4`; writes `output.mp4` and prints each black-frame start as
it is detected. Drain the channel on a separate thread: the filter's default
backpressure policy aborts the run if the sink stalls.
