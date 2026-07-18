# extract_whisper_pcm

Audio → 16 kHz mono `f32` PCM with `SampleExtractor::for_whisper`, the shape
whisper-rs / candle / ort consume. `collect_samples()` flattens the run into
one interleaved `Vec<f32>` and the example prints sample count, duration, peak
and RMS (for a pure tone, peak/RMS ≈ 1.414 — a quick check that the resample
preserved the waveform). A second, independent pass demonstrates the streaming
alternative (`samples()`), which keeps only a small bounded number of
`AudioChunk`s in flight — on inputs too long for one flat buffer, run only
that form.

```bash
cargo run --example extract_whisper_pcm [input-file]
```

Runs on a synthetic 3-second 440 Hz `sine` tone when no file is given.
