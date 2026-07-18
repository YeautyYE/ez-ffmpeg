# ai_media_ingest

One media file → both model inputs, no intermediate files: `FrameExtractor`
samples one 224-wide RGB frame per second for a vision model, and
`SampleExtractor::for_whisper` produces 16 kHz mono `f32` PCM for an ASR
model. Each extractor runs its own single decode pass over the same file and
hands back owned buffers — the ingest shape multimodal pipelines want, without
JPEG/WAV detours or raw-byte plumbing.

```bash
cargo run --example ai_media_ingest [input-file]
```

When no file is given, synthesizes `ai_ingest_input.mp4` (6 s test pattern
plus a 440 Hz tone) first, then ingests it and prints a per-modality summary.
