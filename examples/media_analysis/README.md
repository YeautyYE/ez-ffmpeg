# media_analysis

Surfaces FFmpeg detector/measurement results (`blackdetect`, `scdet`,
`silencedetect`, `ebur128`) as typed events folded into an `AnalysisReport`,
instead of scraping them out of FFmpeg's log output.

```bash
cargo run --example media_analysis
```

Expects a `test.mp4` in the working directory. `Analysis` runs the detectors in
a single decode pass over isolated `split`/`asplit` branches and returns black
regions, scene-change timestamps, silence regions and an EBU R128 loudness
summary (integrated LUFS, LRA, true peak).
