# media_analysis

Surfaces FFmpeg detector/measurement results (`blackdetect`, `scdet`,
`silencedetect`, `ebur128`) plus the crate's native Rust crop/letterbox
detection as typed events folded into an `AnalysisReport`, instead of
scraping them out of FFmpeg's log output.

```bash
cargo run --example media_analysis
```

Expects a `test.mp4` in the working directory. `Analysis` runs the detectors in
a single decode pass over isolated `split`/`asplit` branches and returns black
regions, scene-change timestamps, a crop suggestion, silence regions and an
EBU R128 loudness summary (integrated LUFS, LRA, true peak).

Crop detection is native Rust (progressive CPU frames only — no GPL
`cropdetect` filter, no `--enable-gpl`). An interlaced or hardware frame
fails the run with `Error::AnalysisFrame`; `CropDetectionOptions` offers
finer control (thresholds, rounding, `skip_initial` to step over known-bad
leading frames).
