# hdr_to_sdr

HDR-to-SDR tone-mapping cookbook. Detects the input's color transfer (PQ /
HLG / SDR) from the `StreamInfo::Video` color fields, routes on the **transfer
axis** (so wide-gamut SDR is not wrongly tone-mapped), probes which
tone-mapping filter the FFmpeg build has (`zscale`+`tonemap` or `libplacebo`),
and runs the correct chain — the parameters (`desat=0`, explicit `peak`, BT.709
re-tagging) that keep the output from looking washed out.

```bash
cargo run --example hdr_to_sdr -- input_hdr.mp4 output_sdr.mp4
# optional GPU chain:
cargo run --example hdr_to_sdr -- input_hdr.mp4 output_sdr.mp4 libplacebo
```

Fails closed with an actionable message if the build has no tone-mapping filter
(e.g. stock Homebrew ffmpeg). Verify the look on your own footage; the FFmpeg 8
`scale` fallback chain is not exercised by CI. See the example's module docs for
the full chain reference and the real-test protocol.
