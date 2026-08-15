# hdr_to_sdr

HDR-to-SDR tone-mapping cookbook. Detects the input's color transfer (PQ /
HLG / SDR) from the `StreamInfo::Video` color fields, routes on the **transfer
axis** (so wide-gamut SDR is not wrongly tone-mapped), probes which backend the
linked FFmpeg actually provides, and runs the matching chain — the parameters
(`desat=0`, explicit `peak`, BT.709 re-tagging) that keep the output from
looking washed out.

This is a cookbook, not a public `HdrToSdr` recipe. Conversion is opt-in: you
run this example (or copy the filter string). Ordinary transcode and
`HlsLadder` do **not** tone-map just because the input is tagged PQ/HLG.

```bash
cargo run --example hdr_to_sdr -- input_hdr.mp4 output_sdr.mp4
# third-arg `libplacebo` selects QualityFirst preference; default Compatible
# also tries libplacebo after zscale when the filter is compiled in
# (Vulkan is still required at graph-config time):
cargo run --example hdr_to_sdr -- input_hdr.mp4 output_sdr.mp4 libplacebo
```

## Backend order

Default (no extra argument) keeps the historical zscale-first choice when
those filters exist:

1. `zscale` + `tonemap` (libzimg)
2. `libplacebo` (Vulkan-capable FFmpeg build)
3. FFmpeg 8 `scale` with `intent=perceptual` — **only** when option probing
   shows that `scale` exposes `out_primaries`, `out_transfer`, and `intent`.
   The `scale` filter name alone is not enough: it also exists in FFmpeg 7.1,
   where those options are missing and a naive scale produces gray output.
4. Otherwise fail closed, with no output file, listing the probed facts and
   how to install `--enable-libzimg`, `--enable-libplacebo`, or FFmpeg 8+.

Passing `libplacebo` as the third argument prefers that backend when it is
compiled in (`libplacebo` → `zscale` → FFmpeg 8 `scale`), then fails closed.

**Registered is not the same as ready.** Before committing to a backend, the
example preflights the candidate chain by configuring a tiny lavfi test graph.
A filter that is compiled in but whose graph will not configure — typically
`libplacebo` without a usable Vulkan runtime/ICD — is rejected with that
reason and the next candidate is tried, so zscale / the FFmpeg 8 `scale`
fallback still run on such hosts.

## HDR side data

When the linked FFmpeg has the `sidedata` filter, the chains append
typed `sidedata=mode=delete` filters for HDR frame side data only
(mastering display metadata, content light level, HDR10+/Dolby Vision
dynamic metadata). A53 captions, timecode, and other non-HDR entries are
left in place. Bare `sidedata=delete` is not used — that selector removes
every side-data type. Without the filter the route still runs; leftover
HDR side data may remain and should be stripped downstream if it matters
to your players.

BT.2020 primaries or matrix with an **unspecified** transfer is
`AmbiguousHdr`: the cookbook fails closed instead of guessing missing-tag
PQ versus wide-gamut SDR. Set `color_trc` (PQ / HLG / an SDR transfer)
before converting. Fully untagged `(unspecified, unspecified, unspecified)`
is still ordinary SDR.

Wide-gamut SDR (BT.2020 primaries + an SDR transfer) never tone-maps. It may
use built-in `colorspace` gamut conversion on FFmpeg 7.1 without zimg.

wgpu is **not** an HDR auto-route: the current wgpu path is 8-bit YUV/NV12
with BT.601/709 only. P010 / BT.2020 / PQ / HLG support is out of scope here.

## FFmpeg 7.1 without zimg/libplacebo

FFmpeg 7.1 `tonemap` only accepts linear float RGB, and 7.1 `colorspace` has
no PQ/HLG EOTF. There is no correct built-in PQ/HLG→SDR chain. The example
refuses to run rather than write a washed-out file. Diagnose with
`ffmpeg -filters` and `ffmpeg -h filter=scale`.

Verify the look on your own footage. See the example's module docs for the
full chain reference and the real-test protocol.
