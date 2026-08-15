# http_input

Reads a single HTTP(S) resource through the crate's own rustls-based client
(`HttpInput`) instead of FFmpeg's protocol layer, and remuxes it to a local
file. Typed `HttpInputError` failures (TLS, redirects, truncation) replace
generic FFmpeg errnos.

```bash
cargo run --example http_input --features http-input -- <http(s)-url> <output>
```

HLS / DASH URLs are rejected by design — this input is for one media
resource, not adaptive playlists. Reconnect is off by default; see the
commented `ReconnectPolicy::seekable_default()` (VOD file) and
`ReconnectPolicy::streamed_default()` (live stream) lines in `main.rs`.
Plain `Input::from(url)` still uses FFmpeg's own protocols.
