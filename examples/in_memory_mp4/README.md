# ez-ffmpeg Example: In-Memory MP4 (VideoWriter)

## Functionality

Encodes pushed frames into a complete MP4 held entirely in a `Vec<u8>` — no file
is written. Because MP4 is a seekable container (the muxer rewinds to patch its
`moov`/`stco` atoms), the output is given both a write callback and a seek
callback over a small growable byte sink.

A streaming container (FLV, MPEG-TS) needs no seek callback and can write
straight through — see the note at the end of `main.rs`.

## How to Run

1. From the repository root, run `cargo run --example in_memory_mp4`.
2. The program prints the size of the encoded in-memory MP4.
