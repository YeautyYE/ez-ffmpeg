# ez-ffmpeg Example: Packet Scanner

This example demonstrates how to use `PacketScanner` to iterate over demuxed packet metadata from a media file without decoding. It is useful for inspecting timestamps, keyframe locations, packet sizes, and stream indices at the packet level.

## Features

- **Sequential Scan**: Iterate over all packets in a file using the `packets()` iterator.
- **Keyframe Detection**: Use `is_keyframe()` to identify keyframe packets and their positions.
- **Per-Stream Statistics**: Aggregate packet counts, keyframe counts, and byte sizes per stream.
- **Seek**: Jump to an arbitrary position (in microseconds) with `seek()` and read packets from there.
- **Jump Reading**: Perform multiple seeks to different timestamps for random-access scanning.
- **Corrupt Detection**: Use `is_corrupt()` to find packets flagged as corrupt.

## Key Methods

1. **`PacketScanner::open(url)`**: Opens a media file or URL for packet scanning. Returns a `PacketScanner` instance.

2. **`scanner.packets()`**: Returns an iterator that yields `Result<PacketInfo>` for each packet until EOF.

3. **`scanner.seek(timestamp_us)`**: Seeks to the nearest keyframe before the given timestamp (in microseconds). Can be called repeatedly for jump-reading patterns.

4. **`scanner.next_packet()`**: Reads the next packet. Returns `Ok(Some(PacketInfo))` for a packet, `Ok(None)` at EOF.

5. **`PacketInfo` getters**:
   - `stream_index()` — Which stream this packet belongs to.
   - `pts()` / `dts()` — Presentation / decompression timestamps (`Option<i64>`).
   - `duration()` — Packet duration in stream time-base units.
   - `size()` — Packet data size in bytes.
   - `pos()` — Byte position in the input file.
   - `is_keyframe()` — Whether the packet contains a keyframe.
   - `is_corrupt()` — Whether the packet is flagged as corrupt.

## Example Overview

The following scenarios are demonstrated:

1. **Sequential Scan**: Iterate all packets and count totals.
2. **Keyframe Detection**: Filter keyframes and print their metadata.
3. **Per-Stream Statistics**: Aggregate packet/keyframe/byte counts per stream index.
4. **Seek to Position**: Seek to 1 second and read the next 5 packets.
5. **Multiple Seeks**: Jump to several timestamps (0s, 0.5s, 1s, 2s, 3s) and read one packet each.
6. **Corrupt Packet Detection**: Scan for packets with the corrupt flag set.
7. **First Keyframe Per Stream**: Find and print the first keyframe in each stream.

## When to Use

- **Media analysis tools**: Quickly scan packet metadata without the cost of decoding.
- **Keyframe indexing**: Build a keyframe index for seeking or thumbnail generation.
- **Stream inspection**: Understand the packet structure and interleaving of a media file.
- **Integrity checks**: Detect corrupt packets in a media file.
- **Random-access patterns**: Seek to specific positions and inspect nearby packets.
