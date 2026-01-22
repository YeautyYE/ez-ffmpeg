# ez-ffmpeg Example: RTMP Streaming

This example demonstrates two methods for streaming video using **RTMP**:

1. **Using an Embedded RTMP Server**
2. **Using an External RTMP Server**

## Architecture & Performance (v0.6.0+)

The embedded RTMP server uses a high-performance **Reactor** pattern:

| Metric | Value |
|--------|-------|
| I/O Model | Edge-triggered epoll/kqueue (Linux/macOS), level-triggered WSAPoll (Windows) |
| Thread Model | 2 threads (accept + reactor) |
| Memory/Connection | Variable (8KB read buffer + write queue) |
| Max Connections | 10,000 default on Linux/macOS (auto-adjusted by FD limit × 80%); 8,000 on Windows |
| GOP Clone | O(1) via `Arc<[FrameData]>` |

### Backpressure Management

The write queue uses tiered backpressure to handle slow clients:

| Level | Threshold | Behavior |
|-------|-----------|----------|
| Normal | < 1MB | All frames enqueued |
| Warning | 1-2MB | Drop non-keyframe video, keep audio + keyframes |
| High | 2-4MB | Keep keyframes and sequence headers only |
| Critical | ≥ 4MB | Disconnect client |

### Data Flow (Embedded RTMP)

```
FFmpeg → Output (RTMP input) → WriteQueue → Reactor → Clients
                                    ↓
                              FrozenGop (O(1) clone for new subscribers)
```

### Why Embedded RTMP?

Compared to traditional RTMP servers (e.g., nginx-rtmp), the embedded approach offers significant advantages:

| Dimension | ez-ffmpeg Embedded | nginx-rtmp |
|-----------|-------------------|------------|
| I/O Model | Native epoll/kqueue/WSAPoll (libc FFI) | nginx event loop |
| Trigger Mode | Edge-triggered on Linux/macOS; level-triggered on Windows | Depends on nginx config |
| GOP Sharing | `Arc<[FrameData]>` O(1) clone | Network/IPC copy |
| Data Path | In-process ingest (no TCP between FFmpeg and server) | Network serialization |
| Thread Model | 2 threads | Multi-process workers |
| Backpressure | Tiered (1MB/2MB/4MB) | Socket buffer only |
| Deployment | Zero-config, code-embedded | Separate deployment |

**Data Path Comparison:**

```
Traditional (nginx-rtmp):
  FFmpeg → TCP → nginx-rtmp → TCP → Clients
           ↑                   ↑
     Network copy         Network copy

Embedded (ez-ffmpeg):
  FFmpeg → In-process → Reactor → Clients
           ↑               ↑
    No TCP ingest     Arc clone (GOP)
```

**Key Implementation Details:**

Native edge-triggered I/O (no tokio/mio dependency):
```rust
// Linux: epoll with edge-triggered mode
const EPOLLET: u32 = 1 << 31;
epoll_ctl(epfd, EPOLL_CTL_ADD, fd, &event);

// macOS/BSD: kqueue with EV_CLEAR
const EV_CLEAR: u16 = 0x0020;
kevent(kq, &changelist, 1, null, 0, null);
```

Zero-copy GOP sharing across subscribers (simplified for illustration):
```rust
// FrozenGop wraps Arc<[FrameData]> for O(1) clone
pub struct FrozenGop {
    frames: Arc<[FrameData]>,
}

// New subscriber receives GOP instantly without data copy
let gop_clone = frozen_gop.clone(); // Only Arc reference count increment
```

## Method 1: Using an Embedded RTMP Server

The `EmbedRtmpServer` allows you to create an RTMP server directly in memory. This method is useful for local or test streaming scenarios, and it bypasses the need for actual network sockets.

### Prerequisites
- To use the **Embedded RTMP Server**, the `rtmp` feature must be enabled in your `Cargo.toml`.

### Steps:
1. **Start the RTMP server**:  
   Create and start an embedded RTMP server running on `localhost:1935` (or your desired address/port).

2. **Create the RTMP input stream**:  
   You will create an input stream with a specific app name and stream key (e.g., `my-app` and `my-stream`) that the server can accept.

3. **Prepare the input source**:  
   Specify the video file you want to stream (e.g., `test.mp4`), and optionally set the reading rate for the input file.

4. **Run FFmpeg to stream to the RTMP server**:  
   FFmpeg will push the data from the input file to the embedded RTMP server you just created.

### Feature Flag:
To enable the embedded RTMP server, you need to include the `rtmp` feature in your `Cargo.toml` file like this:

```toml
[dependencies]
ez-ffmpeg = { version = "X.Y.Z", features = ["rtmp"] }
```

## Method 2: Using an External RTMP Server

If you don’t need an embedded RTMP server and instead want to stream to a real, external RTMP server (e.g., YouTube, Twitch, or any other RTMP destination), you can use this method.

### Steps:
1. **Prepare the input source**:  
   Just like in the first method, specify the video file you want to stream (e.g., `test.mp4`).

2. **Set the external RTMP server URL**:  
   Set the output stream to an external RTMP server URL (e.g., `rtmp://localhost/my-app/my-stream`).

3. **Run FFmpeg to stream to the external RTMP server**:  
   FFmpeg will push the data from the input file to the specified external RTMP server.

### Feature Flag:
No special feature flag is required to use an external RTMP server.

## Conclusion

- **Embedded RTMP Server**: Ideal for local streaming or testing purposes. Requires the `rtmp` feature.
- **External RTMP Server**: Stream to any remote RTMP server like YouTube, Twitch, etc. No need for the `rtmp` feature.

## Troubleshooting

### Common Issues

| Symptom | Cause | Solution |
|---------|-------|----------|
| `swscaler: No accelerated colorspace conversion` | FFplay lacks SIMD optimization | This is a client-side warning, safe to ignore |
| `h264: co located POCs unavailable` | Joining stream mid-GOP | Normal when connecting to live stream; wait for next keyframe |
| Client disconnected unexpectedly | Backpressure critical threshold | Client too slow; check network or reduce bitrate |
| High memory usage | Too many cached GOPs | Reduce `max_gops` configuration |

### Performance Tips

1. **Local testing**: Backpressure rarely triggers due to ~0ms localhost latency
2. **Production**: Monitor write queue size; consider load balancing for 1000+ clients
3. **Audio-only streams**: Use shorter timeout (5s vs 10s for video)
