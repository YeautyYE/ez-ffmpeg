use ez_ffmpeg::rtmp::embed_rtmp_server::EmbedRtmpServer;
use ez_ffmpeg::{FfmpegContext, Input, Output};

fn main() {
    // ============================================================
    // Method 1: StreamBuilder API (Recommended for simple cases)
    // ============================================================
    // The simplest way to stream a file to an embedded RTMP server.
    // Just 5 lines of code with clear, self-documenting parameters.

    let handle = EmbedRtmpServer::stream_builder()
        .address("localhost:1935")
        .app_name("my-app")
        .stream_key("my-stream")
        .input_file("../../test.mp4")
        // readrate defaults to 1.0 (realtime), no need to set explicitly
        .start()
        .unwrap();

    handle.wait().unwrap();

    // ============================================================
    // Method 2: Traditional API (For full control)
    // ============================================================
    // Use this when you need more control over the server, input,
    // or FFmpeg context configuration.
    //
    // Architecture: Reactor pattern with edge-triggered IO (epoll/kqueue)
    // Thread model: 2-3 threads (accept + reactor + optional publisher)
    // Backpressure: 1MB warning, 2MB high, 4MB critical (disconnect)

    // 1. Create and start an embedded RTMP server on "localhost:1936"
    let embed_rtmp_server = EmbedRtmpServer::new("localhost:1936")
        .start()
        .unwrap();

    // 2. Create an RTMP "input" stream with app_name="my-app" and stream_key="my-stream"
    //    This returns an `Output` that FFmpeg can push data into.
    let output = embed_rtmp_server
        .create_rtmp_input("my-app", "my-stream")
        .unwrap();

    // 3. Prepare an `Input` using builder pattern (recommended)
    //    Note: Avoid old style `input.readrate = Some(1.0)`
    let input = Input::from("../../test.mp4")
        .set_readrate(1.0); // Optional: limit reading speed to 1x realtime

    // 4. Build and run the FFmpeg context
    FfmpegContext::builder()
        .input(input)
        .output(output)
        .build().unwrap()
        .start().unwrap()
        .wait().unwrap();

    // ============================================================
    // Method 3: Using External RTMP Server (No `rtmp` feature needed)
    // ============================================================

    // 1. Prepare an `Input` using builder pattern
    let input = Input::from("../../test.mp4")
        .set_readrate(1.0);

    // 2. Output the stream to an external RTMP server
    //    Note: RTMP requires FLV format with H.264 video and AAC audio
    let output = Output::from("rtmp://localhost:1937/my-app/my-stream")
        .set_format("flv") // Required for RTMP
        .set_video_codec("h264")
        .set_audio_codec("aac")
        .set_format_opt("flvflags", "no_duration_filesize");

    // 3. Build and run the FFmpeg context for external RTMP streaming
    FfmpegContext::builder()
        .input(input)
        .output(output)
        .build().unwrap()
        .start().unwrap()
        .wait().unwrap();
}
