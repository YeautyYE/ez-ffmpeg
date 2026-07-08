//! PR-4 regression net: shutdown and thread lifecycle.
//!
//! - stop() must not return while worker threads (demux/decode/filter/
//!   encode) are still running: the old synchronizer only counted muxer
//!   threads, so callers could free resources under live workers.
//! - A muxer blocked in a network write must be interruptible by stop():
//!   FFmpeg installs an interrupt_callback on the output AVIOContext
//!   (ffmpeg_mux_init.c:3326,3371); without it stop() hangs forever.

use ez_ffmpeg::{FfmpegContext, Input, Output};
use std::sync::Mutex;
use std::time::{Duration, Instant};

/// Both tests observe process-global state (thread counts, socket buffers):
/// they must not run concurrently inside the shared test binary.
static PROCESS_LOCK: Mutex<()> = Mutex::new(());

/// Threads of this process as reported by /proc (Linux only).
#[cfg(target_os = "linux")]
fn process_thread_count() -> usize {
    let status = std::fs::read_to_string("/proc/self/status").unwrap();
    status
        .lines()
        .find_map(|l| l.strip_prefix("Threads:"))
        .and_then(|v| v.trim().parse().ok())
        .expect("Threads: line missing from /proc/self/status")
}

#[cfg(target_os = "linux")]
#[test]
fn stop_returns_only_after_all_worker_threads_exited() {
    let _lock = PROCESS_LOCK.lock().unwrap_or_else(|e| e.into_inner());
    let out = std::env::temp_dir().join(format!(
        "ez_ffmpeg_lifecycle_{}_stop.mp4",
        std::process::id()
    ));

    let baseline = process_thread_count();

    let scheduler = FfmpegContext::builder()
        .input(Input::from("color=c=black:s=320x240:r=30").set_format("lavfi"))
        .output(Output::from(out.to_string_lossy().as_ref()).set_video_codec("mpeg4"))
        .build()
        .unwrap()
        .start()
        .unwrap();

    std::thread::sleep(Duration::from_millis(300));
    scheduler.stop();

    // Workers poll the stop flag every 100ms: if stop() returns without
    // joining them, they are still alive here. A 50ms grace window absorbs
    // OS-level thread teardown without masking the 100ms polling gap.
    let deadline = Instant::now() + Duration::from_millis(50);
    let mut count = process_thread_count();
    while count > baseline && Instant::now() < deadline {
        std::thread::sleep(Duration::from_millis(5));
        count = process_thread_count();
    }
    assert!(
        count <= baseline,
        "stop() returned while {} worker thread(s) were still running",
        count - baseline
    );
}

#[test]
fn stop_interrupts_muxer_blocked_on_unread_network_output() {
    let _lock = PROCESS_LOCK.lock().unwrap_or_else(|e| e.into_inner());
    // A TCP peer that accepts the connection and then never reads: the
    // muxer eventually fills the socket buffers and blocks inside write.
    let listener = std::net::TcpListener::bind("127.0.0.1:0").unwrap();
    let addr = listener.local_addr().unwrap();
    let accept_thread = std::thread::spawn(move || {
        let (stream, _) = listener.accept().unwrap();
        // Hold the socket open without reading until the test ends.
        std::thread::sleep(Duration::from_secs(30));
        drop(stream);
    });

    let scheduler = FfmpegContext::builder()
        .input(Input::from("color=c=black:s=1280x720:r=30").set_format("lavfi"))
        .output(
            Output::from(format!("tcp://{addr}"))
                .set_format("mpegts")
                .set_video_codec("mpeg4")
                .set_video_codec_opt("qscale", "1"),
        )
        .build()
        .unwrap()
        .start()
        .unwrap();

    // Give the pipeline time to fill the socket buffers and block.
    std::thread::sleep(Duration::from_secs(2));

    let (tx, rx) = std::sync::mpsc::channel();
    std::thread::spawn(move || {
        scheduler.stop();
        let _ = tx.send(());
    });

    let stopped = rx.recv_timeout(Duration::from_secs(5));
    assert!(
        stopped.is_ok(),
        "stop() must interrupt a muxer blocked in a network write \
         (interrupt_callback missing on the output context)"
    );

    drop(accept_thread); // detach; the sleeping peer ends with the process
}
