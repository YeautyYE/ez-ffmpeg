//! PR-4 regression net: shutdown and thread lifecycle.
//!
//! - stop() must not return while worker threads (demux/decode/filter/
//!   encode) are still running: the old synchronizer only counted muxer
//!   threads, so callers could free resources under live workers.
//! - A muxer blocked in a network write must be interruptible by stop():
//!   FFmpeg installs an interrupt_callback on the output AVIOContext
//!   (ffmpeg_mux_init.c:3326,3371); without it stop() hangs forever.

mod common;

use ez_ffmpeg::{FfmpegContext, Input, Output};
use std::sync::Mutex;
use std::time::Duration;
// Only the Linux-gated thread-count test polls against a deadline.
#[cfg(target_os = "linux")]
use std::time::Instant;

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
    scheduler
        .stop()
        .expect("graceful stop of a healthy local job must succeed");

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
    // A TCP peer that accepts the connection and then never reads: the muxer
    // fills the socket buffers and blocks inside write. The listener's SO_RCVBUF
    // is fixed BEFORE listen(2) so the accepted connection inherits a ~16 KiB
    // receive buffer — backpressure then hits after only a few KB, not the
    // megabytes of default autotuned buffers a slow/ASAN/throttled-CI box might
    // not fill in the window below.
    let (listener, listener_small) = common::make_backpressure_listener();
    let addr = listener.local_addr().unwrap();

    // The accept thread verifies the CHILD socket's effective receive buffer and
    // reports whether backpressure is PROVABLY forced (the listener value is not
    // proof it reached the child) back to the main thread.
    let (shrunk_tx, shrunk_rx) = std::sync::mpsc::channel::<bool>();
    let accept_thread = std::thread::spawn(move || {
        let (stream, _) = listener.accept().unwrap();
        let child_small = {
            #[cfg(target_os = "linux")]
            {
                use std::os::unix::io::AsRawFd;
                listener_small && common::rcvbuf_is_small(stream.as_raw_fd())
            }
            #[cfg(not(target_os = "linux"))]
            {
                let _ = listener_small;
                false
            }
        };
        let _ = shrunk_tx.send(child_small);
        // Hold the socket open without reading until the test ends.
        std::thread::sleep(Duration::from_secs(30));
        drop(stream);
    });

    // testsrc2 is a HIGH-ENTROPY pattern — unlike the near-constant black frame
    // an earlier version used (which mpeg4 at qscale=1 still compresses to
    // ~0.8 Mbit/s, far too little to fill even default buffers in a few seconds),
    // it drives tens of Mbit/s. With the accepted socket's receive buffer
    // verifiably ~16 KiB (see `buffers_shrunk`), a non-reading peer fills it
    // within milliseconds on ANY machine, so the muxer is PROVABLY blocked in
    // write (the input is unbounded: a healthy loop cannot have finished) and the
    // grace-cut error is deterministic — that stronger assertion is gated on it.
    let scheduler = FfmpegContext::builder()
        .input(Input::from("testsrc2=s=1920x1080:r=30").set_format("lavfi"))
        .output(
            // send_buffer_size caps the muxer's own send buffer; paired with the
            // shrunk peer receive buffer, the write blocks after only tens of KB.
            Output::from(format!("tcp://{addr}?send_buffer_size=16384"))
                .set_format("mpegts")
                .set_video_codec("mpeg4")
                .set_video_codec_opt("qscale", "1"),
        )
        .build()
        .unwrap()
        .start()
        .unwrap();

    // The muxer connects → accept fires → the child-buffer verdict arrives. Wait
    // for it (bounded) so the strong assertion is gated on PROVEN backpressure; a
    // missing verdict (never connected) degrades to the weaker assertion.
    let buffers_shrunk = shrunk_rx
        .recv_timeout(Duration::from_secs(5))
        .unwrap_or(false);

    // The tiny buffers fill in milliseconds; this window is startup margin.
    std::thread::sleep(Duration::from_secs(4));

    let (tx, rx) = std::sync::mpsc::channel();
    std::thread::spawn(move || {
        let _ = tx.send(scheduler.stop());
    });

    let stopped = rx.recv_timeout(Duration::from_secs(5));
    assert!(
        stopped.is_ok(),
        "stop() must interrupt a muxer blocked in a network write \
         (interrupt_callback missing on the output context)"
    );
    // The grace-cut ERROR is only guaranteed when we could actually force
    // backpressure (both buffers verifiably shrunk). It pins that the
    // trailer-finalize exemption stays conditioned on a healthy write loop:
    // exempting an errored muxer would push the trailer into the same dead
    // socket and hang this stop() forever. Under seccomp / a non-Linux platform
    // where the resize could not be applied, the muxer may not be blocked and a
    // clean stop() is legitimate — so only assert the error when it was forced.
    if buffers_shrunk {
        assert!(
            stopped.unwrap().is_err(),
            "a stop() that had to cut a blocked network write must report the error"
        );
    }

    drop(accept_thread); // detach; the sleeping peer ends with the process
}

/// Probabilistic lifecycle amplifier, not a deterministic race oracle. Each
/// iteration opens a fresh frame-threaded H.264 decoder and signals stop
/// after one scheduler yield, landing the teardown inside the decoder's
/// startup window (where a get_format callback on an FFmpeg frame-threading
/// worker once raced the worker-thread teardown into a double
/// avcodec_free_context — SIGABRT in pthread_frame.c async_unlock). Passing
/// raises regression-detection probability but carries no statistical
/// guarantee; the deterministic ownership invariant is pinned separately by
/// the dec_task unit test.
#[test]
fn immediate_stop_of_frame_threaded_h264_decode_is_clean() {
    let _lock = PROCESS_LOCK.lock().unwrap_or_else(|e| e.into_inner());

    // 512 iterations: measured ~0.2s for 32 under local ASAN, so this stays
    // seconds-scale in the CI ASAN lane while giving a regression a few
    // hundred independent shots at the startup window; the local acceptance
    // bar is the larger stress loop, not this test.
    for i in 0..512 {
        let scheduler = FfmpegContext::builder()
            .input(
                Input::from("test.mp4")
                    .set_video_codec_opt("threads", "4")
                    .set_video_codec_opt("thread_type", "frame"),
            )
            .output(
                Output::from("-")
                    .set_format("null")
                    .add_stream_map("0:v")
                    .set_video_codec("mpeg4"),
            )
            .build()
            .unwrap()
            .start()
            .unwrap();

        std::thread::yield_now();

        // stop() waits for every tracked worker; a teardown race aborts the
        // whole process, so reaching the next iteration IS the assertion.
        scheduler
            .stop()
            .unwrap_or_else(|e| panic!("iteration {i}: clean immediate stop failed: {e}"));
    }
}
