//! Regression net for a PANIC unwinding out of `start()` after worker
//! threads exist.
//!
//! `start()`'s explicit `Err` returns route through `StartFailGuard`, but the
//! guard's real reason to exist is the path no `return` statement covers: a
//! panic inside one of the init calls while earlier workers are already
//! running. The synchronously reachable panic site driven here is the
//! "input does not need to be sent to the destination" warning in
//! `demux_init` — a user-installed `log::Log` implementation may panic, and
//! by the time the demuxer loop runs, the muxer worker (and input 0's demux
//! worker) are live. Without the guard the unwind would drop
//! `self.ffmpeg_context` — sole owner of the `InterruptState` the workers'
//! AVIO interrupt callbacks dereference through a raw pointer — while those
//! workers still run: the original C1 use-after-free, resurrected through
//! `panic` instead of `Err`.
//!
//! Deterministic join oracle, race-free on both sides. The output writer
//! PARKS every write until the test releases it, and the panicking logger
//! (a) waits for the first write to land — so the mux worker is provably
//! parked inside the callback — then (b) signals `panic_reached` and
//! panics. The no-completion window is measured FROM that signal, not from
//! thread spawn, so a slow start cannot open the writer gate before the
//! panic is even underway; and the completion channel carries the panic
//! payload so a scenario-broken panic (fixture, build, watchdog assert)
//! fails loudly instead of impersonating the guard panic. A correct guard
//! is necessarily still blocked in its join while the writer is parked;
//! an unwind that completes in that window means start() returned without
//! joining a provably-live worker (the exact regression). Releasing the
//! gate lets the join finish and the unwind complete.

mod common;

use common::tmp_path_in;
use std::panic::AssertUnwindSafe;
use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};
use std::sync::mpsc::RecvTimeoutError;
use std::sync::Arc;
use std::time::{Duration, Instant};

use ez_ffmpeg::{FfmpegContext, Input, Output};

const SUBDIR: &str = "ez_ffmpeg_start_panic_tests";

/// The panic message only this test's logger produces; the completion
/// channel checks for it so no other panic can impersonate the scenario.
const GUARD_PANIC_MSG: &str = "test logger: panicking on the demux skip warning";

/// Sets the flag from its `Drop` — which, declared right before the
/// `panic!`, runs DURING the unwind. The test observing the flag therefore
/// proves the panic has genuinely started (a plain store before `panic!`
/// would leave a suspend window in which the flag is visible but no panic
/// is underway yet).
struct SignalOnUnwind(Arc<AtomicBool>);

impl Drop for SignalOnUnwind {
    fn drop(&mut self) {
        self.0.store(true, Ordering::SeqCst);
    }
}

/// Panics on the demux-skip warning under test — but only after the paced
/// writer has accepted its first write, so the mux worker is provably parked
/// inside the write callback when the unwind starts; `panic_reached` is
/// published by a drop-guard during the unwind itself, so the test's
/// no-completion window can only start once the panic is real. Every other
/// record is ignored. Installed only AFTER the fixture encode so unrelated
/// warnings cannot trip it.
struct PanicOnSkipWarning {
    writes: Arc<AtomicUsize>,
    panic_reached: Arc<AtomicBool>,
}

impl log::Log for PanicOnSkipWarning {
    fn enabled(&self, _: &log::Metadata) -> bool {
        true
    }

    fn log(&self, record: &log::Record) {
        if !record
            .args()
            .to_string()
            .contains("does not need to be sent")
        {
            return;
        }
        // Runs on the start() thread; the mux worker writes concurrently.
        let deadline = Instant::now() + Duration::from_secs(10);
        while self.writes.load(Ordering::SeqCst) == 0 {
            assert!(
                Instant::now() < deadline,
                "mux worker never reached its first write; scenario broken"
            );
            std::thread::sleep(Duration::from_millis(2));
        }
        // Dropped by the unwind the next statement starts: the flag becomes
        // visible only once the panic is actually in progress.
        let _signal = SignalOnUnwind(self.panic_reached.clone());
        panic!("{GUARD_PANIC_MSG}");
    }

    fn flush(&self) {}
}

/// Single test on purpose: the panicking logger is process-global and must
/// not interleave with other scenarios in this binary.
#[test]
fn start_panic_after_workers_spawned_joins_workers_and_propagates() {
    // Build the fixture BEFORE installing the panicking logger.
    let fixture = tmp_path_in(SUBDIR, "src.mp4");
    {
        let running = FfmpegContext::builder()
            .input(Input::from("testsrc=size=320x240:rate=30:duration=1").set_format("lavfi"))
            .output(
                Output::from(fixture.as_str())
                    .set_video_codec("mpeg4")
                    .set_max_video_frames(30),
            )
            .build()
            .expect("fixture context must build")
            .start()
            .expect("fixture job must start");
        running.wait().expect("fixture encode must finish");
    }

    let writes = Arc::new(AtomicUsize::new(0));
    let release = Arc::new(AtomicBool::new(false));
    let panic_reached = Arc::new(AtomicBool::new(false));

    log::set_boxed_logger(Box::new(PanicOnSkipWarning {
        writes: writes.clone(),
        panic_reached: panic_reached.clone(),
    }))
    .expect("logger installs once");
    log::set_max_level(log::LevelFilter::Warn);

    // Input 0 is mapped; input 1 has no destination, so `demux_init` emits
    // the skip warning — with the manual stream map, auto-mapping never
    // claims input 1's streams. The copy-only output is ready at start(), so
    // its muxer worker is spawned in the muxer loop, well before the demuxer
    // loop reaches input 1; the logger then waits for the worker's first
    // (parked) write before panicking.
    let writes_cb = writes.clone();
    let release_cb = release.clone();
    let fixture_for_job = fixture.clone();
    let (tx, rx) = std::sync::mpsc::channel();
    std::thread::spawn(move || {
        let outcome = std::panic::catch_unwind(AssertUnwindSafe(|| {
            FfmpegContext::builder()
                .input(Input::from(fixture_for_job.as_str()))
                .input(Input::from(fixture_for_job.as_str()))
                .output(
                    Output::new_by_write_callback(move |data: &[u8]| -> i32 {
                        writes_cb.fetch_add(1, Ordering::SeqCst);
                        while !release_cb.load(Ordering::SeqCst) {
                            std::thread::sleep(Duration::from_millis(5));
                        }
                        data.len() as i32
                    })
                    // Streamable container: a custom-IO writer without a seek
                    // callback cannot host mp4's header back-patching.
                    .set_format("mpegts")
                    .add_stream_map_with_copy("0:v"),
                )
                .build()
                .expect("two-input copy context must build")
                .start()
        }));
        // By the time catch_unwind returns, the unwind has completed: the
        // StartFailGuard joined every worker BEFORE the FfmpegContext (and
        // its InterruptState) dropped. Report WHICH panic unwound so a
        // scenario-broken panic cannot impersonate the logger's.
        let verdict = match outcome {
            Ok(_) => "completed-without-panic".to_string(),
            Err(payload) => payload
                .downcast_ref::<String>()
                .map(|s| s.as_str())
                .or_else(|| payload.downcast_ref::<&str>().copied())
                .unwrap_or("non-string panic payload")
                .to_string(),
        };
        let _ = tx.send(verdict);
    });

    // Wait for the logger to reach its panic point (the mux worker is parked
    // in the write callback by construction at that moment). Only THEN does
    // the no-completion window below measure the guard's join.
    let deadline = Instant::now() + Duration::from_secs(30);
    while !panic_reached.load(Ordering::SeqCst) {
        assert!(
            Instant::now() < deadline,
            "the demux skip warning never fired (scenario broken — did the \
             message change?)"
        );
        std::thread::sleep(Duration::from_millis(5));
    }

    // Join oracle, part 1: from the panic point, the unwind must NOT be able
    // to complete while the writer stays parked — a correct guard is blocked
    // in its join on the parked mux worker. Completion here means start()
    // returned without joining a provably-live worker.
    match rx.recv_timeout(Duration::from_secs(2)) {
        Err(RecvTimeoutError::Timeout) => { /* join correctly waiting on the parked worker */ }
        Ok(v) => panic!(
            "start()'s unwind completed (\"{v}\") while the muxer worker was \
             still parked inside the write callback: the failure guard did \
             not join the workers before the context dropped"
        ),
        Err(RecvTimeoutError::Disconnected) => {
            unreachable!("worker thread always sends before exiting")
        }
    }

    // Join oracle, part 2: release the writer; the worker finishes its
    // write, observes the terminal status the guard published, and exits;
    // the join — and with it the unwind — completes, carrying the logger's
    // own panic payload.
    release.store(true, Ordering::SeqCst);
    match rx.recv_timeout(Duration::from_secs(60)) {
        Ok(v) if v == GUARD_PANIC_MSG => { /* panicked, joined, unwound: expected */ }
        Ok(v) => panic!(
            "start() ended with an unexpected outcome \"{v}\" instead of the \
             logger's panic (scenario broken)"
        ),
        Err(RecvTimeoutError::Timeout) => panic!(
            "start() panic did not unwind within 60s of releasing the \
             writer: the failure guard hung joining a worker"
        ),
        Err(RecvTimeoutError::Disconnected) => {
            unreachable!("worker thread always sends before exiting")
        }
    }
}
