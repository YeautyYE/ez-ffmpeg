//! Tripwires for hostile `log::Log` backends against the crate's global
//! init paths.
//!
//! Invariant under test: no log macro runs while `INIT_FFMPEG`'s `Once` is
//! executing or while a hardware-device init lock is held. A logger backend
//! is arbitrary user code — it may panic, block, or call back into
//! ez-ffmpeg — so emitting under those primitives turns a logging quirk
//! into a poisoned `Once` (every later FFmpeg entry point fails) or a
//! same-thread deadlock.
//!
//! One `#[test]` runs both scenarios IN ORDER: FFmpeg init is process-once
//! and `log::set_logger` is once-per-process (a test binary installs at
//! most ONE logger — see tests/common/mod.rs), so the scenarios must share
//! a process, a logger, and a fixed sequence.

use ez_ffmpeg::core::context::filter_complex::FilterComplex;
use ez_ffmpeg::{FfmpegContext, Input, Output};
use log::{LevelFilter, Metadata, Record};
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::mpsc;
use std::time::Duration;

/// Programmable logger: behaviors key on exact message needles and arm via
/// one-shot `AtomicBool` swaps, so a triggered behavior can never recurse
/// into itself.
struct HookLogger;

/// Scenario A: panic on the exact init message (a logger backend is
/// allowed to panic; the crate must survive it).
static PANIC_ON_INIT_MESSAGE: AtomicBool = AtomicBool::new(false);
/// Scenario A: proof the armed message was actually seen, so the assertion
/// cannot silently pass on message drift.
static SAW_INIT_MESSAGE: AtomicBool = AtomicBool::new(false);

/// Scenario B: re-enter the crate from inside the logger when the outer
/// device-init failure message arrives.
static REENTER_ON_OUTER_FAILURE: AtomicBool = AtomicBool::new(false);
static SAW_OUTER_FAILURE: AtomicBool = AtomicBool::new(false);
/// Scenario B: proof the re-entrant inner call returned at all.
static INNER_CALL_COMPLETED: AtomicBool = AtomicBool::new(false);

impl log::Log for HookLogger {
    fn enabled(&self, _metadata: &Metadata) -> bool {
        true
    }

    fn log(&self, record: &Record) {
        let message = record.args().to_string();

        if message == "FFmpeg initialized." && PANIC_ON_INIT_MESSAGE.swap(false, Ordering::SeqCst) {
            SAW_INIT_MESSAGE.store(true, Ordering::SeqCst);
            panic!("logger backend panics on the FFmpeg init message");
        }

        if message.starts_with("Invalid device specification")
            && message.contains("ezffmpeg_bogus_outer")
            && REENTER_ON_OUTER_FAILURE.swap(false, Ordering::SeqCst)
        {
            SAW_OUTER_FAILURE.store(true, Ordering::SeqCst);
            // A logger backend that calls back into the crate: build (and
            // fail) a second pipeline whose filter graph needs its own
            // hardware-device init. When the outer failure was logged with
            // the device-init lock still held, this call re-entered
            // hw_device_init_from_string on the SAME thread and
            // self-deadlocked. The one-shot arming above keeps the inner
            // build's own failure message from recursing further.
            let _ = build_bogus_hw_device_pipeline("ezffmpeg_bogus_inner", "reentrant_inner.mp4");
            INNER_CALL_COMPLETED.store(true, Ordering::SeqCst);
        }
    }

    fn flush(&self) {}
}

static LOGGER: HookLogger = HookLogger;

fn tmp_path(name: &str) -> String {
    let dir = std::env::temp_dir().join(format!(
        "ez_ffmpeg_logger_reentrancy_tests_{}",
        std::process::id()
    ));
    std::fs::create_dir_all(&dir).unwrap();
    dir.join(name).to_string_lossy().into_owned()
}

/// A build whose filter graph asks for a nonexistent hardware device type:
/// device init happens at build() time, so no hardware and no start() are
/// needed, and the build must return Err after logging the failure.
fn build_bogus_hw_device_pipeline(
    hw_device: &str,
    out_name: &str,
) -> ez_ffmpeg::error::Result<FfmpegContext> {
    let out = tmp_path(out_name);
    FfmpegContext::builder()
        .input(Input::from("color=c=blue:s=320x240:r=30:duration=1").set_format("lavfi"))
        .filter_desc(FilterComplex::from("format=rgb24").set_hw_device(hw_device))
        .output(
            Output::from(out.as_str())
                .set_video_codec("mpeg4")
                .set_max_video_frames(1),
        )
        .build()
}

#[test]
fn logger_backends_cannot_poison_init_or_deadlock_device_setup() {
    log::set_logger(&LOGGER).expect("this binary installs exactly one logger");
    log::set_max_level(LevelFilter::Info);

    // ---- Scenario A: a panicking logger must not poison FFmpeg init ----

    PANIC_ON_INIT_MESSAGE.store(true, Ordering::SeqCst);
    let first =
        std::panic::catch_unwind(|| ez_ffmpeg::capabilities::is_muxer_available("matroska"));
    assert!(
        SAW_INIT_MESSAGE.load(Ordering::SeqCst),
        "the \"FFmpeg initialized.\" message never reached the logger — \
         the panic arm was not exercised (message drift?)"
    );
    assert!(
        first.is_err(),
        "the armed logger panic must propagate out of the first call"
    );

    // The Once must have COMPLETED despite the logger panic: every later
    // FFmpeg entry point still works. (When the message was emitted inside
    // call_once, the unwind poisoned the Once and this second call
    // panicked instead of answering.)
    assert!(
        ez_ffmpeg::capabilities::is_muxer_available("matroska"),
        "FFmpeg init must survive a panicking logger backend"
    );

    // ---- Scenario B: a re-entrant logger must not deadlock device init ----

    REENTER_ON_OUTER_FAILURE.store(true, Ordering::SeqCst);
    let (tx, rx) = mpsc::channel();
    std::thread::spawn(move || {
        let _ = tx.send(
            build_bogus_hw_device_pipeline("ezffmpeg_bogus_outer", "reentrant_outer.mp4")
                .map(|_| ()),
        );
    });
    // When the failure message was emitted with the device-init lock still
    // held, the logger's inner build blocked forever on that same lock and
    // only this watchdog reported it; with the log deferred past the guard
    // drop, both builds fail fast.
    let outer = match rx.recv_timeout(Duration::from_secs(60)) {
        Ok(result) => result,
        Err(mpsc::RecvTimeoutError::Timeout) => {
            panic!("re-entrant logger deadlocked hardware-device init (60s watchdog)")
        }
        Err(mpsc::RecvTimeoutError::Disconnected) => {
            panic!("outer build thread panicked before reporting")
        }
    };

    assert!(outer.is_err(), "a bogus hw device spec must fail the build");
    assert!(
        SAW_OUTER_FAILURE.load(Ordering::SeqCst),
        "the outer \"Invalid device specification\" message never reached \
         the logger (message drift?)"
    );
    assert!(
        INNER_CALL_COMPLETED.load(Ordering::SeqCst),
        "the logger's re-entrant build must run to completion"
    );
}
