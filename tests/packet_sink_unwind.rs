//! Packet-sink UNWIND teardown ordering: panic injection into the two
//! pre-worker frames that own the consumer callbacks — the delayed-start
//! waiter and `_mux_init` — via a panicking `log::Log` implementation (the
//! same seam `start_panic_unwind.rs` established: a user-installed logger
//! may panic inside any worker's log call).
//!
//! What each scenario proves: the panic is published as the job error
//! (`WorkerPanicked`), the job terminates without hanging (the queue
//! receivers close and the encoders join), and the consumer's callback
//! captures are destroyed BEFORE `wait()` returns — the defined destruction
//! point holds on the unwind paths, not just the explicit ones.
//!
//! The logger is process-global, so this binary contains ONLY these
//! serialized scenarios. Native AAC keeps them running on GPL-free CI.

mod common;

use common::wait_with_watchdog;
use ez_ffmpeg::error::Error;
use ez_ffmpeg::packet_sink::PacketSink;
use ez_ffmpeg::{FfmpegContext, Input, Output};
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, Mutex, OnceLock};

/// The armed marker: when a log record contains it, the logger panics on the
/// logging thread (disarming first, so unwind-path logs cannot re-trigger).
static TRIGGER: Mutex<Option<&'static str>> = Mutex::new(None);

/// The panicking logger is process-global: scenarios must not interleave.
static PROCESS_LOCK: Mutex<()> = Mutex::new(());

struct PanicOnMarker;

impl log::Log for PanicOnMarker {
    fn enabled(&self, _: &log::Metadata) -> bool {
        true
    }

    fn log(&self, record: &log::Record) {
        let needle = *TRIGGER.lock().unwrap_or_else(|e| e.into_inner());
        if let Some(needle) = needle {
            if record.args().to_string().contains(needle) {
                *TRIGGER.lock().unwrap_or_else(|e| e.into_inner()) = None;
                panic!("test logger: injected panic on '{needle}'");
            }
        }
    }

    fn flush(&self) {}
}

fn install_logger_once() {
    static INSTALLED: OnceLock<()> = OnceLock::new();
    INSTALLED.get_or_init(|| {
        log::set_boxed_logger(Box::new(PanicOnMarker)).expect("logger installs once");
        log::set_max_level(log::LevelFilter::Debug);
    });
}

/// Sets its flag from `Drop` — observed AFTER `wait()` returns, it proves the
/// callback captures were destroyed before the job reported completion.
struct FlagOnDrop(Arc<AtomicBool>);

impl Drop for FlagOnDrop {
    fn drop(&mut self) {
        self.0.store(true, Ordering::Release);
    }
}

fn run_unwind_scenario(needle: &'static str, scenario: &str) {
    let _lock = PROCESS_LOCK.lock().unwrap_or_else(|e| e.into_inner());
    install_logger_once();

    let destroyed = Arc::new(AtomicBool::new(false));
    let capture = FlagOnDrop(destroyed.clone());
    let sink = PacketSink::builder(move |_pkt| {
        let _hold = &capture;
        Ok(())
    })
    .build();

    *TRIGGER.lock().unwrap_or_else(|e| e.into_inner()) = Some(needle);
    let scheduler = FfmpegContext::builder()
        .input(Input::from("sine=frequency=440:duration=2").set_format("lavfi"))
        .output(Output::from(sink).set_audio_codec("aac"))
        .build()
        .unwrap()
        .start()
        .unwrap();
    let result = wait_with_watchdog(scheduler, 60, scenario);
    *TRIGGER.lock().unwrap_or_else(|e| e.into_inner()) = None;

    match result {
        Err(Error::WorkerPanicked(_)) => {}
        other => panic!("{scenario}: expected WorkerPanicked, got {other:?}"),
    }
    assert!(
        destroyed.load(Ordering::Acquire),
        "{scenario}: callback captures must be destroyed before wait() returns"
    );
}

/// Unwind of the delayed-start WAITER frame: the panic fires on the
/// stream-ready debug message, while the waiter still owns the consumer
/// bundle (before the all-ready handoff takes it).
#[test]
fn waiter_unwind_destroys_captures_and_publishes_the_panic() {
    run_unwind_scenario("is readied", "waiter_unwind");
}

/// Unwind of the `_mux_init` frame: the panic fires on the S1 collection
/// debug message, after the worker state (owning the callbacks) was placed
/// in the handoff slot and before the mux worker spawned.
#[test]
fn mux_init_unwind_destroys_captures_and_publishes_the_panic() {
    run_unwind_scenario("configuration collected", "mux_init_unwind");
}
