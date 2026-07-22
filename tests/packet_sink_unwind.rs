//! Packet-sink probes driven by a process-global test logger (the seam
//! `start_panic_unwind.rs` established: a user-installed logger runs inside
//! worker log calls and may panic — or, here, also deliberately block).
//!
//! Three probe families share the logger:
//! - UNWIND ordering: panic injection into the two pre-worker frames that
//!   own the consumer callbacks (the delayed-start waiter and `_mux_init`),
//!   with a Drop-side observable proving the panic is PUBLISHED before the
//!   callback captures are destroyed;
//! - terminal settlement: a late filter-worker panic (its final log fires
//!   after the sink drained) must prevent `on_end`, and the terminal
//!   region's panic compositions (capture-`Drop` + logger; terminal
//!   callback + logger + unconditional capture-`Drop`; logger on the
//!   terminal-entry record itself) must neither un-settle the delivered
//!   result nor abort the process;
//! - the linearization window: an abort landing while the worker is held
//!   between its last packet completion and the terminal region suppresses
//!   both terminal callbacks.
//!
//! A fourth family runs in CHILD processes: panic-source compositions whose
//! pre-containment failure mode is a panic-during-unwind process ABORT
//! (SIGABRT). In-process they would kill this whole binary, and no
//! in-process assertion can observe "did not abort" — so the parent test
//! re-execs this binary filtered to a dispatcher test and asserts the
//! child's exit status.
//!
//! The logger is process-global, so this binary contains ONLY these
//! serialized scenarios. Native AAC keeps them running on GPL-free CI.

mod common;

use common::wait_with_watchdog;
use ez_ffmpeg::error::Error;
use ez_ffmpeg::packet_sink::PacketSink;
use ez_ffmpeg::{FfmpegContext, Input, Output};
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{mpsc, Arc, Mutex, OnceLock};
use std::time::{Duration, Instant};

/// The armed panic marker: when a log record contains it, the logger panics
/// on the logging thread (disarming first, so unwind-path logs cannot
/// re-trigger).
static TRIGGER: Mutex<Option<&'static str>> = Mutex::new(None);

/// The armed blocking marker: when a log record contains it, the logger
/// reports `HELD` and spins until `RELEASE` — parking the logging thread at
/// that exact statement (disarming first, so later records pass through).
static HOLD: Mutex<Option<&'static str>> = Mutex::new(None);
static HELD: AtomicBool = AtomicBool::new(false);
static RELEASE: AtomicBool = AtomicBool::new(false);

/// Abort-store observation latch. While `WATCH_ARMED`, a container muxer's
/// abort-specific records set `ABORT_OBSERVED`: "Muxer detected abort" and
/// "Muxer skipping trailer due to abort" are each logged strictly after an
/// acquire load of the scheduler status returned the ABORT value on that
/// worker's thread (the trailer gate logs the latter on every abort-path
/// container-mux teardown, and abort is never downgraded). The store being
/// waited for therefore happens-before the latch, so `RELEASE` chained
/// through it is ordered after the store — an observed handshake, not a
/// timing guess. A packet sink's terminal path returns before the trailer
/// gate, so only a container output can emit the records.
static WATCH_ARMED: AtomicBool = AtomicBool::new(false);
static ABORT_OBSERVED: AtomicBool = AtomicBool::new(false);

/// The injecting logger is process-global: scenarios must not interleave.
static PROCESS_LOCK: Mutex<()> = Mutex::new(());

struct InjectingLogger;

impl log::Log for InjectingLogger {
    fn enabled(&self, _: &log::Metadata) -> bool {
        true
    }

    fn log(&self, record: &log::Record) {
        if WATCH_ARMED.load(Ordering::Acquire) {
            let msg = record.args().to_string();
            if msg.contains("Muxer detected abort")
                || msg.contains("Muxer skipping trailer due to abort")
            {
                ABORT_OBSERVED.store(true, Ordering::Release);
            }
        }
        let hold = *HOLD.lock().unwrap_or_else(|e| e.into_inner());
        if let Some(needle) = hold {
            if record.args().to_string().contains(needle) {
                *HOLD.lock().unwrap_or_else(|e| e.into_inner()) = None;
                HELD.store(true, Ordering::Release);
                while !RELEASE.load(Ordering::Acquire) {
                    std::thread::sleep(Duration::from_millis(2));
                }
            }
        }
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
        log::set_boxed_logger(Box::new(InjectingLogger)).expect("logger installs once");
        // Trace level: the linearization-window probe parks on a
        // trace-level record.
        log::set_max_level(log::LevelFilter::Trace);
    });
}

/// Disarms every injection marker (each scenario arms exactly what it needs).
fn disarm_all() {
    *TRIGGER.lock().unwrap_or_else(|e| e.into_inner()) = None;
    *HOLD.lock().unwrap_or_else(|e| e.into_inner()) = None;
    HELD.store(false, Ordering::Release);
    RELEASE.store(false, Ordering::Release);
    WATCH_ARMED.store(false, Ordering::Release);
    ABORT_OBSERVED.store(false, Ordering::Release);
}

/// Arms with a HOLD scenario and guarantees the parked worker is released
/// even when the test thread panics before its manual `RELEASE` (otherwise
/// the scheduler teardown would wait forever on the parked thread and hang
/// the whole binary). The manual release remains the ordering-relevant one;
/// this late store is idempotent.
struct ReleaseOnDrop;

impl Drop for ReleaseOnDrop {
    fn drop(&mut self) {
        RELEASE.store(true, Ordering::Release);
    }
}

/// Sets its flag from `Drop` — observed AFTER `wait()` returns, it proves the
/// callback captures were destroyed before the job reported completion.
struct FlagOnDrop(Arc<AtomicBool>);

impl Drop for FlagOnDrop {
    fn drop(&mut self) {
        self.0.store(true, Ordering::Release);
    }
}

/// Publisher-order observable: its `Drop` handshakes with the test thread,
/// which samples `scheduler.is_ended()` WHILE the destructor is suspended.
/// The panic publisher stores the terminal status, so the sampled value is
/// exactly "did the publisher run before this capture began destruction".
struct OrderProbe {
    destroyed: Arc<AtomicBool>,
    published_at_drop: Arc<AtomicBool>,
    in_drop_tx: mpsc::Sender<()>,
    verdict_rx: mpsc::Receiver<bool>,
}

impl Drop for OrderProbe {
    fn drop(&mut self) {
        let _ = self.in_drop_tx.send(());
        if let Ok(published) = self.verdict_rx.recv_timeout(Duration::from_secs(10)) {
            self.published_at_drop.store(published, Ordering::Release);
        }
        self.destroyed.store(true, Ordering::Release);
    }
}

fn run_publisher_order_scenario(needle: &'static str, scenario: &str) {
    let _lock = PROCESS_LOCK.lock().unwrap_or_else(|e| e.into_inner());
    install_logger_once();
    disarm_all();

    let destroyed = Arc::new(AtomicBool::new(false));
    let published_at_drop = Arc::new(AtomicBool::new(false));
    let (in_drop_tx, in_drop_rx) = mpsc::channel();
    let (verdict_tx, verdict_rx) = mpsc::channel();
    let probe = OrderProbe {
        destroyed: destroyed.clone(),
        published_at_drop: published_at_drop.clone(),
        in_drop_tx,
        verdict_rx,
    };
    let sink = PacketSink::builder(move |_pkt| {
        let _hold = &probe;
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

    // The probe's Drop is now suspended mid-destructor (if/when it begins);
    // sample the publisher's observable effect at that exact moment. On the
    // repaired frames the publisher (which stores the terminal status) runs
    // BEFORE any capture is destroyed, so the sample must read `true`.
    match in_drop_rx.recv_timeout(Duration::from_secs(30)) {
        Ok(()) => {
            let _ = verdict_tx.send(scheduler.is_ended());
        }
        Err(e) => panic!("{scenario}: capture Drop never began: {e:?}"),
    }

    let result = wait_with_watchdog(scheduler, 60, scenario);
    disarm_all();

    match result {
        Err(Error::WorkerPanicked(_)) => {}
        other => panic!("{scenario}: expected WorkerPanicked, got {other:?}"),
    }
    assert!(
        destroyed.load(Ordering::Acquire),
        "{scenario}: callback captures must be destroyed before wait() returns"
    );
    assert!(
        published_at_drop.load(Ordering::Acquire),
        "{scenario}: the panic must be PUBLISHED (terminal status stored) before the callback captures are destroyed"
    );
}

/// Unwind of the delayed-start WAITER frame: the panic fires on the
/// stream-ready debug message, while the waiter still owns the consumer
/// bundle (before the all-ready handoff takes it).
#[test]
fn waiter_unwind_publishes_the_panic_before_destroying_captures() {
    run_publisher_order_scenario("is readied", "waiter_unwind");
}

/// Unwind of the `_mux_init` frame: the panic fires on the S1 collection
/// debug message, which runs AFTER ownership reached the ordered
/// `sink_worker_slot` local (and before the mux worker spawned) — so the
/// unwind destroys the captures in publisher-then-captures order.
#[test]
fn mux_init_unwind_publishes_the_panic_before_destroying_captures() {
    run_publisher_order_scenario("configuration collected", "mux_init_unwind");
}

/// Panics from `Drop` unless the thread is already unwinding (never
/// double-panics), recording that destruction began.
struct PanicOnDrop(Arc<AtomicBool>);

impl Drop for PanicOnDrop {
    fn drop(&mut self) {
        self.0.store(true, Ordering::Release);
        if !std::thread::panicking() {
            panic!("injected capture-destructor panic");
        }
    }
}

/// The post-terminal containment: a capture-`Drop` panic (caught) triggers
/// the failure log, and a user-installed logger can panic INSIDE that log
/// call — the composition must still not disturb the settled job result a
/// delivered `on_end` promised.
#[test]
fn on_end_survives_capture_drop_panic_composed_with_logger_panic() {
    let _lock = PROCESS_LOCK.lock().unwrap_or_else(|e| e.into_inner());
    install_logger_once();
    disarm_all();

    let events: Arc<Mutex<Vec<String>>> = Arc::new(Mutex::new(Vec::new()));
    let destroyed = Arc::new(AtomicBool::new(false));
    let bomb = PanicOnDrop(destroyed.clone());
    let (ev_end, ev_err) = (events.clone(), events.clone());
    let sink = PacketSink::builder(move |_pkt| {
        let _hold = &bomb;
        Ok(())
    })
    .on_end(move || ev_end.lock().unwrap().push("end".to_string()))
    .on_delivery_error(move |e| ev_err.lock().unwrap().push(format!("error: {e}")))
    .build();

    // Arm the logger on the containment's own failure log.
    *TRIGGER.lock().unwrap_or_else(|e| e.into_inner()) = Some("panicked during teardown");
    let scheduler = FfmpegContext::builder()
        .input(Input::from("sine=frequency=440:duration=1").set_format("lavfi"))
        .output(Output::from(sink).set_audio_codec("aac"))
        .build()
        .unwrap()
        .start()
        .unwrap();
    let result = wait_with_watchdog(scheduler, 60, "drop_panic_plus_logger_panic");

    // The containment log fired (and panicked): the needle was consumed.
    assert!(
        TRIGGER
            .lock()
            .unwrap_or_else(|e| e.into_inner())
            .is_none(),
        "the containment failure log must have fired and hit the armed logger"
    );
    disarm_all();
    assert!(
        result.is_ok(),
        "a capture-Drop panic composed with a logger panic must not fail the settled job: {result:?}"
    );
    assert!(destroyed.load(Ordering::Acquire));
    let evs = events.lock().unwrap_or_else(|e| e.into_inner()).clone();
    assert_eq!(
        evs,
        vec!["end".to_string()],
        "exactly one on_end and no error callback"
    );
}

/// Panics from `Drop` unconditionally — even on a thread that is already
/// unwinding, where the second panic aborts the process. This is the
/// hostile-destructor case the terminal teardown order must make safe: the
/// sink may only be dropped inside its own catch on a non-unwinding thread,
/// never as a closure capture while a logger panic unwinds past it.
struct AlwaysPanicOnDrop(Arc<AtomicBool>);

impl Drop for AlwaysPanicOnDrop {
    fn drop(&mut self) {
        self.0.store(true, Ordering::Release);
        panic!("injected unconditional capture-destructor panic");
    }
}

/// The full terminal-region composition: the terminal callback (`on_end`)
/// panics, its failure log hits a panicking logger, AND a capture
/// destructor panics unconditionally. The teardown order must keep every
/// unwind single — finish caught first, the sink consumed under its own
/// catch BEFORE any failure logging, logs last — because a sink still owned
/// when the logger panic unwinds would run the capture destructor
/// mid-unwind: a panic-during-unwind process abort. The test completing at
/// all proves no abort; the Ok result and the single delivered `on_end`
/// prove the settled job result survived the composition.
#[test]
fn settled_result_survives_finish_panic_logger_panic_and_drop_panic() {
    let _lock = PROCESS_LOCK.lock().unwrap_or_else(|e| e.into_inner());
    install_logger_once();
    disarm_all();

    let events: Arc<Mutex<Vec<String>>> = Arc::new(Mutex::new(Vec::new()));
    let destroyed = Arc::new(AtomicBool::new(false));
    let bomb = AlwaysPanicOnDrop(destroyed.clone());
    let (ev_end, ev_err) = (events.clone(), events.clone());
    let sink = PacketSink::builder(move |_pkt| {
        let _hold = &bomb;
        Ok(())
    })
    .on_end(move || {
        ev_end.lock().unwrap().push("end".to_string());
        panic!("injected terminal-callback panic");
    })
    .on_delivery_error(move |e| ev_err.lock().unwrap().push(format!("error: {e}")))
    .build();

    // Arm the logger on the terminal-callback failure log: the logger panic
    // then unwinds from inside the containment's logging tail, after the
    // sink must already have been consumed.
    *TRIGGER.lock().unwrap_or_else(|e| e.into_inner()) = Some("terminal callback panicked");
    let scheduler = FfmpegContext::builder()
        .input(Input::from("sine=frequency=440:duration=1").set_format("lavfi"))
        .output(Output::from(sink).set_audio_codec("aac"))
        .build()
        .unwrap()
        .start()
        .unwrap();
    let result = wait_with_watchdog(scheduler, 60, "finish_logger_drop_panic_composition");

    // The terminal-callback failure log fired (and panicked): the needle
    // was consumed.
    assert!(
        TRIGGER
            .lock()
            .unwrap_or_else(|e| e.into_inner())
            .is_none(),
        "the terminal-callback failure log must have fired and hit the armed logger"
    );
    disarm_all();
    assert!(
        result.is_ok(),
        "a terminal-callback panic composed with a logger panic and a capture-destructor \
         panic must not fail the settled job: {result:?}"
    );
    assert!(
        destroyed.load(Ordering::Acquire),
        "the sink captures must be destroyed before wait() returns"
    );
    let evs = events.lock().unwrap_or_else(|e| e.into_inner()).clone();
    assert_eq!(
        evs,
        vec!["end".to_string()],
        "exactly one on_end (delivered before its panic) and no error callback"
    );
}

/// The terminal-ENTRY record: the sink is already taken for the terminal
/// region when "Packet sink muxer finished." is logged, and a user logger
/// panicking on that exact record used to unwind PAST the terminal
/// dispatch — a healthy, fully drained job delivered no `on_end`, its
/// captures were destroyed mid-unwind, and `wait()` repainted the settled
/// completion as a worker panic. The entry log now runs under its own
/// containment, so the terminal dispatch must still run: the delivered
/// `on_end` and the Ok result are the observables.
#[test]
fn terminal_entry_logger_panic_still_delivers_on_end() {
    let _lock = PROCESS_LOCK.lock().unwrap_or_else(|e| e.into_inner());
    install_logger_once();
    disarm_all();

    let events: Arc<Mutex<Vec<String>>> = Arc::new(Mutex::new(Vec::new()));
    let destroyed = Arc::new(AtomicBool::new(false));
    let capture = FlagOnDrop(destroyed.clone());
    let (ev_end, ev_err) = (events.clone(), events.clone());
    let sink = PacketSink::builder(move |_pkt| {
        let _hold = &capture;
        Ok(())
    })
    .on_end(move || ev_end.lock().unwrap().push("end".to_string()))
    .on_delivery_error(move |e| ev_err.lock().unwrap().push(format!("error: {e}")))
    .build();

    *TRIGGER.lock().unwrap_or_else(|e| e.into_inner()) = Some("Packet sink muxer finished.");
    let scheduler = FfmpegContext::builder()
        .input(Input::from("sine=frequency=440:duration=1").set_format("lavfi"))
        .output(Output::from(sink).set_audio_codec("aac"))
        .build()
        .unwrap()
        .start()
        .unwrap();
    let result = wait_with_watchdog(scheduler, 60, "terminal_entry_logger_panic");

    assert!(
        TRIGGER
            .lock()
            .unwrap_or_else(|e| e.into_inner())
            .is_none(),
        "the terminal-entry log must have fired and hit the armed logger"
    );
    disarm_all();
    assert!(
        result.is_ok(),
        "a logger panic on the terminal-entry record must not fail the drained job: {result:?}"
    );
    assert!(
        destroyed.load(Ordering::Acquire),
        "the sink captures must be destroyed before wait() returns"
    );
    let evs = events.lock().unwrap_or_else(|e| e.into_inner()).clone();
    assert_eq!(
        evs,
        vec!["end".to_string()],
        "the terminal dispatch must still run after the contained entry-log panic"
    );
}

/// Scenario selector for the child-process probes: inert without it, so the
/// dispatcher test is a no-op in a normal full run of this binary.
const CHILD_SCENARIO_ENV: &str = "EZ_FFMPEG_SINK_DISPOSAL_CHILD";

/// Runs one hostile-destructor scenario in a CHILD process and asserts the
/// child exits cleanly. These scenarios' pre-containment failure mode is a
/// panic-during-unwind process ABORT — in-process they would kill the whole
/// binary, and an abort is not observable from inside the aborting process.
/// The exit status carries the verdict (on Unix an abort reports SIGABRT).
fn run_child_scenario(scenario: &str) {
    let exe = std::env::current_exe().expect("test binary path");
    let out = std::process::Command::new(&exe)
        .arg("sink_disposal_child")
        .arg("--exact")
        .arg("--nocapture")
        .env(CHILD_SCENARIO_ENV, scenario)
        .output()
        .expect("failed to spawn the child probe process");
    assert!(
        out.status.success(),
        "child scenario '{scenario}' failed — a non-zero/signal exit here means a \
         destructor panic escaped containment: status={:?}\nstdout:\n{}\nstderr:\n{}",
        out.status,
        String::from_utf8_lossy(&out.stdout),
        String::from_utf8_lossy(&out.stderr),
    );
}

/// Child-process dispatcher: runs the scenario named by the environment
/// variable, nothing otherwise.
#[test]
fn sink_disposal_child() {
    match std::env::var(CHILD_SCENARIO_ENV).ok().as_deref() {
        None => {}
        Some("two_destructor_bombs") => child_two_destructor_bombs(),
        Some("panicking_payload_bomb") => child_panicking_payload_bomb(),
        Some("delivery_error_source_bomb") => child_delivery_error_source_bomb(),
        Some(other) => panic!("unknown child scenario '{other}'"),
    }
}

/// TWO unconditional capture bombs in DIFFERENT callback boxes of the same
/// sink. One catch around a plain drop of the aggregate contains only the
/// first bomb: the unwind it stops still destroyed the remaining boxes
/// mid-flight, where the second bomb is a panic-during-unwind abort. The
/// disposal must instead destroy each box under its own catch — the child
/// surviving to assert is the point.
fn child_two_destructor_bombs() {
    let events: Arc<Mutex<Vec<String>>> = Arc::new(Mutex::new(Vec::new()));
    let destroyed_first = Arc::new(AtomicBool::new(false));
    let destroyed_second = Arc::new(AtomicBool::new(false));
    let bomb_first = AlwaysPanicOnDrop(destroyed_first.clone());
    let bomb_second = AlwaysPanicOnDrop(destroyed_second.clone());
    let (ev_end, ev_err) = (events.clone(), events.clone());
    let sink = PacketSink::builder(move |_pkt| {
        let _hold = &bomb_first;
        Ok(())
    })
    .on_end(move || ev_end.lock().unwrap().push("end".to_string()))
    .on_delivery_error(move |e| {
        let _hold = &bomb_second;
        ev_err.lock().unwrap().push(format!("error: {e}"))
    })
    .build();

    let scheduler = FfmpegContext::builder()
        .input(Input::from("sine=frequency=440:duration=1").set_format("lavfi"))
        .output(Output::from(sink).set_audio_codec("aac"))
        .build()
        .unwrap()
        .start()
        .unwrap();
    let result = wait_with_watchdog(scheduler, 60, "two_destructor_bombs");
    assert!(
        result.is_ok(),
        "two contained capture-destructor panics must not fail the settled job: {result:?}"
    );
    assert!(
        destroyed_first.load(Ordering::Acquire) && destroyed_second.load(Ordering::Acquire),
        "both callback boxes must be destroyed before wait() returns"
    );
    let evs = events.lock().unwrap_or_else(|e| e.into_inner()).clone();
    assert_eq!(evs, vec!["end".to_string()], "exactly one on_end, no error");
}

/// Panics from `Drop` the first time it runs on a non-unwinding thread —
/// the shape of a `panic_any` payload whose destructor panics at the
/// discard site.
struct PayloadBomb(Arc<AtomicBool>);

impl Drop for PayloadBomb {
    fn drop(&mut self) {
        self.0.store(true, Ordering::Release);
        if !std::thread::panicking() {
            panic!("injected payload-destructor panic");
        }
    }
}

/// A terminal callback that throws a `panic_any` payload whose own `Drop`
/// panics. Discarding the caught payload raw starts that NEW unwind exactly
/// where the containment believed the panic was over — while the sink (with
/// an unconditional capture bomb) is still owned, so the composition used
/// to abort the process. The payload must be disposed under containment and
/// the sink per-callback: the child asserts the payload's destructor DID
/// run, the captures were destroyed, and the settled result survived.
fn child_panicking_payload_bomb() {
    let events: Arc<Mutex<Vec<String>>> = Arc::new(Mutex::new(Vec::new()));
    let destroyed = Arc::new(AtomicBool::new(false));
    let payload_disposed = Arc::new(AtomicBool::new(false));
    let bomb = AlwaysPanicOnDrop(destroyed.clone());
    let payload_flag = payload_disposed.clone();
    let (ev_end, ev_err) = (events.clone(), events.clone());
    let sink = PacketSink::builder(move |_pkt| {
        let _hold = &bomb;
        Ok(())
    })
    .on_end(move || {
        ev_end.lock().unwrap().push("end".to_string());
        std::panic::panic_any(PayloadBomb(payload_flag.clone()));
    })
    .on_delivery_error(move |e| ev_err.lock().unwrap().push(format!("error: {e}")))
    .build();

    let scheduler = FfmpegContext::builder()
        .input(Input::from("sine=frequency=440:duration=1").set_format("lavfi"))
        .output(Output::from(sink).set_audio_codec("aac"))
        .build()
        .unwrap()
        .start()
        .unwrap();
    let result = wait_with_watchdog(scheduler, 60, "panicking_payload_bomb");
    assert!(
        result.is_ok(),
        "a disposed payload-destructor panic must not fail the settled job: {result:?}"
    );
    assert!(
        payload_disposed.load(Ordering::Acquire),
        "the panic payload must be destroyed (disposed), not leaked on the containment path"
    );
    assert!(
        destroyed.load(Ordering::Acquire),
        "the sink captures must be destroyed before wait() returns"
    );
    let evs = events.lock().unwrap_or_else(|e| e.into_inner()).clone();
    assert_eq!(
        evs,
        vec!["end".to_string()],
        "exactly one on_end (delivered before its panic) and no error callback"
    );
}

/// A caller-supplied error source whose destructor panics unconditionally
/// — even mid-unwind, where an uncontained drop aborts the process. The
/// shape of the delivery-error custody hazard: once first-error-wins let a
/// sibling own the job result, the sink worker's stashed error holds the
/// FINAL `Arc` to this source (the worker's own job-result clone was
/// discarded), so whoever owns that last `Arc` while `on_delivery_error`
/// panics decides between a contained teardown and an abort.
#[derive(Debug)]
struct SourceBomb(Arc<AtomicBool>);

impl std::fmt::Display for SourceBomb {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str("injected error source")
    }
}

impl std::error::Error for SourceBomb {}

impl Drop for SourceBomb {
    fn drop(&mut self) {
        self.0.store(true, Ordering::Release);
        panic!("injected error-source destructor panic");
    }
}

/// The delivery-error custody composition: the sink's `on_packet` fails
/// with a source whose destructor panics; a SIBLING output's error wins
/// first-error-wins (the worker is held at its packet-error log while the
/// gated sibling fails), so the worker's stash becomes the final owner of
/// that source; and the consumer's `on_delivery_error` itself panics at
/// the terminal. The terminal dispatch must hand the callback a borrow
/// while the worker keeps custody: an owned frame-local would drop the
/// final `Arc` mid-unwind, where the panicking source destructor aborts
/// the process before any catch regains control. The child surviving —
/// with the sibling's error intact on `wait()`, the terminal event
/// delivered once, and the source destroyed under containment — is the
/// verdict.
fn child_delivery_error_source_bomb() {
    install_logger_once();
    disarm_all();

    let events: Arc<Mutex<Vec<String>>> = Arc::new(Mutex::new(Vec::new()));
    let source_destroyed = Arc::new(AtomicBool::new(false));
    let bomb_flag = source_destroyed.clone();
    let (ev_end, ev_err) = (events.clone(), events.clone());
    let sink = PacketSink::builder(move |_pkt| {
        Err(
            ez_ffmpeg::packet_sink::PacketCallbackError::with_source(
                "sink consumer rejection",
                SourceBomb(bomb_flag.clone()),
            ),
        )
    })
    .on_end(move || ev_end.lock().unwrap().push("end".to_string()))
    .on_delivery_error(move |e| {
        ev_err.lock().unwrap().push(format!("error: {e}"));
        panic!("injected delivery-error callback panic");
    })
    .build();

    // The sibling parks INSIDE its failing write until the gate opens, so
    // its error cannot be recorded before the sink's own failure is
    // stashed — and once released, it is recorded while the sink worker is
    // still held below, winning first-error-wins.
    let sibling_go = Arc::new(AtomicBool::new(false));
    let go_cb = sibling_go.clone();
    let mut written = 0usize;
    let sibling = Output::new_by_write_callback(move |buf: &[u8]| {
        written += buf.len();
        if written > 1024 {
            while !go_cb.load(Ordering::Acquire) {
                std::thread::sleep(Duration::from_millis(2));
            }
            ffmpeg_sys_next::AVERROR(ffmpeg_sys_next::EIO)
        } else {
            buf.len() as i32
        }
    })
    .set_format("mpegts")
    .set_audio_codec("aac")
    // Small AVIO buffer: TS writes flush continuously, so the gated write
    // is reached during the run rather than at one big trailer flush.
    .set_io_buffer_size(512);
    struct GateOnDrop(Arc<AtomicBool>);
    impl Drop for GateOnDrop {
        fn drop(&mut self) {
            self.0.store(true, Ordering::Release);
        }
    }

    // Park the sink worker at its packet-error log: AFTER the failing
    // callback stashed the bomb-carrying error, BEFORE the worker offers
    // that error to first-error-wins and enters its terminal region.
    *HOLD.lock().unwrap_or_else(|e| e.into_inner()) = Some("Error muxing a packet");
    let scheduler = FfmpegContext::builder()
        .input(Input::from("sine=frequency=440:duration=2").set_format("lavfi"))
        .input(Input::from("sine=frequency=330:duration=2").set_format("lavfi"))
        .output(
            Output::from(sink)
                .set_audio_codec("aac")
                .add_stream_map("0:a"),
        )
        .output(sibling.add_stream_map("1:a"))
        .build()
        .unwrap()
        .start()
        .unwrap();
    // AFTER the scheduler: on an assertion panic these drop FIRST (reverse
    // declaration order), unparking the sibling gate and the held worker
    // before scheduler teardown waits on them.
    let _open_gate_on_unwind = GateOnDrop(sibling_go.clone());
    let _release_on_unwind = ReleaseOnDrop;

    // 1. The sink worker parks with the bomb error stashed.
    let deadline = Instant::now() + Duration::from_secs(30);
    while !HELD.load(Ordering::Acquire) {
        assert!(
            Instant::now() < deadline,
            "the sink worker never reached its packet-error log"
        );
        std::thread::sleep(Duration::from_millis(2));
    }
    // 2. The released sibling fails and records the job error. The held
    // sink worker cannot publish, and a natural end is impossible while
    // two muxers are parked, so a stopping status here proves the
    // sibling's error is recorded (the result is stored before the status
    // publishes).
    sibling_go.store(true, Ordering::Release);
    let deadline = Instant::now() + Duration::from_secs(30);
    while !scheduler.is_ended() {
        assert!(
            Instant::now() < deadline,
            "the sibling's failure was never recorded"
        );
        std::thread::sleep(Duration::from_millis(2));
    }
    // 3. The sink worker resumes: its own error loses first-error-wins
    // (the clone is discarded, leaving the stash as the final source
    // owner), and the terminal dispatches `on_delivery_error` into the
    // injected panic.
    RELEASE.store(true, Ordering::Release);

    let result = wait_with_watchdog(scheduler, 60, "delivery_error_source_bomb");
    assert!(
        matches!(result, Err(Error::Muxing(_))),
        "the sibling's error must stay the job result — the contained \
         terminal-callback panic must not repaint it: {result:?}"
    );
    assert!(
        source_destroyed.load(Ordering::Acquire),
        "the stashed error's source must be destroyed (under containment), not leaked"
    );
    let evs = events.lock().unwrap_or_else(|e| e.into_inner()).clone();
    assert_eq!(evs.len(), 1, "exactly one terminal event: {evs:?}");
    assert!(
        evs[0].starts_with("error") && evs[0].contains("sink consumer rejection"),
        "the terminal must deliver the sink's own stashed error: {evs:?}"
    );
}

/// Parent probe: the two-bomb aggregate disposal (see the child fn).
#[test]
fn sibling_capture_bombs_do_not_abort_the_process() {
    run_child_scenario("two_destructor_bombs");
}

/// Parent probe: the delivery-error custody composition (see the child fn).
#[test]
fn delivery_error_source_destructor_does_not_abort_the_process() {
    run_child_scenario("delivery_error_source_bomb");
}

/// Parent probe: the panicking-payload discard (see the child fn).
#[test]
fn panicking_payload_destructor_does_not_abort_the_process() {
    run_child_scenario("panicking_payload_bomb");
}

/// Full-job settlement: a filter worker's FINAL log fires AFTER the sink
/// already drained, and a panic there must still prevent `on_end` — the
/// sink's barrier waits for the filter's slot, behind which the panic is
/// recorded. The interleaving is forced, not raced: the filter PARKS at its
/// final log (blocking needle) while the sink drains freely and enters the
/// barrier; the release lets the same record fall through to the armed
/// panic needle.
#[test]
fn late_filter_worker_panic_prevents_on_end() {
    let _lock = PROCESS_LOCK.lock().unwrap_or_else(|e| e.into_inner());
    install_logger_once();
    disarm_all();

    let events: Arc<Mutex<Vec<String>>> = Arc::new(Mutex::new(Vec::new()));
    let destroyed = Arc::new(AtomicBool::new(false));
    let delivered = Arc::new(std::sync::atomic::AtomicU32::new(0));
    let capture = FlagOnDrop(destroyed.clone());
    let delivered_cb = delivered.clone();
    let (ev_end, ev_err) = (events.clone(), events.clone());
    let sink = PacketSink::builder(move |_pkt| {
        let _hold = &capture;
        delivered_cb.fetch_add(1, Ordering::Release);
        Ok(())
    })
    .on_end(move || ev_end.lock().unwrap().push("end".to_string()))
    .on_delivery_error(move |e| ev_err.lock().unwrap().push(format!("error: {e}")))
    .build();

    // Both needles on the SAME record: the hold parks the filter thread at
    // its final log; the release lets that record continue into the panic
    // needle check.
    *HOLD.lock().unwrap_or_else(|e| e.into_inner()) = Some("FilterGraph finished.");
    *TRIGGER.lock().unwrap_or_else(|e| e.into_inner()) = Some("FilterGraph finished.");
    let scheduler = FfmpegContext::builder()
        .input(Input::from("sine=frequency=440:duration=1").set_format("lavfi"))
        .output(Output::from(sink).set_audio_codec("aac"))
        .build()
        .unwrap()
        .start()
        .unwrap();
    // Declared AFTER the scheduler: locals drop in reverse declaration
    // order, so on an assertion panic this guard releases the parked
    // worker BEFORE the scheduler's teardown starts waiting on it.
    let _release_on_unwind = ReleaseOnDrop;

    // 1. The filter parks at its final log (its EOF already reached the
    // encoder — the pipeline keeps flowing).
    let deadline = Instant::now() + Duration::from_secs(30);
    while !HELD.load(Ordering::Acquire) {
        assert!(
            Instant::now() < deadline,
            "the filter worker never reached its final log"
        );
        std::thread::sleep(Duration::from_millis(2));
    }
    // 2. The sink drains everything the (already exited) encoder queued:
    // wait for the FIRST delivery (the stability window alone could declare
    // quiescence before anything was delivered), then for delivery
    // quiescence, after which the sink sits in the settlement barrier
    // waiting for the parked filter's slot.
    let deadline = Instant::now() + Duration::from_secs(30);
    while delivered.load(Ordering::Acquire) == 0 {
        assert!(
            Instant::now() < deadline,
            "the sink never delivered a packet"
        );
        std::thread::sleep(Duration::from_millis(10));
    }
    let mut last = delivered.load(Ordering::Acquire);
    let mut stable_since = Instant::now();
    loop {
        std::thread::sleep(Duration::from_millis(50));
        assert!(Instant::now() < deadline, "sink delivery never quiesced");
        let now_count = delivered.load(Ordering::Acquire);
        if now_count != last {
            last = now_count;
            stable_since = Instant::now();
        } else if stable_since.elapsed() >= Duration::from_millis(500) {
            break;
        }
    }
    // 3. Release the filter into the armed panic: it is recorded BEFORE the
    // filter's slot releases, which is exactly what the sink's barrier
    // waits behind.
    RELEASE.store(true, Ordering::Release);

    let result = wait_with_watchdog(scheduler, 60, "late_filter_panic");
    disarm_all();

    match result {
        Err(Error::WorkerPanicked(name)) => {
            assert!(
                name.contains("filtergraph"),
                "expected the filter worker's panic, got {name:?}"
            );
        }
        other => panic!("expected WorkerPanicked, got {other:?}"),
    }
    let evs = events.lock().unwrap_or_else(|e| e.into_inner()).clone();
    assert!(
        !evs.iter().any(|e| e == "end"),
        "on_end must NOT fire when any job worker fails after the sink drained: {evs:?}"
    );
    assert!(
        evs.iter().any(|e| e.starts_with("error")),
        "the failure must surface through on_delivery_error: {evs:?}"
    );
    assert!(destroyed.load(Ordering::Acquire));
}

/// The linearization point: the worker is HELD between its last packet
/// completion (`nb_done` reached, "All streams finished" traced) and the
/// terminal region; an abort issued in that window must be observed by the
/// post-settlement status load and suppress BOTH terminal callbacks. This
/// pins the CURRENT contract (the fresh pre-dispatch load); it does not
/// discriminate the older implementation, whose load also sat after this
/// hold point — the settlement family's discriminating evidence is the
/// parked-filter probe. The worker is released only after a sibling
/// container muxer (fed by a second, infinite input) OBSERVES the abort
/// store — its abort-specific teardown record latches `ABORT_OBSERVED` —
/// so the store happens-before the release: an acknowledged handshake.
#[test]
fn abort_at_the_linearization_point_suppresses_the_terminal() {
    let _lock = PROCESS_LOCK.lock().unwrap_or_else(|e| e.into_inner());
    install_logger_once();
    disarm_all();

    let events: Arc<Mutex<Vec<String>>> = Arc::new(Mutex::new(Vec::new()));
    let destroyed = Arc::new(AtomicBool::new(false));
    let capture = FlagOnDrop(destroyed.clone());
    let (ev_end, ev_err) = (events.clone(), events.clone());
    let sink = PacketSink::builder(move |_pkt| {
        let _hold = &capture;
        Ok(())
    })
    .on_end(move || ev_end.lock().unwrap().push("end".to_string()))
    .on_delivery_error(move |e| ev_err.lock().unwrap().push(format!("error: {e}")))
    .build();

    *HOLD.lock().unwrap_or_else(|e| e.into_inner()) = Some("All streams finished");
    // The second chain is INFINITE and realtime-paced: its container muxer
    // is guaranteed alive whenever the abort lands, and its teardown always
    // crosses the trailer gate, whose abort branch acknowledges the store.
    // The sink's own chain (input 0) still finishes naturally and parks its
    // worker at the hold needle.
    let scheduler = FfmpegContext::builder()
        .input(Input::from("sine=frequency=440:duration=1").set_format("lavfi"))
        .input(
            Input::from("sine=frequency=330")
                .set_format("lavfi")
                .set_readrate(1.0),
        )
        .output(
            Output::from(sink)
                .set_audio_codec("aac")
                .add_stream_map("0:a"),
        )
        .output(
            Output::from("abort-probe.null")
                .set_format("null")
                .set_audio_codec("pcm_s16le")
                .add_stream_map("1:a"),
        )
        .build()
        .unwrap()
        .start()
        .unwrap();
    // AFTER the scheduler: must drop before scheduler teardown (see the
    // filter probe).
    let _release_on_unwind = ReleaseOnDrop;

    // Wait for the worker to park inside the window.
    let deadline = Instant::now() + Duration::from_secs(30);
    while !HELD.load(Ordering::Acquire) {
        assert!(
            Instant::now() < deadline,
            "the sink worker never reached the linearization window"
        );
        std::thread::sleep(Duration::from_millis(2));
    }

    // abort() consumes the scheduler and BLOCKS until every worker exits
    // (RunningGuard's drop) — the worker is parked in the logger, so abort
    // runs on a helper thread. Its ABORT store is its first action; the
    // worker is released into the terminal region only after the sibling
    // muxer's abort record proves that store is published (see the latch's
    // doc comment), so the post-settlement load is ordered after it.
    WATCH_ARMED.store(true, Ordering::Release);
    let abort_thread = std::thread::spawn(move || scheduler.abort());
    let deadline = Instant::now() + Duration::from_secs(30);
    while !ABORT_OBSERVED.load(Ordering::Acquire) {
        assert!(
            Instant::now() < deadline,
            "no worker ever observed the abort store"
        );
        std::thread::sleep(Duration::from_millis(2));
    }
    RELEASE.store(true, Ordering::Release);
    abort_thread
        .join()
        .expect("abort() must return once the job tears down");
    disarm_all();

    // abort() returning proves every slot released, which the defined
    // destruction point precedes.
    assert!(
        destroyed.load(Ordering::Acquire),
        "the aborted job must still destroy the sink captures"
    );

    let evs = events.lock().unwrap_or_else(|e| e.into_inner()).clone();
    assert!(
        evs.is_empty(),
        "an abort observed at the linearization point suppresses BOTH terminal callbacks: {evs:?}"
    );
}

/// Failure-driven shutdown must NOT be classified as cancellation: a
/// capacity-one channel sink is wedged in its blocking send when a sibling
/// output (own input, gated EIO) fails. The blocked send observes the
/// stopping status WITH a recorded job error, abandons as
/// failure-truncation (not Cancelled), and the terminal — held at the
/// sink-only completion log while the test drains the channel to guarantee
/// capacity — must deliver `Error(JobFailed)`, never silence.
#[test]
fn full_channel_sink_reports_a_sibling_failure_not_silence() {
    let _lock = PROCESS_LOCK.lock().unwrap_or_else(|e| e.into_inner());
    install_logger_once();
    disarm_all();

    let (sink, receiver) =
        ez_ffmpeg::packet_sink::PacketSink::channel(std::num::NonZeroUsize::new(1).unwrap());
    // The sibling's write callback PARKS at its failure point (test-local
    // gate, before any error is recorded or published) so the test can
    // guarantee the sink is already wedged in its blocking send when the
    // failure lands — the exact window the classification governs. A
    // guard releases the gate on unwind so an early assertion failure
    // cannot deadlock scheduler teardown.
    let sibling_failing = Arc::new(AtomicBool::new(false));
    let sibling_go = Arc::new(AtomicBool::new(false));
    let (failing_cb, go_cb) = (sibling_failing.clone(), sibling_go.clone());
    let mut written = 0usize;
    let sibling = Output::new_by_write_callback(move |buf: &[u8]| {
        written += buf.len();
        if written > 1024 {
            failing_cb.store(true, Ordering::Release);
            while !go_cb.load(Ordering::Acquire) {
                std::thread::sleep(Duration::from_millis(2));
            }
            ffmpeg_sys_next::AVERROR(ffmpeg_sys_next::EIO)
        } else {
            buf.len() as i32
        }
    })
    .set_format("mpegts")
    .set_audio_codec("aac")
    // Small AVIO buffer: TS writes flush continuously, so the EIO lands
    // during the run rather than at one big trailer flush.
    .set_io_buffer_size(512);
    struct GateOnDrop(Arc<AtomicBool>);
    impl Drop for GateOnDrop {
        fn drop(&mut self) {
            self.0.store(true, Ordering::Release);
        }
    }

    *HOLD.lock().unwrap_or_else(|e| e.into_inner()) = Some("Packet sink muxer finished.");
    let scheduler = FfmpegContext::builder()
        .input(Input::from("sine=frequency=440:duration=2").set_format("lavfi"))
        .input(Input::from("sine=frequency=330:duration=2").set_format("lavfi"))
        .output(
            Output::from(sink)
                .set_audio_codec("aac")
                .add_stream_map("0:a"),
        )
        .output(sibling.add_stream_map("1:a"))
        .build()
        .unwrap()
        .start()
        .unwrap();
    // AFTER the scheduler: on an assertion panic these drop FIRST (reverse
    // declaration order), unparking the sibling gate and the held worker
    // before scheduler teardown waits on them.
    let _open_gate_on_unwind = GateOnDrop(sibling_go.clone());
    let _release_on_unwind = ReleaseOnDrop;

    // 1. Take exactly the stream-info event, then stop draining. The
    // sibling reaches its (parked) failure point on its own input.
    match receiver.recv_timeout(Duration::from_secs(20)) {
        Ok(ez_ffmpeg::packet_sink::PacketSinkEvent::StreamInfo(_)) => {}
        other => panic!("expected the stream-info event first, got {other:?}"),
    }
    let deadline = Instant::now() + Duration::from_secs(30);
    while !sibling_failing.load(Ordering::Acquire) {
        assert!(
            Instant::now() < deadline,
            "the sibling never reached its write-failure point"
        );
        std::thread::sleep(Duration::from_millis(2));
    }
    // 2. With the sibling parked (nothing recorded yet), the sink wedges:
    // the consumed stream-info freed one slot, the first packet fills it,
    // the second blocks. Then the released sibling records its error and
    // publishes the stopping status INTO that blocked send.
    std::thread::sleep(Duration::from_millis(600));
    sibling_go.store(true, Ordering::Release);

    // 3. The sink classifies the failure-driven stop, exits its loop and
    // parks at the sink-only completion log — BEFORE the terminal dispatch.
    let deadline = Instant::now() + Duration::from_secs(30);
    while !HELD.load(Ordering::Acquire) {
        assert!(
            Instant::now() < deadline,
            "the sink worker never exited its delivery loop"
        );
        std::thread::sleep(Duration::from_millis(2));
    }
    // 4. Drain the channel so the (best-effort) terminal event has
    // guaranteed capacity, then release the terminal.
    while receiver.try_recv().is_ok() {}
    RELEASE.store(true, Ordering::Release);

    let result = wait_with_watchdog(scheduler, 60, "full_channel_sibling_failure");
    disarm_all();
    assert!(
        result.is_err(),
        "the sibling write failure must fail the job"
    );

    let mut saw_job_failed = false;
    while let Ok(event) = receiver.try_recv() {
        match event {
            ez_ffmpeg::packet_sink::PacketSinkEvent::End => {
                panic!("End must not be delivered when the job failed elsewhere")
            }
            ez_ffmpeg::packet_sink::PacketSinkEvent::Error(
                ez_ffmpeg::error::PacketSinkError::JobFailed { .. },
            ) => saw_job_failed = true,
            _ => {}
        }
    }
    assert!(
        saw_job_failed,
        "a failure-driven stop observed by the wedged send must surface as Error(JobFailed), not silence"
    );
}
