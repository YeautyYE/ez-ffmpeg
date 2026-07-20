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
//!   after the sink drained) must prevent `on_end`, and a capture-`Drop`
//!   panic composed with a panicking logger must not un-settle a delivered
//!   `on_end`;
//! - the linearization window: an abort landing while the worker is held
//!   between its last packet completion and the terminal region suppresses
//!   both terminal callbacks.
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

/// The injecting logger is process-global: scenarios must not interleave.
static PROCESS_LOCK: Mutex<()> = Mutex::new(());

struct InjectingLogger;

impl log::Log for InjectingLogger {
    fn enabled(&self, _: &log::Metadata) -> bool {
        true
    }

    fn log(&self, record: &log::Record) {
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
    let _release_on_unwind = ReleaseOnDrop;
    let scheduler = FfmpegContext::builder()
        .input(Input::from("sine=frequency=440:duration=1").set_format("lavfi"))
        .output(Output::from(sink).set_audio_codec("aac"))
        .build()
        .unwrap()
        .start()
        .unwrap();

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
    // wait for delivery quiescence, after which the sink sits in the
    // settlement barrier waiting for the parked filter's slot.
    let deadline = Instant::now() + Duration::from_secs(30);
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
    assert!(last > 0, "the sink must have delivered packets before the probe");
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
/// parked-filter probe. The 500 ms grace below is a scheduling heuristic
/// for the helper's ABORT store, not a happens-before acknowledgement.
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
    let _release_on_unwind = ReleaseOnDrop;
    let scheduler = FfmpegContext::builder()
        .input(Input::from("sine=frequency=440:duration=1").set_format("lavfi"))
        .output(Output::from(sink).set_audio_codec("aac"))
        .build()
        .unwrap()
        .start()
        .unwrap();

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
    // grace below dwarfs the helper's spawn+store latency, so the store has
    // landed before the worker is released into the terminal region.
    let abort_thread = std::thread::spawn(move || scheduler.abort());
    std::thread::sleep(Duration::from_millis(500));
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
    // The sibling fails hard right past its header. No test-side gating:
    // the failure's timing relative to the wedge is irrelevant — once the
    // job is stopping with a recorded error, the sink's NEXT blocked send
    // into the undrained capacity-one channel classifies it (a gated
    // failure would instead stall forever behind the input controller's
    // choke once the wedged sink lags).
    let mut written = 0usize;
    let sibling = Output::new_by_write_callback(move |buf: &[u8]| {
        written += buf.len();
        if written > 1024 {
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

    *HOLD.lock().unwrap_or_else(|e| e.into_inner()) = Some("Packet sink muxer finished.");
    let _release_on_unwind = ReleaseOnDrop;
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

    // 1. Take exactly the stream-info event, then stop draining: the
    // capacity-one channel wedges the sink in its blocking send while the
    // sibling (its own input) runs into its EIO.
    match receiver.recv_timeout(Duration::from_secs(20)) {
        Ok(ez_ffmpeg::packet_sink::PacketSinkEvent::StreamInfo(_)) => {}
        other => panic!("expected the stream-info event first, got {other:?}"),
    }

    // 2. The sink observes the failure-driven stop from its blocked send,
    // classifies it, exits its loop and parks at the sink-only completion
    // log — BEFORE the terminal dispatch.
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
