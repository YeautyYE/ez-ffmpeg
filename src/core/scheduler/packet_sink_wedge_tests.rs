//! Wedge proof for a container sibling blocked in a TCP write while a
//! packet sink finalizes.
//!
//! These tests live in the crate rather than `tests/` because the decisive
//! stage reads the `cfg(test)` write probe in `mux_task`: integration
//! binaries link the library compiled WITHOUT `cfg(test)`, so no probe
//! symbol can reach them, while production builds must carry no probe at
//! all. The proof has four stages, each closing a confounder the previous
//! ones admit:
//!
//! 1. **Plateau** — the unread peer's receive queue fills and pins
//!    (socket-level; also pins for a writer that simply went idle).
//! 2. **Drain-and-refill** — freed window refills past any possible
//!    send-buffer residue, so the writer was still producing after the
//!    plateau (also passes for a writer whose refill was a final burst).
//! 3. **Repin** — with draining stopped for good, the queue pins again, so
//!    arrivals have ceased against a closed window (cannot say WHY they
//!    ceased: blocked in write, or exited/stalled holding residue).
//! 4. **Write parked** — the `tcp_write_probe` counters show one
//!    `av_interleaved_write_frame` call entered for a `tcp://` output and
//!    not returned, frozen across the sampling window: the muxer thread is
//!    inside that call, not starved upstream in its packet queue and not
//!    exited. Paired with a scheduler liveness check, this is the state
//!    `stop()` must cut.
//!
//! The socket-stage helpers are duplicated from `tests/common` for the
//! integration suites that still use them; this module cannot import from
//! `tests/`.

use super::mux_task::tcp_write_probe;
use crate::core::context::ffmpeg_context::FfmpegContext;
use crate::core::context::input::Input;
use crate::core::context::output::Output;
use crate::core::packet_sink::PacketSink;
use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};
use std::sync::Arc;
use std::time::{Duration, Instant};

/// Stage 4: requires the probe to show exactly one tcp write entered and
/// not returned, with both counters FROZEN across six consecutive 250 ms
/// samples. A healthy writer cycles the pair between samples; a starved
/// muxer (parked in its packet queue) and an exited one sit at
/// entered == returned. `returned` is loaded before `entered` so a racing
/// completion can never fake an in-flight call. Returns the parked call's
/// generation — the value of `ENTERED` with that call in flight — so the
/// caller can later tie the stop() cut back to THIS write and no other.
fn require_write_parked(deadline: Instant) -> Result<u64, String> {
    let mut prev: Option<(u64, u64)> = None;
    let mut stable = 0u32;
    loop {
        std::thread::sleep(Duration::from_millis(250));
        let returned = tcp_write_probe::RETURNED.load(Ordering::Acquire);
        let entered = tcp_write_probe::ENTERED.load(Ordering::Acquire);
        if entered == returned + 1 && prev == Some((entered, returned)) {
            stable += 1;
            if stable >= 6 {
                // Confirm against a return racing the two loads above: the
                // load order (returned first) means a call returning
                // between them still presents entered == returned + 1. A
                // genuinely parked call cannot return, so RETURNED moving
                // since the `returned` load unmasks the tear — resample.
                if tcp_write_probe::RETURNED.load(Ordering::Acquire) == returned {
                    return Ok(entered);
                }
                stable = 0;
            }
        } else {
            stable = 0;
        }
        prev = Some((entered, returned));
        if Instant::now() >= deadline {
            return Err(format!(
                "the muxer is not parked inside a tcp write \
                 (entered {entered}, returned {returned})"
            ));
        }
    }
}

/// Non-draining size probe of the unread peer's receive queue.
fn queued_bytes(stream: &std::net::TcpStream, buf: &mut [u8]) -> usize {
    match stream.peek(buf) {
        Ok(n) => n,
        Err(e) if e.kind() == std::io::ErrorKind::WouldBlock => 0,
        Err(e) => panic!("peek on the unread peer failed: {e}"),
    }
}

/// Stage 1/3 primitive: waits until the unread receive queue holds a
/// non-zero count that is IDENTICAL across six consecutive 250 ms polls
/// and returns it. A count equal to the peek window is window saturation,
/// not a plateau: the window is widened until the real tail is visible
/// (hence `&mut Vec`).
fn wait_for_receive_queue_plateau(
    stream: &std::net::TcpStream,
    peek_buf: &mut Vec<u8>,
    deadline: Instant,
) -> Result<usize, String> {
    let mut queued_prev = 0usize;
    let mut stable_polls = 0u32;
    loop {
        std::thread::sleep(Duration::from_millis(250));
        if Instant::now() >= deadline {
            return Err(format!(
                "the unread peer's receive queue never reached a plateau \
                 (last observed {queued_prev} bytes)"
            ));
        }
        let queued = queued_bytes(stream, peek_buf);
        if queued == peek_buf.len() {
            *peek_buf = vec![0u8; peek_buf.len() * 2];
            stable_polls = 0;
            queued_prev = 0;
            continue;
        }
        if queued > 0 && queued == queued_prev {
            stable_polls += 1;
            if stable_polls >= 6 {
                return Ok(queued);
            }
        } else {
            stable_polls = 0;
            queued_prev = queued;
        }
    }
}

/// Stage 2: drains chunks and demands a refill. RESIDUE_BOUND exceeds
/// anything the sender's send buffer could still hold at the plateau
/// (16 KiB requested via send_buffer_size on the URL; Linux at most doubles
/// the effective size), so cumulative arrivals past the bound — everything
/// ever observed (still queued + drained) beyond the pinned level — prove
/// the sibling wrote against the pinned queue AFTER the plateau; an idle
/// writer's residue being flushed cannot account for them. Returns the
/// total drained.
fn prove_post_plateau_refill(
    stream: &std::net::TcpStream,
    peek_buf: &mut [u8],
    pinned: usize,
    deadline: Instant,
) -> Result<usize, String> {
    const RESIDUE_BOUND: usize = 64 * 1024;
    let mut drained_total = 0usize;
    loop {
        let queued = queued_bytes(stream, peek_buf);
        if (queued + drained_total).saturating_sub(pinned) > RESIDUE_BOUND {
            return Ok(drained_total);
        }
        if Instant::now() >= deadline {
            return Err(
                "the receive queue never refilled after a drain: the sibling was \
                 not blocked on flow control (writer idle or stalled upstream)"
                    .into(),
            );
        }
        if queued > 0 {
            // Drain half the queue; a large chunk keeps the freed window
            // above the receiver's silly-window-avoidance threshold so the
            // sender is re-opened promptly.
            let take = (queued / 2).max(1);
            let n = std::io::Read::read(&mut (&*stream), &mut peek_buf[..take])
                .map_err(|e| format!("draining the pinned receive queue: {e}"))?;
            drained_total += n;
        }
        std::thread::sleep(Duration::from_millis(25));
    }
}

/// Stage 3: with draining stopped for good, requires the queue to pin a
/// SECOND time. Stage 2 alone can exit mid-refill — receive window open,
/// arrivals in flight — so the repin is what proves arrivals have CEASED
/// against a window nobody is opening: a delivering writer would keep
/// changing the count until flow control stops it. What the socket alone
/// cannot distinguish is WHY arrivals ceased — blocked in write, or the
/// writer exited/stalled after a final burst that happened to fill the
/// queue. No byte-count threshold can close that gap (exhaustion levels
/// vary by segment coalescing and SKB accounting), so callers pair the
/// repin with `require_write_parked` and a scheduler liveness check.
fn require_flow_control_repin(
    stream: &std::net::TcpStream,
    peek_buf: &mut Vec<u8>,
    deadline: Instant,
) -> Result<usize, String> {
    wait_for_receive_queue_plateau(stream, peek_buf, deadline)
        .map_err(|e| format!("no second plateau after the refill: {e}"))
}

/// A loopback TCP listener whose `SO_RCVBUF` is fixed BEFORE `listen(2)`, so
/// accepted connections inherit the small receive buffer. Linux only applies a
/// receive-buffer size to CHILD sockets when it is set before listen; shrinking
/// an already-listening `std::net::TcpListener` does not propagate to accepted
/// connections. Returns `None` if the raw socket setup fails (the caller then
/// falls back to a default listener and the weaker assertion). Linux-only.
#[cfg(target_os = "linux")]
fn listener_with_small_rcvbuf(rcvbuf: libc::c_int) -> Option<std::net::TcpListener> {
    use std::os::unix::io::FromRawFd;
    // SAFETY: the standard socket(2)/setsockopt(2)/bind(2)/listen(2) sequence on
    // a fresh AF_INET stream socket. The fd is closed on every early return and
    // adopted by TcpListener only on the success path.
    unsafe {
        let fd = libc::socket(libc::AF_INET, libc::SOCK_STREAM, 0);
        if fd < 0 {
            return None;
        }
        let set = |opt: libc::c_int, val: libc::c_int| -> bool {
            libc::setsockopt(
                fd,
                libc::SOL_SOCKET,
                opt,
                &val as *const libc::c_int as *const libc::c_void,
                std::mem::size_of::<libc::c_int>() as libc::socklen_t,
            ) == 0
        };
        // SO_RCVBUF before listen so children inherit it; SO_REUSEADDR for a
        // clean re-bind of the ephemeral port.
        if !set(libc::SO_RCVBUF, rcvbuf) || !set(libc::SO_REUSEADDR, 1) {
            libc::close(fd);
            return None;
        }
        let addr = libc::sockaddr_in {
            sin_family: libc::AF_INET as libc::sa_family_t,
            sin_port: 0, // ephemeral port
            sin_addr: libc::in_addr {
                s_addr: u32::from(std::net::Ipv4Addr::LOCALHOST).to_be(),
            },
            sin_zero: [0; 8],
        };
        let bound = libc::bind(
            fd,
            &addr as *const libc::sockaddr_in as *const libc::sockaddr,
            std::mem::size_of::<libc::sockaddr_in>() as libc::socklen_t,
        ) == 0;
        if !bound || libc::listen(fd, 1) != 0 {
            libc::close(fd);
            return None;
        }
        Some(std::net::TcpListener::from_raw_fd(fd))
    }
}

/// Build the backpressure listener and report whether its receive buffer could
/// be provably shrunk before `listen(2)`. On non-Linux (or on raw-socket
/// failure) a default listener is used; the drain-and-refill proof is
/// buffer-size independent either way.
fn make_backpressure_listener() -> (std::net::TcpListener, bool) {
    #[cfg(target_os = "linux")]
    {
        if let Some(l) = listener_with_small_rcvbuf(16 * 1024) {
            return (l, true);
        }
    }
    (std::net::TcpListener::bind("127.0.0.1:0").unwrap(), false)
}

/// Pins stage 3 against the confounder it exists for: a writer that
/// delivers a refill burst after the drain and then goes IDLE, its residue
/// fully drained. Stages 1-2 accept that shape — the burst satisfies the
/// residue bound — so without the repin the finalize-gating probe could
/// proceed against a sibling that is not blocked at all. A synthetic
/// raw-TCP writer (no ffmpeg) plays the stalled producer deterministically;
/// stage 3 must reject it.
#[test]
fn flow_control_repin_rejects_a_writer_that_stalls_after_the_refill() {
    let _serial = tcp_write_probe::exclusive();
    let (listener, _rcvbuf_shrunk) = make_backpressure_listener();
    let addr = listener.local_addr().unwrap();
    let (stream_tx, stream_rx) = std::sync::mpsc::channel();
    let accept_thread = std::thread::spawn(move || {
        if let Ok((stream, _)) = listener.accept() {
            let _ = stream_tx.send(stream);
        }
    });

    let stop_writing = Arc::new(AtomicBool::new(false));
    let stop_flag = stop_writing.clone();
    // The stalled producer: pushes 8 KiB chunks (blocking in write while
    // the receiver's queue is full) until told to stop, then vanishes.
    let writer_thread = std::thread::spawn(move || {
        let mut peer = std::net::TcpStream::connect(addr).expect("writer connect");
        let chunk = [0u8; 8 * 1024];
        while !stop_flag.load(Ordering::Acquire) {
            if std::io::Write::write_all(&mut peer, &chunk).is_err() {
                break;
            }
        }
    });

    let stream = stream_rx
        .recv_timeout(Duration::from_secs(10))
        .expect("the synthetic writer never connected");
    stream.set_nonblocking(true).unwrap();
    let mut peek_buf = vec![0u8; 4 * 1024 * 1024];
    let pinned = wait_for_receive_queue_plateau(
        &stream,
        &mut peek_buf,
        Instant::now() + Duration::from_secs(20),
    )
    .expect("the synthetic writer must reach a first plateau");
    prove_post_plateau_refill(
        &stream,
        &mut peek_buf,
        pinned,
        Instant::now() + Duration::from_secs(15),
    )
    .expect("a live writer must pass stage 2 — the confounder is what comes next");

    // The stall: the writer quits, and every byte it ever sent is drained
    // (its residue is finite once the thread is gone), leaving the receive
    // window open with nothing arriving — the exact state a pinned-count
    // assertion would have to distinguish from a blocked write.
    stop_writing.store(true, Ordering::Release);
    let drain_deadline = Instant::now() + Duration::from_secs(10);
    let mut writer_gone = false;
    let mut empty_since: Option<Instant> = None;
    loop {
        assert!(
            Instant::now() < drain_deadline,
            "the synthetic writer's residue never drained"
        );
        if !writer_gone && writer_thread.is_finished() {
            writer_gone = true;
        }
        let queued = queued_bytes(&stream, &mut peek_buf);
        if queued > 0 {
            let _ = std::io::Read::read(&mut (&stream), &mut peek_buf[..queued])
                .expect("draining the dead writer's residue");
            empty_since = None;
        } else if writer_gone {
            // Empty AND the writer is gone: hold for half a second to let
            // any in-flight segment land, then the queue is empty for good.
            let since = *empty_since.get_or_insert_with(Instant::now);
            if since.elapsed() >= Duration::from_millis(500) {
                break;
            }
        }
        std::thread::sleep(Duration::from_millis(20));
    }
    writer_thread.join().expect("the synthetic writer panicked");

    let verdict = require_flow_control_repin(
        &stream,
        &mut peek_buf,
        Instant::now() + Duration::from_secs(5),
    );
    let rejection = verdict.expect_err(
        "stage 3 accepted a stalled producer: the repin proves nothing and \
         the finalize-gating probe would run against an unblocked sibling",
    );
    assert!(
        rejection.contains("no second plateau"),
        "the rejection must name the missing repin, got: {rejection}"
    );
    drop(stream);
    let _ = accept_thread.join();
}

/// Pins stage 4 against the confounder stages 1-3 CANNOT reject: a writer
/// that produces through the drain-and-refill, then stalls for good while
/// its residue stays queued. The residue pins (stage 1 shape), the earlier
/// refill was real (stage 2 passed while it was alive), and the untouched
/// residue pins again (stage 3 passes) — yet nothing is blocked in a write.
/// A nonblocking synthetic writer plays that producer: it pumps under
/// WouldBlock-retry through stages 1-2, then exits WITHOUT its residue ever
/// being drained. `require_write_parked` must reject the scene — no tcp
/// write is in flight anywhere in the process (the serial lock guarantees
/// no wedge test is running concurrently).
#[test]
fn write_parked_probe_rejects_a_stalled_writer_with_residue() {
    let _serial = tcp_write_probe::exclusive();
    let (listener, _rcvbuf_shrunk) = make_backpressure_listener();
    let addr = listener.local_addr().unwrap();
    let (stream_tx, stream_rx) = std::sync::mpsc::channel();
    let accept_thread = std::thread::spawn(move || {
        if let Ok((stream, _)) = listener.accept() {
            let _ = stream_tx.send(stream);
        }
    });

    let stop_writing = Arc::new(AtomicBool::new(false));
    let stop_flag = stop_writing.clone();
    // Nonblocking so the writer can OBSERVE the stop flag even while the
    // peer's queue is full (a blocking writer parked in write_all could
    // never exit without a drain reopening the window).
    let writer_thread = std::thread::spawn(move || {
        let peer = std::net::TcpStream::connect(addr).expect("writer connect");
        peer.set_nonblocking(true).unwrap();
        let chunk = [0u8; 8 * 1024];
        while !stop_flag.load(Ordering::Acquire) {
            match std::io::Write::write(&mut (&peer), &chunk) {
                Ok(_) => {}
                Err(e) if e.kind() == std::io::ErrorKind::WouldBlock => {
                    std::thread::sleep(Duration::from_millis(5));
                }
                Err(_) => break,
            }
        }
        // Exiting drops the peer socket; the receiver keeps every queued
        // byte readable — the residue this confounder is about.
    });

    let stream = stream_rx
        .recv_timeout(Duration::from_secs(10))
        .expect("the synthetic writer never connected");
    stream.set_nonblocking(true).unwrap();
    let mut peek_buf = vec![0u8; 4 * 1024 * 1024];
    let pinned = wait_for_receive_queue_plateau(
        &stream,
        &mut peek_buf,
        Instant::now() + Duration::from_secs(20),
    )
    .expect("the synthetic writer must reach a first plateau");
    prove_post_plateau_refill(
        &stream,
        &mut peek_buf,
        pinned,
        Instant::now() + Duration::from_secs(15),
    )
    .expect("the writer is alive through stage 2 — the stall comes next");

    // The post-refill stall, residue INTACT: stop the writer and let its
    // last in-flight segments land; nothing drains from here on.
    stop_writing.store(true, Ordering::Release);
    writer_thread.join().expect("the synthetic writer panicked");
    std::thread::sleep(Duration::from_millis(500));

    require_flow_control_repin(
        &stream,
        &mut peek_buf,
        Instant::now() + Duration::from_secs(20),
    )
    .expect("undrained residue pins again: stage 3 cannot see the stall");

    let verdict = require_write_parked(Instant::now() + Duration::from_secs(4));
    let rejection = verdict.expect_err(
        "stage 4 accepted a scene with no tcp write in flight: the probe \
         proves nothing and a stalled sibling would pass the wedge",
    );
    assert!(
        rejection.contains("not parked inside a tcp write"),
        "the rejection must name the missing in-flight write, got: {rejection}"
    );
    drop(stream);
    let _ = accept_thread.join();
}

/// The finalize-gating probe: a packet sink must never hold the
/// scheduler-wide I/O finalize exemption. With a sink provably parked in
/// its terminal coordinator and a sibling container provably blocked inside
/// a TCP write, stop() must cut that very write within the grace window and
/// return — an (ungated) sink-held finalize guard would suppress the cut
/// and hang. The cut is acknowledged end to end: the parked write (and no
/// other) returns an error, stop() surfaces it, and the sink delivers
/// exactly one terminal event, the delivery error. Native codecs only (AAC
/// sink chain, mpeg4 sibling), so every CI lane runs this — the property is
/// scheduler-wide gating, not encoder behavior.
#[test]
fn blocked_sibling_stays_interruptible_while_sink_finalizes() {
    let _serial = tcp_write_probe::exclusive();
    // SO_RCVBUF is pinned small BEFORE listen(2) (Linux; silently falls back
    // to a default listener elsewhere) so the receive queue pins within a
    // couple of polls instead of megabytes later; the drain-and-refill proof
    // below is buffer-size independent either way.
    let (listener, _rcvbuf_shrunk) = make_backpressure_listener();
    let addr = listener.local_addr().unwrap();
    // Hand the accepted socket back to the test thread, which holds it open
    // without reading until the test ends and probes it for wedge evidence.
    let (stream_tx, stream_rx) = std::sync::mpsc::channel();
    let accept_thread = std::thread::spawn(move || {
        if let Ok((stream, _)) = listener.accept() {
            let _ = stream_tx.send(stream);
        }
    });

    // Terminal sink events are counted, and the delivered error's text is
    // kept: nothing may fire while the coordinator is parked, and across
    // the stop exactly one event — the delivery error surfacing the cut —
    // may fire for the finalizing sink.
    let ends = Arc::new(AtomicUsize::new(0));
    let errors = Arc::new(AtomicUsize::new(0));
    let delivered_error = Arc::new(std::sync::Mutex::new(None::<String>));
    let (ends_cb, errors_cb) = (ends.clone(), errors.clone());
    let delivered_cb = delivered_error.clone();
    let sink = PacketSink::builder(move |_pkt| Ok(()))
        .on_end(move || {
            ends_cb.fetch_add(1, Ordering::AcqRel);
        })
        .on_delivery_error(move |err| {
            *delivered_cb
                .lock()
                .unwrap_or_else(std::sync::PoisonError::into_inner) = Some(err.to_string());
            errors_cb.fetch_add(1, Ordering::AcqRel);
        })
        .build();

    let scheduler = FfmpegContext::builder()
        // High-entropy source so the non-reading peer's buffers fill fast.
        .input(Input::from("testsrc2=s=1280x720:r=30").set_format("lavfi"))
        // Unbounded audio for the sink chain; its 300 ms recording limit is
        // what parks the sink in its terminal coordinator mid-run.
        .input(Input::from("sine=frequency=440").set_format("lavfi"))
        .output(
            Output::new_by_packet_sink(sink)
                .set_audio_codec("aac")
                .add_stream_map("1:a")
                .set_recording_time_us(300_000),
        )
        .output(
            Output::from(format!("tcp://{addr}?send_buffer_size=16384"))
                .add_stream_map("0:v")
                .set_format("mpegts")
                .set_video_codec("mpeg4")
                .set_video_codec_opt("qscale", "1"),
        )
        .build()
        .unwrap()
        .start()
        .unwrap();
    // Wedge proof: the four-stage handshake documented at module level, not
    // a timing guess. Stages 1-3 are socket-side (plateau, drain-and-refill,
    // repin); stage 4 is the producer-side heartbeat that the socket alone
    // cannot supply: the repinned queue with a LIVE job still admits a
    // muxer starved upstream in its packet queue (nothing to write, residue
    // pinning the count), which stop() would trivially cut — no wedge would
    // be exercised. Only a frozen entered == returned + 1 pins the sibling
    // INSIDE av_interleaved_write_frame (the infinite testsrc2 input
    // guarantees the pipeline never runs dry on its own).
    let stream = stream_rx
        .recv_timeout(Duration::from_secs(20))
        .expect("the sibling never connected to the TCP peer");
    stream.set_nonblocking(true).unwrap();
    let mut peek_buf = vec![0u8; 4 * 1024 * 1024];
    let pinned = wait_for_receive_queue_plateau(
        &stream,
        &mut peek_buf,
        Instant::now() + Duration::from_secs(30),
    )
    .expect("stage 1");
    let _drained = prove_post_plateau_refill(
        &stream,
        &mut peek_buf,
        pinned,
        Instant::now() + Duration::from_secs(15),
    )
    .expect("stage 2");
    let _repinned = require_flow_control_repin(
        &stream,
        &mut peek_buf,
        Instant::now() + Duration::from_secs(20),
    )
    .expect("stage 3");
    let parked_gen = require_write_parked(Instant::now() + Duration::from_secs(20))
        .expect("stage 4: the sibling must be parked inside a tcp write");
    // The liveness half of the contract: a parked write on an ENDED job
    // would be teardown noise, not the running wedge stop() must cut.
    assert!(
        !scheduler.is_ended(),
        "the job settled during the wedge proof: the sibling exited instead \
         of blocking, so stop() would be cutting nothing"
    );
    // The finalizer half: the sink's terminal coordinator must be PARKED in
    // the settlement barrier when the cut lands — that overlap is the whole
    // scene. The 300 ms recording limit makes it likely long before the
    // socket stages finish, but only the barrier's own waiter count proves
    // it; a sink still short of its wait would let stop() cut the sibling
    // with no finalize exemption in play. Parked also means not admitted:
    // no terminal event may have fired yet.
    let sink_parked_deadline = Instant::now() + Duration::from_secs(10);
    while scheduler.parked_settlement_waiters() == 0 {
        assert!(
            Instant::now() < sink_parked_deadline,
            "the sink never parked in the settlement barrier: stop() would \
             not be racing a finalizing sink"
        );
        std::thread::sleep(Duration::from_millis(2));
    }
    assert_eq!(
        (ends.load(Ordering::Acquire), errors.load(Ordering::Acquire)),
        (0, 0),
        "a terminal sink event fired while the coordinator was still parked"
    );

    let (tx, rx) = std::sync::mpsc::channel();
    let stop_worker = std::thread::spawn(move || {
        let _ = tx.send(scheduler.stop());
    });
    let result = match rx.recv_timeout(Duration::from_secs(30)) {
        Ok(result) => {
            stop_worker.join().expect("the stop worker panicked");
            result
        }
        Err(_) => {
            // A hung stop() must not outlive the serialization guard: left
            // detached, its muxer would keep driving the process-wide
            // probe counters under the next test's samples. Close the
            // unread peer to cut the wedged write loose, join the worker,
            // then fail.
            drop(stream);
            let _ = stop_worker.join();
            panic!("stop() hung: the sink held the finalize exemption over a blocked sibling");
        }
    };
    // Cut acknowledgment, tied to the parked generation. stop() joins every
    // worker before returning, so the counters are settled: exactly the
    // parked call — write number `parked_gen` — may have returned (the write
    // loop exits on its first failed write, and the trailer bypasses the
    // probe), it must have returned an ERROR, and the erroring call's own
    // recorded generation must be the parked one. The peer's queue stayed
    // pinned and unread throughout, so flow control could never complete
    // that write on its own: only the stop-driven output interrupt can have
    // unparked it. Any other final state means the "parked" write was not
    // cut — it finished or cycled on its own and stage 4 watched a healthy
    // writer, exactly the pass-without-a-wedge this acknowledgment exists
    // to reject.
    let entered = tcp_write_probe::ENTERED.load(Ordering::Acquire);
    let returned = tcp_write_probe::RETURNED.load(Ordering::Acquire);
    assert_eq!(
        (entered, returned),
        (parked_gen, parked_gen),
        "tcp writes moved past the parked generation across stop()"
    );
    let last_ret = tcp_write_probe::LAST_RET.load(Ordering::Acquire);
    assert!(
        last_ret < 0,
        "the parked write returned {last_ret} (success): stop() cut nothing"
    );
    let err_gen = tcp_write_probe::LAST_ERR_GEN.load(Ordering::Acquire);
    assert_eq!(
        err_gen, parked_gen,
        "the write that returned the error is not the parked one"
    );
    // The cut must also be VISIBLE downstream, in its exact shape: the
    // write loop records a failed PACKET write as an interleaved-write
    // error before the muxer releases its slot (an EOF break records
    // nothing there, and a trailer failure records the distinct trailer
    // variant), so stop() must surface precisely that variant, and the
    // sink coordinator — admitted only after that release — must deliver
    // exactly one terminal event: the delivery error carrying it. An
    // on_end here would certify a clean job whose sibling was just cut
    // mid-write.
    match &result {
        Err(crate::error::Error::Muxing(
            crate::error::MuxingOperationError::InterleavedWriteError(_),
        )) => {}
        other => panic!("stop() must surface the cut interleaved write, got {other:?}"),
    }
    let (end_count, error_count) = (ends.load(Ordering::Acquire), errors.load(Ordering::Acquire));
    assert_eq!(
        (end_count, error_count),
        (0, 1),
        "the finalizing sink must deliver exactly the cut's failure \
         ({end_count} end, {error_count} error)"
    );
    let delivered = delivered_error
        .lock()
        .unwrap_or_else(std::sync::PoisonError::into_inner)
        .take()
        .expect("the delivery error must have been captured");
    assert!(
        delivered.contains("during interleaved write"),
        "the sink must be handed the cut write's failure, got: {delivered}"
    );
    // The peer socket stayed open (unread) across the whole stop; the accept
    // thread already exited after handing it over.
    drop(stream);
    let _ = accept_thread.join();
}
