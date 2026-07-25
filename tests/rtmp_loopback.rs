#![cfg(feature = "rtmp")]
//! Loopback integration tests for the embedded RTMP server: a real TCP
//! watcher (blocking rml_rtmp `ClientSession`) plays streams published by a
//! real in-process FFmpeg job. Offline only — everything runs on 127.0.0.1.

use bytes::Bytes;
use ez_ffmpeg::rtmp::embed_rtmp_server::EmbedRtmpServer;
use ez_ffmpeg::{FfmpegContext, Input};
use rml_rtmp::handshake::{Handshake, HandshakeProcessResult, PeerType};
use rml_rtmp::sessions::{
    ClientSession, ClientSessionConfig, ClientSessionEvent, ClientSessionResult,
};
use std::io::{ErrorKind, Read, Write};
use std::net::{SocketAddr, TcpStream};
use std::time::{Duration, Instant};

/// Generous upper bound for any single wait; the tests normally finish in a
/// small fraction of this.
const WATCHDOG: Duration = Duration::from_secs(30);

/// Events a watcher records, in arrival order.
#[derive(Debug)]
enum WatcherEvent {
    Video(Bytes),
    Audio,
    Metadata,
    Status(String),
}

/// A minimal blocking RTMP subscriber: TCP + client handshake + `connect` +
/// `play`, recording every media/status event the session raises.
struct Watcher {
    stream: TcpStream,
    session: ClientSession,
    events: Vec<WatcherEvent>,
    connected: bool,
    playing: bool,
    eof: bool,
}

impl Watcher {
    /// Connect to `addr`, complete the RTMP handshake, connect to `app` and
    /// request playback of `stream_key`. Panics (with context) on any failure
    /// or when `watchdog` expires.
    fn connect(addr: SocketAddr, app: &str, stream_key: &str, watchdog: Duration) -> Watcher {
        let deadline = Instant::now() + watchdog;
        let mut stream = TcpStream::connect(addr).expect("watcher connect");
        stream.set_nodelay(true).ok();
        // Short read timeout so the pump loops can re-check their deadline.
        stream
            .set_read_timeout(Some(Duration::from_millis(100)))
            .expect("set read timeout");

        // Client handshake.
        let mut handshake = Handshake::new(PeerType::Client);
        let c0c1 = handshake
            .generate_outbound_p0_and_p1()
            .expect("handshake c0+c1");
        stream.write_all(&c0c1).expect("send c0+c1");
        let mut buf = [0u8; 8192];
        let leftover = loop {
            let n = read_some(&mut stream, &mut buf, deadline);
            assert!(n > 0, "server closed during handshake");
            match handshake.process_bytes(&buf[..n]).expect("handshake") {
                HandshakeProcessResult::InProgress { response_bytes } => {
                    if !response_bytes.is_empty() {
                        stream.write_all(&response_bytes).expect("handshake send");
                    }
                }
                HandshakeProcessResult::Completed {
                    response_bytes,
                    remaining_bytes,
                } => {
                    if !response_bytes.is_empty() {
                        stream.write_all(&response_bytes).expect("handshake send");
                    }
                    break remaining_bytes;
                }
            }
        };

        let (session, initial_results) =
            ClientSession::new(ClientSessionConfig::new()).expect("client session");
        let mut watcher = Watcher {
            stream,
            session,
            events: Vec::new(),
            connected: false,
            playing: false,
            eof: false,
        };
        watcher.apply(initial_results);
        watcher.feed(&leftover);

        let result = watcher
            .session
            .request_connection(app.to_string())
            .expect("request_connection");
        watcher.apply(vec![result]);
        watcher.pump_until(watchdog, |w| w.connected);
        assert!(watcher.connected, "server never accepted the connection");

        let result = watcher
            .session
            .request_playback(stream_key.to_string())
            .expect("request_playback");
        watcher.apply(vec![result]);
        watcher.pump_until(watchdog, |w| w.playing);
        assert!(watcher.playing, "server never accepted the play request");
        watcher
    }

    /// Send outbound packets and record raised events.
    fn apply(&mut self, results: Vec<ClientSessionResult>) {
        for result in results {
            match result {
                ClientSessionResult::OutboundResponse(packet) => {
                    self.stream
                        .write_all(&packet.bytes)
                        .expect("watcher send to server");
                }
                ClientSessionResult::RaisedEvent(event) => match event {
                    ClientSessionEvent::ConnectionRequestAccepted => self.connected = true,
                    ClientSessionEvent::PlaybackRequestAccepted => self.playing = true,
                    ClientSessionEvent::VideoDataReceived { data, .. } => {
                        self.events.push(WatcherEvent::Video(data))
                    }
                    ClientSessionEvent::AudioDataReceived { .. } => {
                        self.events.push(WatcherEvent::Audio)
                    }
                    ClientSessionEvent::StreamMetadataReceived { .. } => {
                        self.events.push(WatcherEvent::Metadata)
                    }
                    ClientSessionEvent::UnhandleableOnStatusCode { code } => {
                        self.events.push(WatcherEvent::Status(code))
                    }
                    _ => {}
                },
                ClientSessionResult::UnhandleableMessageReceived(_) => {}
            }
        }
    }

    fn feed(&mut self, bytes: &[u8]) {
        if bytes.is_empty() {
            return;
        }
        let results = self.session.handle_input(bytes).expect("handle_input");
        self.apply(results);
    }

    /// Pump socket reads until `done(self)`, EOF, or the watchdog expires.
    fn pump_until(&mut self, watchdog: Duration, done: impl Fn(&Watcher) -> bool) {
        let deadline = Instant::now() + watchdog;
        let mut buf = [0u8; 8192];
        while !done(self) && !self.eof {
            assert!(
                Instant::now() < deadline,
                "watchdog expired; events so far: {:?}",
                self.events
            );
            match self.stream.read(&mut buf) {
                Ok(0) => self.eof = true,
                Ok(n) => {
                    let bytes = buf[..n].to_vec();
                    self.feed(&bytes);
                }
                Err(ref e)
                    if e.kind() == ErrorKind::WouldBlock || e.kind() == ErrorKind::TimedOut => {}
                Err(e) => panic!("watcher socket error: {e:?}"),
            }
        }
    }

    fn video_payloads(&self) -> Vec<&Bytes> {
        self.events
            .iter()
            .filter_map(|e| match e {
                WatcherEvent::Video(data) => Some(data),
                _ => None,
            })
            .collect()
    }

    /// Received video tags that carry a real H.264 IDR (an AVCC NAL of type 5),
    /// not merely an FLV keyframe-flagged tag.
    fn idr_count(&self) -> usize {
        self.video_payloads()
            .iter()
            .filter(|d| is_h264_idr(d))
            .count()
    }
}

/// Whether an FLV/RTMP video tag payload is a real H.264 IDR access unit.
///
/// A keyframe-flagged AVC NALU tag (`0x17 0x01`) is necessary but not
/// sufficient: this parses past the 5-byte FLV AVC tag header (frame-type/codec
/// byte, AVCPacketType, 3-byte composition time) and walks the length-prefixed
/// AVCC NAL units (4-byte big-endian length + body), returning true only if at
/// least one NAL has `nal_unit_type == 5` (IDR slice). Bounds-checked: malformed
/// data yields false, never a panic.
fn is_h264_idr(tag: &[u8]) -> bool {
    // FLV AVC tag header: 0x17 = keyframe + AVC codec, 0x01 = AVCPacketType NALU.
    if tag.len() < 5 || tag[0] != 0x17 || tag[1] != 0x01 {
        return false;
    }
    let mut i = 5; // skip the 1-byte header, 1-byte packet type, 3-byte CTS
    while i + 4 <= tag.len() {
        let len = u32::from_be_bytes([tag[i], tag[i + 1], tag[i + 2], tag[i + 3]]) as usize;
        i += 4;
        // checked_add so a corrupt/absurd NAL length near usize::MAX (a u32 len
        // fills the whole width on a 32-bit target) fails closed instead of
        // overflowing i + len: malformed data yields false, never a panic.
        match i.checked_add(len) {
            Some(end) if len != 0 && end <= tag.len() => {
                // NAL unit header byte: low 5 bits are nal_unit_type; 5 == IDR.
                if tag[i] & 0x1f == 5 {
                    return true;
                }
                i = end;
            }
            _ => break,
        }
    }
    false
}

#[test]
fn is_h264_idr_rejects_malformed_avcc_without_panicking() {
    // Too short / not a keyframe-AVC-NALU tag.
    assert!(!is_h264_idr(b""));
    assert!(!is_h264_idr(&[0x27, 0x01, 0, 0, 0]));
    // Keyframe AVC NALU tag but an absurd NAL length (0xFFFFFFFF): the walker
    // must fail closed, never overflow i + len nor index out of bounds.
    assert!(!is_h264_idr(&[
        0x17, 0x01, 0, 0, 0, 0xFF, 0xFF, 0xFF, 0xFF, 0x65
    ]));
    // Truncated: the length prefix (100) runs past the buffer.
    assert!(!is_h264_idr(&[
        0x17, 0x01, 0, 0, 0, 0x00, 0x00, 0x00, 0x64, 0x65
    ]));
    // A well-formed single IDR NAL (len 1, nal_unit_type 5) is still detected.
    assert!(is_h264_idr(&[
        0x17, 0x01, 0, 0, 0, 0x00, 0x00, 0x00, 0x01, 0x65
    ]));
    // A well-formed non-IDR NAL (nal_unit_type 1) is not an IDR.
    assert!(!is_h264_idr(&[
        0x17, 0x01, 0, 0, 0, 0x00, 0x00, 0x00, 0x01, 0x41
    ]));
}

/// One blocking read that retries timeouts until `deadline`.
fn read_some(stream: &mut TcpStream, buf: &mut [u8], deadline: Instant) -> usize {
    loop {
        assert!(Instant::now() < deadline, "watchdog expired in read");
        match stream.read(buf) {
            Ok(n) => return n,
            Err(ref e) if e.kind() == ErrorKind::WouldBlock || e.kind() == ErrorKind::TimedOut => {}
            Err(e) => panic!("watcher socket error: {e:?}"),
        }
    }
}

/// A joiner arriving mid-stream must first receive the AVC sequence header
/// and then an IDR before any delta frame — the replayed GOP burst (frozen
/// GOPs plus the open one) is what makes its picture decodable immediately.
#[test]
fn late_joiner_gets_headers_then_idr_first() {
    let server = EmbedRtmpServer::new_with_gop_limit("127.0.0.1:0", 2)
        .start()
        .expect("server start");
    let addr = server.local_addr().expect("bound address");
    let output = server.create_rtmp_input("app", "live").expect("rtmp input");

    // Loop the clip endlessly at full speed (no readrate pacing) so the late
    // joiner reliably lands mid-stream; abort() ends the job at the end.
    let scheduler = FfmpegContext::builder()
        .input(Input::from("test.mp4").set_stream_loop(-1))
        .output(output)
        .build()
        .expect("context")
        .start()
        .expect("ffmpeg start");

    // An early watcher proves media flows and waits until at least one GOP
    // boundary passed (two IDRs seen => the first GOP is frozen).
    let mut early = Watcher::connect(addr, "app", "live", WATCHDOG);
    early.pump_until(WATCHDOG, |w| w.idr_count() >= 2);
    assert!(early.idr_count() >= 2, "publisher never produced two IDRs");

    // The late joiner: first video tag must be the sequence header, and no
    // delta may precede the first IDR.
    let mut late = Watcher::connect(addr, "app", "live", WATCHDOG);
    late.pump_until(WATCHDOG, |w| w.video_payloads().len() >= 2);

    let videos = late.video_payloads();
    assert!(videos.len() >= 2, "late joiner received too little video");
    assert!(
        videos[0].len() >= 2 && videos[0][0] == 0x17 && videos[0][1] == 0x00,
        "first video tag must be the AVC sequence header, got {:02x?}",
        &videos[0][..videos[0].len().min(2)]
    );
    let first_nalu = videos
        .iter()
        .find(|d| !(d.len() >= 2 && d[0] == 0x17 && d[1] == 0x00))
        .expect("a video NALU after the sequence header");
    assert!(
        is_h264_idr(first_nalu),
        "the first NALU after the sequence header must be a real IDR (AVCC NAL type 5): {:02x?}",
        &first_nalu[..first_nalu.len().min(8)]
    );

    scheduler.abort();
    server.stop();
}

/// When the publisher finishes, every watcher must still receive the
/// play-complete status before the server closes the socket — a raw close
/// used to race the status packet away.
#[test]
fn publisher_finish_delivers_stream_eof_to_watcher() {
    let server = EmbedRtmpServer::new_with_gop_limit("127.0.0.1:0", 2)
        .start()
        .expect("server start");
    let addr = server.local_addr().expect("bound address");
    let output = server.create_rtmp_input("app", "live").expect("rtmp input");

    // Join before any media so the finite full-speed publish cannot win the
    // race against the watcher's handshake.
    let mut watcher = Watcher::connect(addr, "app", "live", WATCHDOG);

    let scheduler = FfmpegContext::builder()
        .input(Input::from("test.mp4"))
        .output(output)
        .build()
        .expect("context")
        .start()
        .expect("ffmpeg start");
    scheduler.wait().expect("publish completes");

    // Publisher done: the watcher must observe the completion status and
    // then an orderly EOF.
    watcher.pump_until(WATCHDOG, |w| w.eof);
    assert!(watcher.eof, "watcher never reached EOF");
    assert!(
        watcher
            .events
            .iter()
            .any(|e| matches!(e, WatcherEvent::Status(code) if code == "NetStream.Play.Complete")),
        "the play-complete status must arrive before the close; events: {:?}",
        watcher
            .events
            .iter()
            .map(|e| match e {
                WatcherEvent::Video(_) => "video",
                WatcherEvent::Audio => "audio",
                WatcherEvent::Metadata => "metadata",
                WatcherEvent::Status(code) => code.as_str(),
            })
            .collect::<Vec<_>>()
    );
    assert!(
        !watcher.video_payloads().is_empty(),
        "media must have flowed before the finish"
    );

    server.stop();
}

/// A StreamBuilder session must release its port once the handle is waited
/// on and dropped (the audit's AddrInUse reproduction).
#[test]
fn stream_builder_session_releases_port() {
    // Bind port 0 and read the OS-assigned port back from the running server —
    // no probe listener, so no reserve/drop/rebind window for a parallel test
    // to steal the port into a flaky AddrInUse.
    let handle = EmbedRtmpServer::stream_builder()
        .address("127.0.0.1:0")
        .app_name("app")
        .stream_key("live")
        .input_file("test.mp4")
        // Keep the pacing negligible: the builder defaults to realtime (1.0).
        .readrate(64.0)
        .gop_limit(2)
        .start()
        .expect("stream builder start");

    let addr = handle.local_addr().expect("server bound address");

    handle.wait().expect("stream completes");

    // wait() consumed the handle, so its Drop already signaled the server;
    // the port must become bindable again.
    let deadline = Instant::now() + WATCHDOG;
    loop {
        match std::net::TcpListener::bind(addr) {
            Ok(_) => break,
            Err(e) => {
                assert!(
                    Instant::now() < deadline,
                    "port not released after StreamHandle wait+drop: {e:?}"
                );
                std::thread::sleep(Duration::from_millis(20));
            }
        }
    }
}
// The post-start failure path (a StreamBuilder that starts the server but then
// fails to build the FFmpeg job) is verified race-free as a module-internal
// test in `embed_rtmp_server.rs` (`server_stop_guard_drop_releases_the_port`):
// it drives the RAII ServerStopGuard directly on a port-0 server, with no
// probe/drop/rebind window a parallel test could steal.

// ===========================================================================
// Load harness (ignored benches)
// ===========================================================================
//
// End-to-end RTMP fanout load measurement over real TCP subscribers. A
// synthetic paced publisher feeds the embedded server through
// `create_stream_sender` (raw RTMP chunk bytes, the same channel a raw
// in-process publisher uses), and W subscriber threads each run a real
// blocking rml_rtmp `ClientSession` — every byte the server emits is fully
// chunk-deserialized, so any wire corruption fails the run loudly.
//
// Run one scenario at a time (they saturate cores by design):
//
// ```text
// cargo test --release --features rtmp --test rtmp_loopback bench_rtmp_load \
//     -- --ignored --nocapture --test-threads=1
// ```
//
// Media plan (~4.6 Mbps, mirroring a 4 Mbps 30 fps camera):
// - video 30 fps: one 64 KiB keyframe every 30 frames, 16 KiB deltas,
// - audio 43 tags/s: 512 B each (AAC-frame cadence).
//
// Each media tag embeds a sequence number and a monotonic nanosecond stamp
// right after its FLV tag header, so subscribers measure glass-to-glass
// latency (publisher build -> client deserialize, one process, one clock)
// and detect shedding drops (sequence gaps) without server cooperation.
//
// Reported per scenario (machine-diffable `load_report,` lines):
// - reactor thread CPU (schedstat runtime delta) and context switches,
// - glass-to-glass p50/p99/max across the fast subscribers,
// - per-class sent/received counts and gap-derived drop estimates,
// - publisher send stalls (bounded-channel backpressure).
//
// Queue depth is not directly observable from outside the reactor; on slow
// readers it is proxied by their glass-to-glass latency (queued bytes ~=
// latency x bitrate).
//
// System-level counters (cycles, instructions, task-clock, migrations,
// writev syscalls) come from `perf stat` attached to the reactor thread
// while a scenario runs; the harness prints the thread id and the exact
// command at start:
//
// ```text
// perf stat -t <reactor-tid> \
//   -e cycles:u,cycles:k,instructions:u,instructions:k,task-clock \
//   -e context-switches,cpu-migrations,syscalls:sys_enter_writev \
//   -- sleep <window-secs>
// ```

use ez_ffmpeg::rtmp::embed_rtmp_server::RtmpStreamSender;
use rml_rtmp::chunk_io::ChunkSerializer;
use rml_rtmp::messages::RtmpMessage;
use rml_rtmp::time::RtmpTimestamp;
use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};
use std::sync::{Arc, OnceLock};

/// Process-wide epoch for the embedded latency stamps: every thread derives
/// nanosecond offsets from the same `Instant`, so publisher stamps and
/// subscriber reads are directly comparable.
fn epoch() -> Instant {
    static EPOCH: OnceLock<Instant> = OnceLock::new();
    *EPOCH.get_or_init(Instant::now)
}

fn nanos_since_epoch() -> u64 {
    epoch().elapsed().as_nanos() as u64
}

// --- media plan ------------------------------------------------------------

const LOAD_VIDEO_FPS: u64 = 30;
const LOAD_KEY_INTERVAL: u64 = 30; // one keyframe per second
const LOAD_KEY_BYTES: usize = 64 * 1024;
const LOAD_DELTA_BYTES: usize = 16 * 1024;
const LOAD_AUDIO_FPS: u64 = 43;
const LOAD_AUDIO_BYTES: usize = 512;

/// FLV video tag body: frame-type/codec byte, AVCPacketType 1 (NALU),
/// 3-byte CTS, then one length-prefixed AVCC NAL. The NAL body carries the
/// sequence number and the latency stamp; the rest is filler.
fn video_tag(keyframe: bool, seq: u32, total_len: usize) -> Vec<u8> {
    let mut v = vec![0u8; total_len.max(22)];
    v[0] = if keyframe { 0x17 } else { 0x27 };
    v[1] = 0x01;
    // v[2..5] CTS = 0
    let nal_len = (v.len() - 9) as u32;
    v[5..9].copy_from_slice(&nal_len.to_be_bytes());
    v[9] = if keyframe { 0x65 } else { 0x41 }; // IDR / non-IDR slice header
    v[10..14].copy_from_slice(&seq.to_be_bytes());
    v[14..22].copy_from_slice(&nanos_since_epoch().to_be_bytes());
    v
}

/// FLV audio tag body: AAC raw frame with the same seq + stamp layout.
fn audio_tag(seq: u32, total_len: usize) -> Vec<u8> {
    let mut v = vec![0u8; total_len.max(14)];
    v[0] = 0xAF;
    v[1] = 0x01;
    v[2..6].copy_from_slice(&seq.to_be_bytes());
    v[6..14].copy_from_slice(&nanos_since_epoch().to_be_bytes());
    v
}

fn video_sequence_header_tag() -> Vec<u8> {
    // 0x17 0x00 = keyframe-flagged AVC sequence header; body is a nominal
    // avcC blob (the harness clients never decode it).
    let mut v = vec![0u8; 46];
    v[0] = 0x17;
    v[1] = 0x00;
    v[5] = 0x01; // avcC version
    v
}

fn audio_sequence_header_tag() -> Vec<u8> {
    vec![0xAF, 0x00, 0x12, 0x10] // AAC AudioSpecificConfig (LC, 44.1k, stereo)
}

/// Parse (seq, stamp) back out of a received media payload.
fn parse_video_stamp(data: &[u8]) -> Option<(u32, u64)> {
    if data.len() < 22 || data[1] != 0x01 {
        return None; // sequence header or foreign tag
    }
    let seq = u32::from_be_bytes(data[10..14].try_into().ok()?);
    let stamp = u64::from_be_bytes(data[14..22].try_into().ok()?);
    Some((seq, stamp))
}

fn parse_audio_stamp(data: &[u8]) -> Option<(u32, u64)> {
    if data.len() < 14 || data[1] != 0x01 {
        return None;
    }
    let seq = u32::from_be_bytes(data[2..6].try_into().ok()?);
    let stamp = u64::from_be_bytes(data[6..14].try_into().ok()?);
    Some((seq, stamp))
}

// --- paced publisher ---------------------------------------------------------

struct PublisherReport {
    video_sent: u64,
    audio_sent: u64,
    /// Total wall time `RtmpStreamSender::send` spent blocked on the bounded
    /// publisher channel (reactor-side backpressure).
    stall: Duration,
}

/// Serialize one media message the way a raw RTMP publisher would and send
/// it into the server. Returns the time spent blocked in `send`.
fn publish_tag(
    sender: &RtmpStreamSender,
    serializer: &mut ChunkSerializer,
    is_video: bool,
    body: Vec<u8>,
    ts_ms: u32,
) -> Duration {
    let data = Bytes::from(body);
    let msg = if is_video {
        RtmpMessage::VideoData { data }
    } else {
        RtmpMessage::AudioData { data }
    };
    let payload = msg
        .into_message_payload(RtmpTimestamp::new(ts_ms), 1)
        .expect("payload conversion");
    let packet = serializer
        .serialize(&payload, false, true)
        .expect("publisher serialize");
    let t0 = Instant::now();
    sender.send(packet.bytes).expect("publisher send");
    t0.elapsed()
}

/// Paced publisher loop: absolute-deadline scheduling of the video and audio
/// cadences until `stop` is set. Sequence headers are sent first so late
/// joiners always receive a decodable prefix.
fn run_publisher(sender: RtmpStreamSender, stop: Arc<AtomicBool>) -> PublisherReport {
    let mut serializer = ChunkSerializer::new();
    let announce = serializer
        .set_max_chunk_size(4096, RtmpTimestamp::new(0))
        .expect("publisher chunk size");
    sender.send(announce.bytes).expect("send SetChunkSize");

    for (is_video, body) in [
        (true, video_sequence_header_tag()),
        (false, audio_sequence_header_tag()),
    ] {
        let data = Bytes::from(body);
        let msg = if is_video {
            RtmpMessage::VideoData { data }
        } else {
            RtmpMessage::AudioData { data }
        };
        let payload = msg
            .into_message_payload(RtmpTimestamp::new(0), 1)
            .expect("payload conversion");
        // Sequence headers are the non-droppable prefix, like real encoders.
        let packet = serializer
            .serialize(&payload, false, false)
            .expect("publisher serialize");
        sender.send(packet.bytes).expect("send sequence header");
    }

    let start = Instant::now();
    let video_interval = Duration::from_nanos(1_000_000_000 / LOAD_VIDEO_FPS);
    let audio_interval = Duration::from_nanos(1_000_000_000 / LOAD_AUDIO_FPS);
    let mut next_video = start;
    let mut next_audio = start;
    let mut video_seq = 0u32;
    let mut audio_seq = 0u32;
    let mut stall = Duration::ZERO;

    while !stop.load(Ordering::Relaxed) {
        let now = Instant::now();
        if now >= next_video {
            let keyframe = (video_seq as u64).is_multiple_of(LOAD_KEY_INTERVAL);
            let size = if keyframe {
                LOAD_KEY_BYTES
            } else {
                LOAD_DELTA_BYTES
            };
            let ts_ms = start.elapsed().as_millis() as u32;
            stall += publish_tag(
                &sender,
                &mut serializer,
                true,
                video_tag(keyframe, video_seq, size),
                ts_ms,
            );
            video_seq = video_seq.wrapping_add(1);
            next_video += video_interval;
            continue;
        }
        if now >= next_audio {
            let ts_ms = start.elapsed().as_millis() as u32;
            stall += publish_tag(
                &sender,
                &mut serializer,
                false,
                audio_tag(audio_seq, LOAD_AUDIO_BYTES),
                ts_ms,
            );
            audio_seq = audio_seq.wrapping_add(1);
            next_audio += audio_interval;
            continue;
        }
        let next = next_video.min(next_audio);
        std::thread::sleep(next.saturating_duration_since(now).min(Duration::from_millis(5)));
    }

    PublisherReport {
        video_sent: video_seq as u64,
        audio_sent: audio_seq as u64,
        stall,
    }
}

// --- subscriber threads ------------------------------------------------------

#[derive(Clone, Copy, PartialEq)]
enum ReaderKind {
    /// Drains the socket as fast as the server can fill it.
    Fast,
    /// Stalls (reads nothing) long enough to exhaust kernel socket buffers
    /// and push the server's write queue into the shedding bands, then
    /// drains fast. The drain traverses the shed region, so the sequence
    /// gaps the backpressure policy created become observable inside the
    /// measurement window — a reader merely throttled below the stream
    /// rate would spend the whole window lagging through intact buffered
    /// data and report zero drops.
    Slow,
}

/// Approximate wire rate of the media plan (payload + chunk framing),
/// used to size the slow readers' stall.
const LOAD_WIRE_BYTES_PER_SEC: u64 = 560_000;

/// How long a slow reader must stall so the unread bytes exceed every
/// buffer between the server's write queue and the client: the TCP
/// autotune caps on both sides plus a 2 MiB margin that lands the queue
/// itself well into the Warning band (shedding) while staying far from
/// the Critical disconnect cap.
fn slow_reader_stall() -> Duration {
    #[cfg(target_os = "linux")]
    fn max_socket_buffers() -> u64 {
        fn third_field(path: &str) -> Option<u64> {
            std::fs::read_to_string(path)
                .ok()?
                .split_whitespace()
                .nth(2)?
                .parse()
                .ok()
        }
        let wmem = third_field("/proc/sys/net/ipv4/tcp_wmem").unwrap_or(4 * 1024 * 1024);
        let rmem = third_field("/proc/sys/net/ipv4/tcp_rmem").unwrap_or(6 * 1024 * 1024);
        wmem + rmem
    }
    #[cfg(not(target_os = "linux"))]
    fn max_socket_buffers() -> u64 {
        16 * 1024 * 1024
    }

    let stall_bytes = max_socket_buffers() + 2 * 1024 * 1024;
    let secs = stall_bytes as f64 / LOAD_WIRE_BYTES_PER_SEC as f64 + 2.0;
    Duration::from_secs_f64(secs.clamp(8.0, 30.0))
}

#[derive(Default)]
struct ClassWindow {
    received: u64,
    first_seq: Option<u32>,
    last_seq: Option<u32>,
}

impl ClassWindow {
    fn record(&mut self, seq: u32) {
        self.received += 1;
        if self.first_seq.is_none() {
            self.first_seq = Some(seq);
        }
        self.last_seq = Some(seq);
    }

    /// Tags the publisher emitted inside this watcher's observed span but
    /// that never arrived — the shedding-drop estimate (per-connection TCP
    /// preserves order, so span - received = dropped).
    fn gap_drops(&self) -> u64 {
        match (self.first_seq, self.last_seq) {
            (Some(first), Some(last)) => {
                let span = last.wrapping_sub(first) as u64 + 1;
                span.saturating_sub(self.received)
            }
            _ => 0,
        }
    }
}

struct SubscriberReport {
    kind: ReaderKind,
    video: ClassWindow,
    audio: ClassWindow,
    /// Glass-to-glass samples (ns), fast readers only.
    latencies: Vec<u64>,
    /// Highest stamp age seen on any received tag (slow readers): the
    /// externally observable proxy for how deep the server-side backlog
    /// (kernel buffers + write queue) grew during the stall.
    peak_lag_ns: u64,
    unexpected_eof: bool,
}

/// Subscribe, then pump reads until `stop`, recording in-window stats while
/// `recording` is set. Fast readers drain continuously; slow readers stall
/// for `slow_stall`, then drain.
fn run_subscriber(
    addr: SocketAddr,
    kind: ReaderKind,
    slow_stall: Duration,
    stop: Arc<AtomicBool>,
    recording: Arc<AtomicBool>,
    connected: Arc<AtomicUsize>,
    watchdog: Duration,
) -> SubscriberReport {
    let mut watcher = Watcher::connect(addr, "app", "live", watchdog);
    connected.fetch_add(1, Ordering::SeqCst);
    // The connect phase recorded a handful of burst events; drop them, the
    // load loop below keeps counters instead of a growing event log.
    watcher.events.clear();

    let mut report = SubscriberReport {
        kind,
        video: ClassWindow::default(),
        audio: ClassWindow::default(),
        latencies: Vec::with_capacity(4096),
        peak_lag_ns: 0,
        unexpected_eof: false,
    };

    if kind == ReaderKind::Slow {
        // Stall phase: leave the socket untouched so the backlog builds.
        let stall_deadline = Instant::now() + slow_stall;
        while Instant::now() < stall_deadline && !stop.load(Ordering::Relaxed) {
            std::thread::sleep(Duration::from_millis(250));
        }
    }

    let mut buf = vec![0u8; 64 * 1024];
    while !stop.load(Ordering::Relaxed) {
        let n = match watcher.stream.read(&mut buf) {
            Ok(0) => {
                report.unexpected_eof = !stop.load(Ordering::Relaxed);
                break;
            }
            Ok(n) => n,
            Err(ref e) if e.kind() == ErrorKind::WouldBlock || e.kind() == ErrorKind::TimedOut => {
                continue;
            }
            Err(e) => panic!("subscriber socket error: {e:?}"),
        };
        // Full RTMP chunk deserialization — the correctness canary under
        // load: a corrupted chunk stream panics the run here.
        let results = watcher.session.handle_input(&buf[..n]).expect("handle_input");
        let now_ns = nanos_since_epoch();
        let is_recording = recording.load(Ordering::Relaxed);
        for result in results {
            match result {
                ClientSessionResult::OutboundResponse(packet) => {
                    watcher
                        .stream
                        .write_all(&packet.bytes)
                        .expect("subscriber send to server");
                }
                ClientSessionResult::RaisedEvent(event) => match event {
                    ClientSessionEvent::VideoDataReceived { data, .. } => {
                        if !is_recording {
                            continue;
                        }
                        if let Some((seq, stamp)) = parse_video_stamp(&data) {
                            report.video.record(seq);
                            let age = now_ns.saturating_sub(stamp);
                            match kind {
                                ReaderKind::Fast => report.latencies.push(age),
                                ReaderKind::Slow => {
                                    report.peak_lag_ns = report.peak_lag_ns.max(age)
                                }
                            }
                        }
                    }
                    ClientSessionEvent::AudioDataReceived { data, .. } => {
                        if !is_recording {
                            continue;
                        }
                        if let Some((seq, stamp)) = parse_audio_stamp(&data) {
                            report.audio.record(seq);
                            let age = now_ns.saturating_sub(stamp);
                            match kind {
                                ReaderKind::Fast => report.latencies.push(age),
                                ReaderKind::Slow => {
                                    report.peak_lag_ns = report.peak_lag_ns.max(age)
                                }
                            }
                        }
                    }
                    _ => {}
                },
                ClientSessionResult::UnhandleableMessageReceived(_) => {}
            }
        }
    }
    report
}

// --- reactor-thread observation ---------------------------------------------

#[derive(Clone, Copy, Default)]
struct ReactorSnapshot {
    /// Cumulative on-CPU time (ns) from /proc schedstat.
    runtime_ns: u64,
    voluntary_switches: u64,
    involuntary_switches: u64,
}

/// The reactor thread's kernel tid, found by its `rtmp-server-worker` name
/// (`/proc/.../comm` truncates to 15 chars).
#[cfg(target_os = "linux")]
fn reactor_tid() -> Option<u64> {
    let tasks = std::fs::read_dir("/proc/self/task").ok()?;
    for task in tasks.flatten() {
        // Tasks come and go while we scan; skip anything unreadable instead
        // of giving up on the whole lookup.
        let Ok(tid) = task.file_name().to_string_lossy().parse::<u64>() else {
            continue;
        };
        let comm = std::fs::read_to_string(task.path().join("comm")).unwrap_or_default();
        if comm.trim() == "rtmp-server-wor" || comm.trim() == "rtmp-server-worker" {
            return Some(tid);
        }
    }
    None
}

#[cfg(not(target_os = "linux"))]
fn reactor_tid() -> Option<u64> {
    None
}

#[cfg(target_os = "linux")]
fn reactor_snapshot(tid: u64) -> ReactorSnapshot {
    let mut snapshot = ReactorSnapshot::default();
    let base = format!("/proc/self/task/{tid}");
    if let Ok(sched) = std::fs::read_to_string(format!("{base}/schedstat")) {
        let mut fields = sched.split_whitespace();
        snapshot.runtime_ns = fields.next().and_then(|f| f.parse().ok()).unwrap_or(0);
    }
    if let Ok(status) = std::fs::read_to_string(format!("{base}/status")) {
        for line in status.lines() {
            let mut kv = line.split_whitespace();
            match kv.next() {
                Some("voluntary_ctxt_switches:") => {
                    snapshot.voluntary_switches =
                        kv.next().and_then(|v| v.parse().ok()).unwrap_or(0);
                }
                Some("nonvoluntary_ctxt_switches:") => {
                    snapshot.involuntary_switches =
                        kv.next().and_then(|v| v.parse().ok()).unwrap_or(0);
                }
                _ => {}
            }
        }
    }
    snapshot
}

#[cfg(not(target_os = "linux"))]
fn reactor_snapshot(_tid: u64) -> ReactorSnapshot {
    ReactorSnapshot::default()
}

// --- scenario runner ----------------------------------------------------------

struct LoadScenario {
    name: &'static str,
    watchers: usize,
    slow_watchers: usize,
    warmup: Duration,
    window: Duration,
    /// Stall applied by the slow readers (see [`ReaderKind::Slow`]);
    /// `Duration::ZERO` for all-fast populations.
    slow_stall: Duration,
}

fn percentile(sorted: &[u64], p: f64) -> u64 {
    if sorted.is_empty() {
        return 0;
    }
    let rank = ((sorted.len() as f64 - 1.0) * p).round() as usize;
    sorted[rank.min(sorted.len() - 1)]
}

fn run_load_scenario(scenario: LoadScenario) {
    let watchdog = Duration::from_secs(120);
    let server = EmbedRtmpServer::new_with_gop_limit("127.0.0.1:0", 2)
        .start()
        .expect("server start");
    let addr = server.local_addr().expect("bound address");
    let sender = server
        .create_stream_sender("app", "live")
        .expect("stream sender");

    let tid = reactor_tid();
    println!("load_report,{},reactor_tid,{:?}", scenario.name, tid);
    if let Some(tid) = tid {
        println!(
            "load_report,{},perf_hint,perf stat -t {tid} \
             -e cycles:u,cycles:k,instructions:u,instructions:k,task-clock \
             -e context-switches,cpu-migrations,syscalls:sys_enter_writev \
             -- sleep {}",
            scenario.name,
            scenario.window.as_secs()
        );
    }

    let stop = Arc::new(AtomicBool::new(false));
    let recording = Arc::new(AtomicBool::new(false));
    let connected = Arc::new(AtomicUsize::new(0));

    let publisher = {
        let stop = stop.clone();
        std::thread::Builder::new()
            .name("load-publisher".into())
            .spawn(move || run_publisher(sender, stop))
            .expect("spawn publisher")
    };

    let mut subscribers = Vec::with_capacity(scenario.watchers);
    for i in 0..scenario.watchers {
        let kind = if i < scenario.slow_watchers {
            ReaderKind::Slow
        } else {
            ReaderKind::Fast
        };
        let stop = stop.clone();
        let recording = recording.clone();
        let connected = connected.clone();
        let slow_stall = scenario.slow_stall;
        let stagger = Duration::from_millis((i as u64) * if scenario.watchers >= 500 { 2 } else { 1 });
        subscribers.push(
            std::thread::Builder::new()
                .name(format!("load-sub-{i}"))
                .spawn(move || {
                    std::thread::sleep(stagger);
                    run_subscriber(addr, kind, slow_stall, stop, recording, connected, watchdog)
                })
                .expect("spawn subscriber"),
        );
    }

    // Wait for the full subscriber population before opening the window.
    let connect_deadline = Instant::now() + watchdog;
    while connected.load(Ordering::SeqCst) < scenario.watchers {
        assert!(
            Instant::now() < connect_deadline,
            "subscribers stuck connecting: {}/{}",
            connected.load(Ordering::SeqCst),
            scenario.watchers
        );
        std::thread::sleep(Duration::from_millis(20));
    }

    std::thread::sleep(scenario.warmup);
    let cpu_before = tid.map(reactor_snapshot);
    recording.store(true, Ordering::SeqCst);
    let window_start = Instant::now();
    std::thread::sleep(scenario.window);
    recording.store(false, Ordering::SeqCst);
    let wall = window_start.elapsed();
    let cpu_after = tid.map(reactor_snapshot);

    stop.store(true, Ordering::SeqCst);
    let publisher_report = publisher.join().expect("publisher thread");
    let mut reports = Vec::with_capacity(scenario.watchers);
    for handle in subscribers {
        reports.push(handle.join().expect("subscriber thread"));
    }
    server.stop();

    // --- aggregate -----------------------------------------------------------
    let name = scenario.name;
    println!(
        "load_report,{name},population,watchers={} slow={} window_secs={:.1}",
        scenario.watchers,
        scenario.slow_watchers,
        wall.as_secs_f64()
    );
    println!(
        "load_report,{name},publisher,video_sent={} audio_sent={} stall_ms={:.1}",
        publisher_report.video_sent,
        publisher_report.audio_sent,
        publisher_report.stall.as_secs_f64() * 1e3
    );

    if let (Some(before), Some(after)) = (cpu_before, cpu_after) {
        let cpu_ns = after.runtime_ns.saturating_sub(before.runtime_ns);
        println!(
            "load_report,{name},reactor_cpu,busy_ms={:.1} pct_core={:.1} vol_switch={} invol_switch={}",
            cpu_ns as f64 / 1e6,
            cpu_ns as f64 / wall.as_nanos() as f64 * 100.0,
            after.voluntary_switches.saturating_sub(before.voluntary_switches),
            after
                .involuntary_switches
                .saturating_sub(before.involuntary_switches),
        );
    }

    let mut all_latencies: Vec<u64> = Vec::new();
    let mut worst_fast_p99 = 0u64;
    let mut fast_video_rx = 0u64;
    let mut fast_audio_rx = 0u64;
    let mut fast_video_drops = 0u64;
    let mut slow_video_rx = 0u64;
    let mut slow_audio_rx = 0u64;
    let mut slow_video_drops = 0u64;
    let mut slow_audio_drops = 0u64;
    let mut slow_peak_lag_ns = 0u64;
    let mut eofs = 0u64;
    for report in &mut reports {
        if report.unexpected_eof {
            eofs += 1;
        }
        match report.kind {
            ReaderKind::Fast => {
                fast_video_rx += report.video.received;
                fast_audio_rx += report.audio.received;
                fast_video_drops += report.video.gap_drops();
                report.latencies.sort_unstable();
                worst_fast_p99 = worst_fast_p99.max(percentile(&report.latencies, 0.99));
                all_latencies.append(&mut report.latencies);
            }
            ReaderKind::Slow => {
                slow_video_rx += report.video.received;
                slow_audio_rx += report.audio.received;
                slow_video_drops += report.video.gap_drops();
                slow_audio_drops += report.audio.gap_drops();
                slow_peak_lag_ns = slow_peak_lag_ns.max(report.peak_lag_ns);
            }
        }
    }
    all_latencies.sort_unstable();
    println!(
        "load_report,{name},fast_readers,video_rx={fast_video_rx} audio_rx={fast_audio_rx} video_gap_drops={fast_video_drops}"
    );
    println!(
        "load_report,{name},slow_readers,video_rx={slow_video_rx} audio_rx={slow_audio_rx} video_gap_drops={slow_video_drops} audio_gap_drops={slow_audio_drops} peak_lag_ms={:.0}",
        slow_peak_lag_ns as f64 / 1e6
    );
    println!(
        "load_report,{name},glass_to_glass_ms,p50={:.2} p99={:.2} max={:.2} worst_watcher_p99={:.2} samples={}",
        percentile(&all_latencies, 0.50) as f64 / 1e6,
        percentile(&all_latencies, 0.99) as f64 / 1e6,
        all_latencies.last().copied().unwrap_or(0) as f64 / 1e6,
        worst_fast_p99 as f64 / 1e6,
        all_latencies.len()
    );
    println!("load_report,{name},unexpected_eof,{eofs}");

    // Sanity floor, not a benchmark assertion: whoever is present must have
    // seen real traffic, nobody may have been disconnected, and a stalling
    // population must have pushed the server into observable shedding (the
    // stall is sized past every buffer in the path, so zero drops would
    // mean the backpressure policy did not engage).
    if scenario.watchers > scenario.slow_watchers {
        assert!(
            fast_video_rx > 0,
            "no video reached the fast readers — the harness itself is broken"
        );
    }
    if scenario.slow_watchers > 0 {
        assert!(
            slow_video_rx > 0,
            "no video reached the slow readers — the harness itself is broken"
        );
        assert!(
            slow_video_drops > 0,
            "stalled readers observed zero gaps — shedding never engaged"
        );
    }
    assert_eq!(eofs, 0, "a subscriber was disconnected mid-run");
}

#[test]
#[ignore]
fn bench_rtmp_load_fast_w10() {
    run_load_scenario(LoadScenario {
        name: "fast_w10",
        watchers: 10,
        slow_watchers: 0,
        warmup: Duration::from_secs(2),
        window: Duration::from_secs(10),
        slow_stall: Duration::ZERO,
    });
}

#[test]
#[ignore]
fn bench_rtmp_load_fast_w100() {
    run_load_scenario(LoadScenario {
        name: "fast_w100",
        watchers: 100,
        slow_watchers: 0,
        warmup: Duration::from_secs(2),
        window: Duration::from_secs(10),
        slow_stall: Duration::ZERO,
    });
}

#[test]
#[ignore]
fn bench_rtmp_load_slow_w10() {
    // All-slow population: every reader stalls past the buffer capacity of
    // its path, forcing the tiered backpressure into shedding, then drains
    // to observe the gaps. The window covers the stall plus the drain.
    let stall = slow_reader_stall();
    run_load_scenario(LoadScenario {
        name: "slow_w10",
        watchers: 10,
        slow_watchers: 10,
        warmup: Duration::from_secs(2),
        window: stall + Duration::from_secs(8),
        slow_stall: stall,
    });
}

#[test]
#[ignore]
fn bench_rtmp_load_mixed_w100() {
    // 80/20 fast/slow: the isolation property under test is that stalled
    // readers shed on their own queues while fast readers keep a low p99.
    let stall = slow_reader_stall();
    run_load_scenario(LoadScenario {
        name: "mixed_w100",
        watchers: 100,
        slow_watchers: 20,
        warmup: Duration::from_secs(2),
        window: stall + Duration::from_secs(8),
        slow_stall: stall,
    });
}

#[test]
#[ignore]
fn bench_rtmp_load_fast_w1000() {
    run_load_scenario(LoadScenario {
        name: "fast_w1000",
        watchers: 1000,
        slow_watchers: 0,
        warmup: Duration::from_secs(4),
        window: Duration::from_secs(10),
        slow_stall: Duration::ZERO,
    });
}
