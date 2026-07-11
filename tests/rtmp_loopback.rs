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
