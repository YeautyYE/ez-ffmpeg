//! Integration tests for the default-off `http-input` feature.
//!
//! `cargo test` without the feature compiles this file as empty. Network
//! fixtures bind 127.0.0.1 only.

#![cfg(feature = "http-input")]

use ez_ffmpeg::http_input::{
    HttpClient, HttpInput, HttpInputError, ManifestKind, ProxyConfig, ProxyPolicy, ReconnectPolicy,
    RootPolicy,
};
use ez_ffmpeg::{FfmpegContext, Input, Output};
use std::io::{BufRead, Read, Write};
use std::net::TcpListener;
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};
use std::sync::{Arc, Mutex};
use std::thread;
use std::time::{Duration, Instant, SystemTime, UNIX_EPOCH};

fn scratch_dir() -> PathBuf {
    // pid + per-process sequence keep parallel tests collision-free even on
    // hosts whose clock is too coarse for `as_nanos` to differ between two
    // simultaneous starts; a name collision would let one test's cleanup
    // delete another test's files mid-run.
    static SEQ: AtomicUsize = AtomicUsize::new(0);
    let nanos = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap()
        .as_nanos();
    let dir = std::env::temp_dir().join(format!(
        "ez-ffmpeg-http-input-{}-{}-{nanos}",
        std::process::id(),
        SEQ.fetch_add(1, Ordering::Relaxed)
    ));
    std::fs::create_dir_all(&dir).unwrap();
    dir
}

static PROXY_ENV_LOCK: Mutex<()> = Mutex::new(());

const PROXY_ENV_KEYS: &[&str] = &[
    "HTTP_PROXY",
    "HTTPS_PROXY",
    "ALL_PROXY",
    "NO_PROXY",
    "http_proxy",
    "https_proxy",
    "all_proxy",
    "no_proxy",
];

struct ProxyEnvGuard {
    saved: Vec<(&'static str, Option<String>)>,
}

impl ProxyEnvGuard {
    fn apply(sets: &[(&str, &str)]) -> Self {
        let saved = PROXY_ENV_KEYS
            .iter()
            .map(|key| (*key, std::env::var(key).ok()))
            .collect();
        // SAFETY: `PROXY_ENV_LOCK` is held by the caller for the whole
        // mutation window, including Drop, so no other test in this file
        // interleaves `set_var`/`remove_var` on the proxy keys.
        unsafe {
            for key in PROXY_ENV_KEYS {
                std::env::remove_var(key);
            }
            for (key, value) in sets {
                std::env::set_var(key, value);
            }
        }
        Self { saved }
    }
}

impl Drop for ProxyEnvGuard {
    fn drop(&mut self) {
        unsafe {
            for (key, value) in &self.saved {
                match value {
                    Some(v) => std::env::set_var(key, v),
                    None => std::env::remove_var(key),
                }
            }
        }
    }
}

fn count_open_fds() -> usize {
    let path = if cfg!(target_os = "linux") {
        "/proc/self/fd"
    } else {
        "/dev/fd"
    };
    std::fs::read_dir(path).map(|rd| rd.count()).unwrap_or(0)
}

fn count_threads() -> usize {
    #[cfg(target_os = "linux")]
    {
        return std::fs::read_to_string("/proc/self/status")
            .ok()
            .and_then(|status| {
                status.lines().find_map(|line| {
                    line.strip_prefix("Threads:")
                        .and_then(|rest| rest.trim().parse().ok())
                })
            })
            .unwrap_or(0);
    }
    #[cfg(target_os = "macos")]
    {
        let output = std::process::Command::new("ps")
            .args(["-M", "-p", &std::process::id().to_string()])
            .output()
            .ok();
        output
            .map(|out| {
                String::from_utf8_lossy(&out.stdout)
                    .lines()
                    .skip(1)
                    .filter(|line| !line.trim().is_empty())
                    .count()
            })
            .unwrap_or(0)
    }
    #[cfg(not(any(target_os = "linux", target_os = "macos")))]
    {
        0
    }
}

fn stalled_http_stop_latency() -> Duration {
    let listener = TcpListener::bind("127.0.0.1:0").unwrap();
    let addr = listener.local_addr().unwrap();
    let done = Arc::new(AtomicBool::new(false));
    let done_t = Arc::clone(&done);
    let server = thread::spawn(move || {
        if let Ok((mut stream, _)) = listener.accept() {
            let mut buf = [0u8; 4096];
            let _ = stream.read(&mut buf);
            // Declare far more than we send, then stall: the demuxer
            // read blocks in wait_event until stop is observed.
            let header = "HTTP/1.1 200 OK\r\nContent-Type: application/octet-stream\r\nContent-Length: 10000000\r\nConnection: close\r\n\r\n";
            let _ = stream.write_all(header.as_bytes());
            let _ = stream.write_all(&[0x40u8; 65536]);
            let deadline = Instant::now() + Duration::from_secs(10);
            while !done_t.load(Ordering::Relaxed) && Instant::now() < deadline {
                thread::sleep(Duration::from_millis(10));
            }
        }
    });
    let url = format!("http://{addr}/stall.u8");
    let dir = scratch_dir();
    let out = dir.join("out.u8");
    let input = Input::from(HttpInput::builder(&url).build().unwrap())
        .set_format("u8")
        .set_format_opt("ar", "8000")
        .set_format_opt("ac", "1")
        // Default stream-info probing would block in build() on the stalled
        // 10 MB Content-Length. Disable it so start()+stop() measure a
        // running demux wait, not open/probe.
        .set_find_stream_info(false);
    let opened = Instant::now();
    let scheduler = FfmpegContext::builder()
        .input(input)
        .output(
            Output::from(out.to_str().unwrap())
                .set_format("u8")
                .set_audio_codec("copy"),
        )
        .build()
        .unwrap()
        .start()
        .unwrap();
    assert!(
        opened.elapsed() < Duration::from_secs(2),
        "stalled-body fixture must finish build/start from the 64 KiB prefix, got {:?}",
        opened.elapsed()
    );
    // Let demux drain the buffered prefix and block on the stalled body.
    thread::sleep(Duration::from_millis(40));
    let begin = Instant::now();
    let _ = scheduler.stop();
    let elapsed = begin.elapsed();
    done.store(true, Ordering::Relaxed);
    let _ = server.join();
    let _ = std::fs::remove_dir_all(dir);
    elapsed
}

fn write_tiny_mp4(path: &Path) {
    FfmpegContext::builder()
        .input(Input::from("testsrc=size=160x120:rate=10:duration=1").set_format("lavfi"))
        .output(Output::from(path.to_str().unwrap()).set_video_codec("mpeg4"))
        .build()
        .unwrap()
        .start()
        .unwrap()
        .wait()
        .unwrap();
    assert!(path.metadata().unwrap().len() > 0);
}

fn serve_file(
    path: PathBuf,
) -> (
    String,
    Arc<AtomicUsize>,
    Arc<std::sync::atomic::AtomicBool>,
    thread::JoinHandle<Option<String>>,
) {
    let listener = TcpListener::bind("127.0.0.1:0").unwrap();
    listener.set_nonblocking(true).unwrap();
    let addr = listener.local_addr().unwrap();
    let hits = Arc::new(AtomicUsize::new(0));
    let hits_t = Arc::clone(&hits);
    let stop = Arc::new(AtomicBool::new(false));
    let stop_t = Arc::clone(&stop);
    let handle = thread::spawn(move || {
        let body = std::fs::read(&path).unwrap();
        let mut accept_encoding = None;
        let deadline = Instant::now() + Duration::from_secs(15);
        while Instant::now() < deadline && !stop_t.load(Ordering::Relaxed) {
            match listener.accept() {
                Ok((mut stream, _)) => {
                    hits_t.fetch_add(1, Ordering::SeqCst);
                    stream.set_nonblocking(false).ok();
                    let mut buf = [0u8; 4096];
                    let n = stream.read(&mut buf).unwrap_or(0);
                    let req = String::from_utf8_lossy(&buf[..n]);
                    let mut range = None;
                    for line in req.lines() {
                        let lower = line.to_ascii_lowercase();
                        if let Some(rest) = lower.strip_prefix("accept-encoding:") {
                            accept_encoding = Some(rest.trim().to_string());
                        }
                        if let Some(rest) = lower.strip_prefix("range:") {
                            range = Some(rest.trim().to_string());
                        }
                    }
                    if let Some(rest) = range.as_deref().and_then(|r| r.strip_prefix("bytes=")) {
                        let start: usize = rest
                            .trim_end_matches('-')
                            .parse()
                            .unwrap_or(0)
                            .min(body.len());
                        let slice = &body[start..];
                        let end = body.len().saturating_sub(1);
                        let header = format!(
                            "HTTP/1.1 206 Partial Content\r\nContent-Type: video/mp4\r\nContent-Range: bytes {start}-{end}/{}\r\nContent-Length: {}\r\nAccept-Ranges: bytes\r\nConnection: close\r\n\r\n",
                            body.len(),
                            slice.len()
                        );
                        let _ = stream.write_all(header.as_bytes());
                        let _ = stream.write_all(slice);
                    } else {
                        let header = format!(
                            "HTTP/1.1 200 OK\r\nContent-Type: video/mp4\r\nContent-Length: {}\r\nAccept-Ranges: bytes\r\nConnection: close\r\n\r\n",
                            body.len()
                        );
                        let _ = stream.write_all(header.as_bytes());
                        let _ = stream.write_all(&body);
                    }
                }
                Err(err) if err.kind() == std::io::ErrorKind::WouldBlock => {
                    thread::sleep(Duration::from_millis(5));
                }
                Err(_) => break,
            }
        }
        accept_encoding
    });
    (format!("http://{addr}/video.mp4"), hits, stop, handle)
}

fn serve_chunked_bytes(chunks: &'static [&'static [u8]], content_type: &'static str) -> String {
    let listener = TcpListener::bind("127.0.0.1:0").unwrap();
    let addr = listener.local_addr().unwrap();
    let total: usize = chunks.iter().map(|c| c.len()).sum();
    thread::spawn(move || {
        if let Ok((mut stream, _)) = listener.accept() {
            let _ = stream.set_nodelay(true);
            let mut buf = [0u8; 2048];
            let _ = stream.read(&mut buf);
            let header = format!(
                "HTTP/1.1 200 OK\r\nContent-Type: {content_type}\r\nContent-Length: {total}\r\nConnection: close\r\n\r\n"
            );
            let _ = stream.write_all(header.as_bytes());
            for (i, chunk) in chunks.iter().enumerate() {
                let _ = stream.write_all(chunk);
                let _ = stream.flush();
                if i + 1 < chunks.len() {
                    thread::sleep(Duration::from_millis(30));
                }
            }
        }
    });
    format!("http://{addr}/resource")
}

fn serve_bytes(body: &'static [u8], content_type: &'static str) -> String {
    let listener = TcpListener::bind("127.0.0.1:0").unwrap();
    let addr = listener.local_addr().unwrap();
    thread::spawn(move || {
        if let Ok((mut stream, _)) = listener.accept() {
            let mut buf = [0u8; 2048];
            let _ = stream.read(&mut buf);
            let header = format!(
                "HTTP/1.1 200 OK\r\nContent-Type: {content_type}\r\nContent-Length: {}\r\nConnection: close\r\n\r\n",
                body.len()
            );
            let _ = stream.write_all(header.as_bytes());
            let _ = stream.write_all(body);
        }
    });
    format!("http://{addr}/resource")
}

/// Spawns a python3 fixture and reads its self-reported port. stderr is
/// captured (not discarded) so a fixture that dies before printing its
/// port panics with the real cause; the spawn is retried once because a
/// loaded host can kill the first interpreter during startup.
fn spawn_python_fixture_with_port(
    mk: impl Fn() -> std::process::Command,
) -> (std::process::Child, u16) {
    let mut diagnostics = Vec::new();
    for attempt in 1..=2 {
        let mut child = match mk()
            .stdout(std::process::Stdio::piped())
            .stderr(std::process::Stdio::piped())
            .spawn()
        {
            Ok(child) => child,
            Err(err) => {
                diagnostics.push(format!("attempt {attempt}: spawn failed: {err}"));
                continue;
            }
        };

        let mut port_buf = String::new();
        let read_res = child
            .stdout
            .take()
            .map(|stdout| std::io::BufReader::new(stdout).read_line(&mut port_buf));
        let outcome = match read_res {
            Some(Ok(_)) => port_buf
                .trim()
                .parse::<u16>()
                .map_err(|err| format!("invalid port {:?}: {err}", port_buf.trim())),
            Some(Err(err)) => Err(format!("read port failed: {err}")),
            None => Err("missing stdout".to_string()),
        };
        match outcome {
            Ok(port) => return (child, port),
            Err(reason) => {
                let _ = child.kill();
                let _ = child.wait();
                let mut stderr = String::new();
                if let Some(mut pipe) = child.stderr.take() {
                    let _ = pipe.read_to_string(&mut stderr);
                }
                diagnostics.push(format!(
                    "attempt {attempt}: {reason}; stderr={}",
                    stderr.trim()
                ));
            }
        }
    }
    panic!(
        "python3 fixture failed to report a port: {}",
        diagnostics.join(" | ")
    );
}

/// Writes a self-contained CA config and returns its path.
///
/// The fixture CA generation must pass an explicit `-config` so the system
/// `openssl.cnf` never contributes extensions: OpenSSL 1.1.1 (still the
/// `openssl` on GitHub macOS runners) emits `-addext` values *alongside* any
/// same-named extension from the config's `v3_ca` section, and rustls rejects
/// certificates with duplicate extensions when they are added as roots.
fn write_ca_config(dir: &Path, cn: &str) -> PathBuf {
    let cnf = dir.join("ca.cnf");
    std::fs::write(
        &cnf,
        format!(
            "[req]\n\
             distinguished_name = dn\n\
             x509_extensions = v3_ca\n\
             prompt = no\n\
             \n\
             [dn]\n\
             CN = {cn}\n\
             \n\
             [v3_ca]\n\
             basicConstraints = critical,CA:TRUE\n\
             keyUsage = critical,keyCertSign,cRLSign\n\
             subjectKeyIdentifier = hash\n"
        ),
    )
    .expect("write ca config");
    cnf
}

/// Renders the fixture CA certificate as text for build-failure diagnostics.
fn ca_text_dump(openssl: &str, ca: &Path) -> String {
    std::process::Command::new(openssl)
        .args(["x509", "-noout", "-text", "-in"])
        .arg(ca)
        .output()
        .map(|o| String::from_utf8_lossy(&o.stdout).into_owned())
        .unwrap_or_default()
}

#[test]
fn http_mp4_demux_via_explicit_api() {
    let dir = scratch_dir();
    let src = dir.join("src.mp4");
    let out = dir.join("out.mp4");
    write_tiny_mp4(&src);
    let (url, hits, stop, server) = serve_file(src);
    let input = HttpInput::builder(&url).build().unwrap();
    FfmpegContext::builder()
        .input(input)
        .output(out.to_str().unwrap())
        .build()
        .unwrap()
        .start()
        .unwrap()
        .wait()
        .unwrap();
    stop.store(true, Ordering::Relaxed);
    let accept_encoding = server.join().unwrap();
    assert_eq!(accept_encoding.as_deref(), Some("identity"));
    assert!(hits.load(Ordering::SeqCst) >= 1);
    assert!(out.metadata().unwrap().len() > 0);
    let _ = std::fs::remove_dir_all(dir);
}

#[test]
fn fragmented_hls_prefix_is_named_rejected() {
    let url = serve_chunked_bytes(
        &[b"#", b"EXTM3U\n#EXTINF:1,\nseg.ts\n"],
        "application/octet-stream",
    );
    let input = HttpInput::builder(&url).build().unwrap();
    let err = match FfmpegContext::builder()
        .input(input)
        .output(
            std::env::temp_dir()
                .join("ez-ffmpeg-http-hls-split-reject.mp4")
                .to_str()
                .unwrap(),
        )
        .build()
    {
        Ok(_) => panic!("split #EXTM3U prefix must be rejected before open"),
        Err(err) => err,
    };
    let msg = err.to_string();
    assert!(
        msg.contains("HLS manifests") || msg.contains("HTTP input"),
        "{msg}"
    );
}

#[test]
fn fragmented_bom_hls_prefix_is_named_rejected() {
    let url = serve_chunked_bytes(
        &[b"\xEF", b"\xBB\xBF#EXTM3U\n#EXTINF:1,\nseg.ts\n"],
        "application/octet-stream",
    );
    let input = HttpInput::builder(&url).build().unwrap();
    let err = match FfmpegContext::builder()
        .input(input)
        .output(
            std::env::temp_dir()
                .join("ez-ffmpeg-http-hls-bom-split-reject.mp4")
                .to_str()
                .unwrap(),
        )
        .build()
    {
        Ok(_) => panic!("split BOM #EXTM3U prefix must be rejected before open"),
        Err(err) => err,
    };
    let msg = err.to_string();
    assert!(
        msg.contains("HLS manifests") || msg.contains("HTTP input"),
        "{msg}"
    );
}

#[test]
fn hls_playlist_body_is_named_rejected() {
    let url = serve_bytes(
        b"#EXTM3U\n#EXTINF:1,\nseg.ts\n",
        "application/vnd.apple.mpegurl",
    );
    let input = HttpInput::builder(&url).build().unwrap();
    let err = match FfmpegContext::builder()
        .input(input)
        .output(
            std::env::temp_dir()
                .join("ez-ffmpeg-http-hls-reject.mp4")
                .to_str()
                .unwrap(),
        )
        .build()
    {
        Ok(_) => panic!("expected HLS playlist to be rejected"),
        Err(err) => err,
    };
    let msg = err.to_string();
    assert!(
        msg.contains("HLS manifests") || msg.contains("HTTP input"),
        "{msg}"
    );
}

/// Unknown-length chunked body (no Content-Length). MPEG-TS is used so the
/// demuxer does not need a trailing index / Range seek.
#[test]
fn chunked_mpegts_without_content_length_demuxes() {
    let dir = scratch_dir();
    let src = dir.join("src.ts");
    FfmpegContext::builder()
        .input(Input::from("testsrc=size=160x120:rate=10:duration=0.5").set_format("lavfi"))
        .output(
            Output::from(src.to_str().unwrap())
                .set_video_codec("mpeg2video")
                .set_format("mpegts"),
        )
        .build()
        .unwrap()
        .start()
        .unwrap()
        .wait()
        .unwrap();
    let body = std::fs::read(&src).unwrap();
    assert!(!body.is_empty());

    let listener = TcpListener::bind("127.0.0.1:0").unwrap();
    let addr = listener.local_addr().unwrap();
    thread::spawn(move || {
        if let Ok((mut stream, _)) = listener.accept() {
            let mut buf = [0u8; 4096];
            let _ = stream.read(&mut buf);
            let _ = stream.write_all(
                b"HTTP/1.1 200 OK\r\nContent-Type: video/mp2t\r\nTransfer-Encoding: chunked\r\nConnection: close\r\n\r\n",
            );
            for chunk in body.chunks(256) {
                let _ = write!(stream, "{:x}\r\n", chunk.len());
                let _ = stream.write_all(chunk);
                let _ = stream.write_all(b"\r\n");
            }
            let _ = stream.write_all(b"0\r\n\r\n");
        }
    });

    let url = format!("http://{addr}/live.ts");
    let out = dir.join("out.mp4");
    let input = HttpInput::builder(&url).build().unwrap();
    remux_http(input, &out).expect("chunked MPEG-TS without Content-Length must demux");
    assert!(out.metadata().unwrap().len() > 0);
    let _ = std::fs::remove_dir_all(dir);
}

#[test]
fn gzip_content_encoding_despite_identity_is_rejected() {
    let listener = TcpListener::bind("127.0.0.1:0").unwrap();
    let addr = listener.local_addr().unwrap();
    thread::spawn(move || {
        if let Ok((mut stream, _)) = listener.accept() {
            let mut buf = [0u8; 2048];
            let _ = stream.read(&mut buf);
            let body = [0u8; 32];
            let header = format!(
                "HTTP/1.1 200 OK\r\nContent-Type: application/octet-stream\r\nContent-Encoding: gzip\r\nContent-Length: {}\r\nConnection: close\r\n\r\n",
                body.len()
            );
            let _ = stream.write_all(header.as_bytes());
            let _ = stream.write_all(&body);
        }
    });
    let url = format!("http://{addr}/resource.bin");
    let dir = scratch_dir();
    let input = HttpInput::builder(&url).build().unwrap();
    let err = match remux_http(input, &dir.join("out.mp4")) {
        Ok(()) => panic!("gzip Content-Encoding must be rejected"),
        Err(err) => err,
    };
    let msg = err.to_string();
    assert!(msg.contains("Content-Encoding"), "{msg}");
    let _ = std::fs::remove_dir_all(dir);
}

#[test]
fn stop_interrupts_stalled_http_read_quickly() {
    // Warm the HTTP runtime / FFmpeg init so the measured window is not the
    // first-process tax. Design §12.3: 100 iterations, P99 < 250 ms, no
    // fd/thread growth across the measured set.
    let _ = stalled_http_stop_latency();
    thread::sleep(Duration::from_millis(200));
    let fds_before = count_open_fds();
    let threads_before = count_threads();

    let mut samples = Vec::with_capacity(100);
    for iteration in 0..100 {
        let elapsed = stalled_http_stop_latency();
        samples.push(elapsed);
        assert!(
            elapsed < Duration::from_millis(2_000),
            "iteration {iteration}: stop hung ({elapsed:?})"
        );
    }
    samples.sort();
    let p99 = samples[98];
    assert!(
        p99 < Duration::from_millis(250),
        "stop P99 {p99:?} must stay under 250ms (min {:?} max {:?})",
        samples[0],
        samples[99]
    );

    thread::sleep(Duration::from_millis(500));
    let fds_after = count_open_fds();
    let threads_after = count_threads();
    assert!(
        fds_after <= fds_before.saturating_add(4),
        "open fds grew from {fds_before} to {fds_after} across 100 stop() iterations"
    );
    if threads_before > 0 {
        assert!(
            threads_after <= threads_before.saturating_add(2),
            "threads grew from {threads_before} to {threads_after} across 100 stop() iterations"
        );
    }
}

#[test]
fn invalid_client_identity_pem_is_rejected() {
    let err = HttpClient::builder()
        .client_identity_pem("not-a-cert")
        .build()
        .unwrap_err();
    assert!(
        matches!(err, HttpInputError::IdentityInvalid),
        "expected IdentityInvalid, got {err:?}"
    );
}

#[test]
fn client_identity_pem_is_not_in_debug_or_errors() {
    let pem = "-----BEGIN CERTIFICATE-----\nMIIFakeIdentityMarker\n-----END CERTIFICATE-----\n-----BEGIN PRIVATE KEY-----\nMIIFakeIdentityMarker\n-----END PRIVATE KEY-----\n";
    let builder = HttpClient::builder().client_identity_pem(pem.as_bytes());
    let dbg = format!("{builder:?}");
    assert!(
        dbg.contains("has_identity: true"),
        "Debug must acknowledge the identity without printing it: {dbg}"
    );
    assert!(
        !dbg.contains("BEGIN CERTIFICATE") && !dbg.contains("MIIFakeIdentityMarker"),
        "builder Debug must not echo PEM: {dbg}"
    );
    let err = builder.build().unwrap_err();
    assert!(
        matches!(err, HttpInputError::IdentityInvalid),
        "expected IdentityInvalid, got {err:?}"
    );
    let text = format!("{err} {err:?}");
    assert!(
        !text.contains("BEGIN CERTIFICATE") && !text.contains("MIIFakeIdentityMarker"),
        "IdentityInvalid must not echo PEM: {text}"
    );
}

#[test]
fn connect_proxy_403_is_a_redacted_transport_error() {
    let listener = TcpListener::bind("127.0.0.1:0").unwrap();
    let addr = listener.local_addr().unwrap();
    thread::spawn(move || {
        // Minimal CONNECT stub: refuse the tunnel with 403.
        if let Ok((mut stream, _)) = listener.accept() {
            let mut buf = [0u8; 2048];
            let n = stream.read(&mut buf).unwrap_or(0);
            let req = String::from_utf8_lossy(&buf[..n]);
            let is_connect = req.starts_with("CONNECT ");
            let _ = stream.write_all(b"HTTP/1.1 403 Forbidden\r\nConnection: close\r\n\r\n");
            assert!(
                is_connect,
                "https origin must reach the proxy as CONNECT: {req}"
            );
        }
    });
    let client = HttpClient::builder()
        .proxy(ProxyPolicy::Explicit(ProxyConfig::new(format!(
            "http://{addr}"
        ))))
        .build()
        .unwrap();
    let input = client
        .input("https://blocked.invalid/video.mp4")
        .build()
        .unwrap();
    let dir = scratch_dir();
    let err = match remux_http(input, &dir.join("out.mp4")) {
        Ok(()) => panic!("refused CONNECT tunnel must fail the input"),
        Err(err) => err,
    };
    let msg = err.to_string();
    assert!(
        !msg.contains("blocked.invalid"),
        "proxy failure must not leak the origin host: {msg}"
    );
    let _ = std::fs::remove_dir_all(dir);
}

#[test]
fn no_proxy_bypasses_environment_proxy() {
    let _env_lock = PROXY_ENV_LOCK.lock().unwrap_or_else(|e| e.into_inner());
    let dir = scratch_dir();
    let src = dir.join("src.mp4");
    write_tiny_mp4(&src);

    let proxy_hits = Arc::new(AtomicUsize::new(0));
    let proxy_hits_t = Arc::clone(&proxy_hits);
    let proxy = TcpListener::bind("127.0.0.1:0").unwrap();
    let proxy_addr = proxy.local_addr().unwrap();
    thread::spawn(move || {
        if let Ok((mut stream, _)) = proxy.accept() {
            proxy_hits_t.fetch_add(1, Ordering::SeqCst);
            let mut buf = [0u8; 2048];
            let _ = stream.read(&mut buf);
            let _ = stream.write_all(b"HTTP/1.1 403 Forbidden\r\nConnection: close\r\n\r\n");
        }
    });

    let (url, origin_hits, stop, handle) = serve_file(src);
    let _guard = ProxyEnvGuard::apply(&[
        ("HTTP_PROXY", &format!("http://{proxy_addr}")),
        ("HTTPS_PROXY", &format!("http://{proxy_addr}")),
        ("ALL_PROXY", &format!("http://{proxy_addr}")),
        ("NO_PROXY", "127.0.0.1,localhost"),
    ]);
    let client = HttpClient::builder()
        .proxy(ProxyPolicy::Environment)
        .build()
        .unwrap();
    let input = client.input(&url).build().unwrap();
    remux_http(input, &dir.join("out.mp4")).expect("NO_PROXY must reach the origin directly");
    stop.store(true, Ordering::Relaxed);
    let _ = handle.join();
    assert_eq!(
        proxy_hits.load(Ordering::SeqCst),
        0,
        "NO_PROXY must suppress HTTP(S)_PROXY / ALL_PROXY for 127.0.0.1"
    );
    assert!(
        origin_hits.load(Ordering::SeqCst) >= 1,
        "origin must see the direct request"
    );
    let _ = std::fs::remove_dir_all(dir);
}

#[test]
fn https_mp4_demux_via_custom_ca() {
    let openssl = match std::process::Command::new("openssl")
        .arg("version")
        .output()
    {
        Ok(out) if out.status.success() => "openssl",
        _ => {
            eprintln!("skip https_mp4_demux_via_custom_ca: openssl not in PATH");
            return;
        }
    };
    if std::process::Command::new("python3")
        .arg("-c")
        .arg("import ssl, http.server")
        .status()
        .map(|s| !s.success())
        .unwrap_or(true)
    {
        eprintln!("skip https_mp4_demux_via_custom_ca: python3 ssl/http.server unavailable");
        return;
    }

    let dir = scratch_dir();
    let src = dir.join("src.mp4");
    let out = dir.join("out.mp4");
    let ca = dir.join("ca.pem");
    let ca_key = dir.join("ca.key");
    let cert = dir.join("server.pem");
    let key = dir.join("server.key");
    let csr = dir.join("server.csr");
    write_tiny_mp4(&src);
    // rustls only accepts CA:TRUE certificates as trust anchors, so the
    // fixture uses a short-lived CA plus a SAN leaf (not a self-signed leaf).
    let ca_cnf = write_ca_config(&dir, "ez-ffmpeg-http-input-test-ca");
    let ca_out = std::process::Command::new(openssl)
        .args([
            "req", "-x509", "-newkey", "rsa:2048", "-sha256", "-days", "1", "-nodes", "-keyout",
        ])
        .arg(&ca_key)
        .arg("-out")
        .arg(&ca)
        .arg("-config")
        .arg(&ca_cnf)
        .output()
        .expect("openssl ca");
    assert!(
        ca_out.status.success(),
        "openssl ca failed: {}",
        String::from_utf8_lossy(&ca_out.stderr)
    );
    let csr_out = std::process::Command::new(openssl)
        .args(["req", "-new", "-newkey", "rsa:2048", "-nodes", "-keyout"])
        .arg(&key)
        .arg("-out")
        .arg(&csr)
        .args([
            "-subj",
            "/CN=127.0.0.1",
            "-addext",
            "subjectAltName=IP:127.0.0.1",
            "-addext",
            "extendedKeyUsage=serverAuth",
        ])
        .output()
        .expect("openssl csr");
    assert!(
        csr_out.status.success(),
        "openssl csr failed: {}",
        String::from_utf8_lossy(&csr_out.stderr)
    );
    // Neither LibreSSL nor OpenSSL 1.1.1 (the GitHub macOS runners' `openssl`)
    // has `x509 -copy_extensions`; pass the leaf extensions via `-extfile` so
    // every toolchain produces the same SAN leaf.
    let ext = dir.join("server.ext");
    std::fs::write(
        &ext,
        "subjectAltName=IP:127.0.0.1\nextendedKeyUsage=serverAuth\n",
    )
    .expect("write server ext");
    let sign = std::process::Command::new(openssl)
        .args(["x509", "-req", "-in"])
        .arg(&csr)
        .arg("-CA")
        .arg(&ca)
        .arg("-CAkey")
        .arg(&ca_key)
        .args(["-CAcreateserial", "-out"])
        .arg(&cert)
        .args(["-days", "1", "-sha256", "-extfile"])
        .arg(&ext)
        .output()
        .expect("openssl sign");
    assert!(
        sign.status.success(),
        "openssl sign failed: {}",
        String::from_utf8_lossy(&sign.stderr)
    );

    let script = dir.join("serve_https.py");
    std::fs::write(
        &script,
        r#"
import ssl, sys, threading
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer

path, cert, key = sys.argv[1], sys.argv[2], sys.argv[3]
body = open(path, "rb").read()

class H(BaseHTTPRequestHandler):
    def do_GET(self):
        rng = self.headers.get("Range")
        if rng and rng.lower().startswith("bytes="):
            start = int(rng.split("=", 1)[1].split("-", 1)[0] or "0")
            start = min(max(start, 0), len(body))
            chunk = body[start:]
            self.send_response(206)
            self.send_header("Content-Type", "video/mp4")
            self.send_header("Content-Range", f"bytes {start}-{len(body)-1}/{len(body)}")
            self.send_header("Content-Length", str(len(chunk)))
            self.send_header("Accept-Ranges", "bytes")
            self.end_headers()
            self.wfile.write(chunk)
            return
        self.send_response(200)
        self.send_header("Content-Type", "video/mp4")
        self.send_header("Content-Length", str(len(body)))
        self.send_header("Accept-Ranges", "bytes")
        self.end_headers()
        self.wfile.write(body)
    def log_message(self, *args):
        pass

httpd = ThreadingHTTPServer(("127.0.0.1", 0), H)
ctx = ssl.SSLContext(ssl.PROTOCOL_TLS_SERVER)
ctx.load_cert_chain(cert, key)
httpd.socket = ctx.wrap_socket(httpd.socket, server_side=True)
print(httpd.server_address[1], flush=True)
httpd.serve_forever()
"#,
    )
    .unwrap();

    let (mut child, port) = spawn_python_fixture_with_port(|| {
        let mut cmd = std::process::Command::new("python3");
        cmd.arg(&script).arg(&src).arg(&cert).arg(&key);
        cmd
    });
    thread::sleep(Duration::from_millis(50));
    let url = format!("https://127.0.0.1:{port}/video.mp4");
    let pem = std::fs::read(&ca).unwrap();
    let client = HttpClient::builder()
        .root_policy(RootPolicy::CustomOnly)
        .add_root_certificate_pem(&pem)
        .unwrap()
        .build()
        .unwrap_or_else(|e| {
            panic!(
                "HttpClient rejected the fixture CA: {e:?}\n{}",
                ca_text_dump(openssl, &ca)
            )
        });
    let input = client.input(&url).build().unwrap();
    let result = FfmpegContext::builder()
        .input(input)
        .output(out.to_str().unwrap())
        .build()
        .and_then(|ctx| ctx.start())
        .and_then(|job| job.wait());
    let _ = child.kill();
    let _ = child.wait();
    result.unwrap();
    // One windows-2025 CI run hit NotFound here right after a successful
    // remux; list the scratch dir so any recurrence shows what survived.
    let out_meta = out.metadata().unwrap_or_else(|err| {
        let listing = match std::fs::read_dir(&dir) {
            Ok(entries) => entries
                .filter_map(|entry| entry.ok())
                .map(|entry| entry.file_name().to_string_lossy().into_owned())
                .collect::<Vec<_>>()
                .join(", "),
            Err(e) => format!("<read_dir failed: {e}>"),
        };
        panic!(
            "out.mp4 missing after a successful remux: {err}; {} contains [{listing}]",
            dir.display()
        )
    });
    assert!(out_meta.len() > 0);
    let _ = std::fs::remove_dir_all(dir);
}

#[test]
fn mtls_custom_ca_requires_and_accepts_client_identity() {
    let openssl = match std::process::Command::new("openssl")
        .arg("version")
        .output()
    {
        Ok(out) if out.status.success() => "openssl",
        _ => {
            eprintln!(
                "skip mtls_custom_ca_requires_and_accepts_client_identity: openssl not in PATH"
            );
            return;
        }
    };
    if std::process::Command::new("python3")
        .arg("-c")
        .arg("import ssl, http.server")
        .status()
        .map(|s| !s.success())
        .unwrap_or(true)
    {
        eprintln!(
            "skip mtls_custom_ca_requires_and_accepts_client_identity: python3 ssl/http.server unavailable"
        );
        return;
    }

    let dir = scratch_dir();
    let src = dir.join("src.mp4");
    let out_fail = dir.join("out-fail.mp4");
    let out_ok = dir.join("out-ok.mp4");
    let ca = dir.join("ca.pem");
    let ca_key = dir.join("ca.key");
    let cert = dir.join("server.pem");
    let key = dir.join("server.key");
    let csr = dir.join("server.csr");
    let client_key = dir.join("client.key");
    let client_csr = dir.join("client.csr");
    let client_cert = dir.join("client.pem");
    write_tiny_mp4(&src);

    let ca_cnf = write_ca_config(&dir, "ez-ffmpeg-http-input-mtls-ca");
    let ca_out = std::process::Command::new(openssl)
        .args([
            "req", "-x509", "-newkey", "rsa:2048", "-sha256", "-days", "1", "-nodes", "-keyout",
        ])
        .arg(&ca_key)
        .arg("-out")
        .arg(&ca)
        .arg("-config")
        .arg(&ca_cnf)
        .output()
        .expect("openssl ca");
    assert!(
        ca_out.status.success(),
        "openssl ca failed: {}",
        String::from_utf8_lossy(&ca_out.stderr)
    );
    let csr_out = std::process::Command::new(openssl)
        .args(["req", "-new", "-newkey", "rsa:2048", "-nodes", "-keyout"])
        .arg(&key)
        .arg("-out")
        .arg(&csr)
        .args([
            "-subj",
            "/CN=127.0.0.1",
            "-addext",
            "subjectAltName=IP:127.0.0.1",
            "-addext",
            "extendedKeyUsage=serverAuth",
        ])
        .output()
        .expect("openssl server csr");
    assert!(
        csr_out.status.success(),
        "openssl server csr failed: {}",
        String::from_utf8_lossy(&csr_out.stderr)
    );
    // Neither LibreSSL nor OpenSSL 1.1.1 has `x509 -copy_extensions`; pass the
    // leaf extensions via `-extfile` on both signings.
    let server_ext = dir.join("server.ext");
    std::fs::write(
        &server_ext,
        "subjectAltName=IP:127.0.0.1\nextendedKeyUsage=serverAuth\n",
    )
    .expect("write server ext");
    let sign = std::process::Command::new(openssl)
        .args(["x509", "-req", "-in"])
        .arg(&csr)
        .arg("-CA")
        .arg(&ca)
        .arg("-CAkey")
        .arg(&ca_key)
        .args(["-CAcreateserial", "-out"])
        .arg(&cert)
        .args(["-days", "1", "-sha256", "-extfile"])
        .arg(&server_ext)
        .output()
        .expect("openssl server sign");
    assert!(
        sign.status.success(),
        "openssl server sign failed: {}",
        String::from_utf8_lossy(&sign.stderr)
    );
    let client_csr_out = std::process::Command::new(openssl)
        .args(["req", "-new", "-newkey", "rsa:2048", "-nodes", "-keyout"])
        .arg(&client_key)
        .arg("-out")
        .arg(&client_csr)
        .args([
            "-subj",
            "/CN=ez-ffmpeg-http-input-client",
            "-addext",
            "extendedKeyUsage=clientAuth",
        ])
        .output()
        .expect("openssl client csr");
    assert!(
        client_csr_out.status.success(),
        "openssl client csr failed: {}",
        String::from_utf8_lossy(&client_csr_out.stderr)
    );
    let client_ext = dir.join("client.ext");
    std::fs::write(&client_ext, "extendedKeyUsage=clientAuth\n").expect("write client ext");
    let client_sign = std::process::Command::new(openssl)
        .args(["x509", "-req", "-in"])
        .arg(&client_csr)
        .arg("-CA")
        .arg(&ca)
        .arg("-CAkey")
        .arg(&ca_key)
        .args(["-CAcreateserial", "-out"])
        .arg(&client_cert)
        .args(["-days", "1", "-sha256", "-extfile"])
        .arg(&client_ext)
        .output()
        .expect("openssl client sign");
    assert!(
        client_sign.status.success(),
        "openssl client sign failed: {}",
        String::from_utf8_lossy(&client_sign.stderr)
    );

    let script = dir.join("serve_mtls.py");
    std::fs::write(
        &script,
        r#"
import ssl, sys
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer

path, cert, key, ca = sys.argv[1], sys.argv[2], sys.argv[3], sys.argv[4]
body = open(path, "rb").read()

class H(BaseHTTPRequestHandler):
    def do_GET(self):
        rng = self.headers.get("Range")
        if rng and rng.lower().startswith("bytes="):
            start = int(rng.split("=", 1)[1].split("-", 1)[0] or "0")
            start = min(max(start, 0), len(body))
            chunk = body[start:]
            self.send_response(206)
            self.send_header("Content-Type", "video/mp4")
            self.send_header("Content-Range", f"bytes {start}-{len(body)-1}/{len(body)}")
            self.send_header("Content-Length", str(len(chunk)))
            self.send_header("Accept-Ranges", "bytes")
            self.end_headers()
            self.wfile.write(chunk)
            return
        self.send_response(200)
        self.send_header("Content-Type", "video/mp4")
        self.send_header("Content-Length", str(len(body)))
        self.send_header("Accept-Ranges", "bytes")
        self.end_headers()
        self.wfile.write(body)
    def log_message(self, *args):
        pass

httpd = ThreadingHTTPServer(("127.0.0.1", 0), H)
ctx = ssl.SSLContext(ssl.PROTOCOL_TLS_SERVER)
ctx.load_cert_chain(cert, key)
ctx.load_verify_locations(ca)
ctx.verify_mode = ssl.CERT_REQUIRED
httpd.socket = ctx.wrap_socket(httpd.socket, server_side=True)
print(httpd.server_address[1], flush=True)
httpd.serve_forever()
"#,
    )
    .unwrap();

    let (mut child, port) = spawn_python_fixture_with_port(|| {
        let mut cmd = std::process::Command::new("python3");
        cmd.arg(&script).arg(&src).arg(&cert).arg(&key).arg(&ca);
        cmd
    });
    thread::sleep(Duration::from_millis(50));
    let url = format!("https://127.0.0.1:{port}/video.mp4");
    let ca_pem = std::fs::read(&ca).unwrap();

    let no_identity = HttpClient::builder()
        .root_policy(RootPolicy::CustomOnly)
        .add_root_certificate_pem(&ca_pem)
        .unwrap()
        .build()
        .unwrap_or_else(|e| {
            panic!(
                "HttpClient rejected the fixture CA: {e:?}\n{}",
                ca_text_dump(openssl, &ca)
            )
        });
    let missing = remux_http(no_identity.input(&url).build().unwrap(), &out_fail);
    assert!(
        missing.is_err(),
        "mTLS server must reject a client that presents no identity"
    );

    let mut identity = std::fs::read(&client_cert).unwrap();
    identity.extend_from_slice(b"\n");
    identity.extend_from_slice(&std::fs::read(&client_key).unwrap());
    let builder = HttpClient::builder()
        .root_policy(RootPolicy::CustomOnly)
        .add_root_certificate_pem(&ca_pem)
        .unwrap()
        .client_identity_pem(identity);
    let builder_dbg = format!("{builder:?}");
    assert!(
        !builder_dbg.contains("BEGIN CERTIFICATE") && !builder_dbg.contains("BEGIN PRIVATE KEY"),
        "mTLS builder Debug must not echo PEM: {builder_dbg}"
    );
    let client = builder.build().unwrap_or_else(|e| {
        panic!(
            "mTLS identity client build failed: {e:?}\n{}",
            ca_text_dump(openssl, &ca)
        )
    });
    let client_dbg = format!("{client:?}");
    assert!(
        !client_dbg.contains("BEGIN CERTIFICATE") && !client_dbg.contains("BEGIN PRIVATE KEY"),
        "HttpClient Debug must not echo PEM: {client_dbg}"
    );
    remux_http(client.input(&url).build().unwrap(), &out_ok)
        .expect("mTLS remux with a matching client identity must succeed");
    let _ = child.kill();
    let _ = child.wait();
    assert!(out_ok.metadata().unwrap().len() > 0);
    let _ = std::fs::remove_dir_all(dir);
}

#[test]
fn hls_url_is_named_rejected_before_connect() {
    let err = HttpInput::builder("https://example.com/live.m3u8")
        .build()
        .unwrap_err();
    assert!(matches!(
        err,
        HttpInputError::ManifestUnsupported {
            kind: ManifestKind::Hls
        }
    ));
}

#[derive(Clone)]
struct BodyFixture {
    body: Vec<u8>,
    etag: Option<String>,
    /// Close the first response after this many entity bytes while still
    /// advertising the full remaining Content-Range / Content-Length.
    drop_after: Option<usize>,
    /// First response is an honest short 206 ending at this inclusive offset.
    short_end: Option<u64>,
    /// Advertise Content-Length = body.len() but write only this many bytes.
    send_only: Option<usize>,
    /// Ignore Range and answer 200 (non-seekable).
    force_200: bool,
    /// Every 206 covers at most this many bytes (windowing CDN).
    window: Option<usize>,
    /// RFC 7233 unknown total: `Content-Range: bytes start-end/*`.
    unknown_total: bool,
}

struct BodyServer {
    url: String,
    hits: Arc<AtomicUsize>,
    ranges: Arc<Mutex<Vec<String>>>,
    stop: Arc<AtomicBool>,
    handle: thread::JoinHandle<()>,
}

impl BodyServer {
    fn shutdown(self) {
        self.stop.store(true, Ordering::Relaxed);
        let _ = self.handle.join();
    }
}

fn patterned_body(len: usize) -> Vec<u8> {
    (0..len)
        .map(|i| ((i.wrapping_mul(131) + 7) % 251) as u8)
        .collect()
}

fn serve_body(fx: BodyFixture) -> BodyServer {
    let listener = TcpListener::bind("127.0.0.1:0").unwrap();
    listener.set_nonblocking(true).unwrap();
    let addr = listener.local_addr().unwrap();
    let hits = Arc::new(AtomicUsize::new(0));
    let hits_t = Arc::clone(&hits);
    let ranges = Arc::new(Mutex::new(Vec::new()));
    let ranges_t = Arc::clone(&ranges);
    let stop = Arc::new(AtomicBool::new(false));
    let stop_t = Arc::clone(&stop);
    let handle = thread::spawn(move || {
        let mut first = true;
        let deadline = Instant::now() + Duration::from_secs(20);
        while Instant::now() < deadline && !stop_t.load(Ordering::Relaxed) {
            match listener.accept() {
                Ok((mut stream, _)) => {
                    hits_t.fetch_add(1, Ordering::SeqCst);
                    stream.set_nonblocking(false).ok();
                    let mut buf = [0u8; 4096];
                    let n = stream.read(&mut buf).unwrap_or(0);
                    let req = String::from_utf8_lossy(&buf[..n]);
                    let mut range = None;
                    for line in req.lines() {
                        if let Some(rest) = line.to_ascii_lowercase().strip_prefix("range:") {
                            range = Some(rest.trim().to_string());
                        }
                    }
                    ranges_t
                        .lock()
                        .unwrap_or_else(|e| e.into_inner())
                        .push(range.clone().unwrap_or_else(|| "none".into()));
                    let start = range
                        .as_deref()
                        .and_then(|r| r.strip_prefix("bytes="))
                        .and_then(|r| r.trim_end_matches('-').parse::<usize>().ok())
                        .unwrap_or(0);
                    let etag = fx
                        .etag
                        .as_deref()
                        .map(|v| format!("ETag: {v}\r\n"))
                        .unwrap_or_default();
                    if start >= fx.body.len() {
                        let header = format!(
                            "HTTP/1.1 416 Range Not Satisfiable\r\nContent-Range: bytes */{}\r\nContent-Length: 0\r\nConnection: close\r\n\r\n",
                            fx.body.len()
                        );
                        let _ = stream.write_all(header.as_bytes());
                        continue;
                    }
                    if fx.force_200 {
                        let declared = fx.body.len();
                        let mut send_len = fx.send_only.unwrap_or(declared);
                        if first {
                            if let Some(drop_after) = fx.drop_after {
                                send_len = drop_after;
                            }
                            first = false;
                        }
                        send_len = send_len.min(fx.body.len());
                        let header = format!(
                            "HTTP/1.1 200 OK\r\nContent-Type: application/octet-stream\r\nContent-Length: {declared}\r\n{etag}Connection: close\r\n\r\n"
                        );
                        let _ = stream.write_all(header.as_bytes());
                        let _ = stream.write_all(&fx.body[..send_len]);
                        continue;
                    }

                    let full_end = fx.body.len().saturating_sub(1);
                    let (mut end, mut payload, mut content_len) = if first {
                        first = false;
                        if let Some(short_end) = fx.short_end {
                            let end = (short_end as usize).min(full_end).max(start);
                            let payload = fx.body[start..=end].to_vec();
                            let content_len = payload.len();
                            (end, payload, content_len)
                        } else if let Some(drop_after) = fx.drop_after {
                            let payload =
                                fx.body[start..fx.body.len().min(start + drop_after)].to_vec();
                            (full_end, payload, fx.body.len() - start)
                        } else {
                            let payload = fx.body[start..].to_vec();
                            let content_len = payload.len();
                            (full_end, payload, content_len)
                        }
                    } else {
                        let payload = fx.body[start..].to_vec();
                        let content_len = payload.len();
                        (full_end, payload, content_len)
                    };
                    if let Some(window) = fx.window {
                        end = start.saturating_add(window).saturating_sub(1).min(end);
                        payload = fx.body[start..=end].to_vec();
                        content_len = payload.len();
                    }
                    let total = if fx.unknown_total {
                        "*".to_string()
                    } else {
                        fx.body.len().to_string()
                    };
                    let header = format!(
                        "HTTP/1.1 206 Partial Content\r\nContent-Type: application/octet-stream\r\nContent-Range: bytes {start}-{end}/{total}\r\nContent-Length: {content_len}\r\nAccept-Ranges: bytes\r\n{etag}Connection: close\r\n\r\n"
                    );
                    let _ = stream.write_all(header.as_bytes());
                    let _ = stream.write_all(&payload);
                }
                Err(err) if err.kind() == std::io::ErrorKind::WouldBlock => {
                    thread::sleep(Duration::from_millis(5));
                }
                Err(_) => break,
            }
        }
    });
    BodyServer {
        url: format!("http://{addr}/resource.bin"),
        hits,
        ranges,
        stop,
        handle,
    }
}

fn copy_u8(input: HttpInput, out: &Path) -> ez_ffmpeg::error::Result<()> {
    let input = Input::from(input)
        .set_format("u8")
        .set_format_opt("ar", "8000")
        .set_format_opt("ac", "1");
    FfmpegContext::builder()
        .input(input)
        .output(
            Output::from(out.to_str().unwrap())
                .set_format("u8")
                .set_audio_codec("copy"),
        )
        .build()?
        .start()?
        .wait()
}

fn remux_http(input: HttpInput, out: &Path) -> ez_ffmpeg::error::Result<()> {
    FfmpegContext::builder()
        .input(input)
        .output(out.to_str().unwrap())
        .build()?
        .start()?
        .wait()
}

#[test]
fn default_policy_does_not_retry_after_body_started() {
    let dir = scratch_dir();
    let src = dir.join("src.mp4");
    write_tiny_mp4(&src);
    let body = std::fs::read(&src).unwrap();
    let server = serve_body(BodyFixture {
        body,
        etag: None,
        drop_after: Some(128),
        short_end: None,
        send_only: None,
        force_200: true,
        window: None,
        unknown_total: false,
    });
    let out = dir.join("out.mp4");
    let input = HttpInput::builder(&server.url).build().unwrap();
    let result = remux_http(input, &out);
    let hits = server.hits.load(Ordering::SeqCst);
    let ranges = server.ranges.lock().unwrap().clone();
    server.shutdown();
    assert!(
        result.is_err(),
        "default policy must not treat a mid-body drop as success: {result:?}"
    );
    assert_eq!(
        hits, 1,
        "default reconnect=0 must not retry after body started; ranges={ranges:?}"
    );
    let _ = std::fs::remove_dir_all(dir);
}

#[test]
fn seekable_default_resumes_range_without_duplicate_bytes() {
    let body = patterned_body(8192);
    let drop_after = 1024usize;
    let server = serve_body(BodyFixture {
        body: body.clone(),
        etag: Some("\"v1\"".into()),
        drop_after: Some(drop_after),
        short_end: None,
        send_only: None,
        force_200: false,
        window: None,
        unknown_total: false,
    });
    let dir = scratch_dir();
    let out = dir.join("out.u8");
    let input = HttpInput::builder(&server.url)
        .reconnect(ReconnectPolicy::seekable_default())
        .build()
        .unwrap();
    copy_u8(input, &out).expect("seekable reconnect should resume");
    let hits = server.hits.load(Ordering::SeqCst);
    let ranges = server.ranges.lock().unwrap().clone();
    server.shutdown();
    assert!(
        hits >= 2,
        "expected a Range resume after the drop; ranges={ranges:?}"
    );
    assert!(
        ranges.iter().any(|r| r == &format!("bytes={drop_after}-")),
        "expected Range bytes={drop_after}- ; ranges={ranges:?}"
    );
    let copied = std::fs::read(&out).unwrap();
    assert_eq!(
        copied, body,
        "resumed body must match the original file with no duplicate bytes"
    );
    let _ = std::fs::remove_dir_all(dir);
}

#[test]
fn content_length_truncated_without_reconnect_is_error() {
    let dir = scratch_dir();
    let src = dir.join("src.mp4");
    write_tiny_mp4(&src);
    let body = std::fs::read(&src).unwrap();
    let server = serve_body(BodyFixture {
        body,
        etag: None,
        drop_after: None,
        short_end: None,
        send_only: Some(128),
        force_200: true,
        window: None,
        unknown_total: false,
    });
    let out = dir.join("out.mp4");
    let input = HttpInput::builder(&server.url).build().unwrap();
    let result = remux_http(input, &out);
    server.shutdown();
    assert!(
        result.is_err(),
        "declared Content-Length with a short body must fail, not succeed: {result:?}"
    );
    let _ = std::fs::remove_dir_all(dir);
}

#[test]
fn short_206_then_continuation_concatenates() {
    let body = patterned_body(4096);
    let short_end = 511u64;
    let server = serve_body(BodyFixture {
        body: body.clone(),
        etag: Some("\"range-v1\"".into()),
        drop_after: None,
        short_end: Some(short_end),
        send_only: None,
        force_200: false,
        window: None,
        unknown_total: false,
    });
    let dir = scratch_dir();
    let out = dir.join("out.u8");
    let input = HttpInput::builder(&server.url).build().unwrap();
    copy_u8(input, &out).expect("short 206 completion is independent of reconnect");
    let ranges = server.ranges.lock().unwrap().clone();
    server.shutdown();
    assert!(
        ranges.iter().any(|r| r == "bytes=0-" || r == "none"),
        "first request should start at 0; ranges={ranges:?}"
    );
    assert!(
        ranges
            .iter()
            .any(|r| r == &format!("bytes={}-", short_end + 1)),
        "expected continuation Range bytes={}- ; ranges={ranges:?}",
        short_end + 1
    );
    let copied = std::fs::read(&out).unwrap();
    assert_eq!(copied, body);
    let _ = std::fs::remove_dir_all(dir);
}

#[test]
fn short_206_without_validator_fails_closed() {
    let body = patterned_body(4096);
    let server = serve_body(BodyFixture {
        body: body.clone(),
        etag: None,
        drop_after: None,
        short_end: Some(511),
        send_only: None,
        force_200: false,
        window: None,
        unknown_total: false,
    });
    let dir = scratch_dir();
    let out = dir.join("out.u8");
    let input = HttpInput::builder(&server.url).build().unwrap();
    let result = copy_u8(input, &out);
    server.shutdown();
    assert!(
        result.is_err(),
        "short 206 continuation without ETag/Last-Modified must fail closed; got {result:?}"
    );
    let _ = std::fs::remove_dir_all(dir);
}

#[test]
fn short_206_many_windows_concatenates() {
    let window = 16usize;
    let body = patterned_body(window * 40);
    let server = serve_body(BodyFixture {
        body: body.clone(),
        etag: Some("\"w40\"".into()),
        drop_after: None,
        short_end: None,
        send_only: None,
        force_200: false,
        window: Some(window),
        unknown_total: false,
    });
    let dir = scratch_dir();
    let out = dir.join("out.u8");
    let input = HttpInput::builder(&server.url).build().unwrap();
    copy_u8(input, &out).expect("honest windowing CDN must not die after 32 ranges");
    let hits = server.hits.load(Ordering::SeqCst);
    server.shutdown();
    assert!(
        hits >= 40,
        "expected one request per 16-byte window, got {hits}"
    );
    assert_eq!(std::fs::read(&out).unwrap(), body);
    let _ = std::fs::remove_dir_all(dir);
}

#[test]
fn short_206_unknown_total_is_not_clean_eof() {
    let body = patterned_body(64);
    let server = serve_body(BodyFixture {
        body: body.clone(),
        etag: Some("\"star\"".into()),
        drop_after: None,
        short_end: Some(63),
        send_only: None,
        force_200: false,
        window: None,
        unknown_total: true,
    });
    let dir = scratch_dir();
    let out = dir.join("out.u8");
    let input = HttpInput::builder(&server.url).build().unwrap();
    let result = copy_u8(input, &out);
    server.shutdown();
    assert!(
        result.is_err(),
        "206 with Content-Range bytes 0-N/* must not succeed as a complete body: {result:?}"
    );
    let _ = std::fs::remove_dir_all(dir);
}

#[test]
fn https_to_http_redirect_is_rejected() {
    let openssl = match std::process::Command::new("openssl")
        .arg("version")
        .output()
    {
        Ok(out) if out.status.success() => "openssl",
        _ => {
            eprintln!("skip https_to_http_redirect_is_rejected: openssl not in PATH");
            return;
        }
    };
    if std::process::Command::new("python3")
        .arg("-c")
        .arg("import ssl, http.server")
        .status()
        .map(|s| !s.success())
        .unwrap_or(true)
    {
        eprintln!("skip https_to_http_redirect_is_rejected: python3 ssl/http.server unavailable");
        return;
    }

    let dir = scratch_dir();
    let ca = dir.join("ca.pem");
    let ca_key = dir.join("ca.key");
    let cert = dir.join("server.pem");
    let key = dir.join("server.key");
    let csr = dir.join("server.csr");
    let ca_cnf = write_ca_config(&dir, "ez-ffmpeg-http-input-test-ca");
    let ca_out = std::process::Command::new(openssl)
        .args([
            "req", "-x509", "-newkey", "rsa:2048", "-sha256", "-days", "1", "-nodes", "-keyout",
        ])
        .arg(&ca_key)
        .arg("-out")
        .arg(&ca)
        .arg("-config")
        .arg(&ca_cnf)
        .output()
        .expect("openssl ca");
    assert!(
        ca_out.status.success(),
        "{}",
        String::from_utf8_lossy(&ca_out.stderr)
    );
    let csr_out = std::process::Command::new(openssl)
        .args(["req", "-new", "-newkey", "rsa:2048", "-nodes", "-keyout"])
        .arg(&key)
        .arg("-out")
        .arg(&csr)
        .args([
            "-subj",
            "/CN=127.0.0.1",
            "-addext",
            "subjectAltName=IP:127.0.0.1",
            "-addext",
            "extendedKeyUsage=serverAuth",
        ])
        .output()
        .expect("openssl csr");
    assert!(
        csr_out.status.success(),
        "{}",
        String::from_utf8_lossy(&csr_out.stderr)
    );
    // Neither LibreSSL nor OpenSSL 1.1.1 has `x509 -copy_extensions`; pass the
    // leaf extensions via `-extfile`.
    let ext = dir.join("server.ext");
    std::fs::write(
        &ext,
        "subjectAltName=IP:127.0.0.1\nextendedKeyUsage=serverAuth\n",
    )
    .expect("write server ext");
    let sign = std::process::Command::new(openssl)
        .args(["x509", "-req", "-in"])
        .arg(&csr)
        .arg("-CA")
        .arg(&ca)
        .arg("-CAkey")
        .arg(&ca_key)
        .args(["-CAcreateserial", "-out"])
        .arg(&cert)
        .args(["-days", "1", "-sha256", "-extfile"])
        .arg(&ext)
        .output()
        .expect("openssl sign");
    assert!(
        sign.status.success(),
        "{}",
        String::from_utf8_lossy(&sign.stderr)
    );

    let script = dir.join("redir_https.py");
    std::fs::write(
        &script,
        r#"
import ssl, sys
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer

cert, key, location = sys.argv[1], sys.argv[2], sys.argv[3]

class H(BaseHTTPRequestHandler):
    def do_GET(self):
        self.send_response(302)
        self.send_header("Location", location)
        self.end_headers()
    def log_message(self, *args):
        pass

httpd = ThreadingHTTPServer(("127.0.0.1", 0), H)
ctx = ssl.SSLContext(ssl.PROTOCOL_TLS_SERVER)
ctx.load_cert_chain(cert, key)
httpd.socket = ctx.wrap_socket(httpd.socket, server_side=True)
print(httpd.server_address[1], flush=True)
httpd.serve_forever()
"#,
    )
    .unwrap();

    let (mut child, port) = spawn_python_fixture_with_port(|| {
        let mut cmd = std::process::Command::new("python3");
        cmd.arg(&script)
            .arg(&cert)
            .arg(&key)
            .arg("http://127.0.0.1:9/downgrade");
        cmd
    });
    thread::sleep(Duration::from_millis(50));
    let url = format!("https://127.0.0.1:{port}/video.bin");
    let pem = std::fs::read(&ca).unwrap();
    let client = HttpClient::builder()
        .root_policy(RootPolicy::CustomOnly)
        .add_root_certificate_pem(&pem)
        .unwrap()
        .build()
        .unwrap_or_else(|e| {
            panic!(
                "HttpClient rejected the fixture CA: {e:?}\n{}",
                ca_text_dump(openssl, &ca)
            )
        });
    let input = client.input(&url).build().unwrap();
    let result = FfmpegContext::builder()
        .input(input)
        .output(dir.join("out.mp4").to_str().unwrap())
        .build();
    let _ = child.kill();
    let _ = child.wait();
    let err = match result {
        Ok(_) => panic!("expected HTTPS to HTTP redirect to be rejected"),
        Err(err) => err,
    };
    let msg = err.to_string();
    assert!(
        msg.contains("HTTPS to HTTP") || msg.contains("HttpInput") || msg.contains("HTTP"),
        "{msg}"
    );
    let _ = std::fs::remove_dir_all(dir);
}
