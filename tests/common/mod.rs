//! Shared helpers for the integration-test binaries (`shortest.rs`,
//! `streamcopy_deadlock.rs`). Cargo compiles `tests/common/mod.rs` into each
//! test binary as an ordinary module (files under a `tests/` subdirectory are
//! NOT collected as their own test binary), so this is the idiomatic home for
//! helpers shared across integration tests.
#![allow(dead_code)]

use ez_ffmpeg::FfmpegScheduler;
use std::time::Duration;

/// Build a per-binary temp file path. `subdir` namespaces one test binary's
/// scratch files from another's (and labels them under `$TMPDIR` for debugging);
/// `std::process::id()` isolates concurrent runs. Each caller passes its own
/// `subdir`, so the two binaries keep their distinct temp directories.
pub fn tmp_path_in(subdir: &str, name: &str) -> String {
    let dir = std::env::temp_dir().join(format!("{subdir}_{}", std::process::id()));
    std::fs::create_dir_all(&dir).unwrap();
    dir.join(name).to_string_lossy().into_owned()
}

/// A hang is a test failure, not a suite timeout. Runs `scheduler.wait()` on a
/// side thread and turns a `secs`-second stall into a `panic!` naming `scenario`,
/// so a deadlock surfaces here instead of stalling the whole suite.
pub fn wait_with_watchdog(
    scheduler: FfmpegScheduler<ez_ffmpeg::core::scheduler::ffmpeg_scheduler::Running>,
    secs: u64,
    scenario: &str,
) -> ez_ffmpeg::error::Result<()> {
    let (tx, rx) = std::sync::mpsc::channel();
    std::thread::spawn(move || {
        let _ = tx.send(scheduler.wait());
    });
    match rx.recv_timeout(Duration::from_secs(secs)) {
        Ok(result) => result,
        Err(std::sync::mpsc::RecvTimeoutError::Timeout) => {
            panic!("scenario `{scenario}` did not finish within {secs}s (hang)")
        }
        Err(std::sync::mpsc::RecvTimeoutError::Disconnected) => {
            panic!("scenario `{scenario}`: wait() thread panicked before reporting")
        }
    }
}

// ---- packet-sink helpers (tests/packet_sink*.rs) ----

use ez_ffmpeg::packet_sink::{PacketSink, PacketStreamInfo};
use ez_ffmpeg::AVRational;
use std::sync::{Arc, Mutex};

/// Whether the linked FFmpeg build provides the named encoder. The CI FFmpeg
/// is configured without GPL components, so libx264-dependent scenarios must
/// skip instead of fail there.
pub fn have_encoder(name: &str) -> bool {
    ez_ffmpeg::codec::get_encoders()
        .iter()
        .any(|e| e.codec_name == name)
}

/// Owned copy of one stream-info entry, as observed by a sink callback.
#[derive(Clone, Debug)]
pub struct SinkInfo {
    pub stream_index: usize,
    pub codec_name: String,
    pub time_base: AVRational,
    pub extradata: Vec<u8>,
    pub width: i32,
    pub height: i32,
    pub frame_rate: Option<AVRational>,
    pub sample_rate: i32,
}

/// Owned copy of one delivered packet, as observed by a sink callback.
#[derive(Clone, Debug)]
pub struct SinkPkt {
    pub stream_index: usize,
    pub pts: i64,
    pub dts: i64,
    pub duration: i64,
    pub time_base: AVRational,
    pub is_key: bool,
    pub applied_offset: i64,
    pub data: Vec<u8>,
    pub thread: std::thread::ThreadId,
}

/// Everything a packet-sink job reported, in callback order.
#[derive(Clone, Debug)]
pub enum SinkEv {
    Info {
        streams: Vec<SinkInfo>,
        thread: std::thread::ThreadId,
    },
    Pkt(SinkPkt),
    End {
        thread: std::thread::ThreadId,
    },
    Error(String),
}

pub type SinkLog = Arc<Mutex<Vec<SinkEv>>>;

/// A sink that records every callback (accepting them all) into a shared log.
pub fn recording_sink() -> (PacketSink, SinkLog) {
    let log: SinkLog = Arc::new(Mutex::new(Vec::new()));
    let (info_log, pkt_log, end_log, err_log) =
        (log.clone(), log.clone(), log.clone(), log.clone());
    let sink = PacketSink::builder()
        .on_stream_info(move |infos: &[PacketStreamInfo]| {
            info_log.lock().unwrap().push(SinkEv::Info {
                streams: infos
                    .iter()
                    .map(|i| SinkInfo {
                        stream_index: i.stream_index(),
                        codec_name: i.codec_name().to_string(),
                        time_base: i.time_base(),
                        extradata: i.extradata().to_vec(),
                        width: i.width(),
                        height: i.height(),
                        frame_rate: i.frame_rate(),
                        sample_rate: i.sample_rate(),
                    })
                    .collect(),
                thread: std::thread::current().id(),
            });
            0
        })
        .on_packet(move |pkt| {
            pkt_log.lock().unwrap().push(SinkEv::Pkt(SinkPkt {
                stream_index: pkt.stream_index(),
                pts: pkt.pts(),
                dts: pkt.dts(),
                duration: pkt.duration(),
                time_base: pkt.time_base(),
                is_key: pkt.is_key(),
                applied_offset: pkt.applied_offset(),
                data: pkt.data().to_vec(),
                thread: std::thread::current().id(),
            }));
            0
        })
        .on_end(move || {
            end_log.lock().unwrap().push(SinkEv::End {
                thread: std::thread::current().id(),
            })
        })
        .on_error(move |e| err_log.lock().unwrap().push(SinkEv::Error(e.to_string())))
        .build();
    (sink, log)
}

/// The packets of a sink log, in delivery order.
pub fn sink_packets(log: &SinkLog) -> Vec<SinkPkt> {
    log.lock()
        .unwrap()
        .iter()
        .filter_map(|e| match e {
            SinkEv::Pkt(p) => Some(p.clone()),
            _ => None,
        })
        .collect()
}

/// Splits a 4-byte length-prefixed AVCC access unit into NAL payloads,
/// panicking on any structural violation (test-side independent reparse).
pub fn parse_avcc_au(data: &[u8]) -> Vec<Vec<u8>> {
    let mut nals = Vec::new();
    let mut pos = 0usize;
    while pos < data.len() {
        assert!(data.len() - pos >= 4, "truncated NAL length prefix");
        let len =
            u32::from_be_bytes([data[pos], data[pos + 1], data[pos + 2], data[pos + 3]]) as usize;
        pos += 4;
        assert!(len > 0, "zero-length NAL");
        assert!(data.len() - pos >= len, "NAL overruns the AU");
        nals.push(data[pos..pos + len].to_vec());
        pos += len;
    }
    assert!(!nals.is_empty(), "AU without NAL units");
    nals
}

/// Exact rational timestamp equality: t1*tb1 == t2*tb2.
pub fn rational_eq(t1: i64, tb1: AVRational, t2: i64, tb2: AVRational) -> bool {
    (t1 as i128) * (tb1.num as i128) * (tb2.den as i128)
        == (t2 as i128) * (tb2.num as i128) * (tb1.den as i128)
}
