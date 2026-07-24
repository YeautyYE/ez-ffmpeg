//! Ignored micro-benchmarks for the RTMP fanout serialization kernel: the
//! per-watcher cost of the current path (one `ChunkSerializer::serialize`
//! per watcher) against the shared path (serialize once per channel, then a
//! `Bytes::clone` + queue push per watcher).
//!
//! Run with:
//!
//! ```text
//! cargo test --release --features rtmp bench_fanout -- --ignored --nocapture
//! ```
//!
//! Scenarios:
//! - `serialize_vs_clone`: per-payload-size table of the two per-watcher
//!   kernels (512 B audio tag up to 256 KiB keyframe),
//! - `aggregate_model`: one 16 KiB video message fanned out to W watchers,
//!   whole-loop time for the current path (W serializes) vs the shared path
//!   (1 serialize + W clone/push),
//! - `reference_costs`: the primitive costs the models are grounded on
//!   (16 KiB alloc+memcpy, `Bytes` refcount clone, `Instant::now`).
//!
//! The serialize kernel mirrors what the live fanout does per watcher:
//! `RtmpMessage::VideoData -> into_message_payload -> serialize(_, false,
//! can_be_dropped = true)` with the serializer pinned to the 4096-byte
//! outbound chunk size at construction — `send_video_data` in rml_rtmp 0.8.0
//! is exactly that sequence. The clone kernel mirrors the shared path's
//! per-watcher work — the shape of `WriteQueue::push_entry`: a `Bytes`
//! refcount bump, a `VecDeque` push, byte accounting and the entry
//! timestamp.

use bytes::Bytes;
use rml_rtmp::chunk_io::ChunkSerializer;
use rml_rtmp::messages::RtmpMessage;
use rml_rtmp::time::RtmpTimestamp;
use std::collections::VecDeque;
use std::hint::black_box;
use std::time::{Duration, Instant};

/// The server's outbound chunk size (`ServerSessionConfig::new()` default,
/// pinned once at session construction; see `OUTBOUND_CHUNK_SIZE` in
/// `rtmp_scheduler`).
const CHUNK_SIZE: u32 = 4096;

fn new_serializer() -> ChunkSerializer {
    let mut serializer = ChunkSerializer::new();
    // Same pinning the server session performs once at construction. The
    // returned SetChunkSize packet is a session-level announcement, not part
    // of the media kernel under measurement.
    let _ = serializer
        .set_max_chunk_size(CHUNK_SIZE, RtmpTimestamp::new(0))
        .expect("chunk size is valid");
    serializer
}

/// Time-based sampling: each of `runs` samples executes `f` until ~60 ms
/// elapse, yielding ns/op; the median across samples is returned.
fn bench<F: FnMut()>(runs: usize, mut f: F) -> f64 {
    let target = Duration::from_millis(60);
    let warmup = Instant::now();
    while warmup.elapsed() < Duration::from_millis(20) {
        f();
    }
    let mut samples: Vec<f64> = (0..runs)
        .map(|_| {
            let t0 = Instant::now();
            let mut n = 0u64;
            loop {
                f();
                n += 1;
                if n.is_multiple_of(16) && t0.elapsed() >= target {
                    break;
                }
            }
            t0.elapsed().as_nanos() as f64 / n as f64
        })
        .collect();
    samples.sort_by(|a, b| a.partial_cmp(b).expect("sample is finite"));
    samples[samples.len() / 2]
}

/// Pseudo-random FLV VideoData body bytes (first byte = keyframe + AVC).
fn make_payload(size: usize) -> Bytes {
    let mut v = vec![0u8; size];
    let mut x: u32 = 0x12345678;
    for b in v.iter_mut() {
        x = x.wrapping_mul(1664525).wrapping_add(1013904223);
        *b = (x >> 24) as u8;
    }
    v[0] = 0x17;
    Bytes::from(v)
}

#[test]
#[ignore]
fn bench_fanout_serialize_vs_clone() {
    println!("payload_size,serialize_ns,clone_enqueue_ns,serialized_wire_len,ratio");
    let sizes = [512usize, 4096, 16 * 1024, 64 * 1024, 256 * 1024];

    for &size in &sizes {
        let payload = make_payload(size);

        // Current path: one serialize per watcher (allocations included —
        // dropping the packet frees its Vec, which is part of the
        // per-watcher cost).
        let mut serializer = new_serializer();
        let mut ts: u32 = 1000;
        let mut wire_len = 0usize;
        let serialize_ns = bench(7, || {
            ts = ts.wrapping_add(33);
            let msg = RtmpMessage::VideoData {
                data: payload.clone(),
            };
            let mp = msg
                .into_message_payload(RtmpTimestamp::new(ts), 1)
                .expect("payload conversion");
            let packet = serializer
                .serialize(&mp, false, true)
                .expect("media serialize");
            wire_len = packet.bytes.len();
            black_box(&packet);
        });

        // Shared path per-watcher work: Bytes::clone + queue push +
        // accounting + entry timestamp (the WriteQueue::push_entry shape).
        let shared: Bytes = {
            let mut one_shot = new_serializer();
            let msg = RtmpMessage::VideoData {
                data: payload.clone(),
            };
            let mp = msg
                .into_message_payload(RtmpTimestamp::new(1000), 1)
                .expect("payload conversion");
            Bytes::from(
                one_shot
                    .serialize(&mp, false, true)
                    .expect("media serialize")
                    .bytes,
            )
        };
        let mut queue: VecDeque<(Bytes, usize, Instant)> = VecDeque::with_capacity(1 << 20);
        let mut total_bytes = 0usize;
        let clone_ns = bench(7, || {
            let b = shared.clone();
            total_bytes += b.len();
            queue.push_back((b, 0, Instant::now()));
            if queue.len() >= (1 << 19) {
                queue.clear(); // keep memory bounded; amortized cost negligible
            }
            black_box(total_bytes);
        });

        println!(
            "{},{:.1},{:.1},{},{:.1}x",
            size,
            serialize_ns,
            clone_ns,
            wire_len,
            serialize_ns / clone_ns
        );
    }
}

#[test]
#[ignore]
fn bench_fanout_aggregate_model() {
    println!("fanout_model,W,current_us_per_packet,shared_us_per_packet");
    let payload = make_payload(16 * 1024);
    for &w in &[10usize, 100, 1000] {
        // Current path: W per-session serializes per fanned-out packet.
        let mut sessions: Vec<ChunkSerializer> = (0..w).map(|_| new_serializer()).collect();
        let mut ts = 1000u32;
        let current_ns = bench(5, || {
            ts = ts.wrapping_add(33);
            for s in sessions.iter_mut() {
                let msg = RtmpMessage::VideoData {
                    data: payload.clone(),
                };
                let mp = msg
                    .into_message_payload(RtmpTimestamp::new(ts), 1)
                    .expect("payload conversion");
                black_box(s.serialize(&mp, false, true).expect("media serialize"));
            }
        });

        // Shared path: 1 serialize + W clones/pushes per fanned-out packet.
        let mut serializer = new_serializer();
        let mut queues: Vec<VecDeque<(Bytes, usize, Instant)>> =
            (0..w).map(|_| VecDeque::with_capacity(4096)).collect();
        let shared_ns = bench(5, || {
            ts = ts.wrapping_add(33);
            let msg = RtmpMessage::VideoData {
                data: payload.clone(),
            };
            let mp = msg
                .into_message_payload(RtmpTimestamp::new(ts), 1)
                .expect("payload conversion");
            let shared = Bytes::from(
                serializer
                    .serialize(&mp, false, true)
                    .expect("media serialize")
                    .bytes,
            );
            for q in queues.iter_mut() {
                q.push_back((shared.clone(), 0, Instant::now()));
                if q.len() >= 2048 {
                    q.clear();
                }
            }
        });

        println!(
            "fanout,{},{:.1},{:.1}",
            w,
            current_ns / 1000.0,
            shared_ns / 1000.0
        );
    }
}

#[test]
#[ignore]
fn bench_fanout_reference_costs() {
    println!("reference,op,ns_per_op");
    let src = make_payload(16 * 1024);
    let alloc_memcpy_ns = bench(7, || {
        // alloc + memcpy 16 KiB: the FLV per-tag floor / adapter-copy shape.
        let mut v = Vec::with_capacity(src.len());
        v.extend_from_slice(&src);
        black_box(&v);
    });
    println!("reference,alloc_plus_memcpy_16k,{:.1}", alloc_memcpy_ns);

    let clone_ns = bench(7, || {
        black_box(black_box(&src).clone()); // Bytes refcount clone + drop
    });
    println!("reference,bytes_clone_drop,{:.2}", clone_ns);

    let now_ns = bench(7, || {
        black_box(Instant::now());
    });
    println!("reference,instant_now,{:.2}", now_ns);
}
