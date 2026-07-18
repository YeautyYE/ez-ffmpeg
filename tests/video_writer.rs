//! End-to-end coverage for [`ez_ffmpeg::VideoWriter`] — pushing raw frames from
//! Rust into a real encode/mux pipeline.
//!
//! The writer feeds the filtergraph's buffersrc directly through a frame-source
//! worker (no demuxer, no decoder), so these scenarios run on every FFmpeg
//! release the crate supports. Teardown paths (finish/drop/abort, worker
//! failure) run under a watchdog: a hang is a named test failure, not a suite
//! timeout.

use ez_ffmpeg::error::{Error, WriterError};
use ez_ffmpeg::frame_export::{FrameExtractor, PixelLayout};
use ez_ffmpeg::stream_info::{find_video_stream_info, StreamInfo};
use ez_ffmpeg::{Input, Output, PushError, VideoWriter};
use std::time::Duration;

mod common;
use common::tmp_path_in;

fn tmp_path(name: &str) -> String {
    tmp_path_in("ez_ffmpeg_video_writer", name)
}

/// mpeg4 is always available (no libx264 needed) and keeps the tests portable.
fn mp4_output(path: &str) -> Output {
    Output::from(path)
        .set_video_codec("mpeg4")
        .set_video_qscale(6)
}

fn video_nb_frames(path: &str) -> i64 {
    match find_video_stream_info(path)
        .expect("failed to probe output")
        .expect("output has no video stream")
    {
        StreamInfo::Video { nb_frames, .. } => nb_frames,
        other => panic!("expected video stream info, got {other:?}"),
    }
}

/// A solid-colour frame of the exact size the writer expects.
fn frame(writer: &VideoWriter, value: u8) -> Vec<u8> {
    vec![value; writer.frame_size()]
}

/// Runs `job` on a side thread and turns a stall into a named failure.
fn within<T: Send + 'static>(
    secs: u64,
    scenario: &str,
    job: impl FnOnce() -> T + Send + 'static,
) -> T {
    let (tx, rx) = std::sync::mpsc::channel();
    std::thread::spawn(move || {
        let _ = tx.send(job());
    });
    match rx.recv_timeout(Duration::from_secs(secs)) {
        Ok(v) => v,
        Err(std::sync::mpsc::RecvTimeoutError::Timeout) => {
            panic!("scenario `{scenario}` did not finish within {secs}s (hang)")
        }
        Err(std::sync::mpsc::RecvTimeoutError::Disconnected) => {
            panic!("scenario `{scenario}`: worker thread panicked")
        }
    }
}

/// I1: N frames in, N frames out, at the declared rate.
#[test]
fn writes_all_frames_to_mp4() {
    let out = tmp_path("i1_thirty_frames.mp4");
    let out2 = out.clone();
    within(30, "i1", move || {
        let mut w = VideoWriter::builder(64, 48)
            .fps(30, 1)
            .open(mp4_output(&out2))
            .expect("open failed");
        for i in 0..30 {
            w.write_owned(frame(&w, i as u8)).expect("write failed");
        }
        w.finish().expect("finish failed");
    });
    assert_eq!(video_nb_frames(&out), 30);
}

/// I2: a single frame produces exactly one output frame (issue #26 regression —
/// no double-frame workaround needed).
#[test]
fn single_frame_produces_one_frame() {
    let out = tmp_path("i2_single.mp4");
    let out2 = out.clone();
    within(20, "i2", move || {
        let mut w = VideoWriter::builder(64, 48)
            .fps(30, 1)
            .open(mp4_output(&out2))
            .unwrap();
        w.write_owned(frame(&w, 200)).unwrap();
        w.finish().unwrap();
    });
    assert_eq!(video_nb_frames(&out), 1);
}

/// I3: zero frames then finish is a clean success (CLI parity). Exercises the
/// zero-frame `fg_send_eof` fallback: the graph is configured from the
/// builder-installed fallback parameters, not from any frame.
#[test]
fn zero_frames_finishes_ok() {
    let out = tmp_path("i3_zero.mp4");
    let out2 = out.clone();
    within(20, "i3", move || {
        let w = VideoWriter::builder(64, 48)
            .fps(30, 1)
            .open(mp4_output(&out2))
            .unwrap();
        w.finish().expect("zero-frame finish must succeed");
    });
}

/// I4: open() returns promptly without waiting for the first frame.
#[test]
fn open_returns_without_first_frame() {
    let out = tmp_path("i4_open.mp4");
    let out2 = out.clone();
    within(10, "i4", move || {
        let w = VideoWriter::builder(64, 48)
            .fps(30, 1)
            .open(mp4_output(&out2))
            .expect("open should return before any frame is pushed");
        // Immediately finish so the empty pipeline tears down cleanly.
        w.finish().unwrap();
    });
}

/// I5: dropping without finish still drains every pushed frame (Drop closes
/// ingress, the frame source emits the EOF marker, then wait() — it is not a
/// stop() that would truncate the tail).
#[test]
fn drop_without_finish_keeps_all_frames() {
    let out = tmp_path("i5_drop.mp4");
    let out2 = out.clone();
    within(20, "i5", move || {
        let mut w = VideoWriter::builder(64, 48)
            .fps(30, 1)
            .open(mp4_output(&out2))
            .unwrap();
        for i in 0..10 {
            w.write_owned(frame(&w, i)).unwrap();
        }
        drop(w); // blocks until the pipeline drains
    });
    assert_eq!(video_nb_frames(&out), 10);
}

/// I6: an unknown encoder surfaces as a typed error from open().
#[test]
fn unknown_encoder_fails_open() {
    let out = tmp_path("i6_bad_codec.mp4");
    let out2 = out.clone();
    let result = within(15, "i6", move || {
        VideoWriter::builder(64, 48)
            .fps(30, 1)
            .open(Output::from(out2.as_str()).set_video_codec("no_such_encoder_xyz"))
            .map(|_| ())
    });
    assert!(result.is_err(), "unknown encoder must fail open()");
}

/// I7: a depth-1 queue applies backpressure without dropping frames. 120
/// frames through a 64-shell pool also forces shell recycling many times over.
#[test]
fn backpressure_delivers_every_frame() {
    let out = tmp_path("i7_backpressure.mp4");
    let out2 = out.clone();
    within(60, "i7", move || {
        let mut w = VideoWriter::builder(64, 48)
            .fps(30, 1)
            .queue_capacity(1)
            .open(mp4_output(&out2))
            .unwrap();
        for i in 0..120 {
            w.write_owned(frame(&w, i as u8)).unwrap();
        }
        w.finish().unwrap();
    });
    assert_eq!(video_nb_frames(&out), 120);
}

/// I9: a planar format (yuv420p) pushes straight through with no conversion.
#[test]
fn yuv420p_direct_push() {
    let out = tmp_path("i9_yuv420p.mp4");
    let out2 = out.clone();
    within(20, "i9", move || {
        let mut w = VideoWriter::builder(64, 48)
            .pixel_format("yuv420p")
            .fps(30, 1)
            .open(mp4_output(&out2))
            .unwrap();
        for i in 0..15 {
            w.write_owned(frame(&w, i)).unwrap();
        }
        w.finish().unwrap();
    });
    assert_eq!(video_nb_frames(&out), 15);
}

/// I10: abort() returns and does not panic; the output is not asserted.
#[test]
fn abort_returns_cleanly() {
    let out = tmp_path("i10_abort.mp4");
    let out2 = out.clone();
    within(20, "i10", move || {
        let mut w = VideoWriter::builder(64, 48)
            .fps(30, 1)
            .open(mp4_output(&out2))
            .unwrap();
        for i in 0..5 {
            w.write_owned(frame(&w, i)).unwrap();
        }
        w.abort();
    });
}

/// I11: an output that consumes no video is rejected at open(), rather than
/// leaving write() to block forever on a full queue.
#[test]
fn no_video_destination_is_rejected() {
    let out = tmp_path("i11_no_video.mp4");
    let out2 = out.clone();
    let result = within(15, "i11", move || {
        VideoWriter::builder(64, 48)
            .fps(30, 1)
            .open(mp4_output(&out2).disable_video())
            .map(|_| ())
    });
    assert!(
        matches!(result, Err(Error::Writer(WriterError::NoVideoDestination))),
        "expected NoVideoDestination, got {result:?}"
    );
}

/// A wrong-sized frame is a typed InvalidSize with both counts.
#[test]
fn wrong_frame_size_is_invalid_size() {
    let out = tmp_path("wrong_size.mp4");
    let out2 = out.clone();
    within(15, "wrong_size", move || {
        let mut w = VideoWriter::builder(64, 48)
            .fps(30, 1)
            .open(mp4_output(&out2))
            .unwrap();
        let expected = w.frame_size();
        let err = w.write(&[0u8; 3]).unwrap_err();
        assert!(matches!(
            err,
            PushError::InvalidSize { expected: e, got: 3 } if e == expected
        ));
        w.finish().unwrap();
    });
}

/// EOF-tail correctness through a buffering filter: `reverse` holds every
/// frame until its input reaches EOF. Only the explicit in-band EOF marker
/// closes the buffersrc — if end-of-stream were just the ingress sender
/// dropping, the filter worker would flush its outputs without ever closing
/// the source and all 10 frames would be lost (nb_frames 0).
#[test]
fn finish_flushes_buffering_filter() {
    let out = tmp_path("eof_reverse_finish.mp4");
    let out2 = out.clone();
    within(30, "eof_reverse_finish", move || {
        let mut w = VideoWriter::builder(64, 48)
            .fps(30, 1)
            .filter_desc("reverse")
            .open(mp4_output(&out2))
            .unwrap();
        for i in 0..10 {
            w.write_owned(frame(&w, i * 20)).unwrap();
        }
        w.finish().expect("finish through reverse failed");
    });
    assert_eq!(video_nb_frames(&out), 10);
}

/// Drop (no finish) must enqueue the same EOF marker: the buffering filter
/// still flushes every frame.
#[test]
fn drop_flushes_buffering_filter() {
    let out = tmp_path("eof_reverse_drop.mp4");
    let out2 = out.clone();
    within(30, "eof_reverse_drop", move || {
        let mut w = VideoWriter::builder(64, 48)
            .fps(30, 1)
            .filter_desc("reverse")
            .open(mp4_output(&out2))
            .unwrap();
        for i in 0..6 {
            w.write_owned(frame(&w, i * 30)).unwrap();
        }
        drop(w);
    });
    assert_eq!(video_nb_frames(&out), 6);
}

/// Plane-aware fill, end to end and byte-exact: odd-geometry planar frames
/// (yuv420p 65x49, chroma 33x25) written through a lossless rawvideo pipeline
/// must come back as the identical byte stream. The rawvideo encoder re-packs
/// each AVFrame tightly (align 1), so any stride/plane mistake in the fill —
/// e.g. a flat memcpy into the padded frame buffer — corrupts the output
/// bytes.
#[test]
fn odd_size_planar_content_round_trips_exactly() {
    let out = tmp_path("plane_exact.raw");
    let out2 = out.clone();
    let (w_px, h_px) = (65u32, 49u32);
    let frames: Vec<Vec<u8>> = (0..3)
        .map(|n: usize| {
            // Distinct deterministic per-byte pattern per frame; primes avoid
            // any accidental alignment with plane or row boundaries.
            let size = 65 * 49 + 2 * (33 * 25);
            (0..size).map(|i| ((i * 7 + n * 31) % 251) as u8).collect()
        })
        .collect();
    let frames2 = frames.clone();
    within(30, "plane_exact", move || {
        let mut w = VideoWriter::builder(w_px, h_px)
            .pixel_format("yuv420p")
            .fps(30, 1)
            .open(
                Output::from(out2.as_str())
                    .set_format("rawvideo")
                    .set_video_codec("rawvideo"),
            )
            .unwrap();
        assert_eq!(w.frame_size(), frames2[0].len());
        for f in &frames2 {
            w.write(f).unwrap();
        }
        w.finish().unwrap();
    });
    let written = std::fs::read(&out).expect("read raw output");
    let expected: Vec<u8> = frames.concat();
    assert_eq!(
        written.len(),
        expected.len(),
        "raw output must contain exactly the pushed frames"
    );
    assert_eq!(written, expected, "plane content must survive byte-exact");
}

/// Real encode → decode round trip: distinct solid-gray frames through mpeg4
/// come back in order with the expected pixel values (lossy tolerance).
#[test]
fn encoded_frames_round_trip_with_expected_pixels() {
    let out = tmp_path("roundtrip_mpeg4.mp4");
    let out2 = out.clone();
    let values: [u8; 5] = [40, 70, 100, 130, 160];
    within(30, "roundtrip_mpeg4", move || {
        let mut w = VideoWriter::builder(64, 48)
            .fps(30, 1)
            .open(mp4_output(&out2))
            .unwrap();
        for v in values {
            // Solid gray: R=G=B=v (alpha ignored by the yuv conversion).
            w.write_owned(frame(&w, v)).unwrap();
        }
        w.finish().unwrap();
    });
    let decoded = FrameExtractor::new(Input::from(out.as_str()))
        .pixel(PixelLayout::Rgb24)
        .collect_frames()
        .expect("extract frames");
    assert_eq!(decoded.len(), values.len(), "frame count after decode");
    for (i, (df, v)) in decoded.iter().zip(values).enumerate() {
        let bytes = df.as_bytes();
        let center = (24 * df.row_bytes()) + (32 * 3);
        for c in 0..3 {
            let got = bytes[center + c] as i32;
            assert!(
                (got - v as i32).abs() <= 16,
                "frame {i} channel {c}: got {got}, want ~{v}"
            );
        }
    }
}

/// A filter_desc actually transforms the frames: hue=s=0 desaturates a solid
/// red into gray (R ≈ G ≈ B).
#[test]
fn filter_desc_transforms_frames() {
    let out = tmp_path("filter_hue.mp4");
    let out2 = out.clone();
    within(30, "filter_hue", move || {
        let mut w = VideoWriter::builder(64, 48)
            .fps(30, 1)
            .filter_desc("hue=s=0")
            .open(mp4_output(&out2))
            .unwrap();
        let size = w.frame_size();
        // Solid red RGBA.
        let red: Vec<u8> = (0..size)
            .map(|i| match i % 4 {
                0 => 200,
                3 => 255,
                _ => 0,
            })
            .collect();
        for _ in 0..5 {
            w.write(&red).unwrap();
        }
        w.finish().unwrap();
    });
    assert_eq!(video_nb_frames(&out), 5);
    let decoded = FrameExtractor::new(Input::from(out.as_str()))
        .pixel(PixelLayout::Rgb24)
        .collect_frames()
        .expect("extract frames");
    let df = &decoded[2];
    let bytes = df.as_bytes();
    let center = (24 * df.row_bytes()) + (32 * 3);
    let (r, g, b) = (
        bytes[center] as i32,
        bytes[center + 1] as i32,
        bytes[center + 2] as i32,
    );
    assert!(
        (r - g).abs() <= 12 && (g - b).abs() <= 12,
        "hue=s=0 must desaturate: got ({r},{g},{b})"
    );
}

/// A filter_desc that does not consume exactly one video input and produce
/// exactly one video output is rejected at open() with a typed error — a
/// stranded second input pad would otherwise buffer forever.
#[test]
fn invalid_filter_shapes_are_rejected() {
    let shapes = [
        ("[a][b]overlay", "two inputs"),
        ("split", "two outputs"),
        ("anull", "audio pads"),
    ];
    for (desc, label) in shapes {
        let out = tmp_path(&format!("shape_{}.mp4", label.replace(' ', "_")));
        let result = VideoWriter::builder(64, 48)
            .filter_desc(desc)
            .open(mp4_output(&out))
            .map(|_| ());
        assert!(
            matches!(result, Err(Error::Writer(WriterError::FilterShape { .. }))),
            "{label} ({desc}) must be FilterShape, got {result:?}"
        );
    }
}

/// A pipeline that dies mid-stream must unblock write() with PipelineClosed
/// (never hang against a dead consumer) and surface the real error from
/// finish(). The h263 encoder only accepts a fixed whitelist of picture sizes
/// (128x96, 176x144, …) and rejects 64x48 when it opens on the first frame —
/// after open() already succeeded.
#[test]
fn worker_failure_unblocks_write_and_finish_reports() {
    let out = tmp_path("enc_open_fail.avi");
    let out2 = out.clone();
    within(30, "enc_open_fail", move || {
        let mut w = VideoWriter::builder(64, 48)
            .fps(30, 1)
            .queue_capacity(1)
            .open(Output::from(out2.as_str()).set_video_codec("h263"))
            .expect("open defers encoder init, must succeed");
        let mut closed = false;
        for i in 0..300 {
            if w.write_owned(frame(&w, i as u8)).is_err() {
                closed = true;
                break;
            }
        }
        assert!(
            closed,
            "write must return PipelineClosed once the encoder failed"
        );
        let result = w.finish();
        assert!(
            result.is_err(),
            "finish must surface the encoder failure, got {result:?}"
        );
    });
}

/// Process-global state stays clean across job types: a normal decode/encode
/// job followed by a writer job (and its own teardown) in the same process.
/// Frame pools are per-scheduler, so this guards the shared globals (FFmpeg
/// init, registries) rather than shell identity.
#[test]
fn writer_after_decode_job_in_same_process() {
    let decode_out = tmp_path("pre_decode.mp4");
    let writer_out = tmp_path("post_writer.mp4");
    let decode_out2 = decode_out.clone();
    let writer_out2 = writer_out.clone();
    within(60, "writer_after_decode", move || {
        // A regular demuxer-driven job first (lavfi input → mpeg4).
        let scheduler = ez_ffmpeg::FfmpegContext::builder()
            .input(Input::from("color=c=red:s=64x48:r=15:d=0.3").set_format("lavfi"))
            .output(mp4_output(&decode_out2))
            .build()
            .expect("decode job build")
            .start()
            .expect("decode job start");
        scheduler.wait().expect("decode job wait");

        // Then a writer job in the same process.
        let mut w = VideoWriter::builder(64, 48)
            .fps(30, 1)
            .open(mp4_output(&writer_out2))
            .unwrap();
        for i in 0..20 {
            w.write_owned(frame(&w, i)).unwrap();
        }
        w.finish().unwrap();
    });
    assert!(video_nb_frames(&decode_out) > 0);
    assert_eq!(video_nb_frames(&writer_out), 20);
}
