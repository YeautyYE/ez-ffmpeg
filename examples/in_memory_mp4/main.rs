//! Encode pushed frames into an MP4 that lives entirely in a `Vec<u8>` — no
//! file touched. MP4 is a seekable container: the muxer rewinds to patch the
//! `moov`/`stco` atoms when finalizing, so the output needs BOTH a write and a
//! seek callback. (A streaming container like FLV or MPEG-TS needs no seek — see
//! the note at the bottom.)
//!
//! Run it:
//!
//! ```sh
//! cargo run --example in_memory_mp4
//! ```

use ez_ffmpeg::{Output, VideoWriter};
use std::sync::{Arc, Mutex};

const WIDTH: u32 = 320;
const HEIGHT: u32 = 240;
const FPS: i32 = 25;

/// A growable, seekable byte sink. FFmpeg calls `write`/`seek` from its muxer
/// thread, so it lives behind a mutex and is shared with the caller to read the
/// finished bytes afterwards.
#[derive(Default)]
struct MemorySink {
    buf: Vec<u8>,
    pos: usize,
}

impl MemorySink {
    fn write(&mut self, data: &[u8]) -> i32 {
        let end = self.pos + data.len();
        if end > self.buf.len() {
            self.buf.resize(end, 0);
        }
        self.buf[self.pos..end].copy_from_slice(data);
        self.pos = end;
        data.len() as i32
    }

    fn seek(&mut self, offset: i64, whence: i32) -> i64 {
        if whence == ffmpeg_sys_next::AVSEEK_SIZE {
            return self.buf.len() as i64;
        }
        let base = match whence {
            ffmpeg_sys_next::SEEK_SET => 0,
            ffmpeg_sys_next::SEEK_CUR => self.pos as i64,
            ffmpeg_sys_next::SEEK_END => self.buf.len() as i64,
            _ => return -1,
        };
        let target = base + offset;
        if target < 0 {
            return -1;
        }
        self.pos = target as usize;
        if self.pos > self.buf.len() {
            self.buf.resize(self.pos, 0);
        }
        self.pos as i64
    }
}

fn main() -> Result<(), Box<dyn std::error::Error>> {
    let sink = Arc::new(Mutex::new(MemorySink::default()));

    let write_sink = sink.clone();
    let seek_sink = sink.clone();
    let output =
        Output::new_by_write_callback(move |data: &[u8]| write_sink.lock().unwrap().write(data))
            .set_seek_callback(move |offset: i64, whence: i32| {
                seek_sink.lock().unwrap().seek(offset, whence)
            })
            .set_format("mp4")
            .set_video_codec("mpeg4")
            .set_video_qscale(5);

    let mut writer = VideoWriter::builder(WIDTH, HEIGHT)
        .fps(FPS, 1)
        .open(output)?;

    // A simple sweeping-bar animation so the bytes are non-trivial.
    let mut frame = vec![0u8; writer.frame_size()];
    for i in 0..50 {
        let bar = (i * WIDTH as usize / 50) as u32;
        for y in 0..HEIGHT {
            for x in 0..WIDTH {
                let idx = ((y * WIDTH + x) * 4) as usize;
                let on = x == bar;
                frame[idx] = if on { 255 } else { 20 };
                frame[idx + 1] = if on { 255 } else { 20 };
                frame[idx + 2] = if on { 255 } else { 40 };
                frame[idx + 3] = 255;
            }
        }
        writer.write(&frame)?;
    }
    writer.finish()?;

    let bytes = std::mem::take(&mut sink.lock().unwrap().buf);
    println!("Encoded an in-memory MP4 of {} bytes", bytes.len());
    // The bytes are a complete, valid MP4; write them out to prove it if you like:
    // std::fs::write("in_memory.mp4", &bytes)?;

    // Streaming containers do not need the seek callback: an FLV/MPEG-TS output
    // built with `Output::new_by_write_callback(...).set_format("flv")` writes
    // straight through without rewinding.
    Ok(())
}
