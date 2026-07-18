//! Render frames in Rust and push them straight into an encoded video — no
//! intermediate files, no `ffmpeg` subprocess.
//!
//! This draws a procedural "plasma" animation (a sum of shifting sine fields)
//! entirely in memory and streams each RGBA frame into a [`VideoWriter`], which
//! encodes them to an MP4. It is the frame-push counterpart of the FFmpeg CLI:
//!
//! ```sh
//! ffmpeg -f rawvideo -pix_fmt rgba -s 640x360 -r 30 -i - -c:v mpeg4 -q:v 5 plasma.mp4
//! ```
//!
//! Run it:
//!
//! ```sh
//! cargo run --example frames_to_video
//! ```

use ez_ffmpeg::{Output, VideoWriter};

const WIDTH: u32 = 640;
const HEIGHT: u32 = 360;
const FPS: i32 = 30;
const SECONDS: i32 = 8;

fn main() -> Result<(), Box<dyn std::error::Error>> {
    // Choose the encoder explicitly. A bare "plasma.mp4" would fall back to
    // mpeg4 at a low default bitrate; naming it lets us also set the quality.
    let output = Output::from("plasma.mp4")
        .set_video_codec("mpeg4")
        .set_video_qscale(5);

    let mut writer = VideoWriter::builder(WIDTH, HEIGHT)
        .pixel_format("rgba") // the default; shown for clarity
        .fps(FPS, 1)
        .open(output)?;

    // The writer tells us exactly how many bytes one frame must be.
    let frame_size = writer.frame_size();
    let mut frame = vec![0u8; frame_size];

    let total = FPS * SECONDS;
    for i in 0..total {
        render_plasma(&mut frame, WIDTH, HEIGHT, i as f32);
        // write() copies the borrowed buffer, so we can reuse `frame` next loop.
        writer.write(&frame)?;
    }

    // finish() drains the encoder, writes the container trailer, and reports the
    // first pipeline error if there was one.
    writer.finish()?;
    println!("Wrote {total} frames to plasma.mp4");
    Ok(())
}

/// Fills `buf` (tightly packed RGBA) with a plasma pattern for frame `t`.
fn render_plasma(buf: &mut [u8], width: u32, height: u32, t: f32) {
    let w = width as f32;
    let h = height as f32;
    let phase = t * 0.06;
    for y in 0..height {
        let fy = y as f32 / h;
        for x in 0..width {
            let fx = x as f32 / w;
            // Sum of a few sine fields at different orientations and speeds.
            let v = (fx * 9.0 + phase).sin()
                + (fy * 7.0 - phase * 1.3).sin()
                + ((fx + fy) * 6.0 + phase * 0.7).sin()
                + ((fx - fy) * 8.0 - phase).sin();
            // Map [-4, 4] → [0, 1] and split into RGB via phase offsets.
            let n = (v + 4.0) / 8.0;
            let r = (0.5 + 0.5 * (n * std::f32::consts::TAU).sin()) * 255.0;
            let g = (0.5 + 0.5 * (n * std::f32::consts::TAU + 2.094).sin()) * 255.0;
            let b = (0.5 + 0.5 * (n * std::f32::consts::TAU + 4.188).sin()) * 255.0;
            let idx = ((y * width + x) * 4) as usize;
            buf[idx] = r as u8;
            buf[idx + 1] = g as u8;
            buf[idx + 2] = b as u8;
            buf[idx + 3] = 255;
        }
    }
}
