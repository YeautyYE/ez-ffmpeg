//! A tiny physics toy: gravity-driven balls bouncing in a box, rendered frame
//! by frame in Rust and pushed into an MP4 with [`VideoWriter`]. Demonstrates a
//! real render loop (mutable per-frame state) feeding the encoder with no
//! temporary files.
//!
//! Run it:
//!
//! ```sh
//! cargo run --example bouncing_balls
//! ```

use ez_ffmpeg::{Output, VideoWriter};

const WIDTH: u32 = 480;
const HEIGHT: u32 = 320;
const FPS: i32 = 60;
const SECONDS: i32 = 10;
const GRAVITY: f32 = 0.35;
const RESTITUTION: f32 = 0.86; // energy kept per bounce

struct Ball {
    x: f32,
    y: f32,
    vx: f32,
    vy: f32,
    r: f32,
    color: [u8; 3],
}

fn main() -> Result<(), Box<dyn std::error::Error>> {
    let output = Output::from("bouncing_balls.mp4")
        .set_video_codec("mpeg4")
        .set_video_qscale(4);

    let mut writer = VideoWriter::builder(WIDTH, HEIGHT)
        .fps(FPS, 1)
        .open(output)?;

    let mut balls = vec![
        Ball {
            x: 90.0,
            y: 60.0,
            vx: 3.1,
            vy: 0.0,
            r: 18.0,
            color: [235, 64, 52],
        },
        Ball {
            x: 240.0,
            y: 40.0,
            vx: -2.2,
            vy: 1.0,
            r: 26.0,
            color: [52, 168, 235],
        },
        Ball {
            x: 360.0,
            y: 90.0,
            vx: 1.6,
            vy: -0.5,
            r: 14.0,
            color: [126, 217, 87],
        },
        Ball {
            x: 180.0,
            y: 120.0,
            vx: -3.0,
            vy: 0.0,
            r: 22.0,
            color: [240, 196, 32],
        },
    ];

    let mut frame = vec![0u8; writer.frame_size()];
    let w = WIDTH as f32;
    let h = HEIGHT as f32;

    for _ in 0..(FPS * SECONDS) {
        for b in &mut balls {
            b.vy += GRAVITY;
            b.x += b.vx;
            b.y += b.vy;
            // Reflect off the walls, losing a little energy each bounce.
            if b.x - b.r < 0.0 {
                b.x = b.r;
                b.vx = -b.vx * RESTITUTION;
            } else if b.x + b.r > w {
                b.x = w - b.r;
                b.vx = -b.vx * RESTITUTION;
            }
            if b.y - b.r < 0.0 {
                b.y = b.r;
                b.vy = -b.vy * RESTITUTION;
            } else if b.y + b.r > h {
                b.y = h - b.r;
                b.vy = -b.vy * RESTITUTION;
            }
        }
        render(&mut frame, &balls);
        writer.write(&frame)?;
    }

    writer.finish()?;
    println!("Wrote bouncing_balls.mp4");
    Ok(())
}

/// Clears to a dark background and draws each ball as a filled circle.
fn render(buf: &mut [u8], balls: &[Ball]) {
    for px in buf.chunks_exact_mut(4) {
        px.copy_from_slice(&[16, 18, 24, 255]);
    }
    for b in balls {
        let r2 = b.r * b.r;
        let x0 = (b.x - b.r).floor().max(0.0) as u32;
        let x1 = (b.x + b.r).ceil().min(WIDTH as f32) as u32;
        let y0 = (b.y - b.r).floor().max(0.0) as u32;
        let y1 = (b.y + b.r).ceil().min(HEIGHT as f32) as u32;
        for y in y0..y1 {
            let dy = y as f32 + 0.5 - b.y;
            for x in x0..x1 {
                let dx = x as f32 + 0.5 - b.x;
                if dx * dx + dy * dy <= r2 {
                    let idx = ((y * WIDTH + x) * 4) as usize;
                    buf[idx] = b.color[0];
                    buf[idx + 1] = b.color[1];
                    buf[idx + 2] = b.color[2];
                    buf[idx + 3] = 255;
                }
            }
        }
    }
}
