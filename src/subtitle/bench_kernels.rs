//! Ignored micro-benchmark comparing blend kernel variants on masks rendered
//! by the real pure-Rust render pipeline (deterministic recipe:
//! FontProvider::None plus an explicit font file).
//!
//! Run with:
//!
//! ```text
//! cargo test --release --features subtitle bench_blend -- --ignored --nocapture
//! ```
//!
//! Scenarios:
//! - `dense`: several simultaneous styled lines (outline + shadow) at 1080p —
//!   the heavy end of realistic subtitle load,
//! - `sparse`: one short bottom line — the common case,
//! - `solid`: synthetic full-frame 255 mask — the vectorization ceiling
//!   (no skippable pixels at all).

use super::backend::SubtitleRenderer;
use super::blend::{
    self,
    lab::{self, DirectKernel},
    ColorMatrix, ColorRange, OverlayImage, PlaneView, SampleFormat,
};
use super::render::fonts::FontStore;
use super::render::layout::RenderOptions;
use super::render::PureRenderer;
use super::test_util;
use std::hint::black_box;
use std::time::Instant;

/// A copy of one rendered overlay node that outlives the renderer.
struct OwnedImage {
    w: usize,
    h: usize,
    stride: usize,
    bitmap: Vec<u8>,
    color: u32,
    dst_x: i32,
    dst_y: i32,
}

impl OwnedImage {
    fn as_view(&self) -> OverlayImage<'_> {
        OverlayImage {
            w: self.w,
            h: self.h,
            stride: self.stride,
            bitmap: &self.bitmap,
            color: self.color,
            dst_x: self.dst_x,
            dst_y: self.dst_y,
        }
    }
}

/// Per-image blend parameters, precomputed outside the timed region so the
/// numbers isolate kernel time (the per-node color conversion is a handful of
/// f64 ops per frame and identical across kernels).
struct Prep {
    alpha: u32,
    src: [u32; 3],
}

fn prepare(images: &[OwnedImage], sample: SampleFormat, scale_bits: u32) -> Vec<Prep> {
    images
        .iter()
        .map(|image| {
            let view = image.as_view();
            Prep {
                alpha: sample.alpha_fixed(view.opacity()),
                src: blend::yuv_components(
                    view.rgb(),
                    ColorMatrix::Bt601,
                    ColorRange::Limited,
                    scale_bits,
                ),
            }
        })
        .collect()
}

const FRAME_W: usize = 1920;
const FRAME_H: usize = 1080;

fn ass_1080(events: &str) -> String {
    format!(
        "[Script Info]\n\
         ScriptType: v4.00+\n\
         PlayResX: 1920\n\
         PlayResY: 1080\n\
         \n\
         [V4+ Styles]\n\
         Format: Name, Fontname, Fontsize, PrimaryColour, SecondaryColour, OutlineColour, BackColour, Bold, Italic, Underline, StrikeOut, ScaleX, ScaleY, Spacing, Angle, BorderStyle, Outline, Shadow, Alignment, MarginL, MarginR, MarginV, Encoding\n\
         Style: Default,DejaVu Sans,64,&H00FFFFFF,&H000000FF,&H00000000,&H80000000,0,0,0,0,100,100,0,0,1,3,2,2,60,60,40,1\n\
         Style: Top,DejaVu Sans,44,&H00FFFFFF,&H000000FF,&H00000000,&H80000000,0,0,0,0,100,100,0,0,1,2,1,8,60,60,30,1\n\
         Style: Side,DejaVu Sans,36,&H00FFFF00,&H000000FF,&H00000000,&H80000000,0,0,0,0,100,100,0,0,1,2,0,9,40,40,120,1\n\
         \n\
         [Events]\n\
         Format: Layer, Start, End, Style, Name, MarginL, MarginR, MarginV, Effect, Text\n\
         {events}"
    )
}

fn dense_events() -> &'static str {
    "Dialogue: 0,0:00:00.00,0:00:05.00,Default,,0,0,0,,The quick brown fox jumps over the lazy dog while 0123456789 encoders race across the finish line tonight.\n\
     Dialogue: 0,0:00:00.00,0:00:05.00,Top,,0,0,0,,SPEAKER ONE: This dense scenario exercises many simultaneous glyph runs.\n\
     Dialogue: 0,0:00:00.00,0:00:05.00,Side,,0,0,0,,1080p dense coverage\n\
     Dialogue: 0,0:00:00.00,0:00:05.00,Default,,0,0,0,,{\\an5}(crowd cheering loudly)\n"
}

fn sparse_events() -> &'static str {
    "Dialogue: 0,0:00:00.00,0:00:05.00,Default,,0,0,0,,Hello world.\n"
}

/// Renders `events` at t=1s on a 1920x1080 canvas and copies every visible
/// node out of the renderer-owned list.
fn capture(events: &str) -> Option<Vec<OwnedImage>> {
    let font = test_util::test_font()?;
    let script = super::ass::parse(&ass_1080(events)).expect("parse benchmark script");
    let mut fonts = FontStore::new(false);
    assert!(
        fonts.load_default_font_file(std::path::Path::new(font)),
        "load benchmark font"
    );
    let mut renderer = PureRenderer::new(script, fonts, RenderOptions::default());
    renderer.set_frame_size(FRAME_W as i32, FRAME_H as i32);
    renderer.set_storage_size(FRAME_W as i32, FRAME_H as i32);

    let mut images = Vec::new();
    for image in renderer.render_frame(1000) {
        images.push(OwnedImage {
            w: image.w,
            h: image.h,
            stride: image.stride,
            bitmap: image.bitmap.to_vec(),
            color: image.color,
            dst_x: image.dst_x,
            dst_y: image.dst_y,
        });
    }
    assert!(!images.is_empty(), "benchmark script rendered no images");
    Some(images)
}

fn solid_images() -> Vec<OwnedImage> {
    vec![OwnedImage {
        w: FRAME_W,
        h: FRAME_H,
        stride: FRAME_W,
        bitmap: vec![255u8; FRAME_W * FRAME_H],
        color: 0xFFFFFF00,
        dst_x: 0,
        dst_y: 0,
    }]
}

/// (nodes, mask pixels, nonzero pixels, all-zero 16-px chunks) — the last one
/// predicts how much a 16-wide chunk skip can elide.
fn stats(images: &[OwnedImage]) -> (usize, usize, usize, f64) {
    let mut px = 0usize;
    let mut nonzero = 0usize;
    let mut chunks = 0usize;
    let mut zero_chunks = 0usize;
    for image in images {
        for row in 0..image.h {
            let start = row * image.stride;
            let mask_row = &image.bitmap[start..start + image.w];
            px += mask_row.len();
            nonzero += mask_row.iter().filter(|&&m| m != 0).count();
            for chunk in mask_row.chunks_exact(16) {
                chunks += 1;
                if chunk.iter().all(|&m| m == 0) {
                    zero_chunks += 1;
                }
            }
        }
    }
    let zero_ratio = if chunks == 0 {
        0.0
    } else {
        zero_chunks as f64 / chunks as f64
    };
    (images.len(), px, nonzero, zero_ratio)
}

/// Times one full composite (all nodes onto the plane) and returns the best
/// per-composite nanoseconds over three batches.
fn measure(mut composite: impl FnMut()) -> f64 {
    composite(); // warmup / page-in
    let start = Instant::now();
    composite();
    let once = start.elapsed().as_nanos().max(1);
    let iters = (250_000_000 / once).clamp(10, 50_000) as usize;
    let mut best = f64::INFINITY;
    for _ in 0..3 {
        let start = Instant::now();
        for _ in 0..iters {
            composite();
        }
        best = best.min(start.elapsed().as_nanos() as f64 / iters as f64);
    }
    best
}

fn time_direct(
    kernel: DirectKernel,
    images: &[OwnedImage],
    preps: &[Prep],
    step: usize,
    sample: SampleFormat,
) -> f64 {
    let linesize = FRAME_W * step;
    let mut data = vec![0x40u8; linesize * FRAME_H];
    let ns = measure(|| {
        for (image, prep) in images.iter().zip(preps) {
            if prep.alpha == 0 {
                continue;
            }
            let mut plane = PlaneView {
                data: &mut data,
                linesize,
                pixel_step: step,
            };
            lab::blend_direct_variant(
                kernel,
                &mut plane,
                FRAME_W,
                FRAME_H,
                &image.as_view(),
                prep.src[0],
                prep.alpha,
                sample,
            );
        }
        black_box(&mut data);
    });
    black_box(data.first().copied());
    ns
}

/// Times the 4:2:0 chroma pair (two quarter-resolution pooled planes) with
/// the selected pooled kernel.
fn time_pooled_pair(
    kernel: lab::PooledKernel,
    images: &[OwnedImage],
    preps: &[Prep],
    step: usize,
    sample: SampleFormat,
) -> f64 {
    let (plane_w, plane_h) = (FRAME_W / 2, FRAME_H / 2);
    let linesize = plane_w * step;
    let mut u_plane = vec![0x80u8; linesize * plane_h];
    let mut v_plane = vec![0x80u8; linesize * plane_h];
    let ns = measure(|| {
        for (image, prep) in images.iter().zip(preps) {
            if prep.alpha == 0 {
                continue;
            }
            for (data, comp) in [(&mut u_plane, 1usize), (&mut v_plane, 2usize)] {
                let mut plane = PlaneView {
                    data,
                    linesize,
                    pixel_step: step,
                };
                lab::blend_pooled_variant(
                    kernel,
                    &mut plane,
                    FRAME_W,
                    FRAME_H,
                    &image.as_view(),
                    prep.src[comp],
                    prep.alpha,
                    1,
                    1,
                    sample,
                );
            }
        }
        black_box(&mut u_plane);
        black_box(&mut v_plane);
    });
    black_box(u_plane.first().copied());
    ns
}

/// Times the two-phase route used by `blend_images` for chroma pairs: pool
/// each node's mask once, apply to both components from the sums.
fn time_pooled_pair_two_phase(
    images: &[OwnedImage],
    preps: &[Prep],
    step: usize,
    sample: SampleFormat,
) -> f64 {
    let (plane_w, plane_h) = (FRAME_W / 2, FRAME_H / 2);
    let linesize = plane_w * step;
    let mut u_plane = vec![0x80u8; linesize * plane_h];
    let mut v_plane = vec![0x80u8; linesize * plane_h];
    let mut scratch: Vec<u16> = Vec::new();
    let ns = measure(|| {
        for (image, prep) in images.iter().zip(preps) {
            if prep.alpha == 0 {
                continue;
            }
            let Some(rect) =
                blend::pool_sums_h2(FRAME_W, FRAME_H, &image.as_view(), 1, &mut scratch)
            else {
                continue;
            };
            for (data, comp) in [(&mut u_plane, 1usize), (&mut v_plane, 2usize)] {
                let mut plane = PlaneView {
                    data,
                    linesize,
                    pixel_step: step,
                };
                blend::blend_pooled_from_sums(
                    &mut plane,
                    &scratch,
                    rect,
                    prep.src[comp],
                    prep.alpha,
                    sample,
                );
            }
        }
        black_box(&mut u_plane);
        black_box(&mut v_plane);
    });
    black_box(u_plane.first().copied());
    ns
}

fn print_pooled_table(
    label: &str,
    images: &[OwnedImage],
    preps: &[Prep],
    step: usize,
    sample: SampleFormat,
) {
    println!("  {label}:");
    let reference = time_pooled_pair(lab::PooledKernel::Reference, images, preps, step, sample);
    println!(
        "    {:<16} {:>12.0} ns/frame   {:>5.2}x",
        "Reference", reference, 1.0
    );
    let shipping = time_pooled_pair(lab::PooledKernel::Shipping, images, preps, step, sample);
    println!(
        "    {:<16} {:>12.0} ns/frame   {:>5.2}x",
        "Shipping",
        shipping,
        reference / shipping
    );
    let two_phase = time_pooled_pair_two_phase(images, preps, step, sample);
    println!(
        "    {:<16} {:>12.0} ns/frame   {:>5.2}x",
        "TwoPhase",
        two_phase,
        reference / two_phase
    );
}

fn print_direct_table(
    label: &str,
    images: &[OwnedImage],
    preps: &[Prep],
    step: usize,
    sample: SampleFormat,
) {
    let mut baseline = 0.0f64;
    println!("  {label}:");
    for kernel in lab::ALL_DIRECT {
        let ns = time_direct(kernel, images, preps, step, sample);
        if kernel == DirectKernel::PerPixelSkip {
            baseline = ns;
        }
        println!(
            "    {:<16} {:>12.0} ns/frame   {:>5.2}x",
            format!("{kernel:?}"),
            ns,
            baseline / ns
        );
    }
}

#[test]
#[ignore = "manual micro-benchmark; run in release with --nocapture"]
fn bench_blend_kernels() {
    let scenarios: Vec<(&str, Vec<OwnedImage>)> = {
        let Some(dense) = capture(dense_events()) else {
            eprintln!("skipping: no known test font present on this machine");
            return;
        };
        let sparse = capture(sparse_events()).expect("font present per the check above");
        vec![
            ("dense", dense),
            ("sparse", sparse),
            ("solid", solid_images()),
        ]
    };

    for (name, images) in &scenarios {
        let (nodes, px, nonzero, zero16) = stats(images);
        println!(
            "scenario {name}: nodes={nodes} mask_px={px} nonzero={:.1}% zero16chunks={:.1}%",
            100.0 * nonzero as f64 / px.max(1) as f64,
            100.0 * zero16
        );

        let preps8 = prepare(images, SampleFormat::U8, 8);
        print_direct_table(
            "direct u8 (1080p luma, step 1)",
            images,
            &preps8,
            1,
            SampleFormat::U8,
        );
        print_pooled_table(
            "pooled u8 (4:2:0 chroma pair)",
            images,
            &preps8,
            1,
            SampleFormat::U8,
        );

        if *name == "dense" {
            let preps10 = prepare(images, SampleFormat::U16Le, 10);
            print_direct_table(
                "direct u16le (10-bit luma, step 2)",
                images,
                &preps10,
                2,
                SampleFormat::U16Le,
            );
            print_pooled_table(
                "pooled u16le (10-bit 4:2:0 chroma pair)",
                images,
                &preps10,
                2,
                SampleFormat::U16Le,
            );
        }
        println!();
    }
}
