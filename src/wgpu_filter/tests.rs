use crate::core::filter::frame_filter_context::FrameFilterContext;
use crate::filter::frame_filter::{FrameFilter, FrameFilterError};
use crate::wgpu_filter::shaders;
use crate::wgpu_filter::wgpu_frame_filter::WgpuFrameFilter;
use crate::wgpu_filter::WgpuFilterError;
use ffmpeg_next::Frame;
use ffmpeg_sys_next::{
    av_frame_get_buffer, av_frame_is_writable, av_frame_make_writable, av_frame_ref, AVPixelFormat,
};
use std::collections::HashMap;

fn make_ctx(map: &mut HashMap<String, Box<dyn std::any::Any + Send>>) -> FrameFilterContext<'_> {
    FrameFilterContext::new("wgpu_test", map)
}

/// Builds a planar YUV frame; luma is either a constant or a deterministic
/// gradient, chroma is neutral (128). Neutral chroma is deliberate: saturated
/// chroma at extreme luma goes out of the RGB gamut and clamps (physically
/// correct but not invertible), which would mask grid-alignment bugs behind
/// gamut error.
fn make_planar_frame(
    w: i32,
    h: i32,
    fmt: AVPixelFormat,
    const_luma: Option<u8>,
    pts: i64,
) -> Frame {
    let (sub_x, sub_y) = match fmt {
        AVPixelFormat::AV_PIX_FMT_YUV420P | AVPixelFormat::AV_PIX_FMT_YUVJ420P => (2usize, 2usize),
        AVPixelFormat::AV_PIX_FMT_YUV422P => (2, 1),
        AVPixelFormat::AV_PIX_FMT_YUV444P => (1, 1),
        other => panic!("unsupported test format {other:?}"),
    };
    unsafe {
        let mut frame = Frame::empty();
        let p = frame.as_mut_ptr();
        (*p).width = w;
        (*p).height = h;
        (*p).format = fmt as i32;
        assert!(av_frame_get_buffer(p, 1) >= 0);
        (*p).pts = pts;
        (*p).time_base = ffmpeg_sys_next::AVRational { num: 1, den: 30 };

        let ls_y = (*p).linesize[0] as usize;
        let y = std::slice::from_raw_parts_mut((*p).data[0], ls_y * h as usize);
        for row in 0..h as usize {
            for col in 0..w as usize {
                y[row * ls_y + col] = const_luma.unwrap_or(((16 + row * 2 + col) % 220 + 16) as u8);
            }
        }
        let cw = (w as usize).div_ceil(sub_x);
        let ch = (h as usize).div_ceil(sub_y);
        for plane in 1..=2 {
            let ls = (*p).linesize[plane] as usize;
            let data = std::slice::from_raw_parts_mut((*p).data[plane], ls * ch);
            for row in 0..ch {
                for col in 0..cw {
                    data[row * ls + col] = 128;
                }
            }
        }
        frame
    }
}

fn make_yuv420p_frame(w: i32, h: i32) -> Frame {
    make_planar_frame(w, h, AVPixelFormat::AV_PIX_FMT_YUV420P, None, 0)
}

/// A props-only frame like the EOF timestamp marker decoders send through
/// pipelines: valid AVFrame, no data buffers.
fn make_marker_frame(pts: i64) -> Frame {
    unsafe {
        let mut frame = Frame::empty();
        (*frame.as_mut_ptr()).pts = pts;
        frame
    }
}

pub(super) fn init_filter(filter: &mut WgpuFrameFilter) -> bool {
    let mut map = HashMap::new();
    let mut ctx = make_ctx(&mut map);
    match filter.init(&mut ctx) {
        Ok(()) => true,
        Err(e) if e.to_string().contains("adapter") || e.to_string().contains("device") => {
            eprintln!("skipping wgpu test (no GPU): {e}");
            false
        }
        Err(e) => panic!("init failed: {e}"),
    }
}

/// Emulates the pipeline driver: `filter_frame` per input, `request_frame`
/// polling after each input and until all expected outputs have drained.
pub(super) fn drive(
    filter: &mut WgpuFrameFilter,
    inputs: Vec<Frame>,
    expected: usize,
) -> Vec<Frame> {
    let mut map = HashMap::new();
    let mut ctx = make_ctx(&mut map);
    let mut out = Vec::new();
    for frame in inputs {
        if let Some(f) = filter.filter_frame(frame, &mut ctx).expect("filter_frame") {
            out.push(f);
        }
        while let Some(f) = filter.request_frame(&mut ctx).expect("request_frame") {
            out.push(f);
        }
    }
    for _ in 0..2000 {
        while let Some(f) = filter.request_frame(&mut ctx).expect("request_frame") {
            out.push(f);
        }
        if out.len() >= expected {
            break;
        }
        std::thread::sleep(std::time::Duration::from_millis(1));
    }
    assert_eq!(out.len(), expected, "expected {expected} outputs");
    out
}

fn max_luma_diff(out: &Frame, expected: &Frame, w: usize, h: usize) -> i32 {
    unsafe {
        let o = out.as_ptr();
        let e = expected.as_ptr();
        let ls_o = (*o).linesize[0] as usize;
        let ls_e = (*e).linesize[0] as usize;
        let od = std::slice::from_raw_parts((*o).data[0], ls_o * h);
        let ed = std::slice::from_raw_parts((*e).data[0], ls_e * h);
        let mut max_diff = 0i32;
        for row in 0..h {
            for col in 0..w {
                let d = (od[row * ls_o + col] as i32 - ed[row * ls_e + col] as i32).abs();
                max_diff = max_diff.max(d);
            }
        }
        max_diff
    }
}

#[test]
fn test_identity_roundtrip() {
    let mut filter = WgpuFrameFilter::new_identity().unwrap();
    if !init_filter(&mut filter) {
        return;
    }
    let (w, h) = (322, 182); // odd-ish sizes exercise stride/tail paths
    let expected = make_yuv420p_frame(w, h);
    let out = drive(&mut filter, vec![make_yuv420p_frame(w, h)], 1)
        .pop()
        .unwrap();
    unsafe {
        assert_eq!((*out.as_ptr()).width, w);
        assert_eq!((*out.as_ptr()).height, h);
        assert_eq!(
            (*out.as_ptr()).format,
            AVPixelFormat::AV_PIX_FMT_YUV420P as i32
        );
    }
    // YUV -> RGB -> YUV roundtrip through 8-bit RGBA allows small error.
    let diff = max_luma_diff(&out, &expected, w as usize, h as usize);
    assert!(diff <= 3, "luma max diff too large: {diff}");
}

#[test]
fn test_yuv444p_and_yuv422p_roundtrip() {
    for fmt in [
        AVPixelFormat::AV_PIX_FMT_YUV444P,
        AVPixelFormat::AV_PIX_FMT_YUV422P,
    ] {
        let mut filter = WgpuFrameFilter::new_identity().unwrap();
        if !init_filter(&mut filter) {
            return;
        }
        let (w, h) = (321, 181);
        let expected = make_planar_frame(w, h, fmt, None, 0);
        let out = drive(&mut filter, vec![make_planar_frame(w, h, fmt, None, 0)], 1)
            .pop()
            .unwrap();
        unsafe {
            assert_eq!((*out.as_ptr()).width, w);
            assert_eq!((*out.as_ptr()).height, h);
            // 4:2:2/4:4:4 inputs are chroma-downsampled to 4:2:0 on output.
            assert_eq!(
                (*out.as_ptr()).format,
                AVPixelFormat::AV_PIX_FMT_YUV420P as i32
            );
        }
        let diff = max_luma_diff(&out, &expected, w as usize, h as usize);
        assert!(diff <= 3, "{fmt:?} luma max diff too large: {diff}");
    }
}

#[test]
fn test_async_ordering_and_eof_flush() {
    let mut filter = WgpuFrameFilter::new_identity().unwrap(); // 2 in flight
    if !init_filter(&mut filter) {
        return;
    }
    let lumas = [40u8, 90, 140, 190];
    let mut inputs: Vec<Frame> = lumas
        .iter()
        .enumerate()
        .map(|(i, &l)| {
            make_planar_frame(64, 48, AVPixelFormat::AV_PIX_FMT_YUV420P, Some(l), i as i64)
        })
        .collect();
    // EOF marker arrives while GPU frames are still in flight; it must leave
    // last, after every real frame, in arrival order.
    inputs.push(make_marker_frame(100));

    let out = drive(&mut filter, inputs, 5);
    unsafe {
        for (i, frame) in out.iter().enumerate().take(4) {
            let p = frame.as_ptr();
            assert_eq!((*p).pts, i as i64, "output order broken at {i}");
            let luma = *(*p).data[0];
            let want = lumas[i] as i32;
            assert!(
                (luma as i32 - want).abs() <= 3,
                "frame {i}: luma {luma}, want ~{want}"
            );
        }
        let marker = out[4].as_ptr();
        assert_eq!((*marker).pts, 100);
        assert!((*marker).data[0].is_null(), "marker must stay props-only");
    }
}

/// Regression: the scheduler's pipeline loop (`run_pipeline`) stops after
/// its first request_frame sweep that yields nothing once the source has
/// disconnected, then `uninit` discards whatever is still pending. The EOF
/// marker is the last thing a source sends, so `filter_frame(marker)` must
/// resolve every in-flight readback before returning — unlike `drive`,
/// this driver grants the filter no grace polling at all.
#[test]
fn test_eof_marker_drains_in_flight_without_polling() {
    let mut filter = WgpuFrameFilter::new_identity().unwrap(); // 2 in flight
    if !init_filter(&mut filter) {
        return;
    }
    let mut map = HashMap::new();
    let mut ctx = make_ctx(&mut map);
    let mut out = Vec::new();
    for i in 0..3i64 {
        let frame = make_planar_frame(
            1280,
            720,
            AVPixelFormat::AV_PIX_FMT_YUV420P,
            Some(40 + 40 * i as u8),
            i,
        );
        if let Some(f) = filter.filter_frame(frame, &mut ctx).expect("filter_frame") {
            out.push(f);
        }
    }
    // The marker is the last send before the source drops its channel.
    if let Some(f) = filter
        .filter_frame(make_marker_frame(100), &mut ctx)
        .expect("marker filter_frame")
    {
        out.push(f);
    }
    // Exactly one non-blocking sweep, like run_pipeline's final pass.
    while let Some(f) = filter.request_frame(&mut ctx).expect("request_frame") {
        out.push(f);
    }
    assert_eq!(
        out.len(),
        4,
        "in-flight frames or the EOF marker were dropped at EOF"
    );
    unsafe {
        for (i, frame) in out.iter().enumerate().take(3) {
            assert_eq!((*frame.as_ptr()).pts, i as i64, "order broken at {i}");
        }
        let marker = out[3].as_ptr();
        assert_eq!((*marker).pts, 100);
        assert!((*marker).data[0].is_null(), "marker must stay props-only");
    }
}

#[test]
fn test_size_change_midstream() {
    let mut filter = WgpuFrameFilter::new_identity().unwrap();
    if !init_filter(&mut filter) {
        return;
    }
    let sizes = [(320, 180), (322, 182), (320, 180)];
    let inputs: Vec<Frame> = sizes
        .iter()
        .enumerate()
        .map(|(i, &(w, h))| {
            make_planar_frame(
                w,
                h,
                AVPixelFormat::AV_PIX_FMT_YUV420P,
                Some(60 + 60 * i as u8),
                i as i64,
            )
        })
        .collect();
    let out = drive(&mut filter, inputs, 3);
    unsafe {
        for (i, frame) in out.iter().enumerate() {
            let p = frame.as_ptr();
            assert_eq!((*p).pts, i as i64);
            assert_eq!(((*p).width, (*p).height), sizes[i], "size mismatch at {i}");
            let luma = *(*p).data[0];
            let want = 60 + 60 * i as i32;
            assert!(
                (luma as i32 - want).abs() <= 3,
                "frame {i}: luma {luma}, want ~{want}"
            );
        }
    }
}

#[test]
fn test_frames_in_flight_one_is_synchronous() {
    let mut filter = WgpuFrameFilter::builder()
        .shader_wgsl(shaders::IDENTITY_FS)
        .frames_in_flight(1)
        .build()
        .unwrap();
    if !init_filter(&mut filter) {
        return;
    }
    let mut map = HashMap::new();
    let mut ctx = make_ctx(&mut map);
    for i in 0..2 {
        let out = filter
            .filter_frame(make_yuv420p_frame(160, 90), &mut ctx)
            .expect("filter_frame");
        assert!(out.is_some(), "sync mode must return its own frame ({i})");
    }
}

#[test]
fn test_oversized_frame_rejected() {
    let mut filter = WgpuFrameFilter::new_identity().unwrap();
    if !init_filter(&mut filter) {
        return;
    }
    let mut map = HashMap::new();
    let mut ctx = make_ctx(&mut map);
    // The device is created with wgpu's default limits, which cap 2D
    // textures at 8192; exceeding that must be a clean Err, not a panic
    // from wgpu's uncaptured-error handler.
    let frame = make_planar_frame(8200, 16, AVPixelFormat::AV_PIX_FMT_YUV420P, Some(60), 0);
    let err = match filter.filter_frame(frame, &mut ctx) {
        Err(e) => e,
        Ok(_) => panic!("oversized frame must be rejected"),
    };
    assert!(
        err.to_string().contains("maximum texture dimension"),
        "unexpected error: {err}"
    );
}

#[test]
fn test_builder_rejects_bad_frames_in_flight() {
    for n in [0usize, 5] {
        let result = WgpuFrameFilter::builder()
            .shader_wgsl(shaders::IDENTITY_FS)
            .frames_in_flight(n)
            .build();
        assert!(result.is_err(), "frames_in_flight({n}) must be rejected");
    }
}

#[test]
fn test_output_resize() {
    let mut filter = WgpuFrameFilter::builder()
        .shader_wgsl(shaders::IDENTITY_FS)
        .output_size(160, 90)
        .build()
        .unwrap();
    if !init_filter(&mut filter) {
        return;
    }
    let out = drive(&mut filter, vec![make_yuv420p_frame(320, 180)], 1)
        .pop()
        .unwrap();
    unsafe {
        assert_eq!((*out.as_ptr()).width, 160);
        assert_eq!((*out.as_ptr()).height, 90);
    }
}

#[test]
fn test_rejects_unsupported_and_hw_formats() {
    let mut filter = WgpuFrameFilter::new_identity().unwrap();
    if !init_filter(&mut filter) {
        return;
    }
    let mut map = HashMap::new();
    let mut ctx = make_ctx(&mut map);

    unsafe {
        let mut rgb = Frame::empty();
        let p = rgb.as_mut_ptr();
        (*p).width = 64;
        (*p).height = 64;
        (*p).format = AVPixelFormat::AV_PIX_FMT_RGB24 as i32;
        assert!(av_frame_get_buffer(p, 1) >= 0);
        let err = match filter.filter_frame(rgb, &mut ctx) {
            Err(e) => e,
            Ok(_) => panic!("RGB24 input must be rejected"),
        };
        assert!(
            err.to_string().contains("format=yuv420p"),
            "unexpected error: {err}"
        );

        // Allocate as YUV420P (so buf[0] is non-null) and then relabel as a
        // hardware format: hardware frames now take the download path, and
        // this fake frame has no hw_frames_ctx, so the download must fail
        // cleanly without anything dereferencing the misdescribed planes.
        let mut hw = make_yuv420p_frame(64, 64);
        (*hw.as_mut_ptr()).format = AVPixelFormat::AV_PIX_FMT_CUDA as i32;
        let err = match filter.filter_frame(hw, &mut ctx) {
            Err(e) => e,
            Ok(_) => panic!("a fake hardware frame must fail the download"),
        };
        assert!(
            err.to_string().contains("download hardware frame"),
            "unexpected error: {err}"
        );
    }
}

#[test]
fn test_params_handle_size_check() {
    let filter = WgpuFrameFilter::builder()
        .shader_wgsl(shaders::IDENTITY_FS)
        .params(1.0f32)
        .build()
        .unwrap();
    assert!(filter.params_handle::<f32>().is_ok());
    assert!(filter.params_handle::<[f32; 4]>().is_err());
}

#[test]
fn test_params_shader_size_mismatch_fails_at_init() {
    // Shader declares a 32-byte params struct, builder provides 4 bytes
    // (buffer rounded to 16): must be a clean Err at init, not a panic
    // on first submit.
    let shader = r#"
        @group(0) @binding(0) var texture1: texture_2d<f32>;
        @group(0) @binding(1) var sampler1: sampler;
        struct EzUniforms { play_time: f32, width: f32, height: f32, _pad: f32 };
        @group(0) @binding(2) var<uniform> ez: EzUniforms;
        struct BigParams { a: vec4<f32>, b: vec4<f32> };
        @group(1) @binding(0) var<uniform> params: BigParams;
        @fragment
        fn fs_main(@location(0) tex_coord: vec2<f32>) -> @location(0) vec4<f32> {
            return textureSample(texture1, sampler1, tex_coord) + params.a * 0.0;
        }
    "#;
    let mut filter = WgpuFrameFilter::builder()
        .shader_wgsl(shader)
        .params(1.0f32)
        .build()
        .unwrap();
    let mut map = HashMap::new();
    let mut ctx = make_ctx(&mut map);
    match filter.init(&mut ctx) {
        Err(e) if e.to_string().contains("adapter") || e.to_string().contains("device") => {
            eprintln!("skipping wgpu test (no GPU): {e}");
        }
        Err(e) => assert!(
            e.to_string().contains("pipeline") || e.to_string().contains("binding"),
            "expected binding-contract diagnostics, got: {e}"
        ),
        Ok(()) => panic!("init must fail when shader params exceed provided buffer"),
    }
}

#[test]
fn test_bad_shader_fails_at_init_with_diagnostics() {
    let mut filter = WgpuFrameFilter::new_simple("not valid wgsl at all").unwrap();
    let mut map = HashMap::new();
    let mut ctx = make_ctx(&mut map);
    match filter.init(&mut ctx) {
        Err(e) if e.to_string().contains("adapter") || e.to_string().contains("device") => {
            eprintln!("skipping wgpu test (no GPU): {e}");
        }
        Err(e) => assert!(
            e.to_string().contains("shader")
                || e.to_string().contains("Shader")
                || e.to_string().contains("parse"),
            "expected shader diagnostics, got: {e}"
        ),
        Ok(()) => panic!("init must fail for invalid WGSL"),
    }
}

/// Pass-through filter counting every frame it sees; stands in for a
/// downstream filter that must receive drained frames via `run_filters_from`.
struct CountingFilter {
    seen: std::sync::Arc<std::sync::atomic::AtomicUsize>,
}

impl FrameFilter for CountingFilter {
    fn media_type(&self) -> ffmpeg_sys_next::AVMediaType {
        ffmpeg_sys_next::AVMediaType::AVMEDIA_TYPE_VIDEO
    }

    fn filter_frame(
        &mut self,
        frame: Frame,
        _ctx: &mut FrameFilterContext,
    ) -> Result<Option<Frame>, FrameFilterError> {
        self.seen.fetch_add(1, std::sync::atomic::Ordering::Relaxed);
        Ok(Some(frame))
    }
}

/// Emulates `run_pipeline` (`src/core/scheduler/frame_filter_pipeline.rs`)
/// against a real `FramePipeline`: `run_filters` per input, then per-filter
/// `request_frame` polling that feeds `run_filters_from(i + 1, ..)`, and
/// continued polling after the source is exhausted. A downstream filter must
/// see every drained frame exactly once, in arrival order, marker last.
#[test]
fn test_frame_pipeline_driver_semantics() {
    use crate::filter::frame_pipeline::FramePipeline;
    use ffmpeg_sys_next::AVMediaType::AVMEDIA_TYPE_VIDEO;

    fn poll_all(pipeline: &mut FramePipeline, out: &mut Vec<Frame>) {
        for i in 0..pipeline.filter_len() {
            loop {
                match pipeline.request_frame(i).expect("request_frame") {
                    Some(f) => {
                        if let Some(f) = pipeline
                            .run_filters_from(i + 1, f)
                            .expect("run_filters_from")
                        {
                            out.push(f);
                        }
                    }
                    None => break,
                }
            }
        }
    }

    let filter = WgpuFrameFilter::builder()
        .shader_wgsl(shaders::IDENTITY_FS)
        .build()
        .expect("builder");
    let seen = std::sync::Arc::new(std::sync::atomic::AtomicUsize::new(0));
    let mut pipeline = FramePipeline::new(AVMEDIA_TYPE_VIDEO, None);
    pipeline.add_filter("wgpu", Box::new(filter));
    pipeline.add_filter(
        "count",
        Box::new(CountingFilter {
            seen: std::sync::Arc::clone(&seen),
        }),
    );
    match pipeline.init_filters() {
        Ok(()) => {}
        Err(e) if e.to_string().contains("adapter") || e.to_string().contains("device") => {
            eprintln!("skipping wgpu test (no GPU): {e}");
            return;
        }
        Err(e) => panic!("init failed: {e}"),
    }

    let n_real = 4usize;
    let mut inputs: Vec<Frame> = (0..n_real)
        .map(|i| {
            make_planar_frame(
                64,
                48,
                AVPixelFormat::AV_PIX_FMT_YUV420P,
                Some((40 + 40 * i) as u8),
                i as i64,
            )
        })
        .collect();
    inputs.push(make_marker_frame(n_real as i64));
    let expected = n_real + 1;

    let mut out = Vec::new();
    for frame in inputs {
        if let Some(f) = pipeline.run_filters(frame).expect("run_filters") {
            out.push(f);
        }
        poll_all(&mut pipeline, &mut out);
    }
    for _ in 0..2000 {
        poll_all(&mut pipeline, &mut out);
        if out.len() >= expected {
            break;
        }
        std::thread::sleep(std::time::Duration::from_millis(1));
    }
    pipeline.uninit_filters();

    assert_eq!(
        out.len(),
        expected,
        "all frames must drain through the chain"
    );
    for (i, f) in out.iter().enumerate() {
        assert_eq!(unsafe { (*f.as_ptr()).pts }, i as i64, "arrival order");
    }
    // SAFETY: reading a pointer field of a live frame.
    unsafe {
        assert!(
            (*out.last().unwrap().as_ptr()).buf[0].is_null(),
            "marker must exit last and stay props-only"
        );
    }
    assert_eq!(
        seen.load(std::sync::atomic::Ordering::Relaxed),
        expected,
        "downstream filter must see every drained frame"
    );
}

/// The non-empty half of the pending-hint contract, pinned WITHOUT a GPU:
/// a queued ready pass-through entry (via the test hook, standing in for
/// the device-only pending states) must flip the hint to `true`, and
/// draining it through the ordinary `request_frame` path must return it to
/// `false`. Deterministic everywhere — including machines whose device
/// completes submissions inside `filter_frame`, where the GPU test below
/// can never observe the in-flight state and an always-`false` override
/// would otherwise slip through.
#[test]
fn test_pending_hint_tracks_a_queued_entry_without_gpu() {
    let mut filter = WgpuFrameFilter::builder()
        .shader_wgsl(shaders::IDENTITY_FS)
        .build()
        .expect("builder");
    assert!(
        !filter.request_frame_pending(),
        "an empty output queue must not report pending output"
    );

    filter.queue_ready_output_for_test(make_yuv420p_frame(64, 48));
    assert!(
        filter.request_frame_pending(),
        "a queued entry must report pending output"
    );

    let mut map = HashMap::new();
    let mut ctx = make_ctx(&mut map);
    let out = filter
        .request_frame(&mut ctx)
        .expect("request_frame on a ready entry");
    assert!(out.is_some(), "the queued entry must drain");
    assert!(
        !filter.request_frame_pending(),
        "a drained queue must not report pending output"
    );
}

/// `request_frame_pending` must track the output queue — the contract the
/// pipeline's wait-interval gating relies on: `false` with nothing queued
/// (letting an idle pipeline park between inputs), `true` while a submission
/// is in flight, and `false` again once everything drained. The in-flight
/// assertion is opportunistic (a device may complete inside `filter_frame`);
/// the deterministic non-empty pin lives in the no-GPU test above.
#[test]
fn test_request_frame_pending_tracks_the_output_queue() {
    let mut filter = WgpuFrameFilter::builder()
        .shader_wgsl(shaders::IDENTITY_FS)
        .build()
        .expect("builder");
    // Before any input — and before the GPU is touched at all — nothing can
    // be pending, and the override must say so; the trait default would
    // claim `true` here.
    assert!(
        !filter.request_frame_pending(),
        "an empty output queue must not report pending output"
    );

    if !init_filter(&mut filter) {
        return;
    }
    let mut map = HashMap::new();
    let mut ctx = make_ctx(&mut map);

    let mut delivered = 0usize;
    if let Some(_out) = filter
        .filter_frame(make_yuv420p_frame(64, 48), &mut ctx)
        .expect("filter_frame")
    {
        delivered += 1;
    }
    // Default frames_in_flight = 2: the submission normally outlives its own
    // filter_frame call — unless the non-blocking probe already resolved and
    // delivered it, in which case the queue is rightfully empty again. Either
    // way the verdict must match the queue.
    if delivered == 0 {
        assert!(
            filter.request_frame_pending(),
            "an in-flight submission must report pending output"
        );
    }

    // Drain to empty; the hint must drop back to idle.
    for _ in 0..2000 {
        while let Some(_out) = filter.request_frame(&mut ctx).expect("request_frame") {
            delivered += 1;
        }
        if delivered >= 1 {
            break;
        }
        std::thread::sleep(std::time::Duration::from_millis(1));
    }
    assert_eq!(delivered, 1, "the submitted frame must drain");
    assert!(
        !filter.request_frame_pending(),
        "a drained queue must not report pending output"
    );
    filter.uninit(&mut ctx);
}

/// A source that ends without any EOF marker (e.g. an upstream abort closes
/// the channel) must not lose real frames: everything already submitted
/// drains through `request_frame` polling alone.
#[test]
fn test_drain_without_marker() {
    let mut filter = WgpuFrameFilter::builder()
        .shader_wgsl(shaders::IDENTITY_FS)
        .build()
        .expect("builder");
    if !init_filter(&mut filter) {
        return;
    }
    let inputs: Vec<Frame> = (0..4)
        .map(|i| {
            make_planar_frame(
                64,
                48,
                AVPixelFormat::AV_PIX_FMT_YUV420P,
                Some((40 + 40 * i) as u8),
                i as i64,
            )
        })
        .collect();
    let out = drive(&mut filter, inputs, 4);
    for (i, f) in out.iter().enumerate() {
        assert_eq!(unsafe { (*f.as_ptr()).pts }, i as i64, "arrival order");
    }
    let mut map = HashMap::new();
    let mut ctx = make_ctx(&mut map);
    filter.uninit(&mut ctx);
}

/// Zero-copy readback: output planes point into the mapped staging buffer.
/// Verifies pixel correctness, that dropping frames recycles buffers back to
/// the filter (drained on the next filter_frame), and ordering across many
/// frames with only `frames_in_flight` submissions outstanding.
#[test]
fn test_zero_copy_readback_roundtrip_and_recycle() {
    let mut filter = WgpuFrameFilter::builder()
        .shader_wgsl(shaders::IDENTITY_FS)
        .zero_copy_readback(true)
        .build()
        .expect("builder");
    if !init_filter(&mut filter) {
        return;
    }

    let (w, h) = (322, 182); // odd-ish sizes exercise stride/tail paths
    let expected = make_yuv420p_frame(w, h);

    let n = 12usize;
    let mut map = HashMap::new();
    let mut ctx = make_ctx(&mut map);
    let mut seen = 0usize;
    for i in 0..n {
        let mut frame = make_yuv420p_frame(w, h);
        unsafe { (*frame.as_mut_ptr()).pts = i as i64 };
        // Frames are dropped right after checking, exercising the recycle
        // path: later iterations must reuse returned buffers instead of
        // growing GPU memory without bound.
        if let Some(out) = filter.filter_frame(frame, &mut ctx).expect("filter_frame") {
            assert_eq!(unsafe { (*out.as_ptr()).pts }, seen as i64, "order");
            let diff = max_luma_diff(&out, &expected, w as usize, h as usize);
            assert!(diff <= 3, "luma diff too large at frame {seen}: {diff}");
            seen += 1;
        }
        while let Some(out) = filter.request_frame(&mut ctx).expect("request_frame") {
            assert_eq!(unsafe { (*out.as_ptr()).pts }, seen as i64, "order");
            seen += 1;
        }
    }
    for _ in 0..2000 {
        while let Some(out) = filter.request_frame(&mut ctx).expect("request_frame") {
            assert_eq!(unsafe { (*out.as_ptr()).pts }, seen as i64, "order");
            seen += 1;
        }
        if seen >= n {
            break;
        }
        std::thread::sleep(std::time::Duration::from_millis(1));
    }
    assert_eq!(seen, n, "all zero-copy frames must drain");
    filter.uninit(&mut ctx);
}

/// Zero-copy frames must survive their filter: uninit while a frame is still
/// alive downstream, then read the frame afterwards. The AVBuffer free
/// callback silently drops the buffer when the recycle channel is gone.
#[test]
fn test_zero_copy_frame_outlives_filter() {
    let mut filter = WgpuFrameFilter::builder()
        .shader_wgsl(shaders::IDENTITY_FS)
        .zero_copy_readback(true)
        .frames_in_flight(1)
        .build()
        .expect("builder");
    if !init_filter(&mut filter) {
        return;
    }
    let (w, h) = (64, 48);
    let expected = make_yuv420p_frame(w, h);
    let out = drive(&mut filter, vec![make_yuv420p_frame(w, h)], 1)
        .pop()
        .unwrap();

    let mut map = HashMap::new();
    let mut ctx = make_ctx(&mut map);
    filter.uninit(&mut ctx);
    drop(filter);

    // The staging buffer behind `out` must still be mapped and readable.
    let diff = max_luma_diff(&out, &expected, w as usize, h as usize);
    assert!(diff <= 3, "luma diff too large after filter drop: {diff}");
}

// --- OutputFramePool: GPU-independent unit tests -------------------------
//
// These exercise the AVBufferPool-backed copy-path output frame directly,
// with synthetic "mapped readback" bytes, so they run on any machine (no GPU
// required) and cover exactly the FFmpeg-refcount / uninit / writability
// semantics that the software-Vulkan integration tests cannot make explicit.

use crate::wgpu_filter::frame_io::OutputFramePool;
use crate::wgpu_filter::gpu_state::OutputGeometry;

fn align4(v: u32) -> usize {
    v.div_ceil(4) as usize * 4
}

/// Geometry + a filled synthetic mapped buffer for the given output size,
/// using the same packed strides `ensure_resources` computes.
fn pool_geo_and_mapped(out_w: u32, out_h: u32) -> (OutputGeometry, Vec<u8>) {
    let y_stride = align4(out_w);
    let c_stride = align4(out_w.div_ceil(2));
    let out_ch = out_h.div_ceil(2) as usize;
    let y_end = y_stride * out_h as usize;
    let u_end = y_end + c_stride * out_ch;
    let v_end = u_end + c_stride * out_ch;

    let mut mapped = vec![0u8; v_end];
    let cw = out_w.div_ceil(2) as usize;
    for row in 0..out_h as usize {
        for col in 0..out_w as usize {
            mapped[row * y_stride + col] = ((row * 7 + col * 3) % 251) as u8;
        }
    }
    for row in 0..out_ch {
        for col in 0..cw {
            mapped[u_end - (c_stride * out_ch) + row * c_stride + col] = (100 + (row % 40)) as u8;
            mapped[v_end - (c_stride * out_ch) + row * c_stride + col] = (200 + (row % 40)) as u8;
        }
    }
    let geo = OutputGeometry {
        out_w,
        out_h,
        y_stride,
        c_stride,
        buf_size: v_end as u64,
    };
    (geo, mapped)
}

/// A property donor carrying pts + time_base, like the input frame the filter
/// copies props from.
fn make_props_donor(pts: i64) -> Frame {
    unsafe {
        let mut f = Frame::empty();
        let p = f.as_mut_ptr();
        (*p).pts = pts;
        (*p).time_base = ffmpeg_sys_next::AVRational { num: 1, den: 25 };
        f
    }
}

#[test]
fn test_output_frame_pool_copies_planes_and_props() {
    let (out_w, out_h) = (10u32, 6u32); // odd chroma width exercises stride tail
    let (geo, mapped) = pool_geo_and_mapped(out_w, out_h);
    let pool = OutputFramePool::new(geo).expect("pool init");
    let src = make_props_donor(77);
    let frame = pool.build_frame(&mapped, &src).expect("build_frame");

    unsafe {
        let p = frame.as_ptr();
        assert_eq!((*p).width, out_w as i32);
        assert_eq!((*p).height, out_h as i32);
        assert_eq!((*p).format, AVPixelFormat::AV_PIX_FMT_YUV420P as i32);
        assert_eq!((*p).pts, 77, "pts must propagate");
        assert_eq!((*p).time_base.num, 1);
        assert_eq!((*p).time_base.den, 25, "time_base must propagate");

        let out_ch = out_h.div_ceil(2) as usize;
        let cw = out_w.div_ceil(2) as usize;
        let dls = (*p).linesize[0] as usize;
        let dy = std::slice::from_raw_parts((*p).data[0], dls * out_h as usize);
        for row in 0..out_h as usize {
            for col in 0..out_w as usize {
                assert_eq!(
                    dy[row * dls + col],
                    ((row * 7 + col * 3) % 251) as u8,
                    "Y[{row}][{col}]"
                );
            }
        }
        let uls = (*p).linesize[1] as usize;
        let du = std::slice::from_raw_parts((*p).data[1], uls * out_ch);
        let vls = (*p).linesize[2] as usize;
        let dv = std::slice::from_raw_parts((*p).data[2], vls * out_ch);
        for row in 0..out_ch {
            for col in 0..cw {
                assert_eq!(
                    du[row * uls + col],
                    (100 + (row % 40)) as u8,
                    "U[{row}][{col}]"
                );
                assert_eq!(
                    dv[row * vls + col],
                    (200 + (row % 40)) as u8,
                    "V[{row}][{col}]"
                );
            }
        }
    }
}

#[test]
fn test_output_frame_pool_refcount_prevents_reuse() {
    let (geo, mapped) = pool_geo_and_mapped(16, 12);
    let pool = OutputFramePool::new(geo).expect("pool init");
    let src = make_props_donor(0);

    let f1 = pool.build_frame(&mapped, &src).expect("frame 1");
    let f1_ptr = unsafe { (*f1.as_ptr()).data[0] };

    // Keep f1's buffer referenced through a clone, then drop f1.
    let mut clone = unsafe { Frame::empty() };
    unsafe { assert!(av_frame_ref(clone.as_mut_ptr(), f1.as_ptr()) >= 0) };
    let clone_ptr = unsafe { (*clone.as_ptr()).data[0] };
    assert_eq!(clone_ptr, f1_ptr, "clone shares f1's pooled buffer");
    drop(f1); // buffer stays alive: clone still holds a reference

    // A new frame must NOT reuse the still-referenced buffer.
    let f2 = pool.build_frame(&mapped, &src).expect("frame 2");
    let f2_ptr = unsafe { (*f2.as_ptr()).data[0] };
    assert_ne!(
        f2_ptr, clone_ptr,
        "pool handed out a buffer still referenced downstream"
    );
}

#[test]
fn test_output_frame_pool_uninit_keeps_live_frame_valid() {
    let (geo, mapped) = pool_geo_and_mapped(16, 12);
    let pool = OutputFramePool::new(geo).expect("pool init");
    let src = make_props_donor(0);
    let frame = pool.build_frame(&mapped, &src).expect("build_frame");

    // Drop the pool (av_buffer_pool_uninit) while a built frame is still live;
    // its AVBufferRef must keep the buffer valid and readable.
    drop(pool);
    unsafe {
        let p = frame.as_ptr();
        let dls = (*p).linesize[0] as usize;
        let dy = std::slice::from_raw_parts((*p).data[0], dls * 12);
        // Row 0 of the synthetic Y pattern is `(0 * 7 + col * 3) % 251`.
        for (col, &got) in dy.iter().take(16).enumerate() {
            assert_eq!(got, ((col * 3) % 251) as u8, "row 0 col {col}");
        }
    }
}

#[test]
fn test_output_frame_pool_frame_writability() {
    let (geo, mapped) = pool_geo_and_mapped(16, 12);
    let pool = OutputFramePool::new(geo).expect("pool init");
    let src = make_props_donor(0);
    let mut frame = pool.build_frame(&mapped, &src).expect("build_frame");

    unsafe {
        assert_eq!(
            av_frame_is_writable(frame.as_mut_ptr()),
            1,
            "a singly-owned pooled frame must be writable"
        );
        let mut clone = Frame::empty();
        assert!(av_frame_ref(clone.as_mut_ptr(), frame.as_ptr()) >= 0);
        assert_eq!(
            av_frame_is_writable(frame.as_mut_ptr()),
            0,
            "a shared pooled frame must be non-writable"
        );

        let before = (*frame.as_ptr()).data[0];
        assert!(av_frame_make_writable(frame.as_mut_ptr()) >= 0);
        assert_eq!(
            av_frame_is_writable(frame.as_mut_ptr()),
            1,
            "make_writable must restore writability"
        );
        let after = (*frame.as_ptr()).data[0];
        assert_ne!(before, after, "make_writable must copy to a fresh buffer");
    }
}

// --- YUV passthrough mode (shader_yuv_wgsl) ---

/// Builds a YUV frame with per-pixel luma and per-sample chroma closures.
/// Supports the planar formats plus NV12 (interleaved UV plane).
pub(super) fn make_yuv_frame_with(
    w: i32,
    h: i32,
    fmt: AVPixelFormat,
    luma: impl Fn(usize, usize) -> u8,
    chroma: impl Fn(usize, usize) -> (u8, u8),
) -> Frame {
    let (sub_x, sub_y, nv12) = match fmt {
        AVPixelFormat::AV_PIX_FMT_YUV420P | AVPixelFormat::AV_PIX_FMT_YUVJ420P => (2, 2, false),
        AVPixelFormat::AV_PIX_FMT_YUV444P => (1, 1, false),
        AVPixelFormat::AV_PIX_FMT_NV12 => (2, 2, true),
        other => panic!("unsupported test format {other:?}"),
    };
    unsafe {
        let mut frame = Frame::empty();
        let p = frame.as_mut_ptr();
        (*p).width = w;
        (*p).height = h;
        (*p).format = fmt as i32;
        assert!(av_frame_get_buffer(p, 1) >= 0);
        (*p).pts = 0;
        (*p).time_base = ffmpeg_sys_next::AVRational { num: 1, den: 30 };

        let ls_y = (*p).linesize[0] as usize;
        let y = std::slice::from_raw_parts_mut((*p).data[0], ls_y * h as usize);
        for row in 0..h as usize {
            for col in 0..w as usize {
                y[row * ls_y + col] = luma(col, row);
            }
        }
        let cw = (w as usize).div_ceil(sub_x);
        let ch = (h as usize).div_ceil(sub_y);
        if nv12 {
            let ls = (*p).linesize[1] as usize;
            let uv = std::slice::from_raw_parts_mut((*p).data[1], ls * ch);
            for row in 0..ch {
                for col in 0..cw {
                    let (u, v) = chroma(col, row);
                    uv[row * ls + col * 2] = u;
                    uv[row * ls + col * 2 + 1] = v;
                }
            }
        } else {
            for plane in 1..=2 {
                let ls = (*p).linesize[plane] as usize;
                let data = std::slice::from_raw_parts_mut((*p).data[plane], ls * ch);
                for row in 0..ch {
                    for col in 0..cw {
                        let (u, v) = chroma(col, row);
                        data[row * ls + col] = if plane == 1 { u } else { v };
                    }
                }
            }
        }
        frame
    }
}

/// Copies one plane of a frame into a tight `Vec` (stride removed).
pub(super) fn plane_to_vec(frame: &Frame, index: usize, w: usize, h: usize) -> Vec<u8> {
    unsafe {
        let p = frame.as_ptr();
        let ls = (*p).linesize[index] as usize;
        let data = std::slice::from_raw_parts((*p).data[index], ls * h);
        let mut out = Vec::with_capacity(w * h);
        for row in 0..h {
            out.extend_from_slice(&data[row * ls..row * ls + w]);
        }
        out
    }
}

fn yuv_identity_filter() -> WgpuFrameFilter {
    WgpuFrameFilter::builder()
        .shader_yuv_wgsl(shaders::YUV_IDENTITY_BODY)
        .build()
        .unwrap()
}

/// A luma pattern that includes super-black (8) and super-white (250) code
/// values, which the RGBA roundtrip would clip/stretch but the passthrough
/// must preserve untouched.
fn wide_luma(col: usize, row: usize) -> u8 {
    match (col + row) % 5 {
        0 => 8,
        1 => 250,
        _ => ((col * 7 + row * 13) % 256) as u8,
    }
}

fn output_color_range(frame: &Frame) -> ffmpeg_sys_next::AVColorRange {
    unsafe { (*frame.as_ptr()).color_range }
}

/// The pack shader's quantizer: `u32(clamp(v, 0, 1) * 255 + 0.5)`.
fn quantize(v: f32) -> u8 {
    (v.clamp(0.0, 1.0) * 255.0 + 0.5) as u8
}

#[test]
fn yuv_identity_444_luma_exact_chroma_is_quad_average() {
    let mut filter = yuv_identity_filter();
    if !init_filter(&mut filter) {
        return;
    }
    let (w, h) = (321usize, 181usize);
    let luma = wide_luma;
    let chroma = |c: usize, r: usize| (((c * 3 + r) % 256) as u8, ((c + r * 5) % 256) as u8);
    let input = make_yuv_frame_with(
        w as i32,
        h as i32,
        AVPixelFormat::AV_PIX_FMT_YUV444P,
        luma,
        chroma,
    );
    let out = drive(&mut filter, vec![input], 1).pop().unwrap();

    // 4:4:4 chroma texels sit exactly at pixel centers, so the effect pass
    // reproduces every plane bit-for-bit; the only transform left is the
    // pack's fixed 2x2 box average down to 4:2:0, reproduced here in the
    // same f32 arithmetic (exact: sums of k/255 fit the f32 mantissa).
    let out_y = plane_to_vec(&out, 0, w, h);
    for row in 0..h {
        for col in 0..w {
            assert_eq!(out_y[row * w + col], luma(col, row), "Y at ({col},{row})");
        }
    }
    let (cw, ch) = (w.div_ceil(2), h.div_ceil(2));
    let out_u = plane_to_vec(&out, 1, cw, ch);
    let out_v = plane_to_vec(&out, 2, cw, ch);
    for by in 0..ch {
        for bx in 0..cw {
            let mut sum_u = 0.0f32;
            let mut sum_v = 0.0f32;
            for (dx, dy) in [(0, 0), (1, 0), (0, 1), (1, 1)] {
                // load_px clamps the out-of-range column/row at odd edges.
                let sx = (bx * 2 + dx).min(w - 1);
                let sy = (by * 2 + dy).min(h - 1);
                let (u, v) = chroma(sx, sy);
                sum_u += u as f32 / 255.0;
                sum_v += v as f32 / 255.0;
            }
            assert_eq!(
                out_u[by * cw + bx],
                quantize(sum_u * 0.25),
                "U at ({bx},{by})"
            );
            assert_eq!(
                out_v[by * cw + bx],
                quantize(sum_v * 0.25),
                "V at ({bx},{by})"
            );
        }
    }
}

#[test]
fn yuv_identity_420_is_bit_exact_with_constant_chroma() {
    for (w, h) in [(1usize, 1usize), (3, 3), (322, 182)] {
        let mut filter = yuv_identity_filter();
        if !init_filter(&mut filter) {
            return;
        }
        let input = make_yuv_frame_with(
            w as i32,
            h as i32,
            AVPixelFormat::AV_PIX_FMT_YUV420P,
            wide_luma,
            |_, _| (77, 200),
        );
        let out = drive(&mut filter, vec![input], 1).pop().unwrap();
        assert_eq!(
            output_color_range(&out),
            ffmpeg_sys_next::AVColorRange::AVCOL_RANGE_MPEG,
            "{w}x{h}: limited input must tag the output limited"
        );
        let out_y = plane_to_vec(&out, 0, w, h);
        for row in 0..h {
            for col in 0..w {
                assert_eq!(
                    out_y[row * w + col],
                    wide_luma(col, row),
                    "{w}x{h}: Y at ({col},{row})"
                );
            }
        }
        // Bilinear upsampling and box downsampling of a constant plane are
        // both the identity, so constant chroma must survive bit-for-bit.
        let (cw, ch) = (w.div_ceil(2), h.div_ceil(2));
        assert!(plane_to_vec(&out, 1, cw, ch).iter().all(|&u| u == 77));
        assert!(plane_to_vec(&out, 2, cw, ch).iter().all(|&v| v == 200));
    }
}

#[test]
fn yuv_identity_nv12_is_bit_exact_with_constant_chroma() {
    let mut filter = yuv_identity_filter();
    if !init_filter(&mut filter) {
        return;
    }
    let (w, h) = (322usize, 182usize);
    let input = make_yuv_frame_with(
        w as i32,
        h as i32,
        AVPixelFormat::AV_PIX_FMT_NV12,
        wide_luma,
        |_, _| (33, 240),
    );
    let out = drive(&mut filter, vec![input], 1).pop().unwrap();
    unsafe {
        assert_eq!(
            (*out.as_ptr()).format,
            AVPixelFormat::AV_PIX_FMT_YUV420P as i32,
            "output stays planar YUV420P"
        );
    }
    let out_y = plane_to_vec(&out, 0, w, h);
    for row in 0..h {
        for col in 0..w {
            assert_eq!(out_y[row * w + col], wide_luma(col, row));
        }
    }
    let (cw, ch) = (w.div_ceil(2), h.div_ceil(2));
    assert!(plane_to_vec(&out, 1, cw, ch).iter().all(|&u| u == 33));
    assert!(plane_to_vec(&out, 2, cw, ch).iter().all(|&v| v == 240));
}

#[test]
fn yuvj420p_identity_preserves_code_values_and_tags_full_range() {
    let mut filter = yuv_identity_filter();
    if !init_filter(&mut filter) {
        return;
    }
    let (w, h) = (64usize, 48usize);
    let input = make_yuv_frame_with(
        w as i32,
        h as i32,
        AVPixelFormat::AV_PIX_FMT_YUVJ420P,
        wide_luma,
        |_, _| (128, 128),
    );
    let out = drive(&mut filter, vec![input], 1).pop().unwrap();
    // The output is plain YUV420P, which does not imply full range the way
    // the J pixel format does — the explicit tag must carry it.
    assert_eq!(
        output_color_range(&out),
        ffmpeg_sys_next::AVColorRange::AVCOL_RANGE_JPEG
    );
    let out_y = plane_to_vec(&out, 0, w, h);
    for row in 0..h {
        for col in 0..w {
            assert_eq!(out_y[row * w + col], wide_luma(col, row));
        }
    }
}

#[test]
fn yuv_full_range_flag_reaches_the_shader() {
    // The body paints Y from ez_full_range(): full-range input turns the
    // luma plane 255, limited input turns it 0.
    let body = r#"
        fn ez_effect(coord: vec2<f32>) -> vec3<f32> {
            if (ez_full_range()) {
                return vec3<f32>(1.0, 0.5, 0.5);
            }
            return vec3<f32>(0.0, 0.5, 0.5);
        }
    "#;
    for (fmt, expected_y) in [
        (AVPixelFormat::AV_PIX_FMT_YUVJ420P, 255u8),
        (AVPixelFormat::AV_PIX_FMT_YUV420P, 0u8),
    ] {
        let mut filter = WgpuFrameFilter::builder()
            .shader_yuv_wgsl(body)
            .build()
            .unwrap();
        if !init_filter(&mut filter) {
            return;
        }
        let input = make_yuv_frame_with(32, 32, fmt, |_, _| 128, |_, _| (128, 128));
        let out = drive(&mut filter, vec![input], 1).pop().unwrap();
        let out_y = plane_to_vec(&out, 0, 32, 32);
        assert!(
            out_y.iter().all(|&y| y == expected_y),
            "{fmt:?}: expected uniform Y={expected_y}"
        );
    }
}

#[test]
fn yuv_resize_keeps_constant_planes_exact() {
    let mut filter = WgpuFrameFilter::builder()
        .shader_yuv_wgsl(shaders::YUV_IDENTITY_BODY)
        .output_size(32, 24)
        .build()
        .unwrap();
    if !init_filter(&mut filter) {
        return;
    }
    let input = make_yuv_frame_with(
        64,
        48,
        AVPixelFormat::AV_PIX_FMT_YUV420P,
        |_, _| 99,
        |_, _| (44, 211),
    );
    let out = drive(&mut filter, vec![input], 1).pop().unwrap();
    unsafe {
        assert_eq!((*out.as_ptr()).width, 32);
        assert_eq!((*out.as_ptr()).height, 24);
    }
    assert!(plane_to_vec(&out, 0, 32, 24).iter().all(|&y| y == 99));
    assert!(plane_to_vec(&out, 1, 16, 12).iter().all(|&u| u == 44));
    assert!(plane_to_vec(&out, 2, 16, 12).iter().all(|&v| v == 211));
}

#[test]
fn yuv_420_gradient_chroma_matches_bilinear_box_reference() {
    for (w, h) in [(64usize, 64usize), (63, 49)] {
        yuv_gradient_chroma_case(w, h);
    }
}

fn yuv_gradient_chroma_case(w: usize, h: usize) {
    let mut filter = yuv_identity_filter();
    if !init_filter(&mut filter) {
        return;
    }
    let chroma = |c: usize, r: usize| ((c * 8 % 256) as u8, ((c * 3 + r * 5) % 256) as u8);
    let input = make_yuv_frame_with(
        w as i32,
        h as i32,
        AVPixelFormat::AV_PIX_FMT_YUV420P,
        |_, _| 128,
        chroma,
    );
    let (cw, ch) = (w.div_ceil(2), h.div_ceil(2));
    let mut in_u = vec![0u8; cw * ch];
    let mut in_v = vec![0u8; cw * ch];
    for r in 0..ch {
        for c in 0..cw {
            let (u, v) = chroma(c, r);
            in_u[r * cw + c] = u;
            in_v[r * cw + c] = v;
        }
    }
    let out = drive(&mut filter, vec![input], 1).pop().unwrap();

    // CPU reference for the documented resample: the effect pass bilinearly
    // samples the half-size chroma plane at every full-res pixel center
    // (ClampToEdge), and the pack averages each 2x2 block. GPU bilinear
    // weights may be fixed-point, hence the small tolerance.
    let bilinear = |plane: &[u8], px: usize, py: usize| -> f32 {
        let u = (px as f32 + 0.5) / w as f32 * cw as f32 - 0.5;
        let v = (py as f32 + 0.5) / h as f32 * ch as f32 - 0.5;
        let (x0, y0) = (u.floor(), v.floor());
        let (fx, fy) = (u - x0, v - y0);
        let sample = |xi: i64, yi: i64| -> f32 {
            let x = xi.clamp(0, cw as i64 - 1) as usize;
            let y = yi.clamp(0, ch as i64 - 1) as usize;
            plane[y * cw + x] as f32 / 255.0
        };
        let (x0, y0) = (x0 as i64, y0 as i64);
        let top = sample(x0, y0) * (1.0 - fx) + sample(x0 + 1, y0) * fx;
        let bot = sample(x0, y0 + 1) * (1.0 - fx) + sample(x0 + 1, y0 + 1) * fx;
        top * (1.0 - fy) + bot * fy
    };
    let out_u = plane_to_vec(&out, 1, cw, ch);
    let out_v = plane_to_vec(&out, 2, cw, ch);
    for by in 0..ch {
        for bx in 0..cw {
            for (plane_in, plane_out, name) in [(&in_u, &out_u, "U"), (&in_v, &out_v, "V")] {
                let mut sum = 0.0f32;
                for (dx, dy) in [(0, 0), (1, 0), (0, 1), (1, 1)] {
                    // load_px clamps the out-of-range column/row at odd edges.
                    let px = (bx * 2 + dx).min(w - 1);
                    let py = (by * 2 + dy).min(h - 1);
                    sum += bilinear(plane_in, px, py);
                }
                let expected = quantize(sum * 0.25) as i32;
                let got = plane_out[by * cw + bx] as i32;
                assert!(
                    (got - expected).abs() <= 2,
                    "{w}x{h} {name} at ({bx},{by}): got {got}, reference {expected}"
                );
            }
        }
    }
}

#[test]
fn yuv_builder_requires_an_ez_effect_body() {
    let err = match WgpuFrameFilter::builder()
        .shader_yuv_wgsl(shaders::IDENTITY_FS)
        .build()
    {
        Ok(_) => panic!("a body without ez_effect must fail build()"),
        Err(e) => e,
    };
    assert!(
        matches!(&err, WgpuFilterError::InvalidOption(msg) if msg.contains("ez_effect")),
        "unexpected error: {err}"
    );
}

#[test]
fn yuv_resize_to_odd_size_keeps_constant_planes_exact() {
    let mut filter = WgpuFrameFilter::builder()
        .shader_yuv_wgsl(shaders::YUV_IDENTITY_BODY)
        .output_size(33, 25)
        .build()
        .unwrap();
    if !init_filter(&mut filter) {
        return;
    }
    let input = make_yuv_frame_with(
        65,
        49,
        AVPixelFormat::AV_PIX_FMT_YUV420P,
        |_, _| 61,
        |_, _| (90, 170),
    );
    let out = drive(&mut filter, vec![input], 1).pop().unwrap();
    unsafe {
        assert_eq!((*out.as_ptr()).width, 33);
        assert_eq!((*out.as_ptr()).height, 25);
    }
    // Odd input, odd output: resampling a constant is still the constant,
    // and the pack's clamped edge quads replicate that constant, so every
    // plane (including the ceil-sized 17x13 chroma) must be uniform.
    assert!(plane_to_vec(&out, 0, 33, 25).iter().all(|&y| y == 61));
    assert!(plane_to_vec(&out, 1, 17, 13).iter().all(|&u| u == 90));
    assert!(plane_to_vec(&out, 2, 17, 13).iter().all(|&v| v == 170));
}

#[test]
fn yuv_builder_rejects_group0_bindings_in_the_body() {
    // Group indices are const-expressions — `0u` names group 0 just as `0`
    // does, and a type-compatible alias (a sampler at binding 3) would
    // silently shadow the prelude's sampler if it slipped through.
    for body in [
        r#"
        @group(0) @binding(9) var<uniform> hijack: f32;
        fn ez_effect(coord: vec2<f32>) -> vec3<f32> {
            return vec3<f32>(hijack, 0.5, 0.5);
        }
        "#,
        r#"
        @group(0u) @binding(3) var my_samp: sampler;
        fn ez_effect(coord: vec2<f32>) -> vec3<f32> {
            return vec3<f32>(ez_luma(coord), 0.5, 0.5);
        }
        "#,
    ] {
        let err = match WgpuFrameFilter::builder().shader_yuv_wgsl(body).build() {
            Ok(_) => panic!("a body declaring a reserved group must fail build()"),
            Err(e) => e,
        };
        assert!(
            matches!(&err, WgpuFilterError::InvalidOption(msg) if msg.contains("group(0)")),
            "unexpected error: {err}"
        );
    }

    // The params group stays allowed when spelled literally.
    let params_body = r#"
        @group(1) @binding(0) var<uniform> gain: vec4<f32>;
        fn ez_effect(coord: vec2<f32>) -> vec3<f32> {
            return ez_sample_yuv(coord) * gain.x;
        }
    "#;
    assert!(WgpuFrameFilter::builder()
        .shader_yuv_wgsl(params_body)
        .build()
        .is_ok());
}

// --- Shared GPU generation cache ------------------------------------------
//
// The slot state machine (single flight, dead-generation replacement,
// failure retry, panic recovery) is generic over `Liveness`, so the first
// tests drive it with a plain payload on private cache instances and run on
// any machine. The GPU-gated tests after them exercise real generations:
// sharing across filters, profile isolation, and prompt death propagation.
// No test ever mutates the process-global cache — death tests build their
// own generation — so they cannot flake concurrently running GPU tests.

use crate::wgpu_filter::shared_gpu::{
    self, GenCache, GpuGenerationLost, GpuProfile, Liveness, SharedGpuGeneration,
};
use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};
use std::sync::Arc;
use std::time::{Duration, Instant};

#[derive(Debug)]
struct TestGen {
    id: usize,
    dead: AtomicBool,
    /// Mirrors the dmabuf transient-failure retirement: alive but not
    /// re-servable from the cache.
    retired: AtomicBool,
    /// Arms a panic in Drop, standing in for a wgpu destructor blowing up
    /// during retired-generation teardown.
    panic_on_drop: AtomicBool,
}

impl TestGen {
    fn new(id: usize) -> Arc<Self> {
        Arc::new(TestGen {
            id,
            dead: AtomicBool::new(false),
            retired: AtomicBool::new(false),
            panic_on_drop: AtomicBool::new(false),
        })
    }
}

impl Liveness for TestGen {
    fn is_dead(&self) -> bool {
        self.dead.load(Ordering::Relaxed)
    }

    fn is_reusable(&self) -> bool {
        !self.is_dead() && !self.retired.load(Ordering::Relaxed)
    }
}

impl Drop for TestGen {
    fn drop(&mut self) {
        if self.panic_on_drop.load(Ordering::Relaxed) && !std::thread::panicking() {
            panic!("simulated teardown panic");
        }
    }
}

#[test]
fn cache_single_flight_builds_once_under_concurrency() {
    let cache = Arc::new(GenCache::<TestGen>::new());
    let builds = Arc::new(AtomicUsize::new(0));
    let handles: Vec<_> = (0..2)
        .map(|_| {
            let cache = Arc::clone(&cache);
            let builds = Arc::clone(&builds);
            std::thread::spawn(move || {
                cache.acquire_with(GpuProfile::Basic, || {
                    builds.fetch_add(1, Ordering::SeqCst);
                    // Long enough that the second acquire reliably arrives
                    // while this build is still in flight (the assertions
                    // hold either way).
                    std::thread::sleep(Duration::from_millis(50));
                    Ok(TestGen::new(1))
                })
            })
        })
        .collect();
    let results: Vec<Arc<TestGen>> = handles
        .into_iter()
        .map(|h| h.join().unwrap().expect("acquire"))
        .collect();
    assert_eq!(builds.load(Ordering::SeqCst), 1, "exactly one build ran");
    assert!(
        Arc::ptr_eq(&results[0], &results[1]),
        "both acquires must share one generation"
    );
}

#[test]
fn cache_builder_thread_reentry_fails_without_waiting_on_itself() {
    let cache = Arc::new(GenCache::<TestGen>::new());
    let (tx, rx) = std::sync::mpsc::channel();
    let worker = std::thread::spawn(move || {
        let reentrant = Arc::clone(&cache);
        let result = cache.acquire_with(GpuProfile::Basic, || {
            let inner = reentrant.acquire_with(GpuProfile::Basic, || {
                panic!("a re-entrant acquire must not start another build")
            });
            match inner {
                Err(error) => assert_eq!(
                    error,
                    "shared GPU generation initialization re-entered on its builder thread"
                ),
                Ok(_) => panic!("a re-entrant acquire must fail promptly"),
            }
            Ok(TestGen::new(1))
        });
        tx.send(result.map(|generation| generation.id))
            .expect("report the outer acquire");
    });

    let result = rx
        .recv_timeout(Duration::from_secs(5))
        .expect("same-thread cache re-entry must not deadlock");
    assert_eq!(result.expect("the outer build succeeds"), 1);
    worker.join().expect("builder thread");
}

#[test]
fn cache_dead_generation_replaced_next_acquire_old_arc_survives() {
    let cache = GenCache::<TestGen>::new();
    let a = cache
        .acquire_with(GpuProfile::Basic, || Ok(TestGen::new(1)))
        .expect("first build");
    let again = cache
        .acquire_with(GpuProfile::Basic, || {
            panic!("a live generation must not be rebuilt")
        })
        .expect("cached acquire");
    assert!(Arc::ptr_eq(&a, &again), "live generation is served back");

    a.dead.store(true, Ordering::Relaxed);
    let b = cache
        .acquire_with(GpuProfile::Basic, || Ok(TestGen::new(2)))
        .expect("replacement build");
    assert!(!Arc::ptr_eq(&a, &b), "a dead generation must be replaced");
    assert_eq!(b.id, 2, "the replacement is the newly built generation");
    // The dead generation's Arc contents stay readable for filters still
    // holding it: they fail promptly, but never dangle.
    assert_eq!(a.id, 1);
    assert!(a.is_dead());
}

#[test]
fn cache_profiles_isolated() {
    let cache = GenCache::<TestGen>::new();
    let basic = cache
        .acquire_with(GpuProfile::Basic, || Ok(TestGen::new(1)))
        .expect("basic build");
    let dmabuf = cache
        .acquire_with(GpuProfile::DmabufCapable, || Ok(TestGen::new(2)))
        .expect("dmabuf build");
    assert!(
        !Arc::ptr_eq(&basic, &dmabuf),
        "profiles must own distinct slots"
    );
    assert_eq!(
        (basic.id, dmabuf.id),
        (1, 2),
        "each profile ran its own build"
    );
    // And each slot serves its own generation back afterwards.
    let basic_again = cache
        .acquire_with(GpuProfile::Basic, || panic!("basic slot must be cached"))
        .expect("basic cached");
    assert!(Arc::ptr_eq(&basic, &basic_again));
}

#[test]
fn cache_failed_build_leaves_slot_retryable() {
    let cache = GenCache::<TestGen>::new();
    let builds = AtomicUsize::new(0);
    let err = cache
        .acquire_with(GpuProfile::Basic, || {
            builds.fetch_add(1, Ordering::SeqCst);
            Err("no backend today".to_string())
        })
        .expect_err("a build failure must reach the caller");
    assert_eq!(err, "no backend today");
    // The failure reset the slot: the next acquire runs a fresh build
    // (per-init failure reporting on GPU-less machines, and recovery once
    // the cause clears).
    let generation = cache
        .acquire_with(GpuProfile::Basic, || {
            builds.fetch_add(1, Ordering::SeqCst);
            Ok(TestGen::new(3))
        })
        .expect("the retry must rebuild");
    assert_eq!(
        builds.load(Ordering::SeqCst),
        2,
        "both acquires ran a build"
    );
    assert_eq!(generation.id, 3);
}

#[test]
fn cache_builder_panic_unblocks_waiters() {
    let cache = Arc::new(GenCache::<TestGen>::new());
    let (started_tx, started_rx) = std::sync::mpsc::channel();
    let panicker = {
        let cache = Arc::clone(&cache);
        std::thread::spawn(move || {
            let _ = cache.acquire_with(GpuProfile::Basic, || {
                started_tx.send(()).expect("signal the waiter");
                // Give the waiter time to block on the Building slot.
                std::thread::sleep(Duration::from_millis(100));
                panic!("simulated build panic");
            });
        })
    };
    started_rx.recv().expect("builder entered its build");
    // This acquire waits on the in-flight build; when the builder's unwind
    // resets the slot it must wake, become the builder itself, and succeed
    // rather than hang forever.
    let generation = cache
        .acquire_with(GpuProfile::Basic, || Ok(TestGen::new(7)))
        .expect("the waiter takes over after the panic");
    assert_eq!(generation.id, 7);
    assert!(
        panicker.join().is_err(),
        "the builder thread dies of its own panic"
    );
}

#[test]
fn cache_replaces_live_but_nonreusable_generation() {
    let cache = GenCache::<TestGen>::new();
    let a = cache
        .acquire_with(GpuProfile::DmabufCapable, || Ok(TestGen::new(1)))
        .expect("first build");
    a.retired.store(true, Ordering::Relaxed);
    // Alive-but-retired (the dmabuf transient-failure shape): the next
    // acquire must rebuild instead of serving the cached Arc back...
    let b = cache
        .acquire_with(GpuProfile::DmabufCapable, || Ok(TestGen::new(2)))
        .expect("replacement build");
    assert!(
        !Arc::ptr_eq(&a, &b),
        "a retired generation must be replaced"
    );
    assert_eq!(b.id, 2);
    // ...while jobs already on the old generation keep running: it is
    // retired, never killed.
    assert!(!a.is_dead(), "retirement must not kill running jobs");
    assert_eq!(a.id, 1);
}

/// A retired generation's teardown — potentially its final `Arc`, hence
/// real destructors — must run outside the slot lock, covered by the
/// builder's reset guard: a panicking destructor surfaces on the acquiring
/// thread, but leaves the slot Idle, the mutex unpoisoned, and the next
/// acquire fully functional. (Dropping the retiree while holding the slot
/// mutex would instead poison it mid-transition and strand the machine.)
#[test]
fn cache_retired_teardown_panics_do_not_wedge_the_slot() {
    let cache = GenCache::<TestGen>::new();
    let doomed = cache
        .acquire_with(GpuProfile::Basic, || Ok(TestGen::new(1)))
        .expect("first build");
    doomed.dead.store(true, Ordering::Relaxed);
    doomed.panic_on_drop.store(true, Ordering::Relaxed);
    drop(doomed); // the cache now holds the final Arc

    let outcome = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
        cache.acquire_with(GpuProfile::Basic, || Ok(TestGen::new(2)))
    }));
    assert!(
        outcome.is_err(),
        "the teardown panic must surface on the acquiring thread"
    );

    // The machine survived: the guard reset the slot and the mutex was
    // never poisoned mid-teardown, so the next acquire builds normally.
    let recovered = cache
        .acquire_with(GpuProfile::Basic, || Ok(TestGen::new(3)))
        .expect("the cache must stay functional after a teardown panic");
    assert_eq!(recovered.id, 3);
}

#[test]
fn shared_generation_pointer_equal_across_filters() {
    let mut first = WgpuFrameFilter::new_identity().unwrap();
    if !init_filter(&mut first) {
        return;
    }
    let mut second = WgpuFrameFilter::new_identity().unwrap();
    if !init_filter(&mut second) {
        return;
    }
    // Read-only on the process-global cache: both inits must have resolved
    // to the same live generation (device, queue and snapshots included).
    let a = first.shared_generation_for_test().expect("initialized");
    let b = second.shared_generation_for_test().expect("initialized");
    assert!(
        Arc::ptr_eq(a, b),
        "two basic-profile filters must share one generation"
    );
}

#[test]
fn profiles_get_distinct_generations_gpu() {
    let mut basic = WgpuFrameFilter::new_identity().unwrap();
    if !init_filter(&mut basic) {
        return;
    }
    let mut hw = WgpuFrameFilter::builder()
        .shader_wgsl(shaders::IDENTITY_FS)
        .hw_zero_copy_input(true)
        .build()
        .unwrap();
    if !init_filter(&mut hw) {
        return;
    }
    // Even where dmabuf extensions are unavailable the DmabufCapable
    // profile owns a separate plain device, so the generations can never
    // be pointer-equal.
    let a = basic.shared_generation_for_test().expect("initialized");
    let b = hw.shared_generation_for_test().expect("initialized");
    assert!(!Arc::ptr_eq(a, b), "profiles must not share a generation");
}

#[test]
fn dead_generation_fails_filter_promptly_with_stable_error() {
    // A private cache running the real builder: killing this generation
    // must never touch the process-global slots other GPU tests stream on.
    let cache = GenCache::<SharedGpuGeneration>::new();
    let mut deferred = Vec::new();
    let generation = cache.acquire_with(GpuProfile::Basic, || {
        shared_gpu::build_generation(GpuProfile::Basic, &mut deferred)
    });
    shared_gpu::emit_deferred_info(deferred);
    let generation = match generation {
        Ok(generation) => generation,
        Err(e) if e.contains("adapter") || e.contains("device") => {
            eprintln!("skipping wgpu test (no GPU): {e}");
            return;
        }
        Err(e) => panic!("generation build failed: {e}"),
    };

    let mut filter = WgpuFrameFilter::new_identity().unwrap();
    filter
        .init_with_generation_for_test(Arc::clone(&generation))
        .expect("init against the private generation");

    // Prove the generation works, then leave one submission in flight so
    // the completion path's death check is exercised too. A fast device
    // can complete a submission within its own filter_frame call
    // (returning Some), so loop until one stays pending.
    drive(&mut filter, vec![make_yuv420p_frame(64, 48)], 1);
    let mut map = HashMap::new();
    let mut ctx = make_ctx(&mut map);
    for attempt in 0.. {
        assert!(attempt < 1000, "device keeps completing within one call");
        let frame = make_yuv420p_frame(64, 48);
        if filter
            .filter_frame(frame, &mut ctx)
            .expect("filter_frame")
            .is_none()
        {
            break; // one readback is now pending in the output queue
        }
    }

    generation.mark_dead();
    let start = Instant::now();
    let err = match filter.filter_frame(make_yuv420p_frame(64, 48), &mut ctx) {
        Err(e) => e,
        Ok(_) => panic!("filter_frame on a dead generation must fail"),
    };
    assert!(
        err.downcast_ref::<GpuGenerationLost>().is_some(),
        "unexpected filter_frame error: {err}"
    );
    let err = match filter.request_frame(&mut ctx) {
        Err(e) => e,
        Ok(_) => panic!("a pending readback on a dead generation must fail"),
    };
    assert!(
        err.downcast_ref::<GpuGenerationLost>().is_some(),
        "unexpected request_frame error: {err}"
    );
    // Prompt failure: well under the 30s wedge window a stuck job used to
    // sit out on its own.
    assert!(
        start.elapsed() < Duration::from_secs(5),
        "death must surface promptly, took {:?}",
        start.elapsed()
    );
}
