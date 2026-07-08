use crate::core::filter::frame_filter_context::FrameFilterContext;
use crate::filter::frame_filter::{FrameFilter, FrameFilterError};
use crate::wgpu_filter::shaders;
use crate::wgpu_filter::wgpu_frame_filter::WgpuFrameFilter;
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

fn init_filter(filter: &mut WgpuFrameFilter) -> bool {
    let mut map = HashMap::new();
    let ctx = make_ctx(&mut map);
    match filter.init(&ctx) {
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
fn drive(filter: &mut WgpuFrameFilter, inputs: Vec<Frame>, expected: usize) -> Vec<Frame> {
    let mut map = HashMap::new();
    let ctx = make_ctx(&mut map);
    let mut out = Vec::new();
    for frame in inputs {
        if let Some(f) = filter.filter_frame(frame, &ctx).expect("filter_frame") {
            out.push(f);
        }
        while let Some(f) = filter.request_frame(&ctx).expect("request_frame") {
            out.push(f);
        }
    }
    for _ in 0..2000 {
        while let Some(f) = filter.request_frame(&ctx).expect("request_frame") {
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
    let ctx = make_ctx(&mut map);
    let mut out = Vec::new();
    for i in 0..3i64 {
        let frame = make_planar_frame(
            1280,
            720,
            AVPixelFormat::AV_PIX_FMT_YUV420P,
            Some(40 + 40 * i as u8),
            i,
        );
        if let Some(f) = filter.filter_frame(frame, &ctx).expect("filter_frame") {
            out.push(f);
        }
    }
    // The marker is the last send before the source drops its channel.
    if let Some(f) = filter
        .filter_frame(make_marker_frame(100), &ctx)
        .expect("marker filter_frame")
    {
        out.push(f);
    }
    // Exactly one non-blocking sweep, like run_pipeline's final pass.
    while let Some(f) = filter.request_frame(&ctx).expect("request_frame") {
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
    let ctx = make_ctx(&mut map);
    for i in 0..2 {
        let out = filter
            .filter_frame(make_yuv420p_frame(160, 90), &ctx)
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
    let ctx = make_ctx(&mut map);
    // The device is created with wgpu's default limits, which cap 2D
    // textures at 8192; exceeding that must be a clean Err, not a panic
    // from wgpu's uncaptured-error handler.
    let frame = make_planar_frame(8200, 16, AVPixelFormat::AV_PIX_FMT_YUV420P, Some(60), 0);
    let err = match filter.filter_frame(frame, &ctx) {
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
    let ctx = make_ctx(&mut map);

    unsafe {
        let mut rgb = Frame::empty();
        let p = rgb.as_mut_ptr();
        (*p).width = 64;
        (*p).height = 64;
        (*p).format = AVPixelFormat::AV_PIX_FMT_RGB24 as i32;
        assert!(av_frame_get_buffer(p, 1) >= 0);
        let err = match filter.filter_frame(rgb, &ctx) {
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
        let err = match filter.filter_frame(hw, &ctx) {
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
    let ctx = make_ctx(&mut map);
    match filter.init(&ctx) {
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
    let ctx = make_ctx(&mut map);
    match filter.init(&ctx) {
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
        _ctx: &FrameFilterContext,
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
    let ctx = make_ctx(&mut map);
    filter.uninit(&ctx);
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
    let ctx = make_ctx(&mut map);
    let mut seen = 0usize;
    for i in 0..n {
        let mut frame = make_yuv420p_frame(w, h);
        unsafe { (*frame.as_mut_ptr()).pts = i as i64 };
        // Frames are dropped right after checking, exercising the recycle
        // path: later iterations must reuse returned buffers instead of
        // growing GPU memory without bound.
        if let Some(out) = filter.filter_frame(frame, &ctx).expect("filter_frame") {
            assert_eq!(unsafe { (*out.as_ptr()).pts }, seen as i64, "order");
            let diff = max_luma_diff(&out, &expected, w as usize, h as usize);
            assert!(diff <= 3, "luma diff too large at frame {seen}: {diff}");
            seen += 1;
        }
        while let Some(out) = filter.request_frame(&ctx).expect("request_frame") {
            assert_eq!(unsafe { (*out.as_ptr()).pts }, seen as i64, "order");
            seen += 1;
        }
    }
    for _ in 0..2000 {
        while let Some(out) = filter.request_frame(&ctx).expect("request_frame") {
            assert_eq!(unsafe { (*out.as_ptr()).pts }, seen as i64, "order");
            seen += 1;
        }
        if seen >= n {
            break;
        }
        std::thread::sleep(std::time::Duration::from_millis(1));
    }
    assert_eq!(seen, n, "all zero-copy frames must drain");
    filter.uninit(&ctx);
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
    let ctx = make_ctx(&mut map);
    filter.uninit(&ctx);
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
