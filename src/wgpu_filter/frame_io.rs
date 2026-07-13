//! CPU<->GPU frame I/O: input format detection, plane access, per-frame
//! upload/encode/submit, and output frame construction from mapped bytes.

use crate::util::ffmpeg_utils::av_err2str;
use crate::util::frame_utils::copy_plane;
use crate::wgpu_filter::gpu_state::{
    EffectPipeline, GpuState, OutputGeometry, PassBinds, StagingSlot,
};
use crate::wgpu_filter::hw_interop::{self, ImportedNv12};
use crate::wgpu_filter::params::SharedParams;
use ffmpeg_next::Frame;
use ffmpeg_sys_next::{
    av_buffer_create, av_buffer_pool_get, av_buffer_pool_init, av_buffer_pool_uninit,
    av_frame_copy_props, av_frame_get_buffer, av_hwframe_map, av_hwframe_transfer_data, av_q2d,
    AVBufferPool, AVDRMFrameDescriptor, AVPixelFormat, AV_BUFFER_FLAG_READONLY,
    AV_HWFRAME_MAP_READ,
};
use std::ptr::NonNull;
use std::sync::atomic::Ordering;
use std::sync::mpsc;

/// Chroma plane layout of a supported input pixel format.
#[derive(Clone, Copy, PartialEq, Eq, Debug)]
pub(crate) enum PlaneLayout {
    /// Three planes (Y, U, V) with chroma subsampled by the given factors:
    /// 4:2:0 = (2, 2), 4:2:2 = (2, 1), 4:4:4 = (1, 1). The convert shader
    /// samples chroma with normalized coordinates, so one pipeline covers
    /// every subsampling ratio.
    Planar { sub_x: u32, sub_y: u32 },
    /// Two planes (Y + interleaved UV) at 4:2:0.
    Nv12,
}

impl PlaneLayout {
    /// Chroma plane size for the given luma size.
    pub(crate) fn chroma_size(self, w: u32, h: u32) -> (u32, u32) {
        match self {
            PlaneLayout::Planar { sub_x, sub_y } => (w.div_ceil(sub_x), h.div_ceil(sub_y)),
            PlaneLayout::Nv12 => (w.div_ceil(2), h.div_ceil(2)),
        }
    }
}

/// Maps a pixel format to its plane layout and effective color range.
/// J-formats are full range by definition; for the rest the frame's
/// `color_range` decides.
pub(crate) fn detect_format(
    pix_fmt: AVPixelFormat,
    color_range: ffmpeg_sys_next::AVColorRange,
) -> Result<(PlaneLayout, bool), String> {
    use AVPixelFormat::*;
    let full = color_range == ffmpeg_sys_next::AVColorRange::AVCOL_RANGE_JPEG;
    match pix_fmt {
        AV_PIX_FMT_YUV420P => Ok((PlaneLayout::Planar { sub_x: 2, sub_y: 2 }, full)),
        AV_PIX_FMT_YUVJ420P => Ok((PlaneLayout::Planar { sub_x: 2, sub_y: 2 }, true)),
        AV_PIX_FMT_YUV422P => Ok((PlaneLayout::Planar { sub_x: 2, sub_y: 1 }, full)),
        AV_PIX_FMT_YUVJ422P => Ok((PlaneLayout::Planar { sub_x: 2, sub_y: 1 }, true)),
        AV_PIX_FMT_YUV444P => Ok((PlaneLayout::Planar { sub_x: 1, sub_y: 1 }, full)),
        AV_PIX_FMT_YUVJ444P => Ok((PlaneLayout::Planar { sub_x: 1, sub_y: 1 }, true)),
        AV_PIX_FMT_NV12 => Ok((PlaneLayout::Nv12, full)),
        other => Err(format!(
            "WgpuFrameFilter supports YUV420P/YUV422P/YUV444P (plus their \
             full-range J variants) and NV12 input, got {other:?}. Insert \
             `format=yuv420p` in filter_desc before this pipeline."
        )),
    }
}

/// 0 = BT.601, 1 = BT.709; unspecified falls back to the common size heuristic.
pub(crate) fn matrix_id_for(colorspace: ffmpeg_sys_next::AVColorSpace, height: i32) -> u32 {
    use ffmpeg_sys_next::AVColorSpace::*;
    match colorspace {
        AVCOL_SPC_BT709 => 1,
        AVCOL_SPC_BT470BG | AVCOL_SPC_SMPTE170M | AVCOL_SPC_SMPTE240M => 0,
        _ => {
            if height >= 720 {
                1
            } else {
                0
            }
        }
    }
}

/// Downloads a hardware frame (VAAPI, CUDA, ...) into a freshly allocated
/// CPU frame via `av_hwframe_transfer_data`, preserving properties. The
/// transfer picks the hw context's native software format (NV12 for VAAPI),
/// which the caller then feeds through the normal software path.
pub(crate) fn download_hw_frame(src: &Frame) -> Result<Frame, String> {
    // SAFETY: `dst` is freshly allocated; `src` is a live hardware frame.
    // av_hwframe_transfer_data allocates dst's buffers and sets its format.
    unsafe {
        let mut dst = Frame::empty();
        let p = dst.as_mut_ptr();
        if p.is_null() {
            return Err("Failed to allocate download frame".to_string());
        }
        let ret = av_hwframe_transfer_data(p, src.as_ptr(), 0);
        if ret < 0 {
            return Err(format!(
                "Failed to download hardware frame to system memory: {}",
                av_err2str(ret)
            ));
        }
        let ret = av_frame_copy_props(p, src.as_ptr());
        if ret < 0 {
            return Err(format!("Failed to copy frame props: {}", av_err2str(ret)));
        }
        (*p).time_base = (*src.as_ptr()).time_base;
        Ok(dst)
    }
}

/// A hardware frame mapped to DRM_PRIME with both NV12 planes imported as
/// wgpu textures. `_drm_frame` keeps the export (and through it the decoder
/// surface) alive; it must outlive all GPU reads of the textures, so the
/// caller stores this in its in-flight slot until readback completes.
pub(crate) struct HwMappedFrame {
    pub(crate) imported: ImportedNv12,
    _drm_frame: Frame,
}

/// Maps a VAAPI (or already DRM_PRIME) frame to a dmabuf descriptor and
/// imports its planes as wgpu textures — the zero-copy input path. Any error
/// is a signal to fall back to `download_hw_frame`.
pub(crate) fn import_hw_frame(gpu: &GpuState, src: &Frame) -> Result<HwMappedFrame, String> {
    let interop = gpu
        .hw_interop
        .as_ref()
        .ok_or("dmabuf import not available on this device")?;

    // SAFETY: `src` is a live hardware frame; the mapped frame owns a ref on
    // it (AV_HWFRAME_MAP_READ) plus the exported fds, released on drop.
    unsafe {
        let mut drm_frame = Frame::empty();
        let p = drm_frame.as_mut_ptr();
        if p.is_null() {
            return Err("Failed to allocate DRM map frame".to_string());
        }
        (*p).format = AVPixelFormat::AV_PIX_FMT_DRM_PRIME as i32;
        let ret = av_hwframe_map(p, src.as_ptr(), AV_HWFRAME_MAP_READ as i32);
        if ret < 0 {
            return Err(format!(
                "av_hwframe_map to DRM_PRIME failed: {}",
                av_err2str(ret)
            ));
        }

        let desc = (*p).data[0] as *const AVDRMFrameDescriptor;
        if desc.is_null() {
            return Err("mapped DRM_PRIME frame has no descriptor".to_string());
        }
        let drm = hw_interop::parse_drm_nv12(desc)?;
        let (w, h) = ((*src.as_ptr()).width as u32, (*src.as_ptr()).height as u32);
        let imported = interop.import_nv12(&gpu.device, &drm, w, h)?;
        Ok(HwMappedFrame {
            imported,
            _drm_frame: drm_frame,
        })
    }
}

/// Returns a read-only slice for one plane plus its stride. The slice is
/// tight: it ends after `width_bytes` of the last row, so externally wrapped
/// frames whose last row is not stride-padded are never over-read.
fn plane_slice(
    frame: &Frame,
    index: usize,
    rows: usize,
    width_bytes: usize,
) -> Result<(&[u8], usize), String> {
    // SAFETY: `frame` is a live frame; reading AVFrame scalar fields is sound.
    let raw = unsafe { frame.as_ptr() };
    let linesize_raw = unsafe { (*raw).linesize[index] };
    if linesize_raw <= 0 {
        return Err(format!(
            "Unsupported linesize {linesize_raw} at plane {index}: zero or negative \
             (bottom-up) strides are not supported by WgpuFrameFilter"
        ));
    }
    let linesize = linesize_raw as usize;
    if linesize < width_bytes {
        return Err(format!(
            "Invalid linesize {linesize} at plane {index}: smaller than row size {width_bytes}"
        ));
    }
    let data_ptr = unsafe { (*frame.as_ptr()).data[index] };
    if data_ptr.is_null() {
        return Err(format!("Data pointer at plane {index} is null"));
    }
    let len = (rows - 1) * linesize + width_bytes;
    // SAFETY: the caller validated this is a software frame; FFmpeg allocates
    // at least `linesize * rows` bytes per plane and `len` <= that. The
    // returned lifetime is bound to `frame`, so the slice cannot outlive it.
    Ok((
        unsafe { std::slice::from_raw_parts(data_ptr, len) },
        linesize,
    ))
}

/// Uploads one validated input frame, encodes the convert/effect/pack passes
/// plus the copy into `staging`, submits, and registers the readback map.
/// Returns the submission index and the map-completion receiver.
///
/// Uploads use `queue.write_texture`, although wgpu-core 26 allocates a
/// transient staging buffer per call: a hand-rolled reusable MAP_WRITE ring
/// was measured SLOWER on this path (COPY_BYTES_PER_ROW_ALIGNMENT pads every
/// row to 256 B, inflating the CPU copy for widths that are not multiples of
/// 256, plus an extra map_async round trip per frame; 1080p upload
/// 0.77 -> 0.95 ms/frame on RADV RENOIR).
pub(crate) fn upload_and_encode(
    gpu: &GpuState,
    frame: &Frame,
    layout: PlaneLayout,
    matrix_id: u32,
    full_range: bool,
    staging: &mut StagingSlot,
    params: &SharedParams,
) -> Result<wgpu::SubmissionIndex, String> {
    let res = gpu.resources.as_ref().expect("resources ensured");
    let (cw, ch) = layout.chroma_size(res.in_w, res.in_h);

    let write_plane = |tex: &wgpu::Texture, data: &[u8], stride: usize, w: u32, h: u32| {
        gpu.queue.write_texture(
            wgpu::TexelCopyTextureInfo {
                texture: tex,
                mip_level: 0,
                origin: wgpu::Origin3d::ZERO,
                aspect: wgpu::TextureAspect::All,
            },
            data,
            wgpu::TexelCopyBufferLayout {
                offset: 0,
                bytes_per_row: Some(stride as u32),
                rows_per_image: None,
            },
            wgpu::Extent3d {
                width: w,
                height: h,
                depth_or_array_layers: 1,
            },
        );
    };

    let (y_data, y_stride) = plane_slice(frame, 0, res.in_h as usize, res.in_w as usize)?;
    write_plane(&res.tex_y, y_data, y_stride, res.in_w, res.in_h);
    match layout {
        PlaneLayout::Planar { .. } => {
            let (u_data, u_stride) = plane_slice(frame, 1, ch as usize, cw as usize)?;
            let (v_data, v_stride) = plane_slice(frame, 2, ch as usize, cw as usize)?;
            write_plane(&res.tex_u, u_data, u_stride, cw, ch);
            let tex_v = res.tex_v.as_ref().expect("planar layout has a V texture");
            write_plane(tex_v, v_data, v_stride, cw, ch);
        }
        PlaneLayout::Nv12 => {
            let (uv_data, uv_stride) = plane_slice(frame, 1, ch as usize, cw as usize * 2)?;
            write_plane(&res.tex_u, uv_data, uv_stride, cw, ch);
        }
    }

    encode_and_submit(
        gpu, layout, None, frame, matrix_id, full_range, staging, params,
    )
}

/// Encodes the frame's passes against an already-uploaded (or imported)
/// input, submits, and registers the readback map. Shared tail of the
/// software and hardware input paths: `hw_input_bind` overrides the cached
/// input bind group with a per-frame one built over imported hw textures.
///
/// RGBA mode encodes convert -> effect -> pack; YUV passthrough mode has no
/// convert pass — the effect samples the input planes directly and the pack
/// runs in identity mode.
#[allow(clippy::too_many_arguments)]
pub(crate) fn encode_and_submit(
    gpu: &GpuState,
    layout: PlaneLayout,
    hw_input_bind: Option<&wgpu::BindGroup>,
    frame: &Frame,
    matrix_id: u32,
    full_range: bool,
    staging: &mut StagingSlot,
    params: &SharedParams,
) -> Result<wgpu::SubmissionIndex, String> {
    let res = gpu.resources.as_ref().expect("resources ensured");
    let yuv_mode = matches!(gpu.effect_pipeline, EffectPipeline::Yuv { .. });

    // Per-frame uniforms. Queue writes are ordered on the queue timeline, so
    // sharing one uniform buffer across in-flight frames is race-free: the
    // write for frame N+1 executes after frame N's submission. The convert
    // and pack payloads only change with geometry/colorspace, so an
    // equal-value write is skipped (the buffer already holds these bytes).
    if !yuv_mode {
        let convert_data: [u32; 4] = [matrix_id, full_range as u32, 0, 0];
        if gpu.convert_uniforms_cache.get() != Some(convert_data) {
            gpu.queue.write_buffer(
                &gpu.convert_uniforms,
                0,
                bytemuck::cast_slice(&convert_data),
            );
            gpu.convert_uniforms_cache.set(Some(convert_data));
        }
    }

    // SAFETY: reading scalar fields from a live, validated frame.
    let play_time = unsafe {
        let pts = (*frame.as_ptr()).pts;
        if pts == ffmpeg_sys_next::AV_NOPTS_VALUE {
            0.0
        } else {
            pts as f64 * av_q2d((*frame.as_ptr()).time_base)
        }
    };
    // The fourth slot is the documented `_pad` of the RGBA shader contract
    // and must stay 0.0 there — an existing shader reading it would observe
    // a value change otherwise. Only the YUV prelude interprets it, as the
    // effective-range flag behind `ez_full_range()`.
    let range_flag = if yuv_mode {
        full_range as u32 as f32
    } else {
        0.0
    };
    let ez_data: [f32; 4] = [
        play_time as f32,
        res.out_w as f32,
        res.out_h as f32,
        range_flag,
    ];
    gpu.queue
        .write_buffer(&gpu.ez_uniforms, 0, bytemuck::cast_slice(&ez_data));

    if params.len > 0 && params.dirty.swap(false, Ordering::AcqRel) {
        // Poison-tolerant like WgpuParamsHandle::set: the buffer is always
        // left in a valid state by the writer.
        let bytes = params
            .bytes
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        gpu.queue.write_buffer(&gpu.params_buf, 0, &bytes);
    }

    let pack_data: [u32; 12] = [
        res.out_w,
        res.out_h,
        (res.y_stride / 4) as u32,
        (res.c_stride / 4) as u32,
        (res.y_stride / 4 * res.out_h as usize) as u32,
        (res.y_stride / 4 * res.out_h as usize + res.c_stride / 4 * res.out_h.div_ceil(2) as usize)
            as u32,
        matrix_id,
        full_range as u32,
        yuv_mode as u32,
        0,
        0,
        0,
    ];
    if gpu.pack_uniforms_cache.get() != Some(pack_data) {
        gpu.queue
            .write_buffer(&gpu.pack_uniforms, 0, bytemuck::cast_slice(&pack_data));
        gpu.pack_uniforms_cache.set(Some(pack_data));
    }

    // Encode the three passes.
    let mut encoder = gpu
        .device
        .create_command_encoder(&wgpu::CommandEncoderDescriptor {
            label: Some("ez_frame"),
        });

    fn color_attachment(view: &wgpu::TextureView) -> Option<wgpu::RenderPassColorAttachment<'_>> {
        Some(wgpu::RenderPassColorAttachment {
            view,
            resolve_target: None,
            ops: wgpu::Operations {
                load: wgpu::LoadOp::Clear(wgpu::Color::BLACK),
                store: wgpu::StoreOp::Store,
            },
            depth_slice: None,
        })
    }

    match (&gpu.effect_pipeline, &res.pass_binds) {
        (
            EffectPipeline::Rgba(effect_pipeline),
            PassBinds::Rgba {
                intermediate_view,
                convert_bind,
                effect_bind,
            },
        ) => {
            {
                let mut pass = encoder.begin_render_pass(&wgpu::RenderPassDescriptor {
                    label: Some("ez_convert_pass"),
                    color_attachments: &[color_attachment(intermediate_view)],
                    depth_stencil_attachment: None,
                    timestamp_writes: None,
                    occlusion_query_set: None,
                });
                pass.set_pipeline(gpu.convert_pipeline(layout));
                pass.set_bind_group(0, hw_input_bind.unwrap_or(convert_bind), &[]);
                pass.draw(0..3, 0..1);
            }
            let mut pass = encoder.begin_render_pass(&wgpu::RenderPassDescriptor {
                label: Some("ez_effect_pass"),
                color_attachments: &[color_attachment(&res.out_view)],
                depth_stencil_attachment: None,
                timestamp_writes: None,
                occlusion_query_set: None,
            });
            pass.set_pipeline(effect_pipeline);
            pass.set_bind_group(0, effect_bind, &[]);
            pass.set_bind_group(1, &res.effect_bind1, &[]);
            pass.draw(0..3, 0..1);
        }
        (EffectPipeline::Yuv { planar, nv12 }, PassBinds::Yuv { effect_bind }) => {
            let mut pass = encoder.begin_render_pass(&wgpu::RenderPassDescriptor {
                label: Some("ez_yuv_effect_pass"),
                color_attachments: &[color_attachment(&res.out_view)],
                depth_stencil_attachment: None,
                timestamp_writes: None,
                occlusion_query_set: None,
            });
            pass.set_pipeline(match layout {
                PlaneLayout::Planar { .. } => planar,
                PlaneLayout::Nv12 => nv12,
            });
            pass.set_bind_group(0, hw_input_bind.unwrap_or(effect_bind), &[]);
            pass.set_bind_group(1, &res.effect_bind1, &[]);
            pass.draw(0..3, 0..1);
        }
        // The pipeline mode is fixed at init and the resources are built by
        // the same GpuState, so the variants can never disagree.
        _ => unreachable!("effect pipeline and pass binds share the mode"),
    }
    {
        let mut pass = encoder.begin_compute_pass(&wgpu::ComputePassDescriptor {
            label: Some("ez_pack_pass"),
            timestamp_writes: None,
        });
        pass.set_pipeline(&gpu.pack_pipeline);
        // Direct-pack mode (unified memory): the pack pass writes the mappable
        // staging buffer itself, so there is no storage buffer and no copy.
        // The bind group targeting that buffer is cached on the staging slot
        // and only rebuilt when the resources (hence `out_view`) were rebuilt
        // under it, detected via the resource generation.
        let pack_bind = if gpu.direct_pack {
            staging.ensure_direct_pack_bind(gpu, &res.out_view, res.resource_generation)
        } else {
            res.pack_bind
                .as_ref()
                .expect("copy-path resources always carry a cached pack bind")
        };
        pass.set_bind_group(0, pack_bind, &[]);
        pass.dispatch_workgroups(res.out_w.div_ceil(64), res.out_h.div_ceil(16), 1);
    }
    if let Some(storage) = &res.storage {
        encoder.copy_buffer_to_buffer(storage, 0, &staging.buffer, 0, res.buf_size);
    }

    let submission = gpu.queue.submit(Some(encoder.finish()));

    // Reuse this slot's map-completion channel instead of allocating one per
    // frame. The slot's previous map is always drained before it is reused, so
    // the channel is empty here and receives exactly this submission's result.
    let tx = staging.map_sender();
    staging
        .buffer
        .slice(..)
        .map_async(wgpu::MapMode::Read, move |result| {
            let _ = tx.send(result);
        });
    Ok(submission)
}

/// Fixed plane layout of the pooled YUV420P output, captured once from a real
/// `av_frame_get_buffer(..., 4)` template so the pool reproduces FFmpeg's exact
/// linesizes, height padding, and per-plane byte offsets instead of a
/// hand-rolled tight layout.
struct OutputFrameLayout {
    /// Size of one pooled buffer, equal to the template's `buf[0]->size`.
    pool_buffer_size: usize,
    /// FFmpeg linesizes for planes 0..=3 (Y, U, V, unused-for-420p).
    linesize: [i32; 4],
    /// Byte offset of each plane's `data[i]` within the single pooled buffer.
    plane_offset: [usize; 3],
}

/// Per-geometry pool of YUV420P output buffers for the default (copy) readback
/// path. A single [`AVBufferPool`] recycles same-sized buffers; each built
/// frame owns an independent `AVBufferRef` obtained from the pool, so its
/// lifetime is driven by FFmpeg refcounting (fan-out `av_frame_ref`, encoder
/// hold, ...) exactly like a freshly allocated frame. Replacing the pool on a
/// geometry change is safe: outstanding frames keep their own `AVBufferRef`
/// and the pool defers its final free until the last buffer is released.
pub(crate) struct OutputFramePool {
    key: OutputGeometry,
    layout: OutputFrameLayout,
    pool: NonNull<AVBufferPool>,
}

// SAFETY: `AVBufferPool` is documented as a lock-free thread-safe pool of
// AVBuffers (FFmpeg libavutil/buffer.h): `av_buffer_pool_get` and the buffer
// free path that returns a buffer to the pool are safe from multiple threads
// as long as the default alloc callback is used — which it is here
// (`av_buffer_pool_init(size, None)`). The `OutputFramePool` itself is only
// used from the owning filter thread; downstream threads that drop a frame's
// last reference merely trigger `AVBufferRef` release, which the pool handles
// internally. The `NonNull` pointer is not otherwise shared.
unsafe impl Send for OutputFramePool {}

impl OutputFramePool {
    /// Builds a pool for `key`. Allocates one throwaway template frame via
    /// `av_frame_get_buffer(..., 4)` to inherit FFmpeg's exact plane layout,
    /// then initialises an `AVBufferPool` of that buffer size.
    pub(crate) fn new(key: OutputGeometry) -> Result<Self, String> {
        // SAFETY: `template` is a freshly allocated frame; av_frame_get_buffer
        // sizes and lays out its single `buf[0]` for us. We only read scalar
        // fields and pointer differences before the template is freed on drop.
        let layout = unsafe {
            let mut template = Frame::empty();
            let p = template.as_mut_ptr();
            if p.is_null() {
                return Err("Failed to allocate output pool template frame".to_string());
            }
            (*p).width = key.out_w as i32;
            (*p).height = key.out_h as i32;
            (*p).format = AVPixelFormat::AV_PIX_FMT_YUV420P as i32;
            // align=4 matches the GPU-side packed strides so copy_plane keeps
            // its contiguous fast path; the copies still consult the recorded
            // linesizes, so any allocator layout stays correct.
            let ret = av_frame_get_buffer(p, 4);
            if ret < 0 {
                return Err(format!(
                    "Failed to allocate output pool template buffer: {}",
                    av_err2str(ret)
                ));
            }
            let buf0 = (*p).buf[0];
            if buf0.is_null() {
                return Err("Output pool template frame has no backing buffer".to_string());
            }
            // av_frame_get_buffer packs all YUV420P planes into a single
            // buf[0] (FFmpeg get_video_buffer), so every data[i] is an offset
            // into buf0->data.
            let base = (*buf0).data as usize;
            let pool_buffer_size = (*buf0).size;
            let linesize = [
                (*p).linesize[0],
                (*p).linesize[1],
                (*p).linesize[2],
                (*p).linesize[3],
            ];
            // Rows each plane occupies in a built frame: Y is full height, the
            // two 4:2:0 chroma planes are half height (rounded up).
            let out_h = key.out_h as usize;
            let out_ch = key.out_h.div_ceil(2) as usize;
            let plane_rows = [out_h, out_ch, out_ch];
            let mut plane_offset = [0usize; 3];
            for (i, off) in plane_offset.iter_mut().enumerate() {
                let d = (*p).data[i];
                if d.is_null() {
                    return Err(format!("Output pool template plane {i} is null"));
                }
                let offset = (d as usize).checked_sub(base).ok_or_else(|| {
                    format!("Output pool template plane {i} lies before its buffer base")
                })?;
                // build_frame copies `linesize[i] * plane_rows[i]` bytes from
                // `base + offset`; validate that extent fits one pooled buffer
                // so the per-frame from_raw_parts_mut slices are provably sound
                // regardless of the allocator's exact layout choices.
                let stride = linesize[i].max(0) as usize;
                let extent = stride
                    .checked_mul(plane_rows[i])
                    .and_then(|span| offset.checked_add(span))
                    .ok_or_else(|| format!("Output pool template plane {i} extent overflow"))?;
                if extent > pool_buffer_size {
                    return Err(format!(
                        "Output pool template plane {i} extent {extent} exceeds buffer size \
                         {pool_buffer_size}"
                    ));
                }
                *off = offset;
            }
            OutputFrameLayout {
                pool_buffer_size,
                linesize,
                plane_offset,
            }
        };

        // SAFETY: passing the default alloc callback (None) makes the pool
        // thread-safe per FFmpeg's contract; size came from the template.
        let pool = unsafe { av_buffer_pool_init(layout.pool_buffer_size, None) };
        let pool =
            NonNull::new(pool).ok_or_else(|| "av_buffer_pool_init returned null".to_string())?;
        Ok(OutputFramePool { key, layout, pool })
    }

    /// Geometry this pool is keyed on. The filter rebuilds the pool when the
    /// current geometry no longer matches.
    pub(crate) fn key(&self) -> OutputGeometry {
        self.key
    }

    /// Builds a YUV420P output frame from mapped readback bytes into a
    /// pool-owned buffer, copying frame properties (pts, color tags, ...) from
    /// `src`.
    pub(crate) fn build_frame(&self, mapped: &[u8], src: &Frame) -> Result<Frame, String> {
        let geo = &self.key;
        let out_w = geo.out_w as usize;
        let out_h = geo.out_h as usize;
        let out_cw = geo.out_w.div_ceil(2) as usize;
        let out_ch = geo.out_h.div_ceil(2) as usize;

        let y_end = geo.y_stride * out_h;
        let u_end = y_end + geo.c_stride * out_ch;
        let v_end = u_end + geo.c_stride * out_ch;
        if mapped.len() < v_end {
            return Err(format!(
                "Mapped readback too small: {} bytes, need {v_end}",
                mapped.len()
            ));
        }

        // SAFETY: `out` is freshly allocated. `buf[0]` is a pool buffer of
        // `pool_buffer_size` bytes laid out identically to the template
        // av_frame_get_buffer(4) produced, so every plane's `offset + stride *
        // rows` stays within it (the template padded height to 32, which
        // bounds each `stride * out_rows` slice below). `src` is only read
        // through FFmpeg's props-copy API. On any error before `out` is
        // returned, dropping `out` releases `buf[0]` back to the pool.
        unsafe {
            let mut out = Frame::empty();
            let p = out.as_mut_ptr();
            if p.is_null() {
                return Err("Failed to allocate output frame".to_string());
            }
            let buf = av_buffer_pool_get(self.pool.as_ptr());
            if buf.is_null() {
                return Err("av_buffer_pool_get returned null".to_string());
            }
            let base = (*buf).data;

            (*p).width = geo.out_w as i32;
            (*p).height = geo.out_h as i32;
            (*p).format = AVPixelFormat::AV_PIX_FMT_YUV420P as i32;
            (*p).buf[0] = buf;
            (*p).data[0] = base.add(self.layout.plane_offset[0]);
            (*p).data[1] = base.add(self.layout.plane_offset[1]);
            (*p).data[2] = base.add(self.layout.plane_offset[2]);
            (*p).linesize[0] = self.layout.linesize[0];
            (*p).linesize[1] = self.layout.linesize[1];
            (*p).linesize[2] = self.layout.linesize[2];

            let ret = av_frame_copy_props(p, src.as_ptr());
            if ret < 0 {
                return Err(format!("Failed to copy frame props: {}", av_err2str(ret)));
            }
            (*p).time_base = (*src.as_ptr()).time_base;
            // copy_props carries the SOURCE frame's tags, which is not enough:
            // the passes may have run with a different effective range than
            // the source advertises (J-format inputs normalize the pix_fmt).
            // The caller re-stamps `color_range` from the slot's effective
            // flag right after this frame is handed back — keep that stamp in
            // sync with any tag logic added here.

            let dst_y_stride = self.layout.linesize[0] as usize;
            let dst_u_stride = self.layout.linesize[1] as usize;
            let dst_v_stride = self.layout.linesize[2] as usize;
            let dst_y = std::slice::from_raw_parts_mut((*p).data[0], dst_y_stride * out_h);
            let dst_u = std::slice::from_raw_parts_mut((*p).data[1], dst_u_stride * out_ch);
            let dst_v = std::slice::from_raw_parts_mut((*p).data[2], dst_v_stride * out_ch);

            copy_plane(
                &mapped[..y_end],
                geo.y_stride,
                dst_y,
                dst_y_stride,
                out_w,
                out_h,
            );
            copy_plane(
                &mapped[y_end..u_end],
                geo.c_stride,
                dst_u,
                dst_u_stride,
                out_cw,
                out_ch,
            );
            copy_plane(
                &mapped[u_end..v_end],
                geo.c_stride,
                dst_v,
                dst_v_stride,
                out_cw,
                out_ch,
            );
            Ok(out)
        }
    }
}

impl Drop for OutputFramePool {
    fn drop(&mut self) {
        // SAFETY: `pool` is a valid pool from av_buffer_pool_init. uninit marks
        // it freeable; buffers still held by live frames keep the pool alive
        // internally and are freed when their last reference drops.
        unsafe {
            let mut p = self.pool.as_ptr();
            av_buffer_pool_uninit(&mut p);
        }
    }
}

/// Owner of a mapped staging buffer lent out inside a zero-copy output frame.
/// Dropped by the AVBuffer free callback when the last frame reference is
/// released (encoder thread, frame pool, ...): unmaps and returns the buffer
/// to the filter for reuse. The filter may already be gone by then — the
/// send failure just lets the buffer drop entirely, which wgpu handles from
/// any thread.
struct ZeroCopyHolder {
    staging: Option<StagingSlot>,
    recycle_tx: mpsc::Sender<StagingSlot>,
}

impl Drop for ZeroCopyHolder {
    fn drop(&mut self) {
        if let Some(staging) = self.staging.take() {
            staging.buffer.unmap();
            let _ = self.recycle_tx.send(staging);
        }
    }
}

/// AVBuffer free callback: reclaims the [`ZeroCopyHolder`] leaked into the
/// buffer's opaque pointer. Runs on whichever thread drops the last frame
/// reference.
unsafe extern "C" fn zero_copy_free(opaque: *mut libc::c_void, _data: *mut u8) {
    drop(Box::from_raw(opaque as *mut ZeroCopyHolder));
}

/// Builds a YUV420P output frame whose planes point directly into the mapped
/// staging buffer — no CPU copy. The buffer stays mapped and borrowed until
/// every reference to the frame is dropped; the AVBuffer free callback then
/// unmaps it and hands it back to the filter through `recycle_tx`.
///
/// The frame is marked read-only (`AV_BUFFER_FLAG_READONLY`): encoders only
/// read their input, and any FFmpeg code that needs to write goes through
/// `av_frame_make_writable`, which copies first.
pub(crate) fn build_output_frame_zero_copy(
    staging: StagingSlot,
    geo: &OutputGeometry,
    src: &Frame,
    recycle_tx: &mpsc::Sender<StagingSlot>,
) -> Result<Frame, String> {
    // The caller (complete_oldest_gpu) has already dropped a STALE cached
    // direct-pack bind group — a zero-copy frame can outlive the current
    // resources, and a stale bind would pin a since-rebuilt `out_view`
    // through it. A current-generation bind stays cached so the next use
    // of this slot skips the create_bind_group.

    let out_h = geo.out_h as usize;
    let out_ch = geo.out_h.div_ceil(2) as usize;
    let y_end = geo.y_stride * out_h;
    let u_end = y_end + geo.c_stride * out_ch;
    let v_end = u_end + geo.c_stride * out_ch;

    let mapped = staging.buffer.slice(..).get_mapped_range();
    if mapped.len() < v_end {
        drop(mapped);
        staging.buffer.unmap();
        let _ = recycle_tx.send(staging);
        return Err(format!("Mapped readback too small: need {v_end} bytes"));
    }
    let base = mapped.as_ptr() as *mut u8;
    drop(mapped);
    // `base` stays valid: the buffer remains mapped until the holder unmaps
    // it in the free callback, and `get_mapped_range` of a whole-buffer map
    // is stable across calls.

    let holder = Box::new(ZeroCopyHolder {
        staging: Some(staging),
        recycle_tx: recycle_tx.clone(),
    });

    // SAFETY: `out` is freshly allocated. The AVBuffer wraps the mapped
    // range; FFmpeg frees it exactly once through `zero_copy_free`, which
    // reclaims the holder. On the error path below the holder is still owned
    // by the Box and cleans up through its Drop.
    unsafe {
        let mut out = Frame::empty();
        let p = out.as_mut_ptr();
        if p.is_null() {
            return Err("Failed to allocate output frame".to_string());
        }
        (*p).width = geo.out_w as i32;
        (*p).height = geo.out_h as i32;
        (*p).format = AVPixelFormat::AV_PIX_FMT_YUV420P as i32;

        let holder_ptr = Box::into_raw(holder);
        let buf = av_buffer_create(
            base,
            v_end,
            Some(zero_copy_free),
            holder_ptr as *mut libc::c_void,
            AV_BUFFER_FLAG_READONLY,
        );
        if buf.is_null() {
            drop(Box::from_raw(holder_ptr));
            return Err("Failed to wrap readback buffer (av_buffer_create)".to_string());
        }
        (*p).buf[0] = buf;
        (*p).data[0] = base;
        (*p).data[1] = base.add(y_end);
        (*p).data[2] = base.add(u_end);
        (*p).linesize[0] = geo.y_stride as i32;
        (*p).linesize[1] = geo.c_stride as i32;
        (*p).linesize[2] = geo.c_stride as i32;

        let ret = av_frame_copy_props(p, src.as_ptr());
        if ret < 0 {
            // Dropping `out` releases buf[0] and thus the holder.
            return Err(format!("Failed to copy frame props: {}", av_err2str(ret)));
        }
        (*p).time_base = (*src.as_ptr()).time_base;
        Ok(out)
    }
}
