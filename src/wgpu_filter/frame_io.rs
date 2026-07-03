//! CPU<->GPU frame I/O: input format detection, plane access, per-frame
//! upload/encode/submit, and output frame construction from mapped bytes.

use crate::util::ffmpeg_utils::av_err2str;
use crate::util::frame_utils::copy_plane;
use crate::wgpu_filter::gpu_state::{GpuState, OutputGeometry};
use crate::wgpu_filter::hw_interop::{self, ImportedNv12};
use crate::wgpu_filter::params::SharedParams;
use ffmpeg_next::Frame;
use ffmpeg_sys_next::{
    av_buffer_create, av_frame_copy_props, av_frame_get_buffer, av_hwframe_map,
    av_hwframe_transfer_data, av_q2d, AVDRMFrameDescriptor, AVPixelFormat,
    AV_BUFFER_FLAG_READONLY, AV_HWFRAME_MAP_READ,
};
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
    Ok((unsafe { std::slice::from_raw_parts(data_ptr, len) }, linesize))
}

/// Uploads one validated input frame, encodes the convert/effect/pack passes
/// plus the copy into `staging`, submits, and registers the readback map.
/// Returns the submission index and the map-completion receiver.
///
/// Uploads use `queue.write_texture`: wgpu's internal staging belt is already
/// a persistently mapped ring with one CPU copy + one GPU copy per plane. A
/// hand-rolled MAP_WRITE ring buffer was measured slower on this path
/// (COPY_BYTES_PER_ROW_ALIGNMENT padding inflates the CPU copy for widths
/// that are not multiples of 256, plus an extra map_async round trip per
/// frame; 1080p upload 0.77 -> 0.95 ms/frame on RADV RENOIR).
pub(crate) fn upload_and_encode(
    gpu: &GpuState,
    frame: &Frame,
    layout: PlaneLayout,
    matrix_id: u32,
    full_range: bool,
    staging: &wgpu::Buffer,
    params: &SharedParams,
) -> Result<
    (
        wgpu::SubmissionIndex,
        mpsc::Receiver<Result<(), wgpu::BufferAsyncError>>,
    ),
    String,
> {
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
        gpu,
        gpu.convert_pipeline(layout),
        &res.convert_bind,
        frame,
        matrix_id,
        full_range,
        staging,
        params,
    )
}

/// Encodes the convert/effect/pack passes against an already-uploaded (or
/// imported) input bound by `convert_bind`, submits, and registers the
/// readback map. Shared tail of the software and hardware input paths.
#[allow(clippy::too_many_arguments)]
pub(crate) fn encode_and_submit(
    gpu: &GpuState,
    convert_pipeline: &wgpu::RenderPipeline,
    convert_bind: &wgpu::BindGroup,
    frame: &Frame,
    matrix_id: u32,
    full_range: bool,
    staging: &wgpu::Buffer,
    params: &SharedParams,
) -> Result<
    (
        wgpu::SubmissionIndex,
        mpsc::Receiver<Result<(), wgpu::BufferAsyncError>>,
    ),
    String,
> {
    let res = gpu.resources.as_ref().expect("resources ensured");

    // Per-frame uniforms. Queue writes are ordered on the queue timeline, so
    // sharing one uniform buffer across in-flight frames is race-free: the
    // write for frame N+1 executes after frame N's submission.
    let convert_data: [u32; 4] = [matrix_id, full_range as u32, 0, 0];
    gpu.queue
        .write_buffer(&gpu.convert_uniforms, 0, bytemuck::cast_slice(&convert_data));

    // SAFETY: reading scalar fields from a live, validated frame.
    let play_time = unsafe {
        let pts = (*frame.as_ptr()).pts;
        if pts == ffmpeg_sys_next::AV_NOPTS_VALUE {
            0.0
        } else {
            pts as f64 * av_q2d((*frame.as_ptr()).time_base)
        }
    };
    let ez_data: [f32; 4] = [play_time as f32, res.out_w as f32, res.out_h as f32, 0.0];
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

    let pack_data: [u32; 8] = [
        res.out_w,
        res.out_h,
        (res.y_stride / 4) as u32,
        (res.c_stride / 4) as u32,
        (res.y_stride / 4 * res.out_h as usize) as u32,
        (res.y_stride / 4 * res.out_h as usize + res.c_stride / 4 * res.out_h.div_ceil(2) as usize)
            as u32,
        matrix_id,
        full_range as u32,
    ];
    gpu.queue
        .write_buffer(&gpu.pack_uniforms, 0, bytemuck::cast_slice(&pack_data));

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

    {
        let mut pass = encoder.begin_render_pass(&wgpu::RenderPassDescriptor {
            label: Some("ez_convert_pass"),
            color_attachments: &[color_attachment(&res.intermediate_view)],
            depth_stencil_attachment: None,
            timestamp_writes: None,
            occlusion_query_set: None,
        });
        pass.set_pipeline(convert_pipeline);
        pass.set_bind_group(0, convert_bind, &[]);
        pass.draw(0..3, 0..1);
    }
    {
        let mut pass = encoder.begin_render_pass(&wgpu::RenderPassDescriptor {
            label: Some("ez_effect_pass"),
            color_attachments: &[color_attachment(&res.out_view)],
            depth_stencil_attachment: None,
            timestamp_writes: None,
            occlusion_query_set: None,
        });
        pass.set_pipeline(&gpu.effect_pipeline);
        pass.set_bind_group(0, &res.effect_bind0, &[]);
        pass.set_bind_group(1, &res.effect_bind1, &[]);
        pass.draw(0..3, 0..1);
    }
    // Direct-pack mode (unified memory): the pack pass writes the mappable
    // staging buffer itself, so there is no storage buffer and no copy. The
    // bind group is transient because staging rotates per frame.
    let direct_pack_bind = gpu
        .direct_pack
        .then(|| gpu.pack_bind_for(&res.out_view, staging));
    {
        let mut pass = encoder.begin_compute_pass(&wgpu::ComputePassDescriptor {
            label: Some("ez_pack_pass"),
            timestamp_writes: None,
        });
        pass.set_pipeline(&gpu.pack_pipeline);
        let pack_bind = direct_pack_bind
            .as_ref()
            .or(res.pack_bind.as_ref())
            .expect("copy-path resources always carry a cached pack bind");
        pass.set_bind_group(0, pack_bind, &[]);
        pass.dispatch_workgroups(res.out_w.div_ceil(64), res.out_h.div_ceil(16), 1);
    }
    if let Some(storage) = &res.storage {
        encoder.copy_buffer_to_buffer(storage, 0, staging, 0, res.buf_size);
    }

    let submission = gpu.queue.submit(Some(encoder.finish()));

    let (tx, rx) = mpsc::channel();
    staging.slice(..).map_async(wgpu::MapMode::Read, move |result| {
        let _ = tx.send(result);
    });
    Ok((submission, rx))
}

/// Builds a YUV420P output frame from mapped readback bytes, copying frame
/// properties (pts, color tags, ...) from `src`.
pub(crate) fn build_output_frame(
    mapped: &[u8],
    geo: &OutputGeometry,
    src: &Frame,
) -> Result<Frame, String> {
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

    // SAFETY: `out` is freshly allocated; av_frame_get_buffer sizes each plane
    // to at least linesize * rows, which bounds every slice below.
    // `src` is only read through FFmpeg's own props-copy API.
    unsafe {
        let mut out = Frame::empty();
        let p = out.as_mut_ptr();
        if p.is_null() {
            return Err("Failed to allocate output frame".to_string());
        }
        (*p).width = geo.out_w as i32;
        (*p).height = geo.out_h as i32;
        (*p).format = AVPixelFormat::AV_PIX_FMT_YUV420P as i32;
        // align=4 makes FFmpeg's linesizes equal to the GPU-side strides
        // (both round the 8-bit plane width up to 4), so every copy_plane
        // below takes its contiguous fast path. The copies still consult the
        // actual linesize, so a different allocator choice stays correct.
        let ret = av_frame_get_buffer(p, 4);
        if ret < 0 {
            return Err(format!(
                "Failed to allocate output frame buffer: {}",
                av_err2str(ret)
            ));
        }
        let ret = av_frame_copy_props(p, src.as_ptr());
        if ret < 0 {
            return Err(format!("Failed to copy frame props: {}", av_err2str(ret)));
        }
        (*p).time_base = (*src.as_ptr()).time_base;
        // The packed output is always 4:2:0 planar; color matrix/range tags
        // carried over by copy_props stay correct by construction of the
        // convert/pack passes.

        let dst_y_stride = (*p).linesize[0] as usize;
        let dst_u_stride = (*p).linesize[1] as usize;
        let dst_v_stride = (*p).linesize[2] as usize;
        let dst_y = std::slice::from_raw_parts_mut((*p).data[0], dst_y_stride * out_h);
        let dst_u = std::slice::from_raw_parts_mut((*p).data[1], dst_u_stride * out_ch);
        let dst_v = std::slice::from_raw_parts_mut((*p).data[2], dst_v_stride * out_ch);

        copy_plane(&mapped[..y_end], geo.y_stride, dst_y, dst_y_stride, out_w, out_h);
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

/// Owner of a mapped staging buffer lent out inside a zero-copy output frame.
/// Dropped by the AVBuffer free callback when the last frame reference is
/// released (encoder thread, frame pool, ...): unmaps and returns the buffer
/// to the filter for reuse. The filter may already be gone by then — the
/// send failure just lets the buffer drop entirely, which wgpu handles from
/// any thread.
struct ZeroCopyHolder {
    staging: Option<wgpu::Buffer>,
    buf_size: u64,
    recycle_tx: mpsc::Sender<(wgpu::Buffer, u64)>,
}

impl Drop for ZeroCopyHolder {
    fn drop(&mut self) {
        if let Some(staging) = self.staging.take() {
            staging.unmap();
            let _ = self.recycle_tx.send((staging, self.buf_size));
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
    staging: wgpu::Buffer,
    geo: &OutputGeometry,
    src: &Frame,
    recycle_tx: &mpsc::Sender<(wgpu::Buffer, u64)>,
) -> Result<Frame, String> {
    let out_h = geo.out_h as usize;
    let out_ch = geo.out_h.div_ceil(2) as usize;
    let y_end = geo.y_stride * out_h;
    let u_end = y_end + geo.c_stride * out_ch;
    let v_end = u_end + geo.c_stride * out_ch;

    let mapped = staging.slice(..).get_mapped_range();
    if mapped.len() < v_end {
        drop(mapped);
        staging.unmap();
        let _ = recycle_tx.send((staging, geo.buf_size));
        return Err(format!(
            "Mapped readback too small: need {v_end} bytes"
        ));
    }
    let base = mapped.as_ptr() as *mut u8;
    drop(mapped);
    // `base` stays valid: the buffer remains mapped until the holder unmaps
    // it in the free callback, and `get_mapped_range` of a whole-buffer map
    // is stable across calls.

    let holder = Box::new(ZeroCopyHolder {
        staging: Some(staging),
        buf_size: geo.buf_size,
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
