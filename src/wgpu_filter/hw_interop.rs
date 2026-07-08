//! Zero-copy import of decoder hardware frames (VAAPI via DRM PRIME dmabuf)
//! into wgpu textures on the Vulkan backend.
//!
//! The decoder's NV12 surface is exported as a dmabuf (`av_hwframe_map` to
//! `AV_PIX_FMT_DRM_PRIME`), its two planes are imported as one `R8Unorm` and
//! one `Rg8Unorm` `VkImage` over the same memory object, and both are wrapped
//! as `wgpu::Texture`s that bind to the existing NV12 convert pipeline. The
//! frame data never touches system memory.
//!
//! Everything here degrades gracefully: any unsupported piece (non-Vulkan
//! backend, missing device extensions, exotic descriptor layout, import
//! failure) makes the caller fall back to the `av_hwframe_transfer_data`
//! download path.
//!
//! Experimental by design: wgpu tracks imported textures as uninitialized,
//! so its first barrier transitions from `UNDEFINED`, which the Vulkan spec
//! allows to discard contents. On drivers where video dmabuf surfaces carry
//! no compression metadata (RADV, and ANV media surfaces without aux state)
//! that transition preserves texels in practice. Pixel correctness is
//! verified by an end-to-end test before trusting a new driver.

use ash::{khr, vk};
#[cfg(target_os = "linux")]
use ash::ext;
#[cfg(target_os = "linux")]
use log::info;
use log::warn;

/// DRM fourccs accepted for NV12-shaped exports (drm_fourcc.h values).
const DRM_FORMAT_R8: u32 = 0x2020_3852; // 'R8  '
const DRM_FORMAT_GR88: u32 = 0x3838_5247; // 'GR88'
const DRM_FORMAT_NV12: u32 = 0x3231_564E; // 'NV12'

/// Raw Vulkan handles needed to import dmabufs into the wgpu device.
/// All handles are non-owning clones of what wgpu-hal keeps alive; this
/// struct must not outlive the `wgpu::Device` it was created with (both live
/// in `GpuState`, dropped together).
#[cfg_attr(not(target_os = "linux"), allow(dead_code))]
pub(crate) struct HwVulkanInterop {
    ash_instance: ash::Instance,
    ash_device: ash::Device,
    physical_device: vk::PhysicalDevice,
    memfd: khr::external_memory_fd::Device,
    /// (format, modifier) pairs already proven importable, so steady-state
    /// frames skip the two per-plane driver queries. Never invalidated:
    /// physical-device format support cannot change at runtime.
    supported_modifiers: std::sync::Mutex<std::collections::HashSet<(i32, u64)>>,
}

/// One plane of a DRM PRIME NV12 frame.
#[derive(Clone, Copy, Debug)]
#[cfg_attr(not(target_os = "linux"), allow(dead_code))]
pub(crate) struct DrmPlane {
    pub(crate) offset: u64,
    pub(crate) pitch: u64,
}

/// Parsed single-object NV12 dmabuf descriptor.
#[derive(Clone, Copy, Debug)]
#[cfg_attr(not(target_os = "linux"), allow(dead_code))]
pub(crate) struct DrmNv12 {
    pub(crate) fd: std::os::raw::c_int,
    pub(crate) modifier: u64,
    pub(crate) y: DrmPlane,
    pub(crate) uv: DrmPlane,
}

/// Both planes imported as wgpu textures. Dropping these releases the
/// VkImages and their imported memory once the GPU is done with them (wgpu
/// defers destruction past in-flight submissions).
pub(crate) struct ImportedNv12 {
    pub(crate) tex_y: wgpu::Texture,
    pub(crate) tex_uv: wgpu::Texture,
}

/// Parses an `AVDRMFrameDescriptor` (from a mapped DRM_PRIME frame's
/// `data[0]`) into the single-object NV12 shape supported here.
///
/// # Safety
/// `desc` must point to a valid descriptor owned by a live frame.
pub(crate) unsafe fn parse_drm_nv12(
    desc: *const ffmpeg_sys_next::AVDRMFrameDescriptor,
) -> Result<DrmNv12, String> {
    let d = &*desc;
    if d.nb_objects != 1 {
        return Err(format!(
            "unsupported DRM frame: {} memory objects (want 1)",
            d.nb_objects
        ));
    }
    let object = &d.objects[0];

    // Two descriptor shapes describe the same NV12 surface: one layer per
    // plane (R8 + GR88, what Mesa exports with SEPARATE_LAYERS), or one
    // NV12 layer holding both planes (COMPOSED_LAYERS).
    let (y, uv) = if d.nb_layers == 2
        && d.layers[0].format == DRM_FORMAT_R8
        && d.layers[0].nb_planes == 1
        && d.layers[1].format == DRM_FORMAT_GR88
        && d.layers[1].nb_planes == 1
    {
        (d.layers[0].planes[0], d.layers[1].planes[0])
    } else if d.nb_layers == 1
        && d.layers[0].format == DRM_FORMAT_NV12
        && d.layers[0].nb_planes == 2
    {
        (d.layers[0].planes[0], d.layers[0].planes[1])
    } else {
        return Err(format!(
            "unsupported DRM frame layout: {} layers, first format 0x{:08x}",
            d.nb_layers, d.layers[0].format
        ));
    };

    let plane = |p: ffmpeg_sys_next::AVDRMPlaneDescriptor| -> Result<DrmPlane, String> {
        if p.object_index != 0 || p.offset < 0 || p.pitch <= 0 {
            return Err(format!(
                "unsupported DRM plane: object {} offset {} pitch {}",
                p.object_index, p.offset, p.pitch
            ));
        }
        Ok(DrmPlane {
            offset: p.offset as u64,
            pitch: p.pitch as u64,
        })
    };
    Ok(DrmNv12 {
        fd: object.fd,
        modifier: object.format_modifier,
        y: plane(y)?,
        uv: plane(uv)?,
    })
}

/// Attempts to open the adapter through wgpu-hal with the dmabuf-import
/// device extensions enabled. Returns `None` (with an info log) whenever the
/// backend or the driver cannot support it, so the caller can fall back to
/// the plain `request_device` path.
#[cfg(target_os = "linux")]
pub(crate) fn try_open_dmabuf_device(
    adapter: &wgpu::Adapter,
    device_desc: &wgpu::DeviceDescriptor,
) -> Option<(wgpu::Device, wgpu::Queue, HwVulkanInterop)> {
    if adapter.get_info().backend != wgpu::Backend::Vulkan {
        info!("hw zero-copy input: unavailable (non-Vulkan backend)");
        return None;
    }
    const EXTENSIONS: [&std::ffi::CStr; 3] = [
        khr::external_memory_fd::NAME,
        ext::external_memory_dma_buf::NAME,
        ext::image_drm_format_modifier::NAME,
    ];

    // SAFETY: the hal adapter guard is used only within this scope; the raw
    // handles cloned out of it stay valid for the wgpu Device's lifetime,
    // which bounds the lifetime of the returned interop (both in GpuState).
    unsafe {
        let open_device = {
            let hal_adapter = adapter.as_hal::<wgpu::hal::api::Vulkan>()?;
            let caps = hal_adapter.physical_device_capabilities();
            if let Some(missing) = EXTENSIONS.iter().find(|e| !caps.supports_extension(e)) {
                info!(
                    "hw zero-copy input: unavailable (missing {})",
                    missing.to_string_lossy()
                );
                return None;
            }
            hal_adapter
                .open_with_callback(
                    device_desc.required_features,
                    &device_desc.memory_hints,
                    Some(Box::new(|args| {
                        args.extensions.extend_from_slice(&EXTENSIONS);
                    })),
                )
                .map_err(|e| info!("hw zero-copy input: device open failed: {e:?}"))
                .ok()?
        };
        let ash_device = open_device.device.raw_device().clone();
        let (device, queue) = adapter
            .create_device_from_hal::<wgpu::hal::api::Vulkan>(open_device, device_desc)
            .map_err(|e| info!("hw zero-copy input: device wrap failed: {e}"))
            .ok()?;
        let (ash_instance, physical_device) = {
            let hal_adapter = adapter.as_hal::<wgpu::hal::api::Vulkan>()?;
            (
                hal_adapter.shared_instance().raw_instance().clone(),
                hal_adapter.raw_physical_device(),
            )
        };
        let memfd = khr::external_memory_fd::Device::new(&ash_instance, &ash_device);
        info!("hw zero-copy input: dmabuf import enabled");
        Some((
            device,
            queue,
            HwVulkanInterop {
                ash_instance,
                ash_device,
                physical_device,
                memfd,
                supported_modifiers: std::sync::Mutex::new(std::collections::HashSet::new()),
            },
        ))
    }
}

/// Non-Linux: the DRM PRIME dmabuf zero-copy path is a Linux/VAAPI feature.
/// Elsewhere the caller opens the wgpu device normally and hardware frames
/// take the `av_hwframe_transfer_data` download path.
#[cfg(not(target_os = "linux"))]
pub(crate) fn try_open_dmabuf_device(
    _adapter: &wgpu::Adapter,
    _device_desc: &wgpu::DeviceDescriptor,
) -> Option<(wgpu::Device, wgpu::Queue, HwVulkanInterop)> {
    None
}

// Only the Linux dmabuf import path (`import_plane`) dups fds; gated with it.
#[cfg(target_os = "linux")]
fn dup_fd(fd: std::os::raw::c_int) -> Result<std::os::raw::c_int, String> {
    // SAFETY: plain fcntl dup of a descriptor owned by the mapped frame.
    let dup = unsafe { libc::fcntl(fd, libc::F_DUPFD_CLOEXEC, 0) };
    if dup < 0 {
        return Err(format!(
            "dup of dmabuf fd failed: {}",
            std::io::Error::last_os_error()
        ));
    }
    Ok(dup)
}

/// Non-Linux stub: the dmabuf zero-copy path exists only on Linux, so the
/// interop is never constructed off Linux and this is never reached at
/// runtime; it exists so the shared call sites compile on every platform.
#[cfg(not(target_os = "linux"))]
impl HwVulkanInterop {
    pub(crate) fn import_nv12(
        &self,
        _device: &wgpu::Device,
        _drm: &DrmNv12,
        _w: u32,
        _h: u32,
    ) -> Result<ImportedNv12, String> {
        Err("dmabuf import is only supported on Linux".to_string())
    }
}

#[cfg(target_os = "linux")]
impl HwVulkanInterop {
    /// Imports both NV12 planes of a single-object dmabuf as wgpu textures.
    /// `w`/`h` are the frame's display dimensions.
    pub(crate) fn import_nv12(
        &self,
        device: &wgpu::Device,
        drm: &DrmNv12,
        w: u32,
        h: u32,
    ) -> Result<ImportedNv12, String> {
        let (cw, ch) = (w.div_ceil(2), h.div_ceil(2));
        let tex_y = self.import_plane(
            device,
            drm,
            drm.y,
            vk::Format::R8_UNORM,
            wgpu::TextureFormat::R8Unorm,
            w,
            h,
            "ez_hw_y",
        )?;
        let tex_uv = self.import_plane(
            device,
            drm,
            drm.uv,
            vk::Format::R8G8_UNORM,
            wgpu::TextureFormat::Rg8Unorm,
            cw,
            ch,
            "ez_hw_uv",
        )?;
        Ok(ImportedNv12 { tex_y, tex_uv })
    }

    /// Verifies driver support for (format, modifier, dmabuf import) via
    /// `vkGetPhysicalDeviceImageFormatProperties2`. Creating an image with an
    /// unsupported modifier is undefined behavior, not a clean error, so this
    /// check is mandatory before every create (cheap CPU-side query).
    fn check_modifier_support(&self, format: vk::Format, modifier: u64) -> Result<(), String> {
        let cache_key = (format.as_raw(), modifier);
        {
            let cache = self
                .supported_modifiers
                .lock()
                .unwrap_or_else(std::sync::PoisonError::into_inner);
            if cache.contains(&cache_key) {
                return Ok(());
            }
        }

        let mut modifier_info = vk::PhysicalDeviceImageDrmFormatModifierInfoEXT::default()
            .drm_format_modifier(modifier)
            .sharing_mode(vk::SharingMode::EXCLUSIVE);
        let mut external_info = vk::PhysicalDeviceExternalImageFormatInfo::default()
            .handle_type(vk::ExternalMemoryHandleTypeFlags::DMA_BUF_EXT);
        let format_info = vk::PhysicalDeviceImageFormatInfo2::default()
            .format(format)
            .ty(vk::ImageType::TYPE_2D)
            .tiling(vk::ImageTiling::DRM_FORMAT_MODIFIER_EXT)
            .usage(vk::ImageUsageFlags::SAMPLED)
            .push_next(&mut modifier_info)
            .push_next(&mut external_info);

        let mut external_props = vk::ExternalImageFormatProperties::default();
        let mut props = vk::ImageFormatProperties2::default().push_next(&mut external_props);

        // SAFETY: valid instance/physical device; plain capability query.
        unsafe {
            self.ash_instance
                .get_physical_device_image_format_properties2(
                    self.physical_device,
                    &format_info,
                    &mut props,
                )
                .map_err(|e| {
                    format!("driver rejects {format:?} with modifier 0x{modifier:016x}: {e}")
                })?;
        }
        if !external_props
            .external_memory_properties
            .external_memory_features
            .contains(vk::ExternalMemoryFeatureFlags::IMPORTABLE)
        {
            return Err(format!(
                "{format:?} with modifier 0x{modifier:016x} is not importable"
            ));
        }
        self.supported_modifiers
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .insert(cache_key);
        Ok(())
    }

    #[allow(clippy::too_many_arguments)]
    fn import_plane(
        &self,
        device: &wgpu::Device,
        drm: &DrmNv12,
        plane: DrmPlane,
        vk_format: vk::Format,
        wgpu_format: wgpu::TextureFormat,
        w: u32,
        h: u32,
        label: &'static str,
    ) -> Result<wgpu::Texture, String> {
        self.check_modifier_support(vk_format, drm.modifier)?;

        let ash_device = &self.ash_device;

        // SAFETY: standard dmabuf import sequence against live handles; every
        // failure path releases exactly what was created before it.
        let (image, memory) = unsafe {
            let plane_layouts = [vk::SubresourceLayout {
                offset: plane.offset,
                size: 0, // must be 0 for explicit modifier layouts
                row_pitch: plane.pitch,
                array_pitch: 0,
                depth_pitch: 0,
            }];
            let mut modifier_info = vk::ImageDrmFormatModifierExplicitCreateInfoEXT::default()
                .drm_format_modifier(drm.modifier)
                .plane_layouts(&plane_layouts);
            let mut external_info = vk::ExternalMemoryImageCreateInfo::default()
                .handle_types(vk::ExternalMemoryHandleTypeFlags::DMA_BUF_EXT);
            let image_info = vk::ImageCreateInfo::default()
                .image_type(vk::ImageType::TYPE_2D)
                .format(vk_format)
                .extent(vk::Extent3D {
                    width: w,
                    height: h,
                    depth: 1,
                })
                .mip_levels(1)
                .array_layers(1)
                .samples(vk::SampleCountFlags::TYPE_1)
                .tiling(vk::ImageTiling::DRM_FORMAT_MODIFIER_EXT)
                .usage(vk::ImageUsageFlags::SAMPLED)
                .sharing_mode(vk::SharingMode::EXCLUSIVE)
                .initial_layout(vk::ImageLayout::UNDEFINED)
                .push_next(&mut external_info)
                .push_next(&mut modifier_info);
            let image = ash_device
                .create_image(&image_info, None)
                .map_err(|e| format!("vkCreateImage(dmabuf) failed: {e}"))?;

            let reqs = ash_device.get_image_memory_requirements(image);
            let fd = match dup_fd(drm.fd) {
                Ok(fd) => fd,
                Err(e) => {
                    ash_device.destroy_image(image, None);
                    return Err(e);
                }
            };
            let mut fd_props = vk::MemoryFdPropertiesKHR::default();
            if let Err(e) = self.memfd.get_memory_fd_properties(
                vk::ExternalMemoryHandleTypeFlags::DMA_BUF_EXT,
                fd,
                &mut fd_props,
            ) {
                libc::close(fd);
                ash_device.destroy_image(image, None);
                return Err(format!("vkGetMemoryFdPropertiesKHR failed: {e}"));
            }
            let type_bits = reqs.memory_type_bits & fd_props.memory_type_bits;
            if type_bits == 0 {
                libc::close(fd);
                ash_device.destroy_image(image, None);
                return Err("no compatible memory type for dmabuf import".to_string());
            }
            let memory_type_index = type_bits.trailing_zeros();

            let mut import_info = vk::ImportMemoryFdInfoKHR::default()
                .handle_type(vk::ExternalMemoryHandleTypeFlags::DMA_BUF_EXT)
                .fd(fd);
            let mut dedicated_info = vk::MemoryDedicatedAllocateInfo::default().image(image);
            let alloc_info = vk::MemoryAllocateInfo::default()
                .allocation_size(reqs.size)
                .memory_type_index(memory_type_index)
                .push_next(&mut import_info)
                .push_next(&mut dedicated_info);
            // On success Vulkan owns `fd`; on failure it is still ours.
            let memory = match ash_device.allocate_memory(&alloc_info, None) {
                Ok(m) => m,
                Err(e) => {
                    libc::close(fd);
                    ash_device.destroy_image(image, None);
                    return Err(format!("vkAllocateMemory(dmabuf import) failed: {e}"));
                }
            };
            if let Err(e) = ash_device.bind_image_memory(image, memory, 0) {
                ash_device.free_memory(memory, None);
                ash_device.destroy_image(image, None);
                return Err(format!("vkBindImageMemory failed: {e}"));
            }
            (image, memory)
        };

        // Wrap for wgpu. The drop callback owns the raw handles and runs once
        // wgpu is done with the texture (deferred past in-flight work).
        let cleanup_device = ash_device.clone();
        let drop_callback = Box::new(move || {
            // SAFETY: handles created above and not destroyed elsewhere; wgpu
            // invokes this after GPU work using the texture has completed.
            unsafe {
                cleanup_device.destroy_image(image, None);
                cleanup_device.free_memory(memory, None);
            }
        });

        let size = wgpu::Extent3d {
            width: w,
            height: h,
            depth_or_array_layers: 1,
        };
        // SAFETY: the VkImage was created respecting this descriptor (2D,
        // one mip/layer, matching format/usage); the device was created via
        // create_device_from_hal on the same adapter.
        let texture = unsafe {
            let hal_texture = {
                let hal_device = device
                    .as_hal::<wgpu::hal::api::Vulkan>()
                    .ok_or("device is not a Vulkan hal device")?;
                hal_device.texture_from_raw(
                    image,
                    &wgpu::hal::TextureDescriptor {
                        label: Some(label),
                        size,
                        mip_level_count: 1,
                        sample_count: 1,
                        dimension: wgpu::TextureDimension::D2,
                        format: wgpu_format,
                        usage: wgpu::wgt::TextureUses::RESOURCE,
                        memory_flags: wgpu::hal::MemoryFlags::empty(),
                        view_formats: vec![],
                    },
                    Some(drop_callback),
                )
            };
            device.create_texture_from_hal::<wgpu::hal::api::Vulkan>(
                hal_texture,
                &wgpu::TextureDescriptor {
                    label: Some(label),
                    size,
                    mip_level_count: 1,
                    sample_count: 1,
                    dimension: wgpu::TextureDimension::D2,
                    format: wgpu_format,
                    usage: wgpu::TextureUsages::TEXTURE_BINDING,
                    view_formats: &[],
                },
            )
        };
        Ok(texture)
    }
}

/// Warns once per process about a hw import failure; later failures fall
/// back silently (the download path takes over, and per-frame logging would
/// only repeat the same reason).
pub(crate) fn warn_import_failed_once(reason: &str) {
    use std::sync::Once;
    static ONCE: Once = Once::new();
    ONCE.call_once(|| {
        warn!(
            "hw zero-copy input failed ({reason}); falling back to downloading \
             hardware frames to system memory"
        );
    });
}
