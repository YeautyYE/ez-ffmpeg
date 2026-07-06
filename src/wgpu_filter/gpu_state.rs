//! Device-level wgpu state (`GpuState`) created once at filter init, and
//! size/format-dependent resources (`FrameResources`) rebuilt only when the
//! input geometry changes.

use crate::wgpu_filter::frame_io::PlaneLayout;
use crate::wgpu_filter::hw_interop::{self, HwVulkanInterop};
use crate::wgpu_filter::shaders;
use log::info;

/// Output-plane geometry captured per submitted frame, so in-flight readbacks
/// stay valid even if `FrameResources` is rebuilt for a new input size while
/// they are still on the GPU. Also the pool key for the copy-path output
/// frame pool, hence `Eq`/`Hash`.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash)]
pub(crate) struct OutputGeometry {
    pub(crate) out_w: u32,
    pub(crate) out_h: u32,
    /// Byte stride (multiple of 4) of packed luma rows in the readback buffer.
    pub(crate) y_stride: usize,
    /// Byte stride (multiple of 4) of packed chroma rows in the readback buffer.
    pub(crate) c_stride: usize,
    pub(crate) buf_size: u64,
}

/// Device-level state created once in `init`.
pub(crate) struct GpuState {
    pub(crate) device: wgpu::Device,
    pub(crate) queue: wgpu::Queue,
    sampler: wgpu::Sampler,
    pub(crate) effect_pipeline: wgpu::RenderPipeline,
    effect_bgl0: wgpu::BindGroupLayout,
    effect_bgl1: wgpu::BindGroupLayout,
    pub(crate) pack_pipeline: wgpu::ComputePipeline,
    pack_bgl: wgpu::BindGroupLayout,
    convert_pipeline_planar: wgpu::RenderPipeline,
    convert_bgl_planar: wgpu::BindGroupLayout,
    convert_pipeline_nv12: wgpu::RenderPipeline,
    convert_bgl_nv12: wgpu::BindGroupLayout,
    pub(crate) ez_uniforms: wgpu::Buffer,
    pub(crate) params_buf: wgpu::Buffer,
    pub(crate) convert_uniforms: wgpu::Buffer,
    pub(crate) pack_uniforms: wgpu::Buffer,
    pub(crate) resources: Option<FrameResources>,
    /// Raw Vulkan handles for dmabuf import; present only when the device
    /// was opened with the external-memory extensions (hw zero-copy input).
    pub(crate) hw_interop: Option<HwVulkanInterop>,
    /// Unified-memory fast path: the pack pass writes the mappable staging
    /// buffer directly (MAPPABLE_PRIMARY_BUFFERS), skipping the storage
    /// buffer and its per-frame copy.
    pub(crate) direct_pack: bool,
    /// Bumped on every real `ensure_resources` rebuild. A cached direct-pack
    /// bind group records the generation it was built against; a mismatch
    /// means `out_view` was recreated and the bind group must be rebuilt.
    resource_generation: u64,
}

/// Size/format-dependent resources, recreated when the input geometry changes.
pub(crate) struct FrameResources {
    pub(crate) in_w: u32,
    pub(crate) in_h: u32,
    pub(crate) layout: PlaneLayout,
    pub(crate) out_w: u32,
    pub(crate) out_h: u32,
    pub(crate) tex_y: wgpu::Texture,
    pub(crate) tex_u: wgpu::Texture,
    pub(crate) tex_v: Option<wgpu::Texture>,
    pub(crate) intermediate_view: wgpu::TextureView,
    pub(crate) out_view: wgpu::TextureView,
    pub(crate) convert_bind: wgpu::BindGroup,
    pub(crate) effect_bind0: wgpu::BindGroup,
    pub(crate) effect_bind1: wgpu::BindGroup,
    /// Cached pack bind group targeting `storage`; `None` in direct-pack
    /// mode, where a transient bind group targets the frame's staging buffer.
    pub(crate) pack_bind: Option<wgpu::BindGroup>,
    /// Intermediate packed-YUV buffer copied into staging per frame; `None`
    /// in direct-pack mode (the pack pass writes staging directly).
    pub(crate) storage: Option<wgpu::Buffer>,
    /// Idle MAP_READ staging slots; one is taken per in-flight frame and
    /// returned (or dropped, after a geometry change) on completion.
    ///
    /// Uploads deliberately stay on `queue.write_texture`: wgpu's internal
    /// staging belt is already a persistently mapped ring with the same
    /// one-CPU-copy + one-GPU-copy cost. A hand-rolled MAP_WRITE ring was
    /// measured slower here (256-aligned row padding plus an extra map_async
    /// per frame; 1080p upload 0.77 -> 0.95 ms/frame on RADV RENOIR).
    pub(crate) staging_pool: Vec<StagingSlot>,
    pub(crate) y_stride: usize,
    pub(crate) c_stride: usize,
    pub(crate) buf_size: u64,
    /// Generation stamp of the `GpuState` at the time these resources were
    /// built; direct-pack bind groups compare against it to detect a stale
    /// `out_view`.
    pub(crate) resource_generation: u64,
}

impl FrameResources {
    pub(crate) fn geometry(&self) -> OutputGeometry {
        OutputGeometry {
            out_w: self.out_w,
            out_h: self.out_h,
            y_stride: self.y_stride,
            c_stride: self.c_stride,
            buf_size: self.buf_size,
        }
    }
}

/// A readback staging buffer paired with an optional cached direct-pack bind
/// group. In direct-pack mode the pack compute pass binds this buffer as its
/// storage target; caching the bind group avoids recreating it every frame.
/// The cache is guarded by the resource generation it was built against, so a
/// bind group targeting a since-rebuilt `out_view` is never reused.
pub(crate) struct StagingSlot {
    pub(crate) buffer: wgpu::Buffer,
    /// Buffer size in bytes; used to match a slot against the current geometry
    /// when recycling (keeps the check identical to the old `(buffer, size)`
    /// recycle tuple).
    pub(crate) buf_size: u64,
    direct_pack_bind: Option<CachedDirectPackBind>,
}

struct CachedDirectPackBind {
    resource_generation: u64,
    bind: wgpu::BindGroup,
}

impl StagingSlot {
    pub(crate) fn new(buffer: wgpu::Buffer, buf_size: u64) -> Self {
        StagingSlot {
            buffer,
            buf_size,
            direct_pack_bind: None,
        }
    }

    /// Returns the direct-pack bind group for this slot, (re)creating it when
    /// none is cached or the cached one predates the current resource
    /// `generation` (its `out_view` was recreated). Only called in direct-pack
    /// mode.
    pub(crate) fn ensure_direct_pack_bind(
        &mut self,
        gpu: &GpuState,
        out_view: &wgpu::TextureView,
        generation: u64,
    ) -> &wgpu::BindGroup {
        let stale = match &self.direct_pack_bind {
            Some(cached) => cached.resource_generation != generation,
            None => true,
        };
        if stale {
            self.direct_pack_bind = Some(CachedDirectPackBind {
                resource_generation: generation,
                bind: gpu.pack_bind_for(out_view, &self.buffer),
            });
        }
        &self
            .direct_pack_bind
            .as_ref()
            .expect("cache populated above")
            .bind
    }

    /// Drops any cached direct-pack bind group. Called before a slot is lent
    /// out in a zero-copy frame, so a long-lived downstream frame does not pin
    /// a stale `out_view` through the cached bind group.
    pub(crate) fn clear_direct_pack_bind(&mut self) {
        self.direct_pack_bind = None;
    }
}

const ALIGN_WORD: u32 = 4;

fn align4(v: u32) -> u32 {
    v.div_ceil(ALIGN_WORD) * ALIGN_WORD
}

/// Creates one MAP_READ readback staging buffer of `buf_size` bytes. With
/// `direct_pack` the pack compute pass writes it directly (STORAGE), else it
/// is the destination of the storage-buffer copy (COPY_DST).
pub(crate) fn create_staging(
    device: &wgpu::Device,
    buf_size: u64,
    direct_pack: bool,
) -> wgpu::Buffer {
    let usage = wgpu::BufferUsages::MAP_READ
        | if direct_pack {
            wgpu::BufferUsages::STORAGE
        } else {
            wgpu::BufferUsages::COPY_DST
        };
    device.create_buffer(&wgpu::BufferDescriptor {
        label: Some("ez_staging"),
        size: buf_size,
        usage,
        mapped_at_creation: false,
    })
}

fn render_pipeline(
    device: &wgpu::Device,
    label: &str,
    layout: &wgpu::PipelineLayout,
    vs: &wgpu::ShaderModule,
    fs: &wgpu::ShaderModule,
) -> wgpu::RenderPipeline {
    device.create_render_pipeline(&wgpu::RenderPipelineDescriptor {
        label: Some(label),
        layout: Some(layout),
        vertex: wgpu::VertexState {
            module: vs,
            entry_point: Some("vs_main"),
            compilation_options: Default::default(),
            buffers: &[],
        },
        fragment: Some(wgpu::FragmentState {
            module: fs,
            entry_point: Some("fs_main"),
            compilation_options: Default::default(),
            targets: &[Some(wgpu::ColorTargetState {
                format: wgpu::TextureFormat::Rgba8Unorm,
                blend: None,
                write_mask: wgpu::ColorWrites::ALL,
            })],
        }),
        primitive: wgpu::PrimitiveState::default(),
        depth_stencil: None,
        multisample: wgpu::MultisampleState::default(),
        multiview: None,
        cache: None,
    })
}

fn texture_bgl_entry(binding: u32) -> wgpu::BindGroupLayoutEntry {
    wgpu::BindGroupLayoutEntry {
        binding,
        visibility: wgpu::ShaderStages::FRAGMENT,
        ty: wgpu::BindingType::Texture {
            sample_type: wgpu::TextureSampleType::Float { filterable: true },
            view_dimension: wgpu::TextureViewDimension::D2,
            multisampled: false,
        },
        count: None,
    }
}

/// `min_binding_size` makes shader/buffer size mismatches fail at pipeline
/// creation (inside init's error scope, with WGSL diagnostics) instead of
/// panicking the pipeline thread on first submit.
fn uniform_bgl_entry(
    binding: u32,
    visibility: wgpu::ShaderStages,
    min_size: u64,
) -> wgpu::BindGroupLayoutEntry {
    wgpu::BindGroupLayoutEntry {
        binding,
        visibility,
        ty: wgpu::BindingType::Buffer {
            ty: wgpu::BufferBindingType::Uniform,
            has_dynamic_offset: false,
            min_binding_size: wgpu::BufferSize::new(min_size),
        },
        count: None,
    }
}

impl GpuState {
    pub(crate) fn new(
        user_fragment_shader: &str,
        params_len: usize,
        hw_input: bool,
    ) -> Result<Self, String> {
        // Prefer the primary backends: probing GL/EGL costs ~30-40ms of init
        // and spams warnings on headless boxes. Fall back to the full set so
        // GL-only machines keep working exactly as before.
        let mut instance = wgpu::Instance::new(&wgpu::InstanceDescriptor {
            backends: wgpu::Backends::PRIMARY,
            ..Default::default()
        });
        let request = wgpu::RequestAdapterOptions {
            power_preference: wgpu::PowerPreference::HighPerformance,
            force_fallback_adapter: false,
            compatible_surface: None,
        };
        let adapter = match pollster::block_on(instance.request_adapter(&request)) {
            Ok(adapter) => adapter,
            Err(_) => {
                instance = wgpu::Instance::new(&wgpu::InstanceDescriptor::default());
                pollster::block_on(instance.request_adapter(&request))
                    .map_err(|e| format!("No suitable GPU adapter found: {e}"))?
            }
        };

        let adapter_info = adapter.get_info();
        info!(
            "WgpuFrameFilter adapter: {} ({:?}, {:?})",
            adapter_info.name, adapter_info.backend, adapter_info.device_type
        );

        // On unified-memory GPUs the pack pass can write straight into the
        // mappable readback buffer, skipping a full copy of the packed frame.
        // Discrete GPUs keep the copy: mappable memory is slow to write over
        // PCIe there. EZ_WGPU_DISABLE_DIRECT_PACK=1 forces the copy path
        // (internal A/B and fallback-coverage knob).
        let direct_pack = adapter_info.device_type == wgpu::DeviceType::IntegratedGpu
            && adapter
                .features()
                .contains(wgpu::Features::MAPPABLE_PRIMARY_BUFFERS)
            && std::env::var("EZ_WGPU_DISABLE_DIRECT_PACK").as_deref() != Ok("1");
        let device_desc = wgpu::DeviceDescriptor {
            required_features: if direct_pack {
                wgpu::Features::MAPPABLE_PRIMARY_BUFFERS
            } else {
                wgpu::Features::empty()
            },
            ..Default::default()
        };
        if direct_pack {
            info!("WgpuFrameFilter: direct pack readback enabled (unified memory)");
        }

        // Zero-copy hardware input needs the device opened with dmabuf-import
        // extensions; when that is not possible the filter still works, hw
        // frames just take the download path.
        let (device, queue, hw_interop) = match hw_input {
            true => match hw_interop::try_open_dmabuf_device(&adapter, &device_desc) {
                Some((device, queue, interop)) => (device, queue, Some(interop)),
                None => {
                    let (device, queue) =
                        pollster::block_on(adapter.request_device(&device_desc))
                            .map_err(|e| format!("Failed to create wgpu device: {e}"))?;
                    (device, queue, None)
                }
            },
            false => {
                let (device, queue) = pollster::block_on(adapter.request_device(&device_desc))
                    .map_err(|e| format!("Failed to create wgpu device: {e}"))?;
                (device, queue, None)
            }
        };

        let vs = device.create_shader_module(wgpu::ShaderModuleDescriptor {
            label: Some("ez_fullscreen_vs"),
            source: wgpu::ShaderSource::Wgsl(shaders::FULLSCREEN_VS.into()),
        });

        // Shader compilation errors surface through the device error scope so
        // users get the WGSL diagnostics instead of a later opaque failure.
        device.push_error_scope(wgpu::ErrorFilter::Validation);
        let user_fs = device.create_shader_module(wgpu::ShaderModuleDescriptor {
            label: Some("ez_user_effect_fs"),
            source: wgpu::ShaderSource::Wgsl(user_fragment_shader.into()),
        });
        if let Some(err) = pollster::block_on(device.pop_error_scope()) {
            return Err(format!("Effect shader compilation failed: {err}"));
        }

        let convert_fs_planar = device.create_shader_module(wgpu::ShaderModuleDescriptor {
            label: Some("ez_convert_planar_fs"),
            source: wgpu::ShaderSource::Wgsl(shaders::convert_fs_planar().into()),
        });
        let convert_fs_nv12 = device.create_shader_module(wgpu::ShaderModuleDescriptor {
            label: Some("ez_convert_nv12_fs"),
            source: wgpu::ShaderSource::Wgsl(shaders::convert_fs_nv12().into()),
        });
        let pack_cs = device.create_shader_module(wgpu::ShaderModuleDescriptor {
            label: Some("ez_pack_cs"),
            source: wgpu::ShaderSource::Wgsl(shaders::PACK_CS.into()),
        });

        let sampler = device.create_sampler(&wgpu::SamplerDescriptor {
            label: Some("ez_sampler"),
            address_mode_u: wgpu::AddressMode::ClampToEdge,
            address_mode_v: wgpu::AddressMode::ClampToEdge,
            mag_filter: wgpu::FilterMode::Linear,
            min_filter: wgpu::FilterMode::Linear,
            ..Default::default()
        });

        let params_size = (params_len.max(16) as u64).div_ceil(16) * 16;

        let sampler_entry = wgpu::BindGroupLayoutEntry {
            binding: 3,
            visibility: wgpu::ShaderStages::FRAGMENT,
            ty: wgpu::BindingType::Sampler(wgpu::SamplerBindingType::Filtering),
            count: None,
        };

        let convert_bgl_planar = device.create_bind_group_layout(&wgpu::BindGroupLayoutDescriptor {
            label: Some("ez_convert_planar_bgl"),
            entries: &[
                texture_bgl_entry(0),
                texture_bgl_entry(1),
                texture_bgl_entry(2),
                sampler_entry,
                uniform_bgl_entry(4, wgpu::ShaderStages::FRAGMENT, 16),
            ],
        });
        let convert_bgl_nv12 = device.create_bind_group_layout(&wgpu::BindGroupLayoutDescriptor {
            label: Some("ez_convert_nv12_bgl"),
            entries: &[
                texture_bgl_entry(0),
                texture_bgl_entry(1),
                sampler_entry,
                uniform_bgl_entry(4, wgpu::ShaderStages::FRAGMENT, 16),
            ],
        });

        let effect_bgl0 = device.create_bind_group_layout(&wgpu::BindGroupLayoutDescriptor {
            label: Some("ez_effect_bgl0"),
            entries: &[
                texture_bgl_entry(0),
                wgpu::BindGroupLayoutEntry {
                    binding: 1,
                    visibility: wgpu::ShaderStages::FRAGMENT,
                    ty: wgpu::BindingType::Sampler(wgpu::SamplerBindingType::Filtering),
                    count: None,
                },
                uniform_bgl_entry(2, wgpu::ShaderStages::FRAGMENT, 16),
            ],
        });
        let effect_bgl1 = device.create_bind_group_layout(&wgpu::BindGroupLayoutDescriptor {
            label: Some("ez_effect_bgl1"),
            entries: &[uniform_bgl_entry(0, wgpu::ShaderStages::FRAGMENT, params_size)],
        });

        let pack_bgl = device.create_bind_group_layout(&wgpu::BindGroupLayoutDescriptor {
            label: Some("ez_pack_bgl"),
            entries: &[
                wgpu::BindGroupLayoutEntry {
                    binding: 0,
                    visibility: wgpu::ShaderStages::COMPUTE,
                    ty: wgpu::BindingType::Texture {
                        sample_type: wgpu::TextureSampleType::Float { filterable: true },
                        view_dimension: wgpu::TextureViewDimension::D2,
                        multisampled: false,
                    },
                    count: None,
                },
                uniform_bgl_entry(1, wgpu::ShaderStages::COMPUTE, 32),
                wgpu::BindGroupLayoutEntry {
                    binding: 2,
                    visibility: wgpu::ShaderStages::COMPUTE,
                    ty: wgpu::BindingType::Buffer {
                        ty: wgpu::BufferBindingType::Storage { read_only: false },
                        has_dynamic_offset: false,
                        min_binding_size: None,
                    },
                    count: None,
                },
            ],
        });

        let convert_layout_planar =
            device.create_pipeline_layout(&wgpu::PipelineLayoutDescriptor {
                label: Some("ez_convert_planar_layout"),
                bind_group_layouts: &[&convert_bgl_planar],
                push_constant_ranges: &[],
            });
        let convert_layout_nv12 = device.create_pipeline_layout(&wgpu::PipelineLayoutDescriptor {
            label: Some("ez_convert_nv12_layout"),
            bind_group_layouts: &[&convert_bgl_nv12],
            push_constant_ranges: &[],
        });
        let effect_layout = device.create_pipeline_layout(&wgpu::PipelineLayoutDescriptor {
            label: Some("ez_effect_layout"),
            bind_group_layouts: &[&effect_bgl0, &effect_bgl1],
            push_constant_ranges: &[],
        });
        let pack_layout = device.create_pipeline_layout(&wgpu::PipelineLayoutDescriptor {
            label: Some("ez_pack_layout"),
            bind_group_layouts: &[&pack_bgl],
            push_constant_ranges: &[],
        });

        let convert_pipeline_planar = render_pipeline(
            &device,
            "ez_convert_planar",
            &convert_layout_planar,
            &vs,
            &convert_fs_planar,
        );
        let convert_pipeline_nv12 = render_pipeline(
            &device,
            "ez_convert_nv12",
            &convert_layout_nv12,
            &vs,
            &convert_fs_nv12,
        );

        device.push_error_scope(wgpu::ErrorFilter::Validation);
        let effect_pipeline = render_pipeline(&device, "ez_effect", &effect_layout, &vs, &user_fs);
        if let Some(err) = pollster::block_on(device.pop_error_scope()) {
            return Err(format!(
                "Effect pipeline creation failed (does the shader match the \
                 documented binding contract?): {err}"
            ));
        }

        let pack_pipeline = device.create_compute_pipeline(&wgpu::ComputePipelineDescriptor {
            label: Some("ez_pack"),
            layout: Some(&pack_layout),
            module: &pack_cs,
            entry_point: Some("cs_main"),
            compilation_options: Default::default(),
            cache: None,
        });

        let uniform_buf = |label: &str, size: u64| {
            device.create_buffer(&wgpu::BufferDescriptor {
                label: Some(label),
                size,
                usage: wgpu::BufferUsages::UNIFORM | wgpu::BufferUsages::COPY_DST,
                mapped_at_creation: false,
            })
        };
        let ez_uniforms = uniform_buf("ez_uniforms", 16);
        let params_buf = uniform_buf("ez_user_params", params_size);
        let convert_uniforms = uniform_buf("ez_convert_uniforms", 16);
        let pack_uniforms = uniform_buf("ez_pack_uniforms", 32);

        Ok(GpuState {
            device,
            queue,
            sampler,
            effect_pipeline,
            effect_bgl0,
            effect_bgl1,
            pack_pipeline,
            pack_bgl,
            convert_pipeline_planar,
            convert_bgl_planar,
            convert_pipeline_nv12,
            convert_bgl_nv12,
            ez_uniforms,
            params_buf,
            convert_uniforms,
            pack_uniforms,
            resources: None,
            hw_interop,
            direct_pack,
            resource_generation: 0,
        })
    }

    pub(crate) fn convert_pipeline(&self, layout: PlaneLayout) -> &wgpu::RenderPipeline {
        match layout {
            PlaneLayout::Planar { .. } => &self.convert_pipeline_planar,
            PlaneLayout::Nv12 => &self.convert_pipeline_nv12,
        }
    }

    /// Builds a pack bind group targeting `out_buf` (the cached storage
    /// buffer, or a frame's staging buffer in direct-pack mode).
    pub(crate) fn pack_bind_for(
        &self,
        out_view: &wgpu::TextureView,
        out_buf: &wgpu::Buffer,
    ) -> wgpu::BindGroup {
        self.device.create_bind_group(&wgpu::BindGroupDescriptor {
            label: Some("ez_pack_bind"),
            layout: &self.pack_bgl,
            entries: &[
                wgpu::BindGroupEntry {
                    binding: 0,
                    resource: wgpu::BindingResource::TextureView(out_view),
                },
                wgpu::BindGroupEntry {
                    binding: 1,
                    resource: self.pack_uniforms.as_entire_binding(),
                },
                wgpu::BindGroupEntry {
                    binding: 2,
                    resource: out_buf.as_entire_binding(),
                },
            ],
        })
    }

    /// Builds an NV12 convert bind group for a pair of imported hardware
    /// plane textures (per-frame: each hw frame is a distinct VkImage).
    pub(crate) fn hw_convert_bind(
        &self,
        tex_y: &wgpu::Texture,
        tex_uv: &wgpu::Texture,
    ) -> wgpu::BindGroup {
        let y_view = tex_y.create_view(&wgpu::TextureViewDescriptor::default());
        let uv_view = tex_uv.create_view(&wgpu::TextureViewDescriptor::default());
        self.device.create_bind_group(&wgpu::BindGroupDescriptor {
            label: Some("ez_hw_convert_bind"),
            layout: &self.convert_bgl_nv12,
            entries: &[
                wgpu::BindGroupEntry {
                    binding: 0,
                    resource: wgpu::BindingResource::TextureView(&y_view),
                },
                wgpu::BindGroupEntry {
                    binding: 1,
                    resource: wgpu::BindingResource::TextureView(&uv_view),
                },
                wgpu::BindGroupEntry {
                    binding: 3,
                    resource: wgpu::BindingResource::Sampler(&self.sampler),
                },
                wgpu::BindGroupEntry {
                    binding: 4,
                    resource: self.convert_uniforms.as_entire_binding(),
                },
            ],
        })
    }

    /// (Re)creates size/format-dependent resources. In-flight readbacks keep
    /// their own staging buffer and geometry snapshot, so rebuilding here is
    /// safe while older frames are still on the GPU.
    pub(crate) fn ensure_resources(
        &mut self,
        in_w: u32,
        in_h: u32,
        layout: PlaneLayout,
        out_size: Option<(u32, u32)>,
        staging_count: usize,
    ) {
        let (out_w, out_h) = out_size.unwrap_or((in_w, in_h));
        if let Some(res) = &self.resources {
            if res.in_w == in_w && res.in_h == in_h && res.layout == layout {
                return;
            }
        }

        // A real rebuild recreates `out_view` (and every texture/bind group),
        // so bump the generation to invalidate any cached direct-pack bind
        // group that still targets the previous `out_view`.
        self.resource_generation = self.resource_generation.wrapping_add(1);
        let resource_generation = self.resource_generation;

        let device = &self.device;
        let create_tex = |label: &str,
                          w: u32,
                          h: u32,
                          fmt: wgpu::TextureFormat,
                          usage: wgpu::TextureUsages| {
            device.create_texture(&wgpu::TextureDescriptor {
                label: Some(label),
                size: wgpu::Extent3d {
                    width: w,
                    height: h,
                    depth_or_array_layers: 1,
                },
                mip_level_count: 1,
                sample_count: 1,
                dimension: wgpu::TextureDimension::D2,
                format: fmt,
                usage,
                view_formats: &[],
            })
        };

        let (cw, ch) = layout.chroma_size(in_w, in_h);
        let upload = wgpu::TextureUsages::TEXTURE_BINDING | wgpu::TextureUsages::COPY_DST;

        let tex_y = create_tex("ez_y", in_w, in_h, wgpu::TextureFormat::R8Unorm, upload);
        let (tex_u, tex_v) = match layout {
            PlaneLayout::Planar { .. } => (
                create_tex("ez_u", cw, ch, wgpu::TextureFormat::R8Unorm, upload),
                Some(create_tex("ez_v", cw, ch, wgpu::TextureFormat::R8Unorm, upload)),
            ),
            PlaneLayout::Nv12 => (
                create_tex("ez_uv", cw, ch, wgpu::TextureFormat::Rg8Unorm, upload),
                None,
            ),
        };

        let intermediate = create_tex(
            "ez_intermediate",
            in_w,
            in_h,
            wgpu::TextureFormat::Rgba8Unorm,
            wgpu::TextureUsages::RENDER_ATTACHMENT | wgpu::TextureUsages::TEXTURE_BINDING,
        );
        let out_tex = create_tex(
            "ez_out",
            out_w,
            out_h,
            wgpu::TextureFormat::Rgba8Unorm,
            wgpu::TextureUsages::RENDER_ATTACHMENT | wgpu::TextureUsages::TEXTURE_BINDING,
        );

        let y_stride = align4(out_w) as usize;
        let c_stride = align4(out_w.div_ceil(2)) as usize;
        let out_ch = out_h.div_ceil(2) as usize;
        let y_words = y_stride / 4 * out_h as usize;
        let c_words = c_stride / 4 * out_ch;
        let total_words = y_words + 2 * c_words;
        let buf_size = (total_words * 4) as u64;

        let storage = (!self.direct_pack).then(|| {
            device.create_buffer(&wgpu::BufferDescriptor {
                label: Some("ez_pack_storage"),
                size: buf_size,
                usage: wgpu::BufferUsages::STORAGE | wgpu::BufferUsages::COPY_SRC,
                mapped_at_creation: false,
            })
        });
        let staging_pool: Vec<StagingSlot> = (0..staging_count)
            .map(|_| StagingSlot::new(create_staging(device, buf_size, self.direct_pack), buf_size))
            .collect();

        let view = |t: &wgpu::Texture| t.create_view(&wgpu::TextureViewDescriptor::default());
        let y_view = view(&tex_y);
        let u_view = view(&tex_u);
        let intermediate_view = view(&intermediate);
        let out_view = view(&out_tex);

        let convert_bind = match layout {
            PlaneLayout::Planar { .. } => {
                let v_view = view(tex_v.as_ref().expect("planar layout has a V texture"));
                device.create_bind_group(&wgpu::BindGroupDescriptor {
                    label: Some("ez_convert_bind"),
                    layout: &self.convert_bgl_planar,
                    entries: &[
                        wgpu::BindGroupEntry {
                            binding: 0,
                            resource: wgpu::BindingResource::TextureView(&y_view),
                        },
                        wgpu::BindGroupEntry {
                            binding: 1,
                            resource: wgpu::BindingResource::TextureView(&u_view),
                        },
                        wgpu::BindGroupEntry {
                            binding: 2,
                            resource: wgpu::BindingResource::TextureView(&v_view),
                        },
                        wgpu::BindGroupEntry {
                            binding: 3,
                            resource: wgpu::BindingResource::Sampler(&self.sampler),
                        },
                        wgpu::BindGroupEntry {
                            binding: 4,
                            resource: self.convert_uniforms.as_entire_binding(),
                        },
                    ],
                })
            }
            PlaneLayout::Nv12 => device.create_bind_group(&wgpu::BindGroupDescriptor {
                label: Some("ez_convert_bind"),
                layout: &self.convert_bgl_nv12,
                entries: &[
                    wgpu::BindGroupEntry {
                        binding: 0,
                        resource: wgpu::BindingResource::TextureView(&y_view),
                    },
                    wgpu::BindGroupEntry {
                        binding: 1,
                        resource: wgpu::BindingResource::TextureView(&u_view),
                    },
                    wgpu::BindGroupEntry {
                        binding: 3,
                        resource: wgpu::BindingResource::Sampler(&self.sampler),
                    },
                    wgpu::BindGroupEntry {
                        binding: 4,
                        resource: self.convert_uniforms.as_entire_binding(),
                    },
                ],
            }),
        };

        let effect_bind0 = device.create_bind_group(&wgpu::BindGroupDescriptor {
            label: Some("ez_effect_bind0"),
            layout: &self.effect_bgl0,
            entries: &[
                wgpu::BindGroupEntry {
                    binding: 0,
                    resource: wgpu::BindingResource::TextureView(&intermediate_view),
                },
                wgpu::BindGroupEntry {
                    binding: 1,
                    resource: wgpu::BindingResource::Sampler(&self.sampler),
                },
                wgpu::BindGroupEntry {
                    binding: 2,
                    resource: self.ez_uniforms.as_entire_binding(),
                },
            ],
        });
        let effect_bind1 = device.create_bind_group(&wgpu::BindGroupDescriptor {
            label: Some("ez_effect_bind1"),
            layout: &self.effect_bgl1,
            entries: &[wgpu::BindGroupEntry {
                binding: 0,
                resource: self.params_buf.as_entire_binding(),
            }],
        });
        let pack_bind = storage
            .as_ref()
            .map(|storage| self.pack_bind_for(&out_view, storage));

        self.resources = Some(FrameResources {
            in_w,
            in_h,
            layout,
            out_w,
            out_h,
            tex_y,
            tex_u,
            tex_v,
            intermediate_view,
            out_view,
            convert_bind,
            effect_bind0,
            effect_bind1,
            pack_bind,
            storage,
            staging_pool,
            y_stride,
            c_stride,
            buf_size,
            resource_generation,
        });
    }
}
