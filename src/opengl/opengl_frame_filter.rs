//! Deprecated OpenGL-based frame filtering. Superseded by
//! [`crate::wgpu_filter::WgpuFrameFilter`] (feature `wgpu`); see the
//! `opengl` module docs for the migration mapping.

// The deprecation targets downstream users; the module's own impls and tests
// legitimately keep using the type.
#![allow(deprecated)]

use crate::core::filter::frame_filter_context::FrameFilterContext;
use crate::filter::frame_filter::{FrameFilter, FrameFilterError, RequestFrameMode};
use crate::util::ffmpeg_utils::av_err2str;
use crate::util::frame_utils::ensure_software_format;
use ffmpeg_next::Frame;
use ffmpeg_sys_next::{
    av_frame_alloc, av_frame_copy_props, av_frame_free, av_frame_get_buffer, av_frame_is_writable,
    av_frame_make_writable, av_frame_unref, av_q2d, sws_scale, AVFrame, AVMediaType,
};
use glow::{HasContext, NativeProgram, PixelPackData, PixelUnpackData};
use log::{info, warn};
use surfman::{Connection, ContextAttributeFlags, ContextAttributes};

/// OpenGLFrameFilter: A struct to manage OpenGL-based frame filtering.
/// It allows custom shader setup, OpenGL initialization, and texture-based processing of video frames.
/// This is particularly useful for applying GPU-accelerated filters in video processing pipelines.
///
/// **Deprecated**: superseded by [`crate::wgpu_filter::WgpuFrameFilter`],
/// which runs headless, converts colors on the GPU with the correct matrix,
/// and overlaps GPU work with CPU work. This type remains functional but
/// will be removed in a future major release.
///
/// Context sharing: each instance owns its own GL context and re-binds it
/// (`make_context_current`) on every `filter_frame`/`uninit` entry, so
/// several instances — or another filter that switches the thread's GL
/// context — can coexist on one pipeline thread. GL object names are
/// per-context; without the re-bind a sibling's context would silently
/// resolve this filter's ids to its own objects.
#[deprecated(
    since = "0.11.0",
    note = "use `wgpu_filter::WgpuFrameFilter` (feature \"wgpu\") for custom GPU shaders, or \
            FFmpeg native hardware filters (scale_vaapi/scale_cuda/...) in filter_desc; the \
            OpenGL path requires a display connection and does CPU color conversion"
)]
pub struct OpenGLFrameFilter {
    /// GLSL vertex shader code provided by the user.
    vertex_shader_code: String,

    /// GLSL fragment shader code provided by the user.
    fragment_shader_code: String,

    /// Optional function for setting up vertex data (VAO, VBO, EBO).
    /// If None, a default setup is used.
    /// Default: Configures a quad with positions and texture coordinates using VAO, VBO, and EBO.
    setup_vertex_data_fn: Option<
        fn(
            &glow::Context,
        ) -> Result<
            (
                glow::NativeVertexArray,
                glow::NativeBuffer,
                glow::NativeBuffer,
            ),
            String,
        >,
    >,

    /// Optional function for setting uniforms (playTime, width, height).
    /// If None, a default implementation is used.
    /// Default: Uses `glUniform*` calls to set `playTime`, `width`, and `height` in the shader.
    set_uniforms_fn: Option<fn(&glow::Context, NativeProgram, &Frame) -> Result<(), String>>,

    /// Optional function for rendering the frame.
    /// If None, a default `glDrawElements`-based implementation is used.
    /// Default: Renders the quad using `glDrawElements` with `GL_TRIANGLES` mode.
    render_frame_fn: Option<fn(&glow::Context)>,

    // Surfman-specific objects for OpenGL context and device management.
    surfman_device: surfman::Device,
    surfman_context: surfman::Context,

    // OpenGL context and program.
    gl: Option<glow::Context>,
    program: Option<glow::Program>,

    // Vertex array object and buffers.
    vao: Option<glow::NativeVertexArray>,
    vbo: Option<glow::NativeBuffer>,
    ebo: Option<glow::NativeBuffer>,

    // Dimensions and pixel format the buffers, viewport and scalers were
    // built for; a mid-stream change forces a rebuild.
    width: i32,
    height: i32,
    format: ffmpeg_sys_next::AVPixelFormat,

    // Framebuffer and textures for rendering.
    framebuffer: Option<glow::Framebuffer>,
    output_texture: Option<glow::Texture>,
    input_texture: Option<glow::Texture>,

    // FFmpeg scaler contexts for converting to and from RGB.
    rgb_frame: Option<Frame>,
    to_rgb_scaler: Option<ffmpeg_next::software::scaling::Context>,
    to_original_scaler: Option<ffmpeg_next::software::scaling::Context>,
}

// SAFETY: the filter is constructed on the caller's thread but every GL
// operation happens on the single frame-pipeline thread: the surfman context
// is created without being made current (new_* -> init_surfman) and only
// init() — which runs on the pipeline thread — calls make_context_current.
// A context that was never current on the creating thread may be moved to
// and used on another thread under EGL/CGL rules. Drop may run elsewhere;
// surfman's destroy_context fails gracefully there (logged in Drop) rather
// than invoking UB. No Sync is implemented: &self is never shared across
// threads.
unsafe impl Send for OpenGLFrameFilter {}

impl OpenGLFrameFilter {
    /// Creates a new OpenGLFrameFilter with a default vertex shader and a custom fragment shader.
    ///
    /// Parameters:
    /// - `fragment_shader_code`: The GLSL code for the fragment shader. Must contain `in vec2 TexCoord` for texture coordinates.
    ///
    /// Returns:
    /// - `Ok(OpenGLFrameFilter)`: On successful initialization.
    /// - `Err(String)`: If the fragment shader does not contain the required texture coordinate variable.
    pub fn new_simple(fragment_shader_code: impl Into<String>) -> Result<Self, String> {
        // Default vertex shader code, assumes texture coordinates as input.
        let vertex_shader = r##"
            #version 330 core

            layout(location = 0) in vec3 aPosition;
            layout(location = 1) in vec2 aTexCoord;

            out vec2 TexCoord;  // Passes texture coordinates to the fragment shader

            void main() {
                gl_Position = vec4(aPosition, 1.0);
                TexCoord = aTexCoord;
            }
        "##;

        // Ensure the fragment shader contains the required texture coordinate variable.
        let fragment_shader_code = fragment_shader_code.into();
        if !fragment_shader_code.contains("in vec2 TexCoord;") {
            return Err(String::from(
                "Fragment shader code must contain a variable with 'in vec2 TexCoord;' for texture coordinates.",
            ));
        }

        // Delegate to `new_with_custom_shaders` with default vertex shader and no custom callbacks.
        Self::new_with_custom_shaders(3, 3, vertex_shader, fragment_shader_code, None, None, None)
    }

    /// Creates a new OpenGLFrameFilter with custom shaders and optional callback functions.
    ///
    /// Parameters:
    /// - `opengl_version_major`: Major version of the OpenGL context to create (e.g., 3 for OpenGL 3.x).
    /// - `opengl_version_minor`: Minor version of the OpenGL context to create (e.g., 3 for OpenGL 3.3).
    /// - `vertex_shader_code`: The GLSL code for the vertex shader.
    /// - `fragment_shader_code`: The GLSL code for the fragment shader.
    /// - `setup_vertex_data_fn`: Optional function to set up vertex data (VAO, VBO, EBO). Uses a default setup if None.
    ///   Default: Configures a quad with positions and texture coordinates using VAO, VBO, and EBO.
    /// - `set_uniforms_fn`: Optional function to set uniforms (e.g., playTime, width, height). Uses a default implementation if None.
    ///   Default: Uses `glUniform*` calls to set `playTime`, `width`, and `height` in the shader.
    /// - `render_frame_fn`: Optional function to render the frame. Uses a default implementation if None.
    ///   Default: Renders the quad using `glDrawElements` with `GL_TRIANGLES` mode.
    ///
    /// Returns:
    /// - `Ok(OpenGLFrameFilter)`: On successful initialization.
    /// - `Err(String)`: On failure (e.g., OpenGL context creation failure).
    pub fn new_with_custom_shaders(
        opengl_version_major: u8,
        opengl_version_minor: u8,
        vertex_shader_code: impl Into<String>,
        fragment_shader_code: impl Into<String>,
        setup_vertex_data_fn: Option<
            fn(
                &glow::Context,
            ) -> Result<
                (
                    glow::NativeVertexArray,
                    glow::NativeBuffer,
                    glow::NativeBuffer,
                ),
                String,
            >,
        >,
        set_uniforms_fn: Option<fn(&glow::Context, NativeProgram, &Frame) -> Result<(), String>>,
        render_frame_fn: Option<fn(&glow::Context)>,
    ) -> Result<Self, String> {
        // Initialize Surfman device and context.
        let (device, context) = Self::init_surfman(opengl_version_major, opengl_version_minor)?;

        // Return a new OpenGLFrameFilter instance with the specified parameters.
        Ok(OpenGLFrameFilter {
            vertex_shader_code: vertex_shader_code.into(),
            fragment_shader_code: fragment_shader_code.into(),
            setup_vertex_data_fn,
            set_uniforms_fn,
            render_frame_fn,
            surfman_device: device,
            surfman_context: context,
            gl: None,
            program: None,
            vao: None,
            vbo: None,
            ebo: None,
            width: -1,
            height: -1,
            format: ffmpeg_sys_next::AVPixelFormat::AV_PIX_FMT_NONE,
            framebuffer: None,
            output_texture: None,
            input_texture: None,
            rgb_frame: None,
            to_rgb_scaler: None,
            to_original_scaler: None,
        })
    }

    /// Initializes Surfman device and context for OpenGL rendering.
    ///
    /// Parameters:
    /// - `opengl_version_major`: Major version of OpenGL to request.
    /// - `opengl_version_minor`: Minor version of OpenGL to request.
    ///
    /// Returns:
    /// - `Ok((surfman::Device, surfman::Context))`: On success.
    /// - `Err(String)`: On failure (e.g., connection or context creation errors).
    fn init_surfman(
        opengl_version_major: u8,
        opengl_version_minor: u8,
    ) -> Result<(surfman::Device, surfman::Context), String> {
        // Create a Surfman connection.
        let connection = Connection::new()
            .map_err(|e| format!("Failed to create Surfman connection: {:?}", e))?;

        // Create an adapter for the current system's GPU.
        let adapter = connection
            .create_adapter()
            .map_err(|e| format!("Failed to create adapter: {:?}", e))?;

        // Create a device for OpenGL rendering.
        let mut device = connection
            .create_device(&adapter)
            .map_err(|e| format!("Failed to create device: {:?}", e))?;

        // Set up OpenGL context attributes.
        let context_attributes = ContextAttributes {
            version: surfman::GLVersion::new(opengl_version_major, opengl_version_minor),
            flags: ContextAttributeFlags::empty(),
        };

        // Create a context descriptor for the requested OpenGL version.
        let context_descriptor = device
            .create_context_descriptor(&context_attributes)
            .map_err(|e| format!("Failed to create context descriptor: {:?}", e))?;

        // Create the actual OpenGL context.
        let context = device
            .create_context(&context_descriptor, None)
            .map_err(|e| format!("Failed to create OpenGL context: {:?}", e))?;

        Ok((device, context))
    }

    /// Sets up and links the shader program for OpenGL.
    /// This compiles the vertex and fragment shaders, links them into a program, and stores the program in the struct.
    ///
    /// Returns:
    /// - `Ok(())`: If the program is successfully created and linked.
    /// - `Err(String)`: If shader compilation or program linking fails.
    fn setup_program(&mut self) -> Result<(), String> {
        let gl = self.gl.as_ref().unwrap();
        let vertex_shader =
            Self::compile_shader(gl, glow::VERTEX_SHADER, &self.vertex_shader_code)?;
        let fragment_shader =
            Self::compile_shader(gl, glow::FRAGMENT_SHADER, &self.fragment_shader_code)?;

        unsafe {
            let result = gl.create_program();
            if let Err(e) = result {
                return Err(format!("Failed to create program: {e}"));
            }
            let program = result.unwrap();

            gl.attach_shader(program, vertex_shader);
            gl.attach_shader(program, fragment_shader);
            gl.link_program(program);
            if !gl.get_program_link_status(program) {
                return Err(format!(
                    "Program link error: {}",
                    gl.get_program_info_log(program)
                ));
            }

            gl.delete_shader(vertex_shader);
            gl.delete_shader(fragment_shader);

            gl.use_program(Some(program));
            self.program = Some(program)
        }
        Ok(())
    }

    fn compile_shader(
        gl: &glow::Context,
        shader_type: u32,
        source: &str,
    ) -> Result<glow::Shader, String> {
        unsafe {
            let result = gl.create_shader(shader_type);
            if let Err(e) = result {
                return Err(format!("Failed to create shader: {e}"));
            }
            let shader = result.unwrap();

            gl.shader_source(shader, source);
            gl.compile_shader(shader);
            if !gl.get_shader_compile_status(shader) {
                return Err(format!(
                    "Shader compile error: {}",
                    gl.get_shader_info_log(shader)
                ));
            }

            Ok(shader)
        }
    }

    fn init_buffer(
        &mut self,
        width: i32,
        height: i32,
        format: ffmpeg_sys_next::AVPixelFormat,
    ) -> Result<(), String> {
        self.width = width;
        self.height = height;
        self.format = format;

        unsafe {
            self.gl.as_ref().unwrap().viewport(0, 0, width, height);
        }

        self.setup_framebuffer(width, height)?;

        self.setup_texture(width, height)?;

        self.setup_scaler(width, height, format)?;

        Ok(())
    }

    /// Makes this filter's GL context current on the calling thread. A
    /// sibling filter on the same pipeline thread (a second
    /// `OpenGLFrameFilter`, or a wgpu filter that fell back to wgpu's GL
    /// backend) may have switched contexts since our last call — GL names
    /// are per-context, so issuing calls against a sibling's context would
    /// silently resolve to its objects. Re-binding when already current is
    /// a cheap driver no-op.
    fn rebind_context(&self) -> Result<(), String> {
        self.surfman_device
            .make_context_current(&self.surfman_context)
            .map_err(|e| format!("Failed to make OpenGL context current: {:?}", e))
    }

    fn init_opengl(&mut self) -> Result<(), String> {
        self.rebind_context()?;

        let gl = unsafe {
            glow::Context::from_loader_function(|s| {
                self.surfman_device
                    .get_proc_address(&self.surfman_context, s)
            })
        };

        self.gl = Some(gl);

        Ok(())
    }

    fn setup_framebuffer(&mut self, width: i32, height: i32) -> Result<(), String> {
        let gl = self.gl.as_ref().unwrap();
        unsafe {
            let result = gl.create_framebuffer();
            if let Err(e) = result {
                return Err(format!("Failed to create framebuffer: {e}"));
            }
            let framebuffer = result.unwrap();
            gl.bind_framebuffer(glow::FRAMEBUFFER, Some(framebuffer));

            let result = gl.create_texture();
            if let Err(e) = result {
                gl.delete_framebuffer(framebuffer);
                return Err(format!("Failed to create texture for output: {e}"));
            }
            let output_texture = result.unwrap();

            gl.bind_texture(glow::TEXTURE_2D, Some(output_texture));
            gl.tex_image_2d(
                glow::TEXTURE_2D,
                0,
                glow::RGB8 as i32,
                width,
                height,
                0,
                glow::RGB,
                glow::UNSIGNED_BYTE,
                PixelUnpackData::Slice(None),
            );
            gl.tex_parameter_i32(
                glow::TEXTURE_2D,
                glow::TEXTURE_MIN_FILTER,
                glow::NEAREST as i32,
            );
            gl.tex_parameter_i32(
                glow::TEXTURE_2D,
                glow::TEXTURE_MAG_FILTER,
                glow::LINEAR as i32,
            );
            gl.bind_texture(glow::TEXTURE_2D, None);

            gl.framebuffer_texture_2d(
                glow::FRAMEBUFFER,
                glow::COLOR_ATTACHMENT0,
                glow::TEXTURE_2D,
                Some(output_texture),
                0,
            );

            if gl.check_framebuffer_status(glow::FRAMEBUFFER) != glow::FRAMEBUFFER_COMPLETE {
                gl.delete_framebuffer(framebuffer);
                gl.delete_texture(output_texture);
                return Err("Framebuffer is not complete!".to_string());
            }

            self.output_texture = Some(output_texture);
            self.framebuffer = Some(framebuffer);

            Ok(())
        }
    }

    fn setup_texture(&mut self, width: i32, height: i32) -> Result<(), String> {
        let gl = self.gl.as_ref().unwrap();
        unsafe {
            let result = gl.create_texture();
            if let Err(e) = result {
                return Err(format!("Failed to create texture for input: {e}"));
            }
            let tex = result.unwrap();
            gl.bind_texture(glow::TEXTURE_2D, Some(tex));
            gl.tex_image_2d(
                glow::TEXTURE_2D,
                0,
                glow::RGB8 as i32,
                width,
                height,
                0,
                glow::RGB,
                glow::UNSIGNED_BYTE,
                glow::PixelUnpackData::Slice(None),
            );
            gl.tex_parameter_i32(
                glow::TEXTURE_2D,
                glow::TEXTURE_WRAP_S,
                glow::CLAMP_TO_EDGE as i32,
            );
            gl.tex_parameter_i32(
                glow::TEXTURE_2D,
                glow::TEXTURE_WRAP_T,
                glow::CLAMP_TO_EDGE as i32,
            );
            gl.tex_parameter_i32(
                glow::TEXTURE_2D,
                glow::TEXTURE_MIN_FILTER,
                glow::NEAREST as i32,
            );
            gl.tex_parameter_i32(
                glow::TEXTURE_2D,
                glow::TEXTURE_MAG_FILTER,
                glow::LINEAR as i32,
            );

            self.input_texture = Some(tex);
        }

        Ok(())
    }

    fn setup_scaler(
        &mut self,
        width: i32,
        height: i32,
        format: ffmpeg_sys_next::AVPixelFormat,
    ) -> Result<(), String> {
        if format == ffmpeg_sys_next::AVPixelFormat::AV_PIX_FMT_RGB24 {
            return Ok(());
        }

        let mut rgb_frame = unsafe { Frame::empty() };
        unsafe {
            if rgb_frame.as_ptr().is_null() {
                return Err("Failed to create RGB frame: Out of memory.".to_string());
            }
            (*rgb_frame.as_mut_ptr()).width = width;
            (*rgb_frame.as_mut_ptr()).height = height;
            (*rgb_frame.as_mut_ptr()).format =
                ffmpeg_sys_next::AVPixelFormat::AV_PIX_FMT_RGB24 as i32;

            let ret = av_frame_get_buffer(rgb_frame.as_mut_ptr(), 0);
            if ret < 0 {
                return Err(format!(
                    "Failed to allocate buffer for RGB frame. {}",
                    av_err2str(ret)
                ));
            }
        }
        self.rgb_frame = Some(rgb_frame);

        let to_rgb_scaler = ffmpeg_next::software::scaling::Context::get(
            ffmpeg_next::format::Pixel::from(format),
            width as u32,
            height as u32,
            ffmpeg_next::format::Pixel::RGB24,
            width as u32,
            height as u32,
            ffmpeg_next::software::scaling::Flags::FAST_BILINEAR
                | ffmpeg_next::software::scaling::Flags::BITEXACT,
        )
        .map_err(|e| format!("Failed to create to_rgb_scaler: {:?}", e))?;
        let to_original_scaler = ffmpeg_next::software::scaling::Context::get(
            ffmpeg_next::format::Pixel::RGB24,
            width as u32,
            height as u32,
            ffmpeg_next::format::Pixel::from(format),
            width as u32,
            height as u32,
            ffmpeg_next::software::scaling::Flags::FAST_BILINEAR
                | ffmpeg_next::software::scaling::Flags::BITEXACT,
        )
        .map_err(|e| format!("Failed to create to_original_scaler: {:?}", e))?;

        self.to_rgb_scaler = Some(to_rgb_scaler);
        self.to_original_scaler = Some(to_original_scaler);

        Ok(())
    }

    /// Drops every resource tied to one (width, height, format): the GL
    /// textures/framebuffer plus the RGB staging frame and both scalers.
    /// Safe to call with nothing allocated (each slot is take()n).
    fn release_frame_resources(&mut self) {
        if let Some(gl) = self.gl.as_ref() {
            unsafe {
                if let Some(texture) = self.input_texture.take() {
                    gl.delete_texture(texture);
                }
                if let Some(texture) = self.output_texture.take() {
                    gl.delete_texture(texture);
                }
                if let Some(framebuffer) = self.framebuffer.take() {
                    gl.delete_framebuffer(framebuffer);
                }
            }
        }
        self.rgb_frame = None;
        self.to_rgb_scaler = None;
        self.to_original_scaler = None;
    }

    fn process_frame_through_texture(&self, frame: &mut Frame) -> Result<(), String> {
        self.upload_frame_to_texture(frame)?;

        let gl = self.gl.as_ref().unwrap();

        match self.set_uniforms_fn {
            None => set_uniforms(gl, self.program.clone().unwrap(), frame),
            Some(set_uniforms_fn) => set_uniforms_fn(gl, self.program.clone().unwrap(), frame),
        }?;

        match self.render_frame_fn {
            None => render_frame(gl),
            Some(render_frame_fn) => render_frame_fn(gl),
        }

        self.read_texture_to_frame(frame)?;
        Ok(())
    }

    fn upload_frame_to_texture(&self, frame: &Frame) -> Result<(), String> {
        let gl = self.gl.as_ref().unwrap();

        unsafe {
            let linesize = (*frame.as_ptr()).linesize[0];
            let data = frame_data(frame.as_ptr(), 0)?;

            gl.bind_texture(glow::TEXTURE_2D, self.input_texture);

            // GL defaults to 4-byte row alignment and tightly packed rows;
            // FFmpeg rows carry padding and width*3 is rarely 4-aligned, so
            // uploading with the defaults shears the picture sideways.
            gl.pixel_store_i32(glow::UNPACK_ALIGNMENT, 1);
            match row_layout(linesize, self.width, 3)? {
                RowLayout::Tight => {
                    gl.tex_sub_image_2d(
                        glow::TEXTURE_2D,
                        0,
                        0,
                        0,
                        self.width,
                        self.height,
                        glow::RGB,
                        glow::UNSIGNED_BYTE,
                        glow::PixelUnpackData::Slice(Some(data)),
                    );
                }
                RowLayout::RowLength(pixels) => {
                    gl.pixel_store_i32(glow::UNPACK_ROW_LENGTH, pixels);
                    gl.tex_sub_image_2d(
                        glow::TEXTURE_2D,
                        0,
                        0,
                        0,
                        self.width,
                        self.height,
                        glow::RGB,
                        glow::UNSIGNED_BYTE,
                        glow::PixelUnpackData::Slice(Some(data)),
                    );
                    gl.pixel_store_i32(glow::UNPACK_ROW_LENGTH, 0);
                }
                RowLayout::PerRow => {
                    // The stride is not a whole number of RGB pixels, which
                    // GL cannot express: upload row by row.
                    let stride = linesize as usize;
                    let row_bytes = self.width as usize * 3;
                    for y in 0..self.height {
                        let start = y as usize * stride;
                        gl.tex_sub_image_2d(
                            glow::TEXTURE_2D,
                            0,
                            0,
                            y,
                            self.width,
                            1,
                            glow::RGB,
                            glow::UNSIGNED_BYTE,
                            glow::PixelUnpackData::Slice(Some(&data[start..start + row_bytes])),
                        );
                    }
                }
            }
            Ok(())
        }
    }

    /// Reads the texture data from OpenGL and writes it to the specified AVFrame.
    fn read_texture_to_frame(&self, frame: &mut Frame) -> Result<(), String> {
        let gl = self.gl.as_ref().unwrap();

        unsafe {
            let linesize = (*frame.as_ptr()).linesize[0];
            let data = frame_data_mut(frame.as_mut_ptr(), 0)?;

            gl.bind_texture(glow::TEXTURE_2D, self.output_texture);

            // Mirror of the upload path: readback must honor the frame's
            // row stride or the written picture is sheared.
            gl.pixel_store_i32(glow::PACK_ALIGNMENT, 1);
            match row_layout(linesize, self.width, 3)? {
                RowLayout::Tight => {
                    gl.get_tex_image(
                        glow::TEXTURE_2D,
                        0,                   // Mipmap level
                        glow::RGB,           // Pixel format
                        glow::UNSIGNED_BYTE, // Data type
                        PixelPackData::Slice(Some(data)),
                    );
                }
                RowLayout::RowLength(pixels) => {
                    gl.pixel_store_i32(glow::PACK_ROW_LENGTH, pixels);
                    gl.get_tex_image(
                        glow::TEXTURE_2D,
                        0,
                        glow::RGB,
                        glow::UNSIGNED_BYTE,
                        PixelPackData::Slice(Some(data)),
                    );
                    gl.pixel_store_i32(glow::PACK_ROW_LENGTH, 0);
                }
                RowLayout::PerRow => {
                    // glGetTexImage fetches whole levels only: read tightly
                    // packed, then scatter the rows at the frame's stride.
                    let stride = linesize as usize;
                    let row_bytes = self.width as usize * 3;
                    let mut tight = vec![0u8; row_bytes * self.height as usize];
                    gl.get_tex_image(
                        glow::TEXTURE_2D,
                        0,
                        glow::RGB,
                        glow::UNSIGNED_BYTE,
                        PixelPackData::Slice(Some(&mut tight)),
                    );
                    for y in 0..self.height as usize {
                        data[y * stride..y * stride + row_bytes]
                            .copy_from_slice(&tight[y * row_bytes..(y + 1) * row_bytes]);
                    }
                }
            }
        }
        Ok(())
    }

    fn print_opengl_info(&self) {
        let gl = self.gl.as_ref().unwrap();
        unsafe {
            let version = gl.get_parameter_string(glow::VERSION);
            info!("OpenGL Version: {}", version);

            let shading_language_version = gl.get_parameter_string(glow::SHADING_LANGUAGE_VERSION);
            info!("GLSL Version: {}", shading_language_version);

            let renderer = gl.get_parameter_string(glow::RENDERER);
            let vendor = gl.get_parameter_string(glow::VENDOR);
            info!("OpenGL Renderer: {}", renderer);
            info!("OpenGL Vendor: {}", vendor);
        }
    }

    fn convert_to_rgb(&mut self, frame: &Frame) -> Result<(), String> {
        let rgb_frame = self.rgb_frame.as_mut().unwrap();
        unsafe {
            (*rgb_frame.as_mut_ptr()).pts = (*frame.as_ptr()).pts;
            (*rgb_frame.as_mut_ptr()).time_base = (*frame.as_ptr()).time_base;
        }

        unsafe {
            let ret = sws_scale(
                self.to_rgb_scaler.as_mut().unwrap().as_mut_ptr(),
                (*frame.as_ptr()).data.as_ptr() as *const *const _,
                (*frame.as_ptr()).linesize.as_ptr() as *const _,
                0,
                self.height,
                (*rgb_frame.as_mut_ptr()).data.as_ptr(),
                (*rgb_frame.as_mut_ptr()).linesize.as_ptr() as *mut _,
            );
            if ret <= 0 {
                return Err(format!("Failed to scale frame to rgb: {}", av_err2str(ret)));
            }
        }

        Ok(())
    }

    fn convert_from_rgb(&mut self, original_frame: &mut Frame) -> Result<(), String> {
        let rgb_frame = self.rgb_frame.as_ref().unwrap();
        unsafe {
            let ret = sws_scale(
                self.to_original_scaler.as_mut().unwrap().as_mut_ptr(),
                (*rgb_frame.as_ptr()).data.as_ptr() as *const *const _,
                (*rgb_frame.as_ptr()).linesize.as_ptr() as *const _,
                0,
                self.height,
                (*original_frame.as_mut_ptr()).data.as_ptr(),
                (*original_frame.as_mut_ptr()).linesize.as_ptr() as *mut _,
            );
            if ret <= 0 {
                return Err(format!("Failed to scale frame to rgb: {}", av_err2str(ret)));
            }
        }

        Ok(())
    }
}

/// Sets the `playTime`, `width`, and `height` uniform variables in the shader.
fn set_uniforms(gl: &glow::Context, program: NativeProgram, frame: &Frame) -> Result<(), String> {
    unsafe {
        // Get uniform locations
        let play_time_location = gl.get_uniform_location(program, "playTime");
        let width_location = gl.get_uniform_location(program, "width");
        let height_location = gl.get_uniform_location(program, "height");

        if let Some(location) = play_time_location {
            let pts = frame.pts().unwrap_or(0);
            let play_time = pts as f64 * av_q2d((*frame.as_ptr()).time_base);
            gl.uniform_1_f32(Some(&location), play_time as f32);
        }

        if let Some(location) = width_location {
            let width = (*frame.as_ptr()).width;
            gl.uniform_1_i32(Some(&location), width);
        }

        if let Some(location) = height_location {
            let height = (*frame.as_ptr()).height;
            gl.uniform_1_i32(Some(&location), height);
        }
    }

    Ok(())
}

fn render_frame(gl: &glow::Context) {
    unsafe {
        gl.draw_elements(glow::TRIANGLES, 6, glow::UNSIGNED_INT, 0);
    }
}

/// Sets up vertex and index data, and configures VAO, VBO, and EBO
fn setup_vertex_data(
    gl: &glow::Context,
) -> Result<
    (
        glow::NativeVertexArray,
        glow::NativeBuffer,
        glow::NativeBuffer,
    ),
    String,
> {
    // Vertex data and texture coordinates (quad)
    let vertices: [f32; 20] = [
        // Position       // Texture Coordinates
        -1.0, 1.0, 0.0, 0.0, 1.0, // Top-left corner
        -1.0, -1.0, 0.0, 0.0, 0.0, // Bottom-left corner
        1.0, -1.0, 0.0, 1.0, 0.0, // Bottom-right corner
        1.0, 1.0, 0.0, 1.0, 1.0, // Top-right corner
    ];

    // Index data (two triangles for the quad)
    let indices: [u32; 6] = [
        0, 1, 2, // First triangle
        0, 2, 3, // Second triangle
    ];

    unsafe {
        // Create and bind the VAO
        let result = gl.create_vertex_array();
        if let Err(e) = result {
            return Err(format!("Failed to create VAO: {e}"));
        }
        let vao = result.unwrap();
        gl.bind_vertex_array(Some(vao));

        // Create and bind the VBO
        let result = gl.create_buffer();
        if let Err(e) = result {
            return Err(format!("Failed to create VBO: {e}"));
        }
        let vbo = result.unwrap();
        gl.bind_buffer(glow::ARRAY_BUFFER, Some(vbo));

        // Upload vertex data to the VBO
        gl.buffer_data_u8_slice(
            glow::ARRAY_BUFFER,
            bytemuck::cast_slice(&vertices),
            glow::STATIC_DRAW,
        );

        // Create and bind the EBO (Element Buffer Object for indexed drawing)
        let result = gl.create_buffer();
        if let Err(e) = result {
            return Err(format!("Failed to create EBO: {e}"));
        }
        let ebo = result.unwrap();
        gl.bind_buffer(glow::ELEMENT_ARRAY_BUFFER, Some(ebo));

        // Upload index data to the EBO
        gl.buffer_data_u8_slice(
            glow::ELEMENT_ARRAY_BUFFER,
            bytemuck::cast_slice(&indices),
            glow::STATIC_DRAW,
        );

        // Position attribute
        gl.vertex_attrib_pointer_f32(
            0,                                     // Attribute location
            3,                                     // Number of components (vec3)
            glow::FLOAT,                           // Data type
            false,                                 // Normalized
            5 * std::mem::size_of::<f32>() as i32, // Stride
            0,                                     // Offset
        );
        gl.enable_vertex_attrib_array(0);

        // Texture coordinate attribute
        gl.vertex_attrib_pointer_f32(
            1,                                       // Attribute location
            2,                                       // Number of components (vec2)
            glow::FLOAT,                             // Data type
            false,                                   // Normalized
            5 * std::mem::size_of::<f32>() as i32,   // Stride
            (3 * std::mem::size_of::<f32>()) as i32, // Offset
        );
        gl.enable_vertex_attrib_array(1);

        Ok((vao, vbo, ebo))
    }
}

/// How a frame row maps onto GL's pixel-transfer parameters.
#[derive(Debug, PartialEq, Eq)]
enum RowLayout {
    /// stride == width * bytes_per_pixel: rows are tightly packed.
    Tight,
    /// Padded stride that is a whole number of pixels: GL ROW_LENGTH covers it.
    RowLength(i32),
    /// Stride is not a multiple of the pixel size: transfer row by row.
    PerRow,
}

fn row_layout(linesize: i32, width: i32, bytes_per_pixel: i32) -> Result<RowLayout, String> {
    let row_bytes = width * bytes_per_pixel;
    if linesize < row_bytes {
        // A stride shorter than one row of pixels cannot hold the picture;
        // slicing by it would run past the buffer end row by row.
        return Err(format!(
            "linesize {linesize} is shorter than a row of {width} pixels ({row_bytes} bytes)"
        ));
    }
    Ok(if linesize == row_bytes {
        RowLayout::Tight
    } else if linesize % bytes_per_pixel == 0 {
        RowLayout::RowLength(linesize / bytes_per_pixel)
    } else {
        RowLayout::PerRow
    })
}

/// Validate plane `index` of `frame` and return (data pointer, byte length).
unsafe fn frame_plane(frame: *const AVFrame, index: usize) -> Result<(*mut u8, usize), String> {
    if frame.is_null() {
        return Err("Frame pointer is null".to_owned());
    }
    if index >= (*frame).data.len() {
        return Err(format!("Plane index {} out of range", index));
    }

    let linesize = (*frame).linesize[index];
    if linesize <= 0 {
        // Negative linesize means a bottom-up frame layout, which this GL
        // path does not support; casting it to usize would slice far out of
        // bounds.
        return Err(format!("Invalid linesize {} at index {}", linesize, index));
    }
    let linesize = linesize as usize;

    let data_ptr = (*frame).data[index];
    if data_ptr.is_null() {
        return Err(format!("Data pointer at index {} is null", index));
    }

    let height = (*frame).height;
    if height <= 0 {
        return Err(format!("Invalid frame height {height}"));
    }

    // Rows in THIS plane, not the frame height: the chroma planes (1 and 2)
    // of a subsampled format hold AV_CEIL_RSHIFT(height, log2_chroma_h) rows,
    // so sizing every plane by the full height reaches past the chroma
    // allocations — an out-of-bounds read on upload and an out-of-bounds
    // WRITE on readback (libavutil imgutils parity).
    let rows = if index == 1 || index == 2 {
        let fmt = crate::util::format_convert::pix_fmt_from_raw((*frame).format)
            .ok_or_else(|| format!("Unknown pixel format {}", (*frame).format))?;
        let desc = ffmpeg_sys_next::av_pix_fmt_desc_get(fmt);
        if desc.is_null() {
            return Err(format!("No descriptor for pixel format {fmt:?}"));
        }
        let shift = (*desc).log2_chroma_h as usize;
        ((height as usize) + (1 << shift) - 1) >> shift
    } else {
        height as usize
    };

    Ok((data_ptr, linesize * rows))
}

/// Shared view of a frame plane, for reading pixels (GL upload).
///
/// # Safety
/// `frame` must point to a valid AVFrame whose plane buffers outlive the
/// returned slice, with no concurrent writer.
pub(crate) unsafe fn frame_data<'a>(
    frame: *const AVFrame,
    index: usize,
) -> Result<&'a [u8], String> {
    let (data_ptr, len) = frame_plane(frame, index)?;
    Ok(std::slice::from_raw_parts(data_ptr, len))
}

/// Exclusive view of a frame plane, for writing pixels (GL readback).
///
/// # Safety
/// `frame` must point to a valid, writable AVFrame (`av_frame_make_writable`)
/// with no other reference to the plane while the slice lives; taking
/// `*mut AVFrame` keeps the exclusivity requirement visible at the call site.
pub(crate) unsafe fn frame_data_mut<'a>(
    frame: *mut AVFrame,
    index: usize,
) -> Result<&'a mut [u8], String> {
    let (data_ptr, len) = frame_plane(frame, index)?;
    Ok(std::slice::from_raw_parts_mut(data_ptr, len))
}

impl FrameFilter for OpenGLFrameFilter {
    fn media_type(&self) -> AVMediaType {
        AVMediaType::AVMEDIA_TYPE_VIDEO
    }

    /// Synchronous: the GL round-trip completes inside `filter_frame`, which
    /// always returns the processed frame; nothing is deferred, so the pipeline
    /// need not poll this filter for produced frames (PERF-8).
    fn request_frame_mode(&self) -> RequestFrameMode {
        RequestFrameMode::Never
    }

    fn init(&mut self, _ctx: &mut FrameFilterContext) -> Result<(), FrameFilterError> {
        self.init_opengl()?;

        self.print_opengl_info();

        self.setup_program()?;

        let gl = self.gl.as_ref().unwrap();
        let (vao, vbo, ebo) = match self.setup_vertex_data_fn {
            None => setup_vertex_data(gl),
            Some(setup_vertex_data_fn) => setup_vertex_data_fn(gl),
        }?;

        self.vao = Some(vao);
        self.vbo = Some(vbo);
        self.ebo = Some(ebo);

        Ok(())
    }

    fn filter_frame(
        &mut self,
        mut frame: Frame,
        _ctx: &mut FrameFilterContext,
    ) -> Result<Option<Frame>, FrameFilterError> {
        if crate::util::ffmpeg_utils::frame_is_eof_marker(&frame) {
            return Ok(Some(frame));
        }

        // Validated conversion: a raw transmute of an arbitrary int into
        // AVPixelFormat is UB for invalid values, and hardware-frame formats
        // carry no CPU-readable data for the GL upload path.
        let original_format = ensure_software_format(unsafe { (*frame.as_ptr()).format })
            .map_err(|e| format!("OpenGLFrameFilter: {e}"))?;

        let (width, height) = unsafe { ((*frame.as_ptr()).width, (*frame.as_ptr()).height) };
        if width <= 0 || height <= 0 {
            return Ok(Some(frame));
        }

        // A sibling filter on this pipeline thread may own a different GL
        // context and have left it current; re-bind ours before any GL work.
        self.rebind_context()?;

        // Buffers, viewport and both sws contexts are sized for exactly one
        // (width, height, format). A mid-stream change (h264 resolution
        // switch, filter reconfiguration) must rebuild them: scaling into
        // the old geometry writes past the new frame's smaller buffers.
        if self.input_texture.is_none()
            || width != self.width
            || height != self.height
            || original_format != self.format
        {
            self.release_frame_resources();
            self.init_buffer(width, height, original_format)?;
        }

        if original_format == ffmpeg_sys_next::AVPixelFormat::AV_PIX_FMT_RGB24 {
            // The GL result is written back into this frame's own buffers,
            // which may still be shared with the decoder's frame pool
            // (refcount > 1) — here the pre-copy is real work, since only
            // the drawn region changes.
            unsafe {
                let ret = av_frame_make_writable(frame.as_mut_ptr());
                if ret < 0 {
                    return Err(
                        format!("Failed to make frame writable: {}", av_err2str(ret)).into(),
                    );
                }
            }
            self.process_frame_through_texture(&mut frame)?;
        } else {
            self.convert_to_rgb(&frame)?;

            // Take the scratch frame out so the exclusive borrow does not
            // overlap &self; put it back before propagating any error.
            let mut rgb_frame = self.rgb_frame.take().unwrap();
            let result = self.process_frame_through_texture(&mut rgb_frame);
            self.rgb_frame = Some(rgb_frame);
            result?;

            // convert_from_rgb overwrites every byte of the frame's planes,
            // so a shared frame needs fresh buffers, NOT make_writable's
            // full-plane deep copy of bytes that are then 100% overwritten
            // (~3 MB of dead memcpy per 1080p yuv420p frame).
            unsafe { ensure_overwritable(&mut frame)? };

            self.convert_from_rgb(&mut frame)?;
        }

        Ok(Some(frame))
    }

    fn uninit(&mut self, _ctx: &mut FrameFilterContext) {
        // Deleting against a sibling's context would free ITS objects (GL
        // names are per-context). If ours cannot be made current the
        // context is dead and Drop's destroy_context reclaims everything.
        if let Err(e) = self.rebind_context() {
            warn!("OpenGL uninit skipped GL deletes (context not current): {e}");
            return;
        }
        // init may have failed halfway and uninit may run twice: take() each
        // resource so missing ones are skipped and none is deleted twice.
        self.release_frame_resources();
        if let Some(gl) = self.gl.as_ref() {
            unsafe {
                if let Some(ebo) = self.ebo.take() {
                    gl.delete_buffer(ebo);
                }
                if let Some(vbo) = self.vbo.take() {
                    gl.delete_buffer(vbo);
                }
                if let Some(vao) = self.vao.take() {
                    gl.delete_vertex_array(vao);
                }
                if let Some(program) = self.program.take() {
                    gl.delete_program(program);
                }
            }
        }
    }
}

/// Gives `frame` exclusively-owned planes WITHOUT copying pixel data, for
/// callers about to overwrite every byte: `av_frame_make_writable` on a
/// shared frame allocates AND deep-copies the planes, and that copy is pure
/// waste when the very next operation overwrites them all. A frame that is
/// already writable is returned untouched (its planes are reused in place).
///
/// # Safety
/// `frame` must wrap a valid, non-null `AVFrame` describing video pixel
/// data (width/height/format set).
unsafe fn ensure_overwritable(frame: &mut Frame) -> Result<(), String> {
    if av_frame_is_writable(frame.as_mut_ptr()) != 0 {
        return Ok(());
    }
    let mut props = av_frame_alloc();
    if props.is_null() {
        return Err("Failed to allocate a frame props holder".to_string());
    }
    let result = (|| {
        let ret = av_frame_copy_props(props, frame.as_ptr());
        if ret < 0 {
            return Err(format!(
                "Failed to snapshot frame props: {}",
                av_err2str(ret)
            ));
        }
        let raw = frame.as_mut_ptr();
        let (width, height, format) = ((*raw).width, (*raw).height, (*raw).format);
        av_frame_unref(raw);
        (*raw).width = width;
        (*raw).height = height;
        (*raw).format = format;
        let ret = av_frame_get_buffer(raw, 0);
        if ret < 0 {
            return Err(format!(
                "Failed to allocate replacement planes: {}",
                av_err2str(ret)
            ));
        }
        let ret = av_frame_copy_props(raw, props);
        if ret < 0 {
            return Err(format!(
                "Failed to restore frame props: {}",
                av_err2str(ret)
            ));
        }
        Ok(())
    })();
    av_frame_free(&mut props);
    result
}

impl Drop for OpenGLFrameFilter {
    fn drop(&mut self) {
        if let Err(e) = self
            .surfman_device
            .destroy_context(&mut self.surfman_context)
        {
            warn!("Failed to destroy surfman context: {:?}", e);
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::core::context::ffmpeg_context::FfmpegContext;
    use crate::core::context::output::Output;
    use crate::core::scheduler::ffmpeg_scheduler::{FfmpegScheduler, Initialization};
    use crate::filter::frame_pipeline_builder::FramePipelineBuilder;
    use ffmpeg_sys_next::AVPixelFormat;

    #[test]
    fn test_ensure_software_format_accepts_cpu_formats() {
        assert!(ensure_software_format(AVPixelFormat::AV_PIX_FMT_YUV420P as i32).is_ok());
        assert!(ensure_software_format(AVPixelFormat::AV_PIX_FMT_RGB24 as i32).is_ok());
        assert!(ensure_software_format(AVPixelFormat::AV_PIX_FMT_NV12 as i32).is_ok());
    }

    #[test]
    fn test_ensure_software_format_rejects_hw_formats() {
        for fmt in [
            AVPixelFormat::AV_PIX_FMT_CUDA,
            AVPixelFormat::AV_PIX_FMT_VAAPI,
            AVPixelFormat::AV_PIX_FMT_VULKAN,
            AVPixelFormat::AV_PIX_FMT_QSV,
        ] {
            let err = ensure_software_format(fmt as i32).unwrap_err();
            assert!(err.contains("hardware frame"), "unexpected error: {err}");
        }
    }

    #[test]
    fn test_ensure_software_format_rejects_invalid_values() {
        assert!(ensure_software_format(-1).is_err());
        // Value from upstream issue #46 ("enum from invalid value 0x2005").
        assert!(ensure_software_format(0x2005).is_err());
        assert!(ensure_software_format(i32::MAX).is_err());
    }

    #[test]
    fn row_layout_tight_rows_need_no_stride_handling() {
        // 320 * 3 == 960: no padding.
        assert_eq!(row_layout(960, 320, 3), Ok(RowLayout::Tight));
    }

    #[test]
    fn row_layout_padded_rows_map_to_row_length_pixels() {
        // 1918 * 3 == 5754, padded to 5760 by av_frame_get_buffer:
        // expressible as 1920 whole RGB pixels.
        assert_eq!(row_layout(5760, 1918, 3), Ok(RowLayout::RowLength(1920)));
    }

    #[test]
    fn row_layout_non_pixel_stride_falls_back_to_per_row() {
        // 100 * 3 == 300, padded to 320: not a multiple of 3, GL cannot
        // express this stride via ROW_LENGTH.
        assert_eq!(row_layout(320, 100, 3), Ok(RowLayout::PerRow));
    }

    #[test]
    fn row_layout_rejects_strides_shorter_than_a_row() {
        // A malformed upstream FrameFilter could hand over linesize <
        // width * 3; slicing rows by it would run out of bounds, so the
        // layout classification itself must fail.
        assert!(row_layout(300, 320, 3).is_err());
    }

    #[test]
    fn test_filter_with_fg() {
        let _ = env_logger::builder()
            .filter_level(log::LevelFilter::Trace)
            .is_test(true)
            .try_init();

        let fragment_shader = r##"
            #version 330 core

            in vec2 TexCoord;

            out vec4 FragColor;

            uniform sampler2D texture1;

            uniform float playTime;       // Play time in seconds
            uniform int width;            // Frame width
            uniform int height;           // Frame height

            void main() {
                float duration = 0.9;
                float maxAlpha = 0.1;
                float maxScale = 1.5;

                float progress = mod(playTime, duration) / duration;
                float alpha = maxAlpha * (1.0 - progress);
                float scale = 1.0 + (maxScale - 1.0) * progress;

                float weakX = 0.5 + (TexCoord.x - 0.5) / scale;
                float weakY = 0.5 + (TexCoord.y - 0.5) / scale;

                vec2 weakTextureCoords = vec2(weakX, weakY);
                vec4 weakMask = texture(texture1, weakTextureCoords);

                vec4 mask = texture(texture1, TexCoord);

                FragColor = mask * (1.0 - alpha) + weakMask * alpha;
            }
        "##;

        let mut output: Output = "output.mp4".into();
        let frame_pipeline_builder: FramePipelineBuilder = AVMediaType::AVMEDIA_TYPE_VIDEO.into();
        let filter = OpenGLFrameFilter::new_simple(fragment_shader.to_string()).unwrap();
        let frame_pipeline_builder = frame_pipeline_builder.filter("test", Box::new(filter));
        let output = output.add_frame_pipeline(frame_pipeline_builder);

        let context = FfmpegContext::builder()
            .input("test.mp4")
            .filter_desc("hue=s=0")
            .output(output)
            .build()
            .unwrap();

        let scheduler: FfmpegScheduler<Initialization> = context.into();
        let scheduler = scheduler.start().unwrap();
        scheduler.wait().unwrap();
    }

    #[test]
    fn test_filter() {
        let _ = env_logger::builder()
            .filter_level(log::LevelFilter::Trace)
            .is_test(true)
            .try_init();

        let fragment_shader = r##"
            #version 330 core

            in vec2 TexCoord;

            out vec4 FragColor;

            uniform sampler2D texture1;

            uniform float playTime;       // Play time in seconds
            uniform int width;            // Frame width
            uniform int height;           // Frame height

            void main() {
                float duration = 0.5;
                float maxAlpha = 0.1;
                float maxScale = 1.5;

                float progress = mod(playTime, duration) / duration;
                float alpha = maxAlpha * (1.0 - progress);
                float scale = 1.0 + (maxScale - 1.0) * progress;

                float weakX = 0.5 + (TexCoord.x - 0.5) / scale;
                float weakY = 0.5 + (TexCoord.y - 0.5) / scale;

                vec2 weakTextureCoords = vec2(weakX, weakY);
                vec4 weakMask = texture(texture1, weakTextureCoords);

                vec4 mask = texture(texture1, TexCoord);

                FragColor = mask * (1.0 - alpha) + weakMask * alpha;
            }
        "##;

        let mut output: Output = "output.mp4".into();
        let frame_pipeline_builder: FramePipelineBuilder = AVMediaType::AVMEDIA_TYPE_VIDEO.into();
        let filter = OpenGLFrameFilter::new_simple(fragment_shader.to_string()).unwrap();
        let frame_pipeline_builder = frame_pipeline_builder.filter("opengl", Box::new(filter));
        let output = output.add_frame_pipeline(frame_pipeline_builder);

        let context = FfmpegContext::builder()
            .input("test.mp4")
            .output(output)
            .build()
            .unwrap();

        let scheduler: FfmpegScheduler<Initialization> = context.into();
        let scheduler = scheduler.start().unwrap();
        scheduler.wait().unwrap();
    }
}
