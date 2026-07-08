use ez_ffmpeg::{FfmpegContext, Input, Output};

fn main() {
    // Create input and output stream objects from files
    let mut input: Input = "test.mp4".into();
    let mut output: Output = "output.mp4".into();

    // Select the hardware acceleration for the current platform. Only the
    // block matching the build target compiles; the others are shown for
    // reference. Swap in the vendor variant that matches your GPU.
    #[cfg(target_os = "macos")]
    {
        // macOS: VideoToolbox
        input = input.set_hwaccel("videotoolbox");
        output = output.set_video_codec("h264_videotoolbox");
    }

    #[cfg(target_os = "windows")]
    {
        // Windows: Direct3D 12 VA decode + Media Foundation encode.
        // NVIDIA alternative: set_hwaccel("cuda")/"h264_cuvid" + "h264_nvenc".
        input = input.set_hwaccel("d3d12va");
        output = output.set_video_codec("h264_mf");
    }

    #[cfg(target_os = "linux")]
    {
        // Linux: VAAPI (broadly available on Intel/AMD).
        // NVIDIA: "cuda"/"h264_cuvid" + "h264_nvenc"; Intel: "qsv"/"h264_qsv";
        // AMD: set_hwaccel("vulkan") + "h264_amf".
        input = input.set_hwaccel("vaapi");
        output = output.set_video_codec("h264_vaapi");
    }

    // Build the FFMPEG context, configure input and output, then start the process
    FfmpegContext::builder()
        .input(input) // Set input stream
        .output(output) // Set output stream
        .build()
        .unwrap() // Build context
        .start()
        .unwrap() // Start the process
        .wait()
        .unwrap(); // Wait for the process to finish
}
