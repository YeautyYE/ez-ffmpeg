use ez_ffmpeg::{FfmpegContext, Input, Output};

fn main() {
    // Example 1: Generate a single thumbnail
    FfmpegContext::builder()
        // Specify the input video file
        .input("test.mp4")
        // Apply the scale filter to resize the video to a width of 160 (height is auto-adjusted to maintain aspect ratio)
        .filter_desc("scale='min(160,iw)':-1")
        // Set the output file to a JPEG image, limiting to 1 frame (equivalent to -vframes 1 in FFmpeg CLI)
        .output(
            Output::from("output.jpg")
                .set_max_video_frames(1)
                // Set the JPEG quality (lower value means higher quality, typical range: 2-31)
                .set_video_qscale(2),
        )
        // Build the context, start the process, and wait for completion
        .build()
        .unwrap()
        .start()
        .unwrap()
        .wait()
        .unwrap();

    // Example 2: Generate multiple thumbnails using %03d pattern in the filename
    FfmpegContext::builder()
        // Specify the input video file
        .input("test.mp4")
        // Apply the same scale filter as before
        .filter_desc("scale='min(160,iw)':-1")
        // Set the output file with %03d to generate multiple images (e.g., output_001.jpg, output_002.jpg, etc.)
        .output(
            Output::from("output_%03d.jpg")
                // Limit the output to 5 frames; adjust as needed
                .set_max_video_frames(5)
                // Set the JPEG quality (lower value means higher quality)
                .set_video_qscale(2),
        )
        // Build the context, start the process, and wait for completion
        .build()
        .unwrap()
        .start()
        .unwrap()
        .wait()
        .unwrap();

    // Example 3: Fastest single thumbnail - decode keyframes only (skip_frame=nokey)
    //
    // Combining an input seek with the decoder option `skip_frame=nokey` skips all
    // non-key frames, so typically only one or two keyframes are decoded (plus a
    // small scheduler read-ahead) no matter how long the GOP is, while the default
    // seek path decodes every frame from the previous keyframe up to the requested
    // time. This relies on the container seek succeeding (normal for local files);
    // on unseekable inputs the decoder walks keyframes from the file start instead.
    // The speedup grows with GOP length and resolution. Trade-offs:
    // - The captured frame snaps forward to the first keyframe at or after the
    //   requested time (up to one GOP later), instead of the exact requested frame.
    // - If no keyframe exists at or after the requested time (a target inside the
    //   file's last GOP), the run fails with an encoder error instead of writing
    //   an image.
    // Prefer this mode for scrub previews and batch thumbnail grids; keep the
    // default path (Examples 1/2) when the exact frame matters.

    // The bundled test.mp4 holds a single keyframe at t=0, so first generate a
    // 12-second clip with a keyframe every second to make the snapping visible.
    FfmpegContext::builder()
        .input(Input::from("testsrc2=duration=12:size=640x360:rate=10").set_format("lavfi"))
        .output(
            Output::from("output_keyframe_source.mp4")
                // mpeg4 ships in every FFmpeg build (the default H.264 pick
                // needs an external encoder such as libx264).
                .set_video_codec("mpeg4")
                // Force a keyframe every 10 frames (one per second at 10 fps)
                .set_video_codec_opt("g", "10"),
        )
        .build()
        .unwrap()
        .start()
        .unwrap()
        .wait()
        .unwrap();

    // Request 5.3s: the container-level seek jumps near the target, the decoder
    // emits keyframes only, and the image ends up being the 6.0s keyframe (the
    // first keyframe after 5.3s).
    FfmpegContext::builder()
        .input(
            Input::from("output_keyframe_source.mp4")
                // Container-level seek close to the target time
                .set_start_time_us(5_300_000)
                // Decode keyframes only: skips the keyframe-to-target pre-roll
                .set_video_codec_opt("skip_frame", "nokey"),
        )
        .filter_desc("scale='min(160,iw)':-1")
        .output(
            Output::from("output_keyframe.jpg")
                .set_max_video_frames(1)
                .set_video_qscale(2),
        )
        // Build the context, start the process, and wait for completion
        .build()
        .unwrap()
        .start()
        .unwrap()
        .wait()
        .unwrap();
}
