# ez-ffmpeg Example: Thumbnail Extraction

This example demonstrates how to extract thumbnail images from a video using `ez-ffmpeg`. Three examples are provided: generating a single thumbnail, generating multiple thumbnails, and a keyframe-only fast path for scrub previews.

## Examples

### Example 1: Single Thumbnail Extraction

- **Input Video:** `test.mp4`
- **Thumbnail Extraction:**
  - The `scale` filter resizes the video to a width of `160`, while automatically adjusting the height to maintain the aspect ratio (using `-1`).
  - The expression `min(160, iw)` ensures that the width does not exceed 160 pixels.
- **Output:** The first frame of the video is extracted and saved as `output.jpg`.
- **FFmpeg Option:**
  - `set_max_video_frames(1)`: Limits the output to 1 video frame, effectively extracting only a single frame (the thumbnail).

### Example 2: Multiple Thumbnails Extraction

- **Input Video:** `test.mp4`
- **Thumbnail Extraction:**
  - The same `scale` filter is applied to resize the video.
- **Output:** The first five decoded frames (consecutive, from the start of the video) are saved using a `%03d` pattern in the filename (e.g., `output_001.jpg`, `output_002.jpg`, etc.). To space the images out in time instead, add a sampling filter such as `fps=1` to the filter description.
- **FFmpeg Option:**
  - `set_max_video_frames(5)`: Limits the output to 5 video frames, generating multiple thumbnails.

### Example 3: Fastest Single Thumbnail (Keyframes Only)

- **Input Video:** `output_keyframe_source.mp4` — generated first by the example itself (a 12-second clip with a keyframe every second), because the bundled `test.mp4` holds a single keyframe at `t=0`.
- **Thumbnail Extraction:**
  - `set_start_time_us(5_300_000)` seeks the container close to the requested time.
  - The decoder option `skip_frame=nokey` decodes keyframes only, skipping the keyframe-to-target pre-roll that the default seek path decodes frame by frame.
- **Output:** `output_keyframe.jpg` — the first keyframe at or after the requested time (here the 6.0 s keyframe for a 5.3 s request; the captured frame snaps forward by up to one GOP).
- **Trade-off:** fastest path for scrub previews and batch grids; use Examples 1/2 when the exact requested frame matters.

## When to Use

- **Single Thumbnail Extraction:** Use this method when you need one preview image from the video.
- **Multiple Thumbnails Extraction:** Use this method when you want several preview images from the video.
- **Keyframe-Only Extraction:** Use this method when speed matters more than frame-exact placement (scrub previews, batch thumbnail grids).

