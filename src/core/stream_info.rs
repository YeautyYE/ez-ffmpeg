use std::collections::HashMap;
use std::ffi::{CStr, CString};
use std::ptr::{null, null_mut};

use ffmpeg_sys_next::{avformat_close_input, avformat_open_input};
use ffmpeg_sys_next::AVMediaType::{AVMEDIA_TYPE_ATTACHMENT, AVMEDIA_TYPE_AUDIO, AVMEDIA_TYPE_DATA, AVMEDIA_TYPE_SUBTITLE, AVMEDIA_TYPE_UNKNOWN, AVMEDIA_TYPE_VIDEO};
#[cfg(not(feature = "docs-rs"))]
use ffmpeg_sys_next::AVChannelOrder;
use ffmpeg_sys_next::{av_dict_get, av_find_best_stream, avcodec_get_name, avformat_find_stream_info, AVCodecID, AVDictionary, AVDictionaryEntry, AVRational, AV_DICT_IGNORE_SUFFIX};

use crate::error::{FindStreamError, OpenInputError, Result};

#[derive(Debug, Clone)]
pub enum StreamInfo {
    /// Video stream information
    Video {
        // from AVStream

        /// The index of the stream within the media file.
        index: i32,

        /// The time base for the stream, representing the unit of time for each frame or packet.
        time_base: AVRational,

        /// The start time of the stream, in `time_base` units.
        start_time: i64,

        /// The total duration of the stream, in `time_base` units.
        duration: i64,

        /// The total number of frames in the video stream.
        nb_frames: i64,

        /// The raw frame rate (frames per second) of the video stream, represented as a rational number.
        framerate: AVRational,

        /// The sample aspect ratio of the video frames, which represents the shape of individual pixels.
        sample_aspect_ratio: AVRational,

        /// Metadata associated with the video stream, such as title, language, etc.
        metadata: HashMap<String, String>,

        /// The average frame rate of the stream, potentially accounting for variable frame rates.
        avg_framerate: AVRational,

        // from AVCodecParameters

        /// The codec identifier (e.g., `AV_CODEC_ID_H264`) used to decode the video stream.
        codec_id: AVCodecID,

        /// A human-readable name of the codec used for the video stream.
        codec_name: String,

        /// The width of the video frame in pixels.
        width: i32,

        /// The height of the video frame in pixels.
        height: i32,

        /// The bitrate of the video stream, measured in bits per second (bps).
        bit_rate: i64,

        /// The pixel format of the video stream (e.g., `AV_PIX_FMT_YUV420P`).
        pixel_format: i32,

        /// Delay introduced by the video codec, measured in frames.
        video_delay: i32,

        /// The frames per second (FPS) of the video stream, represented as a floating point number.
        /// It is calculated from the `framerate` field (framerate.num / framerate.den).
        fps: f32,

        /// The rotation of the video stream in degrees. This value is retrieved from the metadata.
        /// Common values are 0, 90, 180, and 270.
        rotate: i32,
    },
    /// Audio stream information
    Audio {
        // from AVStream

        /// The index of the audio stream within the media file.
        index: i32,

        /// The time base for the stream, representing the unit of time for each audio packet.
        time_base: AVRational,

        /// The start time of the audio stream, in `time_base` units.
        start_time: i64,

        /// The total duration of the audio stream, in `time_base` units.
        duration: i64,

        /// The total number of frames in the audio stream.
        nb_frames: i64,

        /// Metadata associated with the audio stream, such as language, title, etc.
        metadata: HashMap<String, String>,

        /// The average frame rate of the audio stream, which might not always be applicable for audio streams.
        avg_framerate: AVRational,

        // from AVCodecParameters

        /// The codec identifier used to decode the audio stream (e.g., `AV_CODEC_ID_AAC`).
        codec_id: AVCodecID,

        /// A human-readable name of the codec used for the audio stream.
        codec_name: String,

        /// The audio sample rate, measured in samples per second (Hz).
        sample_rate: i32,

        /// Channel order used in this layout.
        #[cfg(not(feature = "docs-rs"))]
        order: AVChannelOrder,

        /// Number of channels in this layout.
        nb_channels: i32,

        /// The bitrate of the audio stream, measured in bits per second (bps).
        bit_rate: i64,

        /// The format of the audio samples (e.g., `AV_SAMPLE_FMT_FLTP` for planar float samples).
        sample_format: i32,

        /// The size of each audio frame, typically representing the number of samples per channel in one frame.
        frame_size: i32,
    },
    /// Subtitle stream information
    Subtitle {
        // from AVStream

        /// The index of the subtitle stream within the media file.
        index: i32,

        /// The time base for the stream, representing the unit of time for each subtitle event.
        time_base: AVRational,

        /// The start time of the subtitle stream, in `time_base` units.
        start_time: i64,

        /// The total duration of the subtitle stream, in `time_base` units.
        duration: i64,

        /// The total number of subtitle events in the stream.
        nb_frames: i64,

        /// Metadata associated with the subtitle stream, such as language.
        metadata: HashMap<String, String>,

        // from AVCodecParameters

        /// The codec identifier used to decode the subtitle stream (e.g., `AV_CODEC_ID_ASS`).
        codec_id: AVCodecID,

        /// A human-readable name of the codec used for the subtitle stream.
        codec_name: String,
    },
    /// Data stream information
    Data {
        // From AVStream

        /// The index of the data stream within the media file.
        index: i32,

        /// The time base for the data stream, representing the unit of time for each data packet.
        time_base: AVRational,

        /// The start time of the data stream, in `time_base` units.
        start_time: i64,

        /// The total duration of the data stream, in `time_base` units.
        duration: i64,

        /// Metadata associated with the data stream, such as additional information about the stream content.
        metadata: HashMap<String, String>,
    },
    /// Attachment stream information
    Attachment {
        // From AVStream

        /// The index of the attachment stream within the media file.
        index: i32,

        /// Metadata associated with the attachment stream, such as details about the attached file.
        metadata: HashMap<String, String>,

        // From AVCodecParameters

        /// The codec identifier used to decode the attachment stream (e.g., `AV_CODEC_ID_PNG` for images).
        codec_id: AVCodecID,

        /// A human-readable name of the codec used for the attachment stream.
        codec_name: String,
    },
    // Unknown stream information
    Unknown {
        // From AVStream

        /// The index of the unknown stream within the media file.
        index: i32,

        /// Metadata associated with the unknown stream, which might provide further information about the stream.
        metadata: HashMap<String, String>,
    },
}


impl StreamInfo {
    pub fn stream_type(&self) -> &'static str {
        match self {
            StreamInfo::Video { .. } => "Video",
            StreamInfo::Audio { .. } => "Audio",
            StreamInfo::Subtitle { .. } => "Subtitle",
            StreamInfo::Data { .. } => "Data",
            StreamInfo::Attachment { .. } => "Attachment",
            StreamInfo::Unknown { .. } => "Unknown"
        }
    }
}


/// Retrieves video stream information from a given media URL.
///
/// This function opens the media file or stream specified by the URL and
/// searches for the best video stream. If a video stream is found, it
/// returns the relevant metadata and codec parameters wrapped in a
/// `StreamInfo::Video` enum variant.
///
/// # Parameters
/// - `url`: The URL or file path of the media file to analyze.
///
/// # Returns
/// - `Ok(Some(StreamInfo::Video))`: Contains the video stream information if found.
/// - `Ok(None)`: Returned if no video stream is found.
/// - `Err`: If an error occurs during the operation (e.g., file cannot be opened or stream information cannot be found).
pub fn find_video_stream_info(url: &str) -> Result<Option<StreamInfo>> {
    let mut in_fmt_ctx = null_mut();

    let url_cstr = CString::new(url)?;
    let mut ret = 0;
    unsafe {
        #[cfg(not(feature = "docs-rs"))]
        {
            ret = avformat_open_input(&mut in_fmt_ctx, url_cstr.as_ptr(), null(), null_mut());
        }
        if ret < 0 {
            avformat_close_input(&mut in_fmt_ctx);
            return Err(OpenInputError::from(ret).into());
        }

        ret = avformat_find_stream_info(in_fmt_ctx, null_mut());
        if ret < 0 {
            avformat_close_input(&mut in_fmt_ctx);
            return Err(FindStreamError::from(ret).into());
        }

        let video_index = av_find_best_stream(in_fmt_ctx, AVMEDIA_TYPE_VIDEO, -1, -1, null_mut(), 0);
        if video_index < 0 {
            avformat_close_input(&mut in_fmt_ctx);
            return Ok(None);
        }
        let streams = (*in_fmt_ctx).streams;
        let video_stream = *streams.offset(video_index as isize);

        let index = (*video_stream).index;
        let time_base = (*video_stream).time_base;
        let start_time = (*video_stream).start_time;
        let duration = (*video_stream).duration;
        let nb_frames = (*video_stream).nb_frames;
        let framerate = (*video_stream).r_frame_rate;
        let sample_aspect_ratio = (*video_stream).sample_aspect_ratio;
        let metadata = (*video_stream).metadata;
        let metadata = av_dict_to_hashmap(metadata);
        let avg_framerate = (*video_stream).avg_frame_rate;

        let codec_parameters = (*video_stream).codecpar;
        let codec_id = (*codec_parameters).codec_id;
        let codec_name = CStr::from_ptr(avcodec_get_name(codec_id));
        let codec_name = codec_name.to_str().unwrap_or("Unknown codec");
        let width = (*codec_parameters).width;
        let height = (*codec_parameters).height;
        let bit_rate = (*codec_parameters).bit_rate;
        let pixel_format = (*codec_parameters).format;
        let video_delay = (*codec_parameters).video_delay;
        let fps = framerate.num as f32 / framerate.den as f32;

        // Fetch the rotation info from metadata (if present)
        let rotate = metadata
            .get("rotate")
            .and_then(|rotate| rotate.parse::<i32>().ok())
            .unwrap_or(0); // Default to 0 if no "rotate" key is found

        let video_stream_info = StreamInfo::Video {
            index,
            time_base,
            start_time,
            duration,
            nb_frames,
            framerate,
            sample_aspect_ratio,
            metadata,
            avg_framerate,
            codec_id,
            codec_name: codec_name.to_string(),
            width,
            height,
            bit_rate,
            pixel_format,
            video_delay,
            fps,
            rotate,
        };

        avformat_close_input(&mut in_fmt_ctx);

        Ok(Some(video_stream_info))
    }
}

/// Retrieves audio stream information from a given media URL.
///
/// This function opens the media file or stream specified by the URL and
/// searches for the best audio stream. If an audio stream is found, it
/// returns the relevant metadata and codec parameters wrapped in a
/// `StreamInfo::Audio` enum variant.
///
/// # Parameters
/// - `url`: The URL or file path of the media file to analyze.
///
/// # Returns
/// - `Ok(Some(StreamInfo::Audio))`: Contains the audio stream information if found.
/// - `Ok(None)`: Returned if no audio stream is found.
/// - `Err`: If an error occurs during the operation (e.g., file cannot be opened or stream information cannot be found).
pub fn find_audio_stream_info(url: &str) -> Result<Option<StreamInfo>> {
    let mut in_fmt_ctx = null_mut();

    let url_cstr = CString::new(url)?;
    let mut ret = 0;
    unsafe {
        #[cfg(not(feature = "docs-rs"))]
        {
            ret = avformat_open_input(&mut in_fmt_ctx, url_cstr.as_ptr(), null(), null_mut());
        }
        if ret < 0 {
            avformat_close_input(&mut in_fmt_ctx);
            return Err(OpenInputError::from(ret).into());
        }

        ret = avformat_find_stream_info(in_fmt_ctx, null_mut());
        if ret < 0 {
            avformat_close_input(&mut in_fmt_ctx);
            return Err(FindStreamError::from(ret).into());
        }

        let audio_index = av_find_best_stream(in_fmt_ctx, AVMEDIA_TYPE_AUDIO, -1, -1, null_mut(), 0);
        if audio_index < 0 {
            avformat_close_input(&mut in_fmt_ctx);
            return Ok(None);
        }
        let streams = (*in_fmt_ctx).streams;
        let audio_stream = *streams.offset(audio_index as isize);

        let index = (*audio_stream).index;
        let time_base = (*audio_stream).time_base;
        let start_time = (*audio_stream).start_time;
        let duration = (*audio_stream).duration;
        let nb_frames = (*audio_stream).nb_frames;
        let metadata = (*audio_stream).metadata;
        let metadata = av_dict_to_hashmap(metadata);
        let avg_framerate = (*audio_stream).avg_frame_rate;

        let codec_parameters = (*audio_stream).codecpar;
        let codec_id = (*codec_parameters).codec_id;
        let codec_name = CStr::from_ptr(avcodec_get_name(codec_id));
        let codec_name = codec_name.to_str().unwrap_or("Unknown codec");
        let sample_rate = (*codec_parameters).sample_rate;
        #[cfg(not(feature = "docs-rs"))]
        let ch_layout = (*codec_parameters).ch_layout;
        let bit_rate = (*codec_parameters).bit_rate;
        let sample_format = (*codec_parameters).format;
        let frame_size = (*codec_parameters).frame_size;

        let audio_stream_info = StreamInfo::Audio {
            index,
            time_base,
            start_time,
            duration,
            nb_frames,
            metadata,
            avg_framerate,
            codec_id,
            codec_name: codec_name.to_string(),
            sample_rate,
            #[cfg(not(feature = "docs-rs"))]
            order: ch_layout.order,
            #[cfg(feature = "docs-rs")]
            nb_channels: 0,
            #[cfg(not(feature = "docs-rs"))]
            nb_channels: ch_layout.nb_channels,
            bit_rate,
            sample_format,
            frame_size,
        };

        avformat_close_input(&mut in_fmt_ctx);

        Ok(Some(audio_stream_info))
    }
}

/// Retrieves subtitle stream information from a given media URL.
///
/// This function opens the media file or stream specified by the URL and
/// searches for the best subtitle stream. If a subtitle stream is found, it
/// returns the relevant metadata and codec parameters wrapped in a
/// `StreamInfo::Subtitle` enum variant. It also attempts to retrieve any
/// language information from the stream metadata.
///
/// # Parameters
/// - `url`: The URL or file path of the media file to analyze.
///
/// # Returns
/// - `Ok(Some(StreamInfo::Subtitle))`: Contains the subtitle stream information if found.
/// - `Ok(None)`: Returned if no subtitle stream is found.
/// - `Err`: If an error occurs during the operation (e.g., file cannot be opened or stream information cannot be found).
pub fn find_subtitle_stream_info(url: &str) -> Result<Option<StreamInfo>> {
    let mut in_fmt_ctx = null_mut();

    let url_cstr = CString::new(url)?;
    let mut ret = 0;
    unsafe {
        #[cfg(not(feature = "docs-rs"))]
        {
            ret = avformat_open_input(&mut in_fmt_ctx, url_cstr.as_ptr(), null(), null_mut());
        }
        if ret < 0 {
            avformat_close_input(&mut in_fmt_ctx);
            return Err(OpenInputError::from(ret).into());
        }

        ret = avformat_find_stream_info(in_fmt_ctx, null_mut());
        if ret < 0 {
            avformat_close_input(&mut in_fmt_ctx);
            return Err(FindStreamError::from(ret).into());
        }

        let subtitle_index = av_find_best_stream(in_fmt_ctx, AVMEDIA_TYPE_SUBTITLE, -1, -1, null_mut(), 0);
        if subtitle_index < 0 {
            avformat_close_input(&mut in_fmt_ctx);
            return Ok(None);
        }

        let streams = (*in_fmt_ctx).streams;
        let subtitle_stream = *streams.offset(subtitle_index as isize);

        let index = (*subtitle_stream).index;
        let time_base = (*subtitle_stream).time_base;
        let start_time = (*subtitle_stream).start_time;
        let duration = (*subtitle_stream).duration;
        let nb_frames = (*subtitle_stream).nb_frames;
        let metadata = (*subtitle_stream).metadata;
        let metadata = av_dict_to_hashmap(metadata);

        let codec_parameters = (*subtitle_stream).codecpar;
        let codec_id = (*codec_parameters).codec_id;
        let codec_name = CStr::from_ptr(avcodec_get_name(codec_id));
        let codec_name = codec_name.to_str().unwrap_or("Unknown codec");

        let subtitle_stream_info = StreamInfo::Subtitle {
            index,
            time_base,
            start_time,
            duration,
            nb_frames,
            metadata,
            codec_id,
            codec_name: codec_name.to_string(),
        };

        avformat_close_input(&mut in_fmt_ctx);

        Ok(Some(subtitle_stream_info))
    }
}

/// Finds the data stream information from the given media URL.
///
/// This function opens the media file or stream specified by the URL and
/// searches for a data stream (`AVMEDIA_TYPE_DATA`). It returns relevant metadata
/// wrapped in a `StreamInfo::Data` enum variant.
///
/// # Parameters
/// - `url`: The URL or file path of the media file.
///
/// # Returns
/// - `Ok(Some(StreamInfo::Data))`: Contains the data stream information if found.
/// - `Ok(None)`: Returned if no data stream is found.
/// - `Err`: If an error occurs during the operation.
pub fn find_data_stream_info(url: &str) -> Result<Option<StreamInfo>> {
    let mut in_fmt_ctx = null_mut();
    let url_cstr = CString::new(url)?;
    let mut ret = 0;
    unsafe {
        #[cfg(not(feature = "docs-rs"))]
        {
            ret = avformat_open_input(&mut in_fmt_ctx, url_cstr.as_ptr(), null(), null_mut());
        }
        if ret < 0 {
            avformat_close_input(&mut in_fmt_ctx);
            return Err(OpenInputError::from(ret).into());
        }

        ret = avformat_find_stream_info(in_fmt_ctx, null_mut());
        if ret < 0 {
            avformat_close_input(&mut in_fmt_ctx);
            return Err(FindStreamError::from(ret).into());
        }

        let data_index = av_find_best_stream(in_fmt_ctx, AVMEDIA_TYPE_DATA, -1, -1, null_mut(), 0);
        if data_index < 0 {
            avformat_close_input(&mut in_fmt_ctx);
            return Ok(None);
        }

        let streams = (*in_fmt_ctx).streams;
        let data_stream = *streams.offset(data_index as isize);

        let index = (*data_stream).index;
        let time_base = (*data_stream).time_base;
        let start_time = (*data_stream).start_time;
        let duration = (*data_stream).duration;
        let metadata = av_dict_to_hashmap((*data_stream).metadata);

        avformat_close_input(&mut in_fmt_ctx);

        Ok(Some(StreamInfo::Data {
            index,
            time_base,
            start_time,
            duration,
            metadata,
        }))
    }
}

/// Finds the attachment stream information from the given media URL.
///
/// This function opens the media file or stream specified by the URL and
/// searches for an attachment stream (`AVMEDIA_TYPE_ATTACHMENT`). It returns
/// relevant metadata and codec information wrapped in a `StreamInfo::Attachment`
/// enum variant.
///
/// # Parameters
/// - `url`: The URL or file path of the media file.
///
/// # Returns
/// - `Ok(Some(StreamInfo::Attachment))`: Contains the attachment stream information if found.
/// - `Ok(None)`: Returned if no attachment stream is found.
/// - `Err`: If an error occurs during the operation.
pub fn find_attachment_stream_info(url: &str) -> Result<Option<StreamInfo>> {
    let mut in_fmt_ctx = null_mut();
    let url_cstr = CString::new(url)?;
    let mut ret = 0;
    unsafe {
        #[cfg(not(feature = "docs-rs"))]
        {
            ret = avformat_open_input(&mut in_fmt_ctx, url_cstr.as_ptr(), null(), null_mut());
        }
        if ret < 0 {
            avformat_close_input(&mut in_fmt_ctx);
            return Err(OpenInputError::from(ret).into());
        }

        ret = avformat_find_stream_info(in_fmt_ctx, null_mut());
        if ret < 0 {
            avformat_close_input(&mut in_fmt_ctx);
            return Err(FindStreamError::from(ret).into());
        }

        let attachment_index = av_find_best_stream(in_fmt_ctx, AVMEDIA_TYPE_ATTACHMENT, -1, -1, null_mut(), 0);
        if attachment_index < 0 {
            avformat_close_input(&mut in_fmt_ctx);
            return Ok(None);
        }

        let streams = (*in_fmt_ctx).streams;
        let attachment_stream = *streams.offset(attachment_index as isize);

        let index = (*attachment_stream).index;
        let metadata = av_dict_to_hashmap((*attachment_stream).metadata);

        let codec_parameters = (*attachment_stream).codecpar;
        let codec_id = (*codec_parameters).codec_id;
        let codec_name = CStr::from_ptr(avcodec_get_name(codec_id))
            .to_str()
            .unwrap_or("Unknown codec")
            .to_string();

        avformat_close_input(&mut in_fmt_ctx);

        Ok(Some(StreamInfo::Attachment {
            index,
            metadata,
            codec_id,
            codec_name,
        }))
    }
}

/// Finds the unknown stream information from the given media URL.
///
/// This function opens the media file or stream specified by the URL and
/// searches for any unknown stream (`AVMEDIA_TYPE_UNKNOWN`). It returns
/// relevant metadata wrapped in a `StreamInfo::Unknown` enum variant.
///
/// # Parameters
/// - `url`: The URL or file path of the media file.
///
/// # Returns
/// - `Ok(Some(StreamInfo::Unknown))`: Contains the unknown stream information if found.
/// - `Ok(None)`: Returned if no unknown stream is found.
/// - `Err`: If an error occurs during the operation.
pub fn find_unknown_stream_info(url: &str) -> Result<Option<StreamInfo>> {
    let mut in_fmt_ctx = null_mut();
    let url_cstr = CString::new(url)?;
    let mut ret = 0;
    unsafe {
        #[cfg(not(feature = "docs-rs"))]
        {
            ret = avformat_open_input(&mut in_fmt_ctx, url_cstr.as_ptr(), null(), null_mut());
        }
        if ret < 0 {
            avformat_close_input(&mut in_fmt_ctx);
            return Err(OpenInputError::from(ret).into());
        }

        ret = avformat_find_stream_info(in_fmt_ctx, null_mut());
        if ret < 0 {
            avformat_close_input(&mut in_fmt_ctx);
            return Err(FindStreamError::from(ret).into());
        }

        let unknown_index = av_find_best_stream(in_fmt_ctx, AVMEDIA_TYPE_UNKNOWN, -1, -1, null_mut(), 0);
        if unknown_index < 0 {
            avformat_close_input(&mut in_fmt_ctx);
            return Ok(None);
        }

        let streams = (*in_fmt_ctx).streams;
        let unknown_stream = *streams.offset(unknown_index as isize);

        let index = (*unknown_stream).index;
        let metadata = av_dict_to_hashmap((*unknown_stream).metadata);

        avformat_close_input(&mut in_fmt_ctx);

        Ok(Some(StreamInfo::Unknown {
            index,
            metadata,
        }))
    }
}

/// Retrieves information for all streams (video, audio, subtitle, etc.) from a given media URL.
///
/// This function opens the media file or stream specified by the URL and
/// retrieves information for all available streams (e.g., video, audio, subtitles).
/// The information for each stream is wrapped in a corresponding `StreamInfo` enum
/// variant and collected into a `Vec<StreamInfo>`.
///
/// # Parameters
/// - `url`: The URL or file path of the media file to analyze.
///
/// # Returns
/// - `Ok(Vec<StreamInfo>)`: A vector containing information for all detected streams.
/// - `Err`: If an error occurs during the operation (e.g., file cannot be opened or stream information cannot be found).
pub fn find_all_stream_infos(url: &str) -> Result<Vec<StreamInfo>> {
    let mut in_fmt_ctx = null_mut();
    let url_cstr = CString::new(url)?;
    let mut ret = 0;
    unsafe {
        #[cfg(not(feature = "docs-rs"))]
        {
            ret = avformat_open_input(&mut in_fmt_ctx, url_cstr.as_ptr(), null(), null_mut());
        }
        if ret < 0 {
            avformat_close_input(&mut in_fmt_ctx);
            return Err(OpenInputError::from(ret).into());
        }

        ret = avformat_find_stream_info(in_fmt_ctx, null_mut());
        if ret < 0 {
            avformat_close_input(&mut in_fmt_ctx);
            return Err(FindStreamError::from(ret).into());
        }

        let mut stream_infos = Vec::new();

        let stream_count = (*in_fmt_ctx).nb_streams;

        for i in 0..stream_count {
            let stream = *(*in_fmt_ctx).streams.add(i as usize);
            let codec_parameters = (*stream).codecpar;
            let codec_id = (*codec_parameters).codec_id;
            let codec_name = CStr::from_ptr(avcodec_get_name(codec_id)).to_str().unwrap_or("Unknown codec").to_string();

            let index = (*stream).index;
            let time_base = (*stream).time_base;
            let start_time = (*stream).start_time;
            let duration = (*stream).duration;
            let nb_frames = (*stream).nb_frames;
            let avg_framerate = (*stream).avg_frame_rate;
            let metadata = av_dict_to_hashmap((*stream).metadata);

            match (*codec_parameters).codec_type {
                AVMEDIA_TYPE_VIDEO => {
                    let width = (*codec_parameters).width;
                    let height = (*codec_parameters).height;
                    let bit_rate = (*codec_parameters).bit_rate;
                    let pixel_format = (*codec_parameters).format;
                    let video_delay = (*codec_parameters).video_delay;
                    let framerate = (*stream).r_frame_rate;
                    let sample_aspect_ratio = (*stream).sample_aspect_ratio;
                    let fps = framerate.num as f32 / framerate.den as f32;

                    // Fetch the rotation info from metadata (if present)
                    let rotate = metadata
                        .get("rotate")
                        .and_then(|rotate| rotate.parse::<i32>().ok())
                        .unwrap_or(0); // Default to 0 if no "rotate" key is found

                    stream_infos.push(StreamInfo::Video {
                        index,
                        time_base,
                        start_time,
                        duration,
                        nb_frames,
                        framerate,
                        sample_aspect_ratio,
                        metadata,
                        avg_framerate,
                        codec_id,
                        codec_name,
                        width,
                        height,
                        bit_rate,
                        pixel_format,
                        video_delay,
                        fps,
                        rotate
                    });
                }
                AVMEDIA_TYPE_AUDIO => {
                    let sample_rate = (*codec_parameters).sample_rate;
                    #[cfg(not(feature = "docs-rs"))]
                    let ch_layout = (*codec_parameters).ch_layout;
                    let sample_format = (*codec_parameters).format;
                    let frame_size = (*codec_parameters).frame_size;
                    let bit_rate = (*codec_parameters).bit_rate;

                    stream_infos.push(StreamInfo::Audio {
                        index,
                        time_base,
                        start_time,
                        duration,
                        nb_frames,
                        metadata,
                        avg_framerate,
                        codec_id,
                        codec_name,
                        sample_rate,
                        #[cfg(not(feature = "docs-rs"))]
                        order: ch_layout.order,
                        #[cfg(feature = "docs-rs")]
                        nb_channels: 0,
                        #[cfg(not(feature = "docs-rs"))]
                        nb_channels: ch_layout.nb_channels,
                        bit_rate,
                        sample_format,
                        frame_size,
                    });
                }
                AVMEDIA_TYPE_SUBTITLE => {
                    stream_infos.push(StreamInfo::Subtitle {
                        index,
                        time_base,
                        start_time,
                        duration,
                        nb_frames,
                        metadata,
                        codec_id,
                        codec_name,
                    });
                }
                AVMEDIA_TYPE_DATA => {
                    stream_infos.push(StreamInfo::Data {
                        index,
                        time_base,
                        start_time,
                        duration,
                        metadata,
                    });
                }
                AVMEDIA_TYPE_ATTACHMENT => {
                    stream_infos.push(StreamInfo::Attachment {
                        index,
                        metadata,
                        codec_id,
                        codec_name,
                    });
                }
                AVMEDIA_TYPE_UNKNOWN => {
                    stream_infos.push(StreamInfo::Unknown {
                        index,
                        metadata,
                    });
                }
                _ => {}
            }
        }

        avformat_close_input(&mut in_fmt_ctx);

        Ok(stream_infos)
    }
}

unsafe fn av_dict_to_hashmap(dict: *mut AVDictionary) -> HashMap<String, String> {
    let mut map = HashMap::new();
    let mut entry: *mut AVDictionaryEntry = null_mut();

    while {
        entry = av_dict_get(dict, null(), entry, AV_DICT_IGNORE_SUFFIX);
        !entry.is_null()
    } {
        let key = CStr::from_ptr((*entry).key).to_string_lossy().into_owned();
        let value = CStr::from_ptr((*entry).value).to_string_lossy().into_owned();

        map.insert(key, value);
    }

    map
}


#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_not_found() {
        let result = find_all_stream_infos("not_found.mp4");
        assert!(result.is_err());

        let error = result.err().unwrap();
        println!("{error}");
        assert!(matches!(error, crate::error::Error::OpenInputStream(OpenInputError::NotFound)))
    }

    #[test]
    fn test_find_all_stream_infos() {
        let stream_infos = find_all_stream_infos("test.mp4").unwrap();
        assert_eq!(2, stream_infos.len());
        for stream_info in stream_infos {
            println!("{:?}", stream_info);
        }
    }

    #[test]
    fn test_find_video_stream_info() {
        let option = find_video_stream_info("test.mp4").unwrap();
        assert!(option.is_some());
        let video_stream_info = option.unwrap();
        println!("video_stream_info:{:?}", video_stream_info);
    }

    #[test]
    fn test_find_audio_stream_info() {
        let option = find_audio_stream_info("test.mp4").unwrap();
        assert!(option.is_some());
        let audio_stream_info = option.unwrap();
        println!("audio_stream_info:{:?}", audio_stream_info);
    }

    #[test]
    fn test_find_subtitle_stream_info() {
        let option = find_subtitle_stream_info("test.mp4").unwrap();
        assert!(option.is_none())
    }

    #[test]
    fn test_find_data_stream_info() {
        let option = find_data_stream_info("test.mp4").unwrap();
        assert!(option.is_none());
    }

    #[test]
    fn test_find_attachment_stream_info() {
        let option = find_attachment_stream_info("test.mp4").unwrap();
        assert!(option.is_none())
    }

    #[test]
    fn test_find_unknown_stream_info() {
        let option = find_unknown_stream_info("test.mp4").unwrap();
        assert!(option.is_none())
    }
}