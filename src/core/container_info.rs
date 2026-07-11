use crate::core::stream_info::init_format_context;
use crate::error::{ContainerInfoError, Result};
use ffmpeg_sys_next::{av_dict_iterate, AVDictionary, AVDictionaryEntry};
use std::ffi::CStr;
use std::ptr::null;

/// Collect an `AVDictionary` into an ordered `Vec` of key/value pairs.
///
/// SAFETY: `dict` must be null or a valid `AVDictionary`. The returned strings are
/// owned copies, so they remain valid after the dictionary is freed.
unsafe fn dict_to_vec(dict: *mut AVDictionary) -> Vec<(String, String)> {
    let mut out = Vec::new();
    if dict.is_null() {
        return out;
    }
    // Iterate from null: av_dict_iterate returns the first entry for a null
    // `prev`, then each subsequent entry, and null once the dictionary is
    // exhausted.
    let mut entry: *const AVDictionaryEntry = null();
    while {
        entry = av_dict_iterate(dict, entry);
        !entry.is_null()
    } {
        let k = CStr::from_ptr((*entry).key).to_string_lossy().into_owned();
        let v = CStr::from_ptr((*entry).value)
            .to_string_lossy()
            .into_owned();
        out.push((k, v));
    }
    out
}

/// Gets the duration of a media file in microseconds.
///
/// # Arguments
/// - `input`: The path to the input file (e.g., `"video.mp4"`).
///
/// # Returns
/// - `Result<i64>`: the duration in microseconds, or a [`crate::error::Error`] if the
///   file cannot be opened.
///
/// # Example
/// ```rust,ignore
/// let duration = get_duration_us("video.mp4").unwrap();
/// println!("Duration: {} us", duration);
/// ```
pub fn get_duration_us(input: impl Into<String>) -> Result<i64> {
    // init_format_context installs the crate's FFmpeg log/network init and returns a
    // crate error, so a container_info query is safe as the process's first call.
    let ctx = init_format_context(input)?;
    // SAFETY: init_format_context returns a live, opened input context.
    Ok(unsafe { (*ctx.as_ptr()).duration })
}

/// Gets the format name of a media file (e.g., "mp4", "avi").
///
/// # Arguments
/// - `input`: The path to the input file (e.g., `"video.mp4"`).
///
/// # Returns
/// - `Result<String>`: the demuxer's format name, or a [`crate::error::Error`].
///
/// # Example
/// ```rust,ignore
/// let format = get_format("video.mp4").unwrap();
/// println!("Format: {}", format);
/// ```
pub fn get_format(input: impl Into<String>) -> Result<String> {
    let ctx = init_format_context(input)?;
    // SAFETY: a successfully opened input context has a non-null `iformat` whose
    // `name` is a valid C string.
    unsafe {
        let iformat = (*ctx.as_ptr()).iformat;
        Ok(CStr::from_ptr((*iformat).name)
            .to_string_lossy()
            .into_owned())
    }
}

/// Gets the container-level metadata of a media file (e.g., title, artist).
///
/// # Arguments
/// - `input`: The path to the input file (e.g., `"video.mp4"`).
///
/// # Returns
/// - `Result<Vec<(String, String)>>`: key/value metadata pairs, or a
///   [`crate::error::Error`].
///
/// # Example
/// ```rust,ignore
/// let metadata = get_metadata("video.mp4").unwrap();
/// for (key, value) in metadata {
///     println!("{}: {}", key, value);
/// }
/// ```
pub fn get_metadata(input: impl Into<String>) -> Result<Vec<(String, String)>> {
    let ctx = init_format_context(input)?;
    // SAFETY: live input context; `metadata` is null or a valid dictionary.
    Ok(unsafe { dict_to_vec((*ctx.as_ptr()).metadata) })
}

/// Gets the metadata of a specific chapter in a media file.
///
/// FFmpeg reference: `AVChapter->metadata` in `libavformat/avformat.h`.
///
/// # Arguments
/// - `input`: The path to the input file (e.g., `"video.mp4"`).
/// - `chapter_index`: The index of the chapter (0-based, not the chapter ID).
///
/// # Returns
/// - `Result<Vec<(String, String)>>`: key/value metadata pairs, or a
///   [`crate::error::Error`]. A `chapter_index` past the last chapter yields
///   [`ContainerInfoError::ChapterIndexOutOfRange`]
///   carrying the index and the container's chapter count.
///
/// # Example
/// ```rust,ignore
/// let metadata = get_chapter_metadata("video.mp4", 0).unwrap();
/// for (key, value) in metadata {
///     println!("{}: {}", key, value);
/// }
/// ```
pub fn get_chapter_metadata(
    input: impl Into<String>,
    chapter_index: usize,
) -> Result<Vec<(String, String)>> {
    let ctx = init_format_context(input)?;
    // SAFETY: live input context; `nb_chapters` bounds the `chapters` array.
    unsafe {
        let fmt = ctx.as_ptr();
        let count = (*fmt).nb_chapters as usize;
        if chapter_index >= count {
            return Err(ContainerInfoError::ChapterIndexOutOfRange {
                index: chapter_index,
                count,
            }
            .into());
        }
        let chapter = *(*fmt).chapters.add(chapter_index);
        Ok(dict_to_vec((*chapter).metadata))
    }
}

/// Gets the metadata of a specific stream in a media file.
///
/// FFmpeg reference: `AVStream->metadata` in `libavformat/avformat.h`.
///
/// # Arguments
/// - `input`: The path to the input file (e.g., `"video.mp4"`).
/// - `stream_index`: The index of the stream (0-based).
///
/// # Returns
/// - `Result<Vec<(String, String)>>`: key/value metadata pairs, or a
///   [`crate::error::Error`]. A `stream_index` past the last stream yields
///   [`ContainerInfoError::StreamIndexOutOfRange`]
///   carrying the index and the container's stream count.
///
/// # Example
/// ```rust,ignore
/// let metadata = get_stream_metadata("video.mp4", 0).unwrap();
/// for (key, value) in metadata {
///     println!("{}: {}", key, value);
/// }
/// ```
pub fn get_stream_metadata(
    input: impl Into<String>,
    stream_index: usize,
) -> Result<Vec<(String, String)>> {
    let ctx = init_format_context(input)?;
    // SAFETY: live input context; `nb_streams` bounds the `streams` array.
    unsafe {
        let fmt = ctx.as_ptr();
        let count = (*fmt).nb_streams as usize;
        if stream_index >= count {
            return Err(ContainerInfoError::StreamIndexOutOfRange {
                index: stream_index,
                count,
            }
            .into());
        }
        let stream = *(*fmt).streams.add(stream_index);
        Ok(dict_to_vec((*stream).metadata))
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::error::Error;
    use std::path::Path;

    /// Integration tests rely on the sample `test.mp4` stored at the repository root.
    const TEST_VIDEO_PATH: &str = "test.mp4";

    fn require_test_asset() {
        assert!(
            Path::new(TEST_VIDEO_PATH).exists(),
            "Expected '{}' to exist in the repo root for container_info tests",
            TEST_VIDEO_PATH
        );
    }

    /// Write a tiny `ffmetadata` file with one chapter so the chapter-array
    /// success path (`(*fmt).chapters.add(i)` deref) is covered without a binary
    /// fixture. The `;FFMETADATA1` header makes FFmpeg auto-detect the demuxer,
    /// which parses `[CHAPTER]` blocks into `AVChapter`s; the file has no streams,
    /// which `avformat_find_stream_info` accepts.
    fn write_ffmetadata_with_chapter() -> std::path::PathBuf {
        let path = std::env::temp_dir().join(format!(
            "ez_ffmpeg_container_info_{}.ffmeta",
            std::process::id()
        ));
        let body = ";FFMETADATA1\ntitle=Test Container\n\
                    [CHAPTER]\nTIMEBASE=1/1000\nSTART=0\nEND=5000\ntitle=Chapter One\n";
        std::fs::write(&path, body).expect("write ffmetadata chapter fixture");
        path
    }

    #[test]
    fn test_get_chapter_metadata_success_reads_chapter_title() {
        let path = write_ffmetadata_with_chapter();
        let result = get_chapter_metadata(path.to_string_lossy().as_ref(), 0);
        let _ = std::fs::remove_file(&path);
        let metadata = result.expect("chapter 0 of the fixture must be readable");
        assert!(
            metadata
                .iter()
                .any(|(k, v)| k == "title" && v == "Chapter One"),
            "chapter metadata must include the title we wrote: {metadata:?}"
        );
    }

    #[test]
    fn test_get_chapter_metadata_out_of_range_on_a_chapterless_file() {
        require_test_asset();
        // test.mp4 has no chapters, so index 0 is already past the end.
        let result = get_chapter_metadata(TEST_VIDEO_PATH, 0);
        assert!(
            matches!(
                result,
                Err(Error::ContainerInfo(
                    ContainerInfoError::ChapterIndexOutOfRange { index: 0, count: 0 }
                ))
            ),
            "got {result:?}"
        );
    }

    #[test]
    fn test_get_chapter_metadata_out_of_range_reports_index_and_count() {
        require_test_asset();
        let result = get_chapter_metadata(TEST_VIDEO_PATH, 999);
        assert!(
            matches!(
                result,
                Err(Error::ContainerInfo(
                    ContainerInfoError::ChapterIndexOutOfRange {
                        index: 999,
                        count: 0
                    }
                ))
            ),
            "got {result:?}"
        );
    }

    #[test]
    fn test_get_stream_metadata_video_stream() {
        require_test_asset();
        let metadata = get_stream_metadata(TEST_VIDEO_PATH, 0).unwrap();
        assert!(!metadata.is_empty());
    }

    #[test]
    fn test_get_stream_metadata_audio_stream() {
        require_test_asset();
        let metadata = get_stream_metadata(TEST_VIDEO_PATH, 1).unwrap();
        assert!(!metadata.is_empty());
    }

    #[test]
    fn test_get_stream_metadata_out_of_range_reports_index_and_count() {
        require_test_asset();
        match get_stream_metadata(TEST_VIDEO_PATH, 999) {
            Err(Error::ContainerInfo(ContainerInfoError::StreamIndexOutOfRange {
                index,
                count,
            })) => {
                assert_eq!(index, 999);
                assert!(
                    count >= 2,
                    "test.mp4 has at least a video and an audio stream, got count={count}"
                );
            }
            other => panic!("expected StreamIndexOutOfRange, got {other:?}"),
        }
    }

    #[test]
    fn test_get_metadata_returns_global_entries() {
        require_test_asset();
        let metadata = get_metadata(TEST_VIDEO_PATH).unwrap();
        assert!(!metadata.is_empty());
    }

    /// Compile-only contract: every query returns `crate::error::Result`, so a
    /// caller whose own function returns the crate Result can `?` them directly
    /// (the whole point of dropping the `ffmpeg_next::Error` return type). That
    /// this type-checks is the assertion; it is not run against a real file.
    #[test]
    fn all_queries_compose_with_the_question_mark_operator() {
        fn _compose(path: &str, stream: usize, chapter: usize) -> Result<()> {
            get_duration_us(path)?;
            get_format(path)?;
            get_metadata(path)?;
            get_chapter_metadata(path, chapter)?;
            get_stream_metadata(path, stream)?;
            Ok(())
        }
        let _ = _compose as fn(&str, usize, usize) -> Result<()>;
    }

    #[test]
    fn test_get_duration_and_format() {
        require_test_asset();
        let duration = get_duration_us(TEST_VIDEO_PATH).unwrap();
        assert!(
            duration > 0,
            "sample asset should report a positive duration"
        );
        let format = get_format(TEST_VIDEO_PATH).unwrap();
        assert!(!format.is_empty(), "format name must be non-empty");
    }
}
