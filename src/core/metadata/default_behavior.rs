// Default metadata copying behavior
// Based on FFmpeg ffmpeg_mux_init.c:2784-2983

use ffmpeg_sys_next::{
    av_dict_copy, av_dict_set, av_mallocz, av_realloc_array, av_rescale_q, AVChapter,
    AVFormatContext, AV_DICT_DONT_OVERWRITE, AV_TIME_BASE_Q,
};
use std::cmp::{max, min};
use std::ffi::c_void;
use std::mem;

fn clamp_i128_to_i64(value: i128) -> i64 {
    if value > i64::MAX as i128 {
        i64::MAX
    } else if value < i64::MIN as i128 {
        i64::MIN
    } else {
        value as i64
    }
}

/// Copy metadata from first input to output with FFmpeg's default behavior
///
/// Default behavior includes:
/// 1. Copy global metadata from first input (if not manually specified)
/// 2. Remove specific keys: creation_time, company_name, product_name, product_version
/// 3. Remove duration if recording_time is set
/// 4. Copy stream metadata from corresponding input streams (if not manually specified)
/// 5. Remove encoder key for streams being encoded
///
/// # Safety
/// This function is unsafe because it dereferences raw FFmpeg pointers.
/// The caller must ensure that:
/// - `input_ctxs` contains valid pointers to initialized AVFormatContext structures
/// - `output_ctx` is a valid pointer to an initialized AVFormatContext
/// - `output_stream_count` matches the actual number of output streams
/// - `stream_input_mapping` contains valid indices into input_ctxs
/// - `encoding_streams` contains valid stream indices
/// - All contexts remain valid for the duration of this call
pub unsafe fn copy_metadata_default(
    input_ctxs: &[*const AVFormatContext],
    output_ctx: *mut AVFormatContext,
    output_stream_count: usize,
    stream_input_mapping: &[(usize, (usize, usize))], // (output_stream_idx, (input_file_idx, input_stream_idx))
    encoding_streams: &[usize],                       // indices of streams being encoded
    recording_time_set: bool,
    auto_copy_enabled: bool, // from Output::auto_copy_metadata
    skip_global_copy: bool,
    skip_stream_copy: bool,
) -> Result<(), String> {
    if output_ctx.is_null() {
        return Err("Output AVFormatContext is null".to_string());
    }

    if !auto_copy_enabled {
        // User disabled auto-copy with disable_auto_copy_metadata()
        return Ok(());
    }

    let output_ref = &mut *output_ctx;

    // 1. Copy global metadata from first input (if exists)
    if !skip_global_copy && !input_ctxs.is_empty() && !input_ctxs[0].is_null() {
        let first_input = &*input_ctxs[0];

        // Copy global metadata with DONT_OVERWRITE to preserve user settings
        av_dict_copy(
            &mut output_ref.metadata,
            first_input.metadata,
            AV_DICT_DONT_OVERWRITE,
        );

        // Remove specific keys per FFmpeg's default behavior
        // These keys are typically encoder/software specific and shouldn't be copied
        av_dict_set(
            &mut output_ref.metadata,
            b"creation_time\0".as_ptr() as *const std::os::raw::c_char,
            std::ptr::null(),
            0,
        );
        av_dict_set(
            &mut output_ref.metadata,
            b"company_name\0".as_ptr() as *const std::os::raw::c_char,
            std::ptr::null(),
            0,
        );
        av_dict_set(
            &mut output_ref.metadata,
            b"product_name\0".as_ptr() as *const std::os::raw::c_char,
            std::ptr::null(),
            0,
        );
        av_dict_set(
            &mut output_ref.metadata,
            b"product_version\0".as_ptr() as *const std::os::raw::c_char,
            std::ptr::null(),
            0,
        );

        // Remove duration if recording_time is set (output duration will be different)
        if recording_time_set {
            av_dict_set(
                &mut output_ref.metadata,
                b"duration\0".as_ptr() as *const std::os::raw::c_char,
                std::ptr::null(),
                0,
            );
        }
    }

    // 2. Copy stream metadata from corresponding input streams
    if !skip_stream_copy && output_stream_count > 0 && !output_ref.streams.is_null() {
        let output_streams =
            std::slice::from_raw_parts_mut(output_ref.streams, output_stream_count);

        for (output_idx, (input_file_idx, input_stream_idx)) in stream_input_mapping {
            let output_idx = *output_idx;
            let input_file_idx = *input_file_idx;
            let input_stream_idx = *input_stream_idx;

            // Validate indices
            if output_idx >= output_stream_count {
                log::warn!("Invalid output stream index {}", output_idx);
                continue;
            }

            if input_file_idx >= input_ctxs.len() {
                log::warn!("Invalid input file index {}", input_file_idx);
                continue;
            }

            let input_ctx = input_ctxs[input_file_idx];
            if input_ctx.is_null() {
                continue;
            }

            let input_ref = &*input_ctx;
            if input_stream_idx >= input_ref.nb_streams as usize {
                log::warn!("Invalid input stream index {}", input_stream_idx);
                continue;
            }

            let output_stream_ptr = output_streams[output_idx];
            if output_stream_ptr.is_null() {
                continue;
            }
            let output_stream = &mut *output_stream_ptr;

            let input_streams =
                std::slice::from_raw_parts(input_ref.streams, input_ref.nb_streams as usize);
            let input_stream_ptr = input_streams[input_stream_idx];
            if input_stream_ptr.is_null() {
                continue;
            }
            let input_stream = &*input_stream_ptr;

            // Copy stream metadata with DONT_OVERWRITE to preserve user settings
            av_dict_copy(
                &mut output_stream.metadata,
                input_stream.metadata,
                AV_DICT_DONT_OVERWRITE,
            );

            // Remove encoder key for streams being encoded
            // (we'll be setting our own encoder info)
            if encoding_streams.contains(&output_idx) {
                av_dict_set(
                    &mut output_stream.metadata,
                    b"encoder\0".as_ptr() as *const std::os::raw::c_char,
                    std::ptr::null(),
                    0,
                );
            }
        }
    }

    Ok(())
}

/// Copy chapters from an input AVFormatContext into the output context, emulating
/// FFmpeg's `copy_chapters` (ffmpeg_mux_init.c:2784-2824).
pub unsafe fn copy_chapters_from_input(
    input_ctx: *const AVFormatContext,
    input_ts_offset_us: i64,
    output_ctx: *mut AVFormatContext,
    mux_start_time_us: Option<i64>,
    mux_recording_time_us: Option<i64>,
    copy_metadata: bool,
) -> Result<(), String> {
    if input_ctx.is_null() || output_ctx.is_null() {
        return Err("AVFormatContext is null".to_string());
    }

    let input_ref = &*input_ctx;
    if input_ref.nb_chapters == 0 {
        return Ok(());
    }

    let output_ref = &mut *output_ctx;
    let existing = output_ref.nb_chapters as usize;
    let additional = input_ref.nb_chapters as usize;
    // Keep the count total and representable as the c_uint `nb_chapters` field:
    // checked_add rejects a 32-bit usize overflow, and the u32::MAX ceiling
    // rejects a capacity that could not be counted without wrapping the field
    // (av_max_alloc is caller-tunable, so a rejected allocation is not a
    // reliable backstop). Either guard returns Err before allocating, so the
    // error path has nothing to orphan.
    let total_capacity = existing
        .checked_add(additional)
        .ok_or_else(|| "Chapter count overflow".to_string())?;
    if total_capacity > u32::MAX as usize {
        return Err("Chapter count exceeds AVFormatContext capacity".to_string());
    }

    // av_realloc_array keeps the same overflow-checked nelem*elsize contract as
    // av_realloc_f but leaves the existing table untouched on failure, so
    // `output_ref.chapters` stays valid for avformat_free_context (av_realloc_f
    // frees the old table on failure, which would leave the pointer dangling).
    let tmp = av_realloc_array(
        output_ref.chapters as *mut c_void,
        total_capacity.max(1),
        mem::size_of::<*mut AVChapter>(),
    ) as *mut *mut AVChapter;
    if tmp.is_null() {
        return Err("Failed to grow chapter table".to_string());
    }
    output_ref.chapters = tmp;

    let start_time = mux_start_time_us.unwrap_or(0);
    let recording_time = mux_recording_time_us.unwrap_or(i64::MAX);

    let chapters = std::slice::from_raw_parts(input_ref.chapters, input_ref.nb_chapters as usize);

    for &chapter_ptr in chapters {
        if chapter_ptr.is_null() {
            continue;
        }
        let in_ch: &AVChapter = &*chapter_ptr;

        let delta = clamp_i128_to_i64(start_time as i128 - input_ts_offset_us as i128);
        let ts_off = av_rescale_q(delta, AV_TIME_BASE_Q, in_ch.time_base);
        let rt = if recording_time == i64::MAX {
            i64::MAX
        } else {
            av_rescale_q(recording_time, AV_TIME_BASE_Q, in_ch.time_base)
        };

        if in_ch.end < ts_off {
            continue;
        }
        if rt != i64::MAX {
            let limit = ts_off.saturating_add(rt);
            if in_ch.start > limit {
                break;
            }
        }

        let mut start = in_ch.start.saturating_sub(ts_off);
        start = max(0, start);

        let mut end = in_ch.end.saturating_sub(ts_off);
        if rt != i64::MAX {
            end = min(end, rt);
        }
        end = max(end, start);

        let out_ptr = av_mallocz(mem::size_of::<AVChapter>()) as *mut AVChapter;
        if out_ptr.is_null() {
            return Err("Failed to allocate chapter".to_string());
        }

        (*out_ptr).id = in_ch.id;
        (*out_ptr).time_base = in_ch.time_base;
        (*out_ptr).start = start;
        (*out_ptr).end = end;

        // Commit ownership to the context at the store, mirroring upstream
        // `os->chapters[os->nb_chapters++] = out_ch`: any later early return
        // leaves every stored chapter counted, so avformat_free_context frees
        // it instead of orphaning it. total_capacity <= u32::MAX (checked above)
        // guarantees the `+= 1` fits the c_uint field; the debug_assert is the
        // table-bounds tripwire for the ASAN lane's raw store.
        debug_assert!((output_ref.nb_chapters as usize) < total_capacity);
        *output_ref.chapters.add(output_ref.nb_chapters as usize) = out_ptr;
        output_ref.nb_chapters += 1;

        if copy_metadata {
            av_dict_copy(&mut (*out_ptr).metadata, in_ch.metadata, 0);
        }
    }

    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use ffmpeg_sys_next::{
        av_dict_get, av_malloc, avformat_alloc_context, avformat_free_context, AVDictionary,
        AVRational,
    };
    use std::ffi::{CStr, CString};
    use std::ptr;

    /// Build an input context whose chapter table is fully av_malloc-family
    /// allocated, so `avformat_free_context` reclaims everything (table,
    /// chapter structs, metadata dicts). Each spec is (id, start, end, title)
    /// in a 1/1000 time base.
    unsafe fn make_input_ctx(specs: &[(i64, i64, i64, &str)]) -> *mut AVFormatContext {
        let ctx = avformat_alloc_context();
        assert!(!ctx.is_null());

        let table = av_malloc(specs.len() * mem::size_of::<*mut AVChapter>());
        assert!(!table.is_null());
        let table = table as *mut *mut AVChapter;

        for (i, &(id, start, end, title)) in specs.iter().enumerate() {
            let ch = av_mallocz(mem::size_of::<AVChapter>()) as *mut AVChapter;
            assert!(!ch.is_null());
            (*ch).id = id;
            (*ch).time_base = AVRational { num: 1, den: 1000 };
            (*ch).start = start;
            (*ch).end = end;
            let title_c = CString::new(title).unwrap();
            av_dict_set(&mut (*ch).metadata, c"title".as_ptr(), title_c.as_ptr(), 0);
            *table.add(i) = ch;
        }

        (*ctx).chapters = table;
        (*ctx).nb_chapters = specs.len() as u32;
        ctx
    }

    /// Read a dict value as an owned String (None when the key is absent).
    unsafe fn dict_str(dict: *mut AVDictionary, key: &str) -> Option<String> {
        let key_c = CString::new(key).unwrap();
        let entry = av_dict_get(dict, key_c.as_ptr(), ptr::null(), 0);
        if entry.is_null() {
            None
        } else {
            Some(
                CStr::from_ptr((*entry).value)
                    .to_string_lossy()
                    .into_owned(),
            )
        }
    }

    #[test]
    fn copies_chapters_and_commits_count() {
        unsafe {
            let input = make_input_ctx(&[(1, 0, 10_000, "one"), (2, 10_000, 20_000, "two")]);
            let output = avformat_alloc_context();
            assert!(!output.is_null());

            copy_chapters_from_input(input, 0, output, None, None, true)
                .expect("copy_chapters_from_input failed");
            assert_eq!((*output).nb_chapters, 2);

            let ch0 = *(*output).chapters.add(0);
            assert_eq!((*ch0).id, 1);
            assert_eq!((*ch0).start, 0);
            assert_eq!((*ch0).end, 10_000);
            assert_eq!(dict_str((*ch0).metadata, "title").as_deref(), Some("one"));

            let ch1 = *(*output).chapters.add(1);
            assert_eq!((*ch1).id, 2);
            assert_eq!((*ch1).start, 10_000);
            assert_eq!((*ch1).end, 20_000);
            assert_eq!(dict_str((*ch1).metadata, "title").as_deref(), Some("two"));

            avformat_free_context(input);
            avformat_free_context(output);
        }
    }

    #[test]
    fn second_call_appends_to_existing_table() {
        unsafe {
            let input_a = make_input_ctx(&[(1, 0, 10_000, "a1"), (2, 10_000, 20_000, "a2")]);
            let input_b = make_input_ctx(&[(3, 0, 5_000, "b1"), (4, 5_000, 9_000, "b2")]);
            let output = avformat_alloc_context();
            assert!(!output.is_null());

            copy_chapters_from_input(input_a, 0, output, None, None, true)
                .expect("first copy failed");
            // The second call grows a non-NULL table: this is exactly the
            // av_realloc_array call whose failure semantics changed, plus the
            // existing > 0 offset arithmetic.
            copy_chapters_from_input(input_b, 0, output, None, None, true)
                .expect("second copy failed");
            assert_eq!((*output).nb_chapters, 4);

            // Entries from the first call survive the reallocation intact.
            let ch0 = *(*output).chapters.add(0);
            assert_eq!((*ch0).id, 1);
            assert_eq!(dict_str((*ch0).metadata, "title").as_deref(), Some("a1"));
            let ch1 = *(*output).chapters.add(1);
            assert_eq!((*ch1).id, 2);

            let ch2 = *(*output).chapters.add(2);
            assert_eq!((*ch2).id, 3);
            assert_eq!((*ch2).start, 0);
            assert_eq!((*ch2).end, 5_000);
            let ch3 = *(*output).chapters.add(3);
            assert_eq!((*ch3).id, 4);
            assert_eq!(dict_str((*ch3).metadata, "title").as_deref(), Some("b2"));

            avformat_free_context(input_a);
            avformat_free_context(input_b);
            avformat_free_context(output);
        }
    }

    #[test]
    fn recording_time_break_commits_partial() {
        unsafe {
            // Chapters at [0s, 10s] and [60s, 70s] in a 1/1000 time base.
            let input = make_input_ctx(&[(1, 0, 10_000, "kept"), (2, 60_000, 70_000, "cut")]);
            let output = avformat_alloc_context();
            assert!(!output.is_null());

            // A 20s recording window: the second chapter starts past the
            // limit and triggers the loop break, so exactly one chapter must
            // be committed to the context.
            copy_chapters_from_input(input, 0, output, None, Some(20_000_000), true)
                .expect("copy_chapters_from_input failed");
            assert_eq!((*output).nb_chapters, 1);

            let ch0 = *(*output).chapters.add(0);
            assert_eq!((*ch0).id, 1);
            assert_eq!(dict_str((*ch0).metadata, "title").as_deref(), Some("kept"));

            avformat_free_context(input);
            avformat_free_context(output);
        }
    }
}
