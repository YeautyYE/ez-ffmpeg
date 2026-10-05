use crate::core::context::demuxer::Demuxer;
use crate::core::context::muxer::Muxer;
use crate::core::metadata::StreamSpecifier;
use ffmpeg_sys_next::{
    av_opt_set, AVMediaType, AV_DISPOSITION_ATTACHED_PIC, AV_DISPOSITION_DEFAULT,
};
use std::ffi::CString;


/// Port of `set_dispositions` in fftools/ffmpeg_mux_init.c:3037-3101.
///
/// # Safety
/// The muxer's output context and all demuxers' input contexts must remain
/// live, with valid stream pointers and stream-source indices, for this call.
pub(super) unsafe fn set_dispositions(mux: &Muxer, demuxs: &[Demuxer]) -> Result<(), String> {
    let ctx = mux.out_fmt_ctx_ptr();
    if ctx.is_null() {
        return Err("Output format context is null".to_string());
    }

    let output_streams = (*ctx).nb_streams as usize;
    // AVMEDIA_TYPE_UNKNOWN is -1, so index every media type by type + 1.
    let mut nb_streams = [0usize; AVMediaType::AVMEDIA_TYPE_NB as usize + 1];
    let mut have_default = [false; AVMediaType::AVMEDIA_TYPE_NB as usize + 1];
    let mut have_manual = false;
    let mut dispositions = Vec::with_capacity(output_streams);

    let mut sources = vec![None; output_streams];
    for (output_index, input) in mux.stream_input_mapping() {
        sources[output_index] = Some(input);
    }

    let options = mux
        .dispositions
        .iter()
        .map(|(spec, value)| {
            // The empty specifier is the CLI's unqualified `-disposition` and
            // matches every stream; `parse` rejects it, so use the wildcard
            // `default()` specifier (list type `All`, no media type).
            let specifier = if spec.is_empty() {
                StreamSpecifier::default()
            } else {
                StreamSpecifier::parse(spec).map_err(|err| {
                    format!("Invalid disposition stream specifier '{spec}': {err}")
                })?
            };
            Ok((specifier, spec.as_str(), value.as_str()))
        })
        .collect::<Result<Vec<_>, String>>()?;

    // Pass 1: count every output stream, resolve last-matching manual values,
    // and overwrite input-backed output dispositions with their input values.
    for (index, source) in sources.iter().enumerate() {
        let stream = *(*ctx).streams.add(index);
        let media_type = (*(*stream).codecpar).codec_type as i32 + 1;
        let type_index = usize::try_from(media_type)
            .ok()
            .filter(|&idx| idx < nb_streams.len())
            .ok_or_else(|| format!("Invalid media type for output stream {index}"))?;
        nb_streams[type_index] += 1;

        let mut selected = None;
        let mut selected_spec = "";
        let mut matches = 0;
        for (specifier, spec, value) in &options {
            if specifier.matches(ctx, stream) {
                selected = Some(*value);
                selected_spec = spec;
                matches += 1;
            }
        }
        if matches > 1 {
            log::warn!(target: super::LOG_TARGET,
                "Multiple -disposition options specified for stream {index}, only the last option '-disposition:{selected_spec} {}' will be used",
                selected.expect("at least two matching options")
            );
        }
        have_manual |= selected.is_some();
        dispositions.push(selected);

        if let Some(&(input_file, input_stream)) = source.as_ref() {
            let input_ctx = demuxs[input_file].in_fmt_ctx_ptr();
            let input = *(*input_ctx).streams.add(input_stream);
            (*stream).disposition = (*input).disposition;
            if (*stream).disposition & AV_DISPOSITION_DEFAULT != 0 {
                have_default[type_index] = true;
            }
        }
    }

    if have_manual {
        // Pass 2a: manual AVOptions override the copied value on matched streams.
        for (index, disposition) in dispositions.iter().enumerate() {
            let Some(disposition) = disposition else {
                continue;
            };
            let stream = *(*ctx).streams.add(index);
            let value = CString::new(*disposition).map_err(|_| {
                format!("Disposition for output stream {index} contains a NUL byte")
            })?;
            let ret = av_opt_set(stream.cast(), c"disposition".as_ptr(), value.as_ptr(), 0);
            if ret < 0 {
                return Err(format!(
                    "Invalid disposition '{disposition}' for output stream {index}: FFmpeg error {ret}"
                ));
            }
        }
    } else {
        // Pass 2b: without any matching manual option, mark the first suitable
        // stream of each multi-stream type as default, skipping attached pictures.
        for index in 0..output_streams {
            let stream = *(*ctx).streams.add(index);
            let type_index = usize::try_from((*(*stream).codecpar).codec_type as i32 + 1)
                .ok()
                .filter(|&idx| idx < nb_streams.len())
                .ok_or_else(|| format!("Invalid media type for output stream {index}"))?;
            if nb_streams[type_index] < 2
                || have_default[type_index]
                || (*stream).disposition & AV_DISPOSITION_ATTACHED_PIC != 0
            {
                continue;
            }
            (*stream).disposition |= AV_DISPOSITION_DEFAULT;
            have_default[type_index] = true;
        }
    }

    Ok(())
}
