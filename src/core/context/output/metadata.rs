use super::Output;
use std::collections::HashMap;

impl Output {
    // ========== Metadata API Methods ==========
    // The following helpers mirror FFmpeg's command-line metadata options as implemented in
    // fftools/ffmpeg_opt.c (opt_metadata / opt_map_metadata) and the automatic propagation rules
    // in fftools/ffmpeg_mux_init.c:2913-2983. Each method references the corresponding FFmpeg
    // behavior so callers can cross-check the C implementation when needed.

    /// Add or update global metadata for the output file.
    ///
    /// If value is empty string, the key is removed from the output file
    /// (FFmpeg behavior). This also removes a key that was automatically
    /// copied from the input, matching the CLI's `-metadata key=`.
    /// Replicates FFmpeg's `-metadata key=value` option.
    ///
    /// FFmpeg reference: fftools/ffmpeg_opt.c (`opt_metadata()` handles `-metadata key=value`).
    ///
    /// # Examples
    /// ```rust,ignore
    /// let output = Output::from("output.mp4")
    ///     .add_metadata("title", "My Video")
    ///     .add_metadata("author", "John Doe");
    /// ```
    pub fn add_metadata(mut self, key: impl Into<String>, value: impl Into<String>) -> Self {
        let key = key.into();
        let value = value.into();

        // An empty value is FFmpeg's deletion spelling (`-metadata key=`) and is
        // kept in the map as a marker rather than resolved here. The marker must
        // survive to `of_add_metadata`, which runs AFTER the automatic
        // input->output copy: deleting the key at this point would only drop it
        // from the builder's own configuration, leaving the copy's value on the
        // output.
        self.global_metadata
            .get_or_insert_with(HashMap::new)
            .insert(key, value);
        self
    }

    /// Add multiple global metadata entries at once.
    ///
    /// FFmpeg reference: fftools/ffmpeg_opt.c (consecutive `-metadata` invocations append to the
    /// same dictionary; this helper simply batches the calls on the Rust side).
    ///
    /// # Examples
    /// ```rust,ignore
    /// let mut metadata = HashMap::new();
    /// metadata.insert("title".to_string(), "My Video".to_string());
    /// metadata.insert("author".to_string(), "John Doe".to_string());
    ///
    /// let output = Output::from("output.mp4")
    ///     .add_metadata_map(metadata);
    /// ```
    pub fn add_metadata_map(mut self, metadata: HashMap<String, String>) -> Self {
        for (key, value) in metadata {
            self = self.add_metadata(key, value);
        }
        self
    }

    /// Remove a global metadata key from the output file.
    ///
    /// This removes a key that was automatically copied from the input as well
    /// as any value set through [`Self::add_metadata`]. Implemented by leaving
    /// an empty-value deletion marker for `of_add_metadata` rather than by
    /// clearing the builder's own configuration, so the removal is applied
    /// after the automatic input->output copy.
    ///
    /// FFmpeg reference: fftools/ffmpeg_opt.c (`-metadata key=` deletes the key when value is
    /// empty; we follow the same rule by interpreting an empty string as removal).
    ///
    /// # Examples
    /// ```rust,ignore
    /// let output = Output::from("output.mp4")
    ///     .add_metadata("title", "My Video")
    ///     .remove_metadata("title");  // Remove the title
    /// ```
    pub fn remove_metadata(mut self, key: &str) -> Self {
        self.global_metadata
            .get_or_insert_with(HashMap::new)
            .insert(key.to_string(), String::new());
        self
    }

    /// Clear all metadata (global, stream, chapter, program) and mappings.
    ///
    /// Useful when you want to start fresh without any metadata. Disables the
    /// automatic input->output metadata copy, so keys inherited from the input
    /// do not reappear afterwards.
    ///
    /// FFmpeg reference: fftools/ffmpeg_opt.c (users typically issue `-map_metadata -1` and then
    /// reapply `-metadata` options; this helper emulates that workflow programmatically).
    ///
    /// # Examples
    /// ```rust,ignore
    /// let output = Output::from("output.mp4")
    ///     .add_metadata("title", "My Video")
    ///     .clear_all_metadata();  // Remove all metadata
    /// ```
    pub fn clear_all_metadata(mut self) -> Self {
        self.global_metadata = None;
        self.stream_metadata.clear();
        self.chapter_metadata.clear();
        self.program_metadata.clear();
        self.metadata_map.clear();
        // `-map_metadata -1` also stops the automatic copy. Without this the
        // cleared state is refilled by `copy_metadata_default`, which runs
        // before `of_add_metadata` and has no marker to suppress it.
        self.auto_copy_metadata = false;
        self
    }

    /// Disable automatic metadata copying from input files.
    ///
    /// By default, FFmpeg automatically copies global and stream metadata
    /// from input files to output. This method disables that behavior,
    /// similar to FFmpeg's `-map_metadata -1` option.
    /// FFmpeg reference: ffmpeg_mux_init.c (`copy_meta()` sets metadata_global_manual when
    /// `-map_metadata -1` is used; `auto_copy_metadata` mirrors the same flag).
    ///
    /// # Examples
    /// ```rust,ignore
    /// let output = Output::from("output.mp4")
    ///     .disable_auto_copy_metadata()  // Don't copy any metadata from input
    ///     .add_metadata("title", "New Title");  // Only use explicitly set metadata
    /// ```
    pub fn disable_auto_copy_metadata(mut self) -> Self {
        self.auto_copy_metadata = false;
        self
    }

    /// Add or update stream-specific metadata.
    ///
    /// Uses FFmpeg's stream specifier syntax to identify target streams.
    /// If value is empty string, the key will be removed (FFmpeg behavior).
    /// Replicates FFmpeg's `-metadata:s:spec key=value` option.
    ///
    /// FFmpeg reference: fftools/ffmpeg_opt.c (`opt_metadata()` with stream specifiers, lines
    /// 2465-2520 in FFmpeg 7.x).
    ///
    /// # Stream Specifier Syntax
    /// - `"v:0"` - First video stream
    /// - `"a:1"` - Second audio stream
    /// - `"s"` - All subtitle streams
    /// - `"v"` - All video streams
    /// - `"p:0:v"` - Video streams in program 0
    /// - `"#0x100"` or `"i:256"` - Stream with specific ID
    /// - `"m:language:eng"` - Streams with metadata language=eng
    /// - `"u"` - Usable streams only
    /// - `"disp:default"` - Streams with default disposition
    ///
    /// # Examples
    /// ```rust,ignore
    /// let output = Output::from("output.mp4")
    ///     .add_stream_metadata("v:0", "language", "eng")
    ///     .add_stream_metadata("a:0", "title", "Main Audio");
    /// ```
    ///
    /// # Errors
    /// Returns error if the stream specifier syntax is invalid.
    pub fn add_stream_metadata(
        mut self,
        stream_spec: impl Into<String>,
        key: impl Into<String>,
        value: impl Into<String>,
    ) -> Result<Self, String> {
        use crate::core::metadata::StreamSpecifier;

        let stream_spec_str = stream_spec.into();
        let key = key.into();
        let value = value.into();

        // Parse and validate stream specifier
        let _specifier = StreamSpecifier::parse(&stream_spec_str)?;

        // Store as (spec, key, value) tuple
        // During output initialization, this will be matched against actual streams
        // using StreamSpecifier::matches and applied to all matching streams
        // Replicates FFmpeg's of_add_metadata behavior
        self.stream_metadata.push((stream_spec_str, key, value));

        Ok(self)
    }

    /// Add or update chapter-specific metadata.
    ///
    /// Chapters are used for DVD-like navigation points in media files.
    /// If value is empty string, the key is removed from the chapter
    /// (FFmpeg behavior). As with global metadata, the empty value is kept as a
    /// deletion marker so it is applied after chapters are copied from the input.
    /// Replicates FFmpeg's `-metadata:c:N key=value` option.
    /// FFmpeg reference: fftools/ffmpeg_opt.c (`opt_metadata()` handles the `c:` target selector).
    ///
    /// # Examples
    /// ```rust,ignore
    /// let output = Output::from("output.mp4")
    ///     .add_chapter_metadata(0, "title", "Introduction")
    ///     .add_chapter_metadata(1, "title", "Main Content");
    /// ```
    pub fn add_chapter_metadata(
        mut self,
        chapter_index: usize,
        key: impl Into<String>,
        value: impl Into<String>,
    ) -> Self {
        let key = key.into();
        let value = value.into();

        // Empty value = deletion marker; see `add_metadata` for why it is not
        // resolved here.
        self.chapter_metadata
            .entry(chapter_index)
            .or_default()
            .insert(key, value);
        self
    }

    /// Add or update program-specific metadata.
    ///
    /// Programs are used in multi-program transport streams (e.g., MPEG-TS).
    /// If value is empty string, the key is removed from the program
    /// (FFmpeg behavior). As with global metadata, the empty value is kept as a
    /// deletion marker so it is applied after metadata is copied from the input.
    /// Replicates FFmpeg's `-metadata:p:N key=value` option.
    /// FFmpeg reference: fftools/ffmpeg_opt.c (`opt_metadata()` with `p:` selector).
    ///
    /// # Examples
    /// ```rust,ignore
    /// let output = Output::from("output.ts")
    ///     .add_program_metadata(0, "service_name", "Channel 1")
    ///     .add_program_metadata(1, "service_name", "Channel 2");
    /// ```
    pub fn add_program_metadata(
        mut self,
        program_index: usize,
        key: impl Into<String>,
        value: impl Into<String>,
    ) -> Self {
        let key = key.into();
        let value = value.into();

        // Empty value = deletion marker; see `add_metadata` for why it is not
        // resolved here.
        self.program_metadata
            .entry(program_index)
            .or_default()
            .insert(key, value);
        self
    }

    /// Map metadata from an input file to this output.
    ///
    /// Replicates FFmpeg's `-map_metadata [src_file_idx]:src_type:dst_type` option.
    /// This allows copying metadata from specific locations in input files to
    /// specific locations in the output file.
    ///
    /// # Type Specifiers
    /// - `"g"` or `""` - Global metadata
    /// - `"s"` or `"s:spec"` - Stream metadata (with optional stream specifier)
    /// - `"c:N"` - Chapter N metadata
    /// - `"p:N"` - Program N metadata
    ///
    /// # Examples
    /// ```rust,ignore
    /// use ez_ffmpeg::core::metadata::{MetadataType, MetadataMapping};
    ///
    /// let output = Output::from("output.mp4")
    ///     // Copy global metadata from input 0 to output global
    ///     .map_metadata_from_input(0, "g", "g")?
    ///     // Copy first video stream metadata from input 1 to output first video stream
    ///     .map_metadata_from_input(1, "s:v:0", "s:v:0")?;
    /// ```
    ///
    /// # Errors
    /// Returns error if the type specifier syntax is invalid.
    /// FFmpeg reference: fftools/ffmpeg_opt.c (`opt_map_metadata()` parses the same
    /// `[file][:type]` triplet and feeds it into `MetadataMapping`).
    pub fn map_metadata_from_input(
        mut self,
        input_index: usize,
        src_type_spec: impl Into<String>,
        dst_type_spec: impl Into<String>,
    ) -> Result<Self, String> {
        use crate::core::metadata::{MetadataMapping, MetadataType};

        let src_type = MetadataType::parse(&src_type_spec.into())?;
        let dst_type = MetadataType::parse(&dst_type_spec.into())?;

        self.metadata_map.push(MetadataMapping {
            src_type,
            dst_type,
            input_index,
        });

        Ok(self)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    // These pin the DELETION-MARKER contract at the setter level. The
    // end-to-end behavior is covered by `tests/metadata.rs`; what matters here
    // is that an empty value survives as an empty-string entry, because
    // `of_add_metadata` is what turns it into `av_dict_set(key, NULL, 0)` and
    // it runs AFTER the automatic input->output copy. A setter that resolves
    // the empty value locally (by dropping the key) would leave the copied
    // value on the output — the bug these tests exist to prevent.

    #[test]
    fn empty_global_value_is_kept_as_deletion_marker() {
        let output = Output::from("out.mp4")
            .add_metadata("artist", "Someone")
            .add_metadata("artist", "");

        let map = output.global_metadata.expect("map exists");
        assert_eq!(
            map.get("artist").map(String::as_str),
            Some(""),
            "empty value must survive as a marker, not be dropped: {map:?}"
        );
    }

    #[test]
    fn remove_metadata_records_a_deletion_marker() {
        let output = Output::from("out.mp4").remove_metadata("artist");

        let map = output
            .global_metadata
            .expect("remove_metadata must record a marker even when nothing was set");
        assert_eq!(map.get("artist").map(String::as_str), Some(""));
    }

    #[test]
    fn last_write_wins_for_global_metadata() {
        // Delete then set: the later value wins.
        let output = Output::from("out.mp4")
            .add_metadata("artist", "")
            .add_metadata("artist", "Final");
        assert_eq!(
            output
                .global_metadata
                .expect("map")
                .get("artist")
                .map(String::as_str),
            Some("Final")
        );
    }

    #[test]
    fn empty_chapter_value_is_kept_as_deletion_marker() {
        let output = Output::from("out.mkv")
            .add_chapter_metadata(0, "title", "One")
            .add_chapter_metadata(0, "title", "");

        let map = output.chapter_metadata.get(&0).expect("chapter 0 entry");
        assert_eq!(
            map.get("title").map(String::as_str),
            Some(""),
            "empty chapter value must survive as a marker: {map:?}"
        );
    }

    #[test]
    fn empty_program_value_is_kept_as_deletion_marker() {
        let output = Output::from("out.ts")
            .add_program_metadata(0, "service_name", "Ch1")
            .add_program_metadata(0, "service_name", "");

        let map = output.program_metadata.get(&0).expect("program 0 entry");
        assert_eq!(
            map.get("service_name").map(String::as_str),
            Some(""),
            "empty program value must survive as a marker: {map:?}"
        );
    }

    #[test]
    fn clear_all_metadata_disables_auto_copy() {
        // The whole point of the clear is that the automatic input->output
        // copy must not refill the container afterwards.
        let output = Output::from("out.mp4")
            .add_metadata("title", "T")
            .clear_all_metadata();

        assert!(!output.auto_copy_metadata, "auto-copy must be disabled");
        assert!(output.global_metadata.is_none());
        assert!(output.stream_metadata.is_empty());
        assert!(output.chapter_metadata.is_empty());
        assert!(output.program_metadata.is_empty());
        assert!(output.metadata_map.is_empty());
    }
}
