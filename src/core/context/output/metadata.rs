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
    /// If value is empty string, the key will be removed (FFmpeg behavior).
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

        if value.is_empty() {
            // Empty value means remove the key (FFmpeg behavior)
            if let Some(ref mut metadata) = self.global_metadata {
                metadata.remove(&key);
            }
        } else {
            self.global_metadata
                .get_or_insert_with(HashMap::new)
                .insert(key, value);
        }
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

    /// Remove a global metadata key.
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
        if let Some(ref mut metadata) = self.global_metadata {
            metadata.remove(key);
        }
        self
    }

    /// Clear all metadata (global, stream, chapter, program) and mappings.
    ///
    /// Useful when you want to start fresh without any metadata.
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
    /// If value is empty string, the key will be removed (FFmpeg behavior).
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

        if value.is_empty() {
            // Empty value means remove the key (FFmpeg behavior)
            if let Some(metadata) = self.chapter_metadata.get_mut(&chapter_index) {
                metadata.remove(&key);
            }
        } else {
            self.chapter_metadata
                .entry(chapter_index)
                .or_default()
                .insert(key, value);
        }
        self
    }

    /// Add or update program-specific metadata.
    ///
    /// Programs are used in multi-program transport streams (e.g., MPEG-TS).
    /// If value is empty string, the key will be removed (FFmpeg behavior).
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

        if value.is_empty() {
            // Empty value means remove the key (FFmpeg behavior)
            if let Some(metadata) = self.program_metadata.get_mut(&program_index) {
                metadata.remove(&key);
            }
        } else {
            self.program_metadata
                .entry(program_index)
                .or_default()
                .insert(key, value);
        }
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
