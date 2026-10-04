//! Regression coverage for the metadata deletion contract.
//!
//! FFmpeg's `-metadata key=` deletes the key from the OUTPUT — including a key
//! the automatic input->output copy had just placed there. An empty value is
//! therefore a deletion marker that must survive to `of_add_metadata`, which
//! runs after the automatic copy. These tests pin that end-to-end: each
//! deletion spelling is applied to a key the input carries, and the output must
//! not have it. The replacement case is the control — adding a non-empty value
//! must still update the inherited key, so a "fix" that simply dropped the
//! automatic copy would fail here.

use ez_ffmpeg::container_info::{get_chapter_metadata, get_metadata};
use ez_ffmpeg::{FfmpegContext, FfmpegScheduler, Input, Output};

mod common;
use common::tmp_path_in;

const INHERITED_KEY: &str = "artist";
const INHERITED_VALUE: &str = "OriginalArtist";
const TITLE_KEY: &str = "title";

fn tmp_path(name: &str) -> String {
    tmp_path_in("ez_ffmpeg_metadata", name)
}

/// Runs one build+start+wait cycle, panicking with the job's own error.
fn run(context: FfmpegContext) {
    FfmpegScheduler::new(context)
        .start()
        .expect("job failed to start")
        .wait()
        .expect("job failed");
}

/// A tiny video carrying the two global keys these tests operate on. mpeg4
/// keeps the fixture portable (no libx264) and runs on every FFmpeg release
/// the crate supports.
fn fixture_with_metadata(name: &str) -> String {
    let path = tmp_path(name);
    let context = FfmpegContext::builder()
        .input(Input::from("testsrc2=size=64x48:rate=10:duration=1").set_format("lavfi"))
        .output(
            Output::from(path.as_str())
                .set_video_codec("mpeg4")
                .add_metadata(INHERITED_KEY, INHERITED_VALUE)
                .add_metadata(TITLE_KEY, "Fixture Title"),
        )
        .build()
        .expect("fixture build");
    run(context);
    path
}

/// The container's metadata as lowercase key/value pairs. Container writers
/// normalize metadata key case (mp4 lowercases), so comparisons here are
/// case-insensitive by construction.
fn metadata_of(path: &str) -> Vec<(String, String)> {
    get_metadata(path)
        .expect("metadata probe failed")
        .into_iter()
        .map(|(k, v)| (k.to_lowercase(), v))
        .collect()
}

fn value_of(meta: &[(String, String)], key: &str) -> Option<String> {
    meta.iter()
        .find(|(k, _)| k == key)
        .map(|(_, v)| v.clone())
}

/// Sanity check for every scenario below: the fixture must actually carry the
/// inherited key, otherwise "the output lacks it" would pass vacuously.
fn assert_fixture_carries_the_key(path: &str) {
    let meta = metadata_of(path);
    assert_eq!(
        value_of(&meta, INHERITED_KEY).as_deref(),
        Some(INHERITED_VALUE),
        "fixture does not carry {INHERITED_KEY}; got {meta:?}"
    );
}

/// `-metadata key=` spelled as an empty `add_metadata` value must delete the
/// key that the automatic input->output copy placed on the output.
#[test]
fn empty_value_add_metadata_deletes_inherited_global_key() {
    let input = fixture_with_metadata("empty_value_in.mp4");
    assert_fixture_carries_the_key(&input);

    let out = tmp_path("empty_value_out.mp4");
    let context = FfmpegContext::builder()
        .input(input.as_str())
        .output(Output::from(out.as_str()).add_metadata(INHERITED_KEY, ""))
        .build()
        .expect("build");
    run(context);

    let meta = metadata_of(&out);
    assert_eq!(
        value_of(&meta, INHERITED_KEY),
        None,
        "empty-value add_metadata left {INHERITED_KEY} on the output: {meta:?}"
    );
    // The deletion is surgical: an unrelated inherited key stays.
    assert_eq!(
        value_of(&meta, TITLE_KEY).as_deref(),
        Some("Fixture Title"),
        "deleting one key dropped an unrelated one: {meta:?}"
    );
}

/// `remove_metadata(key)` must reach the output too, not just the builder's
/// own configuration map.
#[test]
fn remove_metadata_deletes_inherited_global_key() {
    let input = fixture_with_metadata("remove_in.mp4");
    assert_fixture_carries_the_key(&input);

    let out = tmp_path("remove_out.mp4");
    let context = FfmpegContext::builder()
        .input(input.as_str())
        .output(Output::from(out.as_str()).remove_metadata(INHERITED_KEY))
        .build()
        .expect("build");
    run(context);

    let meta = metadata_of(&out);
    assert_eq!(
        value_of(&meta, INHERITED_KEY),
        None,
        "remove_metadata left {INHERITED_KEY} on the output: {meta:?}"
    );
}

/// `clear_all_metadata()` promises a fresh start, which means the automatic
/// input->output copy must not put the keys back afterwards.
#[test]
fn clear_all_metadata_drops_inherited_global_keys() {
    let input = fixture_with_metadata("clear_in.mp4");
    assert_fixture_carries_the_key(&input);

    let out = tmp_path("clear_out.mp4");
    let context = FfmpegContext::builder()
        .input(input.as_str())
        .output(Output::from(out.as_str()).clear_all_metadata())
        .build()
        .expect("build");
    run(context);

    let meta = metadata_of(&out);
    assert_eq!(
        value_of(&meta, INHERITED_KEY),
        None,
        "clear_all_metadata left {INHERITED_KEY}: {meta:?}"
    );
    assert_eq!(
        value_of(&meta, TITLE_KEY),
        None,
        "clear_all_metadata left {TITLE_KEY}: {meta:?}"
    );
}

/// Control: a non-empty value still updates the inherited key. Guard against a
/// "fix" that disables the automatic copy wholesale.
#[test]
fn replacement_value_updates_inherited_global_key() {
    let input = fixture_with_metadata("replace_in.mp4");
    assert_fixture_carries_the_key(&input);

    let out = tmp_path("replace_out.mp4");
    let context = FfmpegContext::builder()
        .input(input.as_str())
        .output(
            Output::from(out.as_str()).add_metadata(INHERITED_KEY, "ReplacementArtist"),
        )
        .build()
        .expect("build");
    run(context);

    let meta = metadata_of(&out);
    assert_eq!(
        value_of(&meta, INHERITED_KEY).as_deref(),
        Some("ReplacementArtist"),
        "replacement value did not win: {meta:?}"
    );
}

/// Last write wins in call order: setting then clearing leaves the key gone.
#[test]
fn empty_value_after_set_deletes_it() {
    let input = fixture_with_metadata("set_then_empty_in.mp4");

    let out = tmp_path("set_then_empty_out.mp4");
    let context = FfmpegContext::builder()
        .input(input.as_str())
        .output(
            Output::from(out.as_str())
                .add_metadata(INHERITED_KEY, "Temporary")
                .add_metadata(INHERITED_KEY, ""),
        )
        .build()
        .expect("build");
    run(context);

    let meta = metadata_of(&out);
    assert_eq!(
        value_of(&meta, INHERITED_KEY),
        None,
        "a later empty value did not delete an earlier set: {meta:?}"
    );
}

/// And the reverse: clearing then setting leaves the later value.
#[test]
fn set_after_empty_value_wins() {
    let input = fixture_with_metadata("empty_then_set_in.mp4");

    let out = tmp_path("empty_then_set_out.mp4");
    let context = FfmpegContext::builder()
        .input(input.as_str())
        .output(
            Output::from(out.as_str())
                .add_metadata(INHERITED_KEY, "")
                .add_metadata(INHERITED_KEY, "FinalArtist"),
        )
        .build()
        .expect("build");
    run(context);

    let meta = metadata_of(&out);
    assert_eq!(
        value_of(&meta, INHERITED_KEY).as_deref(),
        Some("FinalArtist"),
        "a set after an empty value did not win: {meta:?}"
    );
}

// ---- chapter metadata ----
//
// Chapters reach the output through a different path than global metadata:
// `copy_chapters_from_input` copies each chapter's whole metadata dict, and
// `of_add_metadata` then deletes keys on the OUTPUT chapters. Matroska is the
// container of choice — mp4 stores chapters on a separate track, where a
// deleted tag comes back as an empty string rather than disappearing.
//
// The fixture is built through the ffmpeg CLI because the crate deliberately
// rejects a zero-stream input (`FindStreamError::NoStreamFound`), which is
// exactly what an `ffmetadata` chapter source is. An environment without the
// CLI skips, like `tests/data_stream_passthrough.rs`.

/// A matroska fixture whose single chapter carries a title. `None` only when
/// the ffmpeg CLI is unavailable; a fixture built WITHOUT a chapter is a
/// broken fixture and asserts, not a skip — otherwise the deletion scenarios
/// below would silently degrade into no-ops.
fn chapter_fixture(name: &str) -> Option<String> {
    use std::process::Command;

    let chapters = tmp_path(&format!("{name}.chapters.ffmetadata"));
    std::fs::write(
        &chapters,
        ";FFMETADATA1\n\
         [CHAPTER]\n\
         TIMEBASE=1/1000\n\
         START=0\n\
         END=1000\n\
         title=Chapter One\n",
    )
    .expect("write chapter fixture");

    let out = tmp_path(&format!("{name}.mkv"));
    let built = Command::new("ffmpeg")
        .args([
            "-y",
            "-loglevel",
            "error",
            "-f",
            "lavfi",
            "-i",
            "testsrc2=size=64x48:rate=10:duration=1",
            "-i",
            &chapters,
            "-map",
            "0:v",
            "-map_metadata",
            "1",
            "-c:v",
            "mpeg4",
            &out,
        ])
        .status()
        .ok()?; // ffmpeg CLI absent -> skip
    assert!(
        built.success() && std::path::Path::new(&out).exists(),
        "chapter fixture build failed"
    );

    // Without a chapter the scenarios below assert nothing.
    assert_eq!(
        chapter_title_of(&out).as_deref(),
        Some("Chapter One"),
        "fixture was built without its chapter; the deletion scenarios would be no-ops"
    );
    Some(out)
}

/// The chapter title an output carries, or `None` when the chapter has no
/// title. A missing chapter is a probe error, not a silent `None`.
fn chapter_title_of(path: &str) -> Option<String> {
    let chapters = get_chapter_metadata(path, 0).expect("chapter metadata probe failed");
    chapters
        .into_iter()
        .find(|(k, _)| k.eq_ignore_ascii_case("title"))
        .map(|(_, v)| v)
}

/// `-metadata:c:0 key=` spelled as an empty chapter-metadata value must delete
/// the title that the automatic chapter copy placed on the output chapter.
#[test]
fn empty_chapter_value_deletes_inherited_chapter_title() {
    let Some(input) = chapter_fixture("chapter_delete") else {
        eprintln!("skipping: ffmpeg CLI unavailable");
        return;
    };

    let out = tmp_path("chapter_delete_out.mkv");
    let context = FfmpegContext::builder()
        .input(input.as_str())
        .output(
            Output::from(out.as_str())
                .set_video_codec("copy")
                .add_chapter_metadata(0, "title", ""),
        )
        .build()
        .expect("build");
    run(context);

    assert_eq!(
        chapter_title_of(&out),
        None,
        "empty chapter-metadata value did not delete the inherited title"
    );
}

/// Control: a non-empty chapter value still updates the inherited title.
#[test]
fn replacement_chapter_value_updates_inherited_title() {
    let Some(input) = chapter_fixture("chapter_replace") else {
        eprintln!("skipping: ffmpeg CLI unavailable");
        return;
    };

    let out = tmp_path("chapter_replace_out.mkv");
    let context = FfmpegContext::builder()
        .input(input.as_str())
        .output(
            Output::from(out.as_str())
                .set_video_codec("copy")
                .add_chapter_metadata(0, "title", "Renamed Chapter"),
        )
        .build()
        .expect("build");
    run(context);

    assert_eq!(
        chapter_title_of(&out).as_deref(),
        Some("Renamed Chapter"),
        "replacement chapter title did not win"
    );
}
