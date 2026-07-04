use ez_ffmpeg::analysis::{MetadataEvent, MetadataEventFilter};
use ez_ffmpeg::filter::frame_pipeline_builder::FramePipelineBuilder;
use ez_ffmpeg::{FfmpegContext, Output};
use ffmpeg_sys_next::AVMediaType::AVMEDIA_TYPE_VIDEO;
use std::sync::mpsc::sync_channel;

/// Detect black frames *while transcoding*, receiving results live on a channel
/// — the lower-level streaming counterpart to the one-shot `Analysis` runner.
///
/// A `blackdetect` filter in the graph attaches `lavfi.black_*` metadata to
/// frames; `MetadataEventFilter` on the output pipeline parses that metadata
/// into typed [`MetadataEvent`]s and forwards each frame unchanged.
fn main() -> Result<(), Box<dyn std::error::Error>> {
    let (tx, rx) = sync_channel::<MetadataEvent>(256);

    // Drain events on a background thread so the media pipeline is never
    // back-pressured (the filter's default policy aborts if the sink stalls).
    let consumer = std::thread::spawn(move || {
        for ev in rx {
            if let MetadataEvent::BlackStart { at } = ev {
                println!("black frame starts at {} us", at.time_us);
            }
        }
    });

    let detector = MetadataEventFilter::new(AVMEDIA_TYPE_VIDEO, tx);
    let pipeline = FramePipelineBuilder::new(AVMEDIA_TYPE_VIDEO)
        .filter("detect", Box::new(detector))
        .build();

    FfmpegContext::builder()
        .input("test.mp4")
        .filter_desc("blackdetect=d=0.1")
        .output(Output::from("output.mp4").set_frame_pipelines(vec![pipeline]))
        .build()?
        .start()?
        .wait()?;

    consumer.join().ok();
    println!("transcode + streaming detection complete");
    Ok(())
}
