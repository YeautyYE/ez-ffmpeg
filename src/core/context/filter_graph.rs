use crate::core::context::input_filter::InputFilter;
use crate::core::context::output_filter::OutputFilter;
use crate::core::context::FrameBox;
use crate::core::scheduler::input_controller::SchNode;
use crossbeam_channel::{Receiver, Sender};
use std::sync::atomic::{AtomicBool, AtomicUsize};
use std::sync::Arc;

pub(crate) struct FilterGraph {
    pub(crate) graph_desc: String,

    pub(crate) inputs: Vec<InputFilter>,
    pub(crate) outputs: Vec<OutputFilter>,

    pub(crate) src: Option<(Sender<FrameBox>, Receiver<FrameBox>, Arc<[AtomicBool]>)>,

    /// Graph-level sws options for auto-inserted `scale` filters
    /// (`AVFilterGraph.scale_sws_opts`). `Some` only for an explicit
    /// `FilterComplex` that called `set_sws_opts`; `None` for implicit
    /// per-output graphs (their value, if any, comes from the bound outputs).
    pub(crate) sws_opts: Option<String>,

    /// Graph-level swr options for auto-inserted `aresample` filters
    /// (`AVFilterGraph.aresample_swr_opts`). See [`sws_opts`](Self::sws_opts).
    pub(crate) swr_opts: Option<String>,

    pub(crate) node: Arc<SchNode>,
}

impl FilterGraph {
    // The graph's filter hw device is not stored here: init_filter_graph
    // registers it in the global hwaccel registry at context-build time
    // (before the probe parse), and the filter task resolves it back through
    // hw_device_for_filter().
    pub(crate) fn new(
        graph_desc: String,
        inputs: Vec<InputFilter>,
        outputs: Vec<OutputFilter>,
        sws_opts: Option<String>,
        swr_opts: Option<String>,
    ) -> Self {
        let (sender, receiver) = crossbeam_channel::bounded(8);
        let finished_flag_list: Vec<AtomicBool> =
            inputs.iter().map(|_| AtomicBool::new(false)).collect();
        // One scheduler-input slot per filter pad, pre-sized so a cross-graph-
        // bound pad is left as an explicit hole (None) rather than shifting the
        // demuxer-bound entries out of pad-index alignment (see ifilter_bind_ist).
        let pad_count = inputs.len();

        Self {
            graph_desc,
            inputs,
            outputs,
            src: Some((sender, receiver, Arc::from(finished_flag_list))),
            sws_opts,
            swr_opts,
            node: Arc::new(SchNode::Filter {
                inputs: (0..pad_count).map(|_| None).collect(),
                best_input: Arc::new(AtomicUsize::from(0)),
            }),
        }
    }

    pub(crate) fn take_src(&mut self) -> (Receiver<FrameBox>, Arc<[AtomicBool]>) {
        let (_sender, receiver, finished_flag_list) = self.src.take().unwrap();
        (receiver, finished_flag_list)
    }

    pub(crate) fn get_src_sender(&mut self) -> (Sender<FrameBox>, Arc<[AtomicBool]>) {
        let (sender, _, finished_flag_list) = self.src.as_ref().unwrap();
        (sender.clone(), finished_flag_list.clone())
    }
}
