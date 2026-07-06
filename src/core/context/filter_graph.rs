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

    pub(crate) node: Arc<SchNode>
}

impl FilterGraph {
    // The graph's filter hw device is not stored here: init_filter_graph
    // registers it in the global hwaccel registry at context-build time
    // (before the probe parse), and the filter task resolves it back through
    // hw_device_for_filter().
    pub(crate) fn new(graph_desc: String,
                      inputs: Vec<InputFilter>,
                      outputs: Vec<OutputFilter>) -> Self {
        let (sender, receiver) = crossbeam_channel::bounded(8);
        let finished_flag_list: Vec<AtomicBool> = inputs.iter()
            .map(|_| AtomicBool::new(false))
            .collect();

        Self {
            graph_desc,
            inputs,
            outputs,
            src: Some((sender, receiver, Arc::from(finished_flag_list))),
            node: Arc::new(SchNode::Filter { inputs: Vec::new(), best_input: Arc::new(AtomicUsize::from(0)) })
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