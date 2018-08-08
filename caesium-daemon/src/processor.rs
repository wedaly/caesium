use caesium_core::network::message::Message;
use caesium_core::quantile::writable::WritableSketch;
use caesium_core::time::window::TimeWindow;
use circuit::CircuitState;
use slab::Slab;
use std::collections::HashMap;
use std::sync::mpsc::{Receiver, Sender};
use std::sync::{Arc, RwLock};

pub fn processor_thread(
    input: Receiver<ProcessorCommand>,
    output: Sender<Message>,
    circuit_lock: Arc<RwLock<CircuitState>>,
) {
    let mut p = Processor::new(&output, &circuit_lock);
    loop {
        match input.recv() {
            Ok(cmd) => p.process_cmd(cmd),
            Err(_) => {
                info!("Channel closed, stopping processing thread");
                break;
            }
        }
    }
}

#[derive(Debug)]
pub enum ProcessorCommand {
    InsertMetric(String, u64),
    CloseWindow(TimeWindow),
}

struct Processor<'a> {
    metric_states: Slab<MetricState>,
    metric_name_idx: HashMap<String, usize>, // metric name to slab ID
    output: &'a Sender<Message>,
    circuit_lock: &'a Arc<RwLock<CircuitState>>,
}

impl<'a> Processor<'a> {
    pub fn new(
        output: &'a Sender<Message>,
        circuit_lock: &'a Arc<RwLock<CircuitState>>,
    ) -> Processor<'a> {
        Processor {
            metric_name_idx: HashMap::new(),
            metric_states: Slab::new(),
            output,
            circuit_lock,
        }
    }

    pub fn process_cmd(&mut self, cmd: ProcessorCommand) {
        debug!("Processing {:?}", cmd);
        match cmd {
            ProcessorCommand::InsertMetric(metric_name, value) => {
                match self.metric_name_idx.get(&metric_name) {
                    None => self.insert(&metric_name, value),
                    Some(&metric_id) => self.update(metric_id, value),
                }
            }
            ProcessorCommand::CloseWindow(window) => self.process_close_cmd(window),
        }
    }

    fn insert(&mut self, metric_name: &str, value: u64) {
        let metric_state = MetricState::new(metric_name, value);
        let metric_id = self.metric_states.insert(metric_state);
        self.metric_name_idx
            .insert(metric_name.to_string(), metric_id);
    }

    fn update(&mut self, metric_id: usize, value: u64) {
        let metric_state = self
            .metric_states
            .get_mut(metric_id)
            .expect("Could not retrieve metric state from slab");
        metric_state.sketch.insert(value);
    }

    fn process_close_cmd(&mut self, window: TimeWindow) {
        if self.is_circuit_closed() {
            for &metric_id in self.metric_name_idx.values() {
                let state = self.metric_states.remove(metric_id);
                let msg = Message::InsertReq {
                    metric: state.metric_name,
                    sketch: state.sketch,
                    window,
                };
                self.output
                    .send(msg)
                    .expect("Could not output message from processor");
            }
            self.metric_name_idx.clear();
        }
    }

    fn is_circuit_closed(&self) -> bool {
        let circuit_state = self
            .circuit_lock
            .read()
            .expect("Could not acquire read lock on circuit state");
        match *circuit_state {
            CircuitState::Closed => true,
            CircuitState::Open => false,
        }
    }
}

struct MetricState {
    metric_name: String,
    sketch: WritableSketch,
}

impl MetricState {
    fn new(metric_name: &str, value: u64) -> MetricState {
        let mut sketch = WritableSketch::new();
        sketch.insert(value);
        MetricState {
            metric_name: metric_name.to_string(),
            sketch,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::mpsc::channel;

    #[test]
    fn it_inserts_new_metrics() {
        let commands = vec![
            ProcessorCommand::InsertMetric("foo".to_string(), 1),
            ProcessorCommand::InsertMetric("bar".to_string(), 2),
            ProcessorCommand::CloseWindow(TimeWindow::new(30, 60)),
        ];
        let expected = vec![
            ("foo".to_string(), TimeWindow::new(30, 60), 1),
            ("bar".to_string(), TimeWindow::new(30, 60), 1),
        ];
        assert_processor(commands, expected, CircuitState::Closed);
    }

    #[test]
    fn it_updates_existing_metrics() {
        let commands = vec![
            ProcessorCommand::InsertMetric("foo".to_string(), 1),
            ProcessorCommand::InsertMetric("foo".to_string(), 2),
            ProcessorCommand::CloseWindow(TimeWindow::new(30, 60)),
        ];
        let expected = vec![("foo".to_string(), TimeWindow::new(30, 60), 2)];
        assert_processor(commands, expected, CircuitState::Closed);
    }

    #[test]
    fn it_flushes_metrics_on_window_close() {
        let commands = vec![
            ProcessorCommand::InsertMetric("foo".to_string(), 1),
            ProcessorCommand::InsertMetric("bar".to_string(), 2),
            ProcessorCommand::CloseWindow(TimeWindow::new(30, 60)),
            ProcessorCommand::InsertMetric("baz".to_string(), 3),
            ProcessorCommand::InsertMetric("bat".to_string(), 4),
            ProcessorCommand::CloseWindow(TimeWindow::new(60, 90)),
            ProcessorCommand::CloseWindow(TimeWindow::new(90, 120)),
        ];
        let expected = vec![
            ("foo".to_string(), TimeWindow::new(30, 60), 1),
            ("bar".to_string(), TimeWindow::new(30, 60), 1),
            ("baz".to_string(), TimeWindow::new(60, 90), 1),
            ("bat".to_string(), TimeWindow::new(60, 90), 1),
        ];
        assert_processor(commands, expected, CircuitState::Closed);
    }

    #[test]
    fn it_does_not_flush_if_circuit_open() {
        let commands = vec![
            ProcessorCommand::InsertMetric("foo".to_string(), 1),
            ProcessorCommand::InsertMetric("bar".to_string(), 2),
            ProcessorCommand::CloseWindow(TimeWindow::new(30, 60)),
            ProcessorCommand::InsertMetric("baz".to_string(), 3),
            ProcessorCommand::InsertMetric("bat".to_string(), 4),
            ProcessorCommand::CloseWindow(TimeWindow::new(60, 90)),
            ProcessorCommand::CloseWindow(TimeWindow::new(90, 120)),
        ];
        let expected = vec![];
        assert_processor(commands, expected, CircuitState::Open);
    }

    fn assert_processor(
        mut commands: Vec<ProcessorCommand>,
        mut expected: Vec<(String, TimeWindow, usize)>,
        circuit_state: CircuitState,
    ) {
        let (tx, rx) = channel();
        let circuit_lock = Arc::new(RwLock::new(circuit_state));
        {
            let mut p = Processor::new(&tx, &circuit_lock);
            for cmd in commands.drain(..) {
                p.process_cmd(cmd);
            }
        }
        drop(tx);
        let mut output: Vec<(String, TimeWindow, usize)> = rx
            .iter()
            .map(|msg| match msg {
                Message::InsertReq {
                    metric,
                    window,
                    sketch,
                } => (metric.clone(), window, sketch.count()),
                _ => panic!("Unexpected message type"),
            })
            .collect();
        expected.sort_unstable();
        output.sort_unstable();
        assert_eq!(output, expected);
    }
}
