use caesium_core::protocol::messages::InsertMessage;
use caesium_core::quantile::writable::WritableSketch;
use caesium_core::time::timestamp::TimeStamp;
use caesium_core::time::window::TimeWindow;
use circuit::CircuitState;
use slab::Slab;
use std::collections::HashMap;
use std::sync::mpsc::{Receiver, Sender};
use std::sync::{Arc, RwLock};

pub fn processor_thread(
    input: Receiver<ProcessorCommand>,
    output: Sender<InsertMessage>,
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
    InsertMetric(String, u32),
    CloseWindow(TimeWindow),
}

struct Processor<'a> {
    metric_states: Slab<MetricState>,
    metric_name_idx: HashMap<String, usize>, // metric name to slab ID
    output: &'a Sender<InsertMessage>,
    circuit_lock: &'a Arc<RwLock<CircuitState>>,
    window_start: Option<TimeStamp>,
}

impl<'a> Processor<'a> {
    pub fn new(
        output: &'a Sender<InsertMessage>,
        circuit_lock: &'a Arc<RwLock<CircuitState>>,
    ) -> Processor<'a> {
        Processor {
            metric_name_idx: HashMap::new(),
            metric_states: Slab::new(),
            output,
            circuit_lock,
            window_start: None,
        }
    }

    pub fn process_cmd(&mut self, cmd: ProcessorCommand) {
        trace!("Processing {:?}", cmd);
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

    fn insert(&mut self, metric_name: &str, value: u32) {
        let metric_state = MetricState::new(metric_name, value);
        let metric_id = self.metric_states.insert(metric_state);
        self.metric_name_idx
            .insert(metric_name.to_string(), metric_id);
    }

    fn update(&mut self, metric_id: usize, value: u32) {
        let metric_state = self
            .metric_states
            .get_mut(metric_id)
            .expect("Could not retrieve metric state from slab");
        metric_state.sketch.insert(value);
    }

    fn process_close_cmd(&mut self, window: TimeWindow) {
        if self.is_circuit_closed() {
            let window_start = self.window_start.unwrap_or(window.start());
            let window = TimeWindow::new(window_start, window.end());
            for &metric_id in self.metric_name_idx.values() {
                let state = self.metric_states.remove(metric_id);
                let msg = InsertMessage {
                    metric: state.metric_name,
                    window,
                    sketch: state.sketch,
                };
                self.output
                    .send(msg)
                    .expect("Could not output message from processor");
            }
            self.window_start = Some(window.end());
            self.metric_name_idx.clear();
        } else {
            self.window_start = self.window_start.or(Some(window.start()));
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
    fn new(metric_name: &str, value: u32) -> MetricState {
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
            (
                ProcessorCommand::InsertMetric("foo".to_string(), 1),
                CircuitState::Closed,
            ),
            (
                ProcessorCommand::InsertMetric("bar".to_string(), 2),
                CircuitState::Closed,
            ),
            (
                ProcessorCommand::CloseWindow(TimeWindow::new(30, 60)),
                CircuitState::Closed,
            ),
        ];
        let expected = vec![
            ("foo".to_string(), TimeWindow::new(30, 60), 1),
            ("bar".to_string(), TimeWindow::new(30, 60), 1),
        ];
        assert_processor(commands, expected);
    }

    #[test]
    fn it_updates_existing_metrics() {
        let commands = vec![
            (
                ProcessorCommand::InsertMetric("foo".to_string(), 1),
                CircuitState::Closed,
            ),
            (
                ProcessorCommand::InsertMetric("foo".to_string(), 2),
                CircuitState::Closed,
            ),
            (
                ProcessorCommand::CloseWindow(TimeWindow::new(30, 60)),
                CircuitState::Closed,
            ),
        ];
        let expected = vec![("foo".to_string(), TimeWindow::new(30, 60), 2)];
        assert_processor(commands, expected);
    }

    #[test]
    fn it_flushes_metrics_on_window_close() {
        let commands = vec![
            (
                ProcessorCommand::InsertMetric("foo".to_string(), 1),
                CircuitState::Closed,
            ),
            (
                ProcessorCommand::InsertMetric("bar".to_string(), 2),
                CircuitState::Closed,
            ),
            (
                ProcessorCommand::CloseWindow(TimeWindow::new(30, 60)),
                CircuitState::Closed,
            ),
            (
                ProcessorCommand::InsertMetric("baz".to_string(), 3),
                CircuitState::Closed,
            ),
            (
                ProcessorCommand::InsertMetric("bat".to_string(), 4),
                CircuitState::Closed,
            ),
            (
                ProcessorCommand::CloseWindow(TimeWindow::new(60, 90)),
                CircuitState::Closed,
            ),
            (
                ProcessorCommand::CloseWindow(TimeWindow::new(90, 120)),
                CircuitState::Closed,
            ),
        ];
        let expected = vec![
            ("foo".to_string(), TimeWindow::new(30, 60), 1),
            ("bar".to_string(), TimeWindow::new(30, 60), 1),
            ("baz".to_string(), TimeWindow::new(60, 90), 1),
            ("bat".to_string(), TimeWindow::new(60, 90), 1),
        ];
        assert_processor(commands, expected);
    }

    #[test]
    fn it_does_not_flush_if_circuit_open() {
        let commands = vec![
            (
                ProcessorCommand::InsertMetric("foo".to_string(), 1),
                CircuitState::Open,
            ),
            (
                ProcessorCommand::InsertMetric("bar".to_string(), 2),
                CircuitState::Open,
            ),
            (
                ProcessorCommand::CloseWindow(TimeWindow::new(30, 60)),
                CircuitState::Open,
            ),
            (
                ProcessorCommand::InsertMetric("baz".to_string(), 3),
                CircuitState::Open,
            ),
            (
                ProcessorCommand::InsertMetric("bat".to_string(), 4),
                CircuitState::Open,
            ),
            (
                ProcessorCommand::CloseWindow(TimeWindow::new(60, 90)),
                CircuitState::Open,
            ),
            (
                ProcessorCommand::CloseWindow(TimeWindow::new(90, 120)),
                CircuitState::Open,
            ),
        ];
        let expected = vec![];
        assert_processor(commands, expected);
    }

    #[test]
    fn it_flushes_when_circuit_closes() {
        let commands = vec![
            (
                ProcessorCommand::InsertMetric("foo".to_string(), 1),
                CircuitState::Open,
            ),
            (
                ProcessorCommand::InsertMetric("bar".to_string(), 2),
                CircuitState::Open,
            ),
            (
                ProcessorCommand::CloseWindow(TimeWindow::new(30, 60)),
                CircuitState::Open,
            ),
            (
                ProcessorCommand::InsertMetric("baz".to_string(), 3),
                CircuitState::Open,
            ),
            (
                ProcessorCommand::InsertMetric("bat".to_string(), 4),
                CircuitState::Open,
            ),
            (
                ProcessorCommand::CloseWindow(TimeWindow::new(60, 90)),
                CircuitState::Open,
            ),
            (
                ProcessorCommand::CloseWindow(TimeWindow::new(90, 120)),
                CircuitState::Closed,
            ),
        ];
        let expected = vec![
            ("foo".to_string(), TimeWindow::new(30, 120), 1),
            ("bar".to_string(), TimeWindow::new(30, 120), 1),
            ("baz".to_string(), TimeWindow::new(30, 120), 1),
            ("bat".to_string(), TimeWindow::new(30, 120), 1),
        ];
        assert_processor(commands, expected);
    }

    fn assert_processor(
        mut commands: Vec<(ProcessorCommand, CircuitState)>,
        mut expected: Vec<(String, TimeWindow, usize)>,
    ) {
        let (tx, rx) = channel();
        let circuit_lock = Arc::new(RwLock::new(CircuitState::Closed));
        {
            let mut p = Processor::new(&tx, &circuit_lock);
            for (cmd, circuit_state) in commands.drain(..) {
                {
                    let mut cs = circuit_lock.write().unwrap();
                    *cs = circuit_state;
                }
                p.process_cmd(cmd);
            }
        }
        drop(tx);
        let mut output: Vec<(String, TimeWindow, usize)> = rx
            .iter()
            .map(|msg| (msg.metric.to_string(), msg.window, msg.sketch.count()))
            .collect();
        expected.sort_unstable();
        output.sort_unstable();
        assert_eq!(output, expected);
    }
}
