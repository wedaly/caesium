use network::circuit::CircuitState;
use network::daemon::command::InsertCmd;
use network::daemon::state::MetricState;
use slab::Slab;
use std::cmp::{max, min, Ordering};
use std::collections::{BinaryHeap, HashMap};
use std::sync::mpsc::{Receiver, RecvTimeoutError, Sender};
use std::sync::{Arc, RwLock};
use std::time::Duration;
use time::clock::{Clock, SystemClock};
use time::timestamp::TimeStamp;

const RECV_TIMEOUT: Duration = Duration::from_millis(1000);
const EXPIRATION_SECONDS: u64 = 30;

pub fn processor_thread(
    input: Receiver<InsertCmd>,
    output: Sender<MetricState>,
    circuit_lock: Arc<RwLock<CircuitState>>,
) {
    let clock = SystemClock::new();
    let mut p = Processor::new();
    loop {
        match input.recv_timeout(RECV_TIMEOUT) {
            Ok(cmd) => p.process_cmd(cmd),
            Err(RecvTimeoutError::Timeout) => trace!("Channel timeout"),
            Err(RecvTimeoutError::Disconnected) => {
                info!("Channel closed, stopping processing thread");
                break;
            }
        }
        p.process_expirations(&clock, &circuit_lock, &output);
    }
}

struct Processor {
    metric_states: Slab<MetricState>,
    metric_name_idx: HashMap<String, usize>, // metric name to slab ID
    expiration_queue: BinaryHeap<Expiration>,
    next_expiration_ts: Option<TimeStamp>,
}

impl Processor {
    pub fn new() -> Processor {
        Processor {
            metric_name_idx: HashMap::new(),
            expiration_queue: BinaryHeap::new(),
            metric_states: Slab::new(),
            next_expiration_ts: None,
        }
    }

    pub fn process_cmd(&mut self, cmd: InsertCmd) {
        debug!("Processing {:?}", cmd);
        match self.metric_name_idx.get(cmd.metric()) {
            None => self.insert(cmd.metric(), cmd.ts(), cmd.value()),
            Some(&metric_id) => self.update(metric_id, cmd.ts(), cmd.value()),
        }
    }

    pub fn process_expirations(
        &mut self,
        clock: &Clock,
        circuit_lock: &Arc<RwLock<CircuitState>>,
        output: &Sender<MetricState>,
    ) {
        let cutoff_ts = clock.now();
        if !self.is_time_to_check_expirations(cutoff_ts) {
            return;
        }

        if !self.is_circuit_closed(circuit_lock) {
            debug!("Circuit is open, skipping expired metrics check");
            return;
        }

        self.find_and_expire_metrics(cutoff_ts, output);
    }

    fn insert(&mut self, metric_name: &str, ts: TimeStamp, value: u64) {
        let metric_state = MetricState::new(metric_name, ts, value);
        let metric_id = self.metric_states.insert(metric_state);
        self.metric_name_idx
            .insert(metric_name.to_string(), metric_id);
        self.schedule_expiration(metric_id, ts);
    }

    fn update(&mut self, metric_id: usize, ts: TimeStamp, value: u64) {
        let metric_state = self.metric_states
            .get_mut(metric_id)
            .expect("Could not retrieve metric state from slab");
        metric_state.sketch.insert(value);
        // Possible, though unlikely, that timestamps will be
        // out-of-order due to clock synchronization
        metric_state.window_start = min(metric_state.window_start, ts);
        metric_state.window_end = max(metric_state.window_end, ts);
    }

    fn schedule_expiration(&mut self, metric_id: usize, ts: TimeStamp) {
        let expires_ts = ts + EXPIRATION_SECONDS;
        let expiration = Expiration {
            expires_ts,
            metric_id,
        };
        self.expiration_queue.push(expiration);
        self.next_expiration_ts = self.calculate_next_expiration_ts();
        debug!(
            "Scheduled expiration for {}, next check scheduled for {:?}",
            expires_ts, self.next_expiration_ts
        );
    }

    fn find_and_expire_metrics(&mut self, cutoff_ts: TimeStamp, output: &Sender<MetricState>) {
        info!("Checking for expired metrics...");
        loop {
            match self.expiration_queue.pop() {
                Some(expiration) => {
                    if expiration.expires_ts <= cutoff_ts {
                        self.expire_metric(expiration.metric_id, output);
                        self.next_expiration_ts = self.calculate_next_expiration_ts();
                    } else {
                        self.expiration_queue.push(expiration);
                        break;
                    }
                }
                None => {
                    self.next_expiration_ts = None;
                    break;
                }
            }
        }
        info!(
            "Finished checking for expired metrics, next check scheduled for {:?}",
            self.next_expiration_ts
        );
    }

    fn expire_metric(&mut self, metric_id: usize, output: &Sender<MetricState>) {
        let metric_state = self.metric_states.remove(metric_id);
        info!("Expiring metric {}", &metric_state.metric_name);
        self.metric_name_idx.remove(&metric_state.metric_name);
        output
            .send(metric_state)
            .expect("Could not send expired metric to output queue");
    }

    fn calculate_next_expiration_ts(&self) -> Option<TimeStamp> {
        self.expiration_queue.peek().map(|x| x.expires_ts)
    }

    fn is_time_to_check_expirations(&self, cutoff_ts: TimeStamp) -> bool {
        match self.next_expiration_ts {
            Some(ts) if ts <= cutoff_ts => true,
            _ => false,
        }
    }

    fn is_circuit_closed(&self, circuit_lock: &Arc<RwLock<CircuitState>>) -> bool {
        let circuit_state = circuit_lock
            .read()
            .expect("Could not acquire read lock on circuit state");
        match *circuit_state {
            CircuitState::Closed => true,
            CircuitState::Open => false,
        }
    }
}

#[derive(Eq)]
struct Expiration {
    expires_ts: TimeStamp,
    metric_id: usize,
}

impl Ord for Expiration {
    fn cmp(&self, other: &Expiration) -> Ordering {
        // Order desc by timestamp, so max-heap will prioritize earlier timestamps
        self.expires_ts.cmp(&other.expires_ts).reverse()
    }
}

impl PartialOrd for Expiration {
    fn partial_cmp(&self, other: &Expiration) -> Option<Ordering> {
        Some(self.cmp(other))
    }
}

impl PartialEq for Expiration {
    fn eq(&self, other: &Expiration) -> bool {
        self.expires_ts == other.expires_ts
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::mpsc::channel;
    use time::clock::MockClock;

    #[test]
    fn it_inserts_new_metric() {
        let mut clock = MockClock::new(0);
        let mut p = Processor::new();
        insert("foo:1234|ms", &mut p, &clock);
        assert_expirations(&mut p, &clock, CircuitState::Closed, &vec![]);
        clock.tick(EXPIRATION_SECONDS - 1);
        assert_expirations(&mut p, &clock, CircuitState::Closed, &vec![]);
        clock.tick(1);
        assert_expirations(
            &mut p,
            &clock,
            CircuitState::Closed,
            &vec![("foo".to_string(), 0, 0, 1)],
        );
        clock.tick(1);
        assert_expirations(&mut p, &clock, CircuitState::Closed, &vec![]);
    }

    #[test]
    fn it_updates_existing_metric() {
        let mut clock = MockClock::new(0);
        let mut p = Processor::new();
        insert("foo:1234|ms", &mut p, &clock);
        clock.tick(15);
        insert("foo:4567|ms", &mut p, &clock);
        assert_expirations(&mut p, &clock, CircuitState::Closed, &vec![]);
        clock.tick(EXPIRATION_SECONDS - 16);
        assert_expirations(&mut p, &clock, CircuitState::Closed, &vec![]);
        clock.tick(1);
        assert_expirations(
            &mut p,
            &clock,
            CircuitState::Closed,
            &vec![("foo".to_string(), 0, 15, 2)],
        );
        clock.tick(1);
        assert_expirations(&mut p, &clock, CircuitState::Closed, &vec![]);
    }

    #[test]
    fn it_expires_multiple_metrics() {
        let mut clock = MockClock::new(0);
        let mut p = Processor::new();
        insert("foo:1234|ms", &mut p, &clock);
        clock.tick(15);
        insert("bar:1234|ms", &mut p, &clock);
        assert_expirations(&mut p, &clock, CircuitState::Closed, &vec![]);
        clock.tick(EXPIRATION_SECONDS - 16);
        assert_expirations(&mut p, &clock, CircuitState::Closed, &vec![]);
        clock.tick(1);
        assert_expirations(
            &mut p,
            &clock,
            CircuitState::Closed,
            &vec![("foo".to_string(), 0, 0, 1)],
        );
        clock.tick(14);
        assert_expirations(&mut p, &clock, CircuitState::Closed, &vec![]);
        clock.tick(1);
        assert_expirations(
            &mut p,
            &clock,
            CircuitState::Closed,
            &vec![("bar".to_string(), 15, 15, 1)],
        );
        clock.tick(1);
        assert_expirations(&mut p, &clock, CircuitState::Closed, &vec![]);
    }

    #[test]
    fn it_stops_expiring_metrics_while_circuit_is_open() {
        let mut clock = MockClock::new(0);
        let mut p = Processor::new();
        insert("foo:1234|ms", &mut p, &clock);
        clock.tick(EXPIRATION_SECONDS);
        assert_expirations(&mut p, &clock, CircuitState::Open, &vec![]);
        clock.tick(1);
        insert("foo:4567|ms", &mut p, &clock);
        assert_expirations(
            &mut p,
            &clock,
            CircuitState::Closed,
            &vec![("foo".to_string(), 0, EXPIRATION_SECONDS + 1, 2)],
        );
    }

    fn insert(s: &str, p: &mut Processor, clock: &Clock) {
        let cmd = InsertCmd::parse_from_str(s, clock).unwrap();
        p.process_cmd(cmd);
    }

    fn assert_expirations(
        p: &mut Processor,
        clock: &Clock,
        circuit_state: CircuitState,
        expected: &[(String, TimeStamp, TimeStamp, usize)],
    ) {
        let (tx, rx) = channel();
        let circuit_lock = Arc::new(RwLock::new(circuit_state));
        p.process_expirations(clock, &circuit_lock, &tx);
        drop(tx);

        let actual: Vec<(String, TimeStamp, TimeStamp, usize)> = rx.iter()
            .map(|s| {
                (
                    s.metric_name.clone(),
                    s.window_start,
                    s.window_end,
                    s.sketch.count(),
                )
            })
            .collect();
        assert_eq!(actual, expected);
    }
}
