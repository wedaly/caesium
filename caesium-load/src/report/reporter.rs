use report::event::Event;
use report::sink::ReportSink;
use report::tracker::{InsertTracker, QueryTracker};
use std::sync::mpsc::Receiver;
use std::sync::{Arc, Mutex};
use time::Timespec;

pub struct Reporter {
    rx: Receiver<Event>,
    metric_insert_tracker: InsertTracker,
    sketch_insert_tracker: InsertTracker,
    query_tracker: QueryTracker,
    sample_interval_sec: u64,
    last_flush_ts: Option<Timespec>,
}

impl Reporter {
    pub fn new(rx: Receiver<Event>, sample_interval_sec: u64) -> Reporter {
        assert!(sample_interval_sec > 0);
        let metric_insert_tracker = InsertTracker::new("Metric".to_string());
        let sketch_insert_tracker = InsertTracker::new("Sketch".to_string());
        let query_tracker = QueryTracker::new();
        Reporter {
            rx,
            metric_insert_tracker,
            sketch_insert_tracker,
            query_tracker,
            sample_interval_sec,
            last_flush_ts: None,
        }
    }

    pub fn run<T>(mut self, sink_mutex: Arc<Mutex<T>>)
    where
        T: ReportSink,
    {
        loop {
            match self.rx.recv() {
                Ok(event) => self.process_event(event, sink_mutex.clone()),
                Err(_) => {
                    info!("Channel closed, stopping reporter");
                    break;
                }
            }
        }
    }

    fn process_event<T>(&mut self, event: Event, sink_mutex: Arc<Mutex<T>>)
    where
        T: ReportSink,
    {
        let event_ts = event.get_ts();
        if let None = self.last_flush_ts {
            self.set_last_flush_ts(event_ts);
        }

        if self.is_time_to_flush(event_ts) {
            let mut sink = sink_mutex.lock().expect("Could not acquire lock on sink");
            self.metric_insert_tracker.flush(&mut *sink);
            self.sketch_insert_tracker.flush(&mut *sink);
            self.query_tracker.flush(&mut *sink);
            self.set_last_flush_ts(event_ts);
        }

        match event {
            Event::MetricSentEvent { event_ts } => {
                self.metric_insert_tracker.track_insert(event_ts);
            }
            Event::SketchSentEvent { event_ts } => {
                self.sketch_insert_tracker.track_insert(event_ts);
            }
            Event::QuerySentEvent {
                event_ts,
                worker_id,
                query_id,
            } => {
                self.query_tracker.track_sent(event_ts, worker_id, query_id);
            }
            Event::QueryBytesReceivedEvent {
                event_ts,
                worker_id,
                query_id,
            } => {
                self.query_tracker
                    .track_received(event_ts, worker_id, query_id);
            }
        };
    }

    fn is_time_to_flush(&self, event_ts: Timespec) -> bool {
        match self.last_flush_ts {
            Some(last_flush_ts) => {
                let elapsed_sec = (event_ts - last_flush_ts).num_seconds();
                if elapsed_sec > 0 {
                    elapsed_sec as u64 >= self.sample_interval_sec
                } else {
                    false
                }
            }
            None => false,
        }
    }

    fn set_last_flush_ts(&mut self, ts: Timespec) {
        self.last_flush_ts = Some(ts);
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use report::sink::MemorySink;
    use std::sync::mpsc::channel;
    use std::thread;

    #[test]
    fn it_flushes_metric_inserted_report_at_end_of_interval() {
        let (tx, rx) = channel();
        let r = Reporter::new(rx, 1);
        let sink = Arc::new(Mutex::new(MemorySink::new()));
        let sink_ref = sink.clone();
        let thread = thread::spawn(|| r.run(sink_ref));
        tx.send(Event::MetricSentEvent {
            event_ts: Timespec::new(0, 0),
        })
        .unwrap();
        tx.send(Event::MetricSentEvent {
            event_ts: Timespec::new(0, 50),
        })
        .unwrap();
        tx.send(Event::MetricSentEvent {
            event_ts: Timespec::new(1, 0),
        })
        .unwrap();
        tx.send(Event::MetricSentEvent {
            event_ts: Timespec::new(1, 50),
        })
        .unwrap();
        tx.send(Event::MetricSentEvent {
            event_ts: Timespec::new(1, 60),
        })
        .unwrap();
        tx.send(Event::MetricSentEvent {
            event_ts: Timespec::new(1, 70),
        })
        .unwrap();
        tx.send(Event::MetricSentEvent {
            event_ts: Timespec::new(2, 0),
        })
        .unwrap();
        drop(tx);
        thread.join().expect("Could not join thread");

        {
            let s = sink.lock().expect("Could not acquire lock on sink");
            let measurements = s.get_insert_measurements();
            assert_eq!(measurements, &[2.0f64, 4.0f64]);
        }
    }

    #[test]
    fn it_flushes_query_report_at_end_of_interval() {
        let (tx, rx) = channel();
        let r = Reporter::new(rx, 1);
        let sink = Arc::new(Mutex::new(MemorySink::new()));
        let sink_ref = sink.clone();
        let thread = thread::spawn(|| r.run(sink_ref));
        tx.send(Event::QuerySentEvent {
            event_ts: Timespec::new(0, 0),
            worker_id: 0,
            query_id: 0,
        })
        .unwrap();
        tx.send(Event::QueryBytesReceivedEvent {
            event_ts: Timespec::new(0, 50),
            worker_id: 0,
            query_id: 0,
        })
        .unwrap();
        tx.send(Event::QuerySentEvent {
            event_ts: Timespec::new(0, 70),
            worker_id: 0,
            query_id: 0,
        })
        .unwrap();
        tx.send(Event::QueryBytesReceivedEvent {
            event_ts: Timespec::new(1, 10),
            worker_id: 0,
            query_id: 0,
        })
        .unwrap();
        tx.send(Event::QuerySentEvent {
            event_ts: Timespec::new(1, 20),
            worker_id: 0,
            query_id: 0,
        })
        .unwrap();
        tx.send(Event::QueryBytesReceivedEvent {
            event_ts: Timespec::new(1, 40),
            worker_id: 0,
            query_id: 0,
        })
        .unwrap();
        tx.send(Event::MetricSentEvent {
            event_ts: Timespec::new(3, 0),
        })
        .unwrap();
        drop(tx);
        thread.join().expect("Could not join thread");

        {
            let s = sink.lock().expect("Could not acquire lock on sink");
            let measurements = s.get_query_measurements();
            let sample_counts: Vec<usize> = measurements
                .iter()
                .map(|(_, summary)| summary.sample_count())
                .collect();
            assert_eq!(sample_counts, &[1, 2]);
        }
    }
}
