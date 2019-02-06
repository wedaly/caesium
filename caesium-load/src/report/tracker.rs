use report::sink::ReportSink;
use report::summary::StatSummary;
use std::cmp::{max, min};
use std::collections::HashMap;
use time::{Duration, Timespec};

pub struct InsertTracker {
    name: String,
    count: u64,
    start_ts: Option<Timespec>,
    end_ts: Option<Timespec>,
}

impl InsertTracker {
    pub fn new(name: String) -> InsertTracker {
        InsertTracker {
            name,
            count: 0,
            start_ts: None,
            end_ts: None,
        }
    }

    pub fn track_insert(&mut self, ts: Timespec) {
        self.count += 1;
        self.start_ts = match self.start_ts.take() {
            Some(old_ts) => Some(min(ts, old_ts)),
            None => Some(ts),
        };
        self.end_ts = match self.end_ts.take() {
            Some(old_ts) => Some(max(ts, old_ts)),
            None => Some(ts),
        };
    }

    pub fn flush<T>(&mut self, sink: &mut T)
    where
        T: ReportSink,
    {
        match (self.start_ts, self.end_ts) {
            (Some(start_ts), Some(end_ts)) => {
                let insert_rate =
                    InsertTracker::calculate_insert_rate(start_ts, end_ts, self.count);
                sink.write_insert_rate(&self.name, insert_rate);
            }
            _ => {}
        }
        self.reset();
    }

    fn reset(&mut self) {
        self.count = 0;
        self.start_ts = None;
        self.end_ts = None;
    }

    fn calculate_insert_rate(start_ts: Timespec, end_ts: Timespec, count: u64) -> f64 {
        let duration_sec: u64 = max((end_ts - start_ts).num_seconds(), 1) as u64;
        (count as f64) / (duration_sec as f64)
    }
}

pub struct QueryTracker {
    // Key is (worker_id, query_id)
    sent_ts_map: HashMap<(usize, usize), Timespec>,

    // Key is query_id
    duration_map: HashMap<usize, Vec<Duration>>,
}

impl QueryTracker {
    pub fn new() -> QueryTracker {
        QueryTracker {
            sent_ts_map: HashMap::new(),
            duration_map: HashMap::new(),
        }
    }

    pub fn track_sent(&mut self, ts: Timespec, worker_id: usize, query_id: usize) {
        self.sent_ts_map.insert((worker_id, query_id), ts);
    }

    pub fn track_received(&mut self, received_ts: Timespec, worker_id: usize, query_id: usize) {
        let key = (worker_id, query_id);
        if let Some(sent_ts) = self.sent_ts_map.remove(&key) {
            let d = received_ts - sent_ts;
            self.duration_map
                .entry(query_id)
                .and_modify(|v| v.push(d))
                .or_insert(vec![d]);
        }
    }

    pub fn flush<T>(&mut self, sink: &mut T)
    where
        T: ReportSink,
    {
        for (query_id, mut durations) in self.duration_map.drain() {
            let summary = StatSummary::new(durations);
            sink.write_query_duration(query_id, summary);
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use report::sink::MemorySink;

    #[test]
    fn it_tracks_insert_rate_no_data() {
        let mut s = MemorySink::new();
        let mut t = InsertTracker::new("Test".to_string());
        t.flush(&mut s);
        assert_eq!(s.get_insert_measurements().len(), 0);
    }

    #[test]
    fn it_tracks_insert_rate() {
        let mut s = MemorySink::new();
        let mut t = InsertTracker::new("Test".to_string());
        t.track_insert(Timespec::new(0, 0));
        t.track_insert(Timespec::new(2, 0));
        t.track_insert(Timespec::new(3, 0));
        t.track_insert(Timespec::new(4, 0));
        t.track_insert(Timespec::new(5, 0));
        t.flush(&mut s);
        assert_eq!(s.get_insert_measurements(), &[1.0f64]);
    }

    #[test]
    fn it_tracks_query_ttfb_no_data() {
        let mut s = MemorySink::new();
        let mut t = QueryTracker::new();
        t.flush(&mut s);
        assert_eq!(s.get_query_measurements().len(), 0);
    }

    #[test]
    fn it_tracks_query_ttfb() {
        let mut s = MemorySink::new();
        let mut t = QueryTracker::new();
        t.track_sent(Timespec::new(0, 0), 0, 0);
        t.track_received(Timespec::new(11, 22), 0, 0);
        t.flush(&mut s);
        let measurements = s.get_query_measurements();
        assert_eq!(measurements.len(), 1);
        let (query_id, ref summary) = measurements[0];
        assert_eq!(query_id, 0);
        assert_eq!(summary.sample_count(), 1);
        assert_eq!(summary.median(), Some(Duration::nanoseconds(11000000022)));
    }

    #[test]
    fn it_tracks_query_ttfb_sent_but_not_received() {
        let mut s = MemorySink::new();
        let mut t = QueryTracker::new();
        t.track_sent(Timespec::new(0, 0), 0, 0);
        t.flush(&mut s);
        assert_eq!(s.get_query_measurements().len(), 0);
    }

    #[test]
    fn it_tracks_query_ttfb_multiple_receive_events() {
        let mut s = MemorySink::new();
        let mut t = QueryTracker::new();
        t.track_sent(Timespec::new(0, 0), 0, 0);
        t.track_received(Timespec::new(11, 22), 0, 0);
        t.track_received(Timespec::new(23, 24), 0, 0);
        t.flush(&mut s);
        let measurements = s.get_query_measurements();
        assert_eq!(measurements.len(), 1);
        let (query_id, ref summary) = measurements[0];
        assert_eq!(query_id, 0);
        assert_eq!(summary.sample_count(), 1);
        assert_eq!(summary.median(), Some(Duration::nanoseconds(11000000022)));
    }

    #[test]
    fn it_tracks_query_ttfb_multiple_distinct_queries() {
        let mut s = MemorySink::new();
        let mut t = QueryTracker::new();

        t.track_sent(Timespec::new(0, 1), 0, 0);
        t.track_sent(Timespec::new(0, 2), 0, 1);
        t.track_sent(Timespec::new(0, 3), 1, 0);
        t.track_sent(Timespec::new(0, 4), 2, 1);

        t.track_received(Timespec::new(11, 40), 2, 1);
        t.track_received(Timespec::new(11, 30), 1, 0);
        t.track_received(Timespec::new(11, 20), 0, 1);
        t.track_received(Timespec::new(11, 10), 0, 0);

        t.flush(&mut s);
        let measurements = s.get_query_measurements();

        // One measurement for each distinct query
        assert_eq!(measurements.len(), 2);
        assert_summary(
            measurements,
            0,
            2,
            Duration::nanoseconds(11000000009),
            Duration::nanoseconds(11000000027),
        );
        assert_summary(
            measurements,
            1,
            2,
            Duration::nanoseconds(11000000018),
            Duration::nanoseconds(11000000036),
        );
    }

    fn assert_summary(
        measurements: &[(usize, StatSummary<Duration>)],
        query_id: usize,
        expected_sample_count: usize,
        expected_min: Duration,
        expected_max: Duration,
    ) {
        let summary = measurements
            .iter()
            .find(|(qid, _)| *qid == query_id)
            .map(|(_, summary)| summary)
            .unwrap();
        assert_eq!(summary.sample_count(), expected_sample_count);
        assert_eq!(summary.min(), Some(expected_min));
        assert_eq!(summary.max(), Some(expected_max));
    }
}
