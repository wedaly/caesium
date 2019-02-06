use report::summary::StatSummary;
use time::Duration;

pub trait ReportSink {
    fn write_insert_rate(&mut self, name: &str, inserts_per_sec: f64);
    fn write_query_duration(&mut self, query_id: usize, summary: StatSummary<Duration>);
}

pub struct LogSink {}

impl LogSink {
    pub fn new() -> LogSink {
        LogSink {}
    }
}

impl ReportSink for LogSink {
    fn write_insert_rate(&mut self, name: &str, inserts_per_sec: f64) {
        info!("{} insert rate {} per second", name, inserts_per_sec);
    }

    fn write_query_duration(&mut self, query_id: usize, summary: StatSummary<Duration>) {
        info!(
            "Query {} time-to-first-byte summary: sample_count={}, median={:?}, 95th={:?}, min={:?}, max={:?}",
            query_id, summary.sample_count(), summary.median(), summary.ninety_fifth_percentile(), summary.min(), summary.max()
        );
    }
}

#[cfg(test)]
pub struct MemorySink {
    insert_measurements: Vec<f64>,
    query_measurements: Vec<(usize, StatSummary<Duration>)>,
}

#[cfg(test)]
impl MemorySink {
    pub fn new() -> MemorySink {
        MemorySink {
            insert_measurements: Vec::new(),
            query_measurements: Vec::new(),
        }
    }

    pub fn get_insert_measurements(&self) -> &[f64] {
        &self.insert_measurements
    }

    pub fn get_query_measurements(&self) -> &[(usize, StatSummary<Duration>)] {
        &self.query_measurements
    }
}

#[cfg(test)]
impl ReportSink for MemorySink {
    fn write_insert_rate(&mut self, _name: &str, inserts_per_sec: f64) {
        self.insert_measurements.push(inserts_per_sec)
    }

    fn write_query_duration(&mut self, query_id: usize, summary: StatSummary<Duration>) {
        self.query_measurements.push((query_id, summary))
    }
}
