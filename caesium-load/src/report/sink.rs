use report::summary::StatSummary;
use time::Duration;

pub trait ReportSink {
    fn write_rate(&mut self, name: &str, num_per_sec: f64);
    fn write_count(&mut self, name: &str, count: usize);
    fn write_query_duration(&mut self, query_id: usize, summary: StatSummary<Duration>);
}

pub struct LogSink {}

impl LogSink {
    pub fn new() -> LogSink {
        LogSink {}
    }
}

impl ReportSink for LogSink {
    fn write_rate(&mut self, name: &str, num_per_sec: f64) {
        info!("{} rate {} per second", name, num_per_sec);
    }

    fn write_count(&mut self, name: &str, count: usize) {
        info!("{} count was {}", name, count);
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
    rate_measurements: Vec<f64>,
    count_measurements: Vec<usize>,
    query_measurements: Vec<(usize, StatSummary<Duration>)>,
}

#[cfg(test)]
impl MemorySink {
    pub fn new() -> MemorySink {
        MemorySink {
            rate_measurements: Vec::new(),
            count_measurements: Vec::new(),
            query_measurements: Vec::new(),
        }
    }

    pub fn get_rate_measurements(&self) -> &[f64] {
        &self.rate_measurements
    }

    pub fn get_count_measurements(&self) -> &[usize] {
        &self.count_measurements
    }

    pub fn get_query_measurements(&self) -> &[(usize, StatSummary<Duration>)] {
        &self.query_measurements
    }
}

#[cfg(test)]
impl ReportSink for MemorySink {
    fn write_rate(&mut self, _name: &str, num_per_sec: f64) {
        self.rate_measurements.push(num_per_sec)
    }

    fn write_count(&mut self, _name: &str, count: usize) {
        self.count_measurements.push(count);
    }

    fn write_query_duration(&mut self, query_id: usize, summary: StatSummary<Duration>) {
        self.query_measurements.push((query_id, summary))
    }
}
