use std::collections::{BTreeSet, HashMap};
use storage::datasource::{DataRow, DataSource};
use storage::error::StorageError;
use storage::wildcard::wildcard_match;
use time::timestamp::TimeStamp;

pub struct MockDataSource {
    data: HashMap<String, Vec<DataRow>>,
    metrics: BTreeSet<String>,
    empty: Vec<DataRow>,
}

impl MockDataSource {
    pub fn new() -> MockDataSource {
        MockDataSource {
            data: HashMap::new(),
            metrics: BTreeSet::new(),
            empty: Vec::new(),
        }
    }

    pub fn add_row(&mut self, metric: &str, row: DataRow) {
        self.metrics.insert(metric.to_string());
        let rows = self
            .data
            .entry(metric.to_string())
            .or_insert_with(|| Vec::new());
        rows.push(row);
    }
}

impl DataSource for MockDataSource {
    fn fetch<'a>(
        &'a self,
        metric: String,
        start: Option<TimeStamp>,
        end: Option<TimeStamp>,
    ) -> Result<Box<Iterator<Item = DataRow> + 'a>, StorageError> {
        let start_ts = start.unwrap_or(0);
        let end_ts = end.unwrap_or(TimeStamp::max_value());
        let rows = self.data.get(&metric).unwrap_or(&self.empty);
        let iter = rows.iter().filter_map(move |r| {
            let w = r.window;
            if w.start() >= start_ts && w.end() <= end_ts {
                Some(r.clone())
            } else {
                None
            }
        });
        Ok(Box::new(iter))
    }

    fn search<'a>(
        &'a self,
        pattern: String,
    ) -> Result<Box<Iterator<Item = String> + 'a>, StorageError> {
        let iter = self.metrics.iter().filter_map(move |m| {
            if wildcard_match(m, &pattern) {
                Some(m.to_string())
            } else {
                None
            }
        });
        Ok(Box::new(iter))
    }
}
