use std::collections::HashMap;
use storage::datasource::{DataRow, DataSource};
use storage::error::StorageError;
use time::timestamp::TimeStamp;

pub struct MockDataSource {
    data: HashMap<String, Vec<DataRow>>,
    empty: Vec<DataRow>,
}

impl MockDataSource {
    pub fn new() -> MockDataSource {
        MockDataSource {
            data: HashMap::new(),
            empty: Vec::new(),
        }
    }

    pub fn add_row(&mut self, metric: &str, row: DataRow) {
        let rows = self.data
            .entry(metric.to_string())
            .or_insert_with(|| Vec::new());
        rows.push(row);
    }
}

impl DataSource for MockDataSource {
    fn fetch_range<'a>(
        &'a self,
        metric: &str,
        start: Option<TimeStamp>,
        end: Option<TimeStamp>,
    ) -> Result<Box<Iterator<Item = DataRow> + 'a>, StorageError> {
        let start_ts = start.unwrap_or(0);
        let end_ts = end.unwrap_or(TimeStamp::max_value());
        let rows = self.data.get(metric).unwrap_or(&self.empty);
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
}
