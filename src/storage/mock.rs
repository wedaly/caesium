use std::collections::HashMap;
use storage::datasource::{DataCursor, DataRow, DataSource};
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
    ) -> Result<Box<DataCursor + 'a>, StorageError> {
        match self.data.get(metric) {
            Some(rows) => {
                let start_ts = start.unwrap_or(0);
                let end_ts = end.unwrap_or(TimeStamp::max_value());
                let cursor = MockDataCursor::new(rows, start_ts, end_ts);
                Ok(Box::new(cursor))
            }
            None => {
                let cursor = MockDataCursor::new(&self.empty, 0, 0);
                Ok(Box::new(cursor))
            }
        }
    }
}

pub struct MockDataCursor<'a> {
    idx: usize,
    data: &'a [DataRow],
    start_ts: TimeStamp,
    end_ts: TimeStamp,
}

impl<'a> MockDataCursor<'a> {
    fn new(data: &[DataRow], start_ts: TimeStamp, end_ts: TimeStamp) -> MockDataCursor {
        MockDataCursor {
            idx: 0,
            data,
            start_ts,
            end_ts,
        }
    }
}

impl<'a> DataCursor for MockDataCursor<'a> {
    fn get_next(&mut self) -> Result<Option<DataRow>, StorageError> {
        while self.idx < self.data.len() {
            let row_opt = self.data.get(self.idx).cloned();
            self.idx += 1;

            if let Some(row) = row_opt {
                if row.window.start() >= self.start_ts && row.window.end() <= self.end_ts {
                    return Ok(Some(row));
                }
            }
        }
        Ok(None)
    }
}
