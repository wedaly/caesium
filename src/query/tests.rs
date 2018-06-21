use quantile::writable::WritableSketch;
use query::error::QueryError;
use query::execute::execute_query;
use query::result::QueryResult;
use std::collections::HashMap;
use storage::datasource::{DataCursor, DataRow, DataSource};
use storage::error::StorageError;
use time::{TimeStamp, TimeWindow};

struct MockDataSource {
    data: HashMap<String, Vec<DataRow>>,
}

impl MockDataSource {
    fn new() -> MockDataSource {
        MockDataSource {
            data: HashMap::new(),
        }
    }

    fn add_row(&mut self, metric: &str, row: DataRow) {
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
        _start: Option<TimeStamp>,
        _end: Option<TimeStamp>,
    ) -> Result<Box<DataCursor + 'a>, StorageError> {
        match self.data.get(metric) {
            Some(rows) => Ok(Box::new(MockDataCursor::new(&rows))),
            None => Err(StorageError::NotFound),
        }
    }
}

struct MockDataCursor<'a> {
    idx: usize,
    data: &'a [DataRow],
}

impl<'a> MockDataCursor<'a> {
    fn new(data: &[DataRow]) -> MockDataCursor {
        MockDataCursor { idx: 0, data: data }
    }
}

impl<'a> DataCursor for MockDataCursor<'a> {
    fn get_next(&mut self) -> Result<Option<DataRow>, StorageError> {
        let row_opt = self.data.get(self.idx).cloned();
        self.idx += 1;
        Ok(row_opt)
    }
}

fn build_data_row(window: TimeWindow) -> DataRow {
    let mut s = WritableSketch::new();
    for i in 0..100 {
        s.insert(i as u64);
    }
    DataRow {
        window: window,
        sketch: s.to_serializable().to_mergable(),
    }
}

#[test]
fn it_queries_quantile_by_metric() {
    let mut source = MockDataSource::new();
    source.add_row("foo", build_data_row(TimeWindow::new(1, 2)));
    source.add_row("foo", build_data_row(TimeWindow::new(2, 3)));
    source.add_row("bar", build_data_row(TimeWindow::new(3, 4)));
    let query = "quantile(0.5, fetch(foo))";
    let results = execute_query(&query, &mut source).expect("Could not execute query");
    assert_eq!(results.len(), 2);

    let r1 = results.get(0).unwrap();
    assert_eq!(
        *r1,
        QueryResult {
            window: TimeWindow::new(1, 2),
            value: 50
        }
    );

    let r2 = results.get(1).unwrap();
    assert_eq!(
        *r2,
        QueryResult {
            window: TimeWindow::new(2, 3),
            value: 50
        }
    );
}

#[test]
fn it_queries_quantile_metric_not_found() {
    let mut source = MockDataSource::new();
    source.add_row("foo", build_data_row(TimeWindow::new(1, 2)));
    let query = "quantile(0.5, fetch(bar))";
    match execute_query(&query, &mut source) {
        Err(QueryError::StorageError(StorageError::NotFound)) => {}
        _ => panic!("Expected not found error!"),
    }
}

#[test]
fn it_coalesces_adjacent_time_windows() {
    let mut source = MockDataSource::new();
    source.add_row("foo", build_data_row(TimeWindow::new(0, 30)));
    source.add_row("foo", build_data_row(TimeWindow::new(30, 60)));
    let query = "quantile(0.5, coalesce(fetch(foo)))";
    let results = execute_query(&query, &mut source).expect("Could not execute query");
    assert_eq!(results.len(), 1);
    let r = results.first().unwrap();
    assert_eq!(
        *r,
        QueryResult {
            window: TimeWindow::new(0, 60),
            value: 50
        }
    );
}

#[test]
fn it_coalesces_overlapping_time_windows() {
    let mut source = MockDataSource::new();
    source.add_row("foo", build_data_row(TimeWindow::new(30, 60)));
    source.add_row("foo", build_data_row(TimeWindow::new(15, 35)));
    let query = "quantile(0.5, coalesce(fetch(foo)))";
    let results = execute_query(&query, &mut source).expect("Could not execute query");
    assert_eq!(results.len(), 1);
    let r = results.first().unwrap();
    assert_eq!(
        *r,
        QueryResult {
            window: TimeWindow::new(15, 60),
            value: 50
        }
    );
}

#[test]
fn it_coalesces_nonadjacent_time_windows() {
    let mut source = MockDataSource::new();
    source.add_row("foo", build_data_row(TimeWindow::new(10, 20)));
    source.add_row("foo", build_data_row(TimeWindow::new(40, 90)));
    let query = "quantile(0.5, coalesce(fetch(foo)))";
    let results = execute_query(&query, &mut source).expect("Could not execute query");
    assert_eq!(results.len(), 1);
    let r = results.first().unwrap();
    assert_eq!(
        *r,
        QueryResult {
            window: TimeWindow::new(10, 90),
            value: 50
        }
    );
}

#[test]
fn it_coalesces_idempotent() {
    let mut source = MockDataSource::new();
    source.add_row("foo", build_data_row(TimeWindow::new(10, 20)));
    source.add_row("foo", build_data_row(TimeWindow::new(40, 90)));
    let query = "quantile(0.5, coalesce(coalesce(fetch(foo))))";
    let results = execute_query(&query, &mut source).expect("Could not execute query");
    let r = results.first().unwrap();
    assert_eq!(
        *r,
        QueryResult {
            window: TimeWindow::new(10, 90),
            value: 50
        }
    );
}
