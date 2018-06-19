use quantile::writable::WritableSketch;
use query::error::QueryError;
use query::execute::execute_query;
use query::result::QueryResult;
use std::collections::HashMap;
use storage::datasource::{DataCursor, DataRow, DataSource};
use storage::error::StorageError;
use time::{TimeBucket, TimeRange, TimeStamp, SECONDS_PER_BUCKET};

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

fn build_data_row(bucket: TimeBucket) -> DataRow {
    let mut s = WritableSketch::new();
    for i in 0..100 {
        s.insert(i as u64);
    }
    DataRow {
        range: TimeRange::from_bucket(bucket, 1),
        sketch: s.to_serializable().to_mergable(),
    }
}

#[test]
fn it_queries_quantile_by_metric() {
    let mut source = MockDataSource::new();
    source.add_row("foo", build_data_row(1));
    source.add_row("foo", build_data_row(2));
    source.add_row("bar", build_data_row(3));
    let query = "quantile(0.5, fetch(foo))";
    let results = execute_query(&query, &mut source).expect("Could not execute query");
    assert_eq!(results.len(), 2);

    let r1 = results.get(0).unwrap();
    assert_eq!(
        *r1,
        QueryResult {
            range: TimeRange::from_bucket(1, 1),
            value: 50
        }
    );

    let r2 = results.get(1).unwrap();
    assert_eq!(
        *r2,
        QueryResult {
            range: TimeRange::from_bucket(2, 1),
            value: 50
        }
    );
}

#[test]
fn it_queries_quantile_metric_not_found() {
    let mut source = MockDataSource::new();
    source.add_row("foo", build_data_row(1));
    let query = "quantile(0.5, fetch(bar))";
    match execute_query(&query, &mut source) {
        Err(QueryError::StorageError(StorageError::NotFound)) => {}
        _ => panic!("Expected not found error!"),
    }
}

#[test]
fn it_queries_quantile_bucket_by_hour() {
    let mut source = MockDataSource::new();
    let hours = 2;
    let buckets_per_hour = 3_600 / SECONDS_PER_BUCKET;
    let num_buckets = hours * buckets_per_hour;
    for i in 0..num_buckets {
        source.add_row("foo", build_data_row(i))
    }
    let query = "quantile(0.5, bucket(1, fetch(foo)))";
    let results = execute_query(&query, &mut source).expect("Could not execute query");
    assert_eq!(results.len(), hours as usize);
    for row in results.iter() {
        assert_eq!(row.range.duration(), 3_600);
    }
}

#[test]
fn it_queries_quantile_bucket_by_day() {
    let mut source = MockDataSource::new();
    let days = 2;
    let buckets_per_day = 86_400 / SECONDS_PER_BUCKET;
    let num_buckets = days * buckets_per_day;
    for i in 0..num_buckets {
        source.add_row("foo", build_data_row(i))
    }
    let query = "quantile(0.5, bucket(24, fetch(foo)))";
    let results = execute_query(&query, &mut source).expect("Could not execute query");
    assert_eq!(results.len(), days as usize);
    for row in results.iter() {
        assert_eq!(row.range.duration(), 86_400);
    }
}

#[test]
fn it_errors_if_bucket_applied_twice() {
    let mut source = MockDataSource::new();
    let hours = 2;
    let buckets_per_hour = 3_600 / SECONDS_PER_BUCKET;
    let num_buckets = hours * buckets_per_hour;
    for i in 0..num_buckets {
        source.add_row("foo", build_data_row(i))
    }
    let query = "quantile(0.5, bucket(1, bucket(5, fetch(foo))))";
    match execute_query(&query, &mut source) {
        Err(QueryError::InvalidWindowSize(s)) => {
            assert_eq!(s, 18_000);
        }
        _ => panic!("Expected invalid window size error!"),
    }
}
