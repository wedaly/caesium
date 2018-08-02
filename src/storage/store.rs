use encode::Decodable;
use quantile::writable::WritableSketch;
use regex::Regex;
use rocksdb;
use std::cmp::Ordering;
use std::str;
use storage::datasource::{DataRow, DataSource};
use storage::downsample::{DownsampleAction, DownsampleStrategy};
use storage::error::StorageError;
use storage::key::StorageKey;
use storage::value::StorageValue;
use storage::wildcard::{exact_prefix, wildcard_match};
use time::timestamp::TimeStamp;
use time::window::TimeWindow;

const WINDOWS_CF_NAME: &'static str = "windows";
const METRICS_CF_NAME: &'static str = "metrics";

pub struct MetricStore {
    raw_db: rocksdb::DB,
}

impl MetricStore {
    pub fn open(path: &str) -> Result<MetricStore, StorageError> {
        let column_families = vec![
            MetricStore::windows_cf_desc(),
            MetricStore::metrics_cf_desc(),
        ];
        let mut opts = rocksdb::Options::default();
        opts.create_if_missing(true);
        opts.create_missing_column_families(true);
        let raw_db = rocksdb::DB::open_cf_descriptors(&opts, path, column_families)?;
        Ok(MetricStore { raw_db })
    }

    pub fn destroy(path: &str) -> Result<(), StorageError> {
        let mut opts = rocksdb::Options::default();
        opts.create_if_missing(false);
        rocksdb::DB::destroy(&opts, path).map_err(From::from)
    }

    pub fn insert(
        &self,
        metric: &str,
        window: TimeWindow,
        sketch: WritableSketch,
    ) -> Result<(), StorageError> {
        MetricStore::validate_metric_name(metric)?;
        let key = StorageKey::as_bytes(metric, window.start())?;
        let val = StorageValue::as_bytes(window, sketch)?;
        debug!(
            "Inserting key for metric {} and window {:?}",
            metric, window
        );
        let mut batch = rocksdb::WriteBatch::default();
        batch.put_cf(self.metrics_cf()?, metric.as_bytes(), &[1u8; 0])?;
        batch.merge_cf(self.windows_cf()?, &key, &val)?;
        self.raw_db.write(batch)?;
        Ok(())
    }

    pub fn downsample<T>(&self, strategy: &T) -> Result<(), StorageError>
    where
        T: DownsampleStrategy,
    {
        let snapshot = self.raw_db.snapshot();
        let cf = self.windows_cf()?;
        let kv_iter = snapshot.iterator_cf(cf, rocksdb::IteratorMode::Start)?;
        for (key_bytes, val_bytes) in kv_iter {
            let key = StorageKey::decode(&mut &key_bytes[..])?;
            let val = StorageValue::decode(&mut &val_bytes[..])?;
            match strategy.get_action(val.window()) {
                DownsampleAction::Ignore => {
                    debug!("Ignored key during downsampling: {:?}", key);
                }
                DownsampleAction::Discard => {
                    debug!("Deleting key during downsampling: {:?}", key);
                    let key_bytes = key.to_bytes()?;
                    self.raw_db.delete_cf(cf, &key_bytes)?;
                }
                DownsampleAction::ExpandWindow(new_window) => {
                    debug!(
                        "Expanding window for key {:?} during downsampling: \
                         old_window={:?}, new_window={:?}",
                        key,
                        val.window(),
                        new_window
                    );
                    let mut batch = rocksdb::WriteBatch::default();
                    let old_key_bytes = key.to_bytes()?;
                    batch.delete_cf(cf, &old_key_bytes)?;

                    let new_key = key.with_window_start(new_window.start());
                    let key_bytes = new_key.to_bytes()?;
                    let new_val = val.with_window(new_window);
                    let val_bytes = new_val.to_bytes()?;
                    batch.merge_cf(cf, &key_bytes, &val_bytes)?;

                    self.raw_db.write(batch)?;
                }
            }
        }
        Ok(())
    }

    fn windows_cf_desc() -> rocksdb::ColumnFamilyDescriptor {
        let mut opts = rocksdb::Options::default();
        opts.set_comparator("key_comparator", MetricStore::compare_keys);
        opts.set_merge_operator("sketch_merger", MetricStore::merge_op, None);
        rocksdb::ColumnFamilyDescriptor::new(WINDOWS_CF_NAME, opts)
    }

    fn metrics_cf_desc() -> rocksdb::ColumnFamilyDescriptor {
        let opts = rocksdb::Options::default();
        rocksdb::ColumnFamilyDescriptor::new(METRICS_CF_NAME, opts)
    }

    fn windows_cf(&self) -> Result<rocksdb::ColumnFamily, StorageError> {
        self.raw_db
            .cf_handle(WINDOWS_CF_NAME)
            .ok_or(StorageError::InternalError(
                "Could not open windows column family",
            ))
    }

    fn metrics_cf(&self) -> Result<rocksdb::ColumnFamily, StorageError> {
        self.raw_db
            .cf_handle(METRICS_CF_NAME)
            .ok_or(StorageError::InternalError(
                "Could not open metrics column family",
            ))
    }

    fn compare_keys(mut x: &[u8], mut y: &[u8]) -> Ordering {
        let k1 = StorageKey::decode(&mut x).expect("Could not decode storage key");
        let k2 = StorageKey::decode(&mut y).expect("Could not decode storage key");
        k1.cmp(&k2)
    }

    fn merge_op(
        _key: &[u8],
        existing_val: Option<&[u8]>,
        operands: &mut rocksdb::MergeOperands,
    ) -> Option<Vec<u8>> {
        let mut value_opt: Option<StorageValue> =
            existing_val.and_then(|mut bytes| match StorageValue::decode(&mut bytes) {
                Ok(v) => Some(v),
                Err(err) => {
                    error!("Could not deserialize existing value: {:?}", err);
                    None
                }
            });

        for mut bytes in operands {
            value_opt = match StorageValue::decode(&mut bytes) {
                Ok(v1) => match value_opt {
                    None => Some(v1),
                    Some(v2) => Some(v1.merge(v2)),
                },
                Err(err) => {
                    error!("Could not deserialize operand value: {:?}", err);
                    value_opt
                }
            }
        }

        let result = value_opt.and_then(|v| match v.to_bytes() {
            Ok(bytes) => Some(bytes),
            Err(err) => {
                error!("Could not serialize merged value to bytes: {:?}", err);
                None
            }
        });

        // RocksDB will crash if we return `None` from a merge operation
        // Under normal operation, this should never happen
        assert!(
            result.is_some(),
            "Could not execute merge operation; storage DB is corrupted!"
        );

        result
    }

    fn validate_metric_name(s: &str) -> Result<(), StorageError> {
        lazy_static! {
            static ref METRIC_RE: Regex =
                Regex::new("^[a-zA-Z][a-zA-Z0-9._-]*$").expect("Could not compile regex");
        }
        if METRIC_RE.is_match(s) {
            Ok(())
        } else {
            Err(StorageError::InvalidMetricName)
        }
    }
}

impl DataSource for MetricStore {
    fn fetch<'a>(
        &'a self,
        metric: String,
        start: Option<TimeStamp>,
        end: Option<TimeStamp>,
    ) -> Result<Box<Iterator<Item = DataRow> + 'a>, StorageError> {
        MetricStore::validate_metric_name(&metric)?;
        let ts = start.unwrap_or(0);
        let end_ts = end.unwrap_or(u64::max_value());
        let start_key = StorageKey::as_bytes(&metric, ts)?;
        let cf = self.windows_cf()?;
        let kv_iter_mode = rocksdb::IteratorMode::From(&start_key, rocksdb::Direction::Forward);
        let kv_iter = self.raw_db.iterator_cf(cf, kv_iter_mode)?;
        let iter = kv_iter
            .filter_map(
                |(key_bytes, val_bytes)| match StorageKey::decode(&mut &key_bytes[..]) {
                    Ok(key) => Some((key, val_bytes)),
                    Err(err) => {
                        error!("Error decoding key: {:?}", err);
                        None
                    }
                },
            )
            .take_while(move |(key, _)| key.metric() == metric && key.window_start() < end_ts)
            .filter_map(
                |(_, val_bytes)| match StorageValue::decode(&mut &val_bytes[..]) {
                    Ok(val) => Some(val.to_data_row()),
                    Err(err) => {
                        error!("Error decoding value: {:?}", err);
                        None
                    }
                },
            );
        Ok(Box::new(iter))
    }

    fn search<'a>(
        &'a self,
        pattern: String,
    ) -> Result<Box<Iterator<Item = String> + 'a>, StorageError> {
        let prefix_str = exact_prefix(&pattern);
        let kv_iter_mode =
            rocksdb::IteratorMode::From(prefix_str.as_bytes(), rocksdb::Direction::Forward);
        let prefix_bytes = prefix_str.as_bytes().to_vec();
        let kv_iter = self.raw_db.iterator_cf(self.metrics_cf()?, kv_iter_mode)?;
        let metric_iter = kv_iter
            .take_while(move |(key, _)| key.starts_with(&prefix_bytes))
            .filter_map(move |(key, _)| match str::from_utf8(&*key) {
                Ok(metric) => {
                    if wildcard_match(metric, &pattern) {
                        Some(metric.to_string())
                    } else {
                        None
                    }
                }
                Err(err) => {
                    error!("Could not decode metric name: {:?}", err);
                    None
                }
            });
        Ok(Box::new(metric_iter))
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use quantile::writable::WritableSketch;
    use std::panic;
    use uuid::Uuid;

    #[test]
    fn it_fetches_no_result() {
        with_test_store(|store| {
            let mut row_iter = store
                .fetch("ghost".to_string(), None, None)
                .expect("Could not fetch range");
            for _ in 0..5 {
                let next_row = row_iter.next();
                assert!(next_row.is_none());
            }
        })
    }

    #[test]
    fn it_stores_and_fetches_sketch() {
        with_test_store(|store| {
            let metric = "foo".to_string();
            store
                .insert(&metric, TimeWindow::new(0, 30), build_sketch())
                .expect("Could not insert sketch");
            let rows: Vec<DataRow> = store
                .fetch(metric, None, None)
                .expect("Could not fetch range")
                .collect();
            assert_rows(rows, vec![(0, 30, 50)]);
        })
    }

    #[test]
    fn it_fetches_by_metric() {
        with_test_store(|store| {
            let metric = "foo".to_string();
            store
                .insert(&metric, TimeWindow::new(0, 30), build_sketch())
                .expect("Could not insert sketch");
            store
                .insert(&"bar", TimeWindow::new(60, 90), build_sketch())
                .expect("Could not insert sketch");
            let rows: Vec<DataRow> = store
                .fetch(metric, None, None)
                .expect("Could not fetch range")
                .collect();
            assert_rows(rows, vec![(0, 30, 50)]);
        })
    }

    #[test]
    fn it_fetches_select_by_time_range() {
        with_test_store(|store| {
            let metric = "foo".to_string();
            store
                .insert(&metric, TimeWindow::new(0, 30), build_sketch())
                .expect("Could not insert sketch");
            store
                .insert(&metric, TimeWindow::new(30, 60), build_sketch())
                .expect("Could not insert sketch");
            store
                .insert(&metric, TimeWindow::new(60, 90), build_sketch())
                .expect("Could not insert sketch");
            store
                .insert(&metric, TimeWindow::new(90, 120), build_sketch())
                .expect("Could not insert sketch");
            let rows: Vec<DataRow> = store
                .fetch(metric, Some(30), Some(90))
                .expect("Could not fetch range")
                .collect();
            assert_rows(rows, vec![(30, 60, 50), (60, 90, 50)]);
        })
    }

    #[test]
    fn it_fetches_by_metric_sequential_name_same_timestamp() {
        with_test_store(|store| {
            let (m1, m2) = ("m1".to_string(), "m2".to_string());
            store
                .insert(&m1, TimeWindow::new(30, 60), build_sketch())
                .expect("Could not insert first sketch");
            store
                .insert(&m2, TimeWindow::new(30, 60), build_sketch())
                .expect("Could not insert second sketch");
            let rows: Vec<DataRow> = store
                .fetch(m1, None, None)
                .expect("Could not fetch range")
                .collect();
            assert_rows(rows, vec![(30, 60, 50)]);
        })
    }

    #[test]
    fn it_fetches_by_time_range() {
        with_test_store(|store| {
            let metric = "foo".to_string();
            store
                .insert(&metric, TimeWindow::new(0, 30), build_sketch())
                .expect("Could not insert sketch");
            store
                .insert(&metric, TimeWindow::new(90, 120), build_sketch())
                .expect("Could not insert sketch");
            store
                .insert(&metric, TimeWindow::new(120, 150), build_sketch())
                .expect("Could not insert sketch");
            store
                .insert(&metric, TimeWindow::new(180, 210), build_sketch())
                .expect("Could not insert sketch");
            let rows: Vec<DataRow> = store
                .fetch(metric, Some(85), Some(150))
                .expect("Could not fetch range")
                .collect();
            assert_rows(rows, vec![(90, 120, 50), (120, 150, 50)]);
        })
    }

    #[test]
    fn it_merges_sketches_in_same_time_window() {
        with_test_store(|store| {
            let metric = "foo".to_string();
            store
                .insert(
                    &metric,
                    TimeWindow::new(0, 30),
                    build_sketch_with_values(vec![1, 2]),
                )
                .expect("Could not insert first sketch");
            store
                .insert(
                    &metric,
                    TimeWindow::new(0, 30),
                    build_sketch_with_values(vec![3]),
                )
                .expect("Could not insert second sketch");
            let rows: Vec<DataRow> = store
                .fetch(metric, None, None)
                .expect("Could not fetch range")
                .collect();
            assert_rows(rows, vec![(0, 30, 2)]);
        })
    }

    #[test]
    fn it_merges_sketches_with_overlapping_time_windows() {
        with_test_store(|store| {
            let metric = "foo".to_string();
            store
                .insert(
                    &metric,
                    TimeWindow::new(0, 30),
                    build_sketch_with_values(vec![1, 2]),
                )
                .expect("Could not insert first sketch");
            store
                .insert(
                    &metric,
                    TimeWindow::new(0, 90),
                    build_sketch_with_values(vec![3]),
                )
                .expect("Could not insert second sketch");
            let rows: Vec<DataRow> = store
                .fetch(metric, None, None)
                .expect("Could not fetch range")
                .collect();
            assert_rows(rows, vec![(0, 90, 2)]);
        })
    }

    #[test]
    fn it_validates_metric_name_on_insert() {
        with_test_store(
            |store| match store.insert(&"", TimeWindow::new(0, 30), build_sketch()) {
                Err(StorageError::InvalidMetricName) => {}
                _ => panic!("Expected invalid metric name error"),
            },
        )
    }

    #[test]
    fn it_validates_metric_name_on_fetch() {
        with_test_store(|store| match store.fetch("".to_string(), None, None) {
            Err(StorageError::InvalidMetricName) => {}
            _ => panic!("Expected invalid metric name error"),
        })
    }

    #[test]
    fn it_accepts_metric_name_with_number() {
        assert!(MetricStore::validate_metric_name("foo123").is_ok());
    }

    #[test]
    fn it_accepts_metric_name_with_period() {
        assert!(MetricStore::validate_metric_name("foo.bar").is_ok());
    }

    #[test]
    fn it_accepts_metric_name_with_hyphen() {
        assert!(MetricStore::validate_metric_name("foo-bar").is_ok());
    }

    #[test]
    fn it_accepts_metric_name_with_underscore() {
        assert!(MetricStore::validate_metric_name("foo_bar").is_ok());
    }

    #[test]
    fn it_accepts_metric_name_with_capitals() {
        assert!(MetricStore::validate_metric_name("FooBar").is_ok());
    }

    #[test]
    fn it_rejects_invalid_metric_names() {
        assert_eq!(MetricStore::validate_metric_name("").is_ok(), false);
        assert_eq!(MetricStore::validate_metric_name("1").is_ok(), false);
        assert_eq!(MetricStore::validate_metric_name("1foo").is_ok(), false);
        assert_eq!(MetricStore::validate_metric_name("foo&bar").is_ok(), false);
        assert_eq!(MetricStore::validate_metric_name(".foo").is_ok(), false);
        assert_eq!(MetricStore::validate_metric_name("_foo").is_ok(), false);
        assert_eq!(MetricStore::validate_metric_name("-foo").is_ok(), false);
    }

    #[test]
    fn it_handles_downsample_action_ignore() {
        with_test_store(|store| {
            let metric = "foo".to_string();
            store
                .insert(&metric, TimeWindow::new(0, 30), build_sketch())
                .expect("Could not insert sketch");

            let ignore_strategy = MockStrategy::new(DownsampleAction::Ignore);
            store
                .downsample(&ignore_strategy)
                .expect("Could not downsample");
            let rows: Vec<DataRow> = store
                .fetch(metric, None, None)
                .expect("Could not fetch range")
                .collect();
            assert_rows(rows, vec![(0, 30, 50)]);
        })
    }

    #[test]
    fn it_handles_downsample_action_discard() {
        with_test_store(|store| {
            let metric = "foo".to_string();
            store
                .insert(&metric, TimeWindow::new(0, 30), build_sketch())
                .expect("Could not insert sketch");

            let discard_strategy = MockStrategy::new(DownsampleAction::Discard);
            store
                .downsample(&discard_strategy)
                .expect("Could not downsample");
            let rows: Vec<DataRow> = store
                .fetch(metric, None, None)
                .expect("Could not fetch range")
                .collect();
            assert!(rows.is_empty());
        })
    }

    #[test]
    fn it_handles_downsample_action_update_window() {
        with_test_store(|store| {
            let metric = "foo".to_string();
            store
                .insert(&metric, TimeWindow::new(10, 20), build_sketch())
                .expect("Could not insert sketch");

            let new_window = TimeWindow::new(0, 30);
            let action = DownsampleAction::ExpandWindow(new_window);
            let expand_strategy = MockStrategy::new(action);
            store
                .downsample(&expand_strategy)
                .expect("Could not downsample");
            let rows: Vec<DataRow> = store
                .fetch(metric, None, None)
                .expect("Could not fetch range")
                .collect();
            assert_rows(rows, vec![(0, 30, 50)]);
        })
    }

    #[test]
    fn it_handles_downsample_action_update_window_with_merge() {
        with_test_store(|store| {
            let metric = "foo".to_string();
            store
                .insert(&metric, TimeWindow::new(10, 20), build_sketch())
                .expect("Could not insert sketch");
            store
                .insert(&metric, TimeWindow::new(20, 30), build_sketch())
                .expect("Could not insert sketch");

            let new_window = TimeWindow::new(0, 30);
            let action = DownsampleAction::ExpandWindow(new_window);
            let expand_strategy = MockStrategy::new(action);
            store
                .downsample(&expand_strategy)
                .expect("Could not downsample");
            let rows: Vec<DataRow> = store
                .fetch(metric, None, None)
                .expect("Could not fetch range")
                .collect();
            assert_rows(rows, vec![(0, 30, 50)]);
        })
    }

    #[test]
    fn it_searches_metric_names() {
        with_test_store(|store| {
            store
                .insert(&"foo", TimeWindow::new(0, 1), build_sketch())
                .expect("Could not insert sketch foo (first)");
            store
                .insert(&"foo", TimeWindow::new(1, 2), build_sketch())
                .expect("Could not insert sketch foo (second)");
            store
                .insert(&"foobar", TimeWindow::new(2, 3), build_sketch())
                .expect("Could not insert sketch foobar");
            store
                .insert(&"bazta", TimeWindow::new(3, 4), build_sketch())
                .expect("Could not insert sketch bazta");
            store
                .insert(&"batter", TimeWindow::new(4, 5), build_sketch())
                .expect("Could not insert sketch batter");

            let results: Vec<String> = store
                .search("foo*".to_string())
                .expect("Could not search (first)")
                .collect();
            assert_eq!(results, vec!["foo".to_string(), "foobar".to_string()]);

            let results: Vec<String> = store
                .search("*z*".to_string())
                .expect("Could not search (second)")
                .collect();
            assert_eq!(results, vec!["bazta".to_string()]);

            let results: Vec<String> = store
                .search("baz*".to_string())
                .expect("Could not search (third)")
                .collect();
            assert_eq!(results, vec!["bazta".to_string()]);

            let results: Vec<String> = store
                .search("x*".to_string())
                .expect("Could not search (fourth)")
                .collect();
            assert!(results.is_empty());

            let results: Vec<String> = store
                .search("foobar".to_string())
                .expect("Could not search (fifth)")
                .collect();
            assert_eq!(results, vec!["foobar".to_string()]);

            let results: Vec<String> = store
                .search("".to_string())
                .expect("Could not search (sixth)")
                .collect();
            assert!(results.is_empty());

            let results: Vec<String> = store
                .search("*".to_string())
                .expect("Could not search (seventh)")
                .collect();
            assert_eq!(results, vec!["batter", "bazta", "foo", "foobar"]);
        })
    }

    fn with_test_store<T>(test: T) -> ()
    where
        T: FnOnce(MetricStore) -> () + panic::UnwindSafe,
    {
        let path = format!("testdb_{}", Uuid::new_v4());
        MetricStore::destroy(&path).expect("Setup: could not destroy old test DB");
        let store = MetricStore::open(&path).expect("Setup: could not open test DB");
        let result = panic::catch_unwind(move || test(store));
        MetricStore::destroy(&path).expect("Teardown: could not destroy test DB");
        assert!(result.is_ok())
    }

    fn build_sketch_with_values(values: Vec<u64>) -> WritableSketch {
        let mut s = WritableSketch::new();
        for &i in values.iter() {
            s.insert(i);
        }
        s
    }

    fn build_sketch() -> WritableSketch {
        let vals: Vec<u64> = (0..100).map(|i| i as u64).collect();
        build_sketch_with_values(vals)
    }

    fn assert_rows(mut rows: Vec<DataRow>, expected: Vec<(TimeStamp, TimeStamp, u64)>) {
        assert_eq!(rows.len(), expected.len());
        for (row, (start, end, median)) in rows.drain(..).zip(expected) {
            assert_eq!(row.window.start(), start);
            assert_eq!(row.window.end(), end);
            let val = row.sketch
                .to_readable()
                .query(0.5)
                .map(|q| q.approx_value)
                .expect("Could not query for median");
            assert_eq!(val, median);
        }
    }

    struct MockStrategy {
        action: DownsampleAction,
    }

    impl MockStrategy {
        fn new(action: DownsampleAction) -> MockStrategy {
            MockStrategy { action }
        }
    }

    impl DownsampleStrategy for MockStrategy {
        fn get_action(&self, _: TimeWindow) -> DownsampleAction {
            self.action.clone()
        }
    }
}
