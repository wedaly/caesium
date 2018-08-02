use encode::Decodable;
use quantile::writable::WritableSketch;
use regex::Regex;
use rocksdb;
use std::cmp::Ordering;
use storage::datasource::{DataRow, DataSource};
use storage::downsample::{DownsampleAction, DownsampleStrategy};
use storage::error::StorageError;
use storage::key::StorageKey;
use storage::value::StorageValue;
use time::timestamp::TimeStamp;
use time::window::TimeWindow;

pub struct MetricStore {
    raw_db: rocksdb::DB,
}

impl MetricStore {
    pub fn open(path: &str) -> Result<MetricStore, StorageError> {
        let mut opts = rocksdb::Options::default();
        opts.create_if_missing(true);
        opts.set_comparator("key_comparator", MetricStore::compare_keys);
        opts.set_merge_operator("sketch_merger", MetricStore::merge_op, None);
        let raw_db = rocksdb::DB::open(&opts, path)?;
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
        debug!("Inserted key for metric {} and window {:?}", metric, window);
        self.raw_db.merge(&key, &val)?;
        Ok(())
    }

    pub fn downsample<T>(&self, strategy: &T) -> Result<(), StorageError>
    where
        T: DownsampleStrategy,
    {
        let snapshot = self.raw_db.snapshot();
        let kv_iter = snapshot.iterator(rocksdb::IteratorMode::Start);
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
                    self.raw_db.delete(&key_bytes)?;
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
                    batch.delete(&old_key_bytes)?;

                    let new_key = key.with_window_start(new_window.start());
                    let key_bytes = new_key.to_bytes()?;
                    let new_val = val.with_window(new_window);
                    let val_bytes = new_val.to_bytes()?;
                    batch.merge(&key_bytes, &val_bytes)?;

                    self.raw_db.write(batch)?;
                }
            }
        }
        Ok(())
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
    fn fetch_range<'a>(
        &'a self,
        metric: &str,
        start: Option<TimeStamp>,
        end: Option<TimeStamp>,
    ) -> Result<Box<Iterator<Item = DataRow> + 'a>, StorageError> {
        MetricStore::validate_metric_name(metric)?;
        let ts = start.unwrap_or(0);
        let end_ts = end.unwrap_or(u64::max_value());
        let start_key = StorageKey::as_bytes(metric, ts)?;
        let metric = metric.to_string();
        let kv_iter = self.raw_db.iterator(rocksdb::IteratorMode::From(
            &start_key,
            rocksdb::Direction::Forward,
        ));
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
                .fetch_range(&"ghost", None, None)
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
            let metric = "foo";
            store
                .insert(&metric, TimeWindow::new(0, 30), build_sketch())
                .expect("Could not insert sketch");
            let rows: Vec<DataRow> = store
                .fetch_range(&metric, None, None)
                .expect("Could not fetch range")
                .collect();
            assert_rows(rows, vec![(0, 30, 50)]);
        })
    }

    #[test]
    fn it_fetches_by_metric() {
        with_test_store(|store| {
            let metric = "foo";
            store
                .insert(&metric, TimeWindow::new(0, 30), build_sketch())
                .expect("Could not insert sketch");
            store
                .insert(&"bar", TimeWindow::new(60, 90), build_sketch())
                .expect("Could not insert sketch");
            let rows: Vec<DataRow> = store
                .fetch_range(&metric, None, None)
                .expect("Could not fetch range")
                .collect();
            assert_rows(rows, vec![(0, 30, 50)]);
        })
    }

    #[test]
    fn it_fetches_select_by_time_range() {
        with_test_store(|store| {
            store
                .insert(&"foo", TimeWindow::new(0, 30), build_sketch())
                .expect("Could not insert sketch");
            store
                .insert(&"foo", TimeWindow::new(30, 60), build_sketch())
                .expect("Could not insert sketch");
            store
                .insert(&"foo", TimeWindow::new(60, 90), build_sketch())
                .expect("Could not insert sketch");
            store
                .insert(&"foo", TimeWindow::new(90, 120), build_sketch())
                .expect("Could not insert sketch");
            let rows: Vec<DataRow> = store
                .fetch_range(&"foo", Some(30), Some(90))
                .expect("Could not fetch range")
                .collect();
            assert_rows(rows, vec![(30, 60, 50), (60, 90, 50)]);
        })
    }

    #[test]
    fn it_fetches_by_metric_sequential_name_same_timestamp() {
        with_test_store(|store| {
            let (m1, m2) = ("m1", "m2");
            store
                .insert(&m1, TimeWindow::new(30, 60), build_sketch())
                .expect("Could not insert first sketch");
            store
                .insert(&m2, TimeWindow::new(30, 60), build_sketch())
                .expect("Could not insert second sketch");
            let rows: Vec<DataRow> = store
                .fetch_range(&m1, None, None)
                .expect("Could not fetch range")
                .collect();
            assert_rows(rows, vec![(30, 60, 50)]);
        })
    }

    #[test]
    fn it_fetches_by_time_range() {
        with_test_store(|store| {
            let metric = "foo";
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
                .fetch_range(&metric, Some(85), Some(150))
                .expect("Could not fetch range")
                .collect();
            assert_rows(rows, vec![(90, 120, 50), (120, 150, 50)]);
        })
    }

    #[test]
    fn it_merges_sketches_in_same_time_window() {
        with_test_store(|store| {
            let metric = "foo";
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
                .fetch_range(&metric, None, None)
                .expect("Could not fetch range")
                .collect();
            assert_rows(rows, vec![(0, 30, 2)]);
        })
    }

    #[test]
    fn it_merges_sketches_with_overlapping_time_windows() {
        with_test_store(|store| {
            let metric = "foo";
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
                .fetch_range(&metric, None, None)
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
        with_test_store(|store| match store.fetch_range(&"", None, None) {
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
            store
                .insert(&"foo", TimeWindow::new(0, 30), build_sketch())
                .expect("Could not insert sketch");

            let ignore_strategy = MockStrategy::new(DownsampleAction::Ignore);
            store
                .downsample(&ignore_strategy)
                .expect("Could not downsample");
            let rows: Vec<DataRow> = store
                .fetch_range(&"foo", None, None)
                .expect("Could not fetch range")
                .collect();
            assert_rows(rows, vec![(0, 30, 50)]);
        })
    }

    #[test]
    fn it_handles_downsample_action_discard() {
        with_test_store(|store| {
            store
                .insert(&"foo", TimeWindow::new(0, 30), build_sketch())
                .expect("Could not insert sketch");

            let discard_strategy = MockStrategy::new(DownsampleAction::Discard);
            store
                .downsample(&discard_strategy)
                .expect("Could not downsample");
            let rows: Vec<DataRow> = store
                .fetch_range(&"foo", None, None)
                .expect("Could not fetch range")
                .collect();
            assert!(rows.is_empty());
        })
    }

    #[test]
    fn it_handles_downsample_action_update_window() {
        with_test_store(|store| {
            store
                .insert(&"foo", TimeWindow::new(10, 20), build_sketch())
                .expect("Could not insert sketch");

            let new_window = TimeWindow::new(0, 30);
            let action = DownsampleAction::ExpandWindow(new_window);
            let expand_strategy = MockStrategy::new(action);
            store
                .downsample(&expand_strategy)
                .expect("Could not downsample");
            let rows: Vec<DataRow> = store
                .fetch_range(&"foo", None, None)
                .expect("Could not fetch range")
                .collect();
            assert_rows(rows, vec![(0, 30, 50)]);
        })
    }

    #[test]
    fn it_handles_downsample_action_update_window_with_merge() {
        with_test_store(|store| {
            store
                .insert(&"foo", TimeWindow::new(10, 20), build_sketch())
                .expect("Could not insert sketch");
            store
                .insert(&"foo", TimeWindow::new(20, 30), build_sketch())
                .expect("Could not insert sketch");

            let new_window = TimeWindow::new(0, 30);
            let action = DownsampleAction::ExpandWindow(new_window);
            let expand_strategy = MockStrategy::new(action);
            store
                .downsample(&expand_strategy)
                .expect("Could not downsample");
            let rows: Vec<DataRow> = store
                .fetch_range(&"foo", None, None)
                .expect("Could not fetch range")
                .collect();
            assert_rows(rows, vec![(0, 30, 50)]);
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
