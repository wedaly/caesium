use encode::{Decodable, Encodable};
use quantile::writable::WritableSketch;
use rocksdb;
use std::cmp::{max, min};
use std::io::Cursor;
use storage::datasource::{DataCursor, DataRow, DataSource};
use storage::error::StorageError;
use time::{TimeStamp, TimeWindow};

pub struct MetricStore {
    raw_db: rocksdb::DB,
}

impl MetricStore {
    pub fn open(path: &str) -> Result<MetricStore, StorageError> {
        let mut opts = rocksdb::Options::default();
        opts.create_if_missing(true);
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
        let key = MetricStore::key(metric, window.start())?;
        let val = MetricStore::val(window, sketch)?;
        debug!("Inserted key for metric {} and window {:?}", metric, window);
        self.raw_db.merge(&key, &val)?;
        Ok(())
    }

    fn merge_op(
        _key: &[u8],
        existing_val: Option<&[u8]>,
        operands: &mut rocksdb::MergeOperands,
    ) -> Option<Vec<u8>> {
        let mut window_start = TimeStamp::max_value();
        let mut window_end = 0;
        let mut merged = WritableSketch::new();
        if let Some(bytes) = existing_val {
            merged = MetricStore::safe_merge(merged, &mut window_start, &mut window_end, bytes);
        }

        for mut bytes in operands {
            merged = MetricStore::safe_merge(merged, &mut window_start, &mut window_end, bytes);
        }

        let window = TimeWindow::new(window_start, window_end);
        MetricStore::safe_encode(window, merged)
    }

    fn safe_merge(
        dst: WritableSketch,
        start: &mut TimeStamp,
        end: &mut TimeStamp,
        bytes: &[u8],
    ) -> WritableSketch {
        let mut cursor = Cursor::new(bytes);
        let (merged_start, merged_end) = match TimeWindow::decode(&mut cursor) {
            Ok(w) => {
                let min_start = min(*start, w.start());
                let max_end = max(*end, w.end());
                (min_start, max_end)
            }
            Err(err) => {
                error!("Could not deserialize time window from DB value: {:?}", err);
                return dst;
            }
        };

        match WritableSketch::decode(&mut cursor) {
            Ok(s) => {
                *start = merged_start;
                *end = merged_end;
                dst.merge(s)
            }
            Err(err) => {
                error!("Could not deserialize sketch from DB value: {:?}", err);
                dst
            }
        }
    }

    fn safe_encode(window: TimeWindow, sketch: WritableSketch) -> Option<Vec<u8>> {
        let mut buf = Vec::new();
        if let Err(err) = window.encode(&mut buf) {
            error!("Could not encode time window to DB value: {:?}", err);
            None
        } else if let Err(err) = sketch.encode(&mut buf) {
            error!("Could not encode sketch to DB value: {:?}", err);
            None
        } else {
            Some(buf)
        }
    }

    fn key(metric: &str, window_start: TimeStamp) -> Result<Vec<u8>, StorageError> {
        let mut buf = Vec::new();
        metric.encode(&mut buf)?;
        window_start.encode(&mut buf)?;
        Ok(buf)
    }

    fn val(window: TimeWindow, sketch: WritableSketch) -> Result<Vec<u8>, StorageError> {
        let mut buf = Vec::new();
        window.encode(&mut buf)?;
        sketch.encode(&mut buf)?;
        Ok(buf)
    }
}

impl DataSource for MetricStore {
    fn fetch_range<'a>(
        &'a self,
        metric: &str,
        start: Option<TimeStamp>,
        end: Option<TimeStamp>,
    ) -> Result<Box<DataCursor + 'a>, StorageError> {
        let ts = start.unwrap_or(0);
        let end_ts = end.unwrap_or(u64::max_value());
        let prefix = MetricStore::key(metric, ts)?;
        let raw_iter = self.raw_db.prefix_iterator(&prefix);
        let cursor = MetricCursor::new(raw_iter, metric.to_string(), end_ts);
        Ok(Box::new(cursor))
    }
}

pub struct MetricCursor {
    raw_iter: rocksdb::DBIterator,
    metric: String,
    end: TimeStamp,
}

impl MetricCursor {
    fn new(raw_iter: rocksdb::DBIterator, metric: String, end: TimeStamp) -> MetricCursor {
        MetricCursor {
            raw_iter,
            metric,
            end,
        }
    }
}

impl DataCursor for MetricCursor {
    fn get_next(&mut self) -> Result<Option<DataRow>, StorageError> {
        let row_opt = match self.raw_iter.next() {
            None => None,
            Some((key, val)) => {
                let mut key_reader = Cursor::new(key);
                let metric = String::decode(&mut key_reader)?;
                let window_start = u64::decode(&mut key_reader)?;
                if metric != self.metric || window_start >= self.end {
                    None
                } else {
                    debug!(
                        "Fetching key for metric {} and timestamp {}",
                        metric, window_start
                    );
                    let mut val_reader = Cursor::new(val);
                    let window = TimeWindow::decode(&mut val_reader)?;
                    let sketch = WritableSketch::decode(&mut val_reader)?;
                    Some(DataRow { window, sketch })
                }
            }
        };
        Ok(row_opt)
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
            let mut cursor = store
                .fetch_range(&"ghost", None, None)
                .expect("Could not fetch range");
            for _ in 0..5 {
                let next_row = cursor.get_next().expect("Could not get next row");
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
            let mut cursor = store
                .fetch_range(&metric, None, None)
                .expect("Could not fetch range");
            let first_row = cursor.get_next().expect("Could not get first row");
            assert_row(first_row, 0, 30, 50);
            let next_row = cursor.get_next().expect("Could not get next row");
            assert!(next_row.is_none());
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
            let mut cursor = store
                .fetch_range(&metric, None, None)
                .expect("Could not fetch range");
            let first_row = cursor.get_next().expect("Could not get first row");
            assert_row(first_row, 0, 30, 50);
            let next_row = cursor.get_next().expect("Could not get next row");
            assert!(next_row.is_none());
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
            let mut cursor = store
                .fetch_range(&"foo", Some(30), Some(90))
                .expect("Could not fetch range");
            let first_row = cursor.get_next().expect("Could not get first row");
            assert_row(first_row, 30, 60, 50);
            let second_row = cursor.get_next().expect("Could not get second row");
            assert_row(second_row, 60, 90, 50);
            let next_row = cursor.get_next().expect("Could not get next row");
            assert!(next_row.is_none());
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
            let mut cursor = store
                .fetch_range(&m1, None, None)
                .expect("Could not fetch range");
            let _ = cursor.get_next().expect("Could not get first row");
            let next_row = cursor.get_next().expect("Could not get next row");
            assert!(next_row.is_none());
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
            let mut cursor = store
                .fetch_range(&metric, Some(85), Some(150))
                .expect("Could not fetch range");
            let first_row = cursor.get_next().expect("Could not get first row");
            assert_row(first_row, 90, 120, 50);
            let second_row = cursor.get_next().expect("Could not get second row");
            assert_row(second_row, 120, 150, 50);
            let next_row = cursor.get_next().expect("Could not get next row");
            assert!(next_row.is_none());
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
            let mut cursor = store
                .fetch_range(&metric, None, None)
                .expect("Could not fetch range");
            let first_row = cursor.get_next().expect("Could not get first row");
            assert_row(first_row, 0, 30, 2);
            let next_row = cursor.get_next().expect("Could not get next row");
            assert!(next_row.is_none());
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
            let mut cursor = store
                .fetch_range(&metric, None, None)
                .expect("Could not fetch range");
            let first_row = cursor.get_next().expect("Could not get first row");
            assert_row(first_row, 0, 90, 2);
            let next_row = cursor.get_next().expect("Could not get next row");
            assert!(next_row.is_none());
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

    fn assert_row(row_opt: Option<DataRow>, start: TimeStamp, end: TimeStamp, median: u64) {
        if let Some(row) = row_opt {
            assert_eq!(row.window.start(), start);
            assert_eq!(row.window.end(), end);
            let val = row.sketch
                .to_readable()
                .query(0.5)
                .expect("Could not query for median");
            assert_eq!(val, median);
        } else {
            panic!("Expected a row, but got None!");
        }
    }
}
