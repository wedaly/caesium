use encode::{Decodable, Encodable};
use quantile::mergable::MergableSketch;
use quantile::serializable::SerializableSketch;
use rocksdb;
use std::io::Cursor;
use storage::datasource::{DataCursor, DataRow, DataSource};
use storage::error::StorageError;
use time::{ts_to_bucket, TimeBucket, TimeWindow, TimeStamp};

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
        bucket: TimeBucket,
        sketch: SerializableSketch,
    ) -> Result<(), StorageError> {
        let key = MetricStore::key(metric, bucket)?;
        let val = MetricStore::val(sketch)?;
        debug!("Inserted key for metric {} and bucket {}", metric, bucket);
        self.raw_db.merge(&key, &val)?;
        Ok(())
    }

    fn merge_op(
        _key: &[u8],
        existing_val: Option<&[u8]>,
        operands: &mut rocksdb::MergeOperands,
    ) -> Option<Vec<u8>> {
        let mut merged = MergableSketch::empty();
        if let Some(bytes) = existing_val {
            MetricStore::safe_merge(&mut merged, bytes);
        }

        for mut bytes in operands {
            MetricStore::safe_merge(&mut merged, bytes);
        }

        let mut buf = Vec::new();
        match merged.to_serializable().encode(&mut buf) {
            Ok(()) => Some(buf),
            Err(err) => {
                error!("Could not serialize merged sketch to DB value: {:?}", err);
                None
            }
        }
    }

    fn safe_merge(dst: &mut MergableSketch, mut bytes: &[u8]) {
        match SerializableSketch::decode(&mut bytes) {
            Ok(s) => {
                dst.merge(&s.to_mergable());
            }
            Err(err) => {
                error!("Could not deserialize sketch from DB value: {:?}", err);
            }
        }
    }

    fn key(metric: &str, bucket: TimeBucket) -> Result<Vec<u8>, StorageError> {
        let mut buf = Vec::new();
        metric.encode(&mut buf)?;
        bucket.encode(&mut buf)?;
        Ok(buf)
    }

    fn val(sketch: SerializableSketch) -> Result<Vec<u8>, StorageError> {
        let mut buf = Vec::new();
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
        let prefix = MetricStore::key(metric, ts_to_bucket(ts, 1))?;
        let raw_iter = self.raw_db.prefix_iterator(&prefix);
        let cursor = MetricCursor::new(raw_iter, metric.to_string(), ts_to_bucket(end_ts, 1));
        Ok(Box::new(cursor))
    }
}

pub struct MetricCursor {
    raw_iter: rocksdb::DBIterator,
    metric: String,
    end: TimeBucket,
}

impl MetricCursor {
    fn new(raw_iter: rocksdb::DBIterator, metric: String, end: TimeBucket) -> MetricCursor {
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
                let mut key_buf = Cursor::new(key);
                let metric = String::decode(&mut key_buf)?;
                let bucket: TimeBucket = u64::decode(&mut key_buf)?;
                if metric != self.metric || bucket > self.end {
                    None
                } else {
                    debug!("Fetching key for metric {} and bucket {}", metric, bucket);
                    let mut val_bytes: &[u8] = &val;
                    let window = TimeWindow::from_bucket(bucket, 1);
                    let sketch = SerializableSketch::decode(&mut val_bytes)?.to_mergable();
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
                .insert(&metric, 0, build_sketch())
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
                .insert(&metric, 0, build_sketch())
                .expect("Could not insert sketch");
            store
                .insert(&"bar", 2, build_sketch())
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
    fn it_fetches_by_metric_sequential_name_same_timestamp() {
        with_test_store(|store| {
            let (m1, m2) = ("m1", "m2");
            store
                .insert(&m1, 1, build_sketch())
                .expect("Could not insert first sketch");
            store
                .insert(&m2, 1, build_sketch())
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
                .insert(&metric, 0, build_sketch())
                .expect("Could not insert sketch");
            store
                .insert(&metric, 3, build_sketch())
                .expect("Could not insert sketch");
            store
                .insert(&metric, 4, build_sketch())
                .expect("Could not insert sketch");
            store
                .insert(&metric, 6, build_sketch())
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
    fn it_merges_sketches_in_same_time_bucket() {
        with_test_store(|store| {
            let metric = "foo";
            store
                .insert(&metric, 0, build_sketch_with_values(vec![1, 2]))
                .expect("Could not insert first sketch");
            store
                .insert(&metric, 0, build_sketch_with_values(vec![3]))
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

    fn build_sketch_with_values(values: Vec<u64>) -> SerializableSketch {
        let mut s = WritableSketch::new();
        for &i in values.iter() {
            s.insert(i);
        }
        s.to_serializable()
    }

    fn build_sketch() -> SerializableSketch {
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
