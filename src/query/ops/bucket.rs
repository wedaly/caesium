use quantile::mergable::MergableSketch;
use query::error::QueryError;
use query::ops::{OpOutput, QueryOp};
use std::collections::BTreeMap;
use time::{TimeBucket, TimeRange, SECONDS_PER_BUCKET};

pub struct BucketOp<'a> {
    bucket_size: u64,
    input: Box<QueryOp + 'a>,
    bucket_map: BTreeMap<TimeBucket, MergableSketch>,
    bucket_queue: Vec<TimeBucket>,
}

impl<'a> BucketOp<'a> {
    pub fn new(hours: u64, input: Box<QueryOp + 'a>) -> BucketOp {
        BucketOp {
            bucket_size: BucketOp::bucket_size(hours),
            input: input,
            bucket_map: BTreeMap::new(),
            bucket_queue: Vec::new(),
        }
    }

    fn build_window_map(&mut self) -> Result<(), QueryError> {
        loop {
            match self.input.get_next() {
                Ok(OpOutput::Sketch(window, sketch)) => {
                    BucketOp::validate_window(window)?;
                    let bucket = window.to_bucket(self.bucket_size);
                    self.bucket_map
                        .entry(bucket)
                        .and_modify(|s| s.merge(&sketch))
                        .or_insert_with(|| MergableSketch::empty());
                }
                Ok(OpOutput::End) => return Ok(()),
                Err(err) => return Err(err),
                _ => return Err(QueryError::InvalidInput),
            }
        }
    }

    fn build_key_queue(&mut self) {
        for key in self.bucket_map.keys() {
            self.bucket_queue.push(*key);
        }
        self.bucket_queue.reverse();
    }

    fn bucket_size(hours: u64) -> u64 {
        let buckets_per_hr = 3_600 / SECONDS_PER_BUCKET;
        hours * buckets_per_hr
    }

    fn window_for_bucket(&self, bucket: TimeBucket) -> TimeRange {
        TimeRange::from_bucket(bucket, self.bucket_size)
    }

    fn validate_window(window: TimeRange) -> Result<(), QueryError> {
        let duration = window.duration();
        if duration > SECONDS_PER_BUCKET {
            // Example: bucket(1, bucket(24, fetch(foo)))
            Err(QueryError::InvalidWindowSize(duration))
        } else {
            Ok(())
        }
    }
}

impl<'a> QueryOp for BucketOp<'a> {
    fn get_next(&mut self) -> Result<OpOutput, QueryError> {
        if self.bucket_map.is_empty() {
            self.build_window_map()?;
            self.build_key_queue();
        }

        match self.bucket_queue.pop() {
            Some(bucket) => {
                if let Some(sketch) = self.bucket_map.remove(&bucket) {
                    let window = self.window_for_bucket(bucket);
                    let output = OpOutput::Sketch(window, sketch);
                    Ok(output)
                } else {
                    panic!("Could not retrieve sketch from bucket map");
                }
            }
            None => Ok(OpOutput::End),
        }
    }
}
