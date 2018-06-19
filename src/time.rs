pub type TimeStamp = u64;

#[derive(Debug, Copy, Clone, PartialEq)]
pub struct TimeRange {
    pub start: TimeStamp,
    pub end: TimeStamp,
}

pub const TIME_BUCKET_MS: u64 = 30_000;

pub type TimeBucket = u64;

pub fn ts_to_bucket(ts: TimeStamp, bucket_size: u64) -> TimeBucket {
    (ts / TIME_BUCKET_MS) / bucket_size
}

pub fn bucket_to_range(bucket: TimeBucket, bucket_size: u64) -> TimeRange {
    let start = bucket * bucket_size * TIME_BUCKET_MS;
    let end = start + (bucket_size * TIME_BUCKET_MS);
    TimeRange { start, end }
}
