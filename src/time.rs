pub type TimeStamp = u64;

#[derive(Debug, Copy, Clone, PartialEq)]
pub struct TimeRange {
    pub start: TimeStamp,
    pub end: TimeStamp,
}

const TIME_BUCKET_INTERVAL: u64 = 30_000;

pub type TimeBucket = u64;

pub fn ts_to_bucket(ts: TimeStamp) -> TimeBucket {
    ts / TIME_BUCKET_INTERVAL
}

pub fn bucket_to_range(bucket: TimeBucket) -> TimeRange {
    let start = bucket * TIME_BUCKET_INTERVAL;
    let end = start + TIME_BUCKET_INTERVAL;
    TimeRange { start, end }
}
