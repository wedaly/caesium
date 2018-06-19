use encode::{Decodable, Encodable, EncodableError};
use std::io::{Read, Write};

// Unix timestamp = seconds since 1970-01-01T00:00:00Z
pub type TimeStamp = u64;

// A "bucket" is the smallest representable time range the system can store
// Each bucket is assigned a unique ID, and each timestamp is assigned to exactly one bucket.
pub type TimeBucket = u64;

pub const SECONDS_PER_BUCKET: u64 = 30;

pub fn ts_to_bucket(ts: TimeStamp, bucket_size: u64) -> TimeBucket {
    (ts / SECONDS_PER_BUCKET) / bucket_size
}

#[derive(Debug, Copy, Clone, PartialEq)]
pub struct TimeRange {
    start: TimeStamp,
    end: TimeStamp,
}

impl TimeRange {
    pub fn new(start: TimeStamp, end: TimeStamp) -> TimeRange {
        assert!(start <= end);
        TimeRange { start, end }
    }

    pub fn from_bucket(bucket: TimeBucket, bucket_size: u64) -> TimeRange {
        let start = bucket * bucket_size * SECONDS_PER_BUCKET;
        let end = start + (bucket_size * SECONDS_PER_BUCKET);
        TimeRange { start, end }
    }

    pub fn start(&self) -> TimeStamp {
        self.start
    }

    pub fn end(&self) -> TimeStamp {
        self.end
    }

    pub fn duration(&self) -> u64 {
        self.end - self.start
    }

    pub fn to_bucket(&self, bucket_size: u64) -> TimeBucket {
        ts_to_bucket(self.start, bucket_size)
    }
}

impl<W> Encodable<W> for TimeRange
where
    W: Write,
{
    fn encode(&self, writer: &mut W) -> Result<(), EncodableError> {
        self.start.encode(writer)?;
        self.end.encode(writer)?;
        Ok(())
    }
}

impl<R> Decodable<TimeRange, R> for TimeRange
where
    R: Read,
{
    fn decode(reader: &mut R) -> Result<TimeRange, EncodableError> {
        let start = TimeStamp::decode(reader)?;
        let end = TimeStamp::decode(reader)?;
        if start > end {
            Err(EncodableError::FormatError(
                "TimeRange start must be less than end",
            ))
        } else {
            Ok(TimeRange::new(start, end))
        }
    }
}
