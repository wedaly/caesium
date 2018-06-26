use encode::{Decodable, Encodable, EncodableError};
use std::io::{Read, Write};

// Unix timestamp = seconds since 1970-01-01T00:00:00Z
pub type TimeStamp = u64;

#[derive(Debug, Copy, Clone, Ord, Eq, PartialEq, PartialOrd)]
pub struct TimeWindow {
    start: TimeStamp,
    end: TimeStamp,
}

const SECONDS_PER_HOUR: u64 = 3600;
const SECONDS_PER_DAY: u64 = SECONDS_PER_HOUR * 24;

pub fn hours(ts: TimeStamp) -> u64 {
    ts / SECONDS_PER_HOUR
}

pub fn days(ts: TimeStamp) -> u64 {
    ts / SECONDS_PER_DAY
}

impl TimeWindow {
    pub fn new(start: TimeStamp, end: TimeStamp) -> TimeWindow {
        assert!(start <= end);
        TimeWindow { start, end }
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
}

impl<W> Encodable<W> for TimeWindow
where
    W: Write,
{
    fn encode(&self, writer: &mut W) -> Result<(), EncodableError> {
        self.start.encode(writer)?;
        self.end.encode(writer)?;
        Ok(())
    }
}

impl<R> Decodable<TimeWindow, R> for TimeWindow
where
    R: Read,
{
    fn decode(reader: &mut R) -> Result<TimeWindow, EncodableError> {
        let start = TimeStamp::decode(reader)?;
        let end = TimeStamp::decode(reader)?;
        if start > end {
            Err(EncodableError::FormatError(
                "TimeWindow start must be less than end",
            ))
        } else {
            Ok(TimeWindow::new(start, end))
        }
    }
}
