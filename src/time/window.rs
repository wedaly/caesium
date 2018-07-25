use encode::{Decodable, Encodable, EncodableError};
use std::io::{Read, Write};
use time::timestamp::TimeStamp;

#[derive(Debug, Copy, Clone, Ord, Eq, PartialEq, PartialOrd)]
pub struct TimeWindow {
    start: TimeStamp,
    end: TimeStamp,
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

    pub fn overlaps(&self, other: &TimeWindow) -> bool {
        !self.disjoint(other)
    }

    pub fn disjoint(&self, other: &TimeWindow) -> bool {
        self.end <= other.start || self.start >= other.end
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
