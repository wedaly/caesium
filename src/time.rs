pub type TimeStamp = u64;

#[derive(Debug, Copy, Clone, PartialEq)]
pub struct TimeRange {
    pub start: TimeStamp,
    pub end: TimeStamp,
}
