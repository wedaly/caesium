// Unix timestamp = seconds since 1970-01-01T00:00:00Z
pub type TimeStamp = u64;

const SECONDS_PER_HOUR: u64 = 3600;
const SECONDS_PER_DAY: u64 = SECONDS_PER_HOUR * 24;

pub fn hours(ts: TimeStamp) -> u64 {
    ts / SECONDS_PER_HOUR
}

pub fn days(ts: TimeStamp) -> u64 {
    ts / SECONDS_PER_DAY
}
