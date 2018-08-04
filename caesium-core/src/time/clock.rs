use std::time::{SystemTime, UNIX_EPOCH};
use time::timestamp::TimeStamp;

pub trait Clock {
    fn now(&self) -> TimeStamp;
}

pub struct SystemClock {}

impl SystemClock {
    pub fn new() -> SystemClock {
        SystemClock {}
    }
}

impl Clock for SystemClock {
    fn now(&self) -> TimeStamp {
        SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .expect("Time went backwards")
            .as_secs()
    }
}

pub struct MockClock {
    ts: TimeStamp,
}

impl MockClock {
    pub fn new(ts: TimeStamp) -> MockClock {
        MockClock { ts }
    }

    pub fn tick(&mut self, seconds: u64) {
        self.ts += seconds;
    }
}

impl Clock for MockClock {
    fn now(&self) -> TimeStamp {
        self.ts
    }
}
