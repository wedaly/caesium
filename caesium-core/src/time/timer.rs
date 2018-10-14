use std::time::{Duration, Instant};

pub struct Timer {
    start: Option<Instant>,
    end: Option<Instant>,
}

impl Timer {
    pub fn new() -> Timer {
        Timer {
            start: None,
            end: None,
        }
    }

    pub fn start(&mut self) {
        self.end = None;
        self.start = Some(Instant::now());
    }

    pub fn stop(&mut self) {
        self.end = Some(Instant::now());
        assert!(self.start.is_some());
    }

    pub fn duration(&self) -> Option<Duration> {
        match (self.start, self.end) {
            (Some(start), Some(end)) => Some(end.duration_since(start)),
            _ => None,
        }
    }

    pub fn reset(&mut self) {
        self.start = None;
        self.end = None;
    }
}
