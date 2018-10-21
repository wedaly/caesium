use std::time::{Duration, Instant};

pub struct Timer {
    start: Option<Instant>,
}

impl Timer {
    pub fn new() -> Timer {
        Timer { start: None }
    }

    pub fn start(&mut self) {
        self.start = Some(Instant::now());
    }

    pub fn stop(&mut self) -> Option<Duration> {
        self.start.map(|start| Instant::now().duration_since(start))
    }
}
