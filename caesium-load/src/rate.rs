use std::time::SystemTime;

pub struct RateLimiter {
    limit: Option<usize>,
    count: usize,
    start: SystemTime,
}

impl RateLimiter {
    pub fn new(limit: Option<usize>) -> RateLimiter {
        RateLimiter {
            limit,
            count: 0,
            start: SystemTime::now(),
        }
    }

    pub fn increment(&mut self) {
        if self.is_within_window() {
            self.count += 1;
        } else {
            self.count = 1;
            self.start = SystemTime::now();
        }
    }

    pub fn is_within_limit(&self) -> bool {
        match self.limit {
            None => true,
            Some(limit) => self.count < limit || !self.is_within_window(),
        }
    }

    fn is_within_window(&self) -> bool {
        match self.start.elapsed() {
            Ok(elapsed) => elapsed.as_secs() < 1,
            Err(_) => true,
        }
    }
}
