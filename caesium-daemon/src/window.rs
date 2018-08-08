use caesium_core::time::clock::Clock;
use caesium_core::time::timestamp::TimeStamp;
use caesium_core::time::window::TimeWindow;

pub struct WindowTracker {
    window_size: u64,
    window: TimeWindow,
}

impl WindowTracker {
    pub fn new(window_size: u64, clock: &Clock) -> WindowTracker {
        let window = WindowTracker::window_for_ts(clock.now(), window_size);
        WindowTracker {
            window_size,
            window,
        }
    }

    pub fn update(&mut self, clock: &Clock) -> Option<TimeWindow> {
        let now = clock.now();
        if now >= self.window.end() {
            let window = self.window;
            self.window = WindowTracker::window_for_ts(now, self.window_size);
            Some(window)
        } else {
            None
        }
    }

    fn window_for_ts(ts: TimeStamp, window_size: u64) -> TimeWindow {
        let start = (ts / window_size) * window_size;
        let end = start + window_size;
        TimeWindow::new(start, end)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use caesium_core::time::clock::MockClock;

    #[test]
    fn it_tracks_closed_time_windows() {
        let mut clock = MockClock::new(0);
        let mut tracker = WindowTracker::new(30, &clock);
        assert!(tracker.update(&clock).is_none());
        clock.tick(29);
        assert!(tracker.update(&clock).is_none());
        clock.tick(1);
        assert_eq!(tracker.update(&clock), Some(TimeWindow::new(0, 30)));
        clock.tick(1);
        assert!(tracker.update(&clock).is_none());
        clock.tick(29);
        assert_eq!(tracker.update(&clock), Some(TimeWindow::new(30, 60)));
    }

    #[test]
    fn it_aligns_time_windows() {
        let mut clock = MockClock::new(12); // not aligned to window size
        let mut tracker = WindowTracker::new(30, &clock);
        clock.tick(18);
        assert_eq!(tracker.update(&clock), Some(TimeWindow::new(0, 30)));
    }
}
