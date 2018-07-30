use std::cmp::max;
use time::timestamp::TimeStamp;
use time::window::TimeWindow;

#[derive(Debug, PartialEq, Clone)]
pub enum DownsampleAction {
    Ignore,
    Discard,

    // Downsampled window is guaranteed to have
    // new_start <= old_start and new_end >= old_end
    ExpandWindow(TimeWindow),
}

pub trait DownsampleStrategy {
    fn get_action(&self, window: TimeWindow) -> DownsampleAction;
}

pub mod strategies {
    use super::*;

    const NUM_PARTITIONS: usize = 5;

    const ALIGNED_WINDOW_SIZES: [u64; NUM_PARTITIONS] = [
        1,    // 1 sec
        10,   // 10 sec
        60,   // 1 min
        600,  // 10 min
        3600, // 1 hr
    ];

    const PARTITION_CUTOFFS: [TimeStamp; NUM_PARTITIONS] = [
        300,      // windows >= 1 second until 5 mins
        86400,    // windows >= 10 seconds until 24 hrs
        604800,   // windows >= 1 minutes until 7 days
        2419200,  // windows >= 10 minutes until 28 days
        31536000, // windows >= 1 hour until 365 days
    ];

    pub struct DefaultStrategy {
        now: TimeStamp,
    }

    impl DefaultStrategy {
        pub fn new(now: TimeStamp) -> DefaultStrategy {
            DefaultStrategy { now }
        }

        fn find_aligned_size(seconds_since: u64) -> Option<u64> {
            for p in 0..NUM_PARTITIONS {
                if seconds_since < PARTITION_CUTOFFS[p] {
                    return Some(ALIGNED_WINDOW_SIZES[p]);
                }
            }
            None
        }

        fn expand_window(window: TimeWindow, aligned_size: u64) -> TimeWindow {
            let new_start = (window.start() / aligned_size) * aligned_size;
            let new_end = max(new_start + aligned_size, window.end());
            TimeWindow::new(new_start, new_end)
        }
    }

    impl DownsampleStrategy for DefaultStrategy {
        fn get_action(&self, window: TimeWindow) -> DownsampleAction {
            match self.now.checked_sub(window.start()) {
                Some(seconds_since) => match DefaultStrategy::find_aligned_size(seconds_since) {
                    Some(aligned_size) => {
                        let new_window = DefaultStrategy::expand_window(window, aligned_size);
                        debug_assert!(new_window.start() <= window.start());
                        debug_assert!(new_window.end() >= window.end());
                        if new_window == window {
                            DownsampleAction::Ignore
                        } else {
                            DownsampleAction::ExpandWindow(new_window)
                        }
                    }
                    None => DownsampleAction::Discard,
                },
                None => DownsampleAction::Ignore,
            }
        }
    }

    #[cfg(test)]
    mod tests {
        use super::*;

        #[test]
        fn it_ignores_window_starts_in_future() {
            let s = DefaultStrategy::new(3600);
            let window = TimeWindow::new(3800, 4000);
            let action = s.get_action(window);
            assert_eq!(action, DownsampleAction::Ignore);
        }

        #[test]
        fn it_ignores_window_in_partition_already_aligned() {
            for p in 0..NUM_PARTITIONS {
                println!("Testing partition {}", p);
                let s = DefaultStrategy::new(PARTITION_CUTOFFS[p] - 1);
                let window = TimeWindow::new(0, ALIGNED_WINDOW_SIZES[p]);
                let action = s.get_action(window);
                assert_eq!(action, DownsampleAction::Ignore);
            }
        }

        #[test]
        fn it_expands_window_in_partition_not_aligned() {
            for p in 1..NUM_PARTITIONS {
                println!("Testing partition {}", p);
                let s = DefaultStrategy::new(PARTITION_CUTOFFS[p] - 1);
                let window = TimeWindow::new(1, ALIGNED_WINDOW_SIZES[p] - 1);
                let action = s.get_action(window);
                let expected_action =
                    DownsampleAction::ExpandWindow(TimeWindow::new(0, ALIGNED_WINDOW_SIZES[p]));
                assert_eq!(action, expected_action);
            }
        }

        #[test]
        fn it_discards_window_past_last_partition() {
            let last_cutoff = PARTITION_CUTOFFS[NUM_PARTITIONS - 1];
            let s = DefaultStrategy::new(last_cutoff);
            let window = TimeWindow::new(0, 10);
            let action = s.get_action(window);
            assert_eq!(action, DownsampleAction::Discard);
        }

        #[test]
        fn it_expands_window_with_end_past_aligned_window() {
            let p = 3;
            let s = DefaultStrategy::new(PARTITION_CUTOFFS[p] - 1);
            let window = TimeWindow::new(1, ALIGNED_WINDOW_SIZES[p] * 2);
            let action = s.get_action(window);
            let expected_action = DownsampleAction::ExpandWindow(TimeWindow::new(0, window.end()));
            assert_eq!(action, expected_action);
        }
    }
}
