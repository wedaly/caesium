use std::cmp;
use std::collections::HashMap;

pub struct ErrorCalculator {
    count: usize,
    value_range_map: HashMap<u64, (usize, usize)>,
}

impl ErrorCalculator {
    pub fn new(data: &[u64]) -> ErrorCalculator {
        let value_range_map = ErrorCalculator::create_value_range_map(&data);
        ErrorCalculator {
            count: data.len(),
            value_range_map: value_range_map,
        }
    }

    pub fn calculate_error(&self, phi: f64, approx: u64) -> f64 {
        assert!(phi > 0.0 && phi < 1.0);
        let exact_rank = (self.count as f64 * phi) as usize;
        let &(min_rank, max_rank) = self
            .value_range_map
            .get(&approx)
            .expect("Could not find query result in original dataset");

        if exact_rank <= min_rank {
            self.normalized_rank_error(exact_rank, min_rank)
        } else if exact_rank >= max_rank {
            self.normalized_rank_error(exact_rank, max_rank)
        } else {
            0.0
        }
    }

    fn normalized_rank_error(&self, r1: usize, r2: usize) -> f64 {
        (r1 as i32 - r2 as i32).abs() as f64 / self.count as f64
    }

    fn create_value_range_map(data: &[u64]) -> HashMap<u64, (usize, usize)> {
        let mut sorted = Vec::with_capacity(data.len());
        sorted.extend_from_slice(data);
        sorted.sort_unstable();
        let mut map = HashMap::with_capacity(sorted.len());
        for (idx, v) in sorted.iter().enumerate() {
            map.entry(*v)
                .and_modify(|(old_min, old_max)| {
                    *old_min = cmp::min(*old_min, idx);
                    *old_max = cmp::max(*old_max, idx);
                })
                .or_insert((idx, idx));
        }
        map
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use rand;
    use rand::Rng;

    #[test]
    fn it_calculates_zero_error_for_exact_result() {
        let mut data: Vec<u64> = (0..10).collect();
        shuffle(&mut data);
        let calc = ErrorCalculator::new(&data);
        for i in 1..10 {
            let phi = i as f64 / 10.0;
            assert_eq!(calc.calculate_error(phi, i), 0.0);
        }
    }

    #[test]
    fn it_calculates_rank_error_distinct_values() {
        let mut data: Vec<u64> = (0..10).collect();
        shuffle(&mut data);
        let calc = ErrorCalculator::new(&data);
        assert_eq!(calc.calculate_error(0.5, 0), 0.5);
        assert_eq!(calc.calculate_error(0.5, 1), 0.4);
        assert_eq!(calc.calculate_error(0.5, 2), 0.3);
        assert_eq!(calc.calculate_error(0.5, 3), 0.2);
        assert_eq!(calc.calculate_error(0.5, 4), 0.1);
        assert_eq!(calc.calculate_error(0.5, 5), 0.0);
        assert_eq!(calc.calculate_error(0.5, 6), 0.1);
        assert_eq!(calc.calculate_error(0.5, 7), 0.2);
        assert_eq!(calc.calculate_error(0.5, 8), 0.3);
        assert_eq!(calc.calculate_error(0.5, 9), 0.4);
    }

    #[test]
    fn it_calculates_rank_error_duplicate_values() {
        let mut data = vec![0, 0, 1, 1, 2, 2, 3, 3, 4, 4, 5, 5, 6, 6, 7, 7, 8, 8, 9, 9];
        shuffle(&mut data);
        let calc = ErrorCalculator::new(&data);
        assert_eq!(calc.calculate_error(0.5, 1), 0.35);
        assert_eq!(calc.calculate_error(0.5, 2), 0.25);
        assert_eq!(calc.calculate_error(0.5, 3), 0.15);
        assert_eq!(calc.calculate_error(0.5, 4), 0.05);
        assert_eq!(calc.calculate_error(0.5, 5), 0.0);
        assert_eq!(calc.calculate_error(0.5, 6), 0.1);
        assert_eq!(calc.calculate_error(0.5, 7), 0.2);
        assert_eq!(calc.calculate_error(0.5, 8), 0.3);
        assert_eq!(calc.calculate_error(0.5, 9), 0.4);
    }

    fn shuffle(mut data: &mut [u64]) {
        let mut rng = rand::thread_rng();
        rng.shuffle(&mut data);
    }
}
