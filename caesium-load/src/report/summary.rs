pub struct StatSummary<T> {
    sample_count: usize,
    median: Option<T>,
    ninety_fifth_percentile: Option<T>,
    min: Option<T>,
    max: Option<T>,
}

impl<T> StatSummary<T>
where
    T: Ord + Copy,
{
    pub fn new(mut samples: Vec<T>) -> StatSummary<T> {
        samples.sort_unstable();
        StatSummary {
            sample_count: samples.len(),
            median: StatSummary::calculate_quantile(&samples, 0.5),
            ninety_fifth_percentile: StatSummary::calculate_quantile(&samples, 0.95),
            min: StatSummary::calculate_min(&samples),
            max: StatSummary::calculate_max(&samples),
        }
    }

    pub fn sample_count(&self) -> usize {
        self.sample_count
    }

    pub fn median(&self) -> Option<T> {
        self.median
    }

    pub fn ninety_fifth_percentile(&self) -> Option<T> {
        self.ninety_fifth_percentile
    }

    pub fn min(&self) -> Option<T> {
        self.min
    }

    pub fn max(&self) -> Option<T> {
        self.max
    }

    fn calculate_quantile(sorted_samples: &[T], phi: f64) -> Option<T> {
        assert!(phi > 0.0 && phi < 1.0);
        if sorted_samples.len() == 0 {
            None
        } else {
            let idx = (sorted_samples.len() as f64 * phi) as usize;
            Some(sorted_samples[idx])
        }
    }

    fn calculate_min(sorted_samples: &[T]) -> Option<T> {
        sorted_samples.iter().next().map(|s| *s)
    }

    fn calculate_max(sorted_samples: &[T]) -> Option<T> {
        sorted_samples.iter().last().map(|s| *s)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use rand;
    use rand::Rng;

    #[test]
    fn it_summarizes_empty_set() {
        let values = Vec::<u32>::new();
        let s = StatSummary::<u32>::new(values);
        assert_eq!(s.sample_count(), 0);
        assert_eq!(s.median(), None);
        assert_eq!(s.ninety_fifth_percentile(), None);
        assert_eq!(s.min(), None);
        assert_eq!(s.max(), None);
    }

    #[test]
    fn it_summarizes_single_value() {
        let values = vec![5];
        let s = StatSummary::<u32>::new(values);
        assert_eq!(s.sample_count(), 1);
        assert_eq!(s.median(), Some(5));
        assert_eq!(s.ninety_fifth_percentile(), Some(5));
        assert_eq!(s.min(), Some(5));
        assert_eq!(s.max(), Some(5));
    }

    #[test]
    fn it_summarizes_multiple_values() {
        let mut values = Vec::new();
        for i in 0..100 {
            values.push(i as u32);
        }
        rand::thread_rng().shuffle(&mut values);
        let s = StatSummary::<u32>::new(values);
        assert_eq!(s.sample_count(), 100);
        assert_eq!(s.median(), Some(50));
        assert_eq!(s.ninety_fifth_percentile(), Some(95));
        assert_eq!(s.min(), Some(0));
        assert_eq!(s.max(), Some(99));
    }
}
