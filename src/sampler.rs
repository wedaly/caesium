use rand::{weak_rng, Rng, XorShiftRng};

pub struct Sampler {
    weight: usize,
    max_weight: usize,
    val: u64,
    generator: XorShiftRng,
}

impl Sampler {
    pub fn new() -> Sampler {
        Sampler {
            weight: 0,
            max_weight: 1,
            val: 0,
            generator: weak_rng(),
        }
    }

    pub fn set_max_weight(&mut self, max_weight: usize) {
        debug_assert!(max_weight > 0, "Max weight must be positive");
        debug_assert!(
            self.weight == 0,
            "Cannot max weight if already storing value"
        );
        self.max_weight = max_weight;
    }

    pub fn sample(&mut self, val: u64) -> Option<u64> {
        self.sample_weighted(val, 1)
    }

    pub fn sample_weighted(&mut self, val: u64, weight: usize) -> Option<u64> {
        debug_assert!(
            weight <= self.max_weight,
            "Item weight must be <= max weight"
        );
        let combined_weight = self.weight + weight;
        if combined_weight <= self.max_weight {
            self.reservoir_sample_no_overflow(val, weight, combined_weight)
        } else {
            self.reservoir_sample_with_overflow(val, weight)
        }
    }

    fn reservoir_sample_no_overflow(
        &mut self,
        val: u64,
        weight: usize,
        combined_weight: usize,
    ) -> Option<u64> {
        // Replace stored item with probability = weight / combined_weight
        let cutoff = usize::max_value() / combined_weight * weight;
        let r = self.generator.next_u64() as usize;
        if r <= cutoff {
            self.val = val;
        }
        if combined_weight == self.max_weight {
            self.weight = 0;
            Some(self.val)
        } else {
            self.weight = combined_weight;
            None
        }
    }

    fn reservoir_sample_with_overflow(&mut self, val: u64, weight: usize) -> Option<u64> {
        let (lighter_val, lighter_weight, heavier_val, heavier_weight) = if self.weight < weight {
            (self.val, self.weight, val, weight)
        } else {
            (val, weight, self.val, self.weight)
        };

        self.weight = lighter_weight;
        self.val = lighter_val;

        // output with probability = (heavier_weight / self.max_weight)
        debug_assert!(heavier_weight <= self.max_weight);
        let cutoff = (usize::max_value() / self.max_weight) * heavier_weight;
        let r = self.generator.next_u64() as usize;
        if r <= cutoff {
            Some(heavier_val)
        } else {
            None
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn it_samples_all_with_max_weight_one() {
        let mut s = Sampler::new(); // max weight defaults to one
        for v in 0..100 {
            assert_eq!(s.sample(v), Some(v));
        }
    }

    #[test]
    fn it_samples_randomly_with_larger_max_weight() {
        let mut s = Sampler::new();
        for g in 1..10 {
            s.set_max_weight(g);
            for v in 0..(g - 1) {
                assert_eq!(s.sample(v as u64), None);
            }

            match s.sample(g as u64) {
                None => panic!("Expected at least one sample"),
                Some(v) => assert!(v <= g as u64),
            }
        }
    }

    #[test]
    fn it_samples_weighted_items_no_overflow() {
        let mut s = Sampler::new();
        s.set_max_weight(8);
        for _ in 0..3 {
            assert_eq!(s.sample_weighted(1, 2), None);
        }
        assert_eq!(s.sample_weighted(1, 2), Some(1));
    }

    #[test]
    fn it_samples_weighted_items_with_overflow() {
        let mut s = Sampler::new();
        s.set_max_weight(8);
        assert_eq!(s.sample_weighted(1, 7), None);
        if let Some(n) = s.sample_weighted(2, 2) {
            assert_eq!(n, 1); // if anything output, should be heavier item
        }
    }
}
