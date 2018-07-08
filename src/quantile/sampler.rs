use rand::{weak_rng, Rng, XorShiftRng};

pub struct Sampler {
    count: usize,
    sample_idx: usize,
    val: u64,
    max_weight: usize,
    generator: XorShiftRng,
}

impl Sampler {
    pub fn new() -> Sampler {
        Sampler {
            count: 0,
            sample_idx: 0,
            val: 0,
            max_weight: 1,
            generator: weak_rng(),
        }
    }

    pub fn set_max_weight(&mut self, max_weight: usize) {
        debug_assert!(max_weight > 0, "Max weight must be positive");
        self.max_weight = max_weight;
    }

    pub fn sample(&mut self, val: u64) -> Option<u64> {
        if self.sample_idx == self.count {
            self.val = val;
        }

        self.count += 1;
        if self.count == self.max_weight {
            self.count = 0;
            self.sample_idx = self.generator.gen_range(0, self.max_weight);
            Some(self.val)
        } else {
            None
        }
    }

    pub fn stored_val(&mut self) -> Option<u64> {
        if self.count >= self.sample_idx {
            Some(self.val)
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
}
