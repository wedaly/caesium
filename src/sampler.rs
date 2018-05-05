use rand::{Rng, XorShiftRng, weak_rng};

pub struct Sampler {
    count: usize,
    group_size: usize, // Sample one item out of every group
    reservoir: u64,
    generator: XorShiftRng
}

impl Sampler {
    pub fn new() -> Sampler {
        Sampler {
            count: 0,
            group_size: 1,
            reservoir: 0,
            generator: weak_rng(),
        }
    }

    pub fn set_group_size(&mut self, group_size: usize) {
        assert!(group_size > 0, "Group size must be positive");
        assert!(
            self.count == 0,
            "Cannot change group size if already storing value"
        );
        self.group_size = group_size;
    }

    pub fn sample(&mut self, val: u64) -> Option<u64> {
        self.count += 1;

        let cutoff = usize::max_value() / self.count;
        let r = self.generator.next_u64() as usize;
        if r <= cutoff {
            self.reservoir = val;
        }

        if self.count == self.group_size {
            self.count = 0;
            Some(self.reservoir)
        } else {
            None
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn it_samples_all_with_group_size_one() {
        let mut s = Sampler::new(); // group size default to one
        for v in 0..100 {
            assert_eq!(s.sample(v), Some(v));
        }
    }

    #[test]
    fn it_samples_randomly_with_larger_groups_size() {
        let mut s = Sampler::new();
        for g in 1..10 {
            s.set_group_size(g);
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
