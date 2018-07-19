use encode::{Decodable, Encodable, EncodableError};
use rand::{weak_rng, Rng, XorShiftRng};
use std::io::{Read, Write};

#[derive(Clone)]
pub struct Sampler {
    weight: usize,
    max_weight: usize, // Output item when stored weight >= max_weight
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
        assert!(max_weight >= self.max_weight, "Cannot decrease max weight");
        self.max_weight = max_weight;
    }

    pub fn sample(&mut self, val: u64) -> Option<u64> {
        // Special case for small max_weight values to improve performance
        if self.max_weight == 1 {
            Some(val)
        } else {
            self.sample_weighted(val, 1)
        }
    }

    pub fn sample_weighted(&mut self, val: u64, weight: usize) -> Option<u64> {
        assert!(weight <= self.max_weight);
        assert!(weight > 0);
        let combined_weight = self.weight + weight;
        if combined_weight <= self.max_weight {
            self.reservoir_sample_no_overflow(val, weight, combined_weight)
        } else {
            self.reservoir_sample_with_overflow(val, weight)
        }
    }

    pub fn stored_value(&self) -> u64 {
        self.val
    }

    pub fn stored_weight(&self) -> usize {
        self.weight
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
        assert!(heavier_weight <= self.max_weight);
        let cutoff = (usize::max_value() / self.max_weight) * heavier_weight;
        let r = self.generator.next_u64() as usize;
        if r <= cutoff {
            Some(heavier_val)
        } else {
            None
        }
    }
}

impl<W> Encodable<W> for Sampler
where
    W: Write,
{
    fn encode(&self, writer: &mut W) -> Result<(), EncodableError> {
        self.weight.encode(writer)?;
        self.max_weight.encode(writer)?;
        self.val.encode(writer)?;
        Ok(())
    }
}

impl<R> Decodable<Sampler, R> for Sampler
where
    R: Read,
{
    fn decode(reader: &mut R) -> Result<Sampler, EncodableError> {
        let weight = usize::decode(reader)?;
        let max_weight = usize::decode(reader)?;
        let val = u64::decode(reader)?;
        let sampler = Sampler {
            weight,
            max_weight,
            val,
            generator: weak_rng(),
        };
        Ok(sampler)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn it_samples_all_with_max_weight_one() {
        let mut s = Sampler::new(); // group size default to one
        for v in 0..100 {
            assert_eq!(s.sample(v), Some(v));
        }
    }

    #[test]
    fn it_samples_randomly_with_larger_max_weight() {
        let mut s = Sampler::new();
        for w in 1..10 {
            s.set_max_weight(w);
            for v in 0..(w - 1) {
                assert_eq!(s.sample(v as u64), None);
            }

            match s.sample(w as u64) {
                None => panic!("Expected at least one sample"),
                Some(v) => assert!(v <= w as u64),
            }
        }
    }

    #[test]
    fn it_samples_weighted_items_no_overflow() {
        let mut s = Sampler::new();
        s.set_max_weight(8);
        for _ in 0..3 {
            println!("sample");
            assert_eq!(s.sample_weighted(1, 2), None);
        }
        println!("sample last");
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

    #[test]
    fn it_encodes_and_decodes() {
        let mut s = Sampler::new();
        s.set_max_weight(8);
        for v in 0..100 {
            s.sample(v as u64);
        }

        let mut buf = Vec::<u8>::new();
        s.encode(&mut buf).expect("Could not encode sampler");
        let decoded = Sampler::decode(&mut &buf[..]).expect("Could not decode sampler");
        assert_eq!(s.max_weight, decoded.max_weight);
        assert_eq!(s.stored_value(), decoded.stored_value());
        assert_eq!(s.stored_weight(), decoded.stored_weight());
    }
}
