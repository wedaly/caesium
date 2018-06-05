use quantile::block::Block;
use std::cmp::{Ord, Ordering, PartialEq, PartialOrd};

#[derive(Copy, Clone, Eq)]
pub struct WeightedValue {
    pub value: u64,
    pub weight: usize,
}

impl Ord for WeightedValue {
    fn cmp(&self, other: &WeightedValue) -> Ordering {
        self.value.cmp(&other.value)
    }
}

impl PartialOrd for WeightedValue {
    fn partial_cmp(&self, other: &WeightedValue) -> Option<Ordering> {
        Some(self.cmp(other))
    }
}

impl PartialEq for WeightedValue {
    fn eq(&self, other: &WeightedValue) -> bool {
        self.value == other.value
    }
}

pub struct ReadableSketch {
    data: Vec<(usize, u64)>,
    count: usize,
}

impl ReadableSketch {
    pub fn new(count: usize, levels: &Vec<Block>) -> ReadableSketch {
        let mut weighted_vals = ReadableSketch::calculate_weighted_values(levels);
        let ranked_vals = ReadableSketch::calculate_ranked_vals(&mut weighted_vals);
        ReadableSketch {
            count: count,
            data: ranked_vals,
        }
    }

    pub fn size(&self) -> usize {
        self.data.len()
    }

    pub fn query(&self, phi: f64) -> Option<u64> {
        assert!(0.0 < phi && phi < 1.0);
        let target = phi * self.count as f64;
        let mut start = 0;
        let mut end = self.data.len();
        while end - start > 1 {
            let mid = start + (end - start) / 2;
            let (mid_rank, mid_value) = self.data[mid];
            let rank = mid_rank as f64;
            if target < rank {
                end = mid;
            } else if target > rank {
                start = mid;
            } else {
                return Some(mid_value);
            }
        }
        if end - start == 1 {
            let (_, start_value) = self.data[start];
            Some(start_value)
        } else {
            None
        }
    }

    fn calculate_weighted_values(levels: &Vec<Block>) -> Vec<WeightedValue> {
        let mut result = Vec::new();
        for (level, block) in levels.iter().enumerate() {
            let weight = 1 << level;
            for &value in block.iter_sorted_values() {
                result.push(WeightedValue {
                    weight: weight,
                    value: value,
                });
            }
        }
        result
    }

    fn calculate_ranked_vals(weighted_vals: &mut Vec<WeightedValue>) -> Vec<(usize, u64)> {
        weighted_vals.sort_unstable();
        weighted_vals
            .iter()
            .scan(0, |rank, &x| {
                let ranked_val = (*rank, x.value);
                *rank += x.weight;
                Some(ranked_val)
            })
            .collect()
    }
}
