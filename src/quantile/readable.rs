use quantile::block::Block;

#[derive(Copy, Clone)]
struct WeightedValue {
    level: u8,
    value: u64,
}

impl WeightedValue {
    fn new(level: u8, value: u64) -> WeightedValue {
        debug_assert!(level < 64);
        WeightedValue { level, value }
    }

    fn weight(&self) -> usize {
        1 << self.level
    }
}

pub struct ReadableSketch {
    data: Vec<WeightedValue>,
    count: usize,
}

impl ReadableSketch {
    pub fn new(count: usize, levels: Vec<Block>) -> ReadableSketch {
        assert!(levels.len() < 64); // maximum weight is 2^64
        ReadableSketch {
            count: count,
            data: levels
                .iter()
                .enumerate()
                .flat_map(|(level, block)| {
                    block
                        .iter_sorted_values()
                        .map(move |v| WeightedValue::new(level as u8, *v))
                })
                .collect(),
        }
    }

    pub fn size(&self) -> usize {
        self.data.len()
    }

    pub fn query(&mut self, phi: f64) -> Option<u64> {
        assert!(0.0 < phi && phi < 1.0);
        if self.count > 0 {
            let target_rank = (self.count as f64 * phi) as usize;
            let result = ReadableSketch::quick_select(target_rank, &mut self.data);
            Some(result)
        } else {
            None
        }
    }

    fn quick_select(k: usize, mut data: &mut [WeightedValue]) -> u64 {
        let (mut left, mut right) = (0, data.len());
        let mut left_weight = 0;

        // at each iteration, search the range [left, right)
        while left != right {
            let mut pivot_idx = left + (right - left) / 2;
            let left_to_pivot_weight =
                ReadableSketch::partition(&mut data, &mut pivot_idx, left, right);
            let weight_to_pivot = left_weight + left_to_pivot_weight;
            if weight_to_pivot <= k && k < weight_to_pivot + data[pivot_idx].weight() {
                return data[pivot_idx].value;
            } else if k < weight_to_pivot {
                right = pivot_idx;
            } else {
                let new_weight: usize = data[left..pivot_idx + 1].iter().map(|v| v.weight()).sum();
                left_weight += new_weight;
                left = pivot_idx + 1;
            }
        }
        data[left].value
    }

    fn partition(
        mut data: &mut [WeightedValue],
        pivot_idx: &mut usize,
        left: usize,
        right: usize,
    ) -> usize {
        let pivot_val = data[*pivot_idx].value;
        let mut weight = 0;
        ReadableSketch::swap(&mut data, *pivot_idx, right - 1);
        *pivot_idx = left;
        for j in left..right - 1 {
            if data[j].value < pivot_val {
                weight += data[j].weight();
                ReadableSketch::swap(data, *pivot_idx, j);
                *pivot_idx += 1;
            }
        }
        ReadableSketch::swap(&mut data, *pivot_idx, right - 1);
        weight
    }

    fn swap(data: &mut [WeightedValue], i: usize, j: usize) {
        let tmp = data[i];
        data[i] = data[j];
        data[j] = tmp;
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use rand;
    use rand::Rng;

    #[test]
    fn it_queries_empty() {
        let mut s = ReadableSketch::new(0, Vec::new());
        assert_eq!(s.query(0.5), None);
    }

    #[test]
    fn it_queries_sorted() {
        let data: Vec<u64> = (0..100).map(|v| v as u64).collect();
        let levels = vec![Block::from_sorted_values(&data)];
        assert_queries(levels);
    }

    #[test]
    fn it_queries_unsorted() {
        let mut data: Vec<u64> = (0..100).map(|v| v as u64).collect();
        let mut rng = rand::thread_rng();
        rng.shuffle(&mut data);
        let levels = vec![Block::from_unsorted_values(&data)];
        assert_queries(levels);
    }

    #[test]
    fn it_queries_duplicates() {
        let data: Vec<u64> = (0..100).map(|_| 1 as u64).collect();
        let levels = vec![Block::from_sorted_values(&data)];
        assert_queries(levels);
    }

    #[test]
    fn it_queries_weighted_small() {
        let levels = vec![
            Block::from_sorted_values(&vec![2, 4, 6, 7]), // weight = 1
            Block::from_sorted_values(&vec![1, 3, 5]),    // weight = 2
        ];
        assert_queries(levels);
    }

    #[test]
    fn it_queries_weighted_large() {
        let levels: Vec<Block> = (0..4)
            .map(|level| {
                let values: Vec<u64> = (0..64).map(|v| v * level as u64).collect();
                Block::from_sorted_values(&values)
            })
            .collect();
        assert_queries(levels);
    }

    fn assert_queries(levels: Vec<Block>) {
        let count = levels
            .iter()
            .enumerate()
            .map(|(level, values)| {
                let weight = 1 << level;
                values.len() * weight
            })
            .sum();
        let mut s = ReadableSketch::new(count, levels.to_vec());
        for p in 1..100 {
            let phi = p as f64 / 100.0;
            let expected = calculate_exact(&levels, phi);
            let actual = s.query(phi);
            println!("phi={}, expected={:?}, actual={:?}", phi, expected, actual);
            assert_eq!(actual, expected);
        }
    }

    fn calculate_exact(levels: &[Block], phi: f64) -> Option<u64> {
        let mut values: Vec<u64> = levels
            .iter()
            .enumerate()
            .flat_map(move |(level, block)| {
                let weight = 1 << level;
                block
                    .iter_sorted_values()
                    .flat_map(move |v| (0..weight).map(move |_| *v))
            })
            .collect();
        values.sort_unstable();
        if values.len() > 0 {
            let k = (values.len() as f64 * phi) as usize;
            Some(values[k])
        } else {
            None
        }
    }
}
