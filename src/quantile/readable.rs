use std::cmp::min;

#[derive(Copy, Clone, Debug)]
pub struct WeightedValue {
    weight: usize,
    value: u64,
}

impl WeightedValue {
    pub fn new(weight: usize, value: u64) -> WeightedValue {
        debug_assert!(weight > 0);
        WeightedValue { weight, value }
    }
}

pub struct ReadableSketch {
    data: Vec<WeightedValue>,
    count: usize,
}

impl ReadableSketch {
    pub fn new(count: usize, data: Vec<WeightedValue>) -> ReadableSketch {
        ReadableSketch { count, data }
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
            if weight_to_pivot <= k && k < weight_to_pivot + data[pivot_idx].weight {
                return data[pivot_idx].value;
            } else if k < weight_to_pivot {
                right = pivot_idx;
            } else {
                let new_weight: usize = data[left..pivot_idx + 1].iter().map(|v| v.weight).sum();
                left_weight += new_weight;
                left = pivot_idx + 1;
            }
        }

        // We might "find" an element past the end of the array
        // if `count` is greater than the sum of all weights.
        // When this happens, simply return the last item instead.
        let idx = min(left, data.len() - 1);
        data[idx].value
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
                weight += data[j].weight;
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
        let mut s = ReadableSketch::new(0, vec![]);
        assert_eq!(s.query(0.5), None);
    }

    #[test]
    fn it_queries_sorted() {
        let data: Vec<WeightedValue> = (0..100).map(|v| WeightedValue::new(1, v as u64)).collect();
        assert_queries(data);
    }

    #[test]
    fn it_queries_unsorted() {
        let mut data: Vec<WeightedValue> =
            (0..100).map(|v| WeightedValue::new(1, v as u64)).collect();
        let mut rng = rand::thread_rng();
        rng.shuffle(&mut data);
        assert_queries(data);
    }

    #[test]
    fn it_queries_duplicates() {
        let data: Vec<WeightedValue> = (0..100).map(|_| WeightedValue::new(1, 1)).collect();
        assert_queries(data);
    }

    #[test]
    fn it_queries_weighted_small() {
        let data = vec![
            WeightedValue::new(1, 2),
            WeightedValue::new(1, 4),
            WeightedValue::new(1, 6),
            WeightedValue::new(1, 7),
            WeightedValue::new(2, 1),
            WeightedValue::new(2, 3),
            WeightedValue::new(2, 5),
        ];
        assert_queries(data);
    }

    #[test]
    fn it_queries_weighted_large() {
        let mut data = Vec::new();
        for level in 0..4 {
            for value in 0..64 {
                data.push(WeightedValue::new(1 << level, value as u64));
            }
        }
        assert_queries(data);
    }

    #[test]
    fn it_queries_target_idx_gt_weight_sum() {
        let data = vec![
            WeightedValue::new(1, 1),
            WeightedValue::new(1, 2),
            WeightedValue::new(1, 3),
            WeightedValue::new(1, 4),
            WeightedValue::new(1, 5),
            WeightedValue::new(1, 6),
            WeightedValue::new(1, 8),
            WeightedValue::new(1, 9),
            WeightedValue::new(1, 10),
        ];
        let count = 50;  // greater than total weight
        let mut s = ReadableSketch::new(count, data);
        let result = s.query(0.9999999).expect("Could not query sketch");
        assert_eq!(result, 10);
    }

    fn assert_queries(data: Vec<WeightedValue>) {
        let count = data.iter().map(|v| v.weight).sum();
        let mut s = ReadableSketch::new(count, data.clone());
        for p in 1..100 {
            let phi = p as f64 / 100.0;
            let expected = calculate_exact(&data, phi);
            let actual = s.query(phi);
            println!("phi={}, expected={:?}, actual={:?}", phi, expected, actual);
            assert_eq!(actual, expected);
        }
    }

    fn calculate_exact(data: &[WeightedValue], phi: f64) -> Option<u64> {
        let mut values = Vec::new();
        for v in data {
            for _ in 0..v.weight {
                values.push(v.value);
            }
        }
        values.sort_unstable();
        if values.len() > 0 {
            let k = (values.len() as f64 * phi) as usize;
            Some(values[k])
        } else {
            None
        }
    }
}
