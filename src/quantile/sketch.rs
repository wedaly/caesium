use quantile::sampler::Sampler;
use rand;
use std::cmp::{max, Ord, Ordering, PartialEq, PartialOrd};
use std::collections::HashMap;

const BUFSIZE: usize = 256;
const BUFCOUNT: usize = 8;

pub struct WritableSketch {
    sampler: Sampler,
    current_buffer: usize,
    count: usize,
    buffers: [[u64; BUFSIZE]; BUFCOUNT],
    lengths: [usize; BUFCOUNT],
    levels: [usize; BUFCOUNT],
    active_level: usize,
}

impl WritableSketch {
    pub fn new() -> WritableSketch {
        WritableSketch {
            sampler: Sampler::new(),
            current_buffer: 0,
            count: 0,
            buffers: [[0; BUFSIZE]; BUFCOUNT],
            lengths: [0; BUFCOUNT],
            levels: [0; BUFCOUNT],
            active_level: 0,
        }
    }

    pub fn reset(&mut self) {
        self.current_buffer = 0;
        self.count = 0;
        self.active_level = 0;
        self.sampler.reset();
        for i in 0..BUFCOUNT {
            self.lengths[i] = 0;
            self.levels[i] = 0;
        }
    }

    pub fn insert(&mut self, val: u64) {
        self.count += 1;
        if let Some(val) = self.sampler.sample(val) {
            self.update_active_level();
            let idx = self.choose_insert_buffer();
            let len = self.lengths[idx];
            debug_assert!(len < BUFSIZE);
            self.buffers[idx][len] = val;
            self.lengths[idx] += 1;
            self.current_buffer = idx;
        }
    }

    pub fn to_mergable(&self) -> MergableSketch {
        let mut levels: Vec<Vec<u64>> = Vec::with_capacity(BUFCOUNT);
        let max_level = self.levels.iter().max().unwrap_or(&0);
        for _ in 0..(max_level + 1) {
            levels.push(Vec::new());
        }

        for idx in 0..BUFCOUNT {
            let len = self.lengths[idx];
            if len > 0 {
                let level = self.levels[idx];
                levels[level].extend_from_slice(&self.buffers[idx][..len]);
            }
        }

        MergableSketch::new(self.count, levels)
    }

    pub fn to_readable(&self) -> ReadableSketch {
        let mut weighted_values = Vec::with_capacity(BUFSIZE * BUFCOUNT);
        for idx in 0..BUFCOUNT {
            let len = self.lengths[idx];
            if len > 0 {
                let weight = 1 << self.levels[idx];
                for &val in &self.buffers[idx][..len] {
                    weighted_values.push(WeightedValue {
                        value: val,
                        weight: weight,
                    });
                }
            }
        }
        ReadableSketch::new(self.count, weighted_values)
    }

    fn choose_insert_buffer(&mut self) -> usize {
        if self.lengths[self.current_buffer] < BUFSIZE {
            self.current_buffer
        } else if let Some(idx) = self.find_empty_buffer() {
            idx
        } else {
            self.merge_two_buffers()
        }
    }

    fn merge_two_buffers(&mut self) -> usize {
        if let Some((b1, b2)) = self.find_buffers_to_merge() {
            self.compact_and_return_empty(b1, b2)
        } else {
            panic!("Could not find two buffers to merge!");
        }
    }

    fn compact_and_return_empty(&mut self, b1: usize, b2: usize) -> usize {
        debug_assert!(self.lengths[b1] == BUFSIZE);
        debug_assert!(self.lengths[b2] == BUFSIZE);

        let mut tmp = [0; BUFSIZE * 2];
        tmp[..BUFSIZE].copy_from_slice(&self.buffers[b1][..]);
        tmp[BUFSIZE..BUFSIZE * 2].copy_from_slice(&self.buffers[b2][..]);
        tmp.sort_unstable();

        // Write surviving values to b2
        let mut sel = rand::random::<bool>();
        let mut idx = 0;
        for &val in tmp.iter() {
            if sel {
                self.buffers[b2][idx] = val;
                idx += 1;
            }
            sel = !sel;
        }
        self.levels[b2] += 1;

        // Empty and return b1
        self.lengths[b1] = 0;
        self.levels[b1] = self.active_level;
        b1
    }

    fn find_empty_buffer(&self) -> Option<usize> {
        self.lengths.iter().position(|&len| len == 0)
    }

    fn find_buffers_to_merge(&self) -> Option<(usize, usize)> {
        debug_assert!(self.lengths.iter().all(|&len| len == BUFSIZE));
        let mut level_map = HashMap::with_capacity(BUFCOUNT);
        let mut best_match = None;
        for (b1, level) in self.levels.iter().enumerate() {
            if let Some(b2) = level_map.insert(level, b1) {
                best_match = match best_match {
                    None => Some((level, b1, b2)),
                    Some((old_level, _, _)) if level < old_level => Some((level, b1, b2)),
                    Some(current_best) => Some(current_best),
                }
            }
        }
        best_match.map(|(_, b1, b2)| (b1, b2))
    }

    fn update_active_level(&mut self) {
        let numerator = self.count as f64;
        let denominator = (BUFSIZE * (1 << (BUFCOUNT - 2))) as f64;
        let result = (numerator / denominator).log2().ceil() as i64;
        self.active_level = max(0, result) as usize;
        self.sampler.set_max_weight(1 << self.active_level);
    }
}

pub struct MergableSketch {
    count: usize, // from original datastream
    size: usize,  // count of stored values
    capacity: usize,
    levels: Vec<Vec<u64>>,
}

const LEVEL_CAPACITY: usize = BUFSIZE * BUFCOUNT;

impl MergableSketch {
    fn new(count: usize, levels: Vec<Vec<u64>>) -> MergableSketch {
        let size = MergableSketch::calculate_size(&levels);
        let capacity = MergableSketch::calculate_capacity(&levels);
        MergableSketch {
            count: count,
            size: size,
            capacity: capacity,
            levels: levels,
        }
    }

    pub fn empty() -> MergableSketch {
        MergableSketch {
            count: 0,
            size: 0,
            capacity: 0,
            levels: Vec::new(),
        }
    }

    pub fn merge(&mut self, other: &MergableSketch) {
        self.insert_from_other(other);
        while self.size > self.capacity {
            self.compress();
        }
    }

    pub fn to_readable(&mut self) -> ReadableSketch {
        debug_assert!(self.levels.len() - 1 < 64);
        let mut weighted_vals = Vec::with_capacity(self.size);
        self.levels.iter().enumerate().for_each(|(level, values)| {
            let weight = 1usize << level;
            for &val in values {
                weighted_vals.push(WeightedValue {
                    weight: weight,
                    value: val,
                });
            }
        });
        ReadableSketch::new(self.count, weighted_vals)
    }

    fn insert_from_other(&mut self, other: &MergableSketch) {
        self.count += other.count;

        // Add levels if necessary
        if other.levels.len() > self.levels.len() {
            let levels_to_grow = other.levels.len() - self.levels.len();
            for _ in 0..levels_to_grow {
                self.levels.push(Vec::new());
            }
        }
        debug_assert!(self.levels.len() >= other.levels.len());

        // Concat other's data into self
        for (mut dst, src) in self.levels.iter_mut().zip(other.levels.iter()) {
            dst.extend_from_slice(&src);
        }

        // Size and capacity may have change, since we added levels and inserted vals
        self.update_size_and_capacity();
    }

    fn compress(&mut self) {
        debug_assert!(self.levels.len() > 0);
        debug_assert!(self.size > self.capacity);

        let mut tmp = Vec::new();
        for mut values in self.levels.iter_mut() {
            if tmp.len() > 0 {
                values.extend_from_slice(&tmp);
                tmp.clear();
                break;
            }

            if values.len() > LEVEL_CAPACITY {
                MergableSketch::compact(&mut values, &mut tmp);
            }
        }

        if tmp.len() > 0 {
            self.levels.push(tmp);
        }

        self.update_size_and_capacity();
    }

    fn compact(src: &mut Vec<u64>, dst: &mut Vec<u64>) {
        let mut sel = rand::random::<bool>();
        for v in src.iter() {
            if sel {
                dst.push(*v);
            }
            sel = !sel;
        }
        src.clear();
    }

    fn update_size_and_capacity(&mut self) {
        self.capacity = MergableSketch::calculate_capacity(&self.levels);
        self.size = MergableSketch::calculate_size(&self.levels);
    }

    fn calculate_size(levels: &Vec<Vec<u64>>) -> usize {
        levels.iter().map(|values| values.len()).sum()
    }

    fn calculate_capacity(levels: &Vec<Vec<u64>>) -> usize {
        levels.len() * LEVEL_CAPACITY
    }
}

#[derive(Copy, Clone, Eq)]
struct WeightedValue {
    value: u64,
    weight: usize,
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
    fn new(count: usize, mut weighted_values: Vec<WeightedValue>) -> ReadableSketch {
        weighted_values.sort_unstable();
        let ranked_values = weighted_values
            .iter()
            .scan(0, |rank, &x| {
                let ranked_val = (*rank, x.value);
                *rank += x.weight;
                Some(ranked_val)
            })
            .collect();

        ReadableSketch {
            count: count,
            data: ranked_values,
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
}

#[cfg(test)]
mod tests {
    use super::*;
    use quantile::error::ErrorCalculator;
    use rand;
    use rand::Rng;

    const EPSILON: f64 = 0.01;
    const SMALL_SIZE: usize = 256 * 8;
    const MEDIUM_SIZE: usize = SMALL_SIZE * 10;
    const LARGE_SIZE: usize = SMALL_SIZE * 100;

    #[test]
    fn it_handles_query_with_no_values() {
        let input = Vec::new();
        let s = build_readable_sketch(&input);
        if let Some(_) = s.query(0.1) {
            panic!("expected no result!");
        }
    }

    #[test]
    fn it_handles_small_distinct_ordered_input() {
        let input = sequential_values(SMALL_SIZE);
        let s = build_readable_sketch(&input);
        check_error_bound(&s, &input);
    }

    #[test]
    fn it_handles_small_distinct_unordered_input() {
        let input = random_distinct_values(SMALL_SIZE);
        let s = build_readable_sketch(&input);
        check_error_bound(&s, &input);
    }

    #[test]
    fn it_handles_small_input_with_duplicates() {
        let input = random_duplicate_values(SMALL_SIZE);
        let s = build_readable_sketch(&input);
        check_error_bound(&s, &input);
    }

    #[test]
    fn it_handles_large_distinct_ordered_input() {
        let input = sequential_values(LARGE_SIZE);
        let s = build_readable_sketch(&input);
        check_error_bound(&s, &input);
    }

    #[test]
    fn it_handles_large_distinct_unordered_input() {
        let input = random_distinct_values(LARGE_SIZE);
        let s = build_readable_sketch(&input);
        check_error_bound(&s, &input);
    }

    #[test]
    fn it_handles_large_input_with_duplicates() {
        let input = random_duplicate_values(LARGE_SIZE);
        let s = build_readable_sketch(&input);
        check_error_bound(&s, &input);
    }

    #[test]
    fn it_merges_two_sketches_without_increasing_error() {
        let n = MEDIUM_SIZE * 2;
        let input = random_distinct_values(n);
        let s1 = build_mergable_sketch(&input[..n / 2]);
        let mut s2 = build_mergable_sketch(&input[n / 2..n]);
        s2.merge(&s1);
        let result = s2.to_readable();
        check_error_bound(&result, &input);
    }

    #[test]
    fn it_merges_many_sketches_without_increasing_error() {
        let sketch_size = MEDIUM_SIZE;
        let num_sketches = 30;
        let input = random_distinct_values(sketch_size * num_sketches);
        let mut s = build_mergable_sketch(&input[..sketch_size]);
        for i in 1..num_sketches {
            let start = i * sketch_size;
            let end = start + sketch_size;
            let new_sketch = build_mergable_sketch(&input[start..end]);
            s.merge(&new_sketch);
        }
        let result = s.to_readable();
        check_error_bound(&result, &input);
    }

    fn sequential_values(n: usize) -> Vec<u64> {
        let mut result: Vec<u64> = Vec::with_capacity(n);
        for v in 0..n {
            result.push(v as u64);
        }
        result
    }

    fn random_distinct_values(n: usize) -> Vec<u64> {
        let mut result: Vec<u64> = Vec::with_capacity(n);
        for v in 0..n {
            result.push(v as u64);
        }

        let mut rng = rand::thread_rng();
        rng.shuffle(&mut result);
        result
    }

    fn random_duplicate_values(n: usize) -> Vec<u64> {
        let mut result: Vec<u64> = Vec::with_capacity(n);
        for v in 0..n / 2 {
            result.push(v as u64);
            result.push(v as u64);
        }

        let mut rng = rand::thread_rng();
        rng.shuffle(&mut result);
        result
    }

    fn build_readable_sketch(input: &[u64]) -> ReadableSketch {
        build_writable_sketch(input).to_readable()
    }

    fn build_mergable_sketch(input: &[u64]) -> MergableSketch {
        build_writable_sketch(input).to_mergable()
    }

    fn build_writable_sketch(input: &[u64]) -> WritableSketch {
        let mut sketch = WritableSketch::new();
        for v in input.iter() {
            sketch.insert(*v);
        }
        sketch
    }

    fn check_error_bound(sketch: &ReadableSketch, input: &[u64]) {
        let calc = ErrorCalculator::new(&input);
        for i in 1..10 {
            let phi = i as f64 / 10.0;
            let approx = sketch.query(phi).expect("no result from query");
            let error = calc.calculate_error(phi, approx);
            println!("phi={}, approx={}, error={}", phi, approx, error);
            assert!(error <= EPSILON);
        }
    }
}
