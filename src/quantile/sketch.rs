use rand;
use std::cmp::{Ord, Ordering, PartialEq, PartialOrd};
use std::mem::size_of;

pub struct WritableSketch {
    buffers: Vec<Vec<u64>>,
    size: usize,
    max_size: usize,
    max_height: usize,
}

impl WritableSketch {
    pub fn new() -> WritableSketch {
        let mut sketch = WritableSketch {
            buffers: Vec::with_capacity(1),
            size: 0,
            max_size: 0,
            max_height: 0,
        };
        sketch.grow();
        sketch
    }

    pub fn reset(&mut self) {
        self.buffers.clear();
        self.size = 0;
        self.max_size = 0;
        self.max_height = 0;
        self.grow();
    }

    pub fn insert(&mut self, val: u64) {
        self.buffers[0].push(val);
        self.size += 1;
        if self.size >= self.max_size {
            self.compress();
            debug_assert!(self.size < self.max_size);
        }
    }

    pub fn merge(&mut self, other: &WritableSketch) {
        while self.max_height < other.max_height {
            self.grow();
        }
        for (b1, b2) in self.buffers.iter_mut().zip(other.buffers.iter()) {
            b1.extend_from_slice(b2);
        }
        self.size = self.calculate_size();
        while self.size >= self.max_size {
            self.compress();
        }
    }

    pub fn to_readable_sketch(&self) -> ReadableSketch {
        let mut result = ReadableSketch::new();
        for (h, b) in self.buffers.iter().enumerate() {
            let weight = 1 << h;
            result.extend(weight, &b);
        }
        result.seal();
        result
    }

    pub fn size_in_bytes(&self) -> usize {
        self.size * size_of::<u64>()
    }

    fn grow(&mut self) {
        self.buffers.push(Vec::new());
        self.max_height += 1;
        self.max_size = self.calculate_max_size();
    }

    fn compress(&mut self) {
        let h = self.find_buffer_to_compress();
        if h + 1 >= self.max_height {
            self.grow();
        }

        let mut tmp = Vec::new();
        {
            let mut src = self.buffers
                .get_mut(h)
                .expect("Could not retrieve src buffer");
            WritableSketch::compact(&mut src, &mut tmp);
        }
        {
            let dst = self.buffers
                .get_mut(h + 1)
                .expect("Could not retrieve dst buffer");
            dst.extend_from_slice(&tmp);
        }

        self.size = self.calculate_size();
    }

    fn find_buffer_to_compress(&self) -> usize {
        for (h, b) in self.buffers.iter().enumerate() {
            if b.len() >= self.capacity_at_height(h) {
                return h;
            }
        }
        return 0;
    }

    fn calculate_max_size(&self) -> usize {
        let mut result = 0;
        for h in 0..self.max_height {
            result += self.capacity_at_height(h);
        }
        result
    }

    fn calculate_size(&self) -> usize {
        self.buffers.iter().map(|b| b.len()).sum()
    }

    fn capacity_at_height(&self, h: usize) -> usize {
        4096 // TODO: make this dynamic
    }

    fn compact(src: &mut Vec<u64>, dst: &mut Vec<u64>) {
        let mut r = rand::random::<bool>();
        for val in src.iter() {
            if r {
                dst.push(*val);
            }
            r = !r;
        }
        src.clear();
    }
}

#[derive(Copy, Clone, Eq)]
struct RankedValue {
    value: u64,
    rank: usize,
}

impl Ord for RankedValue {
    fn cmp(&self, other: &RankedValue) -> Ordering {
        self.value.cmp(&other.value)
    }
}

impl PartialOrd for RankedValue {
    fn partial_cmp(&self, other: &RankedValue) -> Option<Ordering> {
        Some(self.cmp(other))
    }
}

impl PartialEq for RankedValue {
    fn eq(&self, other: &RankedValue) -> bool {
        self.value == other.value
    }
}

pub struct ReadableSketch {
    data: Vec<RankedValue>,
    count: usize,
    sealed: bool,
}

impl ReadableSketch {
    fn new() -> ReadableSketch {
        ReadableSketch {
            data: Vec::new(),
            count: 0,
            sealed: false,
        }
    }

    pub fn query(&self, phi: f64) -> Option<u64> {
        assert!(self.sealed);
        assert!(0.0 < phi && phi < 1.0);
        let target = phi * self.count as f64;
        let mut start = 0;
        let mut end = self.data.len();
        while end - start > 1 {
            let mid = start + (end - start) / 2;
            let rank = self.data[mid].rank as f64;
            if target < rank {
                end = mid;
            } else if target > rank {
                start = mid;
            } else {
                return Some(self.data[mid].value);
            }
        }
        if end - start == 1 {
            Some(self.data[start].value)
        } else {
            None
        }
    }

    fn extend(&mut self, weight: usize, values: &[u64]) {
        assert!(!self.sealed);
        self.count += weight * values.len();
        for v in values {
            self.data.push(RankedValue {
                value: *v,
                rank: weight, // tmp store the weight here
            });
        }
    }

    fn seal(&mut self) {
        self.data.sort_unstable();
        let mut rank = 0;
        for x in self.data.iter_mut() {
            let weight = x.rank; // stored weight from earlier
            x.rank = rank;
            rank += weight;
        }
        self.sealed = true;
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
        let s1 = build_writable_sketch(&input[..n / 2]);
        let mut s2 = build_writable_sketch(&input[n / 2..n]);
        s2.merge(&s1);
        let result = s2.to_readable_sketch();
        check_error_bound(&result, &input);
    }

    #[test]
    fn it_merges_many_sketches_without_increasing_error() {
        let sketch_size = MEDIUM_SIZE;
        let num_sketches = 100;
        let input = random_distinct_values(sketch_size * num_sketches);
        let mut s = build_writable_sketch(&input[..sketch_size]);
        for i in 1..num_sketches {
            let start = i * sketch_size;
            let end = start + sketch_size;
            let new_sketch = build_writable_sketch(&input[start..end]);
            s.merge(&new_sketch);
        }
        let result = s.to_readable_sketch();
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
        let sketch = build_writable_sketch(input);
        sketch.to_readable_sketch()
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
            assert!(error <= EPSILON * 2.0);
        }
    }
}
