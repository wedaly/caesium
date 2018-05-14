use rand;
use sampler::Sampler;
use std::cmp;

pub const EPSILON: f64 = 0.01;
pub const BUFCOUNT: usize = 8; // log(1/epsilon) + 1
pub const BUFSIZE: usize = 256; // (1/epsilon) * sqrt(log(1/epsilon))

#[derive(Copy, Clone)]
enum BufState {
    Empty,
    Filling { level: usize, len: usize },
    Full { level: usize },
}

pub struct QuantileSketch {
    sampler: Sampler,
    buffers: [[u64; BUFSIZE]; BUFCOUNT],
    bufstate: [BufState; BUFCOUNT],
    active_level: usize,
    current_buffer: usize,
    count: usize,
}

impl QuantileSketch {
    pub fn new() -> QuantileSketch {
        QuantileSketch {
            sampler: Sampler::new(),
            buffers: [[0; BUFSIZE]; BUFCOUNT],
            bufstate: [BufState::Empty; BUFCOUNT],
            active_level: 0,
            current_buffer: 0,
            count: 0,
        }
    }

    pub fn insert(&mut self, val: u64) {
        self.count += 1;
        if let Some(val) = self.sampler.sample(val) {
            let idx = self.current_buffer;
            let bufstate = self.bufstate[idx];
            self.update_active_level();
            match bufstate {
                BufState::Empty => self.insert_empty_buffer(idx, val),
                BufState::Filling { level, len } => {
                    self.insert_filling_buffer(idx, val, level, len)
                }
                BufState::Full { level: _ } => self.insert_full_buffer(val),
            }
        }
    }

    pub fn query(&self, phi: f64) -> Option<u64> {
        assert!(0.0 < phi && phi < 1.0);
        let error_bound = EPSILON * self.count as f64;
        let mut closest: Option<(f64, u64)> = None;
        let mut rank = 0;

        let reverse = phi > 0.5;
        let target_phi = if reverse { (1.0 - phi) } else { phi };
        let target = target_phi * self.count as f64;

        for (val, weight) in self.sorted_values_and_weights(reverse) {
            rank += weight;
            let error = (rank as f64 - target).abs();
            if error < error_bound {
                return Some(val);
            }

            if let Some((old_error, _)) = closest {
                if error < old_error {
                    closest = Some((error, val));
                }
            }
        }
        closest.map(|(_, val)| val)
    }

    fn sorted_values_and_weights(&self, reverse: bool) -> Vec<(u64, usize)> {
        let mut result = Vec::with_capacity(BUFSIZE * BUFCOUNT);
        for (values, state) in self.buffers.iter().zip(self.bufstate.iter()) {
            match *state {
                BufState::Empty => {}
                BufState::Filling { level, len } => {
                    let weight = 1 << level;
                    let item_iter = values.iter().take(len).map(|v| (*v, weight));
                    result.extend(item_iter);
                }
                BufState::Full { level } => {
                    let weight = 1 << level;
                    let item_iter = values.iter().map(|v| (*v, weight));
                    result.extend(item_iter);
                }
            }
        }

        result.sort_unstable_by(|(v1, _), (v2, _)| {
            let ordering = v1.cmp(v2);
            if reverse {
                ordering.reverse()
            } else {
                ordering
            }
        });
        result
    }

    fn update_active_level(&mut self) {
        let numerator = self.count as f64;
        let denominator = (BUFSIZE * (1 << (BUFCOUNT - 2))) as f64;
        let result = (numerator / denominator).log2().ceil() as i64;
        self.active_level = cmp::max(0, result) as usize;
        self.sampler.set_max_weight(1 << self.active_level);
    }

    fn insert_empty_buffer(&mut self, idx: usize, val: u64) {
        self.buffers[idx][0] = val;
        self.bufstate[idx] = BufState::Filling {
            level: self.active_level,
            len: 1,
        };
    }

    fn insert_filling_buffer(&mut self, idx: usize, val: u64, level: usize, len: usize) {
        self.buffers[idx][len] = val;
        if len + 1 < BUFSIZE {
            self.bufstate[idx] = BufState::Filling {
                level: level,
                len: len + 1,
            };
        } else {
            self.bufstate[idx] = BufState::Full { level: level };
        }
    }

    fn insert_full_buffer(&mut self, val: u64) {
        let idx = self.find_or_create_empty_buffer();
        self.current_buffer = idx;
        self.insert_empty_buffer(idx, val);
    }

    fn find_or_create_empty_buffer(&mut self) -> usize {
        match self.find_empty_buffer() {
            Some(idx) => idx,
            None => {
                if let Some((b1, b2)) = self.find_full_buffers_lowest_levels() {
                    self.merge_and_return_empty_buffer(b1, b2)
                } else {
                    panic!("Could not find two full buffers with same level to merge");
                }
            }
        }
    }

    fn find_empty_buffer(&self) -> Option<usize> {
        self.bufstate.iter().position(|&state| {
            if let BufState::Empty = state {
                true
            } else {
                false
            }
        })
    }

    fn find_full_buffers_lowest_levels(&self) -> Option<(usize, usize)> {
        // vec of (idx, level) tuples for full buffers
        let mut candidates = self.bufstate
            .iter()
            .filter_map(|state| {
                if let BufState::Full { level } = *state {
                    Some(level)
                } else {
                    None
                }
            })
            .enumerate()
            .collect::<Vec<(usize, usize)>>();

        // Sort to guarantee that items with equal levels are adjacent
        candidates.sort_by_key(|&(_, level)| level);
        candidates
            .iter()
            .zip(candidates.iter().skip(1))
            .filter_map(|(&(idx1, v1), &(idx2, v2))| {
                if v1 == v2 {
                    Some((idx1, idx2, v1))
                } else {
                    None
                }
            })
            .min_by_key(|&(_, _, level)| level)
            .map(|(idx1, idx2, _)| (idx1, idx2))
    }

    fn merge_and_return_empty_buffer(&mut self, b1: usize, b2: usize) -> usize {
        let bs1 = self.bufstate[b1];
        let bs2 = self.bufstate[b2];
        if let (BufState::Full { level: l1 }, BufState::Full { level: l2 }) = (bs1, bs2) {
            debug_assert!(l1 == l2, "Cannot merge buffers at different levels");
            let mut tmp = [0; BUFSIZE * 2];
            QuantileSketch::concat_buffers(&self.buffers[b1], &self.buffers[b2], &mut tmp);
            QuantileSketch::compact_into(&mut tmp, &mut self.buffers[b1]);
            self.bufstate[b1] = BufState::Full { level: l1 + 1 };
            self.bufstate[b2] = BufState::Empty;
            b2
        } else {
            panic!("Cannot merge buffers unless they are full");
        }
    }

    fn concat_buffers(b1: &[u64], b2: &[u64], out: &mut [u64]) {
        debug_assert!(out.len() >= b1.len() + b2.len());
        for (idx, val) in b1.iter().enumerate() {
            out[idx] = *val;
        }
        for (idx, val) in b2.iter().enumerate() {
            out[idx + BUFSIZE] = *val;
        }
    }

    fn compact_into(b: &mut [u64], out: &mut [u64]) {
        debug_assert!(out.len() >= b.len() / 2);
        b.sort();
        let r = rand::random::<bool>();
        b.iter()
            .enumerate()
            .filter_map(|(idx, val)| if r == (idx % 2 == 0) { Some(val) } else { None })
            .enumerate()
            .for_each(|(idx, val)| out[idx] = *val);
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use rand::Rng;

    fn check_error_bound(input: Vec<u64>) {
        let mut q = QuantileSketch::new();
        let n = input.len();

        for v in input.iter() {
            q.insert(*v);
        }

        let mut sorted = input.clone();
        sorted.sort();

        for i in 1..10 {
            let phi = i as f64 / 10.0;
            let exact_idx = (n as f64 * phi) as usize;
            let exact = sorted[exact_idx] as i64;
            let result = q.query(phi).expect("no result from query") as i64;
            let error = (exact - result).abs() as f64 / n as f64;
            println!(
                "phi = {}, exact = {}, result = {}, err = {}",
                phi, exact, result, error
            );
            assert!(error <= EPSILON * 2.0);
        }
    }

    #[test]
    fn it_handles_query_with_no_values() {
        let q = QuantileSketch::new();
        if let Some(_) = q.query(0.1) {
            panic!("expected no result!");
        }
    }

    #[test]
    fn it_handles_small_distinct_ordered_input() {
        let n = BUFSIZE * BUFCOUNT;
        let mut input: Vec<u64> = Vec::with_capacity(n);
        for v in 0..n {
            input.push(v as u64);
        }
        check_error_bound(input);
    }

    #[test]
    fn it_handles_small_distinct_unordered_input() {
        let n = BUFSIZE * BUFCOUNT;
        let mut input: Vec<u64> = Vec::with_capacity(n);
        for v in 0..n {
            input.push(v as u64);
        }

        let mut rng = rand::thread_rng();
        rng.shuffle(&mut input);
        check_error_bound(input);
    }

    #[test]
    fn it_handles_small_input_with_duplicates() {
        let n = BUFSIZE * BUFCOUNT;
        let mut input: Vec<u64> = Vec::with_capacity(n);
        for v in 0..(n / 2) {
            input.push(v as u64);
            input.push(v as u64);
        }
        let mut rng = rand::thread_rng();
        rng.shuffle(&mut input);
        check_error_bound(input);
    }

    #[test]
    fn it_handles_large_distinct_ordered_input() {
        let n = BUFSIZE * BUFCOUNT * 100;
        let mut input: Vec<u64> = Vec::with_capacity(n);
        for v in 0..n {
            input.push(v as u64);
        }
        check_error_bound(input);
    }

    #[test]
    fn it_handles_large_distinct_unordered_input() {
        let n = BUFSIZE * BUFCOUNT * 100;
        let mut input: Vec<u64> = Vec::with_capacity(n);
        for v in 0..n {
            input.push(v as u64);
        }
        let mut rng = rand::thread_rng();
        rng.shuffle(&mut input);
        check_error_bound(input);
    }

    #[test]
    fn it_handles_large_input_with_duplicates() {
        let n = BUFSIZE * BUFCOUNT * 100;
        let mut input: Vec<u64> = Vec::with_capacity(n);
        for v in 0..(n / 2) {
            input.push(v as u64);
            input.push(v as u64);
        }
        let mut rng = rand::thread_rng();
        rng.shuffle(&mut input);
        check_error_bound(input);
    }
}
