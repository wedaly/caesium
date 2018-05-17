use quantile::sampler::Sampler;
use quantile::sketch::{Sketch, BUFCOUNT, BUFSIZE};
use rand;
use std::cmp;

#[derive(Copy, Clone)]
enum BufState {
    Empty,
    Filling { level: usize, len: usize },
    Full { level: usize },
}

pub struct SketchBuilder {
    sampler: Sampler,
    buffers: [[u64; BUFSIZE]; BUFCOUNT],
    bufstate: [BufState; BUFCOUNT],
    active_level: usize,
    current_buffer: usize,
    count: usize,
}

impl SketchBuilder {
    pub fn new() -> SketchBuilder {
        SketchBuilder {
            sampler: Sampler::new(),
            buffers: [[0; BUFSIZE]; BUFCOUNT],
            bufstate: [BufState::Empty; BUFCOUNT],
            active_level: 0,
            current_buffer: 0,
            count: 0,
        }
    }

    pub fn clear(&mut self) {
        self.sampler.reset();
        self.bufstate = [BufState::Empty; BUFCOUNT];
        self.active_level = 0;
        self.current_buffer = 0;
        self.count = 0;
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

    pub fn build(&self, out: &mut Sketch) {
        for (idx, state) in self.bufstate.iter().enumerate() {
            let b = out.buffer_mut(idx);
            match *state {
                BufState::Empty => b.set(0, &[]),
                BufState::Filling { level, len } => b.set(level, &self.buffers[idx][..len]),
                BufState::Full { level } => b.set(level, &self.buffers[idx]),
            }
        }
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
                    self.empty_and_return_lowest_buffer()
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
            SketchBuilder::concat_buffers(&self.buffers[b1], &self.buffers[b2], &mut tmp);
            SketchBuilder::compact_into(&mut tmp, &mut self.buffers[b1]);
            self.bufstate[b1] = BufState::Full { level: l1 + 1 };
            self.bufstate[b2] = BufState::Empty;
            b2
        } else {
            panic!("Cannot merge buffers unless they are full");
        }
    }

    fn empty_and_return_lowest_buffer(&mut self) -> usize {
        let idx = self.bufstate
            .iter()
            .filter_map(|&state| match state {
                BufState::Full { level } => Some(level),
                BufState::Filling { level, len: _ } => Some(level),
                _ => None,
            })
            .min()
            .expect("Could not any non-empty buffers");
        self.bufstate[idx] = BufState::Empty;
        idx
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

    #[test]
    fn it_should_empty_and_return_lowest_buffer() {
        let mut b = SketchBuilder::new();
        for v in 0..BUFSIZE {
            b.insert(v as u64);
        }
        let idx = b.empty_and_return_lowest_buffer();
        assert_eq!(idx, 0);

        match b.bufstate[idx] {
            BufState::Empty => (), // pass
            _ => panic!("Expected empty buffer")
        }
    }
}
