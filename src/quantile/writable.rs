use quantile::block::Block;
use quantile::constants::{BUFCOUNT, BUFSIZE};
use quantile::sampler::Sampler;
use quantile::serializable::SerializableSketch;
use rand;
use std::collections::HashMap;

pub struct WritableSketch {
    sampler: Sampler,
    current_buffer: usize,
    count: usize,
    buffers: [[u64; BUFSIZE]; BUFCOUNT],
    lengths: [usize; BUFCOUNT],
    levels: [usize; BUFCOUNT],
    active_level: usize,
    level_limit: usize,
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
            level_limit: WritableSketch::calc_level_limit(0),
        }
    }

    pub fn insert(&mut self, val: u64) {
        self.count += 1;
        if let Some(val) = self.sampler.sample(val) {
            self.insert_sampled(val);
        }
    }

    pub fn to_serializable(mut self) -> SerializableSketch {
        if let Some(val) = self.sampler.stored_val() {
            self.insert_sampled(val);
        }

        let max_level = self.levels.iter().max().unwrap_or(&0);
        let mut levels: Vec<Block> = Vec::new();
        for _ in 0..max_level + 1 {
            levels.push(Block::new())
        }

        for idx in 0..BUFCOUNT {
            let len = self.lengths[idx];
            let level = self.levels[idx];
            levels
                .get_mut(level)
                .expect("Could not retrieve level")
                .insert_unsorted_values(&self.buffers[idx][..len]);
        }

        SerializableSketch::new(self.count, levels)
    }

    fn insert_sampled(&mut self, val: u64) {
        self.update_active_level();
        let idx = self.choose_insert_buffer();
        let len = self.lengths[idx];
        debug_assert!(len < BUFSIZE);
        self.buffers[idx][len] = val;
        self.lengths[idx] += 1;
        self.current_buffer = idx;
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
        if self.count > self.level_limit {
            self.active_level += 1;
            self.sampler.set_max_weight(1 << self.active_level);
            self.level_limit = WritableSketch::calc_level_limit(self.active_level);
        }
    }

    fn calc_level_limit(level: usize) -> usize {
        (1 << (level + BUFCOUNT - 2)) * BUFSIZE
    }
}
