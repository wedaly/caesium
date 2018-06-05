use quantile::constants::{CAPACITY_DECAY, MAX_LEVEL_CAPACITY, MIN_LEVEL_CAPACITY};
use quantile::readable::ReadableSketch;
use rand;
use std::cmp::max;

pub struct MergableSketch {
    count: usize, // from original datastream
    size: usize,  // count of stored values
    capacity: usize,
    sorted_levels: Vec<Vec<u64>>,
}

impl MergableSketch {
    pub fn new(count: usize, sorted_levels: Vec<Vec<u64>>) -> MergableSketch {
        let size = MergableSketch::calculate_size(&sorted_levels);
        let capacity = MergableSketch::calculate_capacity(&sorted_levels);
        MergableSketch {
            count: count,
            size: size,
            capacity: capacity,
            sorted_levels: sorted_levels,
        }
    }

    pub fn empty() -> MergableSketch {
        MergableSketch::new(0, Vec::new())
    }

    pub fn to_readable(&self) -> ReadableSketch {
        let weighted_vals = self.sorted_levels
            .iter()
            .enumerate()
            .flat_map(|(level, values)| ReadableSketch::weighted_values_for_level(level, &values))
            .collect();
        ReadableSketch::new(self.count, weighted_vals)
    }

    pub fn count(&self) -> usize {
        self.count
    }

    pub fn merge(&mut self, other: &MergableSketch) {
        self.insert_from_other(other);
        while self.size > self.capacity {
            self.compress();
        }
    }

    fn insert_from_other(&mut self, other: &MergableSketch) {
        self.count += other.count;

        // Add levels if necessary
        if other.sorted_levels.len() > self.sorted_levels.len() {
            let levels_to_grow = other.sorted_levels.len() - self.sorted_levels.len();
            for _ in 0..levels_to_grow {
                self.sorted_levels.push(Vec::new());
            }
        }
        debug_assert!(self.sorted_levels.len() >= other.sorted_levels.len());

        // Concat other's data into self
        for (mut dst, src) in self.sorted_levels
            .iter_mut()
            .zip(other.sorted_levels.iter())
        {
            MergableSketch::merge_sorted(&src, &mut dst);
        }

        // Size and capacity may have change, since we added levels and inserted vals
        self.update_size_and_capacity();
    }

    fn compress(&mut self) {
        debug_assert!(self.sorted_levels.len() > 0);
        debug_assert!(self.size > self.capacity);

        let max_level = self.sorted_levels.len() - 1;
        let mut tmp = Vec::new();
        for (level, mut values) in self.sorted_levels.iter_mut().enumerate() {
            if tmp.len() > 0 {
                values.extend_from_slice(&tmp);
                tmp.clear();
                break;
            }

            if values.len() > MergableSketch::capacity_at_level(level, max_level) {
                MergableSketch::compact(&mut values, &mut tmp);
            }
        }

        if tmp.len() > 0 {
            self.sorted_levels.push(tmp);
        }

        self.update_size_and_capacity();
    }

    fn merge_sorted(src: &[u64], dst: &mut Vec<u64>) {
        let mut tmp = Vec::with_capacity(src.len() + dst.len());
        let (mut i, mut j) = (0, 0);
        let (n, m) = (src.len(), dst.len());
        while i < n && j < m {
            let lt = src[i] < dst[j];
            let src_mask = !(lt as u64).wrapping_sub(1);
            let dst_mask = !(!lt as u64).wrapping_sub(1);
            let val = (src[i] & src_mask) | (dst[j] & dst_mask);
            tmp.push(val);
            i += lt as usize;
            j += !lt as usize;
        }

        tmp.extend_from_slice(&src[i..n]);
        tmp.extend_from_slice(&dst[j..m]);
        dst.clear();
        dst.extend_from_slice(&tmp[..]);
    }

    fn compact(src: &mut Vec<u64>, mut dst: &mut Vec<u64>) {
        let mut sel = rand::random::<bool>();
        for idx in 0..src.len() {
            if sel {
                src[idx / 2] = src[idx];
            }
            sel = !sel;
        }
        MergableSketch::merge_sorted(&src[..src.len() / 2], &mut dst);
        src.clear();
    }

    fn update_size_and_capacity(&mut self) {
        self.capacity = MergableSketch::calculate_capacity(&self.sorted_levels);
        self.size = MergableSketch::calculate_size(&self.sorted_levels);
    }

    fn calculate_size(levels: &Vec<Vec<u64>>) -> usize {
        levels.iter().map(|values| values.len()).sum()
    }

    fn calculate_capacity(levels: &Vec<Vec<u64>>) -> usize {
        if levels.len() == 0 {
            0
        } else {
            let max_level = levels.len() - 1;
            (0..levels.len())
                .map(|level| MergableSketch::capacity_at_level(level, max_level))
                .sum()
        }
    }

    fn capacity_at_level(level: usize, max_level: usize) -> usize {
        debug_assert!(level <= max_level);
        let depth = max_level - level;
        let decay = CAPACITY_DECAY.powf(depth as f32);
        let calculated_cap = (MAX_LEVEL_CAPACITY as f32 * decay).ceil() as usize;
        max(MIN_LEVEL_CAPACITY, calculated_cap)
    }
}
