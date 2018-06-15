use quantile::block::Block;
use quantile::constants::{CAPACITY_DECAY, MAX_LEVEL_CAPACITY, MIN_LEVEL_CAPACITY};
use quantile::readable::ReadableSketch;
use quantile::serializable::SerializableSketch;
use std::cmp::max;

#[derive(Clone)]
pub struct MergableSketch {
    count: usize, // from original datastream
    size: usize,  // count of stored values
    capacity: usize,
    levels: Vec<Block>,
}

impl MergableSketch {
    pub fn new(count: usize, levels: Vec<Block>) -> MergableSketch {
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
        MergableSketch::new(0, Vec::new())
    }

    pub fn to_readable(self) -> ReadableSketch {
        ReadableSketch::new(self.count, self.levels)
    }

    pub fn to_serializable(self) -> SerializableSketch {
        SerializableSketch::new(self.count, self.levels)
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
        if other.levels.len() > self.levels.len() {
            let levels_to_grow = other.levels.len() - self.levels.len();
            for _ in 0..levels_to_grow {
                self.levels.push(Block::new())
            }
        }
        debug_assert!(self.levels.len() >= other.levels.len());

        // Concat other's data into self
        for (mut dst, src) in self.levels.iter_mut().zip(other.levels.iter()) {
            if src.len() > 0 {
                dst.insert_from_block(&src);
            }
        }

        // Size and capacity may have change, since we added levels and inserted vals
        self.update_size_and_capacity();
    }

    fn compress(&mut self) {
        debug_assert!(self.levels.len() > 0);
        debug_assert!(self.size > self.capacity);

        let max_level = self.levels.len() - 1;
        let mut overflow = Block::new();
        for (level, mut block) in self.levels.iter_mut().enumerate() {
            if overflow.len() > 0 {
                block.insert_from_block(&overflow);
                overflow.clear();
                break;
            }

            if block.len() > MergableSketch::capacity_at_level(level, max_level) {
                block.compact(&mut overflow);
            }
        }

        if overflow.len() > 0 {
            self.levels.push(overflow);
        }

        self.update_size_and_capacity();
    }

    fn update_size_and_capacity(&mut self) {
        self.capacity = MergableSketch::calculate_capacity(&self.levels);
        self.size = MergableSketch::calculate_size(&self.levels);
    }

    fn calculate_size(levels: &Vec<Block>) -> usize {
        levels.iter().map(|block| block.len()).sum()
    }

    fn calculate_capacity(levels: &Vec<Block>) -> usize {
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
