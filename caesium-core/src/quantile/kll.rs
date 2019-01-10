// Based on Karnin, Lang, and Liberty. "Optimal quantile approximation in streams."
// In Foundations of Computer Science (FOCS), 2016 IEEE 57th Annual Symposium on, pp. 71-78. IEEE, 2016.

use encode::{Decodable, Encodable, EncodableError};
use quantile::compactor::Compactor;
use quantile::minmax::MinMax;
use quantile::readable::{ReadableSketch, WeightedValue};
use quantile::sampler::Sampler;
use slab::Slab;
use std::cmp::min;
use std::io::{Read, Write};
use std::ops::RangeInclusive;

const LEVEL_LIMIT: u8 = 64;

// Capacities calculated using:
// * failure probability (delta) = 10e-8
// * maximum normalized rank error (epsilon) = 1.5e-2
// * top levels (s) = log(log(1/delta)) ~= 5
// * top capacity (k) = (1 / epsilon) * s ~= 200
const CAPACITY_AT_DEPTH: [usize; LEVEL_LIMIT as usize] = [
    200, 200, 200, 200, 200, 27, 18, 12, 8, 6, 4, 3, 2, 2, 2, 2, 2, 2, 2, 2, 2, 2, 2, 2, 2, 2, 2,
    2, 2, 2, 2, 2, 2, 2, 2, 2, 2, 2, 2, 2, 2, 2, 2, 2, 2, 2, 2, 2, 2, 2, 2, 2, 2, 2, 2, 2, 2, 2, 2,
    2, 2, 2, 2, 2,
];

pub struct KllSketch {
    count: usize,
    level: u8,
    size: usize,
    capacity: usize,
    minmax: MinMax,
    sampler: Sampler,
    compactor_count: usize,
    compactor_slab: Slab<Compactor>,
    compactor_map: [Option<usize>; LEVEL_LIMIT as usize], // Level to compactor slab ID
}

impl KllSketch {
    pub fn new() -> KllSketch {
        let mut compactor_slab = Slab::new();
        let mut compactor_map = [None; LEVEL_LIMIT as usize];
        let cid = compactor_slab.insert(Compactor::new());
        compactor_map[0] = Some(cid);
        KllSketch {
            count: 0,
            level: 0,
            size: 0,
            capacity: CAPACITY_AT_DEPTH[0],
            minmax: MinMax::new(),
            sampler: Sampler::new(),
            compactor_count: 1,
            compactor_slab,
            compactor_map,
        }
    }

    fn from_parts(
        count: usize,
        level: u8,
        minmax: MinMax,
        sampler: Sampler,
        mut compactors: Vec<Compactor>,
    ) -> KllSketch {
        assert!(level as usize + compactors.len() < LEVEL_LIMIT as usize);
        assert!(!compactors.is_empty());
        let compactor_count = compactors.len();
        let mut compactor_slab = Slab::new();
        let mut compactor_map = [None; LEVEL_LIMIT as usize];
        for (idx, c) in compactors.drain(..).enumerate() {
            let compactor_level = level + idx as u8;
            let cid = compactor_slab.insert(c);
            compactor_map[compactor_level as usize] = Some(cid);
        }

        let mut s = KllSketch {
            count,
            level,
            size: 0,
            capacity: 0,
            minmax,
            sampler,
            compactor_count,
            compactor_slab,
            compactor_map,
        };
        s.size = s.calculate_size();
        s.capacity = s.calculate_capacity();
        s
    }

    pub fn insert(&mut self, val: u32) {
        self.count += 1;
        self.minmax.update(val);
        if let Some(val) = self.sampler.sample(val) {
            {
                let level = self.level;
                let first_compactor = self.get_mut_compactor(level);
                first_compactor.insert(val);
            }
            self.size += 1;
            self.compress()
        }
    }

    pub fn merge(self, other: KllSketch) -> KllSketch {
        let (mut survivor, mut victim) = if self.level > other.level {
            (self, other)
        } else {
            (other, self)
        };

        let mut values = Vec::new();

        // Absorb victim sampler stored value into survivor sampler
        let sampler_val = victim.sampler.stored_value();
        let sampler_weight = victim.sampler.stored_weight();
        if sampler_weight > 0 {
            if let Some(v) = survivor
                .sampler
                .sample_weighted(sampler_val, sampler_weight)
            {
                values.push(v);
            }
        }

        // Absorb victim levels < survivor level into survivor sampler
        for level in victim.level..min(victim.top_level() + 1, survivor.level) {
            let weight = 1 << level;
            for val in victim.get_compactor(level).iter_values() {
                if let Some(v) = survivor.sampler.sample_weighted(*val, weight) {
                    values.push(v);
                }
            }
        }

        // Insert sampled values into survivor's first level
        {
            let first_level = survivor.level;
            let first_compactor = survivor.get_mut_compactor(first_level);
            values.sort_unstable();
            first_compactor.insert_sorted(&values);
        }

        // Absorb victim levels > survivor level into survivor compactors
        let num_to_add = if victim.top_level() > survivor.top_level() {
            victim.top_level() - survivor.top_level()
        } else {
            0
        };
        for _ in 0..num_to_add {
            survivor.add_compactor();
        }
        for level in survivor.level..=victim.top_level() {
            let mut victim_compactor = victim.get_mut_compactor(level);
            let mut survivor_compactor = survivor.get_mut_compactor(level);
            survivor_compactor.insert_from_other(&mut victim_compactor);
        }

        survivor.minmax.update_from_other(&victim.minmax);
        survivor.count += victim.count;

        // Inserted values may have exceeded capacity, so compress
        survivor.size = survivor.calculate_size();
        survivor.compress();

        survivor
    }

    pub fn to_readable(self) -> ReadableSketch {
        let mut data = Vec::with_capacity(self.size + 1);

        let sampler_weight = self.sampler.stored_weight();
        if sampler_weight > 0 {
            let sampler_value = self.sampler.stored_value();
            data.push(WeightedValue::new(sampler_weight, sampler_value));
        }

        for level in self.compactor_level_range() {
            let weight = 1 << level;
            let c = self.get_compactor(level);
            for value in c.iter_values() {
                data.push(WeightedValue::new(weight, *value));
            }
        }

        ReadableSketch::new(self.count, self.minmax, data)
    }

    pub fn count(&self) -> usize {
        self.count
    }

    pub fn size(&self) -> usize {
        self.size
    }

    fn get_compactor_id(&self, level: u8) -> usize {
        self.compactor_map[level as usize].expect("Could not retrieve compactor ID")
    }

    fn get_compactor(&self, level: u8) -> &Compactor {
        let cid = self.get_compactor_id(level);
        self.compactor_slab
            .get(cid)
            .expect("Could not retrieve compactor")
    }

    fn get_mut_compactor(&mut self, level: u8) -> &mut Compactor {
        let cid = self.get_compactor_id(level);
        self.compactor_slab
            .get_mut(cid)
            .expect("Could not retrieve compactor")
    }

    fn top_level(&self) -> u8 {
        debug_assert!(self.level as usize + self.compactor_count < LEVEL_LIMIT as usize);
        self.level + self.compactor_count as u8 - 1
    }

    fn compactor_level_range(&self) -> RangeInclusive<u8> {
        RangeInclusive::new(self.level, self.top_level())
    }

    fn calculate_size(&self) -> usize {
        self.compactor_level_range()
            .map(|level| {
                let c = self.get_compactor(level);
                c.size()
            }).sum()
    }

    fn calculate_capacity(&self) -> usize {
        self.compactor_level_range()
            .map(|level| self.capacity_at_level(level))
            .sum()
    }

    fn capacity_at_level(&self, level: u8) -> usize {
        debug_assert!(level <= self.top_level());
        let depth = self.top_level() - level;
        debug_assert!(depth < 64);
        CAPACITY_AT_DEPTH[depth as usize]
    }

    fn add_compactor(&mut self) {
        let new_level = self.top_level() + 1;
        assert!(new_level < LEVEL_LIMIT as u8);
        let compactor = Compactor::new();
        let cid = self.compactor_slab.insert(compactor);
        self.compactor_map[new_level as usize] = Some(cid);
        self.compactor_count += 1;
        self.capacity = self.calculate_capacity();
    }

    fn compress(&mut self) {
        while self.size > self.capacity {
            self.compact_levels();
        }
        self.absorb_lower_levels_into_sampler();
    }

    fn compact_levels(&mut self) {
        let mut overflow = Vec::new();
        // Compact first level with size > capacity, and insert surviving values into next level
        for level in self.compactor_level_range() {
            let capacity = self.capacity_at_level(level);
            let c = self.get_mut_compactor(level);
            if overflow.len() > 0 {
                c.insert_sorted(&overflow);
                overflow.clear();
                break;
            }

            if c.size() > capacity {
                c.compact(&mut overflow);
            }
        }

        // Add a new level for surviving values if necessary
        if overflow.len() > 0 {
            self.add_compactor();
            let level = self.top_level();
            let c = self.get_mut_compactor(level);
            c.insert_sorted(&overflow);
        }

        self.size = self.calculate_size();
        self.capacity = self.calculate_capacity();
    }

    fn absorb_lower_levels_into_sampler(&mut self) {
        if cfg!(feature = "nosampler") {
            return;
        }

        // Absorb any empty compactors with capacity == 2 into the sampler
        for level in self.compactor_level_range() {
            let capacity = self.capacity_at_level(level);
            let size = self.get_compactor(level).size();
            if capacity == 2 && size == 0 {
                self.level += 1;
                self.compactor_count -= 1;
                let cid = self.compactor_map[level as usize]
                    .take()
                    .expect("Could not find compactor ID to remove");
                self.compactor_slab.remove(cid);
                self.size = self.calculate_size();
                self.sampler.set_max_weight(1 << self.level);
            } else {
                break;
            }
        }
    }
}

impl Clone for KllSketch {
    fn clone(&self) -> Self {
        let mut compactor_slab = Slab::new();
        let mut compactor_map = [None; LEVEL_LIMIT as usize];
        for level in self.compactor_level_range() {
            let compactor = self.get_compactor(level);
            let cid = compactor_slab.insert(compactor.clone());
            compactor_map[level as usize] = Some(cid);
        }

        KllSketch {
            count: self.count,
            level: self.level,
            size: self.size,
            capacity: self.capacity,
            minmax: self.minmax.clone(),
            sampler: self.sampler.clone(),
            compactor_count: self.compactor_count,
            compactor_slab,
            compactor_map,
        }
    }
}

impl<W> Encodable<W> for KllSketch
where
    W: Write,
{
    fn encode(&self, writer: &mut W) -> Result<(), EncodableError> {
        self.count.encode(writer)?;
        self.level.encode(writer)?;
        self.minmax.encode(writer)?;
        self.sampler.encode(writer)?;
        self.compactor_count.encode(writer)?;
        for level in self.compactor_level_range() {
            let c = self.get_compactor(level);
            c.encode(writer)?;
        }
        Ok(())
    }
}

impl<R> Decodable<KllSketch, R> for KllSketch
where
    R: Read,
{
    fn decode(reader: &mut R) -> Result<KllSketch, EncodableError> {
        let count = usize::decode(reader)?;
        let level = u8::decode(reader)?;
        let minmax = MinMax::decode(reader)?;
        let sampler = Sampler::decode(reader)?;
        let num_compactors = usize::decode(reader)?;

        if level as usize + num_compactors >= LEVEL_LIMIT as usize {
            return Err(EncodableError::FormatError("Level value too large"));
        }

        if num_compactors < 1 {
            return Err(EncodableError::FormatError(
                "Must have at least one compactor",
            ));
        }

        let mut compactors = Vec::new();
        for _ in 0..num_compactors {
            let c = Compactor::decode(reader)?;
            compactors.push(c);
        }
        let s = KllSketch::from_parts(count, level, minmax, sampler, compactors);
        Ok(s)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn it_sketches_quantiles_no_compression() {
        let mut s = KllSketch::new();
        for i in 0..100 {
            s.insert(i as u32);
        }
        let median = s
            .to_readable()
            .query(0.5)
            .map(|q| q.approx_value)
            .expect("Could not query median");
        assert_eq!(median, 50);
    }

    #[test]
    fn it_merges_quantiles_no_compression() {
        let mut s1 = KllSketch::new();
        let mut s2 = KllSketch::new();
        for i in 0..100 {
            s1.insert(i as u32);
            s2.insert(i as u32);
        }
        let merged = s1.merge(s2);
        let median = merged
            .to_readable()
            .query(0.5)
            .map(|q| q.approx_value)
            .expect("Could not query median");
        assert_eq!(median, 50);
    }

    #[test]
    fn it_inserts_without_exceeding_capacity() {
        let mut s = KllSketch::new();
        let n = CAPACITY_AT_DEPTH[0] * LEVEL_LIMIT as usize;
        for i in 0..n {
            s.insert(i as u32);
            assert!(s.calculate_size() <= s.calculate_capacity());
        }
    }

    #[test]
    fn it_merges_without_exceeding_capacity() {
        let mut s1 = KllSketch::new();
        let mut s2 = KllSketch::new();
        let n = CAPACITY_AT_DEPTH[0] * LEVEL_LIMIT as usize;
        for i in 0..n {
            s1.insert(i as u32);
            s2.insert(i as u32);
        }
        let merged = s1.merge(s2);
        assert!(merged.calculate_size() <= merged.calculate_capacity());
    }

    #[test]
    fn it_encodes_and_decodes() {
        let mut s = KllSketch::new();
        let n = CAPACITY_AT_DEPTH[0] * LEVEL_LIMIT as usize;
        for i in 0..n {
            s.insert(i as u32);
        }
        let mut buf = Vec::<u8>::new();
        s.encode(&mut buf).expect("Could not encode sketch");
        let decoded = KllSketch::decode(&mut &buf[..]).expect("Could not decode sketch");
        assert_eq!(s.level, decoded.level);
        assert_eq!(s.capacity, decoded.capacity);

        let original_compactors: Vec<usize> = s.compactor_map.iter().filter_map(|v| *v).collect();
        let decoded_compactors: Vec<usize> =
            decoded.compactor_map.iter().filter_map(|v| *v).collect();
        assert_eq!(original_compactors, decoded_compactors);
    }
}
