use encode::{Decodable, Encodable, EncodableError};
use quantile::minmax::MinMax;
use std::io::{Read, Write};

// Estimated empirically, depends on sketch size
const EPSILON: f32 = 0.015;

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

#[derive(Copy, Clone, Debug, PartialEq, Eq)]
pub struct ApproxQuantile {
    pub approx_value: u64,
    pub lower_bound: u64,
    pub upper_bound: u64,
}

impl<W> Encodable<W> for ApproxQuantile
where
    W: Write,
{
    fn encode(&self, writer: &mut W) -> Result<(), EncodableError> {
        self.approx_value.encode(writer)?;
        self.lower_bound.encode(writer)?;
        self.upper_bound.encode(writer)?;
        Ok(())
    }
}

impl<R> Decodable<ApproxQuantile, R> for ApproxQuantile
where
    R: Read,
{
    fn decode(reader: &mut R) -> Result<ApproxQuantile, EncodableError> {
        let approx_value = u64::decode(reader)?;
        let lower_bound = u64::decode(reader)?;
        let upper_bound = u64::decode(reader)?;
        Ok(ApproxQuantile {
            approx_value,
            lower_bound,
            upper_bound,
        })
    }
}

#[derive(Debug)]
struct StoredValue {
    value: u64,
    lowest_rank: usize,
    highest_rank: usize,
}

pub struct ReadableSketch {
    data: Vec<StoredValue>,
    minmax: MinMax,
    count: usize,
}

impl ReadableSketch {
    pub fn new(
        count: usize,
        minmax: MinMax,
        weighted_values: Vec<WeightedValue>,
    ) -> ReadableSketch {
        debug_assert!(count == weighted_values.iter().map(|v| v.weight).sum());
        debug_assert!(minmax.min().is_some() == (count > 0));
        debug_assert!(minmax.max().is_some() == (count > 0));
        let data = ReadableSketch::calculate_stored_values(weighted_values);
        ReadableSketch {
            count,
            minmax,
            data,
        }
    }

    pub fn size(&self) -> usize {
        self.data.len()
    }

    pub fn query(&self, phi: f64) -> Option<ApproxQuantile> {
        assert!(0.0 < phi && phi < 1.0);
        if self.count > 0 {
            let target_rank = (self.count as f64 * phi) as usize;
            let idx = self.binary_search(target_rank);
            let approx_value = self.data[idx].value;
            let max_rank_error = (self.count as f32 * EPSILON).ceil() as usize;
            let lower_bound = self.find_lower_bound(target_rank, idx, approx_value, max_rank_error);
            let upper_bound = self.find_upper_bound(target_rank, idx, approx_value, max_rank_error);
            debug_assert!(lower_bound <= approx_value);
            debug_assert!(upper_bound >= approx_value);
            let result = ApproxQuantile {
                approx_value,
                lower_bound,
                upper_bound,
            };
            Some(result)
        } else {
            None
        }
    }

    fn calculate_stored_values(mut weighted_values: Vec<WeightedValue>) -> Vec<StoredValue> {
        let mut result = Vec::<StoredValue>::with_capacity(weighted_values.len());
        let mut rank = 0;
        weighted_values.sort_unstable_by(|x, y| x.value.cmp(&y.value));
        for wv in weighted_values.iter() {
            let n = result.len();
            if n > 0 && result[n - 1].value == wv.value {
                result[n - 1].highest_rank += wv.weight;
            } else {
                let sv = StoredValue {
                    value: wv.value,
                    lowest_rank: rank,
                    highest_rank: rank + wv.weight - 1,
                };
                result.push(sv);
            }
            rank += wv.weight;
        }
        result
    }

    fn binary_search(&self, rank: usize) -> usize {
        let (mut i, mut j) = (0, self.data.len());
        while i < j {
            // search range [i, j)
            let midpoint = (j - i) / 2 + i;
            let sv = &self.data[midpoint];
            if sv.highest_rank < rank {
                // search right
                i = midpoint + 1;
            } else if sv.lowest_rank > rank {
                // search left
                j = midpoint;
            } else {
                debug_assert!(sv.lowest_rank <= rank);
                debug_assert!(sv.highest_rank >= rank);
                return midpoint;
            }
        }

        // Should always find a result, since stored values cover all ranks
        debug_assert!(i == j);
        debug_assert!(self.data[i].lowest_rank <= rank);
        debug_assert!(self.data[i].highest_rank >= rank);
        i
    }

    fn find_lower_bound(
        &self,
        rank: usize,
        mut idx: usize,
        approx_value: u64,
        max_rank_error: usize,
    ) -> u64 {
        loop {
            if idx == 0 {
                return self.minmax.min().expect("Could not retrieve min");
            }

            let sv = &self.data[idx - 1];
            if sv.highest_rank + max_rank_error < rank && sv.value <= approx_value {
                return sv.value;
            }

            idx -= 1;
        }
    }

    fn find_upper_bound(
        &self,
        rank: usize,
        mut idx: usize,
        approx_value: u64,
        max_rank_error: usize,
    ) -> u64 {
        loop {
            if idx == self.data.len() - 1 {
                return self.minmax.max().expect("Could not retrieve max");
            }

            let sv = &self.data[idx + 1];
            if sv.lowest_rank - max_rank_error < rank && sv.value >= approx_value {
                return sv.value;
            }

            idx += 1;
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use rand;
    use rand::Rng;

    #[test]
    fn it_queries_empty() {
        let s = ReadableSketch::new(0, MinMax::new(), vec![]);
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
    fn it_calculates_upper_and_lower_bounds_single_value() {
        let data = vec![WeightedValue::new(1, 1)];
        let minmax = MinMax::from_values(&vec![1]);
        let s = ReadableSketch::new(1, minmax, data);
        let q = s.query(0.5);
        let lower = q.map(|q| q.lower_bound);
        let upper = q.map(|q| q.upper_bound);
        assert_eq!(lower, Some(1));
        assert_eq!(upper, Some(1));
    }

    #[test]
    fn it_calculates_upper_and_lower_bounds_many_values() {
        let mut data = Vec::new();
        let mut count = 0;
        let mut minmax = MinMax::new();
        for level in 0..4 {
            let weight = 1 << level;
            for value in 0..64 {
                data.push(WeightedValue::new(weight, value as u64));
                minmax.update(value as u64);
                count += weight;
            }
        }

        let s = ReadableSketch::new(count, minmax, data);
        assert_eq!(s.size(), 64); // deduplicate stored values

        let q = s.query(0.5);
        let approx = q.map(|q| q.approx_value).unwrap();
        let lower = q.map(|q| q.lower_bound).unwrap();
        let upper = q.map(|q| q.upper_bound).unwrap();
        assert!(lower > 0);
        assert!(lower <= approx);
        assert!(approx <= upper);
        assert!(upper < 64);
    }

    fn assert_queries(data: Vec<WeightedValue>) {
        let count = data.iter().map(|v| v.weight).sum();
        let values: Vec<u64> = data.iter().map(|v| v.value).collect();
        let minmax = MinMax::from_values(&values);
        let s = ReadableSketch::new(count, minmax, data.clone());
        for p in 1..100 {
            let phi = p as f64 / 100.0;
            let expected = calculate_exact(&data, phi);
            let actual = s.query(phi).map(|q| q.approx_value);
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
