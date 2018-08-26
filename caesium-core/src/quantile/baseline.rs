use encode::delta::{delta_decode, delta_encode};
use encode::{Decodable, Encodable, EncodableError};
use quantile::minmax::MinMax;
use quantile::readable::{ReadableSketch, WeightedValue};
use std::io::{Read, Write};

#[derive(Clone)]
pub struct BaselineSketch {
    data: Vec<u64>,
    minmax: MinMax,
}

impl BaselineSketch {
    pub fn new() -> BaselineSketch {
        BaselineSketch {
            data: Vec::new(),
            minmax: MinMax::new(),
        }
    }

    pub fn insert(&mut self, val: u64) {
        self.minmax.update(val);
        self.data.push(val);
    }

    pub fn merge(mut self, other: BaselineSketch) -> BaselineSketch {
        self.data.extend_from_slice(&other.data);
        self.minmax.update_from_other(&other.minmax);
        self
    }

    pub fn to_readable(self) -> ReadableSketch {
        let weighted_values: Vec<WeightedValue> = self
            .data
            .iter()
            .map(|v| WeightedValue::new(1, *v))
            .collect();
        ReadableSketch::new(self.data.len(), self.minmax, weighted_values)
    }

    pub fn count(&self) -> usize {
        self.data.len()
    }

    pub fn size(&self) -> usize {
        self.data.len()
    }
}

impl<W> Encodable<W> for BaselineSketch
where
    W: Write,
{
    fn encode(&self, writer: &mut W) -> Result<(), EncodableError> {
        let mut tmp = Vec::new();
        tmp.extend_from_slice(&self.data);
        tmp.sort_unstable();
        self.minmax.encode(writer)?;
        delta_encode(&tmp, writer)?;
        Ok(())
    }
}

impl<R> Decodable<BaselineSketch, R> for BaselineSketch
where
    R: Read,
{
    fn decode(reader: &mut R) -> Result<BaselineSketch, EncodableError> {
        let minmax = MinMax::decode(reader)?;
        let data = delta_decode(reader)?;
        Ok(BaselineSketch { data, minmax })
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn it_inserts_values() {
        let mut s = BaselineSketch::new();
        for i in 0..10 {
            s.insert(i as u64);
        }
        let r = s.to_readable();
        let q = r.query(0.5).expect("Could not query");
        assert_eq!(q.count, 10);
        assert_eq!(q.approx_value, 5);
    }

    #[test]
    fn it_merges() {
        let mut s1 = BaselineSketch::new();
        let mut s2 = BaselineSketch::new();
        for i in 0..10 {
            s1.insert(i as u64);
            s2.insert((i + 10) as u64);
        }
        let s = s1.merge(s2);
        let r = s.to_readable();
        let q = r.query(0.5).expect("Could not query");
        assert_eq!(q.count, 20);
        assert_eq!(q.approx_value, 10);
    }

    #[test]
    fn it_encodes_and_decodes() {
        let mut s = BaselineSketch::new();
        for i in 0..10 {
            s.insert(i as u64);
        }

        let mut buf = Vec::<u8>::new();
        s.encode(&mut buf).expect("Could not encode sketch");
        let decoded = BaselineSketch::decode(&mut &buf[..]).expect("Could not decode sketch");
        let r = decoded.to_readable();
        let q = r.query(0.5).expect("Could not query");
        assert_eq!(q.count, 10);
        assert_eq!(q.approx_value, 5);
    }
}
