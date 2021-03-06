use encode::delta::{delta_decode, delta_encode};
use encode::{Decodable, Encodable, EncodableError};
use quantile::query::UnweightedQuerySketch;
use std::io::{Read, Write};

#[derive(Clone)]
pub struct BaselineSketch {
    is_sorted: bool,
    data: Vec<u32>,
}

impl BaselineSketch {
    pub fn new() -> BaselineSketch {
        BaselineSketch {
            is_sorted: true,
            data: Vec::new(),
        }
    }

    pub fn insert(&mut self, val: u32) {
        self.is_sorted = false;
        self.data.push(val);
    }

    pub fn merge(mut self, other: BaselineSketch) -> BaselineSketch {
        self.is_sorted = false;
        self.data.extend_from_slice(&other.data);
        self
    }

    pub fn to_readable(mut self) -> UnweightedQuerySketch {
        if !self.is_sorted {
            self.data.sort_unstable();
        }
        UnweightedQuerySketch::new(self.data)
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
        let data = if self.is_sorted {
            &self.data
        } else {
            tmp.extend_from_slice(&self.data);
            tmp.sort_unstable();
            &tmp
        };
        delta_encode(&data, writer)?;
        Ok(())
    }
}

impl<R> Decodable<BaselineSketch, R> for BaselineSketch
where
    R: Read,
{
    fn decode(reader: &mut R) -> Result<BaselineSketch, EncodableError> {
        let data = delta_decode(reader)?;
        Ok(BaselineSketch {
            is_sorted: true,
            data,
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn it_inserts_values() {
        let mut s = BaselineSketch::new();
        for i in 0..10 {
            s.insert(i as u32);
        }
        assert_query(s, 10, 5);
    }

    #[test]
    fn it_merges() {
        let mut s1 = BaselineSketch::new();
        let mut s2 = BaselineSketch::new();
        for i in 0..10 {
            s1.insert(i as u32);
            s2.insert((i + 10) as u32);
        }
        let s = s1.merge(s2);
        assert_query(s, 20, 10);
    }

    #[test]
    fn it_encodes_and_decodes() {
        let mut s = BaselineSketch::new();
        for i in 0..10 {
            s.insert(i as u32);
        }
        let decoded = encode_and_decode(s);
        assert_query(decoded, 10, 5);
    }

    #[test]
    fn it_encodes_and_decodes_unsorted() {
        let mut s = BaselineSketch::new();
        for i in 0..10 {
            let val = 9 - i;
            s.insert(val as u32);
        }
        let decoded = encode_and_decode(s);
        assert_query(decoded, 10, 5);
    }

    #[test]
    fn it_encodes_and_decodes_after_merge() {
        let mut s1 = BaselineSketch::new();
        let mut s2 = BaselineSketch::new();
        for i in 0..10 {
            s1.insert(i as u32);
            s2.insert((i + 10) as u32);
        }
        let d1 = encode_and_decode(s1);
        let d2 = encode_and_decode(s2);
        let s = encode_and_decode(d2.merge(d1));
        assert_query(s, 20, 10);
    }

    fn encode_and_decode(s: BaselineSketch) -> BaselineSketch {
        let mut buf = Vec::<u8>::new();
        s.encode(&mut buf).expect("Could not encode sketch");
        BaselineSketch::decode(&mut &buf[..]).expect("Could not decode sketch")
    }

    fn assert_query(s: BaselineSketch, expected_count: usize, expected_median: u32) {
        let r = s.to_readable();
        let q = r.query(0.5).expect("Could not query");
        assert_eq!(q.count, expected_count);
        assert_eq!(q.approx_value, expected_median);
    }
}
