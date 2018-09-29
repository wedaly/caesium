use encode::{Decodable, Encodable, EncodableError};
use std::cmp::{max, min};
use std::io::{Read, Write};

#[derive(Clone)]
pub struct MinMax {
    min: u32,
    max: u32,
}

impl MinMax {
    pub fn new() -> MinMax {
        MinMax {
            min: u32::max_value(),
            max: 0u32,
        }
    }

    pub fn from_values(values: &[u32]) -> MinMax {
        let mut m = MinMax::new();
        for &v in values.iter() {
            m.update(v);
        }
        m
    }

    pub fn update(&mut self, val: u32) {
        self.min = min(self.min, val);
        self.max = max(self.max, val);
    }

    pub fn update_from_other(&mut self, other: &MinMax) {
        self.min = min(self.min, other.min);
        self.max = max(self.max, other.max);
    }

    pub fn min(&self) -> Option<u32> {
        if self.has_minmax() {
            Some(self.min)
        } else {
            None
        }
    }

    pub fn max(&self) -> Option<u32> {
        if self.has_minmax() {
            Some(self.max)
        } else {
            None
        }
    }

    fn has_minmax(&self) -> bool {
        self.min <= self.max
    }
}

impl<W> Encodable<W> for MinMax
where
    W: Write,
{
    fn encode(&self, writer: &mut W) -> Result<(), EncodableError> {
        self.min.encode(writer)?;
        self.max.encode(writer)?;
        Ok(())
    }
}

impl<R> Decodable<MinMax, R> for MinMax
where
    R: Read,
{
    fn decode(reader: &mut R) -> Result<MinMax, EncodableError> {
        let min = u32::decode(reader)?;
        let max = u32::decode(reader)?;
        let minmax = MinMax { min, max };
        Ok(minmax)
    }
}

#[cfg(test)]
pub mod tests {
    use super::*;

    #[test]
    fn it_returns_none_if_no_value() {
        let m = MinMax::new();
        assert_eq!(m.min(), None);
        assert_eq!(m.max(), None);
    }

    #[test]
    fn it_returns_min_and_max_same_value() {
        let mut m = MinMax::new();
        m.update(7);
        assert_eq!(m.min(), Some(7));
        assert_eq!(m.max(), Some(7));
    }

    #[test]
    fn it_returns_min_and_max_different_values() {
        let mut m = MinMax::new();
        for i in 0..100 {
            m.update(i as u32);
        }
        assert_eq!(m.min(), Some(0));
        assert_eq!(m.max(), Some(99));
    }

    #[test]
    fn it_updates_from_other() {
        let mut m1 = MinMax::new();
        m1.update(5);
        m1.update(6);

        let mut m2 = MinMax::new();
        m2.update(1);
        m2.update(8);

        m1.update_from_other(&m2);
        assert_eq!(m1.min(), Some(1));
        assert_eq!(m1.max(), Some(8));
    }

    #[test]
    fn it_encodes_and_decodes() {
        let mut m = MinMax::new();
        m.update(1);
        m.update(2);
        let mut buf = Vec::<u8>::new();
        m.encode(&mut buf).expect("Could not encode minmax");
        let decoded = MinMax::decode(&mut &buf[..]).expect("Could not decode minmax");
        assert_eq!(m.min(), decoded.min());
        assert_eq!(m.max(), decoded.max());
    }
}
