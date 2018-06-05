use encode::{Decodable, Encodable, EncodableError};
use quantile::mergable::MergableSketch;
use quantile::readable::ReadableSketch;
use std::io::{Read, Write};

#[derive(Debug, PartialEq)]
pub struct SerializableSketch {
    count: usize,
    sorted_levels: Vec<Vec<u64>>,
}

impl SerializableSketch {
    pub fn new(count: usize, mut levels: Vec<Vec<u64>>) -> SerializableSketch {
        levels.iter_mut().for_each(|values| values.sort_unstable());
        SerializableSketch {
            count: count,
            sorted_levels: levels,
        }
    }

    pub fn to_mergable(self) -> MergableSketch {
        MergableSketch::new(self.count, self.sorted_levels)
    }

    pub fn to_readable(self) -> ReadableSketch {
        let weighted_vals = self.sorted_levels
            .iter()
            .enumerate()
            .flat_map(|(level, values)| ReadableSketch::weighted_values_for_level(level, &values))
            .collect();
        ReadableSketch::new(self.count, weighted_vals)
    }
}

impl<W> Encodable<SerializableSketch, W> for SerializableSketch
where
    W: Write,
{
    fn encode(&self, writer: &mut W) -> Result<(), EncodableError> {
        (self.count as u64).encode(writer)?;
        self.sorted_levels.encode(writer)?;
        Ok(())
    }
}

impl<R> Decodable<SerializableSketch, R> for SerializableSketch
where
    R: Read,
{
    fn decode(reader: &mut R) -> Result<SerializableSketch, EncodableError> {
        let s = SerializableSketch {
            count: usize::decode(reader)?,
            sorted_levels: Vec::<Vec<u64>>::decode(reader)?,
        };
        Ok(s)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn it_serializes_and_deserializes_empty() {
        let s = SerializableSketch::new(0, Vec::new());
        assert_encode_and_decode(&s);
    }

    #[test]
    fn it_serializes_and_deserializes_nonempty() {
        let count = 6;
        let levels = vec![vec![1, 2, 3], vec![4, 5, 6]];
        let s = SerializableSketch::new(count, levels);
        assert_encode_and_decode(&s);
    }

    fn assert_encode_and_decode(s: &SerializableSketch) {
        let mut buf = Vec::new();
        s.encode(&mut buf).expect("Could not encode sketch");
        let decoded = SerializableSketch::decode(&mut &buf[..]).expect("Could not decode sketch");
        assert_eq!(*s, decoded);
    }
}
