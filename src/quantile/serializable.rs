use encode::{Decodable, Encodable, EncodableError};
use quantile::block::Block;
use quantile::mergable::MergableSketch;
use quantile::readable::ReadableSketch;
use std::io::{Read, Write};

#[derive(Debug, PartialEq)]
pub struct SerializableSketch {
    count: usize,
    levels: Vec<Block>,
}

impl SerializableSketch {
    pub fn new(count: usize, levels: Vec<Block>) -> SerializableSketch {
        SerializableSketch {
            count: count,
            levels: levels,
        }
    }

    pub fn to_mergable(self) -> MergableSketch {
        MergableSketch::new(self.count, self.levels)
    }

    pub fn to_readable(self) -> ReadableSketch {
        ReadableSketch::new(self.count, self.levels)
    }
}

impl<W> Encodable<SerializableSketch, W> for SerializableSketch
where
    W: Write,
{
    fn encode(&self, writer: &mut W) -> Result<(), EncodableError> {
        (self.count as u64).encode(writer)?;
        self.levels.encode(writer)?;
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
            levels: Vec::<Block>::decode(reader)?,
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
        let levels = vec![
            Block::from_sorted_values(&vec![1, 2, 3]),
            Block::from_sorted_values(&vec![4, 5, 6]),
        ];
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
