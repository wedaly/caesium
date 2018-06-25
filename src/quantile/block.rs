use encode::vbyte::{vbyte_decode, vbyte_encode};
use encode::{Decodable, Encodable, EncodableError};
use rand;
use std::io::{Read, Write};
use std::slice::Iter;

#[derive(Debug, PartialEq, Clone)]
pub struct Block {
    sorted_values: Vec<u64>,
}

impl Block {
    pub fn new() -> Block {
        Block {
            sorted_values: Vec::new(),
        }
    }

    pub fn from_unsorted_values(values: &[u64]) -> Block {
        let mut b = Block::new();
        b.insert_unsorted_values(values);
        b
    }

    pub fn from_sorted_values(values: &[u64]) -> Block {
        let mut b = Block::new();
        b.insert_sorted_values(values);
        b
    }

    pub fn len(&self) -> usize {
        self.sorted_values.len()
    }

    pub fn iter_sorted_values(&self) -> Iter<u64> {
        self.sorted_values.iter()
    }

    pub fn insert_unsorted_values(&mut self, values: &[u64]) {
        let mut sorted_values = Vec::with_capacity(values.len());
        sorted_values.extend_from_slice(&values);
        sorted_values.sort_unstable();
        self.insert_sorted_values(&sorted_values);
    }

    pub fn insert_sorted_values(&mut self, sorted_values: &[u64]) {
        let mut result = Vec::with_capacity(self.len() + sorted_values.len());
        {
            let (v1, v2) = (&self.sorted_values, &sorted_values);
            let (n, m) = (v1.len(), v2.len());
            let (mut i, mut j) = (0, 0);
            while i < n && j < m {
                let lt = v1[i] < v2[j];
                let v1_mask = !(lt as u64).wrapping_sub(1);
                let v2_mask = !(!lt as u64).wrapping_sub(1);
                let val = (v1[i] & v1_mask) | (v2[j] & v2_mask);
                result.push(val);
                i += lt as usize;
                j += !lt as usize;
            }
            result.extend_from_slice(&v1[i..n]);
            result.extend_from_slice(&v2[j..m]);
        }
        self.sorted_values = result
    }

    pub fn insert_from_block(&mut self, other: &Block) {
        self.insert_sorted_values(&other.sorted_values);
    }

    pub fn compact(&mut self, overflow: &mut Block) {
        debug_assert!(overflow.len() == 0);
        let n = self.sorted_values.len();
        let mut idx = rand::random::<bool>() as usize;
        while idx < n {
            self.sorted_values[idx / 2] = self.sorted_values[idx];
            idx += 2;
        }
        overflow
            .sorted_values
            .extend_from_slice(&self.sorted_values[..n / 2]);
        self.clear();
    }

    pub fn clear(&mut self) {
        self.sorted_values.clear();
    }
}

impl<W> Encodable<W> for Block
where
    W: Write,
{
    fn encode(&self, writer: &mut W) -> Result<(), EncodableError> {
        let n = self.sorted_values.len();
        n.encode(writer)?;
        let mut x0 = 0;
        for x1 in self.sorted_values.iter() {
            let delta = x1 - x0;
            vbyte_encode(delta, writer)?;
            x0 = *x1;
        }
        Ok(())
    }
}

impl<R> Decodable<Block, R> for Block
where
    R: Read,
{
    fn decode(reader: &mut R) -> Result<Block, EncodableError> {
        let n = usize::decode(reader)?;
        let mut v = Vec::with_capacity(n);
        let mut x0 = 0;
        for _ in 0..n {
            let delta = vbyte_decode(reader)?;
            let x1 = delta + x0;
            v.push(x1);
            x0 = x1;
        }

        let b = Block { sorted_values: v };
        Ok(b)
    }
}

build_encodable_vec_type!(Block);

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn it_inserts_unsorted_values() {
        let mut b = Block::new();
        let data = vec![5, 4, 0, 2, 3, 1];
        b.insert_unsorted_values(&data);
        assert_values(&b, vec![0, 1, 2, 3, 4, 5]);
    }

    #[test]
    fn it_inserts_sorted_values() {
        let mut b = Block::new();
        let data = vec![0, 1, 2, 3, 4, 5];
        b.insert_sorted_values(&data);
        assert_values(&b, vec![0, 1, 2, 3, 4, 5]);
    }

    #[test]
    fn it_inserts_from_block() {
        let mut b1 = Block::new();
        let mut b2 = Block::new();

        b1.insert_sorted_values(&vec![2, 4, 5]);
        b2.insert_sorted_values(&vec![1, 3, 6]);
        b1.insert_from_block(&b2);
        assert_values(&b1, vec![1, 2, 3, 4, 5, 6]);
    }

    #[test]
    fn it_encodes_and_decodes_empty() {
        let b = Block::new();
        let mut buf = Vec::<u8>::new();
        b.encode(&mut buf).unwrap();
        let decoded = Block::decode(&mut &buf[..]).unwrap();
        assert_values(&decoded, vec![]);
    }

    #[test]
    fn it_encodes_and_decodes_single_value() {
        let mut b = Block::new();
        b.insert_sorted_values(&vec![42]);

        let mut buf = Vec::<u8>::new();
        b.encode(&mut buf).unwrap();
        let decoded = Block::decode(&mut &buf[..]).unwrap();
        assert_values(&decoded, vec![42]);
    }

    #[test]
    fn it_encodes_and_decodes_two_values() {
        let mut b = Block::new();
        b.insert_sorted_values(&vec![9, 60]);

        let mut buf = Vec::<u8>::new();
        b.encode(&mut buf).unwrap();
        let decoded = Block::decode(&mut &buf[..]).unwrap();
        assert_values(&decoded, vec![9, 60]);
    }

    #[test]
    fn it_encodes_and_decodes_multiple_values() {
        let mut b = Block::new();
        b.insert_sorted_values(&vec![1, 2, 3, 4]);

        let mut buf = Vec::<u8>::new();
        b.encode(&mut buf).unwrap();
        let decoded = Block::decode(&mut &buf[..]).unwrap();
        assert_values(&decoded, vec![1, 2, 3, 4]);
    }

    fn assert_values(block: &Block, expected: Vec<u64>) {
        let output: Vec<u64> = block.iter_sorted_values().map(|&v| v).collect();
        assert_eq!(output, expected);
    }
}
