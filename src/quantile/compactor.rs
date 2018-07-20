use encode::vbyte::{vbyte_decode, vbyte_encode};
use encode::{Decodable, Encodable, EncodableError};
use rand;
use std::io::{Read, Write};
use std::slice::Iter;

#[derive(Clone)]
pub struct Compactor {
    data: Vec<u64>,
    is_sorted: bool,
}

impl Compactor {
    pub fn new() -> Compactor {
        Compactor {
            data: Vec::new(),
            is_sorted: true,
        }
    }

    pub fn insert(&mut self, value: u64) {
        self.data.push(value);
        self.is_sorted = false;
    }

    pub fn insert_sorted(&mut self, sorted_values: &[u64]) {
        self.ensure_sorted();
        self.data = Compactor::merge_sorted(&self.data, sorted_values);
        debug_assert!(self.is_sorted);
    }

    pub fn insert_from_other(&mut self, other: &mut Compactor) {
        other.ensure_sorted();
        self.insert_sorted(&other.data);
    }

    // On input, overflow is empty
    // On output, overflow is sorted (asc by value)
    pub fn compact(&mut self, overflow: &mut Vec<u64>) {
        debug_assert!(overflow.is_empty());
        self.ensure_sorted();

        let n = self.data.len();

        let leftover = if n % 2 != 0 {
            Some(self.data[n - 1])
        } else {
            None
        };

        let mut idx = rand::random::<bool>() as usize;
        while idx < n {
            self.data[idx / 2] = self.data[idx];
            idx += 2;
        }

        overflow.extend_from_slice(&self.data[..n / 2]);

        self.data.clear();
        if let Some(v) = leftover {
            self.data.push(v);
        }
    }

    pub fn iter_values(&self) -> Iter<u64> {
        self.data.iter()
    }

    pub fn size(&self) -> usize {
        self.data.len()
    }

    fn ensure_sorted(&mut self) {
        if !self.is_sorted {
            self.data.sort_unstable();
            self.is_sorted = true;
        }
    }

    fn merge_sorted(v1: &[u64], v2: &[u64]) -> Vec<u64> {
        let (n, m) = (v1.len(), v2.len());
        let mut result = Vec::with_capacity(n + m);
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
        result
    }
}

impl<W> Encodable<W> for Compactor
where
    W: Write,
{
    fn encode(&self, writer: &mut W) -> Result<(), EncodableError> {
        let mut tmp = Vec::new();
        let data = if self.is_sorted {
            &self.data
        } else {
            // Sort before encoding to improve compression
            // and to avoid sorting during later merges
            tmp.extend_from_slice(&self.data);
            tmp.sort_unstable();
            &tmp
        };
        data.len().encode(writer)?;
        let mut x0 = 0;
        for x1 in data.iter() {
            let delta = x1 - x0;
            vbyte_encode(delta, writer)?;
            x0 = *x1;
        }
        Ok(())
    }
}

impl<R> Decodable<Compactor, R> for Compactor
where
    R: Read,
{
    fn decode(reader: &mut R) -> Result<Compactor, EncodableError> {
        let n = usize::decode(reader)?;
        let mut data = Vec::with_capacity(n);
        let mut x0 = 0;
        for _ in 0..n {
            let delta = vbyte_decode(reader)?;
            let x1 = delta + x0;
            data.push(x1);
            x0 = x1;
        }
        let compactor = Compactor {
            data,
            is_sorted: true,
        };
        Ok(compactor)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn it_inserts() {
        let mut c = Compactor::new();
        c.insert(1);
        c.insert(2);
        c.insert(3);
        assert_values(&c, &vec![1, 2, 3]);
    }

    #[test]
    fn it_inserts_sorted() {
        let mut c = Compactor::new();
        c.insert_sorted(&vec![2, 4, 8]);
        c.insert_sorted(&vec![1, 5, 7, 9]);
        assert_values(&c, &vec![1, 2, 4, 5, 7, 8, 9]);
    }

    #[test]
    fn it_inserts_from_other_unsorted() {
        let mut c1 = Compactor::new();
        let mut c2 = Compactor::new();
        c1.insert_sorted(&vec![2, 4, 6, 8]);
        c2.insert(7);
        c2.insert(3);
        c2.insert(9);
        c1.insert_from_other(&mut c2);
        assert_values(&c1, &vec![2, 3, 4, 6, 7, 8, 9]);
    }

    #[test]
    fn it_inserts_from_other_sorted() {
        let mut c1 = Compactor::new();
        let mut c2 = Compactor::new();
        c1.insert_sorted(&vec![2, 4, 6, 8]);
        c2.insert_sorted(&vec![3, 7, 9]);
        c1.insert_from_other(&mut c2);
        assert_values(&c1, &vec![2, 3, 4, 6, 7, 8, 9]);
    }

    #[test]
    fn it_compacts_empty() {
        let mut c = Compactor::new();
        let mut overflow = Vec::new();
        c.compact(&mut overflow);
        assert_eq!(c.size(), 0);
        assert!(overflow.is_empty());
    }

    #[test]
    fn it_compacts_even_num_items() {
        let mut c = Compactor::new();
        c.insert_sorted(&vec![1, 2, 3, 4, 5, 6]);
        let mut overflow = Vec::new();
        c.compact(&mut overflow);
        assert_eq!(c.size(), 0);
        assert_eq!(overflow.len(), 3);
        match overflow.first() {
            Some(1) => assert_eq!(overflow, vec![1, 3, 5]),
            Some(2) => assert_eq!(overflow, vec![2, 4, 6]),
            _ => panic!("Unexpected value in overflow"),
        }
    }

    #[test]
    fn it_compacts_odd_num_items() {
        let mut c = Compactor::new();
        c.insert_sorted(&vec![1, 2, 3, 4, 5]);
        let mut overflow = Vec::new();
        c.compact(&mut overflow);
        assert_eq!(c.size(), 1);
        assert_eq!(overflow.len(), 2);
        match overflow.first() {
            Some(1) => assert_eq!(overflow, vec![1, 3]),
            Some(2) => assert_eq!(overflow, vec![2, 4]),
            _ => panic!("Unexpected value in overflow"),
        }
        assert_eq!(c.data[0], 5);
    }

    fn assert_values(c: &Compactor, expected: &[u64]) {
        let actual: Vec<u64> = c.iter_values().map(|v| *v).collect();
        assert_eq!(c.size(), expected.len());
        assert_eq!(actual, expected);
    }
}
