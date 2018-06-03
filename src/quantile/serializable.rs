#[derive(Serialize, Deserialize)]
pub struct SerializableSketch {
    count: usize,
    levels: Vec<Vec<u64>>,
}

impl SerializableSketch {
    pub fn new() -> SerializableSketch {
        SerializableSketch {
            count: 0,
            levels: Vec::new(),
        }
    }

    pub fn count(&self) -> usize {
        self.count
    }

    pub fn sorted_levels(&self) -> &Vec<Vec<u64>> {
        &self.levels
    }

    pub fn set_count(&mut self, count: usize) {
        self.count = count;
    }

    pub fn add_level(&mut self, values: &[u64], sorted: bool) {
        let mut v = Vec::with_capacity(values.len());
        v.extend_from_slice(values);
        if !sorted {
            v.sort_unstable();
        }
        self.levels.push(v);
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use bincode::{deserialize, serialize};

    #[test]
    fn it_serializes_and_deserializes_empty() {
        let s = SerializableSketch::new();
        assert_encode_and_decode(&s);
    }

    #[test]
    fn it_serializes_and_deserializes_nonempty() {
        let mut s = SerializableSketch::new();
        let mut data = [0; 16];
        for i in 0..data.len() {
            data[i] = i as u64;
        }
        s.set_count(10);
        s.add_level(&data, false);
        data.reverse();
        s.add_level(&data, false);
        assert_encode_and_decode(&s);
    }

    fn assert_encode_and_decode(s: &SerializableSketch) {
        let encoded: Vec<u8> = serialize(&s).unwrap();
        let decoded: SerializableSketch = deserialize(&encoded[..]).unwrap();
        assert_eq!(s.count(), decoded.count());
        assert_eq!(s.sorted_levels(), decoded.sorted_levels());
    }
}
