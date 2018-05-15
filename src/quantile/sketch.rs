use std::slice::Iter;

pub const EPSILON: f64 = 0.01;
pub const BUFCOUNT: usize = 8; // log(1/epsilon) + 1
pub const BUFSIZE: usize = 256; // (1/epsilon) * sqrt(log(1/epsilon))

#[derive(Copy, Clone)]
pub struct Buffer {
    slots: [u64; BUFSIZE],
    len: usize,
    level: usize,
}

impl Buffer {
    pub fn new() -> Buffer {
        Buffer {
            slots: [0; BUFSIZE],
            len: 0,
            level: 0,
        }
    }

    pub fn set(&mut self, level: usize, values: &[u64]) {
        debug_assert!(values.len() <= BUFSIZE);
        self.level = level;
        self.len = values.len();
        self.slots[..self.len].clone_from_slice(values);
    }

    pub fn level(&self) -> usize {
        self.level
    }

    pub fn len(&self) -> usize {
        self.len
    }

    pub fn values(&self) -> &[u64] {
        &self.slots[..self.len]
    }
}

pub struct Sketch {
    buffers: [Buffer; BUFCOUNT],
}

impl Sketch {
    pub fn new() -> Sketch {
        Sketch {
            buffers: [Buffer::new(); BUFCOUNT],
        }
    }

    pub fn buffer_iter(&self) -> Iter<Buffer> {
        self.buffers.iter()
    }

    pub fn buffer_mut(&mut self, idx: usize) -> &mut Buffer {
        debug_assert!(idx < BUFCOUNT);
        &mut self.buffers[idx]
    }

    pub fn count(&self) -> usize {
        // 2 ** level = weight (# of items represented in source data stream)
        self.buffers.iter().map(|b| (1 << b.level) * b.len).sum()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn it_sets_default_buffer_values() {
        let sketch = Sketch::new();
        for b in sketch.buffer_iter() {
            assert_eq!(b.level(), 0);
            assert_eq!(b.len(), 0);
            assert_eq!(b.values().len(), 0);
        }
    }

    #[test]
    fn it_writes_and_reads_data() {
        let mut sketch = Sketch::new();

        // write
        let data = [1, 2, 3, 4];
        sketch.buffer_mut(1).set(5, &data);

        // read
        let b = sketch
            .buffer_iter()
            .nth(1)
            .expect("Could not retrieve buffer");
        assert_eq!(b.level(), 5);
        assert_eq!(b.len(), data.len());
        assert_eq!(b.values(), data);
    }
}
