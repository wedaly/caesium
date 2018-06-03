use std::cmp::min;
use std::io::Result as IOResult;
use std::io::{Read, Write};

pub struct Buffer {
    cursor: usize,
    data: Vec<u8>,
}

impl Buffer {
    pub fn new() -> Buffer {
        Buffer {
            cursor: 0,
            data: Vec::new(),
        }
    }

    pub fn data(&self) -> &Vec<u8> {
        &self.data
    }
}

impl Write for Buffer {
    fn write(&mut self, buf: &[u8]) -> IOResult<usize> {
        self.data.extend_from_slice(buf);
        Ok(buf.len())
    }

    fn flush(&mut self) -> IOResult<()> {
        self.data.clear();
        self.cursor = 0;
        Ok(())
    }
}

impl Read for Buffer {
    fn read(&mut self, buf: &mut [u8]) -> IOResult<usize> {
        let len = min(buf.len(), self.data.len() - self.cursor);
        buf[..len].copy_from_slice(&self.data[self.cursor..self.cursor + len]);
        self.cursor += len;
        Ok(len)
    }
}
