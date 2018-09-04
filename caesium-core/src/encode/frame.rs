use encode::{Decodable, Encodable, EncodableError};
use std::io::Write;
use std::mem::size_of;

pub struct FrameEncoder {
    buf: Vec<u8>,
}

impl FrameEncoder {
    pub fn new() -> FrameEncoder {
        FrameEncoder { buf: Vec::new() }
    }

    pub fn encode_framed_msg<W, E>(&mut self, msg: &E, dst: &mut W) -> Result<(), EncodableError>
    where
        W: Write,
        E: Encodable<Vec<u8>>,
    {
        self.buf.clear();
        msg.encode(&mut self.buf)?;
        let len = self.buf.len().to_be();
        len.encode(dst)?;
        dst.write(&self.buf)?;
        Ok(())
    }
}

#[derive(Debug, PartialEq, Eq)]
pub struct FrameInfo {
    pub prefix_len: usize,
    pub msg_len: usize,
}

impl FrameInfo {
    pub fn from_bytes(buf: &[u8]) -> Option<FrameInfo> {
        let prefix_len = size_of::<u64>();
        if buf.len() < prefix_len {
            None
        } else {
            let msg_len = FrameInfo::decode_len(&buf[..prefix_len]);
            let f = FrameInfo {
                prefix_len,
                msg_len,
            };
            Some(f)
        }
    }

    fn decode_len(mut len_bytes: &[u8]) -> usize {
        debug_assert!(len_bytes.len() == size_of::<u64>());
        let len = u64::decode(&mut len_bytes).unwrap();
        u64::from_be(len) as usize
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn it_encodes_and_decodes_frame() {
        let mut encoder = FrameEncoder::new();
        let msg = 123456u64;
        let mut buf = Vec::new();
        encoder
            .encode_framed_msg(&msg, &mut buf)
            .expect("Could not encode");
        assert_eq!(buf.len(), size_of::<usize>() + size_of::<u64>());
        let frame_info = FrameInfo::from_bytes(&buf).expect("Could not decode frame info");
        assert_eq!(frame_info.prefix_len, size_of::<usize>());
        assert_eq!(frame_info.msg_len, size_of::<u64>());
    }

    #[test]
    fn it_handles_empty_byte_array() {
        let buf = Vec::new();
        assert_eq!(FrameInfo::from_bytes(&buf), None);
    }

    #[test]
    fn it_handles_byte_array_with_fewer_than_8_bytes() {
        let buf: Vec<u8> = vec![1, 2, 3, 4, 5, 6, 7];
        assert_eq!(FrameInfo::from_bytes(&buf), None);
    }
}
