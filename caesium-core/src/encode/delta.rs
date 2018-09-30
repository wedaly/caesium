use bitpacking::{BitPacker, BitPacker4x};
use encode::{Decodable, Encodable, EncodableError};
use std::io::{Read, Write};

// Data *must* be sorted ascending
pub fn delta_encode<W>(data: &[u32], writer: &mut W) -> Result<(), EncodableError>
where
    W: Write,
{
    let n = data.len();
    let num_blocks = n / BitPacker4x::BLOCK_LEN;
    let bp = BitPacker4x::new();
    let mut x0 = 0u32;
    let mut buf = vec![0u8; 4 * BitPacker4x::BLOCK_LEN];
    n.encode(writer)?;
    num_blocks.encode(writer)?;
    for i in 0..num_blocks {
        let start = i * BitPacker4x::BLOCK_LEN;
        let end = start + BitPacker4x::BLOCK_LEN;
        let num_bits = bp.num_bits_sorted(x0, &data[start..end]);
        let len = bp.compress_sorted(x0, &data[start..end], &mut buf[..], num_bits);
        num_bits.encode(writer)?;
        (&buf[..len]).encode(writer)?;
        x0 = data[end - 1];
    }
    &data[num_blocks * BitPacker4x::BLOCK_LEN..].encode(writer)?;
    Ok(())
}

pub fn delta_decode<R>(reader: &mut R) -> Result<Vec<u32>, EncodableError>
where
    R: Read,
{
    let n = usize::decode(reader)?;
    let num_blocks = usize::decode(reader)?;
    let bp = BitPacker4x::new();
    let mut x0 = 0u32;
    let mut buf = vec![0u32; BitPacker4x::BLOCK_LEN];
    let mut out = Vec::with_capacity(n);
    for _ in 0..num_blocks {
        let num_bits = u8::decode(reader)?;
        let block = Vec::<u8>::decode(reader)?;
        bp.decompress_sorted(x0, &block, &mut buf, num_bits);
        out.extend_from_slice(&buf);
        x0 = out[out.len() - 1];
    }
    let remainder = Vec::<u32>::decode(reader)?;
    out.extend_from_slice(&remainder);
    Ok(out)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn it_encodes_and_decodes_empty() {
        let data = Vec::<u32>::new();
        let mut buf = Vec::<u8>::new();
        delta_encode(&data, &mut buf).expect("Could not encode empty data vec");
        let decoded = delta_decode(&mut &buf[..]).expect("Could not decode empty data vec");
        assert_eq!(decoded.len(), 0);
    }

    #[test]
    fn it_encodes_and_decodes() {
        let mut data = Vec::<u32>::new();
        for i in 0..10 {
            data.push((i * 2) as u32);
        }
        let mut buf = Vec::<u8>::new();
        delta_encode(&data, &mut buf).expect("Could not encode data vec");
        let decoded = delta_decode(&mut &buf[..]).expect("Could not decode data vec");
        assert_eq!(decoded, data);
    }
}
