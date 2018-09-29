use encode::{Decodable, Encodable, EncodableError};
use std::io::{Read, Write};
use std::mem::size_of;

const BYTE_MASK: u8 = 0x7F;
const CONTINUE_BIT: u8 = (1 << 7);

pub fn vbyte_encode<W>(values: &[u64], writer: &mut W) -> Result<(), EncodableError>
where
    W: Write,
{
    let mut encoded = Vec::with_capacity(values.len() * size_of::<u64>());
    for &v in values.iter() {
        if v != 0 {
            let mut x = v;
            while x > 0 {
                let mut b = x as u8 & BYTE_MASK;
                x = x >> 7;
                let continue_flag = ((x > 0) as u8) << 7;
                b |= continue_flag;
                encoded.push(b);
            }
        } else {
            encoded.push(0u8);
        }
    }
    encoded.encode(writer)
}

pub fn vbyte_decode<R>(reader: &mut R) -> Result<Vec<u64>, EncodableError>
where
    R: Read,
{
    let encoded = Vec::<u8>::decode(reader)?;
    let mut decoded = Vec::with_capacity(encoded.len() / size_of::<u64>());
    let mut v = 0u64;
    let mut i = 0;
    for b in encoded.iter() {
        v |= ((b & BYTE_MASK) as u64) << (i * 7);
        i += 1;
        if (b & CONTINUE_BIT) == 0 {
            decoded.push(v);
            v = 0u64;
            i = 0;
        }
    }
    Ok(decoded)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn it_encodes_and_decodes_empty() {
        let mut buf = Vec::<u8>::new();
        vbyte_encode(&vec![], &mut buf).unwrap();
        let output = vbyte_decode(&mut &buf[..]).unwrap();
        assert_eq!(output.len(), 0);
    }

    #[test]
    fn it_encodes_and_decodes_multiple() {
        let mut buf = Vec::<u8>::new();
        let input = vec![1, 2, 1 << 23, 3, 4, 1 << 63, 5];
        vbyte_encode(&input, &mut buf).unwrap();
        let output = vbyte_decode(&mut &buf[..]).unwrap();
        assert_eq!(input, output);
    }

    #[test]
    fn it_encodes_and_decodes_vbyte() {
        assert_vbyte(0);
        assert_vbyte(1);
        assert_vbyte(2);
        assert_vbyte((1 << 7) - 1);
        assert_vbyte(1 << 7);
        assert_vbyte((1 << 7) + 1);
        assert_vbyte((1 << 14) - 1);
        assert_vbyte(1 << 14);
        assert_vbyte((1 << 14) + 1);
        assert_vbyte((1 << 21) - 1);
        assert_vbyte(1 << 21);
        assert_vbyte((1 << 21) + 1);
        assert_vbyte((1 << 28) - 1);
        assert_vbyte(1 << 28);
        assert_vbyte((1 << 28) + 1);
        assert_vbyte((1 << 35) - 1);
        assert_vbyte(1 << 35);
        assert_vbyte((1 << 35) + 1);
        assert_vbyte((1 << 42) - 1);
        assert_vbyte(1 << 42);
        assert_vbyte((1 << 42) + 1);
        assert_vbyte((1 << 49) - 1);
        assert_vbyte(1 << 49);
        assert_vbyte((1 << 49) + 1);
        assert_vbyte((1 << 56) - 1);
        assert_vbyte(1 << 56);
        assert_vbyte((1 << 56) + 1);
        assert_vbyte((1 << 63) - 1);
        assert_vbyte(1 << 63);
        assert_vbyte((1 << 63) + 1);
        assert_vbyte((1 << 63) + 7);
    }

    fn assert_vbyte(input: u64) {
        let mut buf = Vec::<u8>::new();
        vbyte_encode(&vec![input], &mut buf).unwrap();
        let output = vbyte_decode(&mut &buf[..]).unwrap();
        assert_eq!(output.len(), 1);
        println!("0x{:x} => {:x?} => 0x{:x}", input, &buf, output[0]);
        assert_eq!(vec![input], output);
    }
}
