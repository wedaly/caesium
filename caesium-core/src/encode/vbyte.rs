use encode::{Decodable, Encodable, EncodableError};
use std::io::{Read, Write};
use stream_vbyte;

pub fn vbyte_encode<W>(values: &[u32], writer: &mut W) -> Result<(), EncodableError>
where
    W: Write,
{
    values.len().encode(writer)?;
    if values.len() < 4 {
        values.encode(writer)?;
    } else {
        let mut encoded_data = vec![0u8; 5 * values.len()];
        stream_vbyte::encode::<stream_vbyte::Scalar>(&values, &mut encoded_data);
        encoded_data.encode(writer)?;
    }
    Ok(())
}

pub fn vbyte_decode<R>(reader: &mut R) -> Result<Vec<u32>, EncodableError>
where
    R: Read,
{
    let n = usize::decode(reader)?;
    if n < 4 {
        Vec::<u32>::decode(reader)
    } else {
        let mut values = vec![0u32; n];
        let encoded_data = Vec::<u8>::decode(reader)?;
        stream_vbyte::decode::<stream_vbyte::Scalar>(&encoded_data, n, &mut values);
        Ok(values)
    }
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
        let input = vec![1, 2, 1 << 23, 3, 4, 1 << 31, 5];
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
        assert_vbyte((1 << 31) - 1);
        assert_vbyte(1 << 31);
        assert_vbyte((1 << 31) + 1);
        assert_vbyte((1 << 31) + 7);
    }

    fn assert_vbyte(input: u32) {
        let mut buf = Vec::<u8>::new();
        vbyte_encode(&vec![input], &mut buf).unwrap();
        let output = vbyte_decode(&mut &buf[..]).unwrap();
        assert_eq!(output.len(), 1);
        println!("0x{:x} => {:x?} => 0x{:x}", input, &buf, output[0]);
        assert_eq!(vec![input], output);
    }
}
