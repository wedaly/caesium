use encode::vbyte::{vbyte_decode, vbyte_encode};
use encode::{Decodable, Encodable, EncodableError};
use std::io::{Read, Write};

// Data *must* be sorted ascending
pub fn delta_encode<W>(data: &[u64], writer: &mut W) -> Result<(), EncodableError>
where
    W: Write,
{
    data.len().encode(writer)?;
    let mut x0 = 0;
    for x1 in data.iter() {
        let delta = x1 - x0;
        vbyte_encode(delta, writer)?;
        x0 = *x1;
    }
    Ok(())
}

pub fn delta_decode<R>(reader: &mut R) -> Result<Vec<u64>, EncodableError>
where
    R: Read,
{
    let n = usize::decode(reader)?;
    let mut data = Vec::with_capacity(n);
    let mut x0 = 0;
    for _ in 0..n {
        let delta = vbyte_decode(reader)?;
        let x1 = delta + x0;
        data.push(x1);
        x0 = x1;
    }
    Ok(data)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn it_encodes_and_decodes_empty() {
        let data = Vec::<u64>::new();
        let mut buf = Vec::<u8>::new();
        delta_encode(&data, &mut buf).expect("Could not encode empty data vec");
        let decoded = delta_decode(&mut &buf[..]).expect("Could not decode empty data vec");
        assert_eq!(decoded.len(), 0);
    }

    #[test]
    fn it_encodes_and_decodes() {
        let mut data = Vec::<u64>::new();
        for i in 0..10 {
            data.push((i * 2) as u64);
        }
        let mut buf = Vec::<u8>::new();
        delta_encode(&data, &mut buf).expect("Could not encode data vec");
        let decoded = delta_decode(&mut &buf[..]).expect("Could not decode data vec");
        assert_eq!(decoded, data);
    }
}