use encode::{Decodable, Encodable, EncodableError};
use std::io::{Read, Write};

const MAX_VEC_LEN: usize = 256000000; // 256 MB, should be enough for anything we need to encode

macro_rules! build_encodable_vec_type {
    ($type:ty) => {
        impl<W> Encodable<W> for Vec<$type>
        where
            W: Write,
        {
            fn encode(&self, writer: &mut W) -> Result<(), EncodableError> {
                let len = self.len();
                if len > MAX_VEC_LEN {
                    return Err(EncodableError::LengthTooLong(len));
                }

                len.encode(writer)?;
                for v in self.iter() {
                    v.encode(writer)?;
                }
                Ok(())
            }
        }

        impl<R> Decodable<Vec<$type>, R> for Vec<$type>
        where
            R: Read,
        {
            fn decode(reader: &mut R) -> Result<Vec<$type>, EncodableError> {
                let len = usize::decode(reader)?;
                if len > MAX_VEC_LEN {
                    return Err(EncodableError::LengthTooLong(len));
                }

                let mut result = Vec::<$type>::with_capacity(len);
                for _ in 0..len {
                    let v = <$type>::decode(reader)?;
                    result.push(v);
                }
                Ok(result)
            }
        }
    };
}

build_encodable_vec_type!(u32);
build_encodable_vec_type!(u64);
build_encodable_vec_type!(usize);

impl<W> Encodable<W> for Vec<u8>
where
    W: Write,
{
    fn encode(&self, writer: &mut W) -> Result<(), EncodableError> {
        let len = self.len();
        if len > MAX_VEC_LEN {
            return Err(EncodableError::LengthTooLong(len));
        }
        len.encode(writer)?;
        writer.write_all(&self)?;
        Ok(())
    }
}

impl<R> Decodable<Vec<u8>, R> for Vec<u8>
where
    R: Read,
{
    fn decode(reader: &mut R) -> Result<Vec<u8>, EncodableError> {
        let n = usize::decode(reader)?;
        if n > MAX_VEC_LEN {
            return Err(EncodableError::LengthTooLong(n));
        }
        let mut data = vec![0; n];
        reader.read_exact(&mut data)?;
        Ok(data)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn it_encodes_and_decodes_empty_u8_vec() {
        let mut buf = Vec::new();
        let data: Vec<u8> = vec![];
        data.encode(&mut buf)
            .expect("Could not encode empty Vec<u8>");
        let decoded = Vec::<u8>::decode(&mut &buf[..]).expect("Could not decode empty Vec<u8>");
        assert_eq!(decoded.len(), 0);
    }

    #[test]
    fn it_encodes_and_decodes_u8_vec() {
        let mut buf = Vec::new();
        let data = vec![1u8, 2u8, 3u8, 4u8, 5u8, 6u8, 7u8];
        data.encode(&mut buf).expect("Could not encode Vec<u8>");
        let decoded = Vec::<u8>::decode(&mut &buf[..]).expect("Could not decode Vec<u8>");
        assert_eq!(data, decoded);
    }

    #[test]
    fn it_rejects_decoding_vec_u8_len_greater_than_limit() {
        let buf = vec![0xFF; 20];
        if let Err(EncodableError::LengthTooLong(_)) = Vec::<u8>::decode(&mut &buf[..]) {
            // Expected error
        } else {
            assert!(false, "Expected error b/c length is too long");
        }
    }

    #[test]
    fn it_encodes_and_decodes_empty_u64_vec() {
        let mut buf = Vec::new();
        let data: Vec<u64> = vec![];
        data.encode(&mut buf)
            .expect("Could not encode empty Vec<u64>");
        let decoded = Vec::<u64>::decode(&mut &buf[..]).expect("Could not decode empty Vec<u64>");
        assert_eq!(decoded.len(), 0);
    }

    #[test]
    fn it_encodes_and_decodes_u64_vec() {
        let mut buf = Vec::new();
        let data = vec![1u64, 2u64, u64::max_value() - 1];
        data.encode(&mut buf).expect("Could not encode Vec<u64>");
        let decoded = Vec::<u64>::decode(&mut &buf[..]).expect("Could not decode Vec<u64>");
        assert_eq!(data, decoded);
    }

    #[test]
    fn it_rejects_decoding_vec_u64_len_greater_than_limit() {
        let buf = vec![0xFF; 20];
        if let Err(EncodableError::LengthTooLong(_)) = Vec::<u64>::decode(&mut &buf[..]) {
            // Expected error
        } else {
            assert!(false, "Expected error b/c length is too long");
        }
    }
}
