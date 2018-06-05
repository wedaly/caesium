use encode::{Decodable, Encodable, EncodableError};
use std::io::{Read, Write};

macro_rules! build_encodable_vec_type {
    ($type:ty) => {
        impl<W> Encodable<Vec<$type>, W> for Vec<$type>
        where
            W: Write,
        {
            fn encode(&self, writer: &mut W) -> Result<(), EncodableError> {
                let len = self.len() as u64;
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
                let len = u64::decode(reader)? as usize;
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

build_encodable_vec_type!(u8);
build_encodable_vec_type!(u16);
build_encodable_vec_type!(u32);
build_encodable_vec_type!(u64);
build_encodable_vec_type!(usize);
build_encodable_vec_type!(Vec<u8>);
build_encodable_vec_type!(Vec<u16>);
build_encodable_vec_type!(Vec<u32>);
build_encodable_vec_type!(Vec<u64>);
build_encodable_vec_type!(Vec<usize>);
