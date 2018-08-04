use encode::{Decodable, Encodable, EncodableError};
use std::io::{Read, Write};
use std::mem::size_of;

macro_rules! build_encodable_int_type {
    ($type:ty) => {
        impl<W> Encodable<W> for $type
        where
            W: Write,
        {
            fn encode(&self, writer: &mut W) -> Result<(), EncodableError> {
                let mut bytes = [0u8; size_of::<$type>()];
                for i in 0..size_of::<$type>() {
                    bytes[i] = (self >> (i * 8)) as u8;
                }
                writer.write_all(&bytes)?;
                Ok(())
            }
        }

        impl<R> Decodable<$type, R> for $type
        where
            R: Read,
        {
            fn decode(reader: &mut R) -> Result<$type, EncodableError> {
                let mut bytes = [0u8; size_of::<$type>()];
                reader.read_exact(&mut bytes)?;

                let mut val: $type = 0;
                for i in 0..size_of::<$type>() {
                    val |= (bytes[i] as $type) << (i * 8);
                }

                Ok(val)
            }
        }
    };
}

build_encodable_int_type!(u8);
build_encodable_int_type!(u16);
build_encodable_int_type!(u32);
build_encodable_int_type!(u64);
build_encodable_int_type!(usize);

#[cfg(test)]
mod tests {
    use super::*;
    use std::io::ErrorKind;

    #[test]
    fn it_errors_if_not_enough_bytes() {
        let mut buf = Vec::new();
        let data = [0u8; 1];
        buf.write(&data).unwrap();
        match u64::decode(&mut &buf[..]) {
            Err(err) => match err {
                EncodableError::IOError(err) => assert_eq!(err.kind(), ErrorKind::UnexpectedEof),
                _ => panic!("Wrong error type"),
            },
            _ => panic!("Expected error"),
        }
    }

    #[test]
    fn it_encodes_and_decodes_u64() {
        let val: u64 = 0xFFEEDDCC;
        let mut buf = Vec::new();
        val.encode(&mut buf).unwrap();
        assert_eq!(u64::decode(&mut &buf[..]).unwrap(), val);
    }
}
