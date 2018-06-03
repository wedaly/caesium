use std::io::Error as IOError;
use std::io::{Read, Write};
use std::mem::size_of;
use std::string::FromUtf8Error;

#[derive(Debug)]
pub enum EncodableError {
    IOError(IOError),
    FromUtf8Error(FromUtf8Error),
}

impl From<IOError> for EncodableError {
    fn from(err: IOError) -> EncodableError {
        EncodableError::IOError(err)
    }
}

impl From<FromUtf8Error> for EncodableError {
    fn from(err: FromUtf8Error) -> EncodableError {
        EncodableError::FromUtf8Error(err)
    }
}

pub trait Encodable<T, W>
where
    W: Write,
{
    fn encode(&self, &mut W) -> Result<(), EncodableError>;
}

pub trait Decodable<T, R>
where
    R: Read,
{
    fn decode(&mut R) -> Result<T, EncodableError>;
}

macro_rules! build_encodable_int_type {
    ($type:ty) => {
        impl<W> Encodable<$type, W> for $type
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

build_encodable_int_type!(u8);
build_encodable_int_type!(u16);
build_encodable_int_type!(u32);
build_encodable_int_type!(u64);
build_encodable_int_type!(usize);
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

impl<W> Encodable<String, W> for String
where
    W: Write,
{
    fn encode(&self, writer: &mut W) -> Result<(), EncodableError> {
        self.as_bytes().to_vec().encode(writer)
    }
}

impl<R> Decodable<String, R> for String
where
    R: Read,
{
    fn decode(reader: &mut R) -> Result<String, EncodableError> {
        let bytes = Vec::<u8>::decode(reader)?;
        String::from_utf8(bytes).map_err(From::from)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use encode::buffer::Buffer;
    use std::io::ErrorKind;

    #[test]
    fn it_errors_if_not_enough_bytes() {
        let mut buf = Buffer::new();
        let data = [0u8; 1];
        buf.write(&data).unwrap();
        match u64::decode(&mut buf) {
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
        let mut buf = Buffer::new();
        val.encode(&mut buf).unwrap();
        assert_eq!(u64::decode(&mut buf).unwrap(), val);
    }

    #[test]
    fn it_encodes_and_decodes_empty_string() {
        let s = String::new();
        let mut buf = Buffer::new();
        s.encode(&mut buf).unwrap();
        assert_eq!(String::decode(&mut buf).unwrap(), s);
    }

    #[test]
    fn it_encodes_and_decodes_nonempty_string() {
        let s = String::from("hello world");
        let mut buf = Buffer::new();
        s.encode(&mut buf).unwrap();
        assert_eq!(String::decode(&mut buf).unwrap(), s);
    }
}
