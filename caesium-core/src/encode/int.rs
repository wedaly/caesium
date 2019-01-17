use encode::{Decodable, Encodable, EncodableError};
use std::io::{Read, Write};
use std::mem::size_of;

impl<W> Encodable<W> for u8
where
    W: Write,
{
    fn encode(&self, writer: &mut W) -> Result<(), EncodableError> {
        writer.write_all(&[*self]).map_err(From::from)
    }
}

impl<R> Decodable<u8, R> for u8
where
    R: Read,
{
    fn decode(reader: &mut R) -> Result<u8, EncodableError> {
        let mut buf = [0u8];
        reader.read_exact(&mut buf)?;
        Ok(buf[0])
    }
}

impl<W> Encodable<W> for u32
where
    W: Write,
{
    fn encode(&self, writer: &mut W) -> Result<(), EncodableError> {
        writer.write_all(&self.to_le_bytes()).map_err(From::from)
    }
}

impl<R> Decodable<u32, R> for u32
where
    R: Read,
{
    fn decode(reader: &mut R) -> Result<u32, EncodableError> {
        let mut buf = [0u8; 4];
        reader.read_exact(&mut buf)?;
        Ok(u32::from_le_bytes(buf))
    }
}

impl<W> Encodable<W> for u64
where
    W: Write,
{
    fn encode(&self, writer: &mut W) -> Result<(), EncodableError> {
        writer.write_all(&self.to_le_bytes()).map_err(From::from)
    }
}

impl<R> Decodable<u64, R> for u64
where
    R: Read,
{
    fn decode(reader: &mut R) -> Result<u64, EncodableError> {
        let mut buf = [0u8; 8];
        reader.read_exact(&mut buf)?;
        Ok(u64::from_le_bytes(buf))
    }
}

impl<W> Encodable<W> for usize
where
    W: Write,
{
    fn encode(&self, writer: &mut W) -> Result<(), EncodableError> {
        debug_assert!(size_of::<usize>() <= size_of::<u64>());
        writer
            .write_all(&((*self as u64).to_le_bytes()))
            .map_err(From::from)
    }
}

impl<R> Decodable<usize, R> for usize
where
    R: Read,
{
    fn decode(reader: &mut R) -> Result<usize, EncodableError> {
        debug_assert!(size_of::<usize>() <= size_of::<u64>());
        let mut buf = [0u8; 8];
        reader.read_exact(&mut buf)?;
        Ok((u64::from_le_bytes(buf)) as usize)
    }
}

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
