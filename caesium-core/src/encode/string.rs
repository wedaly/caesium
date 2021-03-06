use encode::{Decodable, Encodable, EncodableError};
use std::io::{Read, Write};

impl<W> Encodable<W> for str
where
    W: Write,
{
    fn encode(&self, writer: &mut W) -> Result<(), EncodableError> {
        self.as_bytes().encode(writer)
    }
}

impl<W> Encodable<W> for String
where
    W: Write,
{
    fn encode(&self, writer: &mut W) -> Result<(), EncodableError> {
        self.as_bytes().encode(writer)
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

    #[test]
    fn it_encodes_and_decodes_empty_string() {
        let s = String::new();
        let mut buf = Vec::new();
        s.encode(&mut buf).unwrap();
        assert_eq!(String::decode(&mut &buf[..]).unwrap(), s);
    }

    #[test]
    fn it_encodes_and_decodes_nonempty_string() {
        let s = String::from("hello world");
        let mut buf = Vec::new();
        s.encode(&mut buf).unwrap();
        assert_eq!(String::decode(&mut &buf[..]).unwrap(), s);
    }

    #[test]
    fn it_encodes_string_ref() {
        let s = String::from("foobar");
        let buf = encode_str_ref(&s);
        assert_eq!(String::decode(&mut &buf[..]).unwrap(), s);
    }

    fn encode_str_ref(s: &str) -> Vec<u8> {
        let mut buf = Vec::new();
        s.encode(&mut buf).unwrap();
        buf
    }
}
