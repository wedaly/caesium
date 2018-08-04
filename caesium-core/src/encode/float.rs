use encode::{Decodable, Encodable, EncodableError};
use std::io::{Read, Write};

macro_rules! build_encodable_float_type {
    ($ftype:ty, $itype:ty) => {
        impl<W> Encodable<W> for $ftype
        where
            W: Write,
        {
            fn encode(&self, writer: &mut W) -> Result<(), EncodableError> {
                self.to_bits().encode(writer)
            }
        }

        impl<R> Decodable<$ftype, R> for $ftype
        where
            R: Read,
        {
            fn decode(reader: &mut R) -> Result<$ftype, EncodableError> {
                let encoded_int = <$itype>::decode(reader)?;
                Ok(<$ftype>::from_bits(encoded_int))
            }
        }
    };
}

build_encodable_float_type!(f32, u32);
build_encodable_float_type!(f64, u64);

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn it_encodes_and_decodes_float() {
        let f: f64 = 1.2345;
        let mut buf = Vec::<u8>::new();
        f.encode(&mut buf).expect("Could not encode float");
        let decoded = f64::decode(&mut &buf[..]).expect("Could not decode float");
        assert_eq!(f, decoded);
    }
}
