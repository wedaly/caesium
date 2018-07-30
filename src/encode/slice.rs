use encode::{Encodable, EncodableError};
use std::io::Write;

macro_rules! build_encodable_slice_type {
    ($type:ty) => {
        impl<W> Encodable<W> for [$type]
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
    };
}

build_encodable_slice_type!(u8);
build_encodable_slice_type!(u16);
build_encodable_slice_type!(u32);
build_encodable_slice_type!(u64);
build_encodable_slice_type!(usize);

#[cfg(test)]
mod tests {
    use super::*;
    use encode::Decodable;

    #[test]
    fn it_encodes_as_slice_and_decodes_as_vec() {
        let data: Vec<u64> = vec![1, 2, 3, 4, 5];
        let data_slice = &data[..3];
        let mut buf = Vec::<u8>::new();
        data_slice.encode(&mut buf).expect("Could not encode slice");
        let decoded = Vec::<u64>::decode(&mut &buf[..]).expect("Could not decode slice");
        assert_eq!(decoded, vec![1, 2, 3]);
    }

}
