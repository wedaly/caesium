use encode::{Decodable, Encodable, EncodableError};
use std::io::{Read, Write};

const BYTE_MASK: u8 = 0x7F;
const CONTINUE_BIT: u8 = (1 << 7);

pub fn vbyte_encode<W>(mut v: u64, writer: &mut W) -> Result<(), EncodableError>
where
    W: Write,
{
    if v != 0 {
        while v > 0 {
            let mut b = v as u8 & BYTE_MASK;
            v = v >> 7;
            let continue_flag = ((v > 0) as u8) << 7;
            b |= continue_flag;
            b.encode(writer)?;
        }
    } else {
        0u8.encode(writer)?;
    }
    Ok(())
}

pub fn vbyte_decode<R>(reader: &mut R) -> Result<u64, EncodableError>
where
    R: Read,
{
    let mut out = 0u64;
    let mut continue_bit = true;
    let mut i = 0;
    while continue_bit && i < 10 {
        let b = u8::decode(reader)?;
        out |= ((b & BYTE_MASK) as u64) << (i * 7);
        continue_bit = (b & CONTINUE_BIT) > 0;
        i += 1;
    }
    Ok(out)
}

#[cfg(test)]
mod tests {
    use super::*;

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
        assert_vbyte((1 << 35) - 1);
        assert_vbyte(1 << 35);
        assert_vbyte((1 << 35) + 1);
        assert_vbyte((1 << 42) - 1);
        assert_vbyte(1 << 42);
        assert_vbyte((1 << 42) + 1);
        assert_vbyte((1 << 49) - 1);
        assert_vbyte(1 << 49);
        assert_vbyte((1 << 49) + 1);
        assert_vbyte((1 << 56) - 1);
        assert_vbyte(1 << 56);
        assert_vbyte((1 << 56) + 1);
        assert_vbyte((1 << 63) - 1);
        assert_vbyte(1 << 63);
        assert_vbyte((1 << 63) + 1);
        assert_vbyte((1 << 63) + 7);
    }

    #[test]
    fn it_decodes_overflow_vbyte() {
        let overflow_vbyte = [0xFF; 100];
        let result = vbyte_decode(&mut &overflow_vbyte[..]).unwrap();
        assert_eq!(result, u64::max_value());
    }

    fn assert_vbyte(input: u64) {
        let mut buf = Vec::<u8>::new();
        vbyte_encode(input, &mut buf).unwrap();
        let output = vbyte_decode(&mut &buf[..]).unwrap();
        println!("0x{:x} => {:x?} => 0x{:x}", input, &buf, output);
        assert_eq!(input, output);
    }
}
