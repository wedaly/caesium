pub mod int;
pub mod string;

#[macro_use]
pub mod vec;

use std::io::Error as IOError;
use std::io::{Read, Write};
use std::string::FromUtf8Error;

#[derive(Debug)]
pub enum EncodableError {
    IOError(IOError),
    FromUtf8Error(FromUtf8Error),
    FormatError(&'static str),
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

pub trait Encodable<W>
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
