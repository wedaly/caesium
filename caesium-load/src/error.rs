use std::io;
use std::num::ParseIntError;

#[derive(Debug)]
pub enum Error {
    ParseIntError(ParseIntError),
    IOError(io::Error),
    ArgError(&'static str),
    ConfigError(&'static str),
}

impl From<ParseIntError> for Error {
    fn from(err: ParseIntError) -> Error {
        Error::ParseIntError(err)
    }
}

impl From<io::Error> for Error {
    fn from(err: io::Error) -> Error {
        Error::IOError(err)
    }
}
