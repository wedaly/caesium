use encode::EncodableError;
use std::io;

#[derive(Debug)]
pub enum NetworkError {
    IOError(io::Error),
    EncodableError(EncodableError),
    ApplicationError(String),
}

impl From<io::Error> for NetworkError {
    fn from(err: io::Error) -> NetworkError {
        NetworkError::IOError(err)
    }
}

impl From<EncodableError> for NetworkError {
    fn from(err: EncodableError) -> NetworkError {
        NetworkError::EncodableError(err)
    }
}
