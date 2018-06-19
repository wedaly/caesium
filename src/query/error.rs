use encode::EncodableError;
use query::parser::parse::ParseError;
use storage::error::StorageError;

#[derive(Debug)]
pub enum QueryError {
    InvalidInput,
    InvalidExpressionType,
    UnrecognizedFunction(String),
    InvalidOutputType,
    MissingArg,
    InvalidArgType,
    PhiOutOfRange(f64),
    InvalidWindowSize(u64),
    EncodableError(EncodableError),
    ParseError(ParseError),
    StorageError(StorageError),
}

impl From<EncodableError> for QueryError {
    fn from(err: EncodableError) -> QueryError {
        QueryError::EncodableError(err)
    }
}

impl From<ParseError> for QueryError {
    fn from(err: ParseError) -> QueryError {
        QueryError::ParseError(err)
    }
}

impl From<StorageError> for QueryError {
    fn from(err: StorageError) -> QueryError {
        QueryError::StorageError(err)
    }
}
