use encode::EncodableError;
use rocksdb;

#[derive(Debug)]
pub enum StorageError {
    EncodableError(EncodableError),
    DatabaseError(rocksdb::Error),
    InvalidMetricName,
}

impl From<rocksdb::Error> for StorageError {
    fn from(err: rocksdb::Error) -> StorageError {
        StorageError::DatabaseError(err)
    }
}

impl From<EncodableError> for StorageError {
    fn from(err: EncodableError) -> StorageError {
        StorageError::EncodableError(err)
    }
}
