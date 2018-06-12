use storage::error::StorageError;
use time::{TimeRange, TimeStamp};

pub struct DataRow {
    pub range: TimeRange,
    pub bytes: Box<[u8]>,
}

pub trait DataCursor {
    fn get_next(&mut self) -> Result<Option<&DataRow>, StorageError>;
}

pub trait DataSource {
    fn fetch_range<'a>(
        &'a mut self,
        metric: &str,
        start: Option<TimeStamp>,
        end: Option<TimeStamp>,
    ) -> Result<Box<DataCursor + 'a>, StorageError>;
}
