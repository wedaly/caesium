use quantile::writable::WritableSketch;
use storage::error::StorageError;
use time::timestamp::TimeStamp;
use time::window::TimeWindow;

#[derive(Clone)]
pub struct DataRow {
    pub window: TimeWindow,
    pub sketch: WritableSketch,
}

pub trait DataCursor {
    fn get_next(&mut self) -> Result<Option<DataRow>, StorageError>;
}

pub trait DataSource {
    fn fetch_range<'a>(
        &'a self,
        metric: &str,
        start: Option<TimeStamp>,
        end: Option<TimeStamp>,
    ) -> Result<Box<DataCursor + 'a>, StorageError>;
}
