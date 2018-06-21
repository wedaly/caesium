use quantile::mergable::MergableSketch;
use storage::error::StorageError;
use time::{TimeStamp, TimeWindow};

#[derive(Clone)]
pub struct DataRow {
    pub window: TimeWindow,
    pub sketch: MergableSketch,
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
