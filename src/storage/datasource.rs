use quantile::writable::WritableSketch;
use storage::error::StorageError;
use time::timestamp::TimeStamp;
use time::window::TimeWindow;

#[derive(Clone)]
pub struct DataRow {
    pub window: TimeWindow,
    pub sketch: WritableSketch,
}

pub trait DataSource {
    fn fetch<'a>(
        &'a self,
        metric: String,
        start: Option<TimeStamp>,
        end: Option<TimeStamp>,
    ) -> Result<Box<Iterator<Item = DataRow> + 'a>, StorageError>;

    fn search<'a>(
        &'a self,
        pattern: String,
    ) -> Result<Box<Iterator<Item = String> + 'a>, StorageError>;
}
