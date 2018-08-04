use caesium_core::quantile::writable::WritableSketch;
use caesium_core::time::timestamp::TimeStamp;
use caesium_core::time::window::TimeWindow;
use storage::error::StorageError;

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
