use quantile::mergable::MergableSketch;
use query::error::QueryError;
use time::TimeWindow;

pub enum OpOutput {
    End,
    Sketch(TimeWindow, MergableSketch),
    Quantile(TimeWindow, Option<u64>),
}

pub trait QueryOp {
    // Each output MUST be returned in order by starting timestamp
    fn get_next(&mut self) -> Result<OpOutput, QueryError>;
}

pub mod coalesce;
pub mod combine;
pub mod fetch;
pub mod quantile;
