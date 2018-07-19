use quantile::writable::WritableSketch;
use query::error::QueryError;
use time::TimeWindow;

pub enum OpOutput {
    End,
    Sketch(TimeWindow, WritableSketch),
    Quantile(TimeWindow, Option<u64>),
}

pub trait QueryOp {
    // Each output MUST be returned in order by starting timestamp
    fn get_next(&mut self) -> Result<OpOutput, QueryError>;
}

pub mod coalesce;
pub mod combine;
pub mod fetch;
pub mod group;
pub mod quantile;
