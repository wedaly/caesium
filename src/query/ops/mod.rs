use quantile::mergable::MergableSketch;
use query::error::QueryError;
use time::TimeRange;

pub enum OpOutput {
    End,
    Sketch(TimeRange, MergableSketch),
    Quantile(TimeRange, Option<u64>),
}

pub trait QueryOp {
    fn get_next(&mut self) -> Result<OpOutput, QueryError>;
}

pub mod bucket;
pub mod fetch;
pub mod quantile;
