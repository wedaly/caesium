use quantile::mergable::MergableSketch;
use query::error::QueryError;
use time::TimeWindow;

pub enum OpOutput {
    End,
    Sketch(TimeWindow, MergableSketch),
    Quantile(TimeWindow, Option<u64>),
}

pub trait QueryOp {
    fn get_next(&mut self) -> Result<OpOutput, QueryError>;
}

pub mod bucket;
pub mod fetch;
pub mod quantile;
