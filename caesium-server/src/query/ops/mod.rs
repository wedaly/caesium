use caesium_core::quantile::readable::ApproxQuantile;
use caesium_core::quantile::writable::WritableSketch;
use caesium_core::time::window::TimeWindow;
use query::error::QueryError;

pub enum OpOutput {
    End,
    Sketch(TimeWindow, WritableSketch),
    Quantile(TimeWindow, f64, Option<ApproxQuantile>),
    MetricName(String),
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
pub mod search;
