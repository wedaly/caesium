use quantile::mergable::MergableSketch;
use query::error::QueryError;
use query::ops::{OpOutput, QueryOp};
use time::TimeRange;

pub struct QuantileOp<'a> {
    phi: f64,
    input: Box<QueryOp + 'a>,
}

impl<'a> QuantileOp<'a> {
    pub fn new(phi: f64, input: Box<QueryOp + 'a>) -> Result<QuantileOp, QueryError> {
        if phi <= 0.0 || phi >= 1.0 {
            Err(QueryError::PhiOutOfRange(phi))
        } else {
            Ok(QuantileOp { phi, input })
        }
    }

    fn query_sketch(
        &self,
        window: TimeRange,
        sketch: MergableSketch,
    ) -> Result<OpOutput, QueryError> {
        let quantile = sketch.to_readable().query(self.phi);
        Ok(OpOutput::Quantile(window, quantile))
    }
}

impl<'a> QueryOp for QuantileOp<'a> {
    fn get_next(&mut self) -> Result<OpOutput, QueryError> {
        match self.input.get_next() {
            Ok(OpOutput::Sketch(window, sketch)) => self.query_sketch(window, sketch),
            Ok(OpOutput::End) => Ok(OpOutput::End),
            Err(err) => Err(err),
            _ => Err(QueryError::InvalidInput),
        }
    }
}