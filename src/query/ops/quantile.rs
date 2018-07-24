use quantile::writable::WritableSketch;
use query::error::QueryError;
use query::ops::{OpOutput, QueryOp};
use std::collections::VecDeque;
use time::TimeWindow;

pub struct QuantileOp<'a> {
    input: Box<QueryOp + 'a>,
    phi_vec: Vec<f64>,
    output_queue: VecDeque<OpOutput>,
}

impl<'a> QuantileOp<'a> {
    pub fn new(input: Box<QueryOp + 'a>, phi_vec: Vec<f64>) -> Result<QuantileOp, QueryError> {
        for &phi in phi_vec.iter() {
            if phi <= 0.0 || phi >= 1.0 {
                return Err(QueryError::PhiOutOfRange(phi));
            }
        }
        Ok(QuantileOp {
            input,
            phi_vec,
            output_queue: VecDeque::new(),
        })
    }

    fn fill_output_queue(&mut self, window: TimeWindow, sketch: WritableSketch) {
        let readable = sketch.to_readable();
        for &phi in self.phi_vec.iter() {
            let quantile = readable.query(phi);
            let output = OpOutput::Quantile(window, phi, quantile);
            self.output_queue.push_back(output);
        }
    }
}

impl<'a> QueryOp for QuantileOp<'a> {
    fn get_next(&mut self) -> Result<OpOutput, QueryError> {
        if self.output_queue.is_empty() {
            match self.input.get_next()? {
                OpOutput::Sketch(window, sketch) => self.fill_output_queue(window, sketch),
                OpOutput::End => return Ok(OpOutput::End),
                _ => return Err(QueryError::InvalidInput),
            }
        }

        match self.output_queue.pop_front() {
            Some(output) => Ok(output),
            None => Ok(OpOutput::End),
        }
    }
}
