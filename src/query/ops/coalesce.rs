use quantile::mergable::MergableSketch;
use query::error::QueryError;
use query::ops::{OpOutput, QueryOp};
use std::cmp::{max, min};
use time::TimeWindow;

pub struct CoalesceOp<'a> {
    input: Box<QueryOp + 'a>,
    done: bool,
}

impl<'a> CoalesceOp<'a> {
    pub fn new(input: Box<QueryOp + 'a>) -> CoalesceOp {
        CoalesceOp {
            input: input,
            done: false,
        }
    }

    fn coalesce_inputs(&mut self) -> Result<OpOutput, QueryError> {
        let mut min_start = u64::max_value();
        let mut max_end = 0;
        let mut merged = MergableSketch::empty();

        loop {
            match self.input.get_next() {
                Ok(OpOutput::Sketch(window, sketch)) => {
                    min_start = min(min_start, window.start());
                    max_end = max(max_end, window.end());
                    merged.merge(&sketch);
                }
                Ok(OpOutput::End) => {
                    if merged.count() > 0 {
                        let window = TimeWindow::new(min_start, max_end);
                        return Ok(OpOutput::Sketch(window, merged));
                    } else {
                        return Ok(OpOutput::End);
                    }
                }
                Err(err) => {
                    return Err(err);
                }
                _ => {
                    return Err(QueryError::InvalidInput);
                }
            }
        }
    }
}

impl<'a> QueryOp for CoalesceOp<'a> {
    fn get_next(&mut self) -> Result<OpOutput, QueryError> {
        if self.done {
            Ok(OpOutput::End)
        } else {
            self.done = true;
            self.coalesce_inputs()
        }
    }
}
