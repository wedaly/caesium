use query::error::QueryError;
use query::ops::{OpOutput, QueryOp};
use storage::datasource::{DataRow, DataSource};
use time::timestamp::TimeStamp;

pub struct FetchOp<'a> {
    row_iter: Box<Iterator<Item = DataRow> + 'a>,
}

impl<'a> FetchOp<'a> {
    pub fn new(
        metric: String,
        source: &'a DataSource,
        start_ts: Option<TimeStamp>,
        end_ts: Option<TimeStamp>,
    ) -> Result<FetchOp<'a>, QueryError> {
        let row_iter = source.fetch_range(&metric, start_ts, end_ts)?;
        Ok(FetchOp { row_iter })
    }
}

impl<'a> QueryOp for FetchOp<'a> {
    fn get_next(&mut self) -> Result<OpOutput, QueryError> {
        match self.row_iter.next() {
            None => Ok(OpOutput::End),
            Some(row) => Ok(OpOutput::Sketch(row.window, row.sketch)),
        }
    }
}
