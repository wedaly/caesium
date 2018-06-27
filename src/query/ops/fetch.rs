use query::error::QueryError;
use query::ops::{OpOutput, QueryOp};
use storage::datasource::{DataCursor, DataSource};
use time::TimeStamp;

pub struct FetchOp<'a> {
    cursor: Box<DataCursor + 'a>,
}

impl<'a> FetchOp<'a> {
    pub fn new(
        metric: String,
        source: &'a DataSource,
        start_ts: Option<TimeStamp>,
        end_ts: Option<TimeStamp>,
    ) -> Result<FetchOp<'a>, QueryError> {
        let cursor = source.fetch_range(&metric, start_ts, end_ts)?;
        let op = FetchOp { cursor };
        Ok(op)
    }
}

impl<'a> QueryOp for FetchOp<'a> {
    fn get_next(&mut self) -> Result<OpOutput, QueryError> {
        match self.cursor.get_next()? {
            None => Ok(OpOutput::End),
            Some(row) => Ok(OpOutput::Sketch(row.window, row.sketch)),
        }
    }
}
