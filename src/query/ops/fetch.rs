use query::error::QueryError;
use query::ops::{OpOutput, QueryOp};
use storage::datasource::{DataCursor, DataSource};

pub struct FetchOp<'a> {
    cursor: Box<DataCursor + 'a>,
}

impl<'a> FetchOp<'a> {
    pub fn new(metric: String, source: &'a DataSource) -> Result<FetchOp<'a>, QueryError> {
        let cursor = source.fetch_range(&metric, None, None)?;
        Ok(FetchOp { cursor })
    }
}

impl<'a> QueryOp for FetchOp<'a> {
    fn get_next(&mut self) -> Result<OpOutput, QueryError> {
        let next = self.cursor.get_next()?;
        match next {
            None => Ok(OpOutput::End),
            Some(row) => Ok(OpOutput::Sketch(row.range, row.sketch)),
        }
    }
}
