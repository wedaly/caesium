use query::error::QueryError;
use query::ops::{OpOutput, QueryOp};
use storage::datasource::DataSource;

pub struct SearchOp<'a> {
    metric_iter: Box<Iterator<Item = String> + 'a>,
}

impl<'a> SearchOp<'a> {
    pub fn new(pattern: String, source: &'a DataSource) -> Result<SearchOp<'a>, QueryError> {
        let metric_iter = source.search(pattern)?;
        Ok(SearchOp { metric_iter })
    }
}

impl<'a> QueryOp for SearchOp<'a> {
    fn get_next(&mut self) -> Result<OpOutput, QueryError> {
        match self.metric_iter.next() {
            None => Ok(OpOutput::End),
            Some(metric) => Ok(OpOutput::MetricName(metric)),
        }
    }
}
