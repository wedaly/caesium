use encode::Decodable;
use quantile::mergable::MergableSketch;
use quantile::serializable::SerializableSketch;
use query::error::QueryError;
use query::ops::{OpOutput, QueryOp};
use storage::datasource::{DataCursor, DataRow, DataSource};

pub struct FetchOp<'a> {
    cursor: Box<DataCursor + 'a>,
}

impl<'a> FetchOp<'a> {
    pub fn new(metric: String, source: &'a mut DataSource) -> Result<FetchOp<'a>, QueryError> {
        let cursor = source.fetch_range(&metric, None, None)?;
        Ok(FetchOp { cursor })
    }

    pub fn deserialize_row(row: &DataRow) -> Result<OpOutput, QueryError> {
        let sketch = FetchOp::deserialize_row_value(&row.bytes)?;
        Ok(OpOutput::Sketch(row.range, sketch))
    }

    fn deserialize_row_value(mut bytes: &[u8]) -> Result<MergableSketch, QueryError> {
        let sketch = SerializableSketch::decode(&mut bytes)?;
        Ok(sketch.to_mergable())
    }
}

impl<'a> QueryOp for FetchOp<'a> {
    fn get_next(&mut self) -> Result<OpOutput, QueryError> {
        let next = self.cursor.get_next()?;
        match next {
            None => Ok(OpOutput::End),
            Some(row) => FetchOp::deserialize_row(&row),
        }
    }
}
