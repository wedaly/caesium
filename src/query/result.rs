use encode::{Decodable, Encodable, EncodableError};
use std::io::{Read, Write};
use time::{TimeStamp, TimeWindow};

#[derive(Debug, Clone, Copy, PartialEq)]
pub struct QueryResult {
    pub window: TimeWindow,
    pub value: u64,
}

impl QueryResult {
    pub fn new(start: TimeStamp, end: TimeStamp, value: u64) -> QueryResult {
        let window = TimeWindow::new(start, end);
        QueryResult { window, value }
    }
}

impl<W> Encodable<W> for QueryResult
where
    W: Write,
{
    fn encode(&self, mut writer: &mut W) -> Result<(), EncodableError> {
        self.window.encode(&mut writer)?;
        self.value.encode(&mut writer)?;
        Ok(())
    }
}

impl<R> Decodable<QueryResult, R> for QueryResult
where
    R: Read,
{
    fn decode(mut reader: &mut R) -> Result<QueryResult, EncodableError> {
        let window = TimeWindow::decode(&mut reader)?;
        let value = u64::decode(&mut reader)?;
        Ok(QueryResult { window, value })
    }
}

build_encodable_vec_type!(QueryResult);
