use encode::{Decodable, Encodable, EncodableError};
use quantile::readable::ApproxQuantile;
use std::io::{Read, Write};
use time::TimeWindow;

#[derive(Debug, Clone, Copy, PartialEq)]
pub struct QueryResult {
    window: TimeWindow,
    quantile: ApproxQuantile,
}

impl QueryResult {
    pub fn new(window: TimeWindow, quantile: ApproxQuantile) -> QueryResult {
        QueryResult { window, quantile }
    }

    pub fn window(&self) -> TimeWindow {
        self.window
    }

    pub fn quantile(&self) -> ApproxQuantile {
        self.quantile
    }
}

impl<W> Encodable<W> for QueryResult
where
    W: Write,
{
    fn encode(&self, mut writer: &mut W) -> Result<(), EncodableError> {
        self.window.encode(&mut writer)?;
        self.quantile.encode(&mut writer)?;
        Ok(())
    }
}

impl<R> Decodable<QueryResult, R> for QueryResult
where
    R: Read,
{
    fn decode(mut reader: &mut R) -> Result<QueryResult, EncodableError> {
        let window = TimeWindow::decode(&mut reader)?;
        let quantile = ApproxQuantile::decode(&mut reader)?;
        Ok(QueryResult::new(window, quantile))
    }
}

build_encodable_vec_type!(QueryResult);
