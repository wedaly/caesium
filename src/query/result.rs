use encode::{Decodable, Encodable, EncodableError};
use quantile::readable::ApproxQuantile;
use std::io::{Read, Write};
use time::window::TimeWindow;

#[derive(Debug, Clone, Copy, PartialEq)]
pub struct QueryResult {
    window: TimeWindow,
    phi: f64,
    quantile: ApproxQuantile,
}

impl QueryResult {
    pub fn new(window: TimeWindow, phi: f64, quantile: ApproxQuantile) -> QueryResult {
        QueryResult {
            window,
            phi,
            quantile,
        }
    }

    pub fn window(&self) -> TimeWindow {
        self.window
    }

    pub fn phi(&self) -> f64 {
        self.phi
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
        self.phi.to_bits().encode(&mut writer)?;
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
        let phi = f64::from_bits(u64::decode(&mut reader)?);
        let quantile = ApproxQuantile::decode(&mut reader)?;
        Ok(QueryResult::new(window, phi, quantile))
    }
}

build_encodable_vec_type!(QueryResult);
