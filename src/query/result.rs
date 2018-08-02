use encode::{Decodable, Encodable, EncodableError};
use quantile::readable::ApproxQuantile;
use std::io::{Read, Write};
use time::window::TimeWindow;

#[derive(Debug)]
pub enum QueryResult {
    QuantileWindow(TimeWindow, f64, ApproxQuantile),
    MetricName(String),
}

const QUANTILE_WINDOW_TYPE_ID: u8 = 1;
const METRIC_NAME_TYPE_ID: u8 = 2;

impl<W> Encodable<W> for QueryResult
where
    W: Write,
{
    fn encode(&self, mut writer: &mut W) -> Result<(), EncodableError> {
        match self {
            QueryResult::QuantileWindow(window, phi, quantile) => {
                QUANTILE_WINDOW_TYPE_ID.encode(writer)?;
                window.encode(&mut writer)?;
                phi.encode(&mut writer)?;
                quantile.encode(&mut writer)?;
            }
            QueryResult::MetricName(metric) => {
                METRIC_NAME_TYPE_ID.encode(writer)?;
                metric.encode(writer)?;
            }
        }
        Ok(())
    }
}

impl<R> Decodable<QueryResult, R> for QueryResult
where
    R: Read,
{
    fn decode(mut reader: &mut R) -> Result<QueryResult, EncodableError> {
        let type_id = u8::decode(reader)?;
        match type_id {
            QUANTILE_WINDOW_TYPE_ID => {
                let window = TimeWindow::decode(&mut reader)?;
                let phi = f64::decode(&mut reader)?;
                let quantile = ApproxQuantile::decode(&mut reader)?;
                Ok(QueryResult::QuantileWindow(window, phi, quantile))
            }
            METRIC_NAME_TYPE_ID => {
                let metric = String::decode(&mut reader)?;
                Ok(QueryResult::MetricName(metric))
            }
            _ => Err(EncodableError::FormatError("INvalid query result type")),
        }
    }
}

build_encodable_vec_type!(QueryResult);

#[cfg(tests)]
mod tests {
    use super::*;

    #[test]
    fn it_encodes_and_decodes_quantile_window() {
        let r = QuantileResult::QuantileWindow(
            TimeWindow::new(0, 30),
            0.5,
            ApproxQuantile {
                count: 7,
                lower_bound: 1,
                approx_value: 2,
                upper_bound: 3,
            },
        );

        let mut buf = Vec::<u8>::new();
        r.encode(&mut buf).expect("Could not encode result");
        let decoded = QuantileResult::decode(&mut &buf[..]).expect("Could not decode result");
        assert_eq!(r, decoded);
    }

    #[test]
    fn it_encodes_and_decodes_metric_name() {
        let r = QuantileResult::MetricName("foo".to_string());
        let mut buf = Vec::<u8>::new();
        r.encode(&mut buf).expect("Could not encode result");
        let decoded = QuantileResult::decode(&mut &buf[..]).expect("Could not decode result");
        assert_eq!(r, decoded);
    }
}
