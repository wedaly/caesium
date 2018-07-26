use encode::{Decodable, Encodable, EncodableError};
use std::io::Read;
use time::timestamp::TimeStamp;

#[derive(Debug)]
pub struct StorageKey {
    metric: String,
    window_start: TimeStamp,
}

impl StorageKey {
    // Encode directly to bytes to avoid overhead of copying the string into a struct field
    pub fn as_bytes(metric: &str, window_start: TimeStamp) -> Result<Vec<u8>, EncodableError> {
        let mut buf = Vec::new();
        metric.encode(&mut buf)?;
        window_start.encode(&mut buf)?;
        Ok(buf)
    }

    pub fn metric(&self) -> &str {
        &self.metric
    }

    pub fn window_start(&self) -> TimeStamp {
        self.window_start
    }
}

impl<R> Decodable<StorageKey, R> for StorageKey
where
    R: Read,
{
    fn decode(reader: &mut R) -> Result<StorageKey, EncodableError> {
        let metric = String::decode(reader)?;
        let window_start = TimeStamp::decode(reader)?;
        let key = StorageKey {
            metric,
            window_start,
        };
        Ok(key)
    }
}
