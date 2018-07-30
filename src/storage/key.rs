use encode::{Decodable, Encodable, EncodableError};
use std::io::Read;
use time::timestamp::TimeStamp;

#[derive(Debug, Eq, PartialEq, Ord, PartialOrd)]
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

    pub fn with_window_start(self, window_start: TimeStamp) -> StorageKey {
        StorageKey {
            metric: self.metric,
            window_start,
        }
    }

    pub fn to_bytes(&self) -> Result<Vec<u8>, EncodableError> {
        StorageKey::as_bytes(&self.metric, self.window_start)
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

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn it_orders_by_metric_and_timestamp() {
        let mut keys: Vec<StorageKey> = vec![
            key(&"bcd", 2),
            key(&"a", 0),
            key(&"a", 1),
            key(&"aa", 1),
            key(&"bcd", 0),
            key(&"bcd", 3),
            key(&"aa", 0),
        ];
        keys.sort();
        assert_eq!(
            keys,
            vec![
                key(&"a", 0),
                key(&"a", 1),
                key(&"aa", 0),
                key(&"aa", 1),
                key(&"bcd", 0),
                key(&"bcd", 2),
                key(&"bcd", 3),
            ]
        );
    }

    fn key(metric: &str, window_start: TimeStamp) -> StorageKey {
        StorageKey {
            metric: metric.to_string(),
            window_start,
        }
    }
}
