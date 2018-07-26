use encode::{Decodable, Encodable, EncodableError};
use quantile::writable::WritableSketch;
use std::cmp::{max, min};
use std::io::{Read, Write};
use storage::datasource::DataRow;
use time::window::TimeWindow;

pub struct StorageValue {
    window: TimeWindow,
    sketch: WritableSketch,
}

impl StorageValue {
    pub fn new(window: TimeWindow, sketch: WritableSketch) -> StorageValue {
        StorageValue { window, sketch }
    }

    pub fn as_bytes(window: TimeWindow, sketch: WritableSketch) -> Result<Vec<u8>, EncodableError> {
        let mut buf = Vec::new();
        let val = StorageValue::new(window, sketch);
        val.encode(&mut buf)?;
        Ok(buf)
    }

    pub fn to_bytes(&self) -> Result<Vec<u8>, EncodableError> {
        let mut buf = Vec::new();
        self.encode(&mut buf)?;
        Ok(buf)
    }

    pub fn to_data_row(self) -> DataRow {
        DataRow {
            window: self.window,
            sketch: self.sketch,
        }
    }

    pub fn merge(self, other: StorageValue) -> StorageValue {
        let start = min(self.window.start(), other.window.start());
        let end = max(self.window.end(), other.window.end());
        let window = TimeWindow::new(start, end);
        let sketch = self.sketch.merge(other.sketch);
        StorageValue::new(window, sketch)
    }
}

impl<W> Encodable<W> for StorageValue
where
    W: Write,
{
    fn encode(&self, writer: &mut W) -> Result<(), EncodableError> {
        self.window.encode(writer)?;
        self.sketch.encode(writer)?;
        Ok(())
    }
}

impl<R> Decodable<StorageValue, R> for StorageValue
where
    R: Read,
{
    fn decode(reader: &mut R) -> Result<StorageValue, EncodableError> {
        let window = TimeWindow::decode(reader)?;
        let sketch = WritableSketch::decode(reader)?;
        let val = StorageValue::new(window, sketch);
        Ok(val)
    }
}
