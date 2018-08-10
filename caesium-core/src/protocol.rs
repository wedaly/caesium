pub mod messages {
    use encode::{Decodable, Encodable, EncodableError};
    use quantile::writable::WritableSketch;
    use std::io::{Read, Write};
    use time::window::TimeWindow;

    pub struct InsertMessage {
        pub metric: String,
        pub window: TimeWindow,
        pub sketch: WritableSketch,
    }

    impl<W> Encodable<W> for InsertMessage
    where
        W: Write,
    {
        fn encode(&self, writer: &mut W) -> Result<(), EncodableError> {
            self.metric.encode(writer)?;
            self.window.encode(writer)?;
            self.sketch.encode(writer)?;
            Ok(())
        }
    }

    impl<R> Decodable<InsertMessage, R> for InsertMessage
    where
        R: Read,
    {
        fn decode(mut reader: &mut R) -> Result<InsertMessage, EncodableError> {
            let metric = String::decode(&mut reader)?;
            let window = TimeWindow::decode(&mut reader)?;
            let sketch = WritableSketch::decode(&mut reader)?;
            Ok(InsertMessage {
                metric,
                window,
                sketch,
            })
        }
    }

    #[cfg(test)]
    mod tests {
        use super::*;

        #[test]
        fn it_encodes_and_decodes_insert_msg() {
            let msg = InsertMessage {
                metric: "foo".to_string(),
                window: TimeWindow::new(2, 3),
                sketch: WritableSketch::new(),
            };
            let mut buf = Vec::new();
            msg.encode(&mut buf).expect("Could not encode insert msg");
            let decoded =
                InsertMessage::decode(&mut &buf[..]).expect("Could not decode insert msg");
            assert_eq!(decoded.metric, "foo");
            assert_eq!(decoded.window.start(), 2);
            assert_eq!(decoded.window.end(), 3);
            assert_eq!(decoded.sketch.size(), 0);
        }
    }
}
