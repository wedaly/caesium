use encode::{Decodable, Encodable, EncodableError};
use quantile::serializable::SerializableSketch;
use query::result::QueryResult;
use std::io::{Read, Write};
use time::TimeStamp;

#[derive(Debug)]
pub enum Message {
    InsertReq {
        metric: String,
        ts: TimeStamp,
        sketch: SerializableSketch,
    },
    QueryReq(String),
    InsertSuccessResp,
    QuerySuccessResp(Vec<QueryResult>),
    ErrorResp(String),
}

const ERROR_RESP_MSG_TYPE: u8 = 127;

const INSERT_REQ_MSG_TYPE: u8 = 1;
const INSERT_SUCCESS_RESP_MSG_TYPE: u8 = 2;

const QUERY_REQ_MSG_TYPE: u8 = 3;
const QUERY_SUCCESS_RESP_MSG_TYPE: u8 = 4;

impl<W> Encodable<W> for Message
where
    W: Write,
{
    fn encode(&self, mut writer: &mut W) -> Result<(), EncodableError> {
        match self {
            Message::InsertReq { metric, ts, sketch } => {
                INSERT_REQ_MSG_TYPE.encode(&mut writer)?;
                metric.encode(&mut writer)?;
                ts.encode(&mut writer)?;
                sketch.encode(&mut writer)?;
            }
            Message::InsertSuccessResp => {
                INSERT_SUCCESS_RESP_MSG_TYPE.encode(&mut writer)?;
            }
            Message::QueryReq(q) => {
                QUERY_REQ_MSG_TYPE.encode(&mut writer)?;
                q.encode(&mut writer)?;
            }
            Message::QuerySuccessResp(results) => {
                QUERY_SUCCESS_RESP_MSG_TYPE.encode(&mut writer)?;
                results.encode(&mut writer)?;
            }
            Message::ErrorResp(err) => {
                ERROR_RESP_MSG_TYPE.encode(&mut writer)?;
                err.encode(&mut writer)?;
            }
        }
        Ok(())
    }
}

impl<R> Decodable<Message, R> for Message
where
    R: Read,
{
    fn decode(mut reader: &mut R) -> Result<Message, EncodableError> {
        let msg_type = u8::decode(&mut reader)?;
        match msg_type {
            INSERT_REQ_MSG_TYPE => {
                let metric = String::decode(&mut reader)?;
                let ts = TimeStamp::decode(&mut reader)?;
                let sketch = SerializableSketch::decode(&mut reader)?;
                Ok(Message::InsertReq { metric, ts, sketch })
            }
            INSERT_SUCCESS_RESP_MSG_TYPE => Ok(Message::InsertSuccessResp),
            QUERY_REQ_MSG_TYPE => {
                let q = String::decode(&mut reader)?;
                Ok(Message::QueryReq(q))
            }
            QUERY_SUCCESS_RESP_MSG_TYPE => {
                let results = Vec::<QueryResult>::decode(&mut reader)?;
                Ok(Message::QuerySuccessResp(results))
            }
            ERROR_RESP_MSG_TYPE => {
                let err = String::decode(&mut reader)?;
                Ok(Message::ErrorResp(err))
            }
            _ => Err(EncodableError::FormatError("Invalid message type")),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use quantile::block::Block;

    #[test]
    fn it_encodes_and_decodes_insert_msg() {
        let msg = Message::InsertReq {
            metric: "foo".to_string(),
            ts: 30_000,
            sketch: SerializableSketch::new(3, vec![Block::from_sorted_values(&vec![1, 2, 3])]),
        };
        let mut buf = Vec::new();
        msg.encode(&mut buf).expect("Could not encode insert msg");
        let decoded = Message::decode(&mut &buf[..]).expect("Could not decode insert msg");
        match decoded {
            Message::InsertReq { metric, ts, sketch } => {
                assert_eq!(metric, "foo");
                assert_eq!(ts, 30_000);
                assert_eq!(sketch.to_readable().size(), 3);
            }
            _ => panic!("Decoded wrong message type"),
        }
    }

    #[test]
    fn it_encodes_and_decodes_query_msg() {
        let msg = Message::QueryReq("quantile(0.5, fetch(foo))".to_string());
        let mut buf = Vec::new();
        msg.encode(&mut buf).expect("Could not encode query msg");
        let decoded = Message::decode(&mut &buf[..]).expect("Could not decode query msg");
        match decoded {
            Message::QueryReq(q) => assert_eq!(q, "quantile(0.5, fetch(foo))"),
            _ => panic!("Decoded wrong message type"),
        }
    }

    #[test]
    fn it_encodes_and_decodes_query_success_msg() {
        let results = vec![
            QueryResult::new(0, 30_000, 1),
            QueryResult::new(30_000, 60_000, 2),
        ];
        let msg = Message::QuerySuccessResp(results);
        let mut buf = Vec::new();
        msg.encode(&mut buf)
            .expect("Could not encode query result set msg");
        let decoded =
            Message::decode(&mut &buf[..]).expect("Could not decode query result set msg");
        match decoded {
            Message::QuerySuccessResp(results) => {
                assert_eq!(results.len(), 2);

                let first = results.get(0).unwrap();
                assert_eq!(first.range.start, 0);
                assert_eq!(first.range.end, 30_000);
                assert_eq!(first.value, 1);

                let second = results.get(1).unwrap();
                assert_eq!(second.range.start, 30_000);
                assert_eq!(second.range.end, 60_000);
                assert_eq!(second.value, 2);
            }
            _ => panic!("Decoded wrong message type"),
        }
    }
}