use encode::{Decodable, Encodable, EncodableError};
use quantile::writable::WritableSketch;
use query::result::QueryResult;
use std::io::{Read, Write};
use time::TimeWindow;

#[derive(Debug)]
pub enum Message {
    InsertReq {
        metric: String,
        window: TimeWindow,
        sketch: WritableSketch,
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
            Message::InsertReq {
                metric,
                window,
                sketch,
            } => {
                INSERT_REQ_MSG_TYPE.encode(&mut writer)?;
                metric.encode(&mut writer)?;
                window.encode(&mut writer)?;
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
                let window = TimeWindow::decode(&mut reader)?;
                let sketch = WritableSketch::decode(&mut reader)?;
                Ok(Message::InsertReq {
                    metric,
                    window,
                    sketch,
                })
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
    use quantile::readable::ApproxQuantile;

    #[test]
    fn it_encodes_and_decodes_insert_msg() {
        let msg = Message::InsertReq {
            metric: "foo".to_string(),
            window: TimeWindow::new(2, 3),
            sketch: WritableSketch::new(),
        };
        let mut buf = Vec::new();
        msg.encode(&mut buf).expect("Could not encode insert msg");
        let decoded = Message::decode(&mut &buf[..]).expect("Could not decode insert msg");
        match decoded {
            Message::InsertReq {
                metric,
                window,
                sketch,
            } => {
                assert_eq!(metric, "foo");
                assert_eq!(window.start(), 2);
                assert_eq!(window.end(), 3);
                assert_eq!(sketch.size(), 0);
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
            QueryResult::new(
                TimeWindow::new(0, 30),
                ApproxQuantile {
                    approx_value: 1,
                    lower_bound: 0,
                    upper_bound: 2,
                },
            ),
            QueryResult::new(
                TimeWindow::new(30, 60),
                ApproxQuantile {
                    approx_value: 2,
                    lower_bound: 1,
                    upper_bound: 5,
                },
            ),
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
                assert_eq!(first.window().start(), 0);
                assert_eq!(first.window().end(), 30);
                assert_eq!(first.quantile().approx_value, 1);
                assert_eq!(first.quantile().lower_bound, 0);
                assert_eq!(first.quantile().upper_bound, 2);

                let second = results.get(1).unwrap();
                assert_eq!(second.window().start(), 30);
                assert_eq!(second.window().end(), 60);
                assert_eq!(second.quantile().approx_value, 2);
                assert_eq!(second.quantile().lower_bound, 1);
                assert_eq!(second.quantile().upper_bound, 5);
            }
            _ => panic!("Decoded wrong message type"),
        }
    }
}
