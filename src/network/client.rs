use encode::{Decodable, Encodable};
use network::error::NetworkError;
use network::message::Message;
use quantile::writable::WritableSketch;
use query::result::QueryResult;
use std::net::{Shutdown, SocketAddr, TcpStream};
use std::time::Duration;
use time::window::TimeWindow;

pub struct Client {
    addr: SocketAddr,
}

impl Client {
    pub fn new(addr: SocketAddr) -> Client {
        Client { addr }
    }

    pub fn insert(
        &mut self,
        metric: String,
        window: TimeWindow,
        sketch: WritableSketch,
    ) -> Result<(), NetworkError> {
        let req = Message::InsertReq {
            metric,
            window,
            sketch,
        };
        let resp = self.request(req)?;
        match resp {
            Message::InsertSuccessResp => Ok(()),
            msg => Client::handle_bad_resp(msg),
        }
    }

    pub fn query(&mut self, q: &str) -> Result<Vec<QueryResult>, NetworkError> {
        let req = Message::QueryReq(q.to_string());
        let resp = self.request(req)?;
        match resp {
            Message::QuerySuccessResp(results) => Ok(results),
            msg => Client::handle_bad_resp(msg),
        }
    }

    fn request(&self, req: Message) -> Result<Message, NetworkError> {
        let timeout = Duration::new(10, 0);
        let mut conn = TcpStream::connect_timeout(&self.addr, timeout)?;
        req.encode(&mut conn)?;
        conn.shutdown(Shutdown::Write)?;
        let resp = Message::decode(&mut conn)?;
        Ok(resp)
    }

    fn handle_bad_resp<T>(msg: Message) -> Result<T, NetworkError> {
        match msg {
            Message::ErrorResp(err) => Err(NetworkError::ApplicationError(err)),
            _ => Err(NetworkError::ApplicationError(
                "Unexpected response message type".to_string(),
            )),
        }
    }
}
