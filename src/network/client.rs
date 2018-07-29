use encode::{Decodable, Encodable};
use network::error::NetworkError;
use network::message::Message;
use std::net::{Shutdown, SocketAddr, TcpStream};
use std::time::Duration;

pub struct Client {
    addr: SocketAddr,
}

impl Client {
    pub fn new(addr: SocketAddr) -> Client {
        Client { addr }
    }

    pub fn request(&self, req: &Message) -> Result<Message, NetworkError> {
        let timeout = Duration::new(10, 0);
        let mut conn = TcpStream::connect_timeout(&self.addr, timeout)?;
        req.encode(&mut conn)?;
        conn.shutdown(Shutdown::Write)?;
        let resp = Message::decode(&mut conn)?;
        Ok(resp)
    }
}
