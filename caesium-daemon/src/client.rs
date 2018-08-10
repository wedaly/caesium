use caesium_core::encode::{Encodable, EncodableError};
use caesium_core::protocol::messages::InsertMessage;
use std::io;
use std::io::Write;
use std::net::{SocketAddr, TcpStream};
use std::time::Duration;

const TIMEOUT_MS: u64 = 10000;

pub struct Client {
    addr: SocketAddr,
    buf: Vec<u8>,
    socket_opt: Option<TcpStream>,
}

impl Client {
    pub fn new(addr: SocketAddr) -> Client {
        Client {
            addr,
            buf: Vec::new(),
            socket_opt: None,
        }
    }

    pub fn send(&mut self, msg: &InsertMessage) -> Result<(), ClientError> {
        let socket = match self.socket_opt.take() {
            None => {
                let timeout = Duration::from_millis(TIMEOUT_MS);
                let s = TcpStream::connect_timeout(&self.addr, timeout)?;
                s.set_write_timeout(Some(timeout))?;
                s
            }
            Some(s) => s,
        };
        self.write_framed_msg(&msg, &socket)?;
        self.socket_opt = Some(socket);
        Ok(())
    }

    fn write_framed_msg(
        &mut self,
        msg: &InsertMessage,
        mut socket: &TcpStream,
    ) -> Result<(), ClientError> {
        self.buf.clear();
        msg.encode(&mut self.buf)?;
        let len = self.buf.len().to_be();
        len.encode(&mut socket)?;
        socket.write(&self.buf)?;
        Ok(())
    }
}

#[derive(Debug)]
pub enum ClientError {
    IOError(io::Error),
    EncodableError(EncodableError),
}

impl From<io::Error> for ClientError {
    fn from(err: io::Error) -> ClientError {
        ClientError::IOError(err)
    }
}

impl From<EncodableError> for ClientError {
    fn from(err: EncodableError) -> ClientError {
        ClientError::EncodableError(err)
    }
}
