use caesium_core::encode::frame::FrameEncoder;
use caesium_core::encode::EncodableError;
use caesium_core::protocol::messages::InsertMessage;
use std::io;
use std::net::{SocketAddr, TcpStream};
use std::time::Duration;

const TIMEOUT_MS: u64 = 10000;

pub struct Client {
    addr: SocketAddr,
    socket_opt: Option<TcpStream>,
    frame_encoder: FrameEncoder,
}

impl Client {
    pub fn new(addr: SocketAddr) -> Client {
        Client {
            addr,
            socket_opt: None,
            frame_encoder: FrameEncoder::new(),
        }
    }

    pub fn send(&mut self, msg: &InsertMessage) -> Result<(), ClientError> {
        let mut socket = match self.socket_opt.take() {
            None => {
                let timeout = Duration::from_millis(TIMEOUT_MS);
                let s = TcpStream::connect_timeout(&self.addr, timeout)?;
                s.set_write_timeout(Some(timeout))?;
                s
            }
            Some(s) => s,
        };
        self.frame_encoder.encode_framed_msg(msg, &mut socket)?;
        self.socket_opt = Some(socket);
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
