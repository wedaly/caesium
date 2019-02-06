pub mod daemon_writer;
pub mod server_reader;
pub mod server_writer;

use mio::{Poll, Token};
use std::io;

pub trait Worker {
    fn register(&mut self, token: Token, poll: &Poll) -> Result<(), io::Error>;
    fn write(&mut self) -> Result<(), io::Error>;
    fn read(&mut self) -> Result<(), io::Error>;
}
