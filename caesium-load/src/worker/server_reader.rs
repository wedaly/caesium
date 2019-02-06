use mio::net::TcpStream;
use mio::{Poll, PollOpt, Ready, Token};
use rate::RateLimiter;
use report::event::Event;
use std::io;
use std::io::{Read, Write};
use std::net::{Shutdown, SocketAddr};
use std::sync::mpsc::Sender;
use worker::Worker;

enum State {
    Connected(TcpStream),
    Writing(TcpStream, usize),
    Reading(TcpStream),
}

pub struct ServerReader {
    id: usize,
    dst_addr: SocketAddr,
    rate_limiter: RateLimiter,
    queries: Vec<String>,
    query_idx: usize,
    state: Option<State>,
    tx: Sender<Event>,
}

impl ServerReader {
    pub fn new(
        id: usize,
        dst_addr: &SocketAddr,
        queries_slice: &[String],
        query_idx: usize,
        rate_limit: Option<usize>,
        tx: Sender<Event>,
    ) -> ServerReader {
        assert!(queries_slice.len() > 0);
        assert!(query_idx < queries_slice.len());
        let dst_addr = dst_addr.clone();
        let mut queries = Vec::with_capacity(queries_slice.len());
        queries.extend_from_slice(queries_slice);
        let rate_limiter = RateLimiter::new(rate_limit);
        ServerReader {
            id,
            dst_addr,
            queries,
            query_idx,
            rate_limiter,
            state: None,
            tx,
        }
    }

    fn write_until_done_or_blocked(
        s: &mut TcpStream,
        mut num_written: usize,
        buf: &[u8],
    ) -> Result<usize, io::Error> {
        while num_written < buf.len() {
            match s.write(&buf[num_written..]) {
                Ok(n) => {
                    num_written += n;
                }
                Err(err) => {
                    if let io::ErrorKind::WouldBlock = err.kind() {
                        break;
                    } else {
                        return Err(err);
                    }
                }
            }
        }
        return Ok(num_written);
    }

    fn read_until_done_or_blocked(s: &mut TcpStream) -> Result<bool, io::Error> {
        let mut buf = [0; 1024];
        let mut is_done = false;
        loop {
            match s.read(&mut buf) {
                Ok(n) if n == 0 => {
                    is_done = true;
                    break;
                }
                Ok(_) => {}
                Err(err) => {
                    if let io::ErrorKind::WouldBlock = err.kind() {
                        break;
                    } else {
                        return Err(err);
                    }
                }
            }
        }
        Ok(is_done)
    }
}

impl Worker for ServerReader {
    fn register(&mut self, token: Token, poll: &Poll) -> Result<(), io::Error> {
        self.state = match self.state.take() {
            None => {
                let s = TcpStream::connect(&self.dst_addr)?;
                poll.register(&s, token, Ready::writable(), PollOpt::edge())?;
                Some(State::Connected(s))
            }
            Some(State::Connected(s)) => {
                poll.reregister(&s, token, Ready::writable(), PollOpt::edge())?;
                if self.rate_limiter.is_within_limit() {
                    self.rate_limiter.increment();
                    Some(State::Writing(s, 0))
                } else {
                    Some(State::Connected(s))
                }
            }
            Some(State::Writing(s, num_written)) => {
                poll.reregister(&s, token, Ready::writable(), PollOpt::edge())?;
                Some(State::Writing(s, num_written))
            }
            Some(State::Reading(s)) => {
                poll.reregister(&s, token, Ready::readable(), PollOpt::edge())?;
                Some(State::Reading(s))
            }
        };
        Ok(())
    }

    fn write(&mut self) -> Result<(), io::Error> {
        self.state = match self.state.take() {
            Some(State::Writing(mut s, mut num_written)) => {
                let query_buf = self.queries[self.query_idx].as_bytes();
                num_written =
                    ServerReader::write_until_done_or_blocked(&mut s, num_written, query_buf)?;
                if num_written < query_buf.len() {
                    Some(State::Writing(s, num_written))
                } else {
                    s.shutdown(Shutdown::Write)?;
                    self.tx
                        .send(Event::query_sent_event(self.id, self.query_idx))
                        .expect("Could not send query sent event to channel");
                    Some(State::Reading(s))
                }
            }
            Some(state) => Some(state),
            None => None,
        };
        Ok(())
    }

    fn read(&mut self) -> Result<(), io::Error> {
        self.state = match self.state.take() {
            Some(State::Reading(mut s)) => {
                let is_done = ServerReader::read_until_done_or_blocked(&mut s)?;
                self.tx
                    .send(Event::query_bytes_received_event(self.id, self.query_idx))
                    .expect("Could not send query bytes received event to channel");
                if is_done {
                    self.query_idx = (self.query_idx + 1) % self.queries.len();
                    None
                } else {
                    Some(State::Reading(s))
                }
            }
            Some(state) => Some(state),
            None => None,
        };
        Ok(())
    }
}
