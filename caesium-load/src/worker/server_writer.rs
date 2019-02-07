use caesium_core::encode::frame::FrameEncoder;
use caesium_core::protocol::messages::InsertMessage;
use caesium_core::quantile::writable::WritableSketch;
use caesium_core::time::clock::Clock;
use caesium_core::time::window::TimeWindow;
use mio::net::TcpStream;
use mio::{Poll, PollOpt, Ready, Token};
use rand::rngs::SmallRng;
use rand::{FromEntropy, Rng};
use rate::RateLimiter;
use report::event::Event;
use std::io;
use std::io::Write;
use std::net::SocketAddr;
use std::sync::mpsc::Sender;
use uuid::Uuid;
use worker::Worker;

const WINDOW_DURATION: u64 = 10;
const MIN_VAL: u64 = 0;
const MAX_VAL: u64 = 10000;

enum ConnectionState {
    Connected(TcpStream),
    Writing(TcpStream, usize),
}

pub struct ServerWriter {
    dst_addr: SocketAddr,
    rate_limiter: RateLimiter,
    frame_encoder: FrameEncoder,
    metric: String,
    window: TimeWindow,
    sketch: WritableSketch,
    tx: Sender<Event>,
    buf: Vec<u8>,
    conn_state: Option<ConnectionState>,
}

impl ServerWriter {
    pub fn new(
        dst_addr: &SocketAddr,
        sketch_size: usize,
        rate_limit: Option<usize>,
        clock: &Clock,
        tx: Sender<Event>,
    ) -> ServerWriter {
        let rate_limiter = RateLimiter::new(rate_limit);
        let frame_encoder = FrameEncoder::new();
        let start_ts = clock.now();
        let metric = format!("caesium-load-{}", Uuid::new_v4());
        let window = TimeWindow::new(start_ts, start_ts + WINDOW_DURATION);
        let sketch = ServerWriter::build_sketch(sketch_size);
        ServerWriter {
            dst_addr: dst_addr.clone(),
            rate_limiter,
            frame_encoder,
            metric,
            window,
            sketch,
            tx,
            buf: Vec::with_capacity(4096),
            conn_state: None,
        }
    }

    fn build_sketch(size: usize) -> WritableSketch {
        let mut rng = SmallRng::from_entropy();
        let mut sketch = WritableSketch::new();
        for _ in 0..size {
            let v = rng.gen_range(MIN_VAL, MAX_VAL) as u32;
            sketch.insert(v);
        }
        sketch
    }

    fn fill_buffer(&mut self) {
        assert!(self.buf.is_empty());
        let msg = InsertMessage {
            window: self.window.clone(),
            metric: self.metric.clone(),
            sketch: self.sketch.clone(),
        };
        self.frame_encoder
            .encode_framed_msg(&msg, &mut self.buf)
            .expect("Could not encode framed insert message");
    }

    fn send_until_blocked(buf: &[u8], socket: &mut TcpStream) -> Result<usize, io::Error> {
        let mut num_written = 0;
        while num_written < buf.len() {
            match socket.write(buf) {
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
        Ok(num_written)
    }
}

impl Worker for ServerWriter {
    fn register(&mut self, token: Token, poll: &Poll) -> Result<(), io::Error> {
        self.conn_state = match self.conn_state.take() {
            None => {
                let s = TcpStream::connect(&self.dst_addr)?;
                poll.register(&s, token, Ready::writable(), PollOpt::edge())?;
                Some(ConnectionState::Connected(s))
            }
            Some(ConnectionState::Connected(s)) => {
                poll.reregister(&s, token, Ready::writable(), PollOpt::edge())?;
                if self.rate_limiter.is_within_limit() {
                    self.rate_limiter.increment();
                    self.buf.clear();
                    self.fill_buffer();
                    Some(ConnectionState::Writing(s, 0))
                } else {
                    Some(ConnectionState::Connected(s))
                }
            }
            Some(ConnectionState::Writing(s, num_written)) => {
                poll.reregister(&s, token, Ready::writable(), PollOpt::edge())?;
                Some(ConnectionState::Writing(s, num_written))
            }
        };
        Ok(())
    }

    fn write(&mut self) -> Result<(), io::Error> {
        self.conn_state = match self.conn_state.take() {
            Some(ConnectionState::Writing(mut s, mut num_written)) => {
                match ServerWriter::send_until_blocked(&self.buf[num_written..], &mut s) {
                    Ok(n) => {
                        num_written += n;
                        if num_written < self.buf.len() {
                            Some(ConnectionState::Writing(s, num_written))
                        } else {
                            self.tx
                                .send(Event::sketch_sent_event())
                                .expect("Could not send insert sketch event");
                            Some(ConnectionState::Connected(s))
                        }
                    }
                    Err(err) => {
                        error!("Error occurred while writing sketch, will attempt to re-establish the connection.  The error was {:?}", err);
                        self.tx
                            .send(Event::error_event())
                            .expect("Could not send error event");
                        None // Re-establish the connection
                    }
                }
            }
            Some(state) => Some(state),
            None => None,
        };
        Ok(())
    }

    fn read(&mut self) -> Result<(), io::Error> {
        panic!("Server write worker did not register for read events!");
    }
}
