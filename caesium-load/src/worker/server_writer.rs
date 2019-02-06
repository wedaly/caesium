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

pub struct ServerWriter {
    socket: TcpStream,
    rate_limiter: RateLimiter,
    frame_encoder: FrameEncoder,
    metric: String,
    window: TimeWindow,
    sketch: WritableSketch,
    tx: Sender<Event>,
    registered: bool,
    buf: Vec<u8>,
    num_written: usize,
}

impl ServerWriter {
    pub fn new(
        dst_addr: &SocketAddr,
        sketch_size: usize,
        rate_limit: Option<usize>,
        clock: &Clock,
        tx: Sender<Event>,
    ) -> Result<ServerWriter, io::Error> {
        let socket = TcpStream::connect(dst_addr)?;
        let rate_limiter = RateLimiter::new(rate_limit);
        let frame_encoder = FrameEncoder::new();
        let start_ts = clock.now();
        let metric = format!("caesium-load-{}", Uuid::new_v4());
        let window = TimeWindow::new(start_ts, start_ts + WINDOW_DURATION);
        let sketch = ServerWriter::build_sketch(sketch_size);
        Ok(ServerWriter {
            socket,
            rate_limiter,
            frame_encoder,
            metric,
            window,
            sketch,
            tx,
            buf: Vec::with_capacity(4096),
            registered: false,
            num_written: 0,
        })
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

    fn send_until_blocked(&mut self) -> Result<usize, io::Error> {
        let mut num_written = 0;
        let buf = &self.buf[self.num_written..];
        while num_written < buf.len() {
            match self.socket.write(buf) {
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
        if !self.registered {
            self.registered = true;
            poll.register(&self.socket, token, Ready::writable(), PollOpt::edge())
        } else {
            poll.reregister(&self.socket, token, Ready::writable(), PollOpt::edge())
        }
    }

    fn write(&mut self) -> Result<(), io::Error> {
        if !self.rate_limiter.is_within_limit() && self.buf.is_empty() {
            return Ok(());
        }

        if self.buf.is_empty() {
            self.fill_buffer();
        }

        self.num_written += self.send_until_blocked()?;
        if self.num_written == self.buf.len() {
            self.rate_limiter.increment();
            self.tx
                .send(Event::sketch_sent_event())
                .expect("Could not send insert sketch event");
            self.buf.clear();
            self.num_written = 0;
            self.window = TimeWindow::new(
                self.window.start() + WINDOW_DURATION,
                self.window.end() + WINDOW_DURATION,
            );
        }

        Ok(())
    }

    fn read(&mut self) -> Result<(), io::Error> {
        panic!("Server write worker did not register for read events!");
    }
}
