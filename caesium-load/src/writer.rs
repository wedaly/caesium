use mio::net::UdpSocket;
use rand::rngs::SmallRng;
use rand::{FromEntropy, Rng};
use rate::RateLimiter;
use std::io;
use std::net::SocketAddr;

const MIN_VAL: u64 = 0;
const MAX_VAL: u64 = 5000;

pub struct Writer {
    dst_addr: SocketAddr,
    metric_id: usize,
    num_metrics: usize,
    rate_limiter: RateLimiter,
    socket: UdpSocket,
    buf: Vec<u8>,
    num_written: usize,
    rng: SmallRng,
}

impl Writer {
    pub fn new(
        dst_addr: &SocketAddr,
        metric_id: usize,
        num_metrics: usize,
        rate_limit: Option<usize>,
    ) -> Result<Writer, io::Error> {
        let dst_addr = dst_addr.clone();
        let addr: SocketAddr = "0.0.0.0:0".parse().unwrap();
        let rate_limiter = RateLimiter::new(rate_limit);
        let w = Writer {
            dst_addr,
            metric_id,
            num_metrics,
            rate_limiter,
            socket: UdpSocket::bind(&addr)?,
            buf: Vec::new(),
            num_written: 0,
            rng: SmallRng::from_entropy(),
        };
        Ok(w)
    }

    pub fn socket(&self) -> &UdpSocket {
        &self.socket
    }

    pub fn write(&mut self) -> Result<(), io::Error> {
        if !self.rate_limiter.is_within_limit() && self.buf.is_empty() {
            return Ok(());
        }

        if self.buf.is_empty() {
            self.fill_buffer();
        }

        self.num_written += self.send_until_blocked()?;
        if self.num_written == self.buf.len() {
            self.rate_limiter.increment();
            self.buf.clear();
            self.num_written = 0;
            self.metric_id = (self.metric_id + 1) % self.num_metrics;
        }

        Ok(())
    }

    fn fill_buffer(&mut self) {
        let value: u64 = self.rng.gen_range(MIN_VAL, MAX_VAL);
        let s = format!("caesium-load.{}:{}|ms", self.metric_id, value);
        self.buf.extend_from_slice(s.as_bytes());
    }

    fn send_until_blocked(&self) -> Result<usize, io::Error> {
        let mut num_written = 0;
        let buf = &self.buf[self.num_written..];
        while num_written < buf.len() {
            match self.socket.send_to(buf, &self.dst_addr) {
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
