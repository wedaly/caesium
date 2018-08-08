extern crate clap;
extern crate mio;
extern crate rand;

use clap::{App, Arg};
use mio::{Events, Poll, PollOpt, Ready, Token};
use std::env;
use std::io;
use std::net::{SocketAddr, ToSocketAddrs};
use std::num::ParseIntError;
use std::time::Duration;
use writer::Writer;

fn main() -> Result<(), Error> {
    let args = parse_args()?;
    let poll = Poll::new()?;
    let mut writers = init_writers(&args.daemon_addr, args.num_writers, args.num_metrics)?;
    register_writers(&writers, &poll)?;
    println!(
        "Writing to daemon at {}, num_writers={}, num_metrics={}",
        args.daemon_addr, args.num_writers, args.num_metrics
    );
    run_event_loop(&poll, &mut writers)
}

fn init_writers(
    daemon_addr: &SocketAddr,
    num_writers: usize,
    num_metrics: usize,
) -> Result<Vec<Writer>, Error> {
    assert!(num_metrics > 0);
    let mut writers = Vec::with_capacity(num_writers);
    for i in 0..num_writers {
        let metric_id = if num_metrics <= num_writers {
            i % num_metrics
        } else {
            (num_metrics * i) / num_writers
        };
        let w = Writer::new(daemon_addr, metric_id, num_metrics)?;
        writers.push(w);
    }
    Ok(writers)
}

fn register_writers(writers: &[Writer], poll: &Poll) -> Result<(), Error> {
    for (idx, w) in writers.iter().enumerate() {
        let token = Token(idx);
        poll.register(w.socket(), token, Ready::writable(), PollOpt::edge())?;
    }
    Ok(())
}

fn run_event_loop(poll: &Poll, writers: &mut [Writer]) -> Result<(), Error> {
    let mut events = Events::with_capacity(1024);
    loop {
        poll.poll(&mut events, Some(Duration::from_millis(100)))?;
        for event in events.iter() {
            match event.token() {
                Token(t) if t < writers.len() => {
                    let w = writers.get_mut(t).expect("Could not retrieve writer");
                    w.write()?;
                }
                _ => unreachable!(),
            }
        }
    }
}

#[derive(Debug)]
struct Args {
    daemon_addr: SocketAddr,
    num_writers: usize,
    num_metrics: usize,
}

fn parse_args() -> Result<Args, Error> {
    let matches = App::new("Caesium writer")
        .about("Write metric data to the Caesium daemon")
        .arg(
            Arg::with_name("DAEMON_ADDR")
                .short("a")
                .long("addr")
                .takes_value(true)
                .help("IP address and port of the daemon (defaults to $CAESIUM_DAEMON_ADDR, then 127.0.0.1:8001)")
        )
        .arg(
            Arg::with_name("NUM_WRITERS")
            .long("num-writers")
            .short("n")
            .takes_value(true)
            .help("Number of concurrent writers (default 10)")
        )
        .arg(
            Arg::with_name("NUM_METRICS")
            .long("num-metrics")
            .short("m")
            .takes_value(true)
            .help("Number of distinct metrics (default 10)")
        )
        .get_matches();

    let default_addr =
        env::var("CAESIUM_DAEMON_ADDR").unwrap_or_else(|_| "127.0.0.1:8001".to_string());
    let daemon_addr = matches
        .value_of("DAEMON_ADDR")
        .unwrap_or(&default_addr)
        .to_socket_addrs()?
        .next()
        .ok_or(Error::ArgError("Expected socket address"))?;

    let num_writers = matches
        .value_of("NUM_WRITERS")
        .unwrap_or("10")
        .parse::<usize>()?;

    let num_metrics = matches
        .value_of("NUM_METRICS")
        .unwrap_or("10")
        .parse::<usize>()?;
    if num_metrics == 0 {
        return Err(Error::ArgError("NUM_METRICS must be > 0"));
    }

    Ok(Args {
        daemon_addr,
        num_writers,
        num_metrics,
    })
}

#[derive(Debug)]
enum Error {
    ParseIntError(ParseIntError),
    IOError(io::Error),
    ArgError(&'static str),
}

impl From<ParseIntError> for Error {
    fn from(err: ParseIntError) -> Error {
        Error::ParseIntError(err)
    }
}

impl From<io::Error> for Error {
    fn from(err: io::Error) -> Error {
        Error::IOError(err)
    }
}

mod writer {
    use mio::net::UdpSocket;
    use rand::rngs::SmallRng;
    use rand::{FromEntropy, Rng};
    use std::io;
    use std::net::SocketAddr;

    const MIN_VAL: u64 = 0;
    const MAX_VAL: u64 = 5000;

    pub struct Writer {
        dst_addr: SocketAddr,
        num_metrics: usize,
        metric_id: usize,
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
        ) -> Result<Writer, io::Error> {
            let dst_addr = dst_addr.clone();
            let addr: SocketAddr = "0.0.0.0:0".parse().unwrap();
            let w = Writer {
                dst_addr,
                num_metrics,
                metric_id,
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
            if self.buf.is_empty() {
                self.fill_buffer();
            }
            self.num_written += self.send_until_blocked()?;
            if self.num_written == self.buf.len() {
                self.reset();
            }
            Ok(())
        }

        fn fill_buffer(&mut self) {
            let value: u64 = self.rng.gen_range(MIN_VAL, MAX_VAL);
            let s = format!("caesium-writer.{}:{}|ms", self.metric_id, value);
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

        fn reset(&mut self) {
            self.buf.clear();
            self.num_written = 0;
            self.metric_id = (self.metric_id + 1) % self.num_metrics;
        }
    }
}
