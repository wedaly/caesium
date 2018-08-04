extern crate caesium_core;
extern crate caesium_daemon;
extern crate clap;
extern crate env_logger;

use caesium_core::network::error::NetworkError;
use caesium_daemon::run_daemon;
use clap::{App, Arg};
use std::net::{AddrParseError, SocketAddr, ToSocketAddrs};
use std::num::ParseIntError;
use std::io;

fn main() -> Result<(), Error> {
    env_logger::init();
    let args = parse_args()?;
    run_daemon(args.listen_addr, args.publish_addr, args.window_size)?;
    Ok(())
}

#[derive(Debug)]
struct Args {
    listen_addr: SocketAddr,
    publish_addr: SocketAddr,
    window_size: u64,
}

fn parse_args() -> Result<Args, Error> {
    let matches = App::new("Caesium daemon")
        .about("Collect and aggregate metric data, then send to backend server")
        .arg(
            Arg::with_name("LISTEN_ADDR")
                .long("listen-addr")
                .takes_value(true)
                .help("IP address and port to receive metric data (defaults to 127.0.0.1:8001)"),
        )
        .arg(
            Arg::with_name("PUBLISH_ADDR")
                .long("publish-addr")
                .takes_value(true)
                .help("IP address and port of backend server (defaults to 127.0.0.1:8000)"),
        )
        .arg(
            Arg::with_name("WINDOW_SIZE")
                .long("window-size")
                .short("w")
                .takes_value(true)
                .help("Size of aggregation windows in seconds (defaults to 30)"),
        )
        .get_matches();

    let listen_addr = matches
        .value_of("LISTEN_ADDR")
        .unwrap_or("127.0.0.1:8001")
        .to_socket_addrs()?
        .next()
        .ok_or(Error::ArgError("Expected socket address"))?;

    let publish_addr = matches
        .value_of("PUBLISH_ADDR")
        .unwrap_or("127.0.0.1:8000")
        .to_socket_addrs()?
        .next()
        .ok_or(Error::ArgError("Expected socket address"))?;

    let window_size = matches
        .value_of("WINDOW_SIZE")
        .unwrap_or("30")
        .parse::<u64>()?;

    if window_size < 1 {
        return Err(Error::ArgError("Window size must be >= 1"));
    }

    Ok(Args {
        listen_addr,
        publish_addr,
        window_size,
    })
}

#[derive(Debug)]
enum Error {
    AddrParseError(AddrParseError),
    ParseIntError(ParseIntError),
    IOError(io::Error),
    ArgError(&'static str),
    NetworkError(NetworkError),
}

impl From<AddrParseError> for Error {
    fn from(err: AddrParseError) -> Error {
        Error::AddrParseError(err)
    }
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

impl From<NetworkError> for Error {
    fn from(err: NetworkError) -> Error {
        Error::NetworkError(err)
    }
}
