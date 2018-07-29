extern crate caesium;
extern crate clap;
extern crate env_logger;

use caesium::network::daemon::run_daemon;
use caesium::network::error::NetworkError;
use clap::{App, Arg};
use std::net::{AddrParseError, SocketAddr};
use std::num::ParseIntError;

fn main() -> Result<(), Error> {
    env_logger::init();
    let args = parse_args()?;
    run_daemon(args.source_addr, args.sink_addr, args.window_size)?;
    Ok(())
}

#[derive(Debug)]
struct Args {
    source_addr: SocketAddr,
    sink_addr: SocketAddr,
    window_size: u64,
}

fn parse_args() -> Result<Args, Error> {
    let matches = App::new("Caesium daemon")
        .about("Collect and aggregate metric data, then send to backend server")
        .arg(
            Arg::with_name("SOURCE_ADDR")
                .long("source-addr")
                .takes_value(true)
                .help("IP address and port to receive metric data (defaults to 127.0.0.1:8001)"),
        )
        .arg(
            Arg::with_name("SINK_ADDR")
                .long("sink-addr")
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

    let source_addr = matches
        .value_of("SOURCE_ADDR")
        .unwrap_or("127.0.0.1:8001")
        .parse::<SocketAddr>()?;

    let sink_addr = matches
        .value_of("SINK_ADDR")
        .unwrap_or("127.0.0.1:8000")
        .parse::<SocketAddr>()?;

    let window_size = matches
        .value_of("WINDOW_SIZE")
        .unwrap_or("30")
        .parse::<u64>()?;

    if window_size < 1 {
        return Err(Error::ArgError("Window size must be >= 1"))
    }

    Ok(Args {
        source_addr,
        sink_addr,
        window_size,
    })
}

#[derive(Debug)]
enum Error {
    AddrParseError(AddrParseError),
    ParseIntError(ParseIntError),
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

impl From<NetworkError> for Error {
    fn from(err: NetworkError) -> Error {
        Error::NetworkError(err)
    }
}
