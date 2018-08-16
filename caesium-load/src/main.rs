extern crate caesium_load;
extern crate clap;

use caesium_load::generate_load;
use clap::{App, Arg};
use std::io;
use std::net::{SocketAddr, ToSocketAddrs};
use std::num::ParseIntError;

fn main() -> Result<(), Error> {
    let args = parse_args()?;
    println!(
        "Writing to daemon at {}, num_writers={}, num_metrics={}, rate_limit={:?}",
        args.daemon_addr, args.num_writers, args.num_metrics, args.rate_limit
    );
    generate_load(
        &args.daemon_addr,
        args.num_writers,
        args.num_metrics,
        args.rate_limit,
    ).map_err(From::from)
}

#[derive(Debug)]
struct Args {
    daemon_addr: SocketAddr,
    num_writers: usize,
    num_metrics: usize,
    rate_limit: Option<usize>,
}

fn parse_args() -> Result<Args, Error> {
    let matches = App::new("Caesium writer")
        .about("Write metric data to the Caesium daemon")
        .arg(
            Arg::with_name("DAEMON_ADDR")
                .long("daemon-addr")
                .takes_value(true)
                .help("IP address and port of the daemon (defaults to 127.0.0.1:8001)"),
        )
        .arg(
            Arg::with_name("NUM_WRITERS")
                .long("num-writers")
                .takes_value(true)
                .help("Number of concurrent writers (default 1)"),
        )
        .arg(
            Arg::with_name("NUM_METRICS")
                .long("num-metrics")
                .short("m")
                .takes_value(true)
                .help("Number of distinct metrics (default 10)"),
        )
        .arg(
            Arg::with_name("RATE_LIMIT")
                .long("rate-limit")
                .short("r")
                .takes_value(true)
                .help("Maximum number of inserts per second per worker (default 1)"),
        )
        .get_matches();

    let daemon_addr = matches
        .value_of("DAEMON_ADDR")
        .unwrap_or(&"127.0.0.1:8001".to_string())
        .to_socket_addrs()?
        .next()
        .ok_or(Error::ArgError("Expected socket address"))?;

    let num_writers = matches
        .value_of("NUM_WRITERS")
        .unwrap_or("1")
        .parse::<usize>()?;

    let num_metrics = matches
        .value_of("NUM_METRICS")
        .unwrap_or("10")
        .parse::<usize>()?;
    if num_metrics == 0 {
        return Err(Error::ArgError("NUM_METRICS must be > 0"));
    }

    let rate_limit = match matches.value_of("RATE_LIMIT").map(|r| r.parse::<usize>()) {
        None => None,
        Some(Ok(r)) => Some(r),
        Some(Err(err)) => return Err(From::from(err)),
    };

    Ok(Args {
        daemon_addr,
        num_writers,
        num_metrics,
        rate_limit,
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
