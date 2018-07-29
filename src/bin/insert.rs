extern crate caesium;
extern crate clap;

use caesium::network::client::Client;
use caesium::network::error::NetworkError;
use caesium::network::message::Message;
use caesium::quantile::writable::WritableSketch;
use caesium::time::timestamp::TimeStamp;
use caesium::time::window::TimeWindow;
use clap::{App, Arg};
use std::fs::File;
use std::io;
use std::io::{BufRead, BufReader};
use std::net::{AddrParseError, SocketAddr};
use std::num::ParseIntError;

fn main() -> Result<(), Error> {
    let args = parse_args()?;
    let sketch = build_sketch(&args.path)?;
    let client = Client::new(args.server_addr);
    let req = Message::InsertReq {
        metric: args.metric,
        window: args.window,
        sketch,
    };
    match client.request(&req) {
        Ok(Message::InsertSuccessResp) => Ok(()),
        Ok(msg) => Err(Error::UnexpectedRespError(msg)),
        Err(err) => Err(From::from(err)),
    }
}

#[derive(Debug)]
struct Args {
    metric: String,
    window: TimeWindow,
    path: String,
    server_addr: SocketAddr,
}

fn parse_args() -> Result<Args, Error> {
    let matches = App::new("Caesium insert tool")
        .about("Insert metric data directly to backend server (useful for testing)")
        .arg(
            Arg::with_name("METRIC_NAME")
                .index(1)
                .required(true)
                .help("Name of the metric to insert"),
        )
        .arg(
            Arg::with_name("START_TS")
                .index(2)
                .required(true)
                .help("Start timestamp (seconds since UNIX epoch)"),
        )
        .arg(
            Arg::with_name("END_TS")
                .index(3)
                .required(true)
                .help("End timestamp (seconds since UNIX epoch)"),
        )
        .arg(
            Arg::with_name("DATA_PATH")
                .index(4)
                .required(true)
                .help("Path to data file, one unsigned 64-bit integer per line"),
        )
        .arg(
            Arg::with_name("SERVER_ADDR")
                .long("server-addr")
                .short("a")
                .takes_value(true)
                .help("IP address and port of backend server (defaults to 127.0.0.1:8000"),
        )
        .get_matches();

    let metric = matches.value_of("METRIC_NAME").unwrap().to_string();
    let start_ts = matches.value_of("START_TS").unwrap().parse::<TimeStamp>()?;
    let end_ts = matches.value_of("END_TS").unwrap().parse::<TimeStamp>()?;
    let path = matches.value_of("DATA_PATH").unwrap().to_string();
    let server_addr = matches
        .value_of("SERVER_ADDR")
        .unwrap_or("127.0.0.1:8000")
        .parse::<SocketAddr>()?;

    if start_ts > end_ts {
        return Err(Error::ArgError("Start time must be <= end time"));
    }
    let window = TimeWindow::new(start_ts, end_ts);

    Ok(Args {
        metric,
        window,
        path,
        server_addr,
    })
}

fn build_sketch(path: &str) -> Result<WritableSketch, Error> {
    let file = File::open(path)?;
    let reader = BufReader::new(file);
    let mut sketch = WritableSketch::new();
    reader
        .lines()
        .filter_map(|result| {
            result
                .map_err(|e| Error::IOError(e))
                .and_then(|l| l.parse::<u64>().map_err(From::from))
                .ok()
        })
        .for_each(|val| sketch.insert(val));
    Ok(sketch)
}

#[derive(Debug)]
enum Error {
    AddrParseError(AddrParseError),
    IOError(io::Error),
    ParseIntError(ParseIntError),
    ArgError(&'static str),
    UnexpectedRespError(Message),
    NetworkError(NetworkError),
}

impl From<io::Error> for Error {
    fn from(err: io::Error) -> Error {
        Error::IOError(err)
    }
}

impl From<ParseIntError> for Error {
    fn from(err: ParseIntError) -> Error {
        Error::ParseIntError(err)
    }
}

impl From<AddrParseError> for Error {
    fn from(err: AddrParseError) -> Error {
        Error::AddrParseError(err)
    }
}

impl From<NetworkError> for Error {
    fn from(err: NetworkError) -> Error {
        Error::NetworkError(err)
    }
}
