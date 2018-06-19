extern crate caesium;

use caesium::network::client::Client;
use caesium::network::error::NetworkError;
use caesium::quantile::serializable::SerializableSketch;
use caesium::quantile::writable::WritableSketch;
use caesium::time::TimeStamp;
use std::env;
use std::fs::File;
use std::io;
use std::io::{BufRead, BufReader};
use std::net::{IpAddr, Ipv4Addr, SocketAddr};
use std::num::ParseIntError;

#[derive(Debug)]
struct Args {
    metric: String,
    ts: TimeStamp,
    path: String,
}

fn main() -> Result<(), Error> {
    let args = parse_args()?;
    let sketch = build_sketch(&args.path)?;
    let addr = SocketAddr::new(IpAddr::V4(Ipv4Addr::new(127, 0, 0, 1)), 8000);
    let mut client = Client::new(addr);
    client.insert(args.metric.to_string(), args.ts, sketch)?;
    Ok(())
}

fn parse_args() -> Result<Args, Error> {
    let metric = env::args()
        .nth(1)
        .ok_or(Error::ArgParseError("Missing required argument `metric`"))?;
    let ts = env::args()
        .nth(2)
        .ok_or(Error::ArgParseError(
            "Missing required argument `timestamp`",
        ))
        .and_then(|s| s.parse::<TimeStamp>().map_err(From::from))?;
    let path = env::args()
        .nth(3)
        .ok_or(Error::ArgParseError("Missing required argument `path`"))?;
    Ok(Args { metric, ts, path })
}

fn build_sketch(path: &str) -> Result<SerializableSketch, Error> {
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
    Ok(sketch.to_serializable())
}

#[derive(Debug)]
enum Error {
    ArgParseError(&'static str),
    IOError(io::Error),
    ParseIntError(ParseIntError),
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

impl From<NetworkError> for Error {
    fn from(err: NetworkError) -> Error {
        Error::NetworkError(err)
    }
}