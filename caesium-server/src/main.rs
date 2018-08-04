extern crate caesium_core;
extern crate caesium_server;
extern crate clap;
extern crate env_logger;

use caesium_core::network::error::NetworkError;
use caesium_server::run_server;
use caesium_server::storage::error::StorageError;
use caesium_server::storage::store::MetricStore;
use clap::{App, Arg};
use std::io;
use std::net::{AddrParseError, SocketAddr, ToSocketAddrs};

fn main() -> Result<(), Error> {
    env_logger::init();
    let args = parse_args()?;
    let db = MetricStore::open(&args.db_path)?;
    run_server(&args.server_addr, db)?;
    Ok(())
}

#[derive(Debug)]
struct Args {
    db_path: String,
    server_addr: SocketAddr,
}

fn parse_args() -> Result<Args, Error> {
    let matches = App::new("Caesium server")
        .about("Backend server for storing and querying metric data")
        .arg(Arg::with_name("DB_PATH")
            .short("d")
            .long("db-path")
            .takes_value(true)
            .help("Path to the database directory.  The directory will be created if it doesn't exist."))
        .arg(Arg::with_name("SERVER_ADDR")
            .short("a")
            .long("addr")
            .takes_value(true)
            .help("IP address and port the server will listen on (defaults to 127.0.0.1:8000)"))
        .get_matches();

    let db_path = matches.value_of("DB_PATH").unwrap_or("db").to_string();
    let server_addr = matches
        .value_of("SERVER_ADDR")
        .unwrap_or("127.0.0.1:8000")
        .to_socket_addrs()?
        .next()
        .ok_or(Error::ArgError("Expected socket address"))?;
    Ok(Args {
        db_path,
        server_addr,
    })
}

#[derive(Debug)]
enum Error {
    AddrParseError(AddrParseError),
    IOError(io::Error),
    NetworkError(NetworkError),
    StorageError(StorageError),
    ArgError(&'static str),
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

impl From<StorageError> for Error {
    fn from(err: StorageError) -> Error {
        Error::StorageError(err)
    }
}

impl From<io::Error> for Error {
    fn from(err: io::Error) -> Error {
        Error::IOError(err)
    }
}
