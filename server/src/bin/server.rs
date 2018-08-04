extern crate caesium_core;
extern crate caesium_server;
extern crate clap;
extern crate env_logger;

use caesium_core::network::error::NetworkError;
use caesium_server::run_server;
use caesium_server::storage::error::StorageError;
use caesium_server::storage::store::MetricStore;
use clap::{App, Arg};
use std::net::{AddrParseError, SocketAddr};

fn main() -> Result<(), Error> {
    env_logger::init();
    let args = parse_args()?;
    let db = MetricStore::open(&args.db_name)?;
    run_server(&args.server_addr, db)?;
    Ok(())
}

#[derive(Debug)]
struct Args {
    db_name: String,
    server_addr: SocketAddr,
}

fn parse_args() -> Result<Args, Error> {
    let matches = App::new("Caesium server")
        .about("Backend server for storing and querying metric data")
        .arg(Arg::with_name("DB_NAME")
            .short("d")
            .long("db-name")
            .takes_value(true)
            .help("Name of the database.  The database directory will be created if it doesn't already exist."))
        .arg(Arg::with_name("SERVER_ADDR")
            .short("a")
            .long("addr")
            .takes_value(true)
            .help("IP address and port the server will listen on (defaults to 127.0.0.1:8000)"))
        .get_matches();

    let db_name = matches.value_of("DB_NAME").unwrap_or("db").to_string();
    let server_addr = matches
        .value_of("SERVER_ADDR")
        .unwrap_or("127.0.0.1:8000")
        .parse::<SocketAddr>()?;
    Ok(Args {
        db_name,
        server_addr,
    })
}

#[derive(Debug)]
enum Error {
    AddrParseError(AddrParseError),
    NetworkError(NetworkError),
    StorageError(StorageError),
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
