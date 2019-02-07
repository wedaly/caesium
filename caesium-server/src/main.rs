extern crate caesium_core;
extern crate caesium_server;
extern crate clap;
extern crate stackdriver_logger;

#[macro_use]
extern crate log;

use caesium_core::get_sketch_type;
use caesium_core::time::clock::{Clock, SystemClock};
use caesium_server::server::read::ReadServer;
use caesium_server::server::write::WriteServer;
use caesium_server::storage::downsample::strategies::DefaultStrategy;
use caesium_server::storage::error::StorageError;
use caesium_server::storage::store::MetricStore;
use clap::{App, Arg};
use std::env;
use std::io;
use std::net::{AddrParseError, SocketAddr, ToSocketAddrs};
use std::num::ParseIntError;
use std::sync::Arc;
use std::thread;
use std::time::Duration;

fn main() -> Result<(), Error> {
    init_logger();
    info!("Using sketch type {:?}", get_sketch_type());
    let args = parse_args()?;
    let db = MetricStore::open(&args.db_path)?;
    let db_ref = Arc::new(db);
    let threads = vec![
        start_downsample_thread(args.downsample_interval, db_ref.clone()),
        start_read_server_thread(
            &args.query_addr,
            args.num_read_workers,
            args.query_buffer_len,
            db_ref.clone(),
        )?,
        start_write_server_thread(
            &args.insert_addr,
            args.num_write_workers,
            args.insert_buffer_len,
            db_ref.clone(),
        )?,
    ];
    for t in threads {
        if let Err(err) = t.join() {
            error!("Error joining thread: {:?}", err);
        }
    }
    Ok(())
}

fn init_logger() {
    if let Err(_) = env::var("RUST_LOG") {
        env::set_var("RUST_LOG", "caesium=debug");
    }
    stackdriver_logger::init();
}

fn start_downsample_thread(interval: Duration, db_ref: Arc<MetricStore>) -> thread::JoinHandle<()> {
    let clock = SystemClock::new();
    thread::spawn(move || loop {
        thread::sleep(interval);
        info!("Starting downsample background task");
        let strategy = DefaultStrategy::new(clock.now());
        match db_ref.downsample(&strategy) {
            Ok(_) => info!("Finished downsample background task"),
            Err(err) => error!("Error during downsample background task: {:?}", err),
        }
    })
}

fn start_read_server_thread(
    addr: &SocketAddr,
    num_read_workers: usize,
    buffer_len: usize,
    db_ref: Arc<MetricStore>,
) -> Result<thread::JoinHandle<()>, io::Error> {
    let server = ReadServer::new(addr, num_read_workers, buffer_len, db_ref)?;
    let thread = thread::spawn(move || {
        if let Err(err) = server.run() {
            error!("Error running read server: {:?}", err);
        }
    });
    Ok(thread)
}

fn start_write_server_thread(
    addr: &SocketAddr,
    num_write_workers: usize,
    buffer_len: usize,
    db_ref: Arc<MetricStore>,
) -> Result<thread::JoinHandle<()>, io::Error> {
    let server = WriteServer::new(addr, num_write_workers, buffer_len, db_ref)?;
    let thread = thread::spawn(move || {
        if let Err(err) = server.run() {
            error!("Error running write server: {:?}", err);
        }
    });
    Ok(thread)
}

#[derive(Debug)]
struct Args {
    db_path: String,
    num_read_workers: usize,
    num_write_workers: usize,
    query_buffer_len: usize,
    insert_buffer_len: usize,
    query_addr: SocketAddr,
    insert_addr: SocketAddr,
    downsample_interval: Duration,
}

fn parse_args() -> Result<Args, Error> {
    let matches = App::new("Caesium server")
        .about("Backend server for storing and querying metric data")
        .arg(Arg::with_name("DB_PATH")
            .short("d")
            .long("db-path")
            .takes_value(true)
            .help("Path to the database directory.  The directory will be created if it doesn't exist."))
        .arg(Arg::with_name("NUM_READ_WORKERS")
            .long("num-read-workers")
            .takes_value(true)
            .help("Number of threads to process queries (default 1)"))
        .arg(Arg::with_name("NUM_WRITE_WORKERS")
            .long("num-write-workers")
            .takes_value(true)
            .help("Number of threads to process inserts (default 1)"))
        .arg(Arg::with_name("QUERY_BUFFER_LEN")
            .long("query-buffer-len")
            .takes_value(true)
            .help("Number of queries to enqueue before blocking (default 4096)"))
        .arg(Arg::with_name("INSERT_BUFFER_LEN")
            .long("insert-buffer-len")
            .takes_value(true)
            .help("Number of inserts to enqueue before blocking (default 4096)"))
        .arg(Arg::with_name("QUERY_ADDR")
            .long("query-addr")
            .takes_value(true)
            .help("Network address for queries (defaults to 127.0.0.1:8000)"))
        .arg(Arg::with_name("INSERT_ADDR")
            .long("insert-addr")
            .takes_value(true)
            .help("Network address for inserts (defaults to 127.0.0.1:8001)"))
        .arg(Arg::with_name("DOWNSAMPLE_INTERVAL")
            .long("downsample-interval")
            .takes_value(true)
            .help("Number of seconds between downsample background tasks (default 600)"))
        .get_matches();

    let db_path = matches.value_of("DB_PATH").unwrap_or("db").to_string();

    let num_read_workers = matches
        .value_of("NUM_READ_WORKERS")
        .unwrap_or("1")
        .parse::<usize>()?;
    if num_read_workers == 0 {
        return Err(Error::ArgError("Must have at least one read worker"));
    }

    let num_write_workers = matches
        .value_of("NUM_WRITE_WORKERS")
        .unwrap_or("1")
        .parse::<usize>()?;
    if num_write_workers == 0 {
        return Err(Error::ArgError("Must have at least one write worker"));
    }

    let query_buffer_len = matches
        .value_of("QUERY_BUFFER_LEN")
        .unwrap_or("4096")
        .parse::<usize>()?;

    let insert_buffer_len = matches
        .value_of("INSERT_BUFFER_LEN")
        .unwrap_or("4096")
        .parse::<usize>()?;

    let query_addr = matches
        .value_of("QUERY_ADDR")
        .unwrap_or("127.0.0.1:8000")
        .to_socket_addrs()?
        .next()
        .ok_or(Error::ArgError("Expected socket address"))?;

    let insert_addr = matches
        .value_of("INSERT_ADDR")
        .unwrap_or("127.0.0.1:8001")
        .to_socket_addrs()?
        .next()
        .ok_or(Error::ArgError("Expected socket address"))?;

    let downsample_interval = matches
        .value_of("DOWNSAMPLE_INTERVAL")
        .unwrap_or("600")
        .parse::<u64>()
        .map(|secs| Duration::from_secs(secs))?;

    Ok(Args {
        db_path,
        num_read_workers,
        num_write_workers,
        query_buffer_len,
        insert_buffer_len,
        query_addr,
        insert_addr,
        downsample_interval,
    })
}

#[derive(Debug)]
enum Error {
    AddrParseError(AddrParseError),
    IOError(io::Error),
    StorageError(StorageError),
    ParseIntError(ParseIntError),
    ArgError(&'static str),
}

impl From<AddrParseError> for Error {
    fn from(err: AddrParseError) -> Error {
        Error::AddrParseError(err)
    }
}

impl From<io::Error> for Error {
    fn from(err: io::Error) -> Error {
        Error::IOError(err)
    }
}

impl From<StorageError> for Error {
    fn from(err: StorageError) -> Error {
        Error::StorageError(err)
    }
}

impl From<ParseIntError> for Error {
    fn from(err: ParseIntError) -> Error {
        Error::ParseIntError(err)
    }
}
