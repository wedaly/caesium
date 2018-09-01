extern crate caesium_load;
extern crate clap;
extern crate stackdriver_logger;

use caesium_load::error::Error;
use caesium_load::{generate_load, ReaderConfig, WriterConfig};
use clap::{App, Arg, ArgMatches};
use std::env;
use std::net::ToSocketAddrs;

fn main() -> Result<(), Error> {
    init_logger();
    let args = parse_args()?;
    generate_load(args.writer_config, args.reader_config).map_err(From::from)
}

fn init_logger() {
    if let Err(_) = env::var("RUST_LOG") {
        env::set_var("RUST_LOG", "caesium=debug");
    }
    stackdriver_logger::init();
}

struct Args {
    writer_config: WriterConfig,
    reader_config: ReaderConfig,
}

fn parse_args() -> Result<Args, Error> {
    let matches = App::new("Caesium writer")
        .about("Write metric data to the Caesium daemon")
        .arg(
            Arg::with_name("WRITE_ADDR")
                .long("write-addr")
                .takes_value(true)
                .help("Network address of the daemon for inserts (defaults to 127.0.0.1:8001)"),
        )
        .arg(
            Arg::with_name("WRITE_NUM_WORKERS")
                .long("write-num-workers")
                .takes_value(true)
                .help("Number of concurrent writers (default 1)"),
        )
        .arg(
            Arg::with_name("WRITE_NUM_METRICS")
                .long("write-num-metrics")
                .takes_value(true)
                .help("Number of distinct metrics to write (default 10)"),
        )
        .arg(
            Arg::with_name("WRITE_RATE_LIMIT")
                .long("write-rate-limit")
                .takes_value(true)
                .help("Maximum number of inserts per second per write worker (default 1)"),
        )
        .arg(
            Arg::with_name("READ_ADDR")
                .long("read-addr")
                .takes_value(true)
                .help("Network address of the server for queries (defaults to 127.0.0.1:8000)"),
        )
        .arg(
            Arg::with_name("READ_NUM_WORKERS")
                .long("read-num-workers")
                .takes_value(true)
                .help("Number of concurrent readers (default 1)"),
        )
        .arg(
            Arg::with_name("READ_QUERY_FILE")
                .index(1)
                .required(true)
                .help("File of queries to execute, one per line"),
        )
        .arg(
            Arg::with_name("READ_RATE_LIMIT")
                .long("read-rate-limit")
                .takes_value(true)
                .help("Maximum number of queries per second per read worker (default 1)"),
        )
        .get_matches();

    let writer_config = parse_write_args(&matches)?;
    let reader_config = parse_read_args(&matches)?;
    Ok(Args {
        writer_config,
        reader_config,
    })
}

fn parse_write_args(matches: &ArgMatches) -> Result<WriterConfig, Error> {
    let addr = matches
        .value_of("WRITE_ADDR")
        .unwrap_or(&"127.0.0.1:8001".to_string())
        .to_socket_addrs()?
        .next()
        .ok_or(Error::ArgError("Expected socket address"))?;

    let num_workers = matches
        .value_of("WRITE_NUM_WORKERS")
        .unwrap_or("1")
        .parse::<usize>()?;

    let num_metrics = matches
        .value_of("WRITE_NUM_METRICS")
        .unwrap_or("10")
        .parse::<usize>()?;
    if num_metrics == 0 {
        return Err(Error::ArgError("WRITE_NUM_METRICS must be > 0"));
    }

    let rate_limit = match matches
        .value_of("WRITE_RATE_LIMIT")
        .map(|r| r.parse::<usize>())
    {
        None => None,
        Some(Ok(r)) => Some(r),
        Some(Err(err)) => return Err(From::from(err)),
    };

    Ok(WriterConfig {
        addr,
        num_workers,
        num_metrics,
        rate_limit,
    })
}

fn parse_read_args(matches: &ArgMatches) -> Result<ReaderConfig, Error> {
    let addr = matches
        .value_of("READ_ADDR")
        .unwrap_or(&"127.0.0.1:8000".to_string())
        .to_socket_addrs()?
        .next()
        .ok_or(Error::ArgError("Expected socket address"))?;

    let num_workers = matches
        .value_of("READ_NUM_WORKERS")
        .unwrap_or("1")
        .parse::<usize>()?;

    let query_file_path = matches
        .value_of("READ_QUERY_FILE")
        .map(|s| s.to_string())
        .unwrap();

    let rate_limit = match matches
        .value_of("READ_RATE_LIMIT")
        .map(|r| r.parse::<usize>())
    {
        None => None,
        Some(Ok(r)) => Some(r),
        Some(Err(err)) => return Err(From::from(err)),
    };

    Ok(ReaderConfig {
        addr,
        num_workers,
        query_file_path,
        rate_limit,
    })
}
