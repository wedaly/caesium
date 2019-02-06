extern crate caesium_load;
extern crate clap;
extern crate stackdriver_logger;

use caesium_load::error::Error;
use caesium_load::{generate_load, DaemonWriterConfig, ServerReaderConfig, ServerWriterConfig};
use clap::{App, Arg, ArgMatches};
use std::env;
use std::net::ToSocketAddrs;

fn main() -> Result<(), Error> {
    init_logger();
    let args = parse_args()?;
    generate_load(
        args.report_sample_interval,
        args.daemon_writer_config,
        args.server_reader_config,
        args.server_writer_config,
    )
    .map_err(From::from)
}

fn init_logger() {
    if let Err(_) = env::var("RUST_LOG") {
        env::set_var("RUST_LOG", "caesium=debug");
    }
    stackdriver_logger::init();
}

struct Args {
    report_sample_interval: u64,
    daemon_writer_config: DaemonWriterConfig,
    server_reader_config: ServerReaderConfig,
    server_writer_config: ServerWriterConfig,
}

fn parse_args() -> Result<Args, Error> {
    let matches = App::new("Caesium writer")
        .about("Write metric data to the Caesium daemon")
        .arg(
            Arg::with_name("REPORT_SAMPLE_INTERVAL")
                .long("report-sample-interval")
                .takes_value(true)
                .help("Interval in seconds for reporting insert rate and query durations (default 60)")
        )
        .arg(
            Arg::with_name("DAEMON_WRITE_ADDR")
                .long("daemon-write-addr")
                .takes_value(true)
                .help("Network address of the daemon for inserts (defaults to 127.0.0.1:8001)"),
        )
        .arg(
            Arg::with_name("DAEMON_WRITE_NUM_WORKERS")
                .long("daemon-write-num-workers")
                .takes_value(true)
                .help("Number of concurrent writers (default 1)"),
        )
        .arg(
            Arg::with_name("DAEMON_WRITE_NUM_METRICS")
                .long("daemon-write-num-metrics")
                .takes_value(true)
                .help("Number of distinct metrics to write (default 10)"),
        )
        .arg(
            Arg::with_name("DAEMON_WRITE_RATE_LIMIT")
                .long("daemon-write-rate-limit")
                .takes_value(true)
                .help("Maximum number of inserts per second per write worker (default 1)"),
        )
        .arg(
            Arg::with_name("SERVER_QUERY_ADDR")
                .long("server-query-addr")
                .takes_value(true)
                .help("Network address of the server for queries (defaults to 127.0.0.1:8000)"),
        )
        .arg(
            Arg::with_name("SERVER_QUERY_NUM_WORKERS")
                .long("server-query-num-workers")
                .takes_value(true)
                .help("Number of concurrent readers (default 1)"),
        )
        .arg(
            Arg::with_name("SERVER_QUERY_FILE")
                .index(1)
                .required(true)
                .help("File of queries to execute, one per line"),
        )
        .arg(
            Arg::with_name("SERVER_QUERY_RATE_LIMIT")
                .long("server-query-rate-limit")
                .takes_value(true)
                .help("Maximum number of queries per second per read worker (default 1)"),
        )
        .arg(
            Arg::with_name("SERVER_WRITE_ADDR")
                .long("server-write-addr")
                .takes_value(true)
                .help("Network address of the server for inserting sketches directly (defaults to 127.0.0.1:8001)"),
        )
        .arg(
            Arg::with_name("SERVER_WRITE_NUM_WORKERS")
                .long("server-write-num-workers")
                .takes_value(true)
                .help("Number of concurrent writers (default 1)"),
        )
        .arg(
            Arg::with_name("SERVER_WRITE_SKETCH_SIZE")
                .long("server-write-sketch-size")
                .takes_value(true)
                .help("Number of values to insert per sketch (default 1000)")
        )
        .arg(
            Arg::with_name("SERVER_WRITE_RATE_LIMIT")
                .long("server-write-rate-limit")
                .takes_value(true)
                .help("Maximum number of sketches to insert per second per worker (default 1)"),
        )
        .get_matches();

    let report_sample_interval = matches
        .value_of("REPORT_SAMPLE_INTERVAL")
        .unwrap_or("60")
        .parse::<u64>()?;

    let daemon_writer_config = parse_daemon_writer_args(&matches)?;
    let server_reader_config = parse_server_reader_args(&matches)?;
    let server_writer_config = parse_server_writer_args(&matches)?;

    Ok(Args {
        report_sample_interval,
        daemon_writer_config,
        server_reader_config,
        server_writer_config,
    })
}

fn parse_daemon_writer_args(matches: &ArgMatches) -> Result<DaemonWriterConfig, Error> {
    let addr = matches
        .value_of("DAEMON_WRITE_ADDR")
        .unwrap_or(&"127.0.0.1:8001".to_string())
        .to_socket_addrs()?
        .next()
        .ok_or(Error::ArgError("Expected socket address"))?;

    let num_workers = matches
        .value_of("DAEMON_WRITE_NUM_WORKERS")
        .unwrap_or("1")
        .parse::<usize>()?;

    let num_metrics = matches
        .value_of("DAEMON_WRITE_NUM_METRICS")
        .unwrap_or("10")
        .parse::<usize>()?;
    if num_metrics == 0 {
        return Err(Error::ArgError("DAEMON_WRITE_NUM_METRICS must be > 0"));
    }

    let rate_limit = match matches
        .value_of("DAEMON_WRITE_RATE_LIMIT")
        .map(|r| r.parse::<usize>())
    {
        None => None,
        Some(Ok(r)) => Some(r),
        Some(Err(err)) => return Err(From::from(err)),
    };

    Ok(DaemonWriterConfig {
        addr,
        num_workers,
        num_metrics,
        rate_limit,
    })
}

fn parse_server_reader_args(matches: &ArgMatches) -> Result<ServerReaderConfig, Error> {
    let addr = matches
        .value_of("SERVER_QUERY_ADDR")
        .unwrap_or(&"127.0.0.1:8000".to_string())
        .to_socket_addrs()?
        .next()
        .ok_or(Error::ArgError("Expected socket address"))?;

    let num_workers = matches
        .value_of("SERVER_QUERY_NUM_WORKERS")
        .unwrap_or("1")
        .parse::<usize>()?;

    let query_file_path = matches
        .value_of("SERVER_QUERY_FILE")
        .map(|s| s.to_string())
        .unwrap();

    let rate_limit = match matches
        .value_of("SERVER_QUERY_RATE_LIMIT")
        .map(|r| r.parse::<usize>())
    {
        None => None,
        Some(Ok(r)) => Some(r),
        Some(Err(err)) => return Err(From::from(err)),
    };

    Ok(ServerReaderConfig {
        addr,
        num_workers,
        query_file_path,
        rate_limit,
    })
}

fn parse_server_writer_args(matches: &ArgMatches) -> Result<ServerWriterConfig, Error> {
    let addr = matches
        .value_of("SERVER_WRITE_ADDR")
        .unwrap_or(&"127.0.0.1:8001".to_string())
        .to_socket_addrs()?
        .next()
        .ok_or(Error::ArgError("Expected socket address"))?;

    let num_workers = matches
        .value_of("SERVER_WRITE_NUM_WORKERS")
        .unwrap_or("1")
        .parse::<usize>()?;

    let sketch_size = matches
        .value_of("SERVER_WRITE_SKETCH_SIZE")
        .unwrap_or("1000")
        .parse::<usize>()?;

    let rate_limit = match matches
        .value_of("SERVER_WRITE_RATE_LIMIT")
        .map(|r| r.parse::<usize>())
    {
        None => None,
        Some(Ok(r)) => Some(r),
        Some(Err(err)) => return Err(From::from(err)),
    };

    Ok(ServerWriterConfig {
        addr,
        num_workers,
        sketch_size,
        rate_limit,
    })
}
