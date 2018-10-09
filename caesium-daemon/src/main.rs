extern crate caesium_core;
extern crate caesium_daemon;
extern crate clap;
extern crate stackdriver_logger;

#[macro_use]
extern crate log;

use caesium_core::get_sketch_type;
use caesium_daemon::run_daemon;
use clap::{App, Arg};
use std::env;
use std::io;
use std::num::ParseIntError;

fn main() -> Result<(), Error> {
    init_logger();
    let args = parse_args()?;
    info!("Using sketch type {:?}", get_sketch_type());
    info!(
        "Listening on {}, publishing to {}, window size is {}",
        args.listen_addr, args.publish_addr, args.window_size
    );
    run_daemon(args.listen_addr, args.publish_addr, args.window_size)?;
    Ok(())
}

fn init_logger() {
    if let Err(_) = env::var("RUST_LOG") {
        env::set_var("RUST_LOG", "caesium=debug");
    }
    stackdriver_logger::init();
}

#[derive(Debug)]
struct Args {
    listen_addr: String,
    publish_addr: String,
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
        ).arg(
            Arg::with_name("PUBLISH_ADDR")
                .long("publish-addr")
                .takes_value(true)
                .help("IP address and port of backend server (defaults to 127.0.0.1:8000)"),
        ).arg(
            Arg::with_name("WINDOW_SIZE")
                .long("window-size")
                .short("w")
                .takes_value(true)
                .help("Size of aggregation windows in seconds (defaults to 10)"),
        ).get_matches();

    let listen_addr = matches
        .value_of("LISTEN_ADDR")
        .unwrap_or("127.0.0.1:8001")
        .to_string();

    let publish_addr = matches
        .value_of("PUBLISH_ADDR")
        .unwrap_or("127.0.0.1:8001")
        .to_string();

    let window_size = matches
        .value_of("WINDOW_SIZE")
        .unwrap_or("10")
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
