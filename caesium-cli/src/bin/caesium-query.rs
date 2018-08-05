extern crate caesium_core;
extern crate clap;
extern crate rustyline;

use caesium_core::network::client::Client;
use caesium_core::network::error::NetworkError;
use caesium_core::network::message::Message;
use caesium_core::network::result::QueryResult;
use clap::{App, Arg};
use rustyline::error::ReadlineError;
use rustyline::Editor;
use std::env;
use std::io;
use std::net::{AddrParseError, SocketAddr, ToSocketAddrs};
use std::process::exit;

const HISTORY_FILE: &'static str = &".caesium-query-history";

fn main() -> Result<(), Error> {
    let args = parse_args()?;
    println!("Server address: {}", args.server_addr);
    let mut client = Client::new(args.server_addr);

    let mut rl = Editor::<()>::new();
    rl.load_history(HISTORY_FILE).unwrap_or_else(|_e| {});
    loop {
        let line = rl.readline(">> ");
        match line {
            Ok(line) => {
                rl.add_history_entry(&line);
                handle_query(&mut client, line.trim())
            }
            Err(ReadlineError::Eof) | Err(ReadlineError::Interrupted) => {
                break;
            }
            Err(err) => print_error(&format!("{:?}", err)),
        }
    }
    rl.save_history(HISTORY_FILE).unwrap();
    Ok(())
}

#[derive(Debug)]
struct Args {
    server_addr: SocketAddr,
}

fn parse_args() -> Result<Args, Error> {
    let matches = App::new("Caesium query tool")
        .about("Query for metric data")
        .arg(
            Arg::with_name("SERVER_ADDR")
                .short("a")
                .long("addr")
                .takes_value(true)
                .help("IP address and port of the backend server (defaults to $CAESIUM_SERVER_ADDR, then 127.0.0.1:8000)"),
        )
        .get_matches();
    let default_addr =
        env::var("CAESIUM_SERVER_ADDR").unwrap_or_else(|_| "127.0.0.1:8000".to_string());
    let server_addr = matches
        .value_of("SERVER_ADDR")
        .unwrap_or(&default_addr)
        .to_socket_addrs()?
        .next()
        .ok_or(Error::ArgError("Expected socket address"))?;
    Ok(Args { server_addr })
}

fn handle_query(client: &mut Client, q: &str) {
    if q.is_empty() {
        exit(0);
    }

    let req = Message::QueryReq(q.to_string());
    match client.request(&req) {
        Ok(Message::QuerySuccessResp(results)) => print_results(&results),
        Ok(Message::ErrorResp(err)) => print_error(&err),
        Ok(_) => print_error("Unexpected response message type"),
        Err(err) => print_error(&format!("Unexpected error: {:?}", err)),
    }
}

fn print_results(results: &[QueryResult]) {
    for r in results.iter() {
        match r {
            QueryResult::QuantileWindow(window, phi, quantile) => {
                println!(
                    "start={}, end={}, phi={}, count={}, approx={}, lower={}, upper={}",
                    window.start(),
                    window.end(),
                    phi,
                    quantile.count,
                    quantile.approx_value,
                    quantile.lower_bound,
                    quantile.upper_bound
                );
            }
            QueryResult::MetricName(metric) => println!("{}", metric),
        }
    }
}

fn print_error(error: &str) {
    println!("[ERROR] {}", error);
}

#[derive(Debug)]
enum Error {
    AddrParseError(AddrParseError),
    NetworkError(NetworkError),
    IOError(io::Error),
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

impl From<io::Error> for Error {
    fn from(err: io::Error) -> Error {
        Error::IOError(err)
    }
}
