extern crate caesium;
extern crate clap;

use caesium::network::client::Client;
use caesium::network::error::NetworkError;
use caesium::network::message::Message;
use caesium::query::result::QueryResult;
use clap::{App, Arg};
use std::io::stdin;
use std::net::{AddrParseError, SocketAddr};
use std::process::exit;

fn main() -> Result<(), Error> {
    let args = parse_args()?;
    let mut client = Client::new(args.server_addr);
    loop {
        let mut line = String::new();
        match stdin().read_line(&mut line) {
            Ok(_) => handle_query(&mut client, line.trim()),
            Err(err) => println!("[ERROR] {:?}", err),
        }
    }
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
                .help("IP address and port of the backend server (defaults to 127.0.0.1:8000)"),
        )
        .get_matches();
    let server_addr = matches
        .value_of("SERVER_ADDR")
        .unwrap_or("127.0.0.1:8000")
        .parse::<SocketAddr>()?;
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
        println!(
            "start={}, end={}, phi={}, count={}, approx={}, lower={}, upper={}",
            r.window().start(),
            r.window().end(),
            r.phi(),
            r.quantile().count,
            r.quantile().approx_value,
            r.quantile().lower_bound,
            r.quantile().upper_bound
        );
    }
}

fn print_error(error: &str) {
    println!("[ERROR] {}", error);
}

#[derive(Debug)]
enum Error {
    AddrParseError(AddrParseError),
    NetworkError(NetworkError),
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
