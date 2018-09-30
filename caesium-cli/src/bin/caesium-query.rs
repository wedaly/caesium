extern crate clap;
extern crate rustyline;

use clap::{App, Arg};
use rustyline::error::ReadlineError;
use rustyline::Editor;
use std::env;
use std::io;
use std::io::{Read, Write};
use std::net::{AddrParseError, Shutdown, SocketAddr, TcpStream, ToSocketAddrs};
use std::time::Duration;

const READ_TIMEOUT_MS: u64 = 10000;
const HISTORY_FILE: &'static str = &".caesium-query-history";

fn main() -> Result<(), Error> {
    let args = parse_args()?;
    println!("Server address: {}", args.server_addr);
    let mut rl = Editor::<()>::new();
    rl.load_history(HISTORY_FILE).unwrap_or_else(|_e| {});
    loop {
        let result = rl
            .readline(">> ")
            .map_err(|err| Error::from(err))
            .and_then(|line| {
                rl.add_history_entry(&line);
                Ok(line)
            }).and_then(|line| handle_query(&args.server_addr, line.trim()));
        match result {
            Ok(output) => print!("{}", output),
            Err(Error::ReadlineError(ReadlineError::Eof))
            | Err(Error::ReadlineError(ReadlineError::Interrupted)) => {
                break;
            }
            Err(err) => println!("[ERROR] {:?}", err),
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
                .help("Network address of server (defaults to $CAESIUM_SERVER_QUERY_ADDR, then 127.0.0.1:8000)"),
        )
        .get_matches();
    let default_addr =
        env::var("CAESIUM_SERVER_QUERY_ADDR").unwrap_or_else(|_| "127.0.0.1:8000".to_string());
    let server_addr = matches
        .value_of("SERVER_ADDR")
        .unwrap_or(&default_addr)
        .to_socket_addrs()?
        .next()
        .ok_or(Error::ArgError("Expected socket address"))?;
    Ok(Args { server_addr })
}

fn handle_query(addr: &SocketAddr, q: &str) -> Result<String, Error> {
    if q.is_empty() {
        return Ok("".to_string());
    }

    let timeout = Duration::from_millis(READ_TIMEOUT_MS);
    let mut stream = TcpStream::connect_timeout(addr, timeout)?;
    stream.write_all(q.as_bytes())?;
    stream.shutdown(Shutdown::Write)?;
    let mut resp = String::new();
    stream.read_to_string(&mut resp)?;
    Ok(resp)
}

#[derive(Debug)]
enum Error {
    AddrParseError(AddrParseError),
    IOError(io::Error),
    ArgError(&'static str),
    ReadlineError(ReadlineError),
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

impl From<ReadlineError> for Error {
    fn from(err: ReadlineError) -> Error {
        Error::ReadlineError(err)
    }
}
