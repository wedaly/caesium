extern crate caesium;

use caesium::network::client::Client;
use caesium::network::error::NetworkError;
use caesium::query::result::QueryResult;
use std::io::{stdin, stdout, Write};
use std::net::{IpAddr, Ipv4Addr, SocketAddr};

fn main() -> Result<(), NetworkError> {
    let addr = SocketAddr::new(IpAddr::V4(Ipv4Addr::new(127, 0, 0, 1)), 8000);
    let mut client = Client::new(addr);
    loop {
        print!("> ");
        flush_stdout();
        let mut line = String::new();
        match stdin().read_line(&mut line) {
            Ok(_) => handle_query(&mut client, line.trim()),
            Err(err) => println!("[ERROR] {:?}", err),
        }
    }
}

fn flush_stdout() {
    stdout().flush().expect("Could not flush stdout");
}

fn handle_query(client: &mut Client, q: &str) {
    match client.query(q) {
        Ok(results) => print_results(&results),
        Err(err) => println!("[ERROR] {:?}", err),
    }
}

fn print_results(results: &[QueryResult]) {
    println!("=");
    for r in results.iter() {
        println!("[{}, {}] {}", r.range.start, r.range.end, r.value);
    }
}
