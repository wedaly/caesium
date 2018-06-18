extern crate caesium;

use caesium::network::client::Client;
use caesium::network::error::NetworkError;
use caesium::query::result::QueryResult;
use std::io::stdin;
use std::net::{IpAddr, Ipv4Addr, SocketAddr};
use std::process::exit;

fn main() -> Result<(), NetworkError> {
    let addr = SocketAddr::new(IpAddr::V4(Ipv4Addr::new(127, 0, 0, 1)), 8000);
    let mut client = Client::new(addr);
    loop {
        let mut line = String::new();
        match stdin().read_line(&mut line) {
            Ok(_) => handle_query(&mut client, line.trim()),
            Err(err) => println!("[ERROR] {:?}", err),
        }
    }
}

fn handle_query(client: &mut Client, q: &str) {
    if q.is_empty() {
        exit(0);
    }

    match client.query(q) {
        Ok(results) => print_results(&results),
        Err(err) => println!("[ERROR] {:?}", err),
    }
}

fn print_results(results: &[QueryResult]) {
    for r in results.iter() {
        println!("[{}, {}] {}", r.range.start, r.range.end, r.value);
    }
}
