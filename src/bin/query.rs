extern crate caesium;

use caesium::network::client::Client;
use caesium::network::error::NetworkError;
use caesium::network::message::Message;
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
