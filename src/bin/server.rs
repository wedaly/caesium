extern crate caesium;
extern crate env_logger;

use caesium::network::error::NetworkError;
use caesium::network::server::run_server;
use std::net::{IpAddr, Ipv4Addr, SocketAddr};

fn main() -> Result<(), NetworkError> {
    env_logger::init();
    let addr = SocketAddr::new(IpAddr::V4(Ipv4Addr::new(127, 0, 0, 1)), 8000);
    run_server(&addr)
}
