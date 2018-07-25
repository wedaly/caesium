extern crate caesium;
extern crate env_logger;

use caesium::network::daemon::run_daemon;
use caesium::network::error::NetworkError;
use std::net::{IpAddr, Ipv4Addr, SocketAddr};

fn main() -> Result<(), NetworkError> {
    env_logger::init();
    let source_addr = SocketAddr::new(IpAddr::V4(Ipv4Addr::new(127, 0, 0, 1)), 8001);
    let sink_addr = SocketAddr::new(IpAddr::V4(Ipv4Addr::new(127, 0, 0, 1)), 8000);
    run_daemon(source_addr, sink_addr)?;
    Ok(())
}
