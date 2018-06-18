extern crate caesium;
extern crate env_logger;

use caesium::network::error::NetworkError;
use caesium::network::server::run_server;
use caesium::storage::store::MetricStore;
use caesium::storage::error::StorageError;
use std::net::{IpAddr, Ipv4Addr, SocketAddr};

fn main() -> Result<(), ServerError> {
    env_logger::init();
    let addr = SocketAddr::new(IpAddr::V4(Ipv4Addr::new(127, 0, 0, 1)), 8000);
    let db = MetricStore::open("db")?;
    run_server(&addr, db)?;
    Ok(())
}

#[derive(Debug)]
enum ServerError {
    NetworkError(NetworkError),
    StorageError(StorageError)
}

impl From<NetworkError> for ServerError {
    fn from(err: NetworkError) -> ServerError {
        ServerError::NetworkError(err)
    }
}

impl From<StorageError> for ServerError {
    fn from(err: StorageError) -> ServerError {
        ServerError::StorageError(err)
    }
}