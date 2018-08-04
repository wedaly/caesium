extern crate caesium_core;
extern crate regex;
extern crate rocksdb;
extern crate tokio;
extern crate uuid;

#[macro_use]
extern crate lazy_static;

#[macro_use]
extern crate log;

pub mod query;
pub mod storage;

use caesium_core::encode::{Decodable, Encodable};
use caesium_core::network::error::NetworkError;
use caesium_core::network::message::Message;
use caesium_core::quantile::writable::WritableSketch;
use caesium_core::time::clock::{Clock, SystemClock};
use caesium_core::time::window::TimeWindow;
use query::execute::execute_query;
use std::net::SocketAddr;
use std::sync::Arc;
use std::thread;
use std::time::Duration;
use storage::downsample::strategies::DefaultStrategy;
use storage::store::MetricStore;
use tokio::io;
use tokio::net::{TcpListener, TcpStream};
use tokio::prelude::*;

const DOWNSAMPLE_INTERVAL_SECONDS: u64 = 600; // 10 mins

pub fn run_server(addr: &SocketAddr, db: MetricStore) -> Result<(), NetworkError> {
    info!("Server is running on {}", addr);
    let listener = TcpListener::bind(addr)?;
    let db_ref = Arc::new(db);
    start_downsample_thread(db_ref.clone());
    run_server_task(listener, db_ref);
    Ok(())
}

fn start_downsample_thread(db_ref: Arc<MetricStore>) {
    let clock = SystemClock::new();
    thread::spawn(move || loop {
        thread::sleep(Duration::new(DOWNSAMPLE_INTERVAL_SECONDS, 0));
        info!("Starting downsample background task");
        let strategy = DefaultStrategy::new(clock.now());
        match db_ref.downsample(&strategy) {
            Ok(_) => info!("Finished downsample background task"),
            Err(err) => error!("Error during downsample background task: {:?}", err),
        }
    });
}

fn run_server_task(listener: TcpListener, db_ref: Arc<MetricStore>) {
    let server = listener
        .incoming()
        .for_each(move |socket| {
            if let Ok(addr) = socket.peer_addr() {
                debug!("New connection from {}", addr);
                handle_connection(socket, db_ref.clone());
            }
            Ok(())
        })
        .map_err(|err| {
            error!("Error accepting connection: {:?}", err);
        });
    tokio::run(server);
}

fn handle_connection(socket: TcpStream, db_ref: Arc<MetricStore>) {
    let input_buf = Vec::new();
    let handle_conn = io::read_to_end(socket, input_buf)
        .and_then(move |(socket, buf)| {
            let mut output_buf = Vec::new();
            process(&buf, &mut output_buf, db_ref.clone());
            io::write_all(socket, output_buf)
        })
        .then(|_| Ok(()));
    tokio::spawn(handle_conn);
}

fn process(mut input: &[u8], output: &mut Vec<u8>, db_ref: Arc<MetricStore>) {
    let req = match Message::decode(&mut input) {
        Ok(msg) => {
            debug!("Received msg: {:?}", msg);
            msg
        }
        Err(err) => {
            error!("Could not decode message: {:?}", err);
            Message::ErrorResp("Could not decode message".to_string())
        }
    };

    let resp = process_request(req, &*db_ref);
    match resp.encode(output) {
        Ok(_) => {
            debug!("Sent msg: {:?}", resp);
        }
        Err(err) => {
            error!("Could not encode message: {:?}", err);
        }
    };
}

fn process_request(req: Message, db: &MetricStore) -> Message {
    match req {
        Message::InsertReq {
            metric,
            window,
            sketch,
        } => process_insert(&metric, window, sketch, db),
        Message::QueryReq(q) => process_query(&q, db),
        _ => Message::ErrorResp("Invalid message type".to_string()),
    }
}

fn process_insert(
    metric: &str,
    window: TimeWindow,
    sketch: WritableSketch,
    db: &MetricStore,
) -> Message {
    match db.insert(metric, window, sketch) {
        Ok(_) => Message::InsertSuccessResp,
        Err(err) => Message::ErrorResp(format!("{:?}", err)),
    }
}

fn process_query(q: &str, db: &MetricStore) -> Message {
    match execute_query(q, db) {
        Ok(results) => Message::QuerySuccessResp(results),
        Err(err) => Message::ErrorResp(format!("{:?}", err)),
    }
}
