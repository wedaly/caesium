use encode::{Decodable, Encodable};
use network::error::NetworkError;
use network::message::Message;
use quantile::writable::WritableSketch;
use query::execute::execute_query;
use std::net::SocketAddr;
use std::sync::Arc;
use storage::store::MetricStore;
use time::window::TimeWindow;
use tokio;
use tokio::io;
use tokio::net::{TcpListener, TcpStream};
use tokio::prelude::*;

pub fn run_server(addr: &SocketAddr, db: MetricStore) -> Result<(), NetworkError> {
    let listener = TcpListener::bind(addr)?;
    info!("Server is running on {}", addr);

    let db = Arc::new(db);
    let server = listener
        .incoming()
        .for_each(move |socket| {
            if let Ok(addr) = socket.peer_addr() {
                debug!("New connection from {}", addr);
                let db_ref = db.clone();
                handle_connection(socket, db_ref);
            }
            Ok(())
        })
        .map_err(|err| {
            error!("Error accepting connection: {:?}", err);
        });
    tokio::run(server);
    Ok(())
}

fn handle_connection(socket: TcpStream, db: Arc<MetricStore>) {
    let input_buf = Vec::new();
    let handle_conn = io::read_to_end(socket, input_buf)
        .and_then(move |(socket, buf)| {
            let db_ref = db.clone();
            let mut output_buf = Vec::new();
            process(&buf, &mut output_buf, db_ref);
            io::write_all(socket, output_buf)
        })
        .then(|_| Ok(()));
    tokio::spawn(handle_conn);
}

fn process(mut input: &[u8], output: &mut Vec<u8>, db: Arc<MetricStore>) {
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

    let resp = process_request(req, &*db);
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
