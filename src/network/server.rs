use encode::{Decodable, Encodable};
use network::error::NetworkError;
use network::message::Message;
use quantile::serializable::SerializableSketch;
use query::result::QueryResult;
use std::net::SocketAddr;
use time::TimeStamp;
use tokio;
use tokio::io;
use tokio::net::{TcpListener, TcpStream};
use tokio::prelude::*;

pub fn run_server(addr: &SocketAddr) -> Result<(), NetworkError> {
    let listener = TcpListener::bind(addr)?;
    info!("Server is running on {}", addr);

    let server = listener
        .incoming()
        .for_each(|socket| {
            if let Ok(addr) = socket.peer_addr() {
                debug!("New connection from {}", addr);
                handle_connection(socket);
            }
            Ok(())
        })
        .map_err(|err| {
            error!("Error accepting connection: {:?}", err);
        });
    tokio::run(server);
    Ok(())
}

fn handle_connection(socket: TcpStream) {
    let input_buf = Vec::new();
    let mut output_buf = Vec::new();
    let handle_conn = io::read_to_end(socket, input_buf)
        .and_then(move |(socket, buf)| {
            process(&buf, &mut output_buf);
            io::write_all(socket, output_buf)
        })
        .then(|_| Ok(()));
    tokio::spawn(handle_conn);
}

fn process(mut input: &[u8], output: &mut Vec<u8>) {
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

    let resp = process_request(req);
    match resp.encode(output) {
        Ok(_) => {
            debug!("Sent msg: {:?}", resp);
        }
        Err(err) => {
            error!("Could not encode message: {:?}", err);
        }
    };
}

fn process_request(req: Message) -> Message {
    match req {
        Message::InsertReq { metric, ts, sketch } => process_insert(&metric, ts, sketch),
        Message::QueryReq(q) => process_query(&q),
        _ => Message::ErrorResp("Invalid message type".to_string()),
    }
}

fn process_insert(metric: &str, ts: TimeStamp, sketch: SerializableSketch) -> Message {
    // TODO: insert into database... how to get DB handle?
    Message::InsertSuccessResp
}

fn process_query(q: &str) -> Message {
    // TODO: query database... how to get DB handle?
    Message::QuerySuccessResp(vec![])
}
