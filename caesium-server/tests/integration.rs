extern crate caesium_core;
extern crate caesium_server;
extern crate regex;
extern crate uuid;

#[macro_use]
extern crate lazy_static;

use caesium_core::encode::frame::FrameEncoder;
use caesium_core::protocol::messages::InsertMessage;
use caesium_core::quantile::writable::WritableSketch;
use caesium_core::time::timestamp::TimeStamp;
use caesium_core::time::window::TimeWindow;
use caesium_server::server::read::ReadServer;
use caesium_server::server::write::WriteServer;
use caesium_server::storage::store::MetricStore;
use regex::Regex;
use std::env;
use std::fs;
use std::io::{Read, Write};
use std::net::{Shutdown, SocketAddr, TcpStream};
use std::panic;
use std::sync::Arc;
use std::thread;
use std::time::Duration;
use uuid::Uuid;

#[test]
fn it_queries_metrics() {
    with_server(|mut insert_client, query_client| {
        insert_client.insert(&"m1", 0, 30);
        insert_client.insert(&"m1", 30, 60);
        insert_client.insert(&"m2", 60, 90);
        insert_client.insert(&"m2", 90, 100);
        thread::sleep(Duration::from_millis(500));
        let r1 = query_client.query(&"search(\"*\")");
        assert_metric_names(&r1, &[&"m1", &"m2"]);
        let r2 = query_client.query(&"quantile(fetch(\"m2\"), 0.5)");
        assert_windows(
            &r2,
            &vec![TimeWindow::new(60, 90), TimeWindow::new(90, 100)],
        );
        let r3 = query_client.query(&"quantile(fetch(\"m1\", 25, 70), 0.5)");
        assert_windows(&r3, &vec![TimeWindow::new(30, 60)]);
    })
}

struct InsertClient {
    stream: TcpStream,
    frame_encoder: FrameEncoder,
}

impl InsertClient {
    fn new(addr: SocketAddr) -> InsertClient {
        let timeout = Duration::from_millis(1000);
        let stream =
            TcpStream::connect_timeout(&addr, timeout).expect("Could not connect to server");
        stream
            .set_write_timeout(Some(timeout))
            .expect("Could not set write timeout");
        InsertClient {
            stream,
            frame_encoder: FrameEncoder::new(),
        }
    }

    fn insert(&mut self, metric: &str, start: TimeStamp, end: TimeStamp) {
        let window = TimeWindow::new(start, end);
        let sketch = InsertClient::build_sketch();
        let msg = InsertMessage {
            metric: metric.to_string(),
            window,
            sketch,
        };
        self.frame_encoder
            .encode_framed_msg(&msg, &mut self.stream)
            .expect("Could not send framed message");
    }

    fn build_sketch() -> WritableSketch {
        let mut sketch = WritableSketch::new();
        for i in 0..10 {
            sketch.insert(i as u64);
        }
        sketch
    }
}

struct QueryClient {
    addr: SocketAddr,
}

impl QueryClient {
    fn new(addr: SocketAddr) -> QueryClient {
        QueryClient { addr }
    }

    fn query(&self, q: &str) -> String {
        let timeout = Duration::from_millis(1000);
        let mut stream = TcpStream::connect_timeout(&self.addr, timeout)
            .expect("Could not connect to read server");
        stream
            .write_all(q.as_bytes())
            .expect("Could not write query");
        stream
            .shutdown(Shutdown::Write)
            .expect("Could not close stream");
        let mut resp = String::new();
        stream
            .read_to_string(&mut resp)
            .expect("Could not read query result");
        resp
    }
}

fn assert_metric_names(resp: &str, expected: &[&str]) {
    let actual: Vec<&str> = resp.trim().split("\n").collect();
    assert_eq!(actual, expected);
}

fn assert_windows(resp: &str, expected: &[TimeWindow]) {
    let actual: Vec<TimeWindow> = resp
        .trim()
        .split("\n")
        .filter_map(|line| parse_window(line))
        .collect();
    assert_eq!(actual, expected);
}

fn parse_window(line: &str) -> Option<TimeWindow> {
    lazy_static! {
        static ref WINDOW_RE: Regex = Regex::new("^start=(?P<start>[0-9]+), end=(?P<end>[0-9]+)")
            .expect("Could not compile regex");
    }
    WINDOW_RE
        .captures(line)
        .and_then(|c| match (c.name("start"), c.name("end")) {
            (Some(start_match), Some(end_match)) => {
                let start: u64 = start_match.as_str().parse().unwrap();
                let end: u64 = end_match.as_str().parse().unwrap();
                Some(TimeWindow::new(start, end))
            }
            _ => None,
        })
}

fn with_server<T>(test: T) -> ()
where
    T: FnOnce(InsertClient, QueryClient) -> () + panic::UnwindSafe,
{
    let (write_addr, read_addr, db_path) = start_server();
    let insert_client = InsertClient::new(write_addr);
    let query_client = QueryClient::new(read_addr);
    let result = panic::catch_unwind(move || test(insert_client, query_client));
    fs::remove_dir_all(&db_path).expect("Could not delete DB directory");
    assert!(result.is_ok())
}

fn start_server() -> (SocketAddr, SocketAddr, String) {
    let server_addr: SocketAddr = "127.0.0.1:0".parse().unwrap();

    let db_path = unique_tmp_db_path();
    let db = MetricStore::open(&db_path).expect("Could not open db");
    let db_ref = Arc::new(db);

    let write_server =
        WriteServer::new(&server_addr, 1, db_ref.clone()).expect("Could not start write server");
    let write_addr = write_server
        .local_addr()
        .expect("Could not retrieve write server addr");
    thread::spawn(move || write_server.run());

    let read_server =
        ReadServer::new(&server_addr, 1, db_ref.clone()).expect("Could not start read server");
    let read_addr = read_server
        .local_addr()
        .expect("Could not retrieve read server address");
    thread::spawn(move || read_server.run());

    (write_addr, read_addr, db_path)
}

fn unique_tmp_db_path() -> String {
    let mut path = env::temp_dir();
    let db_name = format!("testdb_{}", Uuid::new_v4());
    path.push(db_name);
    path.to_str()
        .expect("Could not construct DB path")
        .to_string()
}
