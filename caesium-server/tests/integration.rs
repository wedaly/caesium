extern crate caesium_core;
extern crate caesium_server;
extern crate uuid;

use caesium_core::network::client::Client;
use caesium_core::network::message::Message;
use caesium_core::network::result::QueryResult;
use caesium_core::quantile::writable::WritableSketch;
use caesium_core::time::timestamp::TimeStamp;
use caesium_core::time::window::TimeWindow;
use caesium_server::server::Server;
use caesium_server::storage::store::MetricStore;
use std::env;
use std::fs;
use std::net::SocketAddr;
use std::panic;
use std::thread;
use uuid::Uuid;

#[test]
fn it_queries_metrics() {
    with_server(|client| {
        insert_data(&client, &"m1", 0, 30);
        insert_data(&client, &"m1", 30, 60);
        insert_data(&client, &"m2", 60, 90);
        insert_data(&client, &"m2", 90, 100);
        let r1 = query(&client, &"search(\"*\")");
        assert_metric_names(&r1, &[&"m1", &"m2"]);
        let r2 = query(&client, &"quantile(fetch(\"m2\"), 0.5)");
        assert_windows(
            &r2,
            &vec![TimeWindow::new(60, 90), TimeWindow::new(90, 100)],
        );
        let r3 = query(&client, &"quantile(fetch(\"m1\", 25, 70), 0.5)");
        assert_windows(&r3, &vec![TimeWindow::new(30, 60)]);
    })
}

fn insert_data(client: &Client, metric_name: &str, start: TimeStamp, end: TimeStamp) {
    let mut sketch = WritableSketch::new();
    for i in 0..10 {
        sketch.insert(i as u64);
    }
    let req = Message::InsertReq {
        metric: metric_name.to_string(),
        window: TimeWindow::new(start, end),
        sketch,
    };
    let resp = client.request(&req).expect("Could not insert data");
    match resp {
        Message::InsertSuccessResp => {}
        msg => panic!(format!("Unexpected query response: {:?}", msg)),
    }
}

fn query(client: &Client, q: &str) -> Vec<QueryResult> {
    let req = Message::QueryReq(q.to_string());
    let resp = client.request(&req).expect("Could not query server");
    match resp {
        Message::QuerySuccessResp(results) => results,
        msg => panic!(format!("Unexpected query response: {:?}", msg)),
    }
}

fn assert_metric_names(results: &[QueryResult], expected: &[&str]) {
    let actual: Vec<&str> = results
        .iter()
        .filter_map(|r| match r {
            QueryResult::MetricName(m) => Some(m.as_str()),
            _ => None,
        })
        .collect();
    assert_eq!(actual, expected);
}

fn assert_windows(results: &[QueryResult], expected: &[TimeWindow]) {
    let actual: Vec<TimeWindow> = results
        .iter()
        .filter_map(|r| match r {
            QueryResult::QuantileWindow(window, _phi, _q) => Some(*window),
            _ => None,
        })
        .collect();
    assert_eq!(actual, expected);
}

fn with_server<T>(test: T) -> ()
where
    T: FnOnce(Client) -> () + panic::UnwindSafe,
{
    let (client, db_path) = start_server();
    let result = panic::catch_unwind(move || test(client));
    fs::remove_dir_all(&db_path).expect("Could not delete DB directory");
    assert!(result.is_ok())
}

fn start_server() -> (Client, String) {
    let server_addr: SocketAddr = "127.0.0.1:0".parse().unwrap();
    let db_path = unique_tmp_db_path();
    let db = MetricStore::open(&db_path).expect("Could not open db");
    let server = Server::new(&server_addr, db).expect("Could not start server");
    let addr = server
        .local_addr()
        .expect("Could not retrieve server address");
    thread::spawn(move || server.run());
    let client = Client::new(addr);
    (client, db_path)
}

fn unique_tmp_db_path() -> String {
    let mut path = env::temp_dir();
    let db_name = format!("testdb_{}", Uuid::new_v4());
    path.push(db_name);
    path.to_str()
        .expect("Could not construct DB path")
        .to_string()
}
