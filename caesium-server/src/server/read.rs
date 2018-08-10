use server::read::worker::spawn_worker;
use std::io;
use std::net::{SocketAddr, TcpListener, TcpStream};
use std::sync::mpsc::{channel, Sender};
use std::sync::{Arc, Mutex};
use storage::store::MetricStore;

pub struct ReadServer {
    listener: TcpListener,
    tx: Sender<TcpStream>,
}

impl ReadServer {
    pub fn new(
        addr: &SocketAddr,
        num_workers: usize,
        db_ref: Arc<MetricStore>,
    ) -> Result<ReadServer, io::Error> {
        assert!(num_workers > 0);
        let listener = TcpListener::bind(addr)?;
        let (tx, rx) = channel();
        let rx_ref = Arc::new(Mutex::new(rx));
        for idx in 0..num_workers {
            spawn_worker(idx, rx_ref.clone(), db_ref.clone())
        }
        Ok(ReadServer { listener, tx })
    }

    pub fn local_addr(&self) -> Result<SocketAddr, io::Error> {
        self.listener.local_addr()
    }

    pub fn run(self) -> Result<(), io::Error> {
        info!("Listening for queries on {}", self.local_addr()?);
        for stream in self.listener.incoming() {
            match stream {
                Ok(stream) => {
                    if let Err(err) = self.tx.send(stream) {
                        error!("Error sending to worker threads: {:?}", err);
                    }
                }
                Err(err) => {
                    error!("Error accepting connection: {:?}", err);
                }
            }
        }
        Ok(())
    }
}

mod worker {
    use query::error::QueryError;
    use query::execute::{execute_query, QueryResult};
    use std::io;
    use std::io::{Read, Write};
    use std::net::TcpStream;
    use std::sync::mpsc::Receiver;
    use std::sync::{Arc, Mutex};
    use std::thread;
    use std::time::Duration;
    use storage::store::MetricStore;

    const READ_TIMEOUT_MS: u64 = 10000;
    const WRITE_TIMEOUT_MS: u64 = 10000;

    pub fn spawn_worker(
        id: usize,
        rx_lock: Arc<Mutex<Receiver<TcpStream>>>,
        db_ref: Arc<MetricStore>,
    ) {
        thread::spawn(move || process_messages(id, rx_lock, db_ref));
    }

    fn process_messages(
        id: usize,
        rx_lock: Arc<Mutex<Receiver<TcpStream>>>,
        db_ref: Arc<MetricStore>,
    ) {
        let mut query_buf = String::new();
        let db = &*db_ref;
        loop {
            let recv_result = rx_lock
                .lock()
                .expect("Could not acquire lock on worker msg queue")
                .recv();
            match recv_result {
                Ok(stream) => {
                    debug!("Processing query in worker thread with id {}", id);
                    if let Err(err) = handle_query(stream, &mut query_buf, db) {
                        error!("Error handling query: {:?}", err);
                    }
                }
                Err(err) => {
                    error!("Error receiving worker msg: {:?}", err);
                }
            }
        }
    }

    fn handle_query(
        mut stream: TcpStream,
        mut query_buf: &mut String,
        db: &MetricStore,
    ) -> Result<(), io::Error> {
        stream.set_read_timeout(Some(Duration::from_millis(READ_TIMEOUT_MS)))?;
        stream.set_write_timeout(Some(Duration::from_millis(WRITE_TIMEOUT_MS)))?;
        query_buf.clear();
        stream.read_to_string(&mut query_buf)?;
        match execute_query(&query_buf, db) {
            Ok(results) => write_query_results(results, stream),
            Err(err) => write_query_error(err, stream),
        }
    }

    fn write_query_results(
        mut results: Vec<QueryResult>,
        mut stream: TcpStream,
    ) -> Result<(), io::Error> {
        results
            .drain(..)
            .map(|r| match r {
                QueryResult::QuantileWindow(window, phi, quantile) => format!(
                    "start={}, end={}, phi={}, count={}, approx={}, lower={}, upper={}\n",
                    window.start(),
                    window.end(),
                    phi,
                    quantile.count,
                    quantile.approx_value,
                    quantile.lower_bound,
                    quantile.upper_bound
                ),
                QueryResult::MetricName(mut metric) => {
                    metric.push_str(&"\n");
                    metric
                }
            })
            .map(|line| stream.write_all(line.as_bytes()))
            .collect()
    }

    fn write_query_error(err: QueryError, mut stream: TcpStream) -> Result<(), io::Error> {
        let err_str = format!("[ERROR] {:?}\n", err);
        stream.write_all(err_str.as_bytes())
    }
}
