extern crate caesium_core;
extern crate mio;
extern crate rand;
extern crate time;
extern crate uuid;

#[macro_use]
extern crate log;

pub mod error;
mod rate;
mod report;
mod worker;

use caesium_core::time::clock::SystemClock;
use error::Error;
use mio::{Events, Poll, Token};
use report::event::Event;
use report::reporter::Reporter;
use report::sink::LogSink;
use std::fs::File;
use std::io;
use std::io::BufRead;
use std::io::BufReader;
use std::net::SocketAddr;
use std::sync::mpsc::{channel, Receiver, Sender};
use std::sync::{Arc, Mutex};
use std::thread;
use std::time::Duration;
use worker::daemon_writer::DaemonWriter;
use worker::server_reader::ServerReader;
use worker::server_writer::ServerWriter;
use worker::Worker;

pub struct DaemonWriterConfig {
    pub addr: SocketAddr,
    pub num_workers: usize,
    pub rate_limit: Option<usize>,
    pub num_metrics: usize,
}

pub struct ServerReaderConfig {
    pub addr: SocketAddr,
    pub num_workers: usize,
    pub query_file_path: String,
    pub rate_limit: Option<usize>,
}

pub struct ServerWriterConfig {
    pub addr: SocketAddr,
    pub num_workers: usize,
    pub sketch_size: usize,
    pub rate_limit: Option<usize>,
}

pub fn generate_load(
    report_sample_interval: u64,
    daemon_writer_config: DaemonWriterConfig,
    server_reader_config: ServerReaderConfig,
    server_writer_config: ServerWriterConfig,
) -> Result<(), Error> {
    let (tx, rx) = channel();
    start_reporter_thread(rx, report_sample_interval);

    let poll = Poll::new()?;
    let mut workers = init_workers(
        daemon_writer_config,
        server_reader_config,
        server_writer_config,
        tx.clone(),
        &poll,
    )?;
    run_event_loop(&poll, &mut workers)
}

fn start_reporter_thread(rx: Receiver<Event>, sample_interval: u64) {
    thread::spawn(move || {
        let reporter = Reporter::new(rx, sample_interval);
        let sink = LogSink::new();
        let sink_mutex = Arc::new(Mutex::new(sink));
        reporter.run(sink_mutex);
    });
}

fn init_workers(
    daemon_writer_config: DaemonWriterConfig,
    server_reader_config: ServerReaderConfig,
    server_writer_config: ServerWriterConfig,
    tx: Sender<Event>,
    poll: &Poll,
) -> Result<Vec<Box<Worker>>, Error> {
    let num_workers = daemon_writer_config.num_workers
        + server_reader_config.num_workers
        + server_writer_config.num_workers;
    let mut workers = Vec::with_capacity(num_workers);
    init_daemon_writers(&mut workers, daemon_writer_config, tx.clone())?;
    init_server_readers(&mut workers, server_reader_config, tx.clone())?;
    init_server_writers(&mut workers, server_writer_config, tx.clone())?;
    register_workers(poll, &mut workers)?;
    Ok(workers)
}

fn init_daemon_writers(
    workers: &mut Vec<Box<Worker>>,
    config: DaemonWriterConfig,
    tx: Sender<Event>,
) -> Result<(), io::Error> {
    assert!(config.num_metrics > 0);
    for i in 0..config.num_workers {
        let metric_id = choose_start_for_worker(i, config.num_workers, config.num_metrics);
        let w = DaemonWriter::new(
            &config.addr,
            metric_id,
            config.num_metrics,
            config.rate_limit,
            tx.clone(),
        )?;
        workers.push(Box::new(w));
    }
    Ok(())
}

fn init_server_readers(
    workers: &mut Vec<Box<Worker>>,
    config: ServerReaderConfig,
    tx: Sender<Event>,
) -> Result<(), Error> {
    let queries = load_queries(&config.query_file_path)?;
    if queries.len() < 1 {
        return Err(Error::ConfigError(
            "Query file must have at least one query",
        ));
    }
    for i in 0..config.num_workers {
        let query_idx = choose_start_for_worker(i, config.num_workers, queries.len());
        let w = ServerReader::new(
            i,
            &config.addr,
            &queries,
            query_idx,
            config.rate_limit,
            tx.clone(),
        );
        workers.push(Box::new(w));
    }
    Ok(())
}

fn init_server_writers(
    workers: &mut Vec<Box<Worker>>,
    config: ServerWriterConfig,
    tx: Sender<Event>,
) -> Result<(), Error> {
    let clock = SystemClock::new();
    for _ in 0..config.num_workers {
        let w = ServerWriter::new(
            &config.addr,
            config.sketch_size,
            config.rate_limit,
            &clock,
            tx.clone(),
        )?;
        workers.push(Box::new(w));
    }
    Ok(())
}

fn choose_start_for_worker(worker_idx: usize, num_workers: usize, num_values: usize) -> usize {
    if num_values <= num_workers {
        worker_idx % num_workers
    } else {
        (num_workers * worker_idx) / num_workers
    }
}

fn load_queries(path: &str) -> Result<Vec<String>, io::Error> {
    let file = BufReader::new(File::open(path)?);
    let queries: Vec<String> = file
        .lines()
        .filter_map(|line_result| line_result.ok())
        .collect();
    Ok(queries)
}

fn register_workers(poll: &Poll, workers: &mut [Box<Worker>]) -> Result<(), io::Error> {
    for (idx, w) in workers.iter_mut().enumerate() {
        let token = Token(idx);
        w.register(token, poll)?;
    }
    Ok(())
}

fn run_event_loop(poll: &Poll, workers: &mut [Box<Worker>]) -> Result<(), Error> {
    let mut events = Events::with_capacity(1024);
    loop {
        poll.poll(&mut events, Some(Duration::from_millis(1000)))?;
        for event in events.iter() {
            match event.token() {
                Token(t) if t < workers.len() => {
                    let w = workers.get_mut(t).expect("Could not retrieve worker");
                    if event.readiness().is_writable() {
                        if let Err(err) = w.write() {
                            error!("Worker {} error while writing: {:?}", t, err);
                        }
                    } else if event.readiness().is_readable() {
                        if let Err(err) = w.read() {
                            error!("Worker {} error while reading: {:?}", t, err);
                        }
                    }
                    w.register(Token(t), poll)?;
                }
                _ => unreachable!(),
            }
        }
    }
}
