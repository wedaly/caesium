extern crate mio;
extern crate rand;

mod rate;
mod writer;

use mio::{Events, Poll, PollOpt, Ready, Token};
use std::io;
use std::net::SocketAddr;
use std::time::Duration;
use writer::Writer;

pub fn generate_load(
    daemon_addr: &SocketAddr,
    num_writers: usize,
    num_metrics: usize,
    rate_limit: Option<usize>,
) -> Result<(), io::Error> {
    let poll = Poll::new()?;
    let mut writers = init_writers(daemon_addr, num_writers, num_metrics, rate_limit)?;
    register_writers(&writers, &poll)?;
    run_event_loop(&poll, &mut writers)
}

fn init_writers(
    daemon_addr: &SocketAddr,
    num_writers: usize,
    num_metrics: usize,
    rate_limit: Option<usize>,
) -> Result<Vec<Writer>, io::Error> {
    assert!(num_metrics > 0);
    let mut writers = Vec::with_capacity(num_writers);
    for i in 0..num_writers {
        let metric_id = if num_metrics <= num_writers {
            i % num_metrics
        } else {
            (num_metrics * i) / num_writers
        };
        let w = Writer::new(daemon_addr, metric_id, num_metrics, rate_limit)?;
        writers.push(w);
    }
    Ok(writers)
}

fn register_writers(writers: &[Writer], poll: &Poll) -> Result<(), io::Error> {
    for (idx, w) in writers.iter().enumerate() {
        let token = Token(idx);
        poll.register(w.socket(), token, Ready::writable(), PollOpt::level())?;
    }
    Ok(())
}

fn run_event_loop(poll: &Poll, writers: &mut [Writer]) -> Result<(), io::Error> {
    let mut events = Events::with_capacity(1024);
    loop {
        poll.poll(&mut events, Some(Duration::from_millis(1000)))?;
        for event in events.iter() {
            match event.token() {
                Token(t) if t < writers.len() => {
                    let w = writers.get_mut(t).expect("Could not retrieve writer");
                    w.write()?;
                }
                _ => unreachable!(),
            }
        }
    }
}
