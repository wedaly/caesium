extern crate caesium_core;
extern crate regex;
extern crate slab;

#[macro_use]
extern crate lazy_static;

#[macro_use]
extern crate log;

mod circuit;
mod client;
mod listener;
mod processor;
mod sender;
mod window;

use circuit::CircuitState;
use client::Client;
use listener::listener_thread;
use processor::processor_thread;
use sender::sender_thread;
use std::io;
use std::net::{SocketAddr, UdpSocket};
use std::sync::mpsc::channel;
use std::sync::{Arc, RwLock};
use std::thread;

pub fn run_daemon(
    listen_addr: SocketAddr,
    publish_addr: SocketAddr,
    window_size: u64,
) -> Result<(), io::Error> {
    let socket = UdpSocket::bind(listen_addr)?;
    let client = Client::new(publish_addr);
    let (circuit_ref1, circuit_ref2) = shared_circuit();
    let (listener_out, processor_in) = channel();
    let (processor_out, sender_in) = channel();
    thread::spawn(move || processor_thread(processor_in, processor_out, circuit_ref1));
    thread::spawn(move || sender_thread(client, sender_in, circuit_ref2));
    info!(
        "Listening on {}, publishing to {}",
        listen_addr, publish_addr
    );
    listener_thread(socket, listener_out, window_size)
}

fn shared_circuit() -> (Arc<RwLock<CircuitState>>, Arc<RwLock<CircuitState>>) {
    let circuit_lock = RwLock::new(CircuitState::Closed);
    let circuit_ref1 = Arc::new(circuit_lock);
    let circuit_ref2 = circuit_ref1.clone();
    (circuit_ref1, circuit_ref2)
}
