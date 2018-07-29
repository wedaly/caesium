mod command;
mod listener;
mod processor;
mod sender;
mod state;

use network::circuit::CircuitState;
use network::client::Client;
use network::daemon::listener::listener_thread;
use network::daemon::processor::processor_thread;
use network::daemon::sender::sender_thread;
use network::error::NetworkError;
use std::net::{SocketAddr, UdpSocket};
use std::sync::mpsc::channel;
use std::sync::{Arc, RwLock};
use std::thread;

pub fn run_daemon(source_addr: SocketAddr, sink_addr: SocketAddr) -> Result<(), NetworkError> {
    let socket = UdpSocket::bind(source_addr)?;
    let client = Client::new(sink_addr);
    let (circuit_ref1, circuit_ref2) = shared_circuit();
    let (listener_out, processor_in) = channel();
    let (processor_out, sender_in) = channel();
    thread::spawn(move || processor_thread(processor_in, processor_out, circuit_ref1));
    thread::spawn(move || sender_thread(client, sender_in, circuit_ref2));
    info!("Listening on {}, publishing to {}", source_addr, sink_addr);
    listener_thread(socket, listener_out)
}

fn shared_circuit() -> (Arc<RwLock<CircuitState>>, Arc<RwLock<CircuitState>>) {
    let circuit_lock = RwLock::new(CircuitState::Closed);
    let circuit_ref1 = Arc::new(circuit_lock);
    let circuit_ref2 = circuit_ref1.clone();
    (circuit_ref1, circuit_ref2)
}
