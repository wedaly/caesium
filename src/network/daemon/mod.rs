mod command;
mod listener;
mod processor;
mod sender;
mod state;

use network::client::Client;
use network::daemon::listener::listener_thread;
use network::daemon::processor::processor_thread;
use network::daemon::sender::sender_thread;
use network::error::NetworkError;
use std::net::{SocketAddr, UdpSocket};
use std::sync::mpsc::channel;
use std::thread;

pub fn run_daemon(source_addr: SocketAddr, sink_addr: SocketAddr) -> Result<(), NetworkError> {
    let socket = UdpSocket::bind(source_addr)?;
    let client = Client::new(sink_addr);
    let (listener_out, processor_in) = channel();
    let (processor_out, sender_in) = channel();
    thread::spawn(move || processor_thread(processor_in, processor_out));
    thread::spawn(move || sender_thread(client, sender_in));
    info!("Listening on {}", source_addr);
    listener_thread(socket, listener_out)
}
