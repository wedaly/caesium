use caesium_core::protocol::messages::InsertMessage;
use circuit::CircuitState;
use client::Client;
use std::cmp::min;
use std::sync::mpsc::Receiver;
use std::sync::{Arc, RwLock};
use std::thread;
use std::time::Duration;

pub fn sender_thread(
    mut client: Client,
    input: Receiver<InsertMessage>,
    circuit: Arc<RwLock<CircuitState>>,
) {
    loop {
        match input.recv() {
            Ok(msg) => send_until_success(msg, &mut client, &circuit),
            Err(_) => {
                info!("Channel closed, stopping sender thread");
                break;
            }
        }
    }
}

enum SendResult {
    Success,
    RetryLater,
}

fn send_until_success(
    msg: InsertMessage,
    mut client: &mut Client,
    circuit_lock: &Arc<RwLock<CircuitState>>,
) {
    let mut retry_count = 0usize;
    loop {
        match send_to_backend(&msg, &mut client) {
            SendResult::Success => {
                debug!("Sent insert message to backend for metric {:?}", msg.metric);
                set_circuit_state(circuit_lock, CircuitState::Closed);
                break;
            }
            SendResult::RetryLater => {
                set_circuit_state(circuit_lock, CircuitState::Open);
            }
        }

        let delay = retry_delay(retry_count);
        retry_count += 1;
        info!(
            "Retry request to backend in {:?} (attempt {})",
            delay, retry_count
        );
        thread::sleep(delay);
    }
}

fn send_to_backend(msg: &InsertMessage, client: &mut Client) -> SendResult {
    match client.send(&msg) {
        Ok(_) => SendResult::Success,
        Err(err) => {
            error!("Error sending message to backend: {:?}", err);
            SendResult::RetryLater
        }
    }
}

fn retry_delay(retry_count: usize) -> Duration {
    const MAX_DELAY_EXPONENT: usize = 12;
    let exponent = min(retry_count, MAX_DELAY_EXPONENT);
    Duration::from_millis(10 * (1 << exponent))
}

fn set_circuit_state(circuit_lock: &Arc<RwLock<CircuitState>>, new_state: CircuitState) {
    let mut state_mut = circuit_lock
        .write()
        .expect("Could not acquire write lock on circuit");
    *state_mut = new_state;
}
