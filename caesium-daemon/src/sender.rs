use caesium_core::network::client::Client;
use caesium_core::network::message::Message;
use circuit::CircuitState;
use std::cmp::min;
use std::sync::mpsc::Receiver;
use std::sync::{Arc, RwLock};
use std::thread;
use std::time::Duration;

pub fn sender_thread(
    mut client: Client,
    input: Receiver<Message>,
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
    TransientFailure,
    PermanentFailure,
}

fn send_until_success(
    msg: Message,
    mut client: &mut Client,
    circuit_lock: &Arc<RwLock<CircuitState>>,
) {
    let mut retry_count = 0usize;
    loop {
        match send_to_backend(&msg, &mut client) {
            SendResult::Success | SendResult::PermanentFailure => {
                set_circuit_state(circuit_lock, CircuitState::Closed);
                break;
            }
            SendResult::TransientFailure => {
                set_circuit_state(circuit_lock, CircuitState::Open);
            }
        }

        let delay = retry_delay(retry_count);
        retry_count += 1;
        debug!(
            "Retry request to backend in {:?} (attempt {})",
            delay, retry_count
        );
        thread::sleep(delay);
    }
}

fn send_to_backend(req: &Message, client: &mut Client) -> SendResult {
    match client.request(&req) {
        Ok(Message::InsertSuccessResp) => {
            debug!("Sent metric to server");
            SendResult::Success
        }
        Ok(Message::ErrorResp(err)) => {
            error!("Error response from server: {:?}", err);
            SendResult::PermanentFailure
        }
        Ok(_) => {
            error!("Unexpected response message type");
            SendResult::PermanentFailure
        }
        Err(err) => {
            error!("Error sending to server: {:?}", err);
            SendResult::TransientFailure
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
