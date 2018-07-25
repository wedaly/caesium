use network::daemon::command::InsertCmd;
use network::error::NetworkError;
use std::net::UdpSocket;
use std::str;
use std::sync::mpsc::Sender;
use time::clock::{Clock, SystemClock};

const MAX_MSG_LEN: usize = 1024;

pub fn listener_thread(socket: UdpSocket, out: Sender<InsertCmd>) -> Result<(), NetworkError> {
    let clock = SystemClock::new();
    let mut buf = [0; MAX_MSG_LEN];
    loop {
        match socket.recv(&mut buf) {
            Ok(n) => handle_datagram(&buf[..n], &out, &clock),
            Err(err) => error!("Error receving msg: {:?}", err),
        }
    }
}

fn handle_datagram(buf: &[u8], out: &Sender<InsertCmd>, clock: &Clock) {
    match str::from_utf8(buf) {
        Ok(s) => {
            debug!("Received input: {}", &s);
            match InsertCmd::parse_from_str(&s, clock) {
                Some(cmd) => {
                    out.send(cmd)
                        .expect("Could not send command to processor thread");
                }
                None => info!("Could not parse string as cmd: {}", &s),
            }
        }
        Err(err) => {
            warn!("Could not parse input as string: {:?}", err);
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::mpsc::{channel, RecvTimeoutError};
    use std::time::Duration;
    use time::clock::MockClock;

    #[test]
    fn it_parses_commands() {
        let clock = MockClock::new(60);
        let data = "foo:1234|ms".as_bytes();
        let (tx, rx) = channel();
        handle_datagram(&data, &tx, &clock);
        match rx.recv_timeout(Duration::from_millis(1000)) {
            Ok(cmd) => {
                assert_eq!(cmd.metric(), "foo");
                assert_eq!(cmd.value(), 1234);
                assert_eq!(cmd.ts(), 60);
            }
            Err(err) => panic!("Error receiving result: {}", err),
        }
    }

    #[test]
    fn it_ignores_invalid_commands() {
        let clock = MockClock::new(60);
        let data = "invalid".as_bytes();
        let (tx, rx) = channel();
        handle_datagram(&data, &tx, &clock);
        match rx.recv_timeout(Duration::from_millis(500)) {
            Err(RecvTimeoutError::Timeout) => {}
            _ => panic!("Expected timeout error"),
        }
    }
}
