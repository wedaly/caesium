use caesium_core::time::clock::SystemClock;
use processor::ProcessorCommand;
use regex::Regex;
use std::io;
use std::net::UdpSocket;
use std::str;
use std::sync::mpsc::Sender;
use std::time::Duration;
use window::WindowTracker;

const MAX_MSG_LEN: usize = 1024;
const READ_TIMEOUT_MS: u64 = 1000;

pub fn listener_thread(
    socket: UdpSocket,
    out: Sender<ProcessorCommand>,
    window_size: u64,
) -> Result<(), io::Error> {
    let clock = SystemClock::new();
    let mut window_tracker = WindowTracker::new(window_size, &clock);
    let mut buf = [0; MAX_MSG_LEN];
    socket.set_read_timeout(Some(Duration::from_millis(READ_TIMEOUT_MS)))?;
    loop {
        match socket.recv(&mut buf) {
            Ok(n) => handle_datagram(&buf[..n], &out),
            Err(err) => match err.kind() {
                io::ErrorKind::WouldBlock | io::ErrorKind::TimedOut => {}
                _ => error!("Error receving msg: {:?}", err),
            },
        }

        if let Some(window) = window_tracker.update(&clock) {
            out.send(ProcessorCommand::CloseWindow(window))
                .expect("Could not send command to processor thread");
        }
    }
}

fn handle_datagram(buf: &[u8], out: &Sender<ProcessorCommand>) {
    match str::from_utf8(buf) {
        Ok(s) => {
            trace!("Received input: {}", &s);
            match parse_metric_str(&s) {
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

fn parse_metric_str(s: &str) -> Option<ProcessorCommand> {
    lazy_static! {
        static ref INSERT_CMD_RE: Regex = Regex::new(
            "^(?P<metric>[a-zA-Z][a-zA-Z0-9._-]*):(?P<value>[0-9]+)[|]ms([|]@[0-9]+[.][0-9]+)?$"
        ).expect("Could not compile regex");
    }

    INSERT_CMD_RE
        .captures(s)
        .and_then(|c| match (c.name("metric"), c.name("value")) {
            (Some(metric_match), Some(value_match)) => {
                value_match.as_str().parse::<u32>().ok().map(|value| {
                    let metric_name = metric_match.as_str().to_string();
                    ProcessorCommand::InsertMetric(metric_name, value)
                })
            }
            _ => None,
        })
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::mpsc::{channel, RecvTimeoutError};
    use std::time::Duration;

    #[test]
    fn it_parses_commands() {
        let data = "foo:1234|ms".as_bytes();
        let (tx, rx) = channel();
        handle_datagram(&data, &tx);
        match rx.recv_timeout(Duration::from_millis(1000)) {
            Ok(cmd) => match cmd {
                ProcessorCommand::InsertMetric(metric, value) => {
                    assert_eq!(metric, "foo");
                    assert_eq!(value, 1234);
                }
                _ => assert!(false, "Unexpected processor command type"),
            },
            Err(err) => assert!(false, "Error receiving result: {}", err),
        }
    }

    #[test]
    fn it_ignores_invalid_commands() {
        let data = "invalid".as_bytes();
        let (tx, rx) = channel();
        handle_datagram(&data, &tx);
        match rx.recv_timeout(Duration::from_millis(500)) {
            Err(RecvTimeoutError::Timeout) => {}
            _ => assert!(false, "Expected timeout error"),
        }
    }

    #[test]
    fn it_parses_insert_cmd() {
        assert_cmd("foo:12345|ms", "foo", 12345);
    }

    #[test]
    fn it_ignores_sample_rate() {
        assert_cmd("foo:12345|ms|@0.1", "foo", 12345);
    }

    #[test]
    fn it_accepts_metric_name_with_numbers() {
        assert_cmd("foo123:12345|ms", "foo123", 12345);
    }

    #[test]
    fn it_accepts_metric_name_with_period() {
        assert_cmd(
            "region.us.server.abc:12345|ms",
            "region.us.server.abc",
            12345,
        );
    }

    #[test]
    fn it_accepts_metric_name_with_hyphen() {
        assert_cmd("us-west:12345|ms", "us-west", 12345);
    }

    #[test]
    fn it_accepts_metric_name_with_underscore() {
        assert_cmd("env_prod:12345|ms", "env_prod", 12345);
    }

    #[test]
    fn it_accepts_metric_name_with_capital() {
        assert_cmd("FooBar:12345|ms", "FooBar", 12345);
    }

    #[test]
    fn it_rejects_metric_name_starting_with_nonalpha() {
        assert_invalid(&"1foo:bar|ms");
        assert_invalid(&".foo:bar|ms");
        assert_invalid(&"-foo:bar|ms");
        assert_invalid(&"_foo:bar|ms");
    }

    #[test]
    fn it_rejects_partial_match() {
        assert_invalid("&&&&||||||foo:123|ms||||||&&&&");
        assert_invalid("foo:123|ms||||||&&&&");
        assert_invalid("&&&&||||||foo:123|ms");
    }

    #[test]
    fn it_handles_invalid_commands() {
        assert_invalid(&"");
        assert_invalid(&"invalid");
        assert_invalid(&":123|ms");
        assert_invalid(&"foo:|ms");
        assert_invalid(&"foo|ms");
        assert_invalid(&"foo:bar|ms");
        assert_invalid(&"foo|bar|ms");
        assert_invalid(&"foo|123|ms");
    }

    fn assert_cmd(s: &str, expected_metric: &str, expected_val: u32) {
        println!("Checking that '{}' is a valid insert command", s);
        let cmd = parse_metric_str(s).expect("Could not parse cmd");
        match cmd {
            ProcessorCommand::InsertMetric(metric, value) => {
                assert_eq!(metric, expected_metric);
                assert_eq!(value, expected_val);
            }
            _ => assert!(false, "Expected insert metric command"),
        }
    }

    fn assert_invalid(s: &str) {
        println!("Checking that '{}' is invalid", s);
        let cmd = parse_metric_str(s);
        assert!(cmd.is_none());
    }
}
