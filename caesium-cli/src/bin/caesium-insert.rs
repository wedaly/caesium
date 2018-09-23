extern crate caesium_core;
extern crate clap;

use caesium_core::encode::frame::FrameEncoder;
use caesium_core::encode::EncodableError;
use caesium_core::protocol::messages::InsertMessage;
use caesium_core::quantile::writable::WritableSketch;
use caesium_core::time::timestamp::TimeStamp;
use caesium_core::time::window::TimeWindow;
use clap::{App, Arg};
use std::env;
use std::fs::File;
use std::io;
use std::time::{SystemTime, UNIX_EPOCH};
use std::io::{BufRead, BufReader};
use std::net::{SocketAddr, TcpStream, ToSocketAddrs};

fn main() -> Result<(), Error> {
    let args = parse_args()?;
    let insert_cmds = load_data_file(&args.data_path)?;
    let mut socket = TcpStream::connect(&args.server_addr)?;
    let mut frame_encoder = FrameEncoder::new();
    let start_time = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap()
        .as_secs();
    for cmd in insert_cmds.iter() {
        println!("Inserting {:?}", cmd);
        insert_sketches(&cmd, start_time, &mut socket, &mut frame_encoder)?;
    }
    Ok(())
}

#[derive(Debug)]
struct InsertCommand {
    num_sketches: usize,
    metric_name: String,
}

fn load_data_file(path: &str) -> Result<Vec<InsertCommand>, Error> {
    println!("Loading data from {}", path);
    let f = BufReader::new(File::open(path)?);
    let commands: Vec<InsertCommand> = f
        .lines()
        .enumerate()
        .filter_map(|(line_num, line_result)| match line_result {
            Ok(line) => parse_line(&line, line_num),
            Err(err) => {
                println!("Could not read line: {:?}", err);
                None
            }
        })
        .collect();
    Ok(commands)
}

fn parse_line(line: &str, line_num: usize) -> Option<InsertCommand> {
    let mut parts = line.splitn(2, " ");
    match (parts.next(), parts.next()) {
        (Some(num_str), Some(name_str)) => match num_str.parse::<usize>() {
            Ok(num_sketches) => Some(InsertCommand {
                num_sketches,
                metric_name: name_str.to_string(),
            }),
            Err(_) => {
                println!("Could not parse sketch count on line {}", line_num);
                None
            }
        },
        _ => {
            println!("Could not parse line {}", line_num);
            None
        }
    }
}

fn insert_sketches(
    cmd: &InsertCommand,
    start_time: u64,
    socket: &mut TcpStream,
    frame_encoder: &mut FrameEncoder,
) -> Result<(), Error> {
    let sketch = build_sketch();
    for i in 0..cmd.num_sketches {
        let window = window_for_idx(start_time, i);
        let msg = InsertMessage {
            metric: cmd.metric_name.clone(),
            window,
            sketch: sketch.clone(),
        };
        frame_encoder.encode_framed_msg(&msg, socket)?;
    }
    Ok(())
}

const WINDOW_SIZE: u64 = 10;

fn window_for_idx(start_time: u64, idx: usize) -> TimeWindow {
    let start = start_time + (idx as TimeStamp) * WINDOW_SIZE;
    let end = start + WINDOW_SIZE;
    TimeWindow::new(start, end)
}

fn build_sketch() -> WritableSketch {
    let mut sketch = WritableSketch::new();
    for i in 0..100000 {
        sketch.insert(i as u64);
    }
    sketch
}

#[derive(Debug)]
struct Args {
    data_path: String,
    server_addr: SocketAddr,
}

#[cfg(not(feature = "baseline"))]
const FEATURE_STR: &'static str = "Compiled using KLL sketch implementation";

#[cfg(feature = "baseline")]
const FEATURE_STR: &'static str = "Compiled using baseline sketch implementation";

fn parse_args() -> Result<Args, Error> {
    let matches = App::new("Sketch insert tool")
        .about("Insert sketch data directly to the server (useful for testing)")
        .after_help(FEATURE_STR)
        .arg(
            Arg::with_name("DATA_PATH")
                .index(1)
                .required(true)
                .help("Path to data file, each line has the form `SKETCH_COUNT METRIC_NAME`")
        )
        .arg(
            Arg::with_name("SERVER_ADDR")
                .short("a")
                .long("addr")
                .takes_value(true)
                .help("Network address of server (defaults to $CAESIUM_SERVER_INSERT_ADDR, then 127.0.0.1:8001)")
        )
        .get_matches();

    let data_path = matches
        .value_of("DATA_PATH")
        .map(|s| s.to_string())
        .unwrap();

    let default_addr =
        env::var("CAESIUM_SERVER_INSERT_ADDR").unwrap_or_else(|_| "127.0.0.1:8001".to_string());
    let server_addr = matches
        .value_of("SERVER_ADDR")
        .unwrap_or(&default_addr)
        .to_socket_addrs()?
        .next()
        .ok_or(Error::ArgError("Expected socket address"))?;

    Ok(Args {
        data_path,
        server_addr,
    })
}

#[derive(Debug)]
enum Error {
    IOError(io::Error),
    EncodableError(EncodableError),
    ArgError(&'static str),
}

impl From<io::Error> for Error {
    fn from(err: io::Error) -> Error {
        Error::IOError(err)
    }
}

impl From<EncodableError> for Error {
    fn from(err: EncodableError) -> Error {
        Error::EncodableError(err)
    }
}
