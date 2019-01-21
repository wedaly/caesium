extern crate caesium_core;
extern crate clap;
extern crate rand;

use caesium_core::encode::frame::FrameEncoder;
use caesium_core::encode::EncodableError;
use caesium_core::get_sketch_type;
use caesium_core::protocol::messages::InsertMessage;
use caesium_core::quantile::writable::WritableSketch;
use caesium_core::time::timestamp::TimeStamp;
use caesium_core::time::window::TimeWindow;
use clap::{App, Arg};
use rand::rngs::SmallRng;
use rand::{FromEntropy, Rng};
use std::env;
use std::fs::File;
use std::io;
use std::io::{BufRead, BufReader};
use std::net::{SocketAddr, TcpStream, ToSocketAddrs};
use std::num::ParseIntError;
use std::time::{SystemTime, UNIX_EPOCH};

const MIN_VAL: u64 = 0;
const MAX_VAL: u64 = 10000;

fn main() -> Result<(), Error> {
    let args = parse_args()?;
    println!("Using sketch type {:?}", get_sketch_type());
    let insert_cmds = load_data_file(&args.data_path)?;
    let mut socket = TcpStream::connect(&args.server_addr)?;
    let mut frame_encoder = FrameEncoder::new();
    for cmd in insert_cmds.iter() {
        println!("Inserting {:?}", cmd);
        insert_sketches(
            &cmd,
            args.window_start,
            args.window_size,
            args.sketch_size,
            &mut socket,
            &mut frame_encoder,
        )?;
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
    window_start: u64,
    window_size: u64,
    sketch_size: usize,
    socket: &mut TcpStream,
    frame_encoder: &mut FrameEncoder,
) -> Result<(), Error> {
    for i in 0..cmd.num_sketches {
        let window = window_for_idx(window_start, window_size, i);
        let msg = InsertMessage {
            metric: cmd.metric_name.clone(),
            window,
            sketch: build_sketch(sketch_size),
        };
        frame_encoder.encode_framed_msg(&msg, socket)?;
    }
    Ok(())
}

fn window_for_idx(window_start: u64, window_size: u64, idx: usize) -> TimeWindow {
    let start = window_start + (idx as TimeStamp) * window_size;
    let end = start + window_size;
    TimeWindow::new(start, end)
}

fn build_sketch(size: usize) -> WritableSketch {
    let mut rng = SmallRng::from_entropy();
    let mut sketch = WritableSketch::new();
    for _ in 0..size {
        let v = rng.gen_range(MIN_VAL, MAX_VAL) as u32;
        sketch.insert(v);
    }
    sketch
}

#[derive(Debug)]
struct Args {
    data_path: String,
    server_addr: SocketAddr,
    window_start: u64,
    window_size: u64,
    sketch_size: usize,
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
        .arg(
            Arg::with_name("WINDOW_START")
                .long("window-start")
                .takes_value(true)
                .help("Start time of first window specified as the number of seconds since the UNIX epoch (defaults to current datetime)")
        )
        .arg(
            Arg::with_name("WINDOW_SIZE")
                .long("window-size")
                .takes_value(true)
                .help("Size of each window in seconds (default 10)")
        )
        .arg(
            Arg::with_name("SKETCH_SIZE")
            .long("sketch-size")
            .takes_value(true)
            .help("Number of values to insert into each sketch (default 1000)")
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

    let default_start = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap()
        .as_secs()
        .to_string();
    let window_start = matches
        .value_of("WINDOW_START")
        .unwrap_or(&default_start)
        .parse::<u64>()?;

    let window_size = matches
        .value_of("WINDOW_SIZE")
        .unwrap_or("10")
        .parse::<u64>()?;

    let sketch_size = matches
        .value_of("SKETCH_SIZE")
        .unwrap_or("1000")
        .parse::<usize>()?;

    Ok(Args {
        data_path,
        server_addr,
        window_start,
        window_size,
        sketch_size,
    })
}

#[derive(Debug)]
enum Error {
    IOError(io::Error),
    EncodableError(EncodableError),
    ParseIntError(ParseIntError),
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

impl From<ParseIntError> for Error {
    fn from(err: ParseIntError) -> Error {
        Error::ParseIntError(err)
    }
}
