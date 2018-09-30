extern crate caesium_core;
extern crate clap;
extern crate rand;

use caesium_core::encode::Encodable;
use caesium_core::quantile::error::ErrorCalculator;
use caesium_core::quantile::readable::ReadableSketch;
use caesium_core::quantile::writable::WritableSketch;
use caesium_core::time::timer::Timer;
use clap::{App, Arg};
use rand::Rng;
use std::fs::File;
use std::io;
use std::io::{BufRead, BufReader};
use std::num::ParseIntError;

fn main() -> Result<(), Error> {
    let args = parse_args()?;
    let data = read_data_file(&args.data_path)?;
    let mut timer = Timer::new();
    let partitions = choose_merge_partitions(data.len(), args.num_merges);

    let sketch = build_sketch(&data, &partitions[..], &mut timer);
    summarize_time(&timer);
    summarize_size(&sketch);
    let calc = ErrorCalculator::new(&data);
    let mut readable = sketch.to_readable();
    summarize_error(&calc, &mut readable);

    Ok(())
}

#[derive(Debug)]
struct Args {
    data_path: String,
    num_merges: usize,
}

fn parse_args() -> Result<Args, Error> {
    let matches = App::new("Quantile tool")
        .about("Calculate error and size of quantile sketches (useful for testing)")
        .arg(
            Arg::with_name("DATA_PATH")
            .index(1)
            .required(true)
            .help("Path to data file, one unsigned 64-bit integer per line")
        )
        .arg(
            Arg::with_name("NUM_MERGES")
            .long("num-merges")
            .short("n")
            .takes_value(true)
            .help("If provided, split dataset into NUM_MERGES parts, then merge the parts before querying")
        )
        .get_matches();

    let data_path = matches.value_of("DATA_PATH").unwrap().to_string();
    let num_merges = matches
        .value_of("NUM_MERGES")
        .unwrap_or("0")
        .parse::<usize>()?;
    Ok(Args {
        data_path,
        num_merges,
    })
}

fn read_data_file(path: &str) -> Result<Vec<u32>, Error> {
    let file = File::open(path)?;
    let reader = BufReader::new(file);
    let values = reader
        .lines()
        .filter_map(|result| {
            result
                .map_err(|e| Error::IOError(e))
                .and_then(|l| l.parse::<u32>().map_err(From::from))
                .ok()
        }).collect();
    Ok(values)
}

fn choose_merge_partitions(data_len: usize, num_merges: usize) -> Vec<usize> {
    let mut candidates: Vec<usize> = (0..data_len).collect();
    rand::thread_rng().shuffle(&mut candidates);
    candidates.iter().take(num_merges).map(|x| *x).collect()
}

fn build_sketch(data: &[u32], partitions: &[usize], timer: &mut Timer) -> WritableSketch {
    debug_assert!(partitions.len() <= data.len());
    debug_assert!(partitions.iter().all(|p| *p < data.len()));
    let mut sorted_partitions = Vec::with_capacity(partitions.len());
    sorted_partitions.extend_from_slice(partitions);
    sorted_partitions.sort_unstable();

    let mut tmp = None;
    let mut result = None;
    let mut b = 0;

    timer.start();
    data.iter().enumerate().for_each(|(idx, val)| {
        let mut writable = match tmp.take() {
            None => WritableSketch::new(),
            Some(w) => w,
        };

        writable.insert(*val);

        let cutoff = match sorted_partitions.get(b) {
            None => data.len() - 1,
            Some(&x) => x,
        };

        if idx >= cutoff {
            result = match result.take() {
                None => Some(writable),
                Some(r) => Some(r.merge(writable)),
            };
            b += 1;
        } else {
            tmp = Some(writable);
        }
    });
    timer.stop();

    match result {
        None => WritableSketch::new(),
        Some(s) => s,
    }
}

fn summarize_time(timer: &Timer) {
    let d = timer.duration().expect("Could not retrieve timer duration");
    let ms = (d.as_secs() * 1_000) + (d.subsec_nanos() / 1_000_000) as u64;
    println!("total insert/merge time (ms) = {}", ms);
}

fn summarize_size(sketch: &WritableSketch) {
    let mut buf = Vec::new();
    sketch.encode(&mut buf).expect("Could not encode sketch");
    println!("encoded size (bytes) = {}", buf.len());
    println!("num stored values = {}", sketch.size());
}

fn summarize_error(calc: &ErrorCalculator, sketch: &mut ReadableSketch) {
    for i in 1..10 {
        let phi = (i as f64) / 10.0;
        let q = sketch.query(phi).expect("Could not query sketch");
        let err = calc.calculate_error(phi, q.approx_value);
        println!(
            "phi={}, approx={}, lower={}, upper={}, err={}",
            phi, q.approx_value, q.lower_bound, q.upper_bound, err
        );
    }
}

#[derive(Debug)]
enum Error {
    IOError(io::Error),
    ParseIntError(ParseIntError),
}

impl From<io::Error> for Error {
    fn from(err: io::Error) -> Error {
        Error::IOError(err)
    }
}

impl From<ParseIntError> for Error {
    fn from(err: ParseIntError) -> Error {
        Error::ParseIntError(err)
    }
}
