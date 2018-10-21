extern crate caesium_core;
extern crate clap;
extern crate rand;

use caesium_core::encode::Encodable;
use caesium_core::get_sketch_type;
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
use std::time::Duration;

fn main() -> Result<(), Error> {
    let args = parse_args()?;
    println!("Using sketch type {:?}", get_sketch_type());
    let data = read_data_file(&args.data_path)?;
    let calc = if args.summarize_error {
        Some(ErrorCalculator::new(&data))
    } else {
        None
    };

    for i in 0..args.num_trials {
        println!("Trial {}", i);
        let mut timer = Timer::new();
        let partitions = choose_merge_partitions(data.len(), args.num_merges);
        let sketch = build_sketch(&data, &partitions[..], &mut timer);

        if args.summarize_size {
            summarize_size(&sketch);
        }

        if args.summarize_error {
            let mut readable = sketch.to_readable();
            if let Some(ref calc) = calc {
                summarize_error(calc, &mut readable);
            }
        }

        println!("================")
    }

    Ok(())
}

#[derive(Debug)]
struct Args {
    data_path: String,
    num_merges: usize,
    num_trials: usize,
    summarize_size: bool,
    summarize_error: bool,
}

fn parse_args() -> Result<Args, Error> {
    let matches = App::new("Quantile tool")
        .about("Calculate error and size of quantile sketches (useful for testing)")
        .arg(
            Arg::with_name("DATA_PATH")
            .index(1)
            .required(true)
            .help("Path to data file, one unsigned 32-bit integer per line")
        )
        .arg(
            Arg::with_name("NUM_MERGES")
            .long("num-merges")
            .short("n")
            .takes_value(true)
            .help("If provided, split dataset into NUM_MERGES parts, then merge the parts before querying")
        )
        .arg(
            Arg::with_name("NUM_TRIALS")
            .long("num-trials")
            .short("t")
            .takes_value(true)
            .help("Number of trials to run (default 1)")
        )
        .arg(
            Arg::with_name("SUMMARIZE_SIZE")
            .long("summarize-size")
            .short("s")
            .help("If set, summarize the sketch size")
        )
        .arg(
            Arg::with_name("SUMMARIZE_ERROR")
            .long("summarize-error")
            .short("e")
            .help("If set, summarize the normalized rank error")
        )
        .get_matches();

    let data_path = matches.value_of("DATA_PATH").unwrap().to_string();
    let num_merges = matches
        .value_of("NUM_MERGES")
        .unwrap_or("0")
        .parse::<usize>()?;
    let num_trials = matches
        .value_of("NUM_TRIALS")
        .unwrap_or("1")
        .parse::<usize>()?;
    let summarize_size = matches.is_present("SUMMARIZE_SIZE");
    let summarize_error = matches.is_present("SUMMARIZE_ERROR");
    Ok(Args {
        data_path,
        num_merges,
        num_trials,
        summarize_size,
        summarize_error,
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
    let mut partitions: Vec<usize> = candidates.iter().take(num_merges).map(|x| *x).collect();
    partitions.push(data_len - 1);
    partitions.sort_unstable();
    partitions
}

fn build_sketch(data: &[u32], partitions: &[usize], timer: &mut Timer) -> WritableSketch {
    debug_assert!(partitions.len() <= data.len());
    debug_assert!(partitions.iter().all(|p| *p < data.len()));

    let mut sketches = Vec::with_capacity(partitions.len());
    let mut last_end = 0;

    timer.start();

    // Inserts
    for &p in partitions.iter() {
        let mut s = WritableSketch::new();
        for i in last_end..p {
            s.insert(data[i]);
        }
        sketches.push(s);
        last_end = p;
    }

    // Merges
    let mut merged = WritableSketch::new();
    for s in sketches {
        merged = merged.merge(s)
    }

    let duration = timer.stop().unwrap();
    summarize_time(duration);

    merged
}

fn summarize_time(d: Duration) {
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
