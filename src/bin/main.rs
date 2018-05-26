extern crate caesium;
extern crate rand;
extern crate time;
use caesium::quantile::sketch::{WritableSketch, ReadableSketch};
use caesium::quantile::error::ErrorCalculator;
use rand::Rng;
use std::env;
use std::fs::File;
use std::io::Error as IOError;
use std::io::{BufRead, BufReader};

#[derive(Debug)]
enum Error {
    ArgParseError(String),
    IOError(IOError),
}

impl Error {
    fn arg_err(msg: &str) -> Error {
        Error::ArgParseError(String::from(msg))
    }
}

impl From<IOError> for Error {
    fn from(err: IOError) -> Error {
        Error::IOError(err)
    }
}

#[derive(Debug)]
struct Args {
    data_path: String,
    num_merges: usize,
}

fn main() -> Result<(), Error> {
    let args = parse_args()?;
    let data = read_data_file(&args.data_path)?;
    let partitions = choose_merge_partitions(data.len(), args.num_merges);

    let (writable_sketch, duration) = build_sketch(&data, &partitions[..]);
    summarize_space(&writable_sketch);
    summarize_time(data.len(), duration);

    let readable_sketch = writable_sketch.to_readable_sketch();
    let calc = ErrorCalculator::new(&data);
    summarize_error(&calc, &readable_sketch);

    Ok(())
}

fn parse_args() -> Result<Args, Error> {
    let data_path = env::args()
        .nth(1)
        .ok_or(Error::arg_err("Missing required argument `data_path`"))?;
    let num_merges = env::args()
        .nth(2)
        .map_or(Ok(0), |s| s.parse::<usize>())
        .map_err(|_| Error::arg_err("Could not parse integer for arg `num_merges`"))?;
    Ok(Args {
        data_path: data_path,
        num_merges: num_merges,
    })
}

fn read_data_file(path: &str) -> Result<Vec<u64>, Error> {
    let file = File::open(path)?;
    let reader = BufReader::new(file);
    let values = reader
        .lines()
        .filter_map(|line| parse_val_from_line(line))
        .collect();
    Ok(values)
}

fn parse_val_from_line(line: Result<String, IOError>) -> Option<u64> {
    if let Ok(l) = line {
        if let Ok(v) = l.parse::<u64>() {
            Some(v)
        } else {
            None
        }
    } else {
        None
    }
}

fn choose_merge_partitions(data_len: usize, num_merges: usize) -> Vec<usize> {
    let mut candidates: Vec<usize> = (0..data_len).collect();
    rand::thread_rng().shuffle(&mut candidates);
    candidates.iter().take(num_merges).map(|x| *x).collect()
}

fn build_sketch(data: &[u64], partitions: &[usize]) -> (WritableSketch, u64) {
    debug_assert!(partitions.len() <= data.len());
    debug_assert!(partitions.iter().all(|p| *p < data.len()));
    let mut sorted_partitions = Vec::with_capacity(partitions.len());
    sorted_partitions.extend_from_slice(partitions);
    sorted_partitions.sort_unstable();

    let mut tmp = WritableSketch::new();
    let mut result = WritableSketch::new();
    let mut b = 0;
    let start_ns = time::precise_time_ns();
    data.iter().enumerate().for_each(|(idx, val)| {
        let cutoff = match sorted_partitions.get(b) {
            None => data.len() - 1,
            Some(&x) => x,
        };
        tmp.insert(*val);
        if idx >= cutoff {
            result.merge(&tmp);
            tmp.reset();
            b += 1;
        }
    });
    let duration = time::precise_time_ns() - start_ns;
    (result, duration)
}

fn summarize_space(sketch: &WritableSketch) {
    println!("Sketch size: {} bytes", sketch.size_in_bytes());
}

fn summarize_time(n: usize, duration: u64) {
    println!("Inserted {} values in {} ns", n, duration);
}

fn summarize_error(calc: &ErrorCalculator, sketch: &ReadableSketch) {
    for i in 1..10 {
        let phi = (i as f64) / 10.0;
        let approx = sketch.query(phi).expect("Could not query sketch");
        let err = calc.calculate_error(phi, approx);
        println!("phi={}, approx={}, err={}", phi, approx, err);
    }
}
