extern crate caesium;
use caesium::quantile::builder::SketchBuilder;
use caesium::quantile::error::ErrorCalculator;
use caesium::quantile::query::QueryableSketch;
use caesium::quantile::sketch::Sketch;
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

struct Args {
    data_path: String,
}

fn main() -> Result<(), Error> {
    let args = parse_args()?;
    let data = read_data_file(&args.data_path)?;
    let mut sketch = Sketch::new();
    build_sketch(&data, &mut sketch);
    let calc = ErrorCalculator::new(&data);
    summarize_error(&calc, &sketch);
    Ok(())
}

fn parse_args() -> Result<Args, Error> {
    let data_path = env::args()
        .nth(1)
        .ok_or(Error::arg_err("Missing required argument `data_path`"))?;
    Ok(Args {
        data_path: data_path,
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

fn build_sketch(data: &[u64], mut sketch: &mut Sketch) {
    let mut builder = SketchBuilder::new();
    for v in data.iter() {
        builder.insert(*v);
    }
    builder.build(&mut sketch);
}

fn summarize_error(calc: &ErrorCalculator, sketch: &Sketch) {
    let q = QueryableSketch::new(sketch);
    for i in 1..10 {
        let phi = (i as f64) / 10.0;
        let approx = q.query(phi).expect("Could not query sketch");
        let err = calc.calculate_error(phi, approx);
        println!("phi={}, err={}", phi, err);
    }
}
