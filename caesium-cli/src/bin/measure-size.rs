extern crate caesium_core;
extern crate rand;

use caesium_core::encode::Encodable;
use caesium_core::quantile::writable::WritableSketch;
use rand::rngs::SmallRng;
use rand::{FromEntropy, Rng};

const NUM_INSERTS: usize = 1_000_000;
const NUM_TRIALS: usize = 100;
const NUM_WARMUPS: usize = 10;

const MIN_VAL: u64 = 0;
const MAX_VAL: u64 = 5000;

fn main() {
    for t in 0..NUM_WARMUPS {
        run_trial(false, t);
    }

    println!("insert,trial,bytes");
    for t in 0..NUM_TRIALS {
        run_trial(true, t);
    }
}

fn run_trial(record: bool, trial: usize) {
    let mut rng = SmallRng::from_entropy();
    let mut s = WritableSketch::new();
    for i in 0..NUM_INSERTS {
        let v = rng.gen_range(MIN_VAL, MAX_VAL) as u32;
        s.insert(v);
        let sz = calculate_size(&s);
        if record {
            println!("{},{},{}", i, trial, sz);
        }
    }
}

fn calculate_size(s: &WritableSketch) -> usize {
    let mut buf = Vec::new();
    s.encode(&mut buf).expect("Could not encode sketch");
    buf.len()
}
