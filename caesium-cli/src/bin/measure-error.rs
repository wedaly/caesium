extern crate caesium_core;
extern crate rand;

use caesium_core::quantile::error::ErrorCalculator;
use caesium_core::quantile::writable::WritableSketch;
use rand::rngs::SmallRng;
use rand::{FromEntropy, Rng};

const NUM_INSERTS: usize = 1_000_000;
const NUM_TRIALS: usize = 10;

fn main() {
    println!("insert,trial,error");
    for t in 0..NUM_TRIALS {
        run_trial(t);
    }
}

fn run_trial(trial: usize) {
    let mut rng = SmallRng::from_entropy();
    for n in 1..NUM_INSERTS {
        let mut s = WritableSketch::new();
        let mut data = Vec::with_capacity(n);
        for _ in 0..n {
            let v: u32 = rng.gen();
            data.push(v);
            s.insert(v);
        }
        let c = ErrorCalculator::new(&data);
        let q = s.to_readable().query(0.5).unwrap().approx_value;
        let e = c.calculate_error(0.5, q);
        println!("{},{},{}", n, trial, e);
    }
}
