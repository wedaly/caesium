extern crate caesium_core;
extern crate rand;

use caesium_core::quantile::error::ErrorCalculator;
use caesium_core::quantile::writable::WritableSketch;
use rand::rngs::SmallRng;
use rand::{FromEntropy, Rng};

const NUM_INSERTS: usize = 10_000;
const NUM_TRIALS: usize = 10;

fn main() {
    println!("phi,trial,error");
    for t in 0..NUM_TRIALS {
        run_trial(t);
    }
}

fn run_trial(trial: usize) {
    let mut rng = SmallRng::from_entropy();
    let mut s = WritableSketch::new();
    let mut data = Vec::with_capacity(NUM_INSERTS);
    for n in 1..NUM_INSERTS {
        for _ in 0..n {
            let v: u32 = rng.gen();
            data.push(v);
            s.insert(v);
        }
    }
    let c = ErrorCalculator::new(&data);
    let r = s.to_readable();

    for i in 1..10 {
        let phi = (i as f64) / 10f64;
        let q = r.query(phi).unwrap().approx_value;
        let e = c.calculate_error(phi, q);
        println!("{},{},{}", phi, trial, e);
    }
}
