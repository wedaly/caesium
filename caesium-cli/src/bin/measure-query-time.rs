extern crate caesium_core;
extern crate rand;

use caesium_core::quantile::writable::WritableSketch;
use caesium_core::time::timer::Timer;
use rand::rngs::SmallRng;
use rand::{FromEntropy, Rng};

const NUM_INSERTS: usize = 1_000_000;
const NUM_TRIALS: usize = 10;
const NUM_WARMUPS: usize = 1;

const MIN_VAL: u64 = 0;
const MAX_VAL: u64 = 5000;

fn main() {
    for t in 0..NUM_WARMUPS {
        run_trial(false, t);
    }

    println!("i,phi,trial,nanoseconds");
    for t in 0..NUM_TRIALS {
        run_trial(true, t);
    }
}

fn run_trial(record: bool, trial: usize) {
    let mut rng = SmallRng::from_entropy();
    let mut t = Timer::new();
    let mut s = WritableSketch::new();

    for i in 0..NUM_INSERTS {
        let v = rng.gen_range(MIN_VAL, MAX_VAL) as u32;
        s.insert(v);

        for j in 1..10 {
            let phi = (j as f64) / 10f64;
            let s1 = s.clone();
            t.start();
            let _q = s1.to_readable().query(phi);
            let d = t.stop().unwrap();
            if record {
                let ns = d.subsec_nanos() as u64 + (d.as_secs() * 1_000_000_000);
                println!("{},{},{},{}", i, phi, trial, ns);
            }
        }
    }
}
