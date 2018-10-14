extern crate caesium_core;
extern crate rand;

use caesium_core::quantile::writable::WritableSketch;
use caesium_core::time::timer::Timer;
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

    println!("insert,trial,nanoseconds");
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
        t.start();
        s.insert(v);
        t.stop();
        if record {
            let d = t.duration().unwrap();
            let ns = d.subsec_nanos() as u64 + (d.as_secs() * 1_000_000_000);
            println!("{},{},{}", i, trial, ns);
        }
    }
}
