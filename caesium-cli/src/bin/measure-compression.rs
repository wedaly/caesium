extern crate caesium_core;
extern crate rand;

use caesium_core::encode::{Decodable, Encodable};
use caesium_core::quantile::writable::WritableSketch;
use caesium_core::time::timer::Timer;
use std::time::Duration;

const NUM_INSERTS: usize = 50_000;
const NUM_TRIALS: usize = 10;
const GAP_PARAMS: [usize; 3] = [0, 8, 16];

fn main() {
    println!("insert,gap_param,trial,count,bytes,encode_ns,decode_ns");
    for &d in GAP_PARAMS.iter() {
        for t in 0..NUM_TRIALS {
            run_trial(t, d);
        }
    }
}

fn run_trial(trial: usize, d: usize) {
    let gap_size = 1 << d;
    let mut s = WritableSketch::new();
    let mut buf = Vec::with_capacity(1 << 16);
    let mut t = Timer::new();
    for i in 0..NUM_INSERTS {
        let v = (gap_size * i) as u32;
        s.insert(v);
        let encode_ns = measure_encode_time_ns(&mut t, &s, &mut buf);
        let encode_sz = buf.len();
        let decode_ns = measure_decode_time_ns(&mut t, &mut buf);
        println!(
            "{},{},{},{},{},{},{}",
            i,
            d,
            trial,
            s.size(),
            encode_sz,
            encode_ns,
            decode_ns
        );
        buf.clear();
    }
}

fn measure_encode_time_ns(t: &mut Timer, s: &WritableSketch, buf: &mut Vec<u8>) -> u64 {
    t.start();
    s.encode(buf).expect("Could not encode sketch");
    t.stop().map(|d| convert_to_ns(d)).unwrap()
}

fn measure_decode_time_ns(t: &mut Timer, buf: &[u8]) -> u64 {
    t.start();
    WritableSketch::decode(&mut &buf[..]).expect("Could not decode sketch");
    t.stop().map(|d| convert_to_ns(d)).unwrap()
}

fn convert_to_ns(d: Duration) -> u64 {
    d.subsec_nanos() as u64 + (d.as_secs() * 1_000_000_000)
}
