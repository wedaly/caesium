#[macro_use]
extern crate bencher;
extern crate rand;
extern crate caesium;

use bencher::Bencher;
use rand::Rng;
use caesium::quantile::{QuantileSketch, BUFCOUNT, BUFSIZE};

fn bench_insert_no_merge(bench: &mut Bencher) {
    let mut q = QuantileSketch::new();
    let n = BUFCOUNT * BUFSIZE;
    let mut input: Vec<u64> = Vec::with_capacity(n);
    for v in 0..n {
        input.push(v as u64);
    }
    let mut rng = rand::thread_rng();
    rng.shuffle(&mut input);
    bench.iter(|| { input.iter().for_each(|v| q.insert(*v)); })
}

fn bench_insert_with_merge(bench: &mut Bencher) {
    let mut q = QuantileSketch::new();
    let n = BUFCOUNT * BUFSIZE;

    // Fill all slots; additional inserts will trigger merges
    for v in 0..n {
        q.insert(v as u64);
    }

    // Now insert into the full sketch and measure
    let mut input: Vec<u64> = Vec::with_capacity(n);
    for v in 0..n {
        input.push(v as u64);
    }
    let mut rng = rand::thread_rng();
    rng.shuffle(&mut input);
    bench.iter(|| { input.iter().for_each(|v| q.insert(*v)); })
}

fn bench_query_small_sketch(bench: &mut Bencher) {
    let mut q = QuantileSketch::new();
    q.insert(1);
    bench.iter(|| q.query(0.5))
}

fn bench_query_full_sketch(bench: &mut Bencher) {
    let mut q = QuantileSketch::new();
    let n = BUFCOUNT * BUFSIZE;
    for v in 0..n {
        q.insert(v as u64);
    }
    bench.iter(|| q.query(0.5))
}

benchmark_group!(
    benches,
    bench_insert_no_merge,
    bench_insert_with_merge,
    bench_query_small_sketch,
    bench_query_full_sketch
);
benchmark_main!(benches);
