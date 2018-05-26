#[macro_use]
extern crate bencher;
extern crate caesium;
extern crate rand;

use bencher::Bencher;
use caesium::quantile::sketch::{WritableSketch, ReadableSketch};
use rand::Rng;

fn insert_sequential(sketch: &mut WritableSketch, n: usize) {
    for v in 0..n {
        sketch.insert(v as u64);
    }
}

fn insert_random(sketch: &mut WritableSketch, n: usize) {
    for v in random_values(n) {
        sketch.insert(v as u64);
    }
}

fn random_values(n: usize) -> Vec<u64> {
    let mut rng = rand::thread_rng();
    let mut result: Vec<u64> = Vec::with_capacity(n);
    for v in 0..n {
        result.push(v as u64);
    }
    rng.shuffle(&mut result);
    result
}

fn build_readable_sketch(n: usize) -> ReadableSketch {
    let mut sketch = WritableSketch::new();
    insert_random(&mut sketch, n);
    sketch.to_readable_sketch()
}

fn bench_insert_one_empty(bench: &mut Bencher) {
    let mut s = WritableSketch::new();
    bench.iter(|| {
        s.insert(1);
    })
}

fn bench_insert_one_nonempty(bench: &mut Bencher) {
    let mut s = WritableSketch::new();
    insert_sequential(&mut s, 4096);
    bench.iter(|| {
        s.insert(1);
    })
}

fn bench_insert_many_empty(bench: &mut Bencher) {
    let mut s = WritableSketch::new();
    let input = random_values(2048);
    bench.iter(|| {
        input.iter().for_each(|v| s.insert(*v));
    })
}

fn bench_insert_many_nonempty(bench: &mut Bencher) {
    let mut s = WritableSketch::new();
    insert_sequential(&mut s, 4096);
    let input = random_values(2048);
    bench.iter(|| {
        input.iter().for_each(|v| s.insert(*v));
    })
}

fn bench_query_small_sketch(bench: &mut Bencher) {
    let s = build_readable_sketch(256);
    bench.iter(|| s.query(0.5))
}

fn bench_query_full_sketch_one_tenth(bench: &mut Bencher) {
    let s = build_readable_sketch(4096);
    bench.iter(|| s.query(0.1))
}

fn bench_query_full_sketch_median(bench: &mut Bencher) {
    let s = build_readable_sketch(4096);
    bench.iter(|| s.query(0.5))
}

fn bench_query_full_sketch_nine_tenths(bench: &mut Bencher) {
    let s = build_readable_sketch(4096);
    bench.iter(|| s.query(0.9))
}

fn bench_merge_two_sketches_sequential_data(bench: &mut Bencher) {
    let mut s1 = WritableSketch::new();
    insert_sequential(&mut s1, 4096);
    let mut s2 = WritableSketch::new();
    insert_sequential(&mut s2, 4096);
    bench.iter(|| s1.merge(&s2))
}

fn bench_merge_two_sketches_random_data(bench: &mut Bencher) {
    let mut s1 = WritableSketch::new();
    insert_random(&mut s1, 4096);
    let mut s2 = WritableSketch::new();
    insert_random(&mut s2, 4096);
    bench.iter(|| s1.merge(&s2))
}

benchmark_group!(
    benches,
    bench_insert_one_empty,
    bench_insert_one_nonempty,
    bench_insert_many_empty,
    bench_insert_many_nonempty,
    bench_query_small_sketch,
    bench_query_full_sketch_one_tenth,
    bench_query_full_sketch_median,
    bench_query_full_sketch_nine_tenths,
    bench_merge_two_sketches_sequential_data,
    bench_merge_two_sketches_random_data,
);
benchmark_main!(benches);
