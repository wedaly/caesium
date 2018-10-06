#[macro_use]
extern crate bencher;
extern crate caesium_core;
extern crate rand;

use bencher::Bencher;
use caesium_core::encode::{Decodable, Encodable};
use caesium_core::quantile::writable::WritableSketch;
use rand::Rng;

fn insert_sequential(sketch: &mut WritableSketch, n: usize) {
    for v in 0..n {
        sketch.insert(v as u32);
    }
}

fn insert_random(sketch: &mut WritableSketch, n: usize) {
    for v in random_values(n) {
        sketch.insert(v as u32);
    }
}

fn random_values(n: usize) -> Vec<u32> {
    let mut rng = rand::thread_rng();
    let mut result: Vec<u32> = Vec::with_capacity(n);
    for v in 0..n {
        result.push(v as u32);
    }
    rng.shuffle(&mut result);
    result
}

fn build_writable_sketch(n: usize, randomize: bool) -> WritableSketch {
    let mut s = WritableSketch::new();
    if randomize {
        insert_random(&mut s, n);
    } else {
        insert_sequential(&mut s, n);
    }
    s
}

fn bench_insert_one_empty(bench: &mut Bencher) {
    let s = WritableSketch::new();
    bench.iter(|| {
        s.clone().insert(1);
    })
}

fn bench_insert_one_nonempty(bench: &mut Bencher) {
    let mut s = WritableSketch::new();
    insert_sequential(&mut s, 4096);
    bench.iter(|| {
        s.clone().insert(1);
    })
}

fn bench_insert_one_large_sketch(bench: &mut Bencher) {
    let mut s = WritableSketch::new();
    insert_sequential(&mut s, 1_000_000);
    bench.iter(|| {
        s.clone().insert(1);
    })
}

fn bench_insert_many_empty(bench: &mut Bencher) {
    let s = WritableSketch::new();
    let input = random_values(2048);
    bench.iter(|| {
        let mut sc = s.clone();
        input.iter().for_each(|v| sc.insert(*v));
    })
}

fn bench_insert_many_nonempty(bench: &mut Bencher) {
    let mut s = WritableSketch::new();
    insert_sequential(&mut s, 4096);
    let input = random_values(2048);
    bench.iter(|| {
        let mut sc = s.clone();
        input.iter().for_each(|v| sc.insert(*v));
    })
}

fn bench_insert_many_large_sketch(bench: &mut Bencher) {
    let mut s = WritableSketch::new();
    insert_sequential(&mut s, 1_000_000);
    let input = random_values(2048);
    bench.iter(|| {
        let mut sc = s.clone();
        input.iter().for_each(|v| sc.insert(*v));
    })
}

fn bench_query_small_sketch(bench: &mut Bencher) {
    let s = build_writable_sketch(256, true);
    bench.iter(|| s.clone().to_readable().query(0.5))
}

fn bench_query_full_sketch_one_tenth(bench: &mut Bencher) {
    let s = build_writable_sketch(4096, true);
    bench.iter(|| s.clone().to_readable().query(0.1))
}

fn bench_query_full_sketch_median(bench: &mut Bencher) {
    let s = build_writable_sketch(4096, true);
    bench.iter(|| s.clone().to_readable().query(0.5))
}

fn bench_query_full_sketch_nine_tenths(bench: &mut Bencher) {
    let s = build_writable_sketch(4096, true);
    bench.iter(|| s.clone().to_readable().query(0.9))
}

fn bench_merge_two_sketches_sequential_data(bench: &mut Bencher) {
    let m1 = build_writable_sketch(4096, false);
    let m2 = build_writable_sketch(4096, false);
    bench.iter(|| {
        let m1_clone = m1.clone();
        let m2_clone = m2.clone();
        m1_clone.merge(m2_clone)
    })
}

fn bench_merge_two_sketches_random_data(bench: &mut Bencher) {
    let m1 = build_writable_sketch(4096, true);
    let m2 = build_writable_sketch(4096, true);
    bench.iter(|| {
        let m1_clone = m1.clone();
        let m2_clone = m2.clone();
        m1_clone.merge(m2_clone)
    })
}

fn bench_merge_two_large_sketches(bench: &mut Bencher) {
    let m1 = build_writable_sketch(1_000_000, false);
    let m2 = build_writable_sketch(1_000_000, false);
    bench.iter(|| {
        let m1_clone = m1.clone();
        let m2_clone = m2.clone();
        m1_clone.merge(m2_clone)
    })
}

fn bench_encode_to_bytes(bench: &mut Bencher) {
    let s = build_writable_sketch(4096, true);
    let mut writer = Vec::new();
    bench.iter(|| s.encode(&mut writer))
}

fn bench_decode_from_bytes(bench: &mut Bencher) {
    let s = build_writable_sketch(4096, true);
    let mut v: Vec<u8> = Vec::new();
    s.encode(&mut v).unwrap();
    bench.iter(|| WritableSketch::decode(&mut &v[..]).unwrap())
}

benchmark_group!(
    benches,
    bench_insert_one_empty,
    bench_insert_one_nonempty,
    bench_insert_one_large_sketch,
    bench_insert_many_empty,
    bench_insert_many_nonempty,
    bench_insert_many_large_sketch,
    bench_query_small_sketch,
    bench_query_full_sketch_one_tenth,
    bench_query_full_sketch_median,
    bench_query_full_sketch_nine_tenths,
    bench_merge_two_sketches_sequential_data,
    bench_merge_two_sketches_random_data,
    bench_merge_two_large_sketches,
    bench_encode_to_bytes,
    bench_decode_from_bytes,
);
benchmark_main!(benches);
