#[macro_use]
extern crate bencher;
extern crate caesium;
extern crate rand;

use bencher::Bencher;
use caesium::encode::{Decodable, Encodable};
use caesium::quantile::mergable::MergableSketch;
use caesium::quantile::readable::ReadableSketch;
use caesium::quantile::serializable::SerializableSketch;
use caesium::quantile::writable::WritableSketch;
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

fn build_writable_sketch(n: usize, randomize: bool) -> WritableSketch {
    let mut s = WritableSketch::new();
    if randomize {
        insert_random(&mut s, n);
    } else {
        insert_sequential(&mut s, n);
    }
    s
}

fn build_mergable_sketch(n: usize, randomize: bool) -> MergableSketch {
    let s = build_writable_sketch(n, randomize);
    s.to_serializable().to_mergable()
}

fn build_readable_sketch(n: usize) -> ReadableSketch {
    let s = build_writable_sketch(n, true);
    s.to_serializable().to_readable()
}

fn build_serializable_sketch(n: usize) -> SerializableSketch {
    let s = build_writable_sketch(n, true);
    s.to_serializable()
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
    let mut s = build_readable_sketch(256);
    bench.iter(|| s.query(0.5))
}

fn bench_query_full_sketch_one_tenth(bench: &mut Bencher) {
    let mut s = build_readable_sketch(4096);
    bench.iter(|| s.query(0.1))
}

fn bench_query_full_sketch_median(bench: &mut Bencher) {
    let mut s = build_readable_sketch(4096);
    bench.iter(|| s.query(0.5))
}

fn bench_query_full_sketch_nine_tenths(bench: &mut Bencher) {
    let mut s = build_readable_sketch(4096);
    bench.iter(|| s.query(0.9))
}

fn bench_merge_two_sketches_sequential_data(bench: &mut Bencher) {
    let mut m1 = build_mergable_sketch(4096, false);
    let m2 = build_mergable_sketch(4096, false);
    bench.iter(|| m1.merge(&m2))
}

fn bench_merge_two_sketches_random_data(bench: &mut Bencher) {
    let mut m1 = build_mergable_sketch(4096, true);
    let m2 = build_mergable_sketch(4096, true);
    bench.iter(|| m1.merge(&m2))
}

fn bench_writable_to_serializable(bench: &mut Bencher) {
    let s = build_writable_sketch(4096, false);
    bench.iter(|| s.to_serializable())
}

fn bench_serializable_to_bytes(bench: &mut Bencher) {
    let s = build_serializable_sketch(4096);
    let mut writer = Vec::new();
    bench.iter(|| s.encode(&mut writer))
}

fn bench_serializable_from_bytes(bench: &mut Bencher) {
    let s = build_serializable_sketch(4096);
    let mut v: Vec<u8> = Vec::new();
    s.encode(&mut v).unwrap();
    bench.iter(|| SerializableSketch::decode(&mut &v[..]).unwrap())
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
    bench_writable_to_serializable,
    bench_serializable_to_bytes,
    bench_serializable_from_bytes
);
benchmark_main!(benches);
