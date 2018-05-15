#[macro_use]
extern crate bencher;
extern crate caesium;
extern crate rand;

use bencher::Bencher;
use caesium::quantile::builder::SketchBuilder;
use caesium::quantile::query::SketchQuery;
use caesium::quantile::sketch::{Sketch, BUFCOUNT, BUFSIZE};
use rand::Rng;

fn insert_sequential(builder: &mut SketchBuilder, n: usize) {
    for v in 0..n {
        builder.insert(v as u64);
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

fn setup_sketch(num_buffers: usize) -> Sketch {
    let mut sketch = Sketch::new();
    let mut data: [u64; BUFSIZE] = [0; BUFSIZE];
    for idx in 0..BUFSIZE {
        data[idx] = idx as u64;
    }
    for level in 0..num_buffers {
        sketch.buffer_mut(level).set(level, &data[..]);
    }
    sketch
}

fn setup_query(num_buffers: usize) -> SketchQuery {
    let sketch = setup_sketch(num_buffers);
    SketchQuery::new(&sketch)
}

fn bench_insert_one_empty(bench: &mut Bencher) {
    let mut builder = SketchBuilder::new();
    bench.iter(|| {
        builder.insert(1);
    })
}

fn bench_insert_one_full(bench: &mut Bencher) {
    let mut builder = SketchBuilder::new();
    insert_sequential(&mut builder, BUFSIZE);
    bench.iter(|| {
        builder.insert(1);
    })
}

fn bench_insert_one_merge(bench: &mut Bencher) {
    let mut builder = SketchBuilder::new();
    insert_sequential(&mut builder, BUFCOUNT * BUFSIZE);
    bench.iter(|| {
        builder.insert(1);
    })
}

fn bench_insert_many_no_merge(bench: &mut Bencher) {
    let mut builder = SketchBuilder::new();
    let input = random_values(BUFCOUNT * BUFSIZE);
    bench.iter(|| {
        input.iter().for_each(|v| builder.insert(*v));
    })
}

fn bench_insert_many_with_merge(bench: &mut Bencher) {
    let mut builder = SketchBuilder::new();
    insert_sequential(&mut builder, BUFCOUNT * BUFSIZE);
    let input = random_values(BUFCOUNT * BUFSIZE);
    bench.iter(|| {
        input.iter().for_each(|v| builder.insert(*v));
    })
}

fn bench_prepare_query_small_sketch(bench: &mut Bencher) {
    bench.iter(|| setup_sketch(1))
}

fn bench_prepare_query_full_sketch(bench: &mut Bencher) {
    bench.iter(|| setup_sketch(BUFCOUNT))
}

fn bench_query_small_sketch(bench: &mut Bencher) {
    let q = setup_query(1);
    bench.iter(|| q.query(0.5))
}

fn bench_query_full_sketch_one_tenth(bench: &mut Bencher) {
    let q = setup_query(BUFCOUNT);
    bench.iter(|| q.query(0.1))
}

fn bench_query_full_sketch_median(bench: &mut Bencher) {
    let q = setup_query(BUFCOUNT);
    bench.iter(|| q.query(0.5))
}

fn bench_query_full_sketch_nine_tenths(bench: &mut Bencher) {
    let q = setup_query(BUFCOUNT);
    bench.iter(|| q.query(0.9))
}

benchmark_group!(
    benches,
    bench_insert_one_empty,
    bench_insert_one_full,
    bench_insert_one_merge,
    bench_insert_many_no_merge,
    bench_insert_many_with_merge,
    bench_prepare_query_small_sketch,
    bench_prepare_query_full_sketch,
    bench_query_small_sketch,
    bench_query_full_sketch_one_tenth,
    bench_query_full_sketch_median,
    bench_query_full_sketch_nine_tenths
);
benchmark_main!(benches);
