use quantile::error::ErrorCalculator;
use quantile::readable::ReadableSketch;
use quantile::writable::WritableSketch;
use rand;
use rand::Rng;

const EPSILON: f64 = 0.01;
const SMALL_SIZE: usize = 256 * 8;
const MEDIUM_SIZE: usize = SMALL_SIZE * 10;
const LARGE_SIZE: usize = SMALL_SIZE * 100;

#[test]
fn it_handles_query_with_no_values() {
    let input = Vec::new();
    let s = build_readable_sketch(&input);
    if let Some(_) = s.query(0.1) {
        panic!("expected no result!");
    }
}

#[test]
fn it_handles_small_distinct_ordered_input() {
    let input = sequential_values(SMALL_SIZE);
    let mut s = build_readable_sketch(&input);
    check_error_bound(&mut s, &input);
}

#[test]
fn it_handles_small_distinct_unordered_input() {
    let input = random_distinct_values(SMALL_SIZE);
    let mut s = build_readable_sketch(&input);
    check_error_bound(&mut s, &input);
}

#[test]
fn it_handles_small_input_with_duplicates() {
    let input = random_duplicate_values(SMALL_SIZE);
    let mut s = build_readable_sketch(&input);
    check_error_bound(&mut s, &input);
}

#[test]
fn it_handles_large_distinct_ordered_input() {
    let input = sequential_values(LARGE_SIZE);
    let mut s = build_readable_sketch(&input);
    check_error_bound(&mut s, &input);
}

#[test]
fn it_handles_large_distinct_unordered_input() {
    let input = random_distinct_values(LARGE_SIZE);
    let mut s = build_readable_sketch(&input);
    check_error_bound(&mut s, &input);
}

#[test]
fn it_handles_large_input_with_duplicates() {
    let input = random_duplicate_values(LARGE_SIZE);
    let mut s = build_readable_sketch(&input);
    check_error_bound(&mut s, &input);
}

#[test]
fn it_merges_two_sketches_without_increasing_error() {
    let n = MEDIUM_SIZE * 2;
    let input = random_distinct_values(n);
    let s1 = build_writable_sketch(&input[..n / 2]);
    let s2 = build_writable_sketch(&input[n / 2..n]);
    let mut result = s2.merge(s1).to_readable();
    check_error_bound(&mut result, &input);
}

#[test]
fn it_merges_many_sketches_without_increasing_error() {
    let sketch_size = MEDIUM_SIZE;
    let num_sketches = 30;
    let input = random_distinct_values(sketch_size * num_sketches);
    let mut s = build_writable_sketch(&input[..sketch_size]);
    for i in 1..num_sketches {
        let start = i * sketch_size;
        let end = start + sketch_size;
        let new_sketch = build_writable_sketch(&input[start..end]);
        s = s.merge(new_sketch);
    }
    let mut result = s.to_readable();
    check_error_bound(&mut result, &input);
}

fn sequential_values(n: usize) -> Vec<u32> {
    let mut result: Vec<u32> = Vec::with_capacity(n);
    for v in 0..n {
        result.push(v as u32);
    }
    result
}

fn random_distinct_values(n: usize) -> Vec<u32> {
    let mut result: Vec<u32> = Vec::with_capacity(n);
    for v in 0..n {
        result.push(v as u32);
    }

    let mut rng = rand::thread_rng();
    rng.shuffle(&mut result);
    result
}

fn random_duplicate_values(n: usize) -> Vec<u32> {
    let mut result: Vec<u32> = Vec::with_capacity(n);
    for v in 0..n / 2 {
        result.push(v as u32);
        result.push(v as u32);
    }

    let mut rng = rand::thread_rng();
    rng.shuffle(&mut result);
    result
}

fn build_readable_sketch(input: &[u32]) -> ReadableSketch {
    let s = build_writable_sketch(input);
    s.to_readable()
}

fn build_writable_sketch(input: &[u32]) -> WritableSketch {
    let mut sketch = WritableSketch::new();
    for v in input.iter() {
        sketch.insert(*v);
    }
    sketch
}

fn check_error_bound(sketch: &mut ReadableSketch, input: &[u32]) {
    let calc = ErrorCalculator::new(&input);
    for i in 1..10 {
        let phi = i as f64 / 10.0;
        let approx = sketch
            .query(phi)
            .map(|q| q.approx_value)
            .expect("no result from query");
        let error = calc.calculate_error(phi, approx);
        println!("phi={}, approx={}, error={}", phi, approx, error);
        assert!(error <= EPSILON * 2.0);
    }
}
