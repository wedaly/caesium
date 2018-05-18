pub mod builder;
pub mod merge;
pub mod query;
pub mod sampler;
pub mod sketch;

#[cfg(test)]
mod tests {
    use super::builder::SketchBuilder;
    use super::merge::SketchMerger;
    use super::query::SketchQuery;
    use super::sketch::{Sketch, BUFCOUNT, BUFSIZE, EPSILON};
    use rand;
    use rand::Rng;

    #[test]
    fn it_handles_query_with_no_values() {
        let sketch = Sketch::new();
        let q = SketchQuery::new(&sketch);
        if let Some(_) = q.query(0.1) {
            panic!("expected no result!");
        }
    }

    #[test]
    fn it_handles_small_distinct_ordered_input() {
        let input = sequential_values(BUFSIZE * BUFCOUNT);
        let s = build_sketch(&input);
        check_error_bound(&s, &input);
    }

    #[test]
    fn it_handles_small_distinct_unordered_input() {
        let input = random_distinct_values(BUFSIZE * BUFCOUNT);
        let s = build_sketch(&input);
        check_error_bound(&s, &input);
    }

    #[test]
    fn it_handles_small_input_with_duplicates() {
        let input = random_duplicate_values(BUFSIZE * BUFCOUNT);
        let s = build_sketch(&input);
        check_error_bound(&s, &input);
    }

    #[test]
    fn it_handles_large_distinct_ordered_input() {
        let input = sequential_values(BUFSIZE * BUFCOUNT * 100);
        let s = build_sketch(&input);
        check_error_bound(&s, &input);
    }

    #[test]
    fn it_handles_large_distinct_unordered_input() {
        let input = random_distinct_values(BUFSIZE * BUFCOUNT * 100);
        let s = build_sketch(&input);
        check_error_bound(&s, &input);
    }

    #[test]
    fn it_handles_large_input_with_duplicates() {
        let input = random_duplicate_values(BUFSIZE * BUFCOUNT * 100);
        let s = build_sketch(&input);
        check_error_bound(&s, &input);
    }

    #[test]
    fn it_merges_two_sketches_without_increasing_error() {
        let n = BUFSIZE * BUFCOUNT * 2;
        let input = random_distinct_values(n);
        let s1 = build_sketch(&input[..n / 2]);
        let mut s2 = build_sketch(&input[n / 2..n]);
        let mut m = SketchMerger::new();
        m.merge(&s1, &mut s2);
        check_error_bound(&s2, &input);
    }

    #[test]
    fn it_merges_many_sketches_without_increasing_error() {
        let sketch_size = BUFSIZE * BUFCOUNT;
        let num_sketches = 100;
        let input = random_distinct_values(sketch_size * num_sketches);
        let mut m = SketchMerger::new();
        let mut s = build_sketch(&input[..sketch_size]);
        for i in 1..num_sketches {
            let start = i * sketch_size;
            let end = start + sketch_size;
            let new_sketch = build_sketch(&input[start..end]);
            m.merge(&new_sketch, &mut s);
        }
        check_error_bound(&s, &input);
    }

    fn sequential_values(n: usize) -> Vec<u64> {
        let mut result: Vec<u64> = Vec::with_capacity(n);
        for v in 0..n {
            result.push(v as u64);
        }
        result
    }

    fn random_distinct_values(n: usize) -> Vec<u64> {
        let mut result: Vec<u64> = Vec::with_capacity(n);
        for v in 0..n {
            result.push(v as u64);
        }

        let mut rng = rand::thread_rng();
        rng.shuffle(&mut result);
        result
    }

    fn random_duplicate_values(n: usize) -> Vec<u64> {
        let mut result: Vec<u64> = Vec::with_capacity(n);
        for v in 0..n / 2 {
            result.push(v as u64);
            result.push(v as u64);
        }

        let mut rng = rand::thread_rng();
        rng.shuffle(&mut result);
        result
    }

    fn build_sketch(input: &[u64]) -> Sketch {
        let mut sketch = Sketch::new();
        let mut builder = SketchBuilder::new();
        for v in input.iter() {
            builder.insert(*v);
        }
        builder.build(&mut sketch);
        sketch
    }

    fn check_error_bound(s: &Sketch, input: &[u64]) {
        let q = SketchQuery::new(s);
        let n = input.len();
        let mut sorted = Vec::with_capacity(input.len());
        sorted.extend_from_slice(input);
        sorted.sort();
        for i in 1..10 {
            let phi = i as f64 / 10.0;
            let exact_idx = (n as f64 * phi) as usize;
            let exact = sorted[exact_idx] as i64;
            let result = q.query(phi).expect("no result from query") as i64;
            let error = (exact - result).abs() as f64 / n as f64;
            println!(
                "phi = {}, exact = {}, result = {}, err = {}",
                phi, exact, result, error
            );
            assert!(error <= EPSILON * 2.0);
        }
    }
}
