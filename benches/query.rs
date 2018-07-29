#[macro_use]
extern crate bencher;
extern crate caesium;

use bencher::Bencher;
use caesium::quantile::writable::WritableSketch;
use caesium::query::execute::execute_query;
use caesium::storage::datasource::DataRow;
use caesium::storage::mock::MockDataSource;
use caesium::time::window::TimeWindow;

fn insert(db: &mut MockDataSource, metric: &str, start: u64, end: u64, count: usize) {
    let mut sketch = WritableSketch::new();
    for v in 0..count {
        sketch.insert(v as u64);
    }
    let row = DataRow {
        window: TimeWindow::new(start, end),
        sketch,
    };
    db.add_row(metric, row);
}

fn bench_quantile_query_single_row(bench: &mut Bencher) {
    let mut db = MockDataSource::new();
    insert(&mut db, "foo", 0, 30, 2048);
    bench.iter(|| execute_query(&"quantile(fetch(foo), 0.5)", &db))
}

fn bench_quantile_query_many_rows(bench: &mut Bencher) {
    let mut db = MockDataSource::new();
    for i in 0..1000 {
        let start = (i * 30) as u64;
        let end = start + 30;
        insert(&mut db, "foo", start, end, 2048);
    }
    bench.iter(|| execute_query(&"quantile(fetch(foo), 0.5)", &db))
}

fn bench_coalesce_query_single_row(bench: &mut Bencher) {
    let mut db = MockDataSource::new();
    insert(&mut db, "foo", 0, 30, 2048);
    bench.iter(|| execute_query(&"quantile(coalesce(fetch(foo)), 0.5)", &db))
}

fn bench_coalesce_query_many_rows(bench: &mut Bencher) {
    let mut db = MockDataSource::new();
    for i in 0..1000 {
        let start = (i * 30) as u64;
        let end = start + 30;
        insert(&mut db, "foo", start, end, 2048);
    }
    bench.iter(|| execute_query(&"quantile(coalesce(fetch(foo)), 0.5)", &db))
}

fn bench_combine_query_single_row(bench: &mut Bencher) {
    let mut db = MockDataSource::new();
    insert(&mut db, "foo", 0, 30, 2048);
    insert(&mut db, "bar", 0, 30, 2048);
    bench.iter(|| execute_query(&"quantile(combine(fetch(foo), fetch(bar)), 0.5)", &db))
}

fn bench_combine_query_many_rows(bench: &mut Bencher) {
    let mut db = MockDataSource::new();
    for i in 0..500 {
        let start = (i * 30) as u64;
        let end = start + 30;
        insert(&mut db, "foo", start, end, 2048);
        insert(&mut db, "bar", start, end, 2048);
    }
    bench.iter(|| execute_query(&"quantile(combine(fetch(foo), fetch(bar)), 0.5)", &db))
}

benchmark_group!(
    benches,
    bench_quantile_query_single_row,
    bench_quantile_query_many_rows,
    bench_coalesce_query_single_row,
    bench_coalesce_query_many_rows,
    bench_combine_query_single_row,
    bench_combine_query_many_rows,
);
benchmark_main!(benches);
