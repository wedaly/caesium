#[macro_use]
extern crate bencher;
extern crate caesium;
extern crate rand;

use bencher::Bencher;
use rand::Rng;
use caesium::storage::mock::MockDataSource;
use caesium::quantile::writable::WritableSketch;
use caesium::time::TimeWindow;
use caesium::storage::datasource::DataRow;
use caesium::query::execute::execute_query;

fn insert(db: &mut MockDataSource, metric: &str, start: u64, end: u64, count: usize) {
    let mut rng = rand::thread_rng();
    let mut s = WritableSketch::new();
    for _ in 0..count {
        let v = rng.gen::<u64>();
        s.insert(v);
    }
    let row = DataRow {
        window: TimeWindow::new(start, end),
        sketch: s.to_serializable().to_mergable()
    };
    db.add_row(metric, row);
}

fn bench_quantile_query_single_row(bench: &mut Bencher) {
    let mut db = MockDataSource::new();
    insert(&mut db, "foo", 0, 30, 2048);
    bench.iter(|| {
        execute_query(&"quantile(0.5, fetch(foo))", &db)
    })
}

fn bench_quantile_query_many_rows(bench: &mut Bencher) {
    let mut db = MockDataSource::new();
    for i in 0..1000 {
        let start = (i * 30) as u64;
        let end = start + 30;
        insert(&mut db, "foo", start, end, 2048);
    }
    bench.iter(|| {
        execute_query(&"quantile(0.5, fetch(foo))", &db)
    })
}

fn bench_coalesce_query_single_row(bench: &mut Bencher) {
    let mut db = MockDataSource::new();
    insert(&mut db, "foo", 0, 30, 2048);
    bench.iter(|| {
        execute_query(&"quantile(0.5, coalesce(fetch(foo)))", &db)
    })
}

fn bench_coalesce_query_many_rows(bench: &mut Bencher) {
    let mut db = MockDataSource::new();
    for i in 0..1000 {
        let start = (i * 30) as u64;
        let end = start + 30;
        insert(&mut db, "foo", start, end, 2048);
    }
    bench.iter(|| {
        execute_query(&"quantile(0.5, coalesce(fetch(foo)))", &db)
    })
}

fn bench_combine_query_single_row(bench: &mut Bencher) {
    let mut db = MockDataSource::new();
    insert(&mut db, "foo", 0, 30, 2048);
    insert(&mut db, "bar", 0, 30, 2048);
    bench.iter(|| {
        execute_query(&"quantile(0.5, combine(fetch(foo), fetch(bar)))", &db)
    })
}

fn bench_combine_query_many_rows(bench: &mut Bencher) {
    let mut db = MockDataSource::new();
    for i in 0..500 {
        let start = (i * 30) as u64;
        let end = start + 30;
        insert(&mut db, "foo", start, end, 2048);
        insert(&mut db, "bar", start, end, 2048);
    }
    bench.iter(|| {
        execute_query(&"quantile(0.5, combine(fetch(foo), fetch(bar)))", &db)
    })
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
