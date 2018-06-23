use quantile::writable::WritableSketch;
use query::execute::execute_query;
use query::result::QueryResult;
use storage::datasource::DataRow;
use storage::mock::MockDataSource;
use time::TimeWindow;

fn build_data_row(window: TimeWindow) -> DataRow {
    let mut s = WritableSketch::new();
    for i in 0..100 {
        s.insert(i as u64);
    }
    DataRow {
        window: window,
        sketch: s.to_serializable().to_mergable(),
    }
}

#[test]
fn it_queries_quantile_by_metric() {
    let mut source = MockDataSource::new();
    source.add_row("foo", build_data_row(TimeWindow::new(1, 2)));
    source.add_row("foo", build_data_row(TimeWindow::new(2, 3)));
    source.add_row("bar", build_data_row(TimeWindow::new(3, 4)));
    let query = "quantile(0.5, fetch(foo))";
    let results = execute_query(&query, &mut source).expect("Could not execute query");
    assert_eq!(results.len(), 2);

    let r1 = results.get(0).unwrap();
    assert_eq!(
        *r1,
        QueryResult {
            window: TimeWindow::new(1, 2),
            value: 50
        }
    );

    let r2 = results.get(1).unwrap();
    assert_eq!(
        *r2,
        QueryResult {
            window: TimeWindow::new(2, 3),
            value: 50
        }
    );
}

#[test]
fn it_queries_quantile_select_time_range() {
    let mut source = MockDataSource::new();
    source.add_row("foo", build_data_row(TimeWindow::new(10, 20)));
    source.add_row("foo", build_data_row(TimeWindow::new(20, 30)));
    source.add_row("foo", build_data_row(TimeWindow::new(30, 40)));
    source.add_row("foo", build_data_row(TimeWindow::new(40, 50)));
    let query = "quantile(0.5, fetch(foo, 20, 40))";
    let results = execute_query(&query, &mut source).expect("Could not execute query");
    assert_eq!(results.len(), 2);

    let r1 = results.get(0).unwrap();
    assert_eq!(
        *r1,
        QueryResult {
            window: TimeWindow::new(20, 30),
            value: 50
        }
    );

    let r2 = results.get(1).unwrap();
    assert_eq!(
        *r2,
        QueryResult {
            window: TimeWindow::new(30, 40),
            value: 50
        }
    );
}

#[test]
fn it_queries_quantile_metric_not_found() {
    let mut source = MockDataSource::new();
    source.add_row("foo", build_data_row(TimeWindow::new(1, 2)));
    let query = "quantile(0.5, fetch(bar))";
    let results = execute_query(&query, &mut source).expect("Could not execute query");
    assert_eq!(results.len(), 0);
}

#[test]
fn it_coalesces_adjacent_time_windows() {
    let mut source = MockDataSource::new();
    source.add_row("foo", build_data_row(TimeWindow::new(0, 30)));
    source.add_row("foo", build_data_row(TimeWindow::new(30, 60)));
    let query = "quantile(0.5, coalesce(fetch(foo)))";
    let results = execute_query(&query, &mut source).expect("Could not execute query");
    assert_eq!(results.len(), 1);
    let r = results.first().unwrap();
    assert_eq!(
        *r,
        QueryResult {
            window: TimeWindow::new(0, 60),
            value: 50
        }
    );
}

#[test]
fn it_coalesces_overlapping_time_windows() {
    let mut source = MockDataSource::new();
    source.add_row("foo", build_data_row(TimeWindow::new(30, 60)));
    source.add_row("foo", build_data_row(TimeWindow::new(15, 35)));
    let query = "quantile(0.5, coalesce(fetch(foo)))";
    let results = execute_query(&query, &mut source).expect("Could not execute query");
    assert_eq!(results.len(), 1);
    let r = results.first().unwrap();
    assert_eq!(
        *r,
        QueryResult {
            window: TimeWindow::new(15, 60),
            value: 50
        }
    );
}

#[test]
fn it_coalesces_nonadjacent_time_windows() {
    let mut source = MockDataSource::new();
    source.add_row("foo", build_data_row(TimeWindow::new(10, 20)));
    source.add_row("foo", build_data_row(TimeWindow::new(40, 90)));
    let query = "quantile(0.5, coalesce(fetch(foo)))";
    let results = execute_query(&query, &mut source).expect("Could not execute query");
    assert_eq!(results.len(), 1);
    let r = results.first().unwrap();
    assert_eq!(
        *r,
        QueryResult {
            window: TimeWindow::new(10, 90),
            value: 50
        }
    );
}

#[test]
fn it_coalesces_idempotent() {
    let mut source = MockDataSource::new();
    source.add_row("foo", build_data_row(TimeWindow::new(10, 20)));
    source.add_row("foo", build_data_row(TimeWindow::new(40, 90)));
    let query = "quantile(0.5, coalesce(coalesce(fetch(foo))))";
    let results = execute_query(&query, &mut source).expect("Could not execute query");
    assert_eq!(results.len(), 1);
    let r = results.first().unwrap();
    assert_eq!(
        *r,
        QueryResult {
            window: TimeWindow::new(10, 90),
            value: 50
        }
    );
}

#[test]
fn it_combines_time_series() {
    let mut source = MockDataSource::new();
    source.add_row("foo", build_data_row(TimeWindow::new(0, 30)));
    source.add_row("foo", build_data_row(TimeWindow::new(30, 60)));
    source.add_row("bar", build_data_row(TimeWindow::new(0, 30)));
    source.add_row("bar", build_data_row(TimeWindow::new(30, 60)));

    let query = "quantile(0.5, combine(fetch(foo), fetch(bar)))";
    let results = execute_query(&query, &mut source).expect("Could not execute query");
    assert_eq!(results.len(), 2);

    let r1 = results.first().unwrap();
    assert_eq!(
        *r1,
        QueryResult {
            window: TimeWindow::new(0, 30),
            value: 50
        }
    );

    let r2 = results.get(1).unwrap();
    assert_eq!(
        *r2,
        QueryResult {
            window: TimeWindow::new(30, 60),
            value: 50
        }
    );
}
