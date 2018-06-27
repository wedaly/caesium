use quantile::writable::WritableSketch;
use query::execute::execute_query;
use query::result::QueryResult;
use storage::datasource::DataRow;
use storage::mock::MockDataSource;
use time::{TimeStamp, TimeWindow};

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

fn assert_rows(rows: &Vec<QueryResult>, expected: &Vec<(TimeStamp, TimeStamp, u64)>) {
    let actual: Vec<(TimeStamp, TimeStamp, u64)> = rows.iter()
        .map(|r| (r.window.start(), r.window.end(), r.value))
        .collect();
    assert_eq!(actual, *expected);
}

#[test]
fn it_queries_quantile_by_metric() {
    let mut source = MockDataSource::new();
    source.add_row("foo", build_data_row(TimeWindow::new(1, 2)));
    source.add_row("foo", build_data_row(TimeWindow::new(2, 3)));
    source.add_row("bar", build_data_row(TimeWindow::new(3, 4)));
    let query = "quantile(0.5, fetch(foo))";
    let results = execute_query(&query, &mut source).expect("Could not execute query");
    assert_rows(&results, &vec![(1, 2, 50), (2, 3, 50)]);
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
    assert_rows(&results, &vec![(20, 30, 50), (30, 40, 50)]);
}

#[test]
fn it_queries_quantile_metric_not_found() {
    let mut source = MockDataSource::new();
    source.add_row("foo", build_data_row(TimeWindow::new(1, 2)));
    let query = "quantile(0.5, fetch(bar))";
    let results = execute_query(&query, &mut source).expect("Could not execute query");
    assert_rows(&results, &vec![]);
}

#[test]
fn it_queries_quantile_group_by_hour() {
    let mut source = MockDataSource::new();
    source.add_row("foo", build_data_row(TimeWindow::new(10, 20)));
    source.add_row("foo", build_data_row(TimeWindow::new(20, 30)));
    source.add_row("foo", build_data_row(TimeWindow::new(30, 40)));
    source.add_row("foo", build_data_row(TimeWindow::new(40, 50)));
    source.add_row("foo", build_data_row(TimeWindow::new(4000, 4500)));
    let query = "quantile(0.5, group(hours, fetch(foo, 0, 10000)))";
    let results = execute_query(&query, &mut source).expect("Could not execute query");
    assert_rows(&results, &vec![(10, 50, 50), (4000, 4500, 50)]);
}

#[test]
fn it_queries_quantile_group_by_day() {
    let mut source = MockDataSource::new();
    source.add_row("foo", build_data_row(TimeWindow::new(10, 20)));
    source.add_row("foo", build_data_row(TimeWindow::new(20, 30)));
    source.add_row("foo", build_data_row(TimeWindow::new(7000, 8000)));
    source.add_row("foo", build_data_row(TimeWindow::new(90000, 91000)));
    let query = "quantile(0.5, group(days, fetch(foo, 0, 100000)))";
    let results = execute_query(&query, &mut source).expect("Could not execute query");
    assert_rows(&results, &vec![(10, 8000, 50), (90000, 91000, 50)]);
}

#[test]
fn it_coalesces_adjacent_time_windows() {
    let mut source = MockDataSource::new();
    source.add_row("foo", build_data_row(TimeWindow::new(0, 30)));
    source.add_row("foo", build_data_row(TimeWindow::new(30, 60)));
    let query = "quantile(0.5, coalesce(fetch(foo)))";
    let results = execute_query(&query, &mut source).expect("Could not execute query");
    assert_rows(&results, &vec![(0, 60, 50)]);
}

#[test]
fn it_coalesces_overlapping_time_windows() {
    let mut source = MockDataSource::new();
    source.add_row("foo", build_data_row(TimeWindow::new(30, 60)));
    source.add_row("foo", build_data_row(TimeWindow::new(15, 35)));
    let query = "quantile(0.5, coalesce(fetch(foo)))";
    let results = execute_query(&query, &mut source).expect("Could not execute query");
    assert_rows(&results, &vec![(15, 60, 50)]);
}

#[test]
fn it_coalesces_nonadjacent_time_windows() {
    let mut source = MockDataSource::new();
    source.add_row("foo", build_data_row(TimeWindow::new(10, 20)));
    source.add_row("foo", build_data_row(TimeWindow::new(40, 90)));
    let query = "quantile(0.5, coalesce(fetch(foo)))";
    let results = execute_query(&query, &mut source).expect("Could not execute query");
    assert_rows(&results, &vec![(10, 90, 50)]);
}

#[test]
fn it_coalesces_idempotent() {
    let mut source = MockDataSource::new();
    source.add_row("foo", build_data_row(TimeWindow::new(10, 20)));
    source.add_row("foo", build_data_row(TimeWindow::new(40, 90)));
    let query = "quantile(0.5, coalesce(coalesce(fetch(foo))))";
    let results = execute_query(&query, &mut source).expect("Could not execute query");
    assert_rows(&results, &vec![(10, 90, 50)]);
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
    assert_rows(&results, &vec![(0, 30, 50), (30, 60, 50)]);
}

#[test]
fn it_combines_empty_inputs() {
    let mut source = MockDataSource::new();
    let query = "quantile(0.5, combine(fetch(foo), fetch(bar)))";
    let results = execute_query(&query, &mut source).expect("Could not execute query");
    assert_rows(&results, &vec![]);
}

#[test]
fn it_combines_single_input() {
    let mut source = MockDataSource::new();
    source.add_row("foo", build_data_row(TimeWindow::new(0, 30)));
    let query = "quantile(0.5, combine(fetch(foo), fetch(bar)))";
    let results = execute_query(&query, &mut source).expect("Could not execute query");
    assert_rows(&results, &vec![(0, 30, 50)]);
}

#[test]
fn it_combines_multiple_inputs() {
    let mut source = MockDataSource::new();
    source.add_row("foo", build_data_row(TimeWindow::new(10, 20)));
    source.add_row("foo", build_data_row(TimeWindow::new(15, 30)));
    source.add_row("foo", build_data_row(TimeWindow::new(40, 50)));
    source.add_row("foo", build_data_row(TimeWindow::new(50, 60)));
    source.add_row("foo", build_data_row(TimeWindow::new(60, 70)));
    source.add_row("bar", build_data_row(TimeWindow::new(40, 50)));
    source.add_row("bar", build_data_row(TimeWindow::new(55, 59)));
    source.add_row("bar", build_data_row(TimeWindow::new(69, 80)));
    source.add_row("bar", build_data_row(TimeWindow::new(90, 100)));
    let query = "quantile(0.5, combine(fetch(foo), fetch(bar)))";
    let results = execute_query(&query, &mut source).expect("Could not execute query");
    assert_rows(
        &results,
        &vec![
            (10, 30, 50),
            (40, 50, 50),
            (50, 60, 50),
            (60, 80, 50),
            (90, 100, 50),
        ],
    );
}
