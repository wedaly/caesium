use caesium_core::quantile::writable::WritableSketch;
use caesium_core::time::timestamp::TimeStamp;
use caesium_core::time::window::TimeWindow;
use query::execute::{execute_query, QueryResult};
use storage::datasource::DataRow;
use storage::mock::MockDataSource;

fn build_data_row(window: TimeWindow) -> DataRow {
    let mut sketch = WritableSketch::new();
    for i in 0..100 {
        sketch.insert(i as u32);
    }
    DataRow { window, sketch }
}

fn assert_windows(rows: &Vec<QueryResult>, expected: &Vec<(TimeStamp, TimeStamp, f64, u32)>) {
    let actual: Vec<(TimeStamp, TimeStamp, f64, u32)> = rows
        .iter()
        .filter_map(|r| match r {
            &QueryResult::QuantileWindow(window, phi, quantile) => {
                Some((window.start(), window.end(), phi, quantile.approx_value))
            }
            _ => None,
        })
        .collect();
    assert_eq!(actual, *expected);
}

fn assert_metrics(rows: &Vec<QueryResult>, expected: &Vec<&str>) {
    let actual: Vec<&str> = rows
        .iter()
        .filter_map(|r| match r {
            QueryResult::MetricName(m) => Some(m.as_str()),
            _ => None,
        })
        .collect();
    assert_eq!(actual, *expected);
}

#[test]
fn it_queries_quantile_by_metric() {
    let mut source = MockDataSource::new();
    source.add_row("foo", build_data_row(TimeWindow::new(1, 2)));
    source.add_row("foo", build_data_row(TimeWindow::new(2, 3)));
    source.add_row("bar", build_data_row(TimeWindow::new(3, 4)));
    let query = "quantile(fetch(\"foo\"), 0.5)";
    let results = execute_query(&query, &mut source).expect("Could not execute query");
    assert_windows(&results, &vec![(1, 2, 0.5, 50), (2, 3, 0.5, 50)]);
}

#[test]
fn it_queries_multiple_quantiles() {
    let mut source = MockDataSource::new();
    source.add_row("foo", build_data_row(TimeWindow::new(10, 20)));
    source.add_row("foo", build_data_row(TimeWindow::new(20, 30)));
    source.add_row("foo", build_data_row(TimeWindow::new(30, 40)));
    source.add_row("foo", build_data_row(TimeWindow::new(40, 50)));
    let query = "quantile(fetch(\"foo\"), 0.1, 0.5, 0.9)";
    let results = execute_query(&query, &mut source).expect("Could not execute query");
    assert_windows(
        &results,
        &vec![
            (10, 20, 0.1, 10),
            (10, 20, 0.5, 50),
            (10, 20, 0.9, 90),
            (20, 30, 0.1, 10),
            (20, 30, 0.5, 50),
            (20, 30, 0.9, 90),
            (30, 40, 0.1, 10),
            (30, 40, 0.5, 50),
            (30, 40, 0.9, 90),
            (40, 50, 0.1, 10),
            (40, 50, 0.5, 50),
            (40, 50, 0.9, 90),
        ],
    );
}

#[test]
fn it_queries_quantile_select_time_range() {
    let mut source = MockDataSource::new();
    source.add_row("foo", build_data_row(TimeWindow::new(10, 20)));
    source.add_row("foo", build_data_row(TimeWindow::new(20, 30)));
    source.add_row("foo", build_data_row(TimeWindow::new(30, 40)));
    source.add_row("foo", build_data_row(TimeWindow::new(40, 50)));
    let query = "quantile(fetch(\"foo\", 20, 40), 0.5)";
    let results = execute_query(&query, &mut source).expect("Could not execute query");
    assert_windows(&results, &vec![(20, 30, 0.5, 50), (30, 40, 0.5, 50)]);
}

#[test]
fn it_queries_quantile_metric_not_found() {
    let mut source = MockDataSource::new();
    source.add_row("foo", build_data_row(TimeWindow::new(1, 2)));
    let query = "quantile(fetch(\"bar\"), 0.5)";
    let results = execute_query(&query, &mut source).expect("Could not execute query");
    assert_windows(&results, &vec![]);
}

#[test]
fn it_queries_quantile_group_by_hour() {
    let mut source = MockDataSource::new();
    source.add_row("foo", build_data_row(TimeWindow::new(10, 20)));
    source.add_row("foo", build_data_row(TimeWindow::new(20, 30)));
    source.add_row("foo", build_data_row(TimeWindow::new(30, 40)));
    source.add_row("foo", build_data_row(TimeWindow::new(40, 50)));
    source.add_row("foo", build_data_row(TimeWindow::new(4000, 4500)));
    let query = "quantile(group(\"hours\", fetch(\"foo\", 0, 10000)), 0.5)";
    let results = execute_query(&query, &mut source).expect("Could not execute query");
    assert_windows(&results, &vec![(10, 50, 0.5, 50), (4000, 4500, 0.5, 50)]);
}

#[test]
fn it_queries_quantile_group_by_day() {
    let mut source = MockDataSource::new();
    source.add_row("foo", build_data_row(TimeWindow::new(10, 20)));
    source.add_row("foo", build_data_row(TimeWindow::new(20, 30)));
    source.add_row("foo", build_data_row(TimeWindow::new(7000, 8000)));
    source.add_row("foo", build_data_row(TimeWindow::new(90000, 91000)));
    let query = "quantile(group(\"days\", fetch(\"foo\", 0, 100000)), 0.5)";
    let results = execute_query(&query, &mut source).expect("Could not execute query");
    assert_windows(
        &results,
        &vec![(10, 8000, 0.5, 50), (90000, 91000, 0.5, 50)],
    );
}

#[test]
fn it_coalesces_adjacent_time_windows() {
    let mut source = MockDataSource::new();
    source.add_row("foo", build_data_row(TimeWindow::new(0, 30)));
    source.add_row("foo", build_data_row(TimeWindow::new(30, 60)));
    let query = "quantile(coalesce(fetch(\"foo\")), 0.5)";
    let results = execute_query(&query, &mut source).expect("Could not execute query");
    assert_windows(&results, &vec![(0, 60, 0.5, 50)]);
}

#[test]
fn it_coalesces_overlapping_time_windows() {
    let mut source = MockDataSource::new();
    source.add_row("foo", build_data_row(TimeWindow::new(30, 60)));
    source.add_row("foo", build_data_row(TimeWindow::new(15, 35)));
    let query = "quantile(coalesce(fetch(\"foo\")), 0.5)";
    let results = execute_query(&query, &mut source).expect("Could not execute query");
    assert_windows(&results, &vec![(15, 60, 0.5, 50)]);
}

#[test]
fn it_coalesces_nonadjacent_time_windows() {
    let mut source = MockDataSource::new();
    source.add_row("foo", build_data_row(TimeWindow::new(10, 20)));
    source.add_row("foo", build_data_row(TimeWindow::new(40, 90)));
    let query = "quantile(coalesce(fetch(\"foo\")), 0.5)";
    let results = execute_query(&query, &mut source).expect("Could not execute query");
    assert_windows(&results, &vec![(10, 90, 0.5, 50)]);
}

#[test]
fn it_coalesces_idempotent() {
    let mut source = MockDataSource::new();
    source.add_row("foo", build_data_row(TimeWindow::new(10, 20)));
    source.add_row("foo", build_data_row(TimeWindow::new(40, 90)));
    let query = "quantile(coalesce(coalesce(fetch(\"foo\"))), 0.5)";
    let results = execute_query(&query, &mut source).expect("Could not execute query");
    assert_windows(&results, &vec![(10, 90, 0.5, 50)]);
}

#[test]
fn it_combines_time_series() {
    let mut source = MockDataSource::new();
    source.add_row("foo", build_data_row(TimeWindow::new(0, 30)));
    source.add_row("foo", build_data_row(TimeWindow::new(30, 60)));
    source.add_row("bar", build_data_row(TimeWindow::new(0, 30)));
    source.add_row("bar", build_data_row(TimeWindow::new(30, 60)));

    let query = "quantile(combine(fetch(\"foo\"), fetch(\"bar\")), 0.5)";
    let results = execute_query(&query, &mut source).expect("Could not execute query");
    assert_windows(&results, &vec![(0, 30, 0.5, 50), (30, 60, 0.5, 50)]);
}

#[test]
fn it_combines_empty_inputs() {
    let mut source = MockDataSource::new();
    let query = "quantile(combine(fetch(\"foo\"), fetch(\"bar\")), 0.5)";
    let results = execute_query(&query, &mut source).expect("Could not execute query");
    assert_windows(&results, &vec![]);
}

#[test]
fn it_combines_single_input() {
    let mut source = MockDataSource::new();
    source.add_row("foo", build_data_row(TimeWindow::new(0, 30)));
    let query = "quantile(combine(fetch(\"foo\"), fetch(\"bar\")), 0.5)";
    let results = execute_query(&query, &mut source).expect("Could not execute query");
    assert_windows(&results, &vec![(0, 30, 0.5, 50)]);
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
    let query = "quantile(combine(fetch(\"foo\"), fetch(\"bar\")), 0.5)";
    let results = execute_query(&query, &mut source).expect("Could not execute query");
    assert_windows(
        &results,
        &vec![
            (10, 30, 0.5, 50),
            (40, 50, 0.5, 50),
            (50, 60, 0.5, 50),
            (60, 80, 0.5, 50),
            (90, 100, 0.5, 50),
        ],
    );
}

#[test]
fn it_searches_metric_names() {
    let mut source = MockDataSource::new();
    source.add_row("foo", build_data_row(TimeWindow::new(10, 20)));
    source.add_row("bar", build_data_row(TimeWindow::new(15, 30)));
    source.add_row("foobar", build_data_row(TimeWindow::new(40, 50)));
    source.add_row("bazbar", build_data_row(TimeWindow::new(50, 60)));
    source.add_row("bazfoobar", build_data_row(TimeWindow::new(50, 60)));
    let query = "search(\"*foo*r\")";
    let results = execute_query(&query, &mut source).expect("Could not execute query");
    assert_metrics(&results, &vec!["bazfoobar", "foobar"]);
}
