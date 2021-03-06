use caesium_core::quantile::query::ApproxQuantile;
use caesium_core::time::window::TimeWindow;
use query::build::build_query;
use query::error::QueryError;
use query::ops::OpOutput;
use storage::datasource::DataSource;

#[derive(Debug)]
pub enum QueryResult {
    QuantileWindow(TimeWindow, f64, ApproxQuantile),
    MetricName(String),
}

pub fn execute_query<'a>(query: &str, source: &DataSource) -> Result<Vec<QueryResult>, QueryError> {
    let mut pipeline = build_query(query, source)?;
    let mut results = Vec::<QueryResult>::new();
    loop {
        let output = pipeline.get_next()?;
        match output {
            OpOutput::End => break,
            OpOutput::Quantile(window, phi, q_opt) => {
                if let Some(q) = q_opt {
                    let r = QueryResult::QuantileWindow(window, phi, q);
                    results.push(r);
                }
            }
            OpOutput::MetricName(metric) => {
                let r = QueryResult::MetricName(metric);
                results.push(r);
            }
            _ => return Err(QueryError::InvalidOutputType),
        }
    }
    Ok(results)
}
