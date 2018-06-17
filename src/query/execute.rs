use query::build::build_query;
use query::error::QueryError;
use query::ops::OpOutput;
use query::result::QueryResult;
use storage::datasource::DataSource;

pub fn execute_query<'a>(
    query: &str,
    source: &mut DataSource,
) -> Result<Vec<QueryResult>, QueryError> {
    let mut pipeline = build_query(query, source)?;
    let mut results = Vec::<QueryResult>::new();
    loop {
        let output = pipeline.get_next()?;
        match output {
            OpOutput::End => break,
            OpOutput::Quantile(range, value_opt) => {
                if let Some(value) = value_opt {
                    results.push(QueryResult {
                        range: range,
                        value: value,
                    })
                }
            }
            _ => return Err(QueryError::InvalidOutputType),
        }
    }
    Ok(results)
}
