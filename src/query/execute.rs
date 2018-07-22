use query::build::build_query;
use query::error::QueryError;
use query::ops::OpOutput;
use query::result::QueryResult;
use storage::datasource::DataSource;

pub fn execute_query<'a>(query: &str, source: &DataSource) -> Result<Vec<QueryResult>, QueryError> {
    let mut pipeline = build_query(query, source)?;
    let mut results = Vec::<QueryResult>::new();
    loop {
        let output = pipeline.get_next()?;
        match output {
            OpOutput::End => break,
            OpOutput::Quantile(window, q_opt) => {
                if let Some(q) = q_opt {
                    let r = QueryResult::new(window, q);
                    results.push(r);
                }
            }
            _ => return Err(QueryError::InvalidOutputType),
        }
    }
    Ok(results)
}
