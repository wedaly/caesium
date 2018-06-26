use query::error::QueryError;
use query::ops::coalesce::CoalesceOp;
use query::ops::combine::CombineOp;
use query::ops::fetch::{FetchOp, GroupType};
use query::ops::quantile::QuantileOp;
use query::ops::QueryOp;
use query::parser::ast::Expression;
use query::parser::parse::parse;
use storage::datasource::DataSource;

pub fn build_query<'a>(
    query: &str,
    source: &'a DataSource,
) -> Result<Box<QueryOp + 'a>, QueryError> {
    let expr = parse(query)?;
    match { *expr } {
        Expression::FunctionCall(name, args) => map_func_to_query_op(&name, &args, source),
        _ => Err(QueryError::InvalidExpressionType),
    }
}

fn map_func_to_query_op<'a>(
    name: &str,
    args: &[Box<Expression>],
    source: &'a DataSource,
) -> Result<Box<QueryOp + 'a>, QueryError> {
    match name {
        "fetch" => build_fetch_op(args, source),
        "quantile" => build_quantile_op(args, source),
        "coalesce" => build_coalesce_op(args, source),
        "combine" => build_combine_op(args, source),
        f => Err(QueryError::UnrecognizedFunction(f.to_string())),
    }
}

fn build_fetch_op<'a>(
    args: &[Box<Expression>],
    source: &'a DataSource,
) -> Result<Box<QueryOp + 'a>, QueryError> {
    let metric = get_string_arg(args, 0)?;
    let start_ts = get_optional_arg(get_int_arg, args, 1)?;
    let end_ts = get_optional_arg(get_int_arg, args, 2)?;
    let group_type_str = get_optional_arg(get_string_arg, args, 3)?;
    let group_type = match group_type_str {
        None => GroupType::Seconds,
        Some(s) => GroupType::from_str(&s)?,
    };
    let op = FetchOp::new(metric, source, group_type, start_ts, end_ts)?;
    Ok(Box::new(op))
}

fn build_quantile_op<'a>(
    args: &[Box<Expression>],
    source: &'a DataSource,
) -> Result<Box<QueryOp + 'a>, QueryError> {
    let phi = get_float_arg(args, 0)?;
    let input = get_func_arg(args, 1, source)?;
    let op = QuantileOp::new(phi, input)?;
    Ok(Box::new(op))
}

fn build_coalesce_op<'a>(
    args: &[Box<Expression>],
    source: &'a DataSource,
) -> Result<Box<QueryOp + 'a>, QueryError> {
    let input = get_func_arg(args, 0, source)?;
    let op = CoalesceOp::new(input);
    Ok(Box::new(op))
}

fn build_combine_op<'a>(
    args: &[Box<Expression>],
    source: &'a DataSource,
) -> Result<Box<QueryOp + 'a>, QueryError> {
    let mut inputs = Vec::new();
    for i in 0..args.len() {
        inputs.push(get_func_arg(args, i, source)?);
    }
    let op = CombineOp::new(inputs);
    Ok(Box::new(op))
}

fn get_optional_arg<F, T>(
    f: F,
    args: &[Box<Expression>],
    idx: usize,
) -> Result<Option<T>, QueryError>
where
    F: FnOnce(&[Box<Expression>], usize) -> Result<T, QueryError>,
{
    match f(args, idx) {
        Ok(v) => Ok(Some(v)),
        Err(QueryError::MissingArg) => Ok(None),
        Err(err) => Err(err),
    }
}

fn get_string_arg(args: &[Box<Expression>], idx: usize) -> Result<String, QueryError> {
    match args.get(idx) {
        Some(expr) => match **expr {
            Expression::StringLiteral(ref s) => Ok(s.to_string()),
            _ => Err(QueryError::InvalidArgType),
        },
        None => Err(QueryError::MissingArg),
    }
}

fn get_int_arg(args: &[Box<Expression>], idx: usize) -> Result<u64, QueryError> {
    match args.get(idx) {
        Some(expr) => match **expr {
            Expression::IntLiteral(i) => Ok(i),
            _ => Err(QueryError::InvalidArgType),
        },
        None => Err(QueryError::MissingArg),
    }
}

fn get_float_arg(args: &[Box<Expression>], idx: usize) -> Result<f64, QueryError> {
    match args.get(idx) {
        Some(expr) => match **expr {
            Expression::FloatLiteral(f) => Ok(f),
            _ => Err(QueryError::InvalidArgType),
        },
        None => Err(QueryError::MissingArg),
    }
}

fn get_func_arg<'a>(
    args: &[Box<Expression>],
    idx: usize,
    source: &'a DataSource,
) -> Result<Box<QueryOp + 'a>, QueryError> {
    match args.get(idx) {
        Some(expr) => match **expr {
            Expression::FunctionCall(ref name, ref args) => {
                map_func_to_query_op(&name, &args, source)
            }
            _ => Err(QueryError::InvalidArgType),
        },
        None => Err(QueryError::MissingArg),
    }
}
