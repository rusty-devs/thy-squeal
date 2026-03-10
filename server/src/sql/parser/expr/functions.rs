use super::super::super::ast::{
    AggregateType, Expression, FunctionCall, ScalarFuncType, ScalarFunction,
};
use super::super::super::error::{SqlError, SqlResult};
use super::super::Rule;
use super::parse_expression;

pub fn parse_aggregate(pair: pest::iterators::Pair<Rule>) -> SqlResult<Expression> {
    let mut inner = pair.into_inner();
    let agg_type_pair = inner
        .next()
        .ok_or_else(|| SqlError::Parse("Missing aggregate type".to_string()))?;
    let agg_type = parse_aggregate_type(agg_type_pair)?;

    let mut args = Vec::new();
    let arg_pair = inner
        .next()
        .ok_or_else(|| SqlError::Parse("Missing aggregate argument".to_string()))?;
    match arg_pair.as_rule() {
        Rule::star => args.push(Expression::Star),
        Rule::expression => args.push(parse_expression(arg_pair)?),
        _ => {
            if arg_pair.as_str() == "*" {
                args.push(Expression::Star);
            } else {
                return Err(SqlError::Parse(format!(
                    "Unexpected aggregate argument: {:?}",
                    arg_pair.as_rule()
                )));
            }
        }
    }

    Ok(Expression::FunctionCall(FunctionCall {
        name: agg_type,
        args,
    }))
}

pub fn parse_aggregate_type(pair: pest::iterators::Pair<Rule>) -> SqlResult<AggregateType> {
    let kw = pair
        .into_inner()
        .next()
        .ok_or_else(|| SqlError::Parse("Missing aggregate keyword".to_string()))?;
    match kw.as_rule() {
        Rule::KW_COUNT => Ok(AggregateType::Count),
        Rule::KW_SUM => Ok(AggregateType::Sum),
        Rule::KW_AVG => Ok(AggregateType::Avg),
        Rule::KW_MIN => Ok(AggregateType::Min),
        Rule::KW_MAX => Ok(AggregateType::Max),
        _ => Err(SqlError::Parse(format!(
            "Unknown aggregate type: {:?}",
            kw.as_rule()
        ))),
    }
}

pub fn parse_scalar_func(pair: pest::iterators::Pair<Rule>) -> SqlResult<Expression> {
    let mut inner = pair.into_inner();
    let name_pair = inner
        .next()
        .ok_or_else(|| SqlError::Parse("Missing scalar function name".to_string()))?;
    let name = parse_scalar_func_type(name_pair)?;
    let arg_pair = inner
        .next()
        .ok_or_else(|| SqlError::Parse("Missing scalar function argument".to_string()))?;
    let arg = parse_expression(arg_pair)?;

    Ok(Expression::ScalarFunc(ScalarFunction {
        name,
        arg: Box::new(arg),
    }))
}

pub fn parse_scalar_func_type(pair: pest::iterators::Pair<Rule>) -> SqlResult<ScalarFuncType> {
    let name = pair.as_str().to_uppercase();
    match name.as_str() {
        "LOWER" => Ok(ScalarFuncType::Lower),
        "UPPER" => Ok(ScalarFuncType::Upper),
        "LENGTH" => Ok(ScalarFuncType::Length),
        "ABS" => Ok(ScalarFuncType::Abs),
        _ => Err(SqlError::Parse(format!(
            "Unknown scalar function: {}",
            name
        ))),
    }
}
