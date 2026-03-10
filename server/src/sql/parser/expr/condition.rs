use super::super::super::ast::{ComparisonOp, Condition, LogicalOp};
use super::super::super::error::{SqlError, SqlResult};
use super::super::Rule;
use super::parse_expression;

pub fn parse_where_clause(pair: pest::iterators::Pair<Rule>) -> SqlResult<Condition> {
    let mut inner = pair.into_inner();
    let cond_pair = inner
        .find(|p| p.as_rule() == Rule::condition)
        .ok_or_else(|| SqlError::Parse("Missing condition in WHERE clause".to_string()))?;
    parse_condition(cond_pair)
}

pub fn parse_condition(pair: pest::iterators::Pair<Rule>) -> SqlResult<Condition> {
    let mut inner = pair.into_inner();
    
    let first = inner.next().ok_or_else(|| SqlError::Parse("Empty condition".to_string()))?;
    let mut left = parse_conjunction(first)?;

    while let Some(op_pair) = inner.next() {
        if op_pair.as_rule() == Rule::KW_OR {
            let right_pair = inner.next().ok_or_else(|| SqlError::Parse("Missing condition after OR".to_string()))?;
            let right = parse_conjunction(right_pair)?;
            left = Condition::Logical(Box::new(left), LogicalOp::Or, Box::new(right));
        }
    }

    Ok(left)
}

pub fn parse_conjunction(pair: pest::iterators::Pair<Rule>) -> SqlResult<Condition> {
    let mut inner = pair.into_inner();
    
    let first = inner.next().ok_or_else(|| SqlError::Parse("Empty conjunction".to_string()))?;
    let mut left = parse_primary_condition(first)?;

    while let Some(op_pair) = inner.next() {
        if op_pair.as_rule() == Rule::KW_AND {
            let right_pair = inner.next().ok_or_else(|| SqlError::Parse("Missing condition after AND".to_string()))?;
            let right = parse_primary_condition(right_pair)?;
            left = Condition::Logical(Box::new(left), LogicalOp::And, Box::new(right));
        }
    }

    Ok(left)
}

pub fn parse_primary_condition(pair: pest::iterators::Pair<Rule>) -> SqlResult<Condition> {
    let mut inner = pair.into_inner();
    let first = inner.next().ok_or_else(|| SqlError::Parse("Empty primary condition".to_string()))?;

    match first.as_rule() {
        Rule::condition => parse_condition(first),
        Rule::KW_NOT => {
            let sub = inner.next().ok_or_else(|| SqlError::Parse("Missing condition after NOT".to_string()))?;
            Ok(Condition::Not(Box::new(parse_primary_condition(sub)?)))
        }
        Rule::expression => {
            let left = parse_expression(first)?;
            let next = inner.next().ok_or_else(|| SqlError::Parse("Missing operator in condition".to_string()))?;

            match next.as_rule() {
                Rule::comparison_op => {
                    let op_str = next.as_str().to_uppercase();
                    if op_str == "IN" {
                        let sub_pair = inner.next().ok_or_else(|| SqlError::Parse("Missing subquery after IN".to_string()))?;
                        let subquery = find_select_stmt(sub_pair)?;
                        return Ok(Condition::InSubquery(left, Box::new(subquery)));
                    }
                    let op = match op_str.as_str() {
                        "=" => ComparisonOp::Eq,
                        "!=" | "<>" => ComparisonOp::NotEq,
                        "<" => ComparisonOp::Lt,
                        ">" => ComparisonOp::Gt,
                        "<=" => ComparisonOp::LtEq,
                        ">=" => ComparisonOp::GtEq,
                        "LIKE" => ComparisonOp::Like,
                        _ => return Err(SqlError::Parse(format!("Unsupported operator: {}", op_str))),
                    };
                    let right_pair = inner.next().ok_or_else(|| SqlError::Parse("Missing right side of comparison".to_string()))?;
                    let right = parse_expression(right_pair)?;
                    Ok(Condition::Comparison(left, op, right))
                }
                Rule::KW_IS => {
                    let mut is_not = false;
                    let mut tok = inner.next().ok_or_else(|| SqlError::Parse("Missing token after IS".to_string()))?;
                    if tok.as_rule() == Rule::KW_NOT {
                        is_not = true;
                        tok = inner.next().ok_or_else(|| SqlError::Parse("Missing token after IS NOT".to_string()))?;
                    }
                    if tok.as_rule() == Rule::KW_NULL {
                        if is_not { Ok(Condition::IsNotNull(left)) } else { Ok(Condition::IsNull(left)) }
                    } else {
                        Err(SqlError::Parse("Expected NULL after IS [NOT]".to_string()))
                    }
                }
                Rule::KW_IN => {
                    let sub_pair = inner.next().ok_or_else(|| SqlError::Parse("Missing subquery after IN".to_string()))?;
                    let subquery = find_select_stmt(sub_pair)?;
                    Ok(Condition::InSubquery(left, Box::new(subquery)))
                }
                _ => Err(SqlError::Parse(format!("Unexpected token in condition: {:?}", next.as_rule()))),
            }
        }
        _ => Err(SqlError::Parse(format!("Unsupported primary condition: {:?}", first.as_rule()))),
    }
}

pub fn find_select_stmt(
    pair: pest::iterators::Pair<Rule>,
) -> SqlResult<super::super::super::ast::SelectStmt> {
    if pair.as_rule() == Rule::select_stmt_inner {
        return super::super::select::parse_select_inner(pair);
    }
    if pair.as_rule() == Rule::select_stmt {
        // select_stmt might be with_clause? ~ select_stmt_inner
        // But for subqueries we usually want the inner part or handle the full thing.
        let stmt = super::super::select::parse_select(pair.clone())?;
        if let super::super::super::ast::SqlStmt::Select(s) = stmt {
            return Ok(s);
        }
    }
    for inner in pair.into_inner() {
        if let Ok(s) = find_select_stmt(inner) {
            return Ok(s);
        }
    }
    Err(SqlError::Parse(
        "Could not find SELECT statement in subquery context".to_string(),
    ))
}
