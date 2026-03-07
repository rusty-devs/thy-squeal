use super::super::super::ast::{ComparisonOp, Condition, Expression, LogicalOp};
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
    let first = inner
        .next()
        .ok_or_else(|| SqlError::Parse("Empty condition".to_string()))?;

    match first.as_rule() {
        Rule::expression => {
            let left = parse_expression(first)?;
            let op_pair = inner
                .next()
                .ok_or_else(|| SqlError::Parse("Missing operator in condition".to_string()))?;

            match op_pair.as_rule() {
                Rule::comparison_op => {
                    let op_str = op_pair.as_str().to_uppercase();
                    if op_str == "IN" {
                        let next_expr = inner.next().ok_or_else(|| {
                            SqlError::Parse("Expected subquery after IN".to_string())
                        })?;
                        // Find select_stmt recursively in this expression
                        let subquery = find_select_stmt(next_expr)?;
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
                        _ => {
                            return Err(SqlError::Parse(format!(
                                "Unsupported comparison operator: {}",
                                op_pair.as_str()
                            )));
                        }
                    };
                    let right_pair = inner
                        .next()
                        .ok_or_else(|| {
                            SqlError::Parse("Missing right side of comparison".to_string())
                        })?;
                    
                    let right = if right_pair.as_rule() == Rule::expression {
                        parse_expression(right_pair)?
                    } else {
                        // Handle (select_stmt)
                        let subquery = find_select_stmt(right_pair)?;
                        Expression::Subquery(Box::new(subquery))
                    };
                    
                    Ok(Condition::Comparison(left, op, right))
                }
                Rule::KW_IS => {
                    let mut is_not = false;
                    let next = inner.next().ok_or_else(|| SqlError::Parse("Missing token after IS".to_string()))?;
                    
                    let final_token = if next.as_rule() == Rule::KW_NOT {
                        is_not = true;
                        inner.next().ok_or_else(|| SqlError::Parse("Expected NULL after IS NOT".to_string()))?
                    } else {
                        next
                    };

                    if final_token.as_rule() != Rule::KW_NULL {
                        return Err(SqlError::Parse(format!("Expected NULL after IS, got {:?}", final_token.as_rule())));
                    }

                    if is_not {
                        Ok(Condition::IsNotNull(left))
                    } else {
                        Ok(Condition::IsNull(left))
                    }
                }
                _ => Err(SqlError::Parse(format!(
                    "Unexpected rule in condition: {:?}",
                    op_pair.as_rule()
                ))),
            }
        }
        Rule::condition => {
            let left = parse_condition(first)?;
            if let Some(op_pair) = inner.find(|p| p.as_rule() == Rule::logical_op) {
                let op = match op_pair
                    .into_inner()
                    .next()
                    .ok_or_else(|| SqlError::Parse("Empty logical operator".to_string()))?
                    .as_rule()
                {
                    Rule::KW_AND => LogicalOp::And,
                    Rule::KW_OR => LogicalOp::Or,
                    r => {
                        return Err(SqlError::Parse(format!(
                            "Unsupported logical operator: {:?}",
                            r
                        )));
                    }
                };
                let right = parse_condition(
                    inner
                        .find(|p| p.as_rule() == Rule::condition)
                        .ok_or_else(|| SqlError::Parse("Missing right condition".to_string()))?,
                )?;
                Ok(Condition::Logical(Box::new(left), op, Box::new(right)))
            } else {
                Ok(left)
            }
        }
        _ => Err(SqlError::Parse(format!(
            "Unsupported condition rule: {:?}",
            first.as_rule()
        ))),
    }
}

pub fn find_select_stmt(pair: pest::iterators::Pair<Rule>) -> SqlResult<super::super::super::ast::SelectStmt> {
    if pair.as_rule() == Rule::select_stmt || pair.as_rule() == Rule::select_stmt_inner {
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
