pub mod condition;
pub mod functions;
pub mod literal;

use super::super::ast::{BinaryOp, Expression, SqlStmt};
use super::super::error::{SqlError, SqlResult};
use crate::sql::parser::Rule;

pub use condition::{parse_condition, parse_where_clause};
pub use functions::{parse_aggregate, parse_scalar_func};
pub use literal::parse_literal;

pub fn parse_any_expression(pair: pest::iterators::Pair<Rule>) -> SqlResult<Expression> {
    match pair.as_rule() {
        Rule::expression => parse_expression(pair),
        Rule::term => parse_term(pair),
        Rule::factor => parse_factor(pair),
        Rule::literal
        | Rule::string_literal
        | Rule::number_literal
        | Rule::boolean_literal
        | Rule::KW_NULL => Ok(Expression::Literal(parse_literal(pair)?)),
        Rule::placeholder => parse_placeholder(pair),
        Rule::aggregate_func => parse_aggregate(pair),
        Rule::scalar_func => parse_scalar_func(pair),
        _ => Err(SqlError::Parse(format!(
            "Unexpected rule for expression: {:?}",
            pair.as_rule()
        ))),
    }
}

pub fn parse_expression(pair: pest::iterators::Pair<Rule>) -> SqlResult<Expression> {
    let mut inner = pair.into_inner();
    let first = inner
        .next()
        .ok_or_else(|| SqlError::Parse("Empty expression/term".to_string()))?;

    let mut left = parse_any_expression(first)?;

    while let Some(op_pair) = inner.next() {
        let op_str = op_pair.as_str().trim();
        let op = match op_str {
            "+" => BinaryOp::Add,
            "-" => BinaryOp::Sub,
            _ => {
                return Err(SqlError::Parse(format!(
                    "Unsupported binary operator in expression: '{}'",
                    op_str
                )));
            }
        };
        let right_pair = inner
            .next()
            .ok_or_else(|| SqlError::Parse("Missing right term".to_string()))?;
        let right = parse_term(right_pair)?;
        left = Expression::BinaryOp(Box::new(left), op, Box::new(right));
    }

    Ok(left)
}

pub fn parse_term(pair: pest::iterators::Pair<Rule>) -> SqlResult<Expression> {
    let mut inner = pair.into_inner();
    let first = inner
        .next()
        .ok_or_else(|| SqlError::Parse("Empty expression/term".to_string()))?;

    let mut left = parse_any_expression(first)?;

    while let Some(op_pair) = inner.next() {
        let op_str = op_pair.as_str().trim();
        let op = match op_str {
            "*" => BinaryOp::Mul,
            "/" => BinaryOp::Div,
            "%" => {
                return Err(SqlError::Parse(
                    "Modulo operator not yet supported".to_string(),
                ));
            }
            _ => {
                return Err(SqlError::Parse(format!(
                    "Unsupported binary operator in term: '{}'",
                    op_str
                )));
            }
        };
        let right_pair = inner
            .next()
            .ok_or_else(|| SqlError::Parse("Missing right factor".to_string()))?;
        let right = parse_factor(right_pair)?;
        left = Expression::BinaryOp(Box::new(left), op, Box::new(right));
    }

    Ok(left)
}

pub fn parse_factor(pair: pest::iterators::Pair<Rule>) -> SqlResult<Expression> {
    let mut inner = pair.clone().into_inner();
    let first = inner
        .next()
        .ok_or_else(|| SqlError::Parse("Empty factor".to_string()))?;

    match first.as_rule() {
        Rule::aggregate_func => parse_aggregate(first),
        Rule::scalar_func => parse_scalar_func(first),
        Rule::literal
        | Rule::string_literal
        | Rule::number_literal
        | Rule::boolean_literal
        | Rule::KW_NULL => Ok(Expression::Literal(parse_literal(first)?)),
        Rule::placeholder => parse_placeholder(first),
        Rule::column_ref => {
            // If it matches KW_NULL exactly, it might be a mistake in rule precedence.
            if first.as_str().to_uppercase() == "NULL" {
                return Ok(Expression::Literal(crate::storage::Value::Null));
            }
            let parts: Vec<String> = first
                .into_inner()
                .filter(|p| p.as_rule() == Rule::path_identifier)
                .map(|p| p.as_str().trim().to_string())
                .collect();
            Ok(Expression::Column(parts.join(".")))
        }

        Rule::select_stmt | Rule::select_stmt_inner => {
            let stmt = super::select::parse_select(first)?;
            if let SqlStmt::Select(s) = stmt {
                Ok(Expression::Subquery(Box::new(s)))
            } else {
                Err(SqlError::Parse(
                    "Expected SELECT statement in subquery".to_string(),
                ))
            }
        }
        Rule::expression => parse_any_expression(first),
        Rule::KW_NOT => {
            let next_factor = inner
                .next()
                .ok_or_else(|| SqlError::Parse("Missing factor after NOT".to_string()))?;
            let _ = parse_factor(next_factor)?;
            Err(SqlError::Parse(
                "NOT in expression factor not yet implemented".to_string(),
            ))
        }
        _ => {
            if first.as_str().starts_with('(')
                && let Some(inner_pair) = first.clone().into_inner().find(|p| {
                    p.as_rule() == Rule::select_stmt || p.as_rule() == Rule::select_stmt_inner
                })
            {
                let stmt = super::select::parse_select(inner_pair)?;
                if let SqlStmt::Select(s) = stmt {
                    return Ok(Expression::Subquery(Box::new(s)));
                }
            }

            Err(SqlError::Parse(format!(
                "Unsupported factor rule: {:?}",
                first.as_rule()
            )))
        }
    }
}

pub fn parse_placeholder(pair: pest::iterators::Pair<Rule>) -> SqlResult<Expression> {
    let s = pair.as_str();
    if s == "?" {
        Ok(Expression::Placeholder(0))
    } else if let Some(idx_str) = s.strip_prefix('$') {
        let idx = idx_str
            .parse::<usize>()
            .map_err(|_| SqlError::Parse(format!("Invalid placeholder index: {}", s)))?;
        Ok(Expression::Placeholder(idx))
    } else {
        Err(SqlError::Parse(format!("Invalid placeholder: {}", s)))
    }
}
