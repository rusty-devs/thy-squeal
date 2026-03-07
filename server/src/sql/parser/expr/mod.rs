pub mod condition;
pub mod functions;
pub mod literal;

use super::super::ast::{BinaryOp, Expression, SqlStmt};
use super::super::error::{SqlError, SqlResult};
use super::Rule;

pub use condition::{parse_condition, parse_where_clause};
pub use functions::{parse_aggregate, parse_scalar_func};
pub use literal::parse_literal;

pub fn parse_expression(pair: pest::iterators::Pair<Rule>) -> SqlResult<Expression> {
    let mut inner = pair.into_inner();
    let first = inner
        .next()
        .ok_or_else(|| SqlError::Parse("Empty expression".to_string()))?;

    let mut left = parse_term(first)?;

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
        .ok_or_else(|| SqlError::Parse("Empty term".to_string()))?;

    let mut left = parse_factor(first)?;

    while let Some(op_pair) = inner.next() {
        let op_str = op_pair.as_str().trim();
        let op = match op_str {
            "*" => BinaryOp::Mul,
            "/" => BinaryOp::Div,
            "%" => {
                return Err(SqlError::Parse("Modulo operator not yet supported".to_string()));
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
        Rule::literal => Ok(Expression::Literal(parse_literal(first)?)),
        Rule::column_ref => {
            let parts: Vec<String> = first
                .into_inner()
                .filter(|p| p.as_rule() == Rule::identifier)
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
        Rule::expression => parse_expression(first),
        Rule::KW_NOT => {
            let next_factor = inner.next().ok_or_else(|| SqlError::Parse("Missing factor after NOT".to_string()))?;
            let _ = parse_factor(next_factor)?;
            Err(SqlError::Parse("NOT in expression factor not yet implemented".to_string()))
        }
        _ => {
            if first.as_str().starts_with('(')
                && let Some(inner_pair) = first.clone().into_inner().find(|p| p.as_rule() == Rule::select_stmt || p.as_rule() == Rule::select_stmt_inner)
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
