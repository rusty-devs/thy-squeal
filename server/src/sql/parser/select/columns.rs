use super::super::super::ast::{Expression, SelectColumn};
use super::super::super::error::{SqlError, SqlResult};
use crate::sql::parser::Rule;
use super::super::expr::parse_expression;

pub fn parse_select_columns(pair: pest::iterators::Pair<Rule>) -> SqlResult<Vec<SelectColumn>> {
    let mut columns = Vec::new();
    let inner = pair.into_inner();
    for p in inner {
        match p.as_rule() {
            Rule::star => columns.push(SelectColumn {
                expr: Expression::Star,
                alias: None,
            }),
            Rule::column_list => {
                for col_pair in p.into_inner() {
                    columns.push(parse_column_expr(col_pair)?);
                }
            }
            _ => {}
        }
    }
    Ok(columns)
}

pub fn parse_column_expr(pair: pest::iterators::Pair<Rule>) -> SqlResult<SelectColumn> {
    let mut inner = pair.into_inner();
    let expr_pair = inner.next().unwrap();
    let expr = parse_expression(expr_pair)?;
    let mut alias = None;
    if let Some(alias_pair) = inner.next() {
        alias = Some(parse_alias(alias_pair)?);
    }
    Ok(SelectColumn { expr, alias })
}

pub fn parse_alias(pair: pest::iterators::Pair<Rule>) -> SqlResult<String> {
    let mut inner = pair.clone().into_inner();
    let first = inner.next().unwrap();
    if first.as_rule() == Rule::identifier {
        Ok(first.as_str().trim().to_string())
    } else {
        // Rule::KW_AS
        let id = inner
            .next()
            .ok_or_else(|| SqlError::Parse("Missing identifier after AS".to_string()))?;
        Ok(id.as_str().trim().to_string())
    }
}
