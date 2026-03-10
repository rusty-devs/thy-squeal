use super::super::super::ast::{Expression, SelectColumn};
use super::super::super::error::{SqlError, SqlResult};
use super::super::super::parser::Rule;
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
    let inner = pair.into_inner().next().unwrap();
    if inner.as_rule() == Rule::identifier {
        Ok(inner.as_str().trim().to_string())
    } else {
        // Skip KW_AS
        let id = inner.into_inner().next().unwrap();
        Ok(id.as_str().trim().to_string())
    }
}
