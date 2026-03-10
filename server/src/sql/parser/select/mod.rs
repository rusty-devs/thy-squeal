pub mod clauses;
pub mod columns;
pub mod joins;

use super::super::ast::{SelectStmt, SqlStmt};
use super::super::error::SqlResult;
use super::super::parser::Rule;
use super::expr::parse_condition;
pub use clauses::*;
pub use columns::*;
pub use joins::*;

pub fn parse_select(pair: pest::iterators::Pair<Rule>) -> SqlResult<SqlStmt> {
    let mut inner = if pair.as_rule() == Rule::select_stmt {
        pair.into_inner().next().unwrap().into_inner()
    } else {
        pair.into_inner()
    };

    // Skip KW_SELECT
    let _ = inner.next();

    let mut distinct = false;
    let mut columns = Vec::new();
    let mut table = String::new();
    let mut table_alias = None;
    let mut joins = Vec::new();
    let mut where_clause = None;
    let mut group_by = Vec::new();
    let mut having = None;
    let mut order_by = Vec::new();
    let mut limit = None;

    for p in inner {
        match p.as_rule() {
            Rule::distinct => distinct = true,
            Rule::select_columns => columns = parse_select_columns(p)?,
            Rule::table_name_with_alias => {
                let mut t_inner = p.into_inner();
                table = t_inner.next().unwrap().as_str().trim().to_string();
                if let Some(alias_pair) = t_inner.next() {
                    table_alias = Some(parse_alias(alias_pair)?);
                }
            }
            Rule::join_clause => joins.push(parse_join(p)?),
            Rule::where_clause => {
                where_clause = Some(parse_condition(p.into_inner().nth(1).unwrap())?)
            }
            Rule::group_by_clause => group_by = parse_group_by(p)?,
            Rule::having_clause => having = Some(parse_condition(p.into_inner().nth(1).unwrap())?),
            Rule::order_by_clause => order_by = parse_order_by(p)?,
            Rule::limit_clause => limit = Some(parse_limit(p)?),
            _ => {}
        }
    }

    Ok(SqlStmt::Select(SelectStmt {
        columns,
        table,
        table_alias,
        distinct,
        joins,
        where_clause,
        group_by,
        having,
        order_by,
        limit,
    }))
}
