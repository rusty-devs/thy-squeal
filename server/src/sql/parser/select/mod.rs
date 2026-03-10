pub mod clauses;
pub mod columns;
pub mod joins;

use super::super::ast::{SelectStmt, SqlStmt};
use super::super::error::{SqlError, SqlResult};
use super::super::parser::Rule;
use super::expr::parse_condition;
pub use clauses::*;
pub use columns::*;
pub use joins::*;

pub fn parse_select(pair: pest::iterators::Pair<Rule>) -> SqlResult<SqlStmt> {
    if pair.as_rule() == Rule::select_stmt_inner {
        let stmt = parse_select_inner(pair)?;
        return Ok(SqlStmt::Select(stmt));
    }

    let mut inner = pair.into_inner();
    let mut with_clause = None;

    let mut first = inner.next().unwrap();
    if first.as_rule() == Rule::with_clause {
        let mut ctes = Vec::new();
        for cte_pair in first.into_inner() {
            if cte_pair.as_rule() == Rule::cte_definition {
                let mut cte_inner = cte_pair.into_inner();
                let name = cte_inner.next().unwrap().as_str().trim().to_string();
                // Skip KW_AS
                let _ = cte_inner.next();
                let query_pair = cte_inner.next().unwrap();
                let query = parse_select_inner(query_pair)?;
                ctes.push(crate::sql::ast::Cte { name, query });
            }
        }
        with_clause = Some(crate::sql::ast::WithClause { ctes });
        first = inner
            .next()
            .ok_or_else(|| SqlError::Parse("Missing SELECT after WITH".to_string()))?;
    }

    let mut stmt = parse_select_inner(first)?;
    stmt.with_clause = with_clause;
    Ok(SqlStmt::Select(stmt))
}

pub fn parse_select_inner(pair: pest::iterators::Pair<Rule>) -> SqlResult<SelectStmt> {
    let mut inner = if pair.as_rule() == Rule::select_stmt_inner {
        pair.into_inner()
    } else {
        return Err(SqlError::Parse(format!(
            "Expected select_stmt_inner, got {:?}",
            pair.as_rule()
        )));
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
            Rule::from_clause => {
                for from_p in p.into_inner() {
                    match from_p.as_rule() {
                        Rule::table_name_with_alias => {
                            let mut t_inner = from_p.into_inner();
                            table = t_inner.next().unwrap().as_str().trim().to_string();
                            if let Some(alias_pair) = t_inner.next() {
                                table_alias = Some(parse_alias(alias_pair)?);
                            }
                        }
                        Rule::join_clause => joins.push(parse_join(from_p)?),
                        Rule::where_clause => {
                            where_clause = Some(parse_condition(from_p.into_inner().nth(1).unwrap())?)
                        }
                        Rule::group_by_clause => group_by = parse_group_by(from_p)?,
                        Rule::having_clause => having = Some(parse_condition(from_p.into_inner().nth(1).unwrap())?),
                        Rule::order_by_clause => order_by = parse_order_by(from_p)?,
                        Rule::limit_clause => limit = Some(parse_limit(from_p)?),
                        _ => {}
                    }
                }
            }
            _ => {}
        }
    }

    Ok(SelectStmt {
        with_clause: None,
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
    })
}
