use super::super::super::ast::{Expression, SqlStmt, UpdateStmt};
use super::super::super::error::{SqlError, SqlResult};
use super::super::super::parser::Rule;
use super::super::expr::{parse_any_expression, parse_where_clause};

pub fn parse_update(pair: pest::iterators::Pair<Rule>) -> SqlResult<SqlStmt> {
    let mut inner = pair.into_inner();
    // Skip KW_UPDATE
    let _ = inner.next();

    let table = inner
        .next()
        .map(|p| p.as_str().trim().to_string())
        .ok_or_else(|| SqlError::Parse("Missing table name in UPDATE".to_string()))?;

    // Skip KW_SET
    let _ = inner.next();

    let set_list_pair = inner
        .next()
        .ok_or_else(|| SqlError::Parse("Missing SET list in UPDATE".to_string()))?;
    let assignments = parse_set_list(set_list_pair)?;

    let mut where_clause = None;
    for p in inner {
        if p.as_rule() == Rule::where_clause {
            where_clause = Some(parse_where_clause(p)?);
        }
    }

    Ok(SqlStmt::Update(UpdateStmt {
        table,
        assignments,
        where_clause,
    }))
}

pub fn parse_set_list(pair: pest::iterators::Pair<Rule>) -> SqlResult<Vec<(String, Expression)>> {
    let mut assignments = Vec::new();
    for item in pair.into_inner() {
        let mut item_inner = item.into_inner();
        let col_name = item_inner
            .next()
            .ok_or_else(|| SqlError::Parse("Missing column name in SET".to_string()))?
            .as_str()
            .trim()
            .to_string();
        let expr = parse_any_expression(
            item_inner
                .next()
                .ok_or_else(|| SqlError::Parse("Missing expression in SET".to_string()))?,
        )?;
        assignments.push((col_name, expr));
    }
    Ok(assignments)
}
