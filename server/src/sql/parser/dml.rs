use super::super::ast::{DeleteStmt, InsertStmt, SqlStmt, UpdateStmt};
use super::super::error::{SqlError, SqlResult};
use super::super::parser::Rule;
use super::expr::{parse_expression, parse_literal};

pub fn parse_insert(pair: pest::iterators::Pair<Rule>) -> SqlResult<SqlStmt> {
    let inner = pair.into_inner();

    let mut table = None;
    let mut values = Vec::new();

    for p in inner {
        match p.as_rule() {
            Rule::table_name => table = Some(p.as_str().trim().to_string()),
            Rule::value_list => {
                values = parse_value_list(p)?;
            }
            _ => {}
        }
    }

    let table = table.ok_or_else(|| SqlError::Parse("Missing table name".to_string()))?;
    if values.is_empty() {
        return Err(SqlError::Parse("Missing values".to_string()));
    }

    Ok(SqlStmt::Insert(InsertStmt { table, values }))
}

pub fn parse_update(pair: pest::iterators::Pair<Rule>) -> SqlResult<SqlStmt> {
    let mut inner = pair.into_inner();
    // Skip KW_UPDATE
    let _ = inner.next();

    let table = inner
        .find(|p| p.as_rule() == Rule::table_name)
        .map(|p| p.as_str().trim().to_string())
        .ok_or_else(|| SqlError::Parse("Missing table name".to_string()))?;

    let set_list = inner
        .find(|p| p.as_rule() == Rule::set_list)
        .ok_or_else(|| SqlError::Parse("Missing SET clause".to_string()))?
        .into_inner();

    let mut assignments = Vec::new();
    for item in set_list {
        if item.as_rule() != Rule::set_item {
            continue;
        }
        let mut item_inner = item.into_inner();
        let col_name = item_inner
            .next()
            .ok_or_else(|| SqlError::Parse("Missing column name in SET".to_string()))?
            .as_str()
            .trim()
            .to_string();
        let expr = parse_expression(
            item_inner
                .next()
                .ok_or_else(|| SqlError::Parse("Missing expression in SET".to_string()))?,
        )?;
        assignments.push((col_name, expr));
    }

    let where_clause = if let Some(where_pair) = inner.find(|p| p.as_rule() == Rule::where_clause) {
        Some(super::expr::parse_where_clause(where_pair)?)
    } else {
        None
    };

    Ok(SqlStmt::Update(UpdateStmt {
        table,
        assignments,
        where_clause,
    }))
}

pub fn parse_delete(pair: pest::iterators::Pair<Rule>) -> SqlResult<SqlStmt> {
    let mut inner = pair.into_inner();
    // Skip KW_DELETE, KW_FROM
    let _ = inner.next();
    let _ = inner.next();

    let table = inner
        .find(|p| p.as_rule() == Rule::table_name)
        .map(|p| p.as_str().trim().to_string())
        .ok_or_else(|| SqlError::Parse("Missing table name".to_string()))?;

    let where_clause = if let Some(where_pair) = inner.find(|p| p.as_rule() == Rule::where_clause) {
        Some(super::expr::parse_where_clause(where_pair)?)
    } else {
        None
    };

    Ok(SqlStmt::Delete(DeleteStmt {
        table,
        where_clause,
    }))
}

pub fn parse_search(pair: pest::iterators::Pair<Rule>) -> SqlResult<SqlStmt> {
    let mut inner = pair.into_inner();
    // Skip KW_SEARCH
    let _ = inner.next();

    let table = inner
        .next()
        .map(|p| p.as_str().trim().to_string())
        .ok_or_else(|| SqlError::Parse("Missing table name in SEARCH".to_string()))?;

    let query = inner
        .next()
        .map(|p| p.as_str().trim_matches('\'').to_string())
        .ok_or_else(|| SqlError::Parse("Missing query in SEARCH".to_string()))?;

    Ok(SqlStmt::Search(crate::sql::ast::SearchStmt { table, query }))
}

pub fn parse_value_list(
    pair: pest::iterators::Pair<Rule>,
) -> SqlResult<Vec<crate::storage::Value>> {
    let inner = pair.into_inner();
    let mut values = Vec::new();
    for p in inner {
        if p.as_rule() == Rule::literal {
            values.push(parse_literal(p)?);
        }
    }
    Ok(values)
}
