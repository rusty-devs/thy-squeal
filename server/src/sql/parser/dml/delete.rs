use super::super::super::ast::{DeleteStmt, SqlStmt};
use super::super::super::error::{SqlError, SqlResult};
use super::super::super::parser::Rule;
use super::super::expr::{parse_any_expression, parse_where_clause};

pub fn parse_delete(pair: pest::iterators::Pair<Rule>) -> SqlResult<SqlStmt> {
    let mut inner = pair.into_inner();
    // Skip KW_DELETE, KW_FROM
    let _ = inner.next();
    let _ = inner.next();

    let table = inner
        .next()
        .map(|p| p.as_str().trim().to_string())
        .ok_or_else(|| SqlError::Parse("Missing table name in DELETE".to_string()))?;

    let mut where_clause = None;
    for p in inner {
        if p.as_rule() == Rule::where_clause {
            where_clause = Some(parse_where_clause(p)?);
        }
    }

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

    Ok(SqlStmt::Search(crate::sql::ast::SearchStmt {
        table,
        query,
    }))
}

pub fn parse_prepare(pair: pest::iterators::Pair<Rule>) -> SqlResult<SqlStmt> {
    let mut inner = pair.into_inner();
    // Skip KW_PREPARE
    let _ = inner.next();

    let name = inner
        .next()
        .map(|p| p.as_str().trim().to_string())
        .ok_or_else(|| SqlError::Parse("Missing statement name in PREPARE".to_string()))?;

    // Skip KW_FROM
    let _ = inner.next();

    let sql = inner
        .next()
        .map(|p| p.as_str().trim_matches('\'').to_string())
        .ok_or_else(|| SqlError::Parse("Missing SQL string in PREPARE".to_string()))?;

    Ok(SqlStmt::Prepare(crate::sql::ast::PrepareStmt { name, sql }))
}

pub fn parse_execute(pair: pest::iterators::Pair<Rule>) -> SqlResult<SqlStmt> {
    let mut inner = pair.into_inner();
    // Skip KW_EXECUTE
    let _ = inner.next();

    let name = inner
        .next()
        .map(|p| p.as_str().trim().to_string())
        .ok_or_else(|| SqlError::Parse("Missing statement name in EXECUTE".to_string()))?;

    let mut params = Vec::new();
    if let Some(using_pair) = inner.find(|p| p.as_rule() == Rule::expression_list) {
        for p in using_pair.into_inner() {
            params.push(parse_any_expression(p)?);
        }
    }

    Ok(SqlStmt::Execute(crate::sql::ast::ExecuteStmt {
        name,
        params,
    }))
}

pub fn parse_deallocate(pair: pest::iterators::Pair<Rule>) -> SqlResult<SqlStmt> {
    let mut inner = pair.into_inner();
    // Find the identifier (the name of the statement)
    let name = inner
        .find(|p| p.as_rule() == Rule::identifier)
        .map(|p| p.as_str().trim().to_string())
        .ok_or_else(|| SqlError::Parse("Missing statement name in DEALLOCATE".to_string()))?;

    Ok(SqlStmt::Deallocate(name))
}
