use super::super::ast::{DeleteStmt, InsertStmt, SqlStmt, UpdateStmt};
use super::super::error::{SqlError, SqlResult};
use super::super::parser::Rule;
use super::expr::{parse_any_expression};

pub fn parse_insert(pair: pest::iterators::Pair<Rule>) -> SqlResult<SqlStmt> {
    let inner = pair.into_inner();

    let mut table = None;
    let mut columns = None;
    let mut values = Vec::new();

    for p in inner {
        match p.as_rule() {
            Rule::table_name => table = Some(p.as_str().trim().to_string()),
            Rule::column_list => {
                let mut cols = Vec::new();
                for col_pair in p.into_inner() {
                    if col_pair.as_rule() == Rule::column_expr {
                        let expr_pair = col_pair.into_inner().next().unwrap();
                        cols.push(expr_pair.as_str().trim().to_string());
                    }
                }
                columns = Some(cols);
            }
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

    Ok(SqlStmt::Insert(InsertStmt { table, columns, values }))
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
        let expr = parse_any_expression(
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

    Ok(SqlStmt::Execute(crate::sql::ast::ExecuteStmt { name, params }))
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

pub fn parse_value_list(
    pair: pest::iterators::Pair<Rule>,
) -> SqlResult<Vec<crate::sql::ast::Expression>> {
    let inner = pair.into_inner();
    let mut values = Vec::new();
    for p in inner {
        match p.as_rule() {
            Rule::literal 
            | Rule::string_literal 
            | Rule::number_literal 
            | Rule::boolean_literal 
            | Rule::KW_NULL 
            | Rule::placeholder => {
                values.push(parse_any_expression(p)?);
            }
            _ => {}
        }
    }
    Ok(values)
}
