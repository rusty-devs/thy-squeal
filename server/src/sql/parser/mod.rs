pub mod ddl;
pub mod dml;
pub mod expr;
pub mod select;
pub mod utils;

use crate::sql::ast::SqlStmt;
use crate::sql::error::{SqlError, SqlResult};
use pest::Parser;
use pest_derive::Parser;

#[derive(Parser)]
#[grammar = "sql/sql.pest"]
pub struct SqlParser;

pub fn parse(sql: &str) -> SqlResult<SqlStmt> {
    let mut pairs = SqlParser::parse(Rule::statement, sql).map_err(|e| {
        SqlError::Parse(format!("Pest error: {}", e))
    })?;

    let pair = pairs
        .next()
        .ok_or_else(|| SqlError::Parse("No statement found".to_string()))?;
    let inner = pair
        .into_inner()
        .next()
        .ok_or_else(|| SqlError::Parse("Empty statement".to_string()))?;

    match inner.as_rule() {
        Rule::begin_stmt => Ok(SqlStmt::Begin),
        Rule::commit_stmt => Ok(SqlStmt::Commit),
        Rule::rollback_stmt => Ok(SqlStmt::Rollback),
        Rule::create_table_stmt => ddl::parse_create_table(inner),
        Rule::drop_table_stmt => ddl::parse_drop_table(inner),
        Rule::create_index_stmt => ddl::parse_create_index(inner),
        Rule::select_stmt => select::parse_select(inner),
        Rule::insert_stmt => dml::parse_insert(inner),
        Rule::update_stmt => dml::parse_update(inner),
        Rule::delete_stmt => dml::parse_delete(inner),
        Rule::explain_stmt => {
            let mut inner_pairs = inner.into_inner();
            let _explain_kw = inner_pairs.next().unwrap();
            let select_pair = inner_pairs.next().ok_or_else(|| SqlError::Parse("Missing SELECT after EXPLAIN".to_string()))?;
            let select_stmt = select::parse_select(select_pair)?;
            if let SqlStmt::Select(s) = select_stmt {
                Ok(SqlStmt::Explain(s))
            } else {
                Err(SqlError::Parse("EXPLAIN must be followed by SELECT".to_string()))
            }
        }
        Rule::search_stmt => {
            let mut inner_pairs = inner.into_inner();
            let _search_kw = inner_pairs.next().unwrap();
            let table_pair = inner_pairs.next().ok_or_else(|| SqlError::Parse("Missing table name in SEARCH".to_string()))?;
            let table = utils::expect_identifier(table_pair.into_inner().next(), "table name")?;
            let query_literal = expr::parse_literal(inner_pairs.next().ok_or_else(|| SqlError::Parse("Missing query in SEARCH".to_string()))?)?;
            let query = query_literal
                .as_text()
                .ok_or_else(|| SqlError::Parse("SEARCH query must be a string".to_string()))?
                .to_string();
            Ok(SqlStmt::Search(crate::sql::ast::SearchStmt { table, query }))
        }
        _ => Err(SqlError::Parse(format!(
            "Unsupported statement rule: {:?}",
            inner.as_rule()
        ))),
    }
}
