pub mod ddl;
pub mod dml;
pub mod expr;
pub mod select;
pub mod utils;

use pest::Parser;
use pest_derive::Parser;

use super::ast::SqlStmt;
use super::error::{SqlError, SqlResult};

#[derive(Parser)]
#[grammar = "sql/sql.pest"]
pub struct SqlParser;

pub fn parse(input: &str) -> SqlResult<SqlStmt> {
    let mut pairs = SqlParser::parse(Rule::statement, input.trim())
        .map_err(|e| SqlError::Parse(e.to_string()))?;

    let stmt_pair = pairs
        .next()
        .ok_or_else(|| SqlError::Parse("Empty SQL statement".to_string()))?;

    let mut inner = stmt_pair.into_inner();
    let kind_pair = inner
        .next()
        .ok_or_else(|| SqlError::Parse("Unable to determine statement type".to_string()))?;

    match kind_pair.as_rule() {
        Rule::select_stmt => select::parse_select(kind_pair),
        Rule::insert_stmt => dml::parse_insert(kind_pair),
        Rule::create_table_stmt => ddl::parse_create_table(kind_pair),
        Rule::drop_table_stmt => ddl::parse_drop_table(kind_pair),
        Rule::create_index_stmt => ddl::parse_create_index(kind_pair),
        Rule::update_stmt => dml::parse_update(kind_pair),
        Rule::delete_stmt => dml::parse_delete(kind_pair),
        _ => Err(SqlError::Parse("Unsupported SQL statement".to_string())),
    }
}
