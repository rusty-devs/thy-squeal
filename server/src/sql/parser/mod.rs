pub mod ddl;
pub mod dml;
pub mod expr;
pub mod select;
pub mod utils;

pub use expr::parse_any_expression;

use super::ast::SqlStmt;
use super::error::{SqlError, SqlResult};
use pest::Parser;
use pest_derive::Parser;

#[derive(Parser)]
#[grammar = "sql/sql.pest"]
pub struct SqlParser;

pub fn parse(sql: &str) -> SqlResult<SqlStmt> {
    let pairs = SqlParser::parse(Rule::statement, sql)
        .map_err(|e| SqlError::Parse(format!("Pest error: {}", e)))?;

    for pair in pairs {
        if pair.as_rule() == Rule::statement {
            let inner = pair.into_inner().next().unwrap();
            let mut stmt = match inner.as_rule() {
                Rule::select_stmt => select::parse_select(inner),
                Rule::insert_stmt => dml::parse_insert(inner),
                Rule::update_stmt => dml::parse_update(inner),
                Rule::delete_stmt => dml::parse_delete(inner),
                Rule::create_table_stmt => ddl::parse_create_table(inner),
                Rule::alter_table_stmt => ddl::parse_alter_table(inner),
                Rule::drop_table_stmt => ddl::parse_drop_table(inner),
                Rule::create_index_stmt => ddl::parse_create_index(inner),
                Rule::explain_stmt => {
                    let inner_select = inner
                        .into_inner()
                        .find(|p| p.as_rule() == Rule::select_stmt_inner)
                        .ok_or_else(|| SqlError::Parse("Missing SELECT in EXPLAIN".to_string()))?;

                    let select_stmt = select::parse_select(inner_select)?;
                    if let SqlStmt::Select(s) = select_stmt {
                        Ok(SqlStmt::Explain(s))
                    } else {
                        Err(SqlError::Parse("EXPLAIN only supports SELECT".to_string()))
                    }
                }
                Rule::search_stmt => dml::parse_search(inner),
                Rule::prepare_stmt => dml::parse_prepare(inner),
                Rule::execute_stmt => dml::parse_execute(inner),
                Rule::deallocate_stmt => dml::parse_deallocate(inner),
                Rule::begin_stmt => Ok(SqlStmt::Begin),
                Rule::commit_stmt => Ok(SqlStmt::Commit),
                Rule::rollback_stmt => Ok(SqlStmt::Rollback),
                _ => {
                    return Err(SqlError::Parse(format!(
                        "Unsupported statement: {:?}",
                        inner.as_rule()
                    )));
                }
            }?;

            stmt.resolve_placeholders();
            return Ok(stmt);
        }
    }

    Err(SqlError::Parse("No statement found".to_string()))
}
