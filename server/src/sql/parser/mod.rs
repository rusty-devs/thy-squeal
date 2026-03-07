pub mod ddl;
pub mod dml;
pub mod expr;
pub mod select;
pub mod utils;

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
        match pair.as_rule() {
            Rule::statement => {
                let inner = pair.into_inner().next().unwrap();
                match inner.as_rule() {
                    Rule::select_stmt => return select::parse_select(inner),
                    Rule::insert_stmt => return dml::parse_insert(inner),
                    Rule::update_stmt => return dml::parse_update(inner),
                    Rule::delete_stmt => return dml::parse_delete(inner),
                    Rule::create_table_stmt => return ddl::parse_create_table(inner),
                    Rule::drop_table_stmt => return ddl::parse_drop_table(inner),
                    Rule::create_index_stmt => return ddl::parse_create_index(inner),
                    Rule::explain_stmt => {
                        let inner_select = inner
                            .into_inner()
                            .find(|p| p.as_rule() == Rule::select_stmt_inner)
                            .ok_or_else(|| SqlError::Parse("Missing SELECT in EXPLAIN".to_string()))?;
                        
                        // We need to wrap it back or adjust parse_select
                        // select_stmt rule just contains select_stmt_inner
                        let select_stmt = select::parse_select(inner_select)?;
                        if let SqlStmt::Select(s) = select_stmt {
                            return Ok(SqlStmt::Explain(s));
                        }
                        return Err(SqlError::Parse("EXPLAIN only supports SELECT".to_string()));
                    }
                    Rule::search_stmt => return dml::parse_search(inner),
                    Rule::begin_stmt => return Ok(SqlStmt::Begin),
                    Rule::commit_stmt => return Ok(SqlStmt::Commit),
                    Rule::rollback_stmt => return Ok(SqlStmt::Rollback),
                    _ => {
                        return Err(SqlError::Parse(format!(
                            "Unsupported statement: {:?}",
                            inner.as_rule()
                        )));
                    }
                }
            }
            Rule::EOI => {}
            _ => {
                return Err(SqlError::Parse(format!(
                    "Unexpected top-level rule: {:?}",
                    pair.as_rule()
                )));
            }
        }
    }

    Err(SqlError::Parse("No statement found".to_string()))
}
