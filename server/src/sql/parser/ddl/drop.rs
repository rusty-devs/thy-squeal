use super::super::super::ast::{DropTableStmt, SqlStmt};
use super::super::super::error::{SqlError, SqlResult};
use crate::sql::parser::Rule;

pub fn parse_drop_table(pair: pest::iterators::Pair<Rule>) -> SqlResult<SqlStmt> {
    let mut inner = pair.into_inner();
    let table_pair = inner
        .find(|p| p.as_rule() == Rule::table_name)
        .ok_or_else(|| SqlError::Parse("Missing table name in DROP TABLE".to_string()))?;
    
    let column_ref_rule = table_pair.into_inner().next().unwrap();
    let name = column_ref_rule
        .into_inner()
        .filter(|pi| pi.as_rule() == Rule::path_identifier)
        .map(|pi| pi.as_str().trim().to_string())
        .collect::<Vec<_>>()
        .join(".");
    
    Ok(SqlStmt::DropTable(DropTableStmt { name }))
}
