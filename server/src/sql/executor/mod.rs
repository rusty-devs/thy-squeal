pub mod select;
pub mod dml;
pub mod ddl;
#[cfg(test)]
mod tests;

use crate::storage::{Database, Value};
use super::error::{SqlResult};
use super::ast::SqlStmt;
use super::parser::parse;

#[derive(Debug)]
pub struct QueryResult {
    pub columns: Vec<String>,
    pub rows: Vec<Vec<Value>>,
    pub rows_affected: u64,
}

pub struct Executor {
    pub(crate) db: tokio::sync::RwLock<Database>,
}

impl Executor {
    pub fn new(db: Database) -> Self {
        Self {
            db: tokio::sync::RwLock::new(db),
        }
    }

    pub fn db(&self) -> &tokio::sync::RwLock<Database> {
        &self.db
    }

    pub async fn execute(&self, sql: &str) -> SqlResult<QueryResult> {
        let stmt = parse(sql)?;

        match stmt {
            SqlStmt::CreateTable(ct) => self.exec_create_table(ct).await,
            SqlStmt::DropTable(dt) => self.exec_drop_table(dt).await,
            SqlStmt::Select(s) => self.exec_select(s).await,
            SqlStmt::Insert(i) => self.exec_insert(i).await,
            SqlStmt::Update(u) => self.exec_update(u).await,
            SqlStmt::Delete(d) => self.exec_delete(d).await,
        }
    }
}
