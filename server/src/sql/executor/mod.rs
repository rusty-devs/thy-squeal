pub mod ddl;
pub mod dml;
pub mod select;
#[cfg(test)]
mod tests;

use super::ast::SqlStmt;
use super::error::SqlResult;
use super::parser::parse;
use crate::storage::{Database, Row, Table, Value};

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
            SqlStmt::CreateIndex(ci) => self.exec_create_index(ci).await,
            SqlStmt::Select(s) => self.exec_select_recursive(s, &[]).await,
            SqlStmt::Explain(s) => self.exec_explain(s).await,
            SqlStmt::Insert(i) => self.exec_insert(i).await,
            SqlStmt::Update(u) => self.exec_update(u).await,
            SqlStmt::Delete(d) => self.exec_delete(d).await,
        }
    }

    pub(crate) async fn exec_select_internal(
        &self,
        stmt: super::ast::SelectStmt,
        outer_contexts: &[(&Table, &Row)],
    ) -> SqlResult<QueryResult> {
        self.exec_select_recursive(stmt, outer_contexts).await
    }
}
