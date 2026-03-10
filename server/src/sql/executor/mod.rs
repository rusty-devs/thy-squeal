pub mod aggregate;
pub mod ddl;
pub mod dml;
pub mod dispatch;
pub mod dump;
pub mod explain;
pub mod result;
pub mod search;
pub mod select;
pub mod tx;
#[cfg(test)]
mod tests;

use super::ast::SqlStmt;
use super::error::{SqlResult};
use super::eval::Evaluator;
use super::parser::parse;
use crate::storage::{Database, DatabaseState, Row, Table, Value};
use dashmap::DashMap;
use futures::future::BoxFuture;

pub use result::QueryResult;

pub struct Executor {
    pub(crate) db: tokio::sync::RwLock<Database>,
    pub(crate) transactions: DashMap<String, DatabaseState>,
    pub(crate) prepared_statements: DashMap<String, SqlStmt>, // name -> stmt
}

impl Executor {
    pub fn new(db: Database) -> Self {
        Self {
            db: tokio::sync::RwLock::new(db),
            transactions: DashMap::new(),
            prepared_statements: DashMap::new(),
        }
    }

    pub async fn execute(&self, sql: &str, params: Vec<Value>, transaction_id: Option<String>) -> SqlResult<QueryResult> {
        let stmt = parse(sql)?;
        self.exec_stmt(stmt, params, transaction_id).await
    }
}

impl Evaluator for Executor {
    fn exec_select_internal<'a>(
        &'a self,
        stmt: super::ast::SelectStmt,
        outer_contexts: &'a [(&'a Table, Option<&'a str>, &'a Row)],
        params: &'a [Value],
        db_state: &'a DatabaseState,
    ) -> BoxFuture<'a, SqlResult<QueryResult>> {
        self.exec_select_recursive(stmt, outer_contexts, params, db_state, None)
    }
}
