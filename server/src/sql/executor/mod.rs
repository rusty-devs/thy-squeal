pub mod aggregate;
pub mod ddl;
pub mod dml;
pub mod dump;
pub mod explain;
pub mod search;
pub mod select;
#[cfg(test)]
mod tests;

use super::ast::SqlStmt;
use super::error::{SqlError, SqlResult};
use super::eval::Evaluator;
use super::parser::parse;
use crate::storage::{Database, DatabaseState, Row, Table, Value, WalRecord};
use dashmap::DashMap;
use futures::future::BoxFuture;
use futures::FutureExt;

#[derive(Debug)]
pub struct QueryResult {
    pub columns: Vec<String>,
    pub rows: Vec<Vec<Value>>,
    pub rows_affected: u64,
    pub transaction_id: Option<String>,
}

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

    pub fn exec_stmt<'a>(&'a self, stmt: SqlStmt, params: Vec<Value>, transaction_id: Option<String>) -> BoxFuture<'a, SqlResult<QueryResult>> {
        async move {
            let mut res = match stmt {
                SqlStmt::Begin => self.exec_begin().await?,
                SqlStmt::Commit => self.exec_commit(transaction_id.as_deref()).await?,
                SqlStmt::Rollback => self.exec_rollback(transaction_id.as_deref()).await?,
                SqlStmt::CreateTable(ct) => self.exec_create_table(ct, transaction_id.as_deref()).await?,
                SqlStmt::DropTable(dt) => self.exec_drop_table(dt, transaction_id.as_deref()).await?,
                SqlStmt::CreateIndex(ci) => self.exec_create_index(ci, transaction_id.as_deref()).await?,
                SqlStmt::Select(s) => {
                    if let Some(id) = transaction_id.as_deref() {
                        let state = self
                            .transactions
                            .get(id)
                            .ok_or_else(|| SqlError::Runtime("Transaction not found".to_string()))?;
                        self.exec_select_recursive(s, &[], &params, &state, Some(id)).await?
                    } else {
                        let db = self.db.read().await;
                        self.exec_select_recursive(s, &[], &params, db.state(), None).await?
                    }
                }
                SqlStmt::Explain(s) => {
                    if let Some(id) = transaction_id.as_deref() {
                        let state = self
                            .transactions
                            .get(id)
                            .ok_or_else(|| SqlError::Runtime("Transaction not found".to_string()))?;
                        self.exec_explain(s, &state, Some(id)).await?
                    } else {
                        let db = self.db.read().await;
                        self.exec_explain(s, db.state(), None).await?
                    }
                }
                SqlStmt::Search(s) => {
                    if let Some(id) = transaction_id.as_deref() {
                        let state = self
                            .transactions
                            .get(id)
                            .ok_or_else(|| SqlError::Runtime("Transaction not found".to_string()))?;
                        self.exec_search(s, &state, Some(id)).await?
                    } else {
                        let db = self.db.read().await;
                        self.exec_search(s, db.state(), None).await?
                    }
                }
                SqlStmt::Insert(i) => self.exec_insert(i, &params, transaction_id.as_deref()).await?,
                SqlStmt::Update(u) => self.exec_update(u, &params, transaction_id.as_deref()).await?,
                SqlStmt::Delete(d) => self.exec_delete(d, &params, transaction_id.as_deref()).await?,
                SqlStmt::Prepare(p) => self.exec_prepare(p).await?,
                SqlStmt::Execute(e) => self.exec_execute(e, params, transaction_id.clone()).await?,
                SqlStmt::Deallocate(name) => self.exec_deallocate(&name).await?,
            };

            if res.transaction_id.is_none() {
                res.transaction_id = transaction_id;
            }

            Ok(res)
        }.boxed()
    }

    async fn exec_prepare(&self, stmt: crate::sql::ast::PrepareStmt) -> SqlResult<QueryResult> {
        let inner_stmt = parse(&stmt.sql)?;
        self.prepared_statements.insert(stmt.name, inner_stmt);
        Ok(QueryResult {
            columns: vec![],
            rows: vec![],
            rows_affected: 0,
            transaction_id: None,
        })
    }

    async fn exec_execute(&self, stmt: crate::sql::ast::ExecuteStmt, params: Vec<Value>, transaction_id: Option<String>) -> SqlResult<QueryResult> {
        let prepared = self.prepared_statements.get(&stmt.name).ok_or_else(|| SqlError::Runtime(format!("Prepared statement '{}' not found", stmt.name)))?;
        let inner_stmt = prepared.value().clone();
        drop(prepared); // Release lock before recursive call

        let mut exec_params = Vec::new();
        if !stmt.params.is_empty() {
            // If EXECUTE ... USING ... is used, these take precedence
            for p in &stmt.params {
                // We need an Evaluator to evaluate expressions in USING. 
                // But USING usually only allows literals or simple expressions.
                // Let's use a dummy evaluator or self.
                
                // For now, let's assume we are not in a table context.
                let db = self.db.read().await;
                let state = if let Some(id) = &transaction_id {
                    self.transactions.get(id).ok_or_else(|| SqlError::Runtime("Transaction not found".to_string()))?.clone()
                } else {
                    db.state().clone()
                };

                let val = super::eval::evaluate_expression_joined(self, p, &[], &params, &[], &state)?;
                exec_params.push(val);
            }
        } else {
            // Otherwise use the params passed to execute() (e.g. via protocol)
            exec_params = params;
        }

        self.exec_stmt(inner_stmt, exec_params, transaction_id).await
    }

    async fn exec_deallocate(&self, name: &str) -> SqlResult<QueryResult> {
        self.prepared_statements.remove(name);
        Ok(QueryResult {
            columns: vec![],
            rows: vec![],
            rows_affected: 0,
            transaction_id: None,
        })
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

impl Executor {
    pub(crate) async fn mutate_state<F, R>(&self, tx_id: Option<&str>, f: F) -> SqlResult<R>
    where
        F: FnOnce(&mut DatabaseState) -> SqlResult<R>,
    {
        if let Some(id) = tx_id {
            let mut state_ref = self
                .transactions
                .get_mut(id)
                .ok_or_else(|| SqlError::Runtime("Transaction not found".to_string()))?;
            f(state_ref.value_mut())
        } else {
            let mut db = self.db.write().await;
            let res = f(db.state_mut())?;
            db.save().map_err(|e| SqlError::Storage(e.to_string()))?;
            Ok(res)
        }
    }

    async fn exec_begin(&self) -> SqlResult<QueryResult> {
        let db = self.db.read().await;
        let tx_id = uuid::Uuid::new_v4().to_string();

        // Log BEGIN to WAL
        db.log_operation(&WalRecord::Begin {
            tx_id: tx_id.clone(),
        })
        .map_err(|e| SqlError::Storage(e.to_string()))?;

        let state = db.state().clone();
        self.transactions.insert(tx_id.clone(), state);

        Ok(QueryResult {
            columns: vec![],
            rows: vec![],
            rows_affected: 0,
            transaction_id: Some(tx_id),
        })
    }

    async fn exec_commit(&self, tx_id: Option<&str>) -> SqlResult<QueryResult> {
        let tx_id = tx_id.ok_or_else(|| SqlError::Runtime("No active transaction".to_string()))?;
        let state = self
            .transactions
            .remove(tx_id)
            .ok_or_else(|| SqlError::Runtime("Transaction not found".to_string()))?
            .1;

        let mut db = self.db.write().await;

        // Log COMMIT to WAL
        db.log_operation(&WalRecord::Commit {
            tx_id: tx_id.to_string(),
        })
        .map_err(|e| SqlError::Storage(e.to_string()))?;

        db.set_state(state)
            .map_err(|e| SqlError::Storage(e.to_string()))?;

        Ok(QueryResult {
            columns: vec![],
            rows: vec![],
            rows_affected: 0,
            transaction_id: None,
        })
    }

    async fn exec_rollback(&self, tx_id: Option<&str>) -> SqlResult<QueryResult> {
        let tx_id = tx_id.ok_or_else(|| SqlError::Runtime("No active transaction".to_string()))?;
        self.transactions.remove(tx_id);

        let db = self.db.read().await;
        // Log ROLLBACK to WAL
        db.log_operation(&WalRecord::Rollback {
            tx_id: tx_id.to_string(),
        })
        .map_err(|e| SqlError::Storage(e.to_string()))?;

        Ok(QueryResult {
            columns: vec![],
            rows: vec![],
            rows_affected: 0,
            transaction_id: None,
        })
    }
}
