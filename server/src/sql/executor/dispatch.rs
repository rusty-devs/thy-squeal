use super::super::ast::SqlStmt;
use super::super::error::{SqlError, SqlResult};
use super::super::parser::parse;
use super::{Executor, QueryResult};
use crate::storage::Value;
use futures::future::{BoxFuture, FutureExt};

impl Executor {
    pub fn exec_stmt<'a>(
        &'a self,
        stmt: SqlStmt,
        params: Vec<Value>,
        transaction_id: Option<String>,
    ) -> BoxFuture<'a, SqlResult<QueryResult>> {
        async move {
            let mut res = match stmt {
                SqlStmt::Begin => self.exec_begin().await?,
                SqlStmt::Commit => self.exec_commit(transaction_id.as_deref()).await?,
                SqlStmt::Rollback => self.exec_rollback(transaction_id.as_deref()).await?,
                SqlStmt::CreateTable(ct) => {
                    self.exec_create_table(ct, transaction_id.as_deref())
                        .await?
                }
                SqlStmt::AlterTable(at) => {
                    self.exec_alter_table(at, transaction_id.as_deref()).await?
                }
                SqlStmt::DropTable(dt) => {
                    self.exec_drop_table(dt, transaction_id.as_deref()).await?
                }
                SqlStmt::CreateIndex(ci) => {
                    self.exec_create_index(ci, transaction_id.as_deref())
                        .await?
                }
                SqlStmt::Select(s) => {
                    if let Some(id) = transaction_id.as_deref() {
                        let state = self.transactions.get(id).ok_or_else(|| {
                            SqlError::Runtime("Transaction not found".to_string())
                        })?;
                        self.exec_select_recursive(s, &[], &params, &state, Some(id))
                            .await?
                    } else {
                        let db = self.db.read().await;
                        self.exec_select_recursive(s, &[], &params, db.state(), None)
                            .await?
                    }
                }
                SqlStmt::Explain(s) => {
                    if let Some(id) = transaction_id.as_deref() {
                        let state = self.transactions.get(id).ok_or_else(|| {
                            SqlError::Runtime("Transaction not found".to_string())
                        })?;
                        self.exec_explain(s, &state, Some(id)).await?
                    } else {
                        let db = self.db.read().await;
                        self.exec_explain(s, db.state(), None).await?
                    }
                }
                SqlStmt::Search(s) => {
                    if let Some(id) = transaction_id.as_deref() {
                        let state = self.transactions.get(id).ok_or_else(|| {
                            SqlError::Runtime("Transaction not found".to_string())
                        })?;
                        self.exec_search(s, &state, Some(id)).await?
                    } else {
                        let db = self.db.read().await;
                        self.exec_search(s, db.state(), None).await?
                    }
                }
                SqlStmt::Insert(i) => {
                    self.exec_insert(i, &params, transaction_id.as_deref())
                        .await?
                }
                SqlStmt::Update(u) => {
                    self.exec_update(u, &params, transaction_id.as_deref())
                        .await?
                }
                SqlStmt::Delete(d) => {
                    self.exec_delete(d, &params, transaction_id.as_deref())
                        .await?
                }
                SqlStmt::Prepare(p) => self.exec_prepare(p).await?,
                SqlStmt::Execute(e) => self.exec_execute(e, params, transaction_id.clone()).await?,
                SqlStmt::Deallocate(name) => self.exec_deallocate(&name).await?,
            };

            if res.transaction_id.is_none() {
                res.transaction_id = transaction_id;
            }

            Ok(res)
        }
        .boxed()
    }

    pub(crate) async fn exec_prepare(
        &self,
        stmt: crate::sql::ast::PrepareStmt,
    ) -> SqlResult<QueryResult> {
        let inner_stmt = parse(&stmt.sql)?;
        self.prepared_statements.insert(stmt.name, inner_stmt);
        Ok(QueryResult {
            columns: vec![],
            rows: vec![],
            rows_affected: 0,
            transaction_id: None,
        })
    }

    pub(crate) async fn exec_execute(
        &self,
        stmt: crate::sql::ast::ExecuteStmt,
        params: Vec<Value>,
        transaction_id: Option<String>,
    ) -> SqlResult<QueryResult> {
        let prepared = self.prepared_statements.get(&stmt.name).ok_or_else(|| {
            SqlError::Runtime(format!("Prepared statement '{}' not found", stmt.name))
        })?;
        let inner_stmt = prepared.value().clone();
        drop(prepared); // Release lock before recursive call

        let mut exec_params = Vec::new();
        if !stmt.params.is_empty() {
            // If EXECUTE ... USING ... is used, these take precedence
            for p in &stmt.params {
                let db = self.db.read().await;
                let state = if let Some(id) = &transaction_id {
                    self.transactions
                        .get(id)
                        .ok_or_else(|| SqlError::Runtime("Transaction not found".to_string()))?
                        .clone()
                } else {
                    db.state().clone()
                };

                let val = crate::sql::eval::evaluate_expression_joined(
                    self,
                    p,
                    &[],
                    &params,
                    &[],
                    &state,
                )?;
                exec_params.push(val);
            }
        } else {
            // Otherwise use the params passed to execute() (e.g. via protocol)
            exec_params = params;
        }

        self.exec_stmt(inner_stmt, exec_params, transaction_id)
            .await
    }

    pub(crate) async fn exec_deallocate(&self, name: &str) -> SqlResult<QueryResult> {
        self.prepared_statements.remove(name);
        Ok(QueryResult {
            columns: vec![],
            rows: vec![],
            rows_affected: 0,
            transaction_id: None,
        })
    }
}
