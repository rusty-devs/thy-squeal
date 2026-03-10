use super::super::ast::SqlStmt;
use super::super::error::{SqlError, SqlResult};
use super::super::parser::parse;

use super::{Executor, QueryResult};
use crate::storage::{Privilege, Value};
use futures::future::{BoxFuture, FutureExt};

impl Executor {
    pub fn exec_stmt<'a>(
        &'a self,
        stmt: SqlStmt,
        params: Vec<Value>,
        transaction_id: Option<String>,
        username: Option<String>,
    ) -> BoxFuture<'a, SqlResult<QueryResult>> {
        async move {
            let user = username.unwrap_or_else(|| "root".to_string());

            let mut res = match stmt {
                SqlStmt::Begin => self.exec_begin().await?,
                SqlStmt::Commit => self.exec_commit(transaction_id.as_deref()).await?,
                SqlStmt::Rollback => self.exec_rollback(transaction_id.as_deref()).await?,
                SqlStmt::CreateTable(ct) => {
                    {
                        let db = self.db.read().await;
                        self.check_privilege(&user, None, Privilege::Create, db.state())?;
                    }
                    self.exec_create_table(ct, transaction_id.as_deref())
                        .await?
                }
                SqlStmt::CreateMaterializedView(mv) => {
                    {
                        let db = self.db.read().await;
                        self.check_privilege(&user, None, Privilege::Create, db.state())?;
                    }
                    self.exec_create_materialized_view(mv, transaction_id.as_deref())
                        .await?
                }
                SqlStmt::AlterTable(at) => {
                    {
                        let db = self.db.read().await;
                        self.check_privilege(&user, Some(&at.table), Privilege::Create, db.state())?;
                    }
                    self.exec_alter_table(at, transaction_id.as_deref()).await?
                }
                SqlStmt::DropTable(dt) => {
                    {
                        let db = self.db.read().await;
                        self.check_privilege(&user, Some(&dt.name), Privilege::Drop, db.state())?;
                    }
                    self.exec_drop_table(dt, transaction_id.as_deref()).await?
                }
                SqlStmt::CreateUser(cu) => {
                    {
                        let db = self.db.read().await;
                        self.check_privilege(&user, None, Privilege::Grant, db.state())?;
                    }
                    self.exec_create_user(cu, transaction_id.as_deref()).await?
                }
                SqlStmt::DropUser(du) => {
                    {
                        let db = self.db.read().await;
                        self.check_privilege(&user, None, Privilege::Grant, db.state())?;
                    }
                    self.exec_drop_user(du, transaction_id.as_deref()).await?
                }
                SqlStmt::Grant(g) => {
                    {
                        let db = self.db.read().await;
                        self.check_privilege(&user, None, Privilege::Grant, db.state())?;
                    }
                    self.exec_grant(g, transaction_id.as_deref()).await?
                }
                SqlStmt::Revoke(r) => {
                    {
                        let db = self.db.read().await;
                        self.check_privilege(&user, None, Privilege::Grant, db.state())?;
                    }
                    self.exec_revoke(r, transaction_id.as_deref()).await?
                }
                SqlStmt::CreateIndex(ci) => {
                    {
                        let db = self.db.read().await;
                        self.check_privilege(&user, Some(&ci.table), Privilege::Create, db.state())?;
                    }
                    self.exec_create_index(ci, transaction_id.as_deref())
                        .await?
                }
                SqlStmt::Select(s) => {
                    let table = s.table.clone();
                    if let Some(id) = transaction_id.as_deref() {
                        let state = self
                            .transactions
                            .get(id)
                            .ok_or_else(|| SqlError::Runtime("Transaction not found".to_string()))?;
                        if !table.is_empty() && !table.starts_with("information_schema.") {
                            self.check_privilege(&user, Some(&table), Privilege::Select, &state)?;
                        }
                        self.exec_select_recursive(s, &[], &params, &state, Some(id))
                            .await?
                    } else {
                        let db = self.db.read().await;
                        if !table.is_empty() && !table.starts_with("information_schema.") {
                            self.check_privilege(&user, Some(&table), Privilege::Select, db.state())?;
                        }
                        self.exec_select_recursive(s, &[], &params, db.state(), None)
                            .await?
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
                        self.check_privilege(&user, Some(&s.table), Privilege::Select, &state)?;
                        self.exec_search(s, &state, Some(id)).await?
                    } else {
                        let db = self.db.read().await;
                        self.check_privilege(&user, Some(&s.table), Privilege::Select, db.state())?;
                        self.exec_search(s, db.state(), None).await?
                    }
                }
                SqlStmt::Insert(i) => {
                    {
                        let db = self.db.read().await;
                        self.check_privilege(&user, Some(&i.table), Privilege::Insert, db.state())?;
                    }
                    self.exec_insert(i, &params, transaction_id.as_deref())
                        .await?
                }
                SqlStmt::Update(u) => {
                    {
                        let db = self.db.read().await;
                        self.check_privilege(&user, Some(&u.table), Privilege::Update, db.state())?;
                    }
                    self.exec_update(u, &params, transaction_id.as_deref())
                        .await?
                }
                SqlStmt::Delete(d) => {
                    {
                        let db = self.db.read().await;
                        self.check_privilege(&user, Some(&d.table), Privilege::Delete, db.state())?;
                    }
                    self.exec_delete(d, &params, transaction_id.as_deref())
                        .await?
                }
                SqlStmt::Prepare(p) => self.exec_prepare(p).await?,
                SqlStmt::Execute(e) => {
                    self.exec_execute(e, params, transaction_id.clone(), Some(user))
                        .await?
                }
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
        username: Option<String>,
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

        self.exec_stmt(inner_stmt, exec_params, transaction_id, username)
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
