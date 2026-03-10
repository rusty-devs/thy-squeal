use super::super::ast::SqlStmt;
use super::super::error::{SqlError, SqlResult};
use super::super::parser::parse;

use super::{ExecutionContext, Executor, QueryResult, SelectQueryPlan, Session};
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
            let session = Session::new(username, transaction_id);
            let ctx = ExecutionContext::new(params, session);

            let mut res = match stmt {
                // Transaction control
                SqlStmt::Begin | SqlStmt::Commit | SqlStmt::Rollback => {
                    self.dispatch_tx(stmt, &ctx).await?
                }

                // DDL (Data Definition)
                SqlStmt::CreateTable(_)
                | SqlStmt::DropTable(_)
                | SqlStmt::AlterTable(_)
                | SqlStmt::CreateIndex(_)
                | SqlStmt::CreateMaterializedView(_) => self.dispatch_ddl(stmt, &ctx).await?,

                // DML (Data Manipulation)
                SqlStmt::Insert(_) | SqlStmt::Update(_) | SqlStmt::Delete(_) => {
                    self.dispatch_dml(stmt, &ctx).await?
                }

                // User management
                SqlStmt::CreateUser(_)
                | SqlStmt::DropUser(_)
                | SqlStmt::Grant(_)
                | SqlStmt::Revoke(_) => self.dispatch_user(stmt, &ctx).await?,

                // Queries
                SqlStmt::Select(_) | SqlStmt::Search(_) | SqlStmt::Explain(_) => {
                    self.dispatch_query(stmt, &ctx).await?
                }

                // Prepared statements
                SqlStmt::Prepare(p) => self.exec_prepare(p).await?,
                SqlStmt::Execute(e) => {
                    self.exec_execute(
                        e,
                        ctx.params.clone(),
                        ctx.session.transaction_id.clone(),
                        Some(ctx.session.username.clone()),
                    )
                    .await?
                }
                SqlStmt::Deallocate(name) => self.exec_deallocate(&name).await?,
            };

            if res.transaction_id.is_none() {
                res.transaction_id = ctx.session.transaction_id.clone();
            }

            Ok(res)
        }
        .boxed()
    }

    async fn dispatch_tx(&self, stmt: SqlStmt, ctx: &ExecutionContext) -> SqlResult<QueryResult> {
        match stmt {
            SqlStmt::Begin => self.exec_begin().await,
            SqlStmt::Commit => {
                self.exec_commit(ctx.session.transaction_id.as_deref())
                    .await
            }
            SqlStmt::Rollback => {
                self.exec_rollback(ctx.session.transaction_id.as_deref())
                    .await
            }
            _ => unreachable!(),
        }
    }

    async fn dispatch_ddl(&self, stmt: SqlStmt, ctx: &ExecutionContext) -> SqlResult<QueryResult> {
        match stmt {
            SqlStmt::CreateTable(ct) => {
                {
                    let db = self.db.read().await;
                    self.check_privilege(
                        &ctx.session.username,
                        None,
                        Privilege::Create,
                        db.state(),
                    )?;
                }
                self.exec_create_table(ct, ctx.session.transaction_id.as_deref())
                    .await
            }
            SqlStmt::CreateMaterializedView(mv) => {
                {
                    let db = self.db.read().await;
                    self.check_privilege(
                        &ctx.session.username,
                        None,
                        Privilege::Create,
                        db.state(),
                    )?;
                }
                self.exec_create_materialized_view(mv, ctx.session.transaction_id.as_deref())
                    .await
            }
            SqlStmt::AlterTable(at) => {
                {
                    let db = self.db.read().await;
                    self.check_privilege(
                        &ctx.session.username,
                        Some(&at.table),
                        Privilege::Create,
                        db.state(),
                    )?;
                }
                self.exec_alter_table(at, ctx.session.transaction_id.as_deref())
                    .await
            }
            SqlStmt::DropTable(dt) => {
                {
                    let db = self.db.read().await;
                    self.check_privilege(
                        &ctx.session.username,
                        Some(&dt.name),
                        Privilege::Drop,
                        db.state(),
                    )?;
                }
                self.exec_drop_table(dt, ctx.session.transaction_id.as_deref())
                    .await
            }
            SqlStmt::CreateIndex(ci) => {
                {
                    let db = self.db.read().await;
                    self.check_privilege(
                        &ctx.session.username,
                        Some(&ci.table),
                        Privilege::Create,
                        db.state(),
                    )?;
                }
                self.exec_create_index(ci, ctx.session.transaction_id.as_deref())
                    .await
            }
            _ => unreachable!(),
        }
    }

    async fn dispatch_dml(&self, stmt: SqlStmt, ctx: &ExecutionContext) -> SqlResult<QueryResult> {
        match stmt {
            SqlStmt::Insert(i) => {
                {
                    let db = self.db.read().await;
                    self.check_privilege(
                        &ctx.session.username,
                        Some(&i.table),
                        Privilege::Insert,
                        db.state(),
                    )?;
                }
                self.exec_insert(i, &ctx.params, ctx.session.transaction_id.as_deref())
                    .await
            }
            SqlStmt::Update(u) => {
                {
                    let db = self.db.read().await;
                    self.check_privilege(
                        &ctx.session.username,
                        Some(&u.table),
                        Privilege::Update,
                        db.state(),
                    )?;
                }
                self.exec_update(u, &ctx.params, ctx.session.transaction_id.as_deref())
                    .await
            }
            SqlStmt::Delete(d) => {
                {
                    let db = self.db.read().await;
                    self.check_privilege(
                        &ctx.session.username,
                        Some(&d.table),
                        Privilege::Delete,
                        db.state(),
                    )?;
                }
                self.exec_delete(d, &ctx.params, ctx.session.transaction_id.as_deref())
                    .await
            }
            _ => unreachable!(),
        }
    }

    async fn dispatch_user(&self, stmt: SqlStmt, ctx: &ExecutionContext) -> SqlResult<QueryResult> {
        {
            let db = self.db.read().await;
            self.check_privilege(&ctx.session.username, None, Privilege::Grant, db.state())?;
        }
        match stmt {
            SqlStmt::CreateUser(cu) => {
                self.exec_create_user(cu, ctx.session.transaction_id.as_deref())
                    .await
            }
            SqlStmt::DropUser(du) => {
                self.exec_drop_user(du, ctx.session.transaction_id.as_deref())
                    .await
            }
            SqlStmt::Grant(g) => {
                self.exec_grant(g, ctx.session.transaction_id.as_deref())
                    .await
            }
            SqlStmt::Revoke(r) => {
                self.exec_revoke(r, ctx.session.transaction_id.as_deref())
                    .await
            }
            _ => unreachable!(),
        }
    }

    async fn dispatch_query(
        &self,
        stmt: SqlStmt,
        ctx: &ExecutionContext,
    ) -> SqlResult<QueryResult> {
        match stmt {
            SqlStmt::Select(s) => {
                let table = s.table.clone();
                if let Some(id) = &ctx.session.transaction_id {
                    let state = self
                        .transactions
                        .get(id)
                        .ok_or_else(|| SqlError::Runtime("Transaction not found".to_string()))?;
                    if !table.is_empty() && !table.starts_with("information_schema.") {
                        self.check_privilege(
                            &ctx.session.username,
                            Some(&table),
                            Privilege::Select,
                            &state,
                        )?;
                    }

                    let plan = SelectQueryPlan::new(s, &state, ctx.session.clone())
                        .with_params(&ctx.params);

                    self.exec_select_recursive(plan).await
                } else {
                    let db = self.db.read().await;
                    if !table.is_empty() && !table.starts_with("information_schema.") {
                        self.check_privilege(
                            &ctx.session.username,
                            Some(&table),
                            Privilege::Select,
                            db.state(),
                        )?;
                    }

                    let plan = SelectQueryPlan::new(s, db.state(), ctx.session.clone())
                        .with_params(&ctx.params);

                    self.exec_select_recursive(plan).await
                }
            }
            SqlStmt::Search(s) => {
                if let Some(id) = &ctx.session.transaction_id {
                    let state = self
                        .transactions
                        .get(id)
                        .ok_or_else(|| SqlError::Runtime("Transaction not found".to_string()))?;
                    self.check_privilege(
                        &ctx.session.username,
                        Some(&s.table),
                        Privilege::Select,
                        &state,
                    )?;
                    self.exec_search(s, &state, Some(id)).await
                } else {
                    let db = self.db.read().await;
                    self.check_privilege(
                        &ctx.session.username,
                        Some(&s.table),
                        Privilege::Select,
                        db.state(),
                    )?;
                    self.exec_search(s, db.state(), None).await
                }
            }
            SqlStmt::Explain(s) => {
                if let Some(id) = &ctx.session.transaction_id {
                    let state = self
                        .transactions
                        .get(id)
                        .ok_or_else(|| SqlError::Runtime("Transaction not found".to_string()))?;
                    self.exec_explain(s, &state, Some(id)).await
                } else {
                    let db = self.db.read().await;
                    self.exec_explain(s, db.state(), None).await
                }
            }
            _ => unreachable!(),
        }
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
