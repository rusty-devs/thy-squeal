pub mod aggregate;
pub mod ddl;
pub mod dispatch;
pub mod dml;
pub mod dump;
pub mod explain;
pub mod result;
pub mod search;
pub mod select;
#[cfg(test)]
mod tests;
pub mod tx;
pub mod user;

use super::ast::SqlStmt;
use super::error::{SqlError, SqlResult};
use crate::storage::{Database, DatabaseState, Privilege, Row, Table, Value};
use dashmap::DashMap;
use futures::future::BoxFuture;

pub use result::QueryResult;

/// A user session containing authentication and transaction state.
#[derive(Clone, Debug)]
pub struct Session {
    pub username: String,
    pub transaction_id: Option<String>,
}

impl Session {
    pub fn new(username: Option<String>, transaction_id: Option<String>) -> Self {
        Self {
            username: username.unwrap_or_else(|| "root".to_string()),
            transaction_id,
        }
    }

    pub fn root() -> Self {
        Self::new(None, None)
    }
}

/// Context for statement execution
pub struct ExecutionContext {
    pub params: Vec<Value>,
    pub session: Session,
}

impl ExecutionContext {
    pub fn new(params: Vec<Value>, session: Session) -> Self {
        Self { params, session }
    }
}

/// A builder-style plan for executing a SELECT query.
/// Reduces argument count in internal executor functions.
pub struct SelectQueryPlan<'a> {
    pub stmt: super::ast::SelectStmt,
    pub outer_contexts: &'a [(&'a Table, Option<&'a str>, &'a Row)],
    pub params: &'a [Value],
    pub db_state: &'a DatabaseState,
    pub session: Session,
}

impl<'a> SelectQueryPlan<'a> {
    pub fn new(
        stmt: super::ast::SelectStmt,
        db_state: &'a DatabaseState,
        session: Session,
    ) -> Self {
        Self {
            stmt,
            outer_contexts: &[],
            params: &[],
            db_state,
            session,
        }
    }

    pub fn with_outer_contexts(
        mut self,
        contexts: &'a [(&'a Table, Option<&'a str>, &'a Row)],
    ) -> Self {
        self.outer_contexts = contexts;
        self
    }

    pub fn with_params(mut self, params: &'a [Value]) -> Self {
        self.params = params;
        self
    }
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

    pub async fn execute(
        &self,
        sql: &str,
        params: Vec<Value>,
        transaction_id: Option<String>,
        username: Option<String>,
    ) -> SqlResult<QueryResult> {
        let stmt = super::parser::parse(sql)?;
        self.exec_stmt(stmt, params, transaction_id, username).await
    }

    pub fn check_privilege(
        &self,
        username: &str,
        table: Option<&str>,
        privilege: Privilege,
        db_state: &DatabaseState,
    ) -> SqlResult<()> {
        let user = db_state
            .users
            .get(username)
            .ok_or_else(|| SqlError::Runtime(format!("User {} not found", username)))?;

        // root always has All
        if user.global_privileges.contains(&Privilege::All) {
            return Ok(());
        }

        if let Some(t) = table
            && let Some(privs) = user.table_privileges.get(t)
            && (privs.contains(&Privilege::All) || privs.contains(&privilege))
        {
            return Ok(());
        }

        if user.global_privileges.contains(&privilege) {
            return Ok(());
        }

        Err(SqlError::PermissionDenied(format!(
            "User {} does not have {:?} privilege{}",
            username,
            privilege,
            table
                .map(|t| format!(" on table {}", t))
                .unwrap_or_default()
        )))
    }

    pub fn refresh_materialized_views(&self, state: &mut DatabaseState) -> SqlResult<()> {
        let views = state.materialized_views.clone();
        for (name, query) in views {
            let plan = SelectQueryPlan::new(query, state, Session::root());
            let res = futures::executor::block_on(self.exec_select_recursive(plan))?;

            if let Some(table) = state.tables.get_mut(&name) {
                table.rows = res
                    .rows
                    .into_iter()
                    .enumerate()
                    .map(|(i, values)| Row {
                        id: format!("mv_{}_{}", name, i),
                        values,
                    })
                    .collect();
            }
        }
        Ok(())
    }
}

impl crate::sql::eval::Evaluator for Executor {
    fn exec_select_internal<'a>(
        &'a self,
        stmt: super::ast::SelectStmt,
        outer_contexts: &'a [(&'a Table, Option<&'a str>, &'a Row)],
        params: &'a [Value],
        db_state: &'a DatabaseState,
    ) -> BoxFuture<'a, SqlResult<QueryResult>> {
        let plan = SelectQueryPlan::new(stmt, db_state, Session::root())
            .with_outer_contexts(outer_contexts)
            .with_params(params);
        self.exec_select_recursive(plan)
    }
}
