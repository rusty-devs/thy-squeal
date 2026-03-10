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
use super::eval::Evaluator;
use super::parser::parse;
use crate::storage::{Database, DatabaseState, Privilege, Row, Table, Value};

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

    pub async fn execute(
        &self,
        sql: &str,
        params: Vec<Value>,
        transaction_id: Option<String>,
        username: Option<String>,
    ) -> SqlResult<QueryResult> {
        let stmt = parse(sql)?;
        self.exec_stmt(stmt, params, transaction_id, username).await
    }

    pub fn check_privilege(
        &self,
        username: &str,
        table: Option<&str>,
        privilege: Privilege,
        db_state: &DatabaseState,
    ) -> SqlResult<()> {
        println!("CHECK PRIVILEGE: user={}, table={:?}, priv={:?}", username, table, privilege);
        let user = db_state.users.get(username).ok_or_else(|| {
            SqlError::Runtime(format!("User {} not found", username))
        })?;

        println!("USER PRIVS: global={:?}, table={:?}", user.global_privileges, user.table_privileges);

        // root always has All
        if user.global_privileges.contains(&Privilege::All) {
            return Ok(());
        }

        if let Some(t) = table {
            if let Some(privs) = user.table_privileges.get(t) {
                if privs.contains(&Privilege::All) || privs.contains(&privilege) {
                    return Ok(());
                }
            }
        }

        if user.global_privileges.contains(&privilege) {
            return Ok(());
        }

        Err(SqlError::Runtime(format!(
            "User {} does not have {:?} privilege{}",
            username,
            privilege,
            table.map(|t| format!(" on table {}", t)).unwrap_or_default()
        )))
    }

    pub(crate) fn refresh_materialized_views(&self, state: &mut DatabaseState) -> SqlResult<()> {
        let views = state.materialized_views.clone();
        for (name, query) in views {
            let res =
                futures::executor::block_on(self.exec_select_internal(query, &[], &[], state))?;

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
