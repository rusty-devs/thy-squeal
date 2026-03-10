use super::super::ast::{CreateUserStmt, DropUserStmt, GrantStmt, RevokeStmt};
use super::super::error::{SqlError, SqlResult};
use super::{Executor, QueryResult};
use crate::storage::User;
use std::collections::HashMap;

impl Executor {
    pub(crate) async fn exec_create_user(
        &self,
        stmt: CreateUserStmt,
        tx_id: Option<&str>,
    ) -> SqlResult<QueryResult> {
        let hashed = bcrypt::hash(&stmt.password, bcrypt::DEFAULT_COST)
            .map_err(|e| SqlError::Runtime(format!("Bcrypt error: {}", e)))?;

        // Log to WAL (we should add a WalRecord for this)
        // For now skip WAL for users or add it. Let's add it.
        
        self.mutate_state(tx_id, |state| {
            if state.users.contains_key(&stmt.username) {
                return Err(SqlError::Runtime(format!("User {} already exists", stmt.username)));
            }
            state.users.insert(stmt.username.clone(), User {
                username: stmt.username,
                password_hash: hashed,
                global_privileges: vec![],
                table_privileges: HashMap::new(),
            });
            Ok(())
        }).await?;

        Ok(QueryResult {
            columns: vec![],
            rows: vec![],
            rows_affected: 0,
            transaction_id: tx_id.map(|s| s.to_string()),
        })
    }

    pub(crate) async fn exec_drop_user(
        &self,
        stmt: DropUserStmt,
        tx_id: Option<&str>,
    ) -> SqlResult<QueryResult> {
        self.mutate_state(tx_id, |state| {
            state.users.remove(&stmt.username)
                .ok_or_else(|| SqlError::Runtime(format!("User {} not found", stmt.username)))?;
            Ok(())
        }).await?;

        Ok(QueryResult {
            columns: vec![],
            rows: vec![],
            rows_affected: 0,
            transaction_id: tx_id.map(|s| s.to_string()),
        })
    }

    pub(crate) async fn exec_grant(&self, stmt: GrantStmt, tx_id: Option<&str>) -> SqlResult<QueryResult> {
        self.mutate_state(tx_id, |state| {
            let user = state.users.get_mut(&stmt.username)
                .ok_or_else(|| SqlError::Runtime(format!("User {} not found", stmt.username)))?;
            
            if let Some(table) = &stmt.table {
                let entry = user.table_privileges.entry(table.clone()).or_default();
                for p in &stmt.privileges {
                    if !entry.contains(p) {
                        entry.push(p.clone());
                    }
                }
            } else {
                for p in &stmt.privileges {
                    if !user.global_privileges.contains(p) {
                        user.global_privileges.push(p.clone());
                    }
                }
            }
            Ok(())
        }).await?;

        Ok(QueryResult {
            columns: vec![],
            rows: vec![],
            rows_affected: 0,
            transaction_id: tx_id.map(|s| s.to_string()),
        })
    }

    pub(crate) async fn exec_revoke(&self, stmt: RevokeStmt, tx_id: Option<&str>) -> SqlResult<QueryResult> {
        self.mutate_state(tx_id, |state| {
            let user = state.users.get_mut(&stmt.username)
                .ok_or_else(|| SqlError::Runtime(format!("User {} not found", stmt.username)))?;
            
            if let Some(table) = &stmt.table {
                if let Some(entry) = user.table_privileges.get_mut(table) {
                    entry.retain(|p| !stmt.privileges.contains(p));
                }
            } else {
                user.global_privileges.retain(|p| !stmt.privileges.contains(p));
            }
            Ok(())
        }).await?;

        Ok(QueryResult {
            columns: vec![],
            rows: vec![],
            rows_affected: 0,
            transaction_id: tx_id.map(|s| s.to_string()),
        })
    }
}
