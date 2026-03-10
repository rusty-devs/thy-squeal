use super::super::error::{SqlError, SqlResult};
use super::{Executor, QueryResult};
use crate::storage::{DatabaseState, WalRecord};

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

    pub(crate) async fn exec_begin(&self) -> SqlResult<QueryResult> {
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

    pub(crate) async fn exec_commit(&self, tx_id: Option<&str>) -> SqlResult<QueryResult> {
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

    pub(crate) async fn exec_rollback(&self, tx_id: Option<&str>) -> SqlResult<QueryResult> {
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
