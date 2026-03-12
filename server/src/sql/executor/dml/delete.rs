use super::super::super::error::{SqlError, SqlResult};
use super::super::super::eval::{EvalContext, Evaluator, evaluate_condition_joined};
use super::super::super::squeal::Delete;
use super::super::{Executor, QueryResult};
use crate::storage::WalRecord;

impl Executor {
    pub(crate) async fn exec_delete(
        &self,
        stmt: Delete,
        params: &[crate::storage::Value],
        tx_id: Option<&str>,
    ) -> SqlResult<QueryResult> {
        let table_name = stmt.table.clone();
        let mut rows_affected = 0;

        let db = self.db.read().await;
        let state = if let Some(id) = tx_id {
            self.transactions
                .get(id)
                .ok_or_else(|| SqlError::Runtime("Transaction not found".to_string()))?
                .clone()
        } else {
            db.state().clone()
        };

        let table = state
            .get_table(&table_name)
            .ok_or_else(|| SqlError::TableNotFound(table_name.clone()))?;

        let mut row_ids_to_delete = Vec::new();

        for row in table.rows() {
            let context_list = [(table, None, row)];
            let eval_ctx = EvalContext::new(&context_list, params, &[], &state);

            let matched = if let Some(ref cond) = stmt.where_clause {
                evaluate_condition_joined(self, cond, &eval_ctx)?
            } else {
                true
            };

            if matched {
                row_ids_to_delete.push(row.id.clone());
            }
        }

        drop(db); // Release read lock before mutation

        for id in row_ids_to_delete {
            // Log to WAL
            {
                let db = self.db.read().await;
                db.log_operation(&WalRecord::Delete {
                    tx_id: tx_id.map(|s| s.to_string()),
                    table: table_name.clone(),
                    id: id.clone(),
                })?;
            }

            self.mutate_state(tx_id, |state| {
                let db_state_copy = state.clone();
                let table = state
                    .get_table_mut(&table_name)
                    .ok_or_else(|| SqlError::TableNotFound(table_name.clone()))?;
                table.delete(self as &dyn Evaluator, &id, &db_state_copy)?;

                self.refresh_materialized_views(state)?;
                Ok(())
            })
            .await?;
            rows_affected += 1;
        }

        Ok(QueryResult {
            columns: vec![],
            rows: vec![],
            rows_affected,
            transaction_id: tx_id.map(|s| s.to_string()),
        })
    }
}
