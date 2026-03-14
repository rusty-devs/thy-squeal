use super::super::super::error::{SqlError, SqlResult};
use super::super::super::eval::{
    EvalContext, Evaluator, evaluate_condition_joined, evaluate_expression_joined,
};
use super::super::{Executor, QueryResult};
use crate::squeal::Update;
use crate::storage::{Value, WalRecord};

impl Executor {
    pub(crate) async fn exec_update(
        &self,
        stmt: Update,
        params: &[Value],
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

        let mut row_updates = Vec::new();

        for row in table.rows() {
            let context_list = [(table, None, row)];
            let eval_ctx = EvalContext::new(&context_list, params, &[], &state);

            let matched = if let Some(ref cond) = stmt.where_clause {
                evaluate_condition_joined(self, cond, &eval_ctx)?
            } else {
                true
            };

            if matched {
                let mut new_values = row.values.clone();
                for (col_name, expr) in &stmt.assignments {
                    let col_idx = table
                        .column_index(col_name)
                        .ok_or_else(|| SqlError::ColumnNotFound(col_name.clone()))?;
                    let mut val = evaluate_expression_joined(self, expr, &eval_ctx)?;

                    // Perform type casting for UPDATE
                    let target_type = &table.columns()[col_idx].data_type;
                    val = val.cast(target_type).map_err(|e| {
                        SqlError::TypeMismatch(format!(
                            "Error casting value for column '{}': {}",
                            col_name, e
                        ))
                    })?;

                    new_values[col_idx] = val;
                }
                row_updates.push((row.id.clone(), new_values));
            }
        }

        drop(db); // Release read lock before mutation

        for (id, values) in row_updates {
            // Log to WAL
            {
                let db = self.db.read().await;
                db.log_operation(&WalRecord::Update {
                    tx_id: tx_id.map(|s| s.to_string()),
                    table: table_name.clone(),
                    id: id.clone(),
                    values: values.clone(),
                })?;
            }

            self.mutate_state(tx_id, |state| {
                let db_state_copy = state.clone();
                let table = state
                    .get_table_mut(&table_name)
                    .ok_or_else(|| SqlError::TableNotFound(table_name.clone()))?;
                table.update(self as &dyn Evaluator, &id, values, &db_state_copy)?;

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
