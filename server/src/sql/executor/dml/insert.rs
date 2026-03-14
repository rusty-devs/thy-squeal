use super::super::super::error::{SqlError, SqlResult};
use super::super::super::eval::{EvalContext, Evaluator, evaluate_expression_joined};
use crate::squeal::Insert;
use super::super::{Executor, QueryResult};
use crate::storage::{Value, WalRecord};

impl Executor {
    pub(crate) async fn exec_insert(
        &self,
        stmt: Insert,
        params: &[Value],
        tx_id: Option<&str>,
    ) -> SqlResult<QueryResult> {
        let table_name = stmt.table.clone();

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

        let column_count = if let Some(ref cols) = stmt.columns {
            cols.len()
        } else {
            table.columns().len()
        };

        if stmt.values.len() != column_count {
            return Err(SqlError::TypeMismatch(format!(
                "Value count mismatch: expected {}, got {}",
                column_count,
                stmt.values.len()
            )));
        }

        let eval_ctx = EvalContext::new(&[], params, &[], &state);

        // Map expressions to table columns
        let mut mapped_values = if let Some(ref col_names) = stmt.columns {
            // Initialize with NULLs
            let mut vals = vec![Value::Null; table.columns().len()];
            for (i, name) in col_names.iter().enumerate() {
                let col_idx = table
                    .column_index(name)
                    .ok_or_else(|| SqlError::ColumnNotFound(format!("{}.{}", table_name, name)))?;

                let mut val = evaluate_expression_joined(self, &stmt.values[i], &eval_ctx)?;
                let target_type = &table.columns()[col_idx].data_type;
                val = val.cast(target_type).map_err(|e| {
                    SqlError::TypeMismatch(format!(
                        "Error casting value for column '{}': {}",
                        name, e
                    ))
                })?;
                vals[col_idx] = val;
            }
            vals
        } else {
            // Position-based mapping
            let mut vals = Vec::new();
            for (i, expr) in stmt.values.iter().enumerate() {
                let mut val = evaluate_expression_joined(self, expr, &eval_ctx)?;
                let target_type = &table.columns()[i].data_type;
                val = val.cast(target_type).map_err(|e| {
                    SqlError::TypeMismatch(format!(
                        "Error casting value for column '{}': {}",
                        table.columns()[i].name,
                        e
                    ))
                })?;
                vals.push(val);
            }
            vals
        };

        drop(db); // Release read lock before mutation

        // Generate auto-increment values
        self.mutate_state(tx_id, |state| {
            let table = state
                .get_table_mut(&table_name)
                .ok_or_else(|| SqlError::TableNotFound(table_name.clone()))?;

            let mut to_generate = Vec::new();
            for (i, col) in table.columns().iter().enumerate() {
                if col.is_auto_increment && matches!(&mapped_values[i], Value::Null) {
                    to_generate.push(i);
                }
            }

            for i in to_generate {
                if let Some(next_val) = table.generate_auto_inc(i) {
                    mapped_values[i] = Value::Int(next_val as i64);
                }
            }
            Ok(())
        })
        .await?;

        // Log to WAL
        {
            let db = self.db.read().await;
            db.log_operation(&WalRecord::Insert {
                tx_id: tx_id.map(|s| s.to_string()),
                table: table_name.clone(),
                values: mapped_values.clone(),
            })?;
        }

        self.mutate_state(tx_id, |state| {
            let db_state_copy = state.clone();
            let table = state
                .get_table_mut(&table_name)
                .ok_or_else(|| SqlError::TableNotFound(table_name.clone()))?;

            table.insert(self as &dyn Evaluator, mapped_values, &db_state_copy)?;

            self.refresh_materialized_views(state)?;
            Ok(())
        })
        .await?;

        Ok(QueryResult {
            columns: vec![],
            rows: vec![],
            rows_affected: 1,
            transaction_id: tx_id.map(|s| s.to_string()),
        })
    }
}
