use crate::storage::{Value, WalRecord};
use super::super::super::ast::InsertStmt;
use super::super::super::error::{SqlError, SqlResult};
use super::super::super::eval::{evaluate_expression_joined, Evaluator};
use super::super::{QueryResult, Executor};

impl Executor {
    pub(crate) async fn exec_insert(
        &self,
        stmt: InsertStmt,
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
            table.columns.len()
        };

        if stmt.values.len() != column_count {
            return Err(SqlError::TypeMismatch(format!(
                "Value count mismatch: expected {}, got {}",
                column_count,
                stmt.values.len()
            )));
        }

        // Map expressions to table columns
        let mapped_values = if let Some(ref col_names) = stmt.columns {
            // Initialize with NULLs
            let mut vals = vec![Value::Null; table.columns.len()];
            for (i, name) in col_names.iter().enumerate() {
                let col_idx = table.column_index(name).ok_or_else(|| {
                    SqlError::ColumnNotFound(format!("{}.{}", table_name, name))
                })?;
                
                let mut val = evaluate_expression_joined(self, &stmt.values[i], &[], params, &[], &state)?;
                let target_type = &table.columns[col_idx].data_type;
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
                let mut val = evaluate_expression_joined(self, expr, &[], params, &[], &state)?;
                let target_type = &table.columns[i].data_type;
                val = val.cast(target_type).map_err(|e| {
                    SqlError::TypeMismatch(format!(
                        "Error casting value for column '{}': {}",
                        table.columns[i].name, e
                    ))
                })?;
                vals.push(val);
            }
            vals
        };

        drop(db); // Release read lock before mutation

        // Log to WAL
        {
            let db = self.db.read().await;
            db.log_operation(&WalRecord::Insert {
                tx_id: tx_id.map(|s| s.to_string()),
                table: table_name.clone(),
                values: mapped_values.clone(),
            })
            .map_err(|e| SqlError::Storage(e.to_string()))?;
        }

        self.mutate_state(tx_id, |state| {
            let db_state_copy = state.clone();
            let table = state
                .get_table_mut(&table_name)
                .ok_or_else(|| SqlError::TableNotFound(table_name.clone()))?;

            table
                .insert(self as &dyn Evaluator, mapped_values, &db_state_copy)
                .map_err(|e| SqlError::Storage(e.to_string()))?;
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
