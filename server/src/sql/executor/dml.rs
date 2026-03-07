use super::super::ast::{DeleteStmt, InsertStmt, UpdateStmt};
use super::super::eval::{evaluate_condition, evaluate_expression};
use super::{Executor, QueryResult};
use crate::sql::error::SqlError;
use crate::sql::error::SqlResult;
use crate::storage::WalRecord;

impl Executor {
    pub(crate) async fn exec_insert(
        &self,
        stmt: InsertStmt,
        tx_id: Option<&str>,
    ) -> SqlResult<QueryResult> {
        let (table_name, values) = (stmt.table.clone(), stmt.values.clone());

        // We need to cast values to column types
        let casted_values = {
            let db = self.db.read().await;
            let table = db
                .get_table(&table_name)
                .ok_or_else(|| SqlError::TableNotFound(table_name.clone()))?;
            if values.len() != table.columns.len() {
                return Err(SqlError::Runtime(format!(
                    "Expected {} values, got {}",
                    table.columns.len(),
                    values.len()
                )));
            }
            let mut res = Vec::new();
            for (val, col) in values.into_iter().zip(&table.columns) {
                res.push(val.cast(&col.data_type).map_err(|e| {
                    SqlError::Runtime(format!("Type error for column {}: {}", col.name, e))
                })?);
            }
            res
        };

        // Log to WAL first
        {
            let db = self.db.read().await;
            db.log_operation(&WalRecord::Insert {
                tx_id: tx_id.map(|s| s.to_string()),
                table: table_name.clone(),
                values: casted_values.clone(),
            })
            .map_err(|e| SqlError::Storage(e.to_string()))?;
        }

        self.mutate_state(tx_id, |state| {
            let db_state = state.clone();
            state
                .get_table_mut(&table_name)
                .ok_or_else(|| SqlError::TableNotFound(table_name.clone()))?
                .insert(self, casted_values, &db_state)
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

    pub(crate) async fn exec_update(
        &self,
        stmt: UpdateStmt,
        tx_id: Option<&str>,
    ) -> SqlResult<QueryResult> {
        // If tx_id is None, we'll hold a write lock on self.db
        // If tx_id is Some, we only lock the DashMap entry

        if let Some(id) = tx_id {
            let mut state = self
                .transactions
                .get_mut(id)
                .ok_or_else(|| SqlError::Runtime("Transaction not found".to_string()))?;

            let mut updated_rows = Vec::new();
            let mut count = 0;
            let db_state_eval = state.clone();
            {
                let table = state
                    .get_table(&stmt.table)
                    .ok_or_else(|| SqlError::TableNotFound(stmt.table.clone()))?;
                for row in &table.rows {
                    let matches = if let Some(ref where_clause) = stmt.where_clause {
                        evaluate_condition(self, where_clause, table, None, row, &db_state_eval)?
                    } else {
                        true
                    };

                    if matches {
                        let mut new_values = row.values.clone();
                        for (col_name, expr) in &stmt.assignments {
                            let col_idx = table
                                .column_index(col_name)
                                .ok_or_else(|| SqlError::ColumnNotFound(col_name.clone()))?;
                            let val = evaluate_expression(self, expr, table, None, row, &db_state_eval)?;
                            // Cast to column type
                            let casted_val = val.cast(&table.columns[col_idx].data_type).map_err(|e| {
                                SqlError::Runtime(format!("Type error for column {}: {}", col_name, e))
                            })?;
                            new_values[col_idx] = casted_val;
                        }
                        updated_rows.push((row.id.clone(), new_values));
                        count += 1;
                    }
                }
            }

            // Log updates to WAL
            {
                let db_lock = self.db.read().await;
                for (row_id, values) in &updated_rows {
                    db_lock
                        .log_operation(&WalRecord::Update {
                            tx_id: Some(id.to_string()),
                            table: stmt.table.clone(),
                            id: row_id.clone(),
                            values: values.clone(),
                        })
                        .map_err(|e| SqlError::Storage(e.to_string()))?;
                }
            }

            let table = state.get_table_mut(&stmt.table).unwrap();
            for (row_id, values) in updated_rows {
                table
                    .update(self, &row_id, values, &db_state_eval)
                    .map_err(|e| SqlError::Storage(e.to_string()))?;
            }

            Ok(QueryResult {
                columns: vec![],
                rows: vec![],
                rows_affected: count,
                transaction_id: Some(id.to_string()),
            })
        } else {
            let mut db = self.db.write().await;
            let mut updated_rows = Vec::new();
            let mut count = 0;
            let db_state_eval = db.state().clone();
            {
                let table = db
                    .get_table(&stmt.table)
                    .ok_or_else(|| SqlError::TableNotFound(stmt.table.clone()))?;
                for row in &table.rows {
                    let matches = if let Some(ref where_clause) = stmt.where_clause {
                        evaluate_condition(self, where_clause, table, None, row, &db_state_eval)?
                    } else {
                        true
                    };

                    if matches {
                        let mut new_values = row.values.clone();
                        for (col_name, expr) in &stmt.assignments {
                            let col_idx = table
                                .column_index(col_name)
                                .ok_or_else(|| SqlError::ColumnNotFound(col_name.clone()))?;
                            let val = evaluate_expression(self, expr, table, None, row, &db_state_eval)?;
                            // Cast to column type
                            let casted_val = val.cast(&table.columns[col_idx].data_type).map_err(|e| {
                                SqlError::Runtime(format!("Type error for column {}: {}", col_name, e))
                            })?;
                            new_values[col_idx] = casted_val;
                        }
                        updated_rows.push((row.id.clone(), new_values));
                        count += 1;
                    }
                }
            }

            for (row_id, values) in &updated_rows {
                db.log_operation(&WalRecord::Update {
                    tx_id: None,
                    table: stmt.table.clone(),
                    id: row_id.clone(),
                    values: values.clone(),
                })
                .map_err(|e| SqlError::Storage(e.to_string()))?;
            }

            let table = db.get_table_mut(&stmt.table).unwrap();
            for (row_id, values) in updated_rows {
                table
                    .update(self, &row_id, values, &db_state_eval)
                    .map_err(|e| SqlError::Storage(e.to_string()))?;
            }
            db.save().map_err(|e| SqlError::Storage(e.to_string()))?;

            Ok(QueryResult {
                columns: vec![],
                rows: vec![],
                rows_affected: count,
                transaction_id: None,
            })
        }
    }

    pub(crate) async fn exec_delete(
        &self,
        stmt: DeleteStmt,
        tx_id: Option<&str>,
    ) -> SqlResult<QueryResult> {
        if let Some(id) = tx_id {
            let mut state = self
                .transactions
                .get_mut(id)
                .ok_or_else(|| SqlError::Runtime("Transaction not found".to_string()))?;

            let mut ids_to_delete = Vec::new();
            let mut count = 0;
            let db_state_eval = state.clone();
            {
                let table = state
                    .get_table(&stmt.table)
                    .ok_or_else(|| SqlError::TableNotFound(stmt.table.clone()))?;
                for row in &table.rows {
                    let matches = if let Some(ref where_clause) = stmt.where_clause {
                        evaluate_condition(self, where_clause, table, None, row, &db_state_eval)?
                    } else {
                        true
                    };

                    if matches {
                        ids_to_delete.push(row.id.clone());
                        count += 1;
                    }
                }
            }

            // Log deletes to WAL
            {
                let db_lock = self.db.read().await;
                for row_id in &ids_to_delete {
                    db_lock
                        .log_operation(&WalRecord::Delete {
                            tx_id: Some(id.to_string()),
                            table: stmt.table.clone(),
                            id: row_id.clone(),
                        })
                        .map_err(|e| SqlError::Storage(e.to_string()))?;
                }
            }

            let table = state.get_table_mut(&stmt.table).unwrap();
            for row_id in ids_to_delete {
                table
                    .delete(self, &row_id, &db_state_eval)
                    .map_err(|e| SqlError::Storage(e.to_string()))?;
            }

            Ok(QueryResult {
                columns: vec![],
                rows: vec![],
                rows_affected: count,
                transaction_id: Some(id.to_string()),
            })
        } else {
            let mut db = self.db.write().await;
            let mut ids_to_delete = Vec::new();
            let mut count = 0;
            let db_state_eval = db.state().clone();
            {
                let table = db
                    .get_table(&stmt.table)
                    .ok_or_else(|| SqlError::TableNotFound(stmt.table.clone()))?;
                for row in &table.rows {
                    let matches = if let Some(ref where_clause) = stmt.where_clause {
                        evaluate_condition(self, where_clause, table, None, row, &db_state_eval)?
                    } else {
                        true
                    };

                    if matches {
                        ids_to_delete.push(row.id.clone());
                        count += 1;
                    }
                }
            }

            for row_id in &ids_to_delete {
                db.log_operation(&WalRecord::Delete {
                    tx_id: None,
                    table: stmt.table.clone(),
                    id: row_id.clone(),
                })
                .map_err(|e| SqlError::Storage(e.to_string()))?;
            }

            let table = db.get_table_mut(&stmt.table).unwrap();
            for row_id in ids_to_delete {
                table
                    .delete(self, &row_id, &db_state_eval)
                    .map_err(|e| SqlError::Storage(e.to_string()))?;
            }
            db.save().map_err(|e| SqlError::Storage(e.to_string()))?;

            Ok(QueryResult {
                columns: vec![],
                rows: vec![],
                rows_affected: count,
                transaction_id: None,
            })
        }
    }
}
