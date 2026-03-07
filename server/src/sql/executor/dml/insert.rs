use crate::storage::WalRecord;
use super::super::super::ast::InsertStmt;
use super::super::super::error::{SqlError, SqlResult};
use super::super::super::eval::Evaluator;
use super::super::{QueryResult, Executor};

impl Executor {
    pub(crate) async fn exec_insert(
        &self,
        stmt: InsertStmt,
        tx_id: Option<&str>,
    ) -> SqlResult<QueryResult> {
        let table_name = stmt.table.clone();
        let mut values = stmt.values.clone();

        // Perform type casting based on table schema
        {
            let db = self.db.read().await;
            let table = db
                .get_table(&table_name)
                .ok_or_else(|| SqlError::TableNotFound(table_name.clone()))?;

            if values.len() != table.columns.len() {
                return Err(SqlError::TypeMismatch(format!(
                    "Column count mismatch: expected {}, got {}",
                    table.columns.len(),
                    values.len()
                )));
            }

            for (i, val) in values.iter_mut().enumerate() {
                let target_type = &table.columns[i].data_type;
                *val = val.clone().cast(target_type).map_err(|e| {
                    SqlError::TypeMismatch(format!(
                        "Error casting value for column '{}': {}",
                        table.columns[i].name, e
                    ))
                })?;
            }
        }

        // Log to WAL
        {
            let db = self.db.read().await;
            db.log_operation(&WalRecord::Insert {
                tx_id: tx_id.map(|s| s.to_string()),
                table: table_name.clone(),
                values: values.clone(),
            })
            .map_err(|e| SqlError::Storage(e.to_string()))?;
        }

        self.mutate_state(tx_id, |state| {
            let db_state_copy = state.clone();
            let table = state
                .get_table_mut(&table_name)
                .ok_or_else(|| SqlError::TableNotFound(table_name.clone()))?;

            table
                .insert(self as &dyn Evaluator, values, &db_state_copy)
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
