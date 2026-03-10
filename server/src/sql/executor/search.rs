use super::super::ast;
use super::super::error::{SqlError, SqlResult};
use super::{Executor, QueryResult};
use crate::storage::{DatabaseState, Value};

impl Executor {
    pub(crate) async fn exec_search(
        &self,
        stmt: ast::SearchStmt,
        db_state: &DatabaseState,
        tx_id: Option<&str>,
    ) -> SqlResult<QueryResult> {
        let table = db_state
            .get_table(&stmt.table)
            .ok_or_else(|| SqlError::TableNotFound(stmt.table.clone()))?;

        let search_index = table.search_index.as_ref().ok_or_else(|| {
            SqlError::Runtime(format!(
                "Full-text search index not enabled for table: {}",
                stmt.table
            ))
        })?;

        let results = search_index
            .lock()
            .unwrap()
            .search(&stmt.query, 100)
            .map_err(|e| SqlError::Runtime(format!("Search error: {}", e)))?;

        let mut rows = Vec::new();
        for (row_id, score) in results {
            if let Some(row) = table.rows.iter().find(|r| r.id == row_id) {
                let mut values = row.values.clone();
                values.push(Value::Float(score as f64));
                rows.push(values);
            }
        }

        let mut columns: Vec<String> = table.columns.iter().map(|c| c.name.clone()).collect();
        columns.push("_score".to_string());

        Ok(QueryResult {
            columns,
            rows,
            rows_affected: 0,
            transaction_id: tx_id.map(|s| s.to_string()),
        })
    }
}
