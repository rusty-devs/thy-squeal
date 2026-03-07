use super::super::ast::{CreateIndexStmt, CreateTableStmt, DropTableStmt};
use super::super::error::{SqlError, SqlResult};
use super::{Executor, QueryResult};

impl Executor {
    pub(crate) async fn exec_create_table(&self, stmt: CreateTableStmt) -> SqlResult<QueryResult> {
        let mut db = self.db.write().await;
        db.create_table(stmt.name, stmt.columns)?;

        Ok(QueryResult {
            columns: vec![],
            rows: vec![],
            rows_affected: 0,
        })
    }

    pub(crate) async fn exec_drop_table(&self, stmt: DropTableStmt) -> SqlResult<QueryResult> {
        let mut db = self.db.write().await;
        db.drop_table(&stmt.name)?;

        Ok(QueryResult {
            columns: vec![],
            rows: vec![],
            rows_affected: 0,
        })
    }

    pub(crate) async fn exec_create_index(&self, stmt: CreateIndexStmt) -> SqlResult<QueryResult> {
        let mut db = self.db.write().await;
        let table = db
            .get_table_mut(&stmt.table)
            .ok_or_else(|| SqlError::TableNotFound(stmt.table.clone()))?;

        table.create_index(&stmt.column)?;
        db.save().map_err(|e| SqlError::Storage(e.to_string()))?;

        Ok(QueryResult {
            columns: vec![],
            rows: vec![],
            rows_affected: 0,
        })
    }
}
