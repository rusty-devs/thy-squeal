use super::super::ast::{
    AlterAction, AlterTableStmt, CreateIndexStmt, CreateMaterializedViewStmt, CreateTableStmt,
    DropTableStmt, IndexType,
};
use super::super::error::{SqlError, SqlResult};
use super::{Executor, QueryResult};
use crate::storage::{Table, WalRecord};

impl Executor {
    pub(crate) async fn exec_create_table(
        &self,
        stmt: CreateTableStmt,
        tx_id: Option<&str>,
    ) -> SqlResult<QueryResult> {
        let name = stmt.name.clone();
        let columns = stmt.columns.clone();
        let primary_key = stmt.primary_key.clone();
        let foreign_keys = stmt.foreign_keys.clone();

        // Log to WAL
        {
            let db = self.db.read().await;
            db.log_operation(&WalRecord::CreateTable {
                tx_id: tx_id.map(|s| s.to_string()),
                name: name.clone(),
                columns: columns.clone(),
                primary_key: primary_key.clone(),
                foreign_keys: foreign_keys.clone(),
            })
            .map_err(|e| SqlError::Storage(e.to_string()))?;
        }

        if let Some(id) = tx_id {
            self.mutate_state(Some(id), |state| {
                if state.get_table(&name).is_some() {
                    return Err(SqlError::Storage(format!("Table {} already exists", name)));
                }
                state.tables.insert(
                    name.clone(),
                    Table::new(name, columns, primary_key, foreign_keys),
                );
                Ok(())
            })
            .await?;
        } else {
            let mut db = self.db.write().await;
            db.create_table(name, columns, primary_key, foreign_keys)
                .map_err(|e| SqlError::Storage(e.to_string()))?;
        }

        Ok(QueryResult {
            columns: vec![],
            rows: vec![],
            rows_affected: 0,
            transaction_id: tx_id.map(|s| s.to_string()),
        })
    }

    pub(crate) async fn exec_create_materialized_view(
        &self,
        stmt: CreateMaterializedViewStmt,
        tx_id: Option<&str>,
    ) -> SqlResult<QueryResult> {
        // Log to WAL
        {
            let db = self.db.read().await;
            db.log_operation(&WalRecord::CreateMaterializedView {
                tx_id: tx_id.map(|s| s.to_string()),
                name: stmt.name.clone(),
                query: Box::new(stmt.query.clone()),
            })
            .map_err(|e| SqlError::Storage(e.to_string()))?;
        }

        if let Some(id) = tx_id {
            self.mutate_state(Some(id), |state| {
                if state.tables.contains_key(&stmt.name) {
                    return Err(SqlError::Storage(format!(
                        "Table {} already exists",
                        stmt.name
                    )));
                }

                let plan =
                    super::SelectQueryPlan::new(stmt.query.clone(), state, super::Session::root());
                let res = futures::executor::block_on(self.exec_select_recursive(plan))
                    .map_err(|e: SqlError| SqlError::Storage(e.to_string()))?;

                let mut cols = Vec::new();
                for col_name in &res.columns {
                    cols.push(crate::storage::Column {
                        name: col_name.clone(),
                        data_type: crate::storage::DataType::Text,
                        is_auto_increment: false,
                    });
                }

                let mut table = Table::new(stmt.name.clone(), cols, None, vec![]);
                table.rows = res
                    .rows
                    .into_iter()
                    .enumerate()
                    .map(|(i, values)| crate::storage::Row {
                        id: format!("mv_{}_{}", stmt.name, i),
                        values,
                    })
                    .collect();

                state
                    .materialized_views
                    .insert(stmt.name.clone(), stmt.query);
                state.tables.insert(stmt.name, table);
                Ok(())
            })
            .await?;
        } else {
            let mut db = self.db.write().await;
            db.create_materialized_view(
                self as &dyn crate::sql::eval::Evaluator,
                stmt.name,
                stmt.query,
            )
            .map_err(|e| SqlError::Storage(e.to_string()))?;
        }

        Ok(QueryResult {
            columns: vec![],
            rows: vec![],
            rows_affected: 0,
            transaction_id: tx_id.map(|s| s.to_string()),
        })
    }

    pub(crate) async fn exec_drop_table(
        &self,
        stmt: DropTableStmt,
        tx_id: Option<&str>,
    ) -> SqlResult<QueryResult> {
        // Log to WAL
        {
            let db = self.db.read().await;
            db.log_operation(&WalRecord::DropTable {
                tx_id: tx_id.map(|s| s.to_string()),
                name: stmt.name.clone(),
            })
            .map_err(|e| SqlError::Storage(e.to_string()))?;
        }

        self.mutate_state(tx_id, |state| {
            state
                .tables
                .remove(&stmt.name)
                .ok_or_else(|| SqlError::TableNotFound(stmt.name.clone()))?;
            Ok(())
        })
        .await?;

        Ok(QueryResult {
            columns: vec![],
            rows: vec![],
            rows_affected: 0,
            transaction_id: tx_id.map(|s| s.to_string()),
        })
    }

    pub(crate) async fn exec_alter_table(
        &self,
        stmt: AlterTableStmt,
        tx_id: Option<&str>,
    ) -> SqlResult<QueryResult> {
        // Log to WAL
        {
            let db = self.db.read().await;
            db.log_operation(&WalRecord::AlterTable {
                tx_id: tx_id.map(|s| s.to_string()),
                table: stmt.table.clone(),
                action: stmt.action.clone(),
            })
            .map_err(|e| SqlError::Storage(e.to_string()))?;
        }

        self.mutate_state(tx_id, |state| {
            if state.get_table(&stmt.table).is_none() {
                return Err(SqlError::TableNotFound(stmt.table.clone()));
            }

            match stmt.action {
                AlterAction::AddColumn(col) => {
                    let table = state.get_table_mut(&stmt.table).unwrap();
                    table
                        .add_column(col)
                        .map_err(|e| SqlError::Storage(e.to_string()))?;
                }
                AlterAction::DropColumn(name) => {
                    let table = state.get_table_mut(&stmt.table).unwrap();
                    table
                        .drop_column(&name)
                        .map_err(|e| SqlError::Storage(e.to_string()))?;
                }
                AlterAction::RenameColumn { old_name, new_name } => {
                    let table = state.get_table_mut(&stmt.table).unwrap();
                    table
                        .rename_column(&old_name, &new_name)
                        .map_err(|e| SqlError::Storage(e.to_string()))?;
                }
                AlterAction::RenameTable(new_name) => {
                    if state.get_table(&new_name).is_some() {
                        return Err(SqlError::Storage(format!(
                            "Table {} already exists",
                            new_name
                        )));
                    }
                    let mut t = state.tables.remove(&stmt.table).unwrap();
                    t.rename_table(new_name.clone());
                    state.tables.insert(new_name, t);
                }
            }
            Ok(())
        })
        .await?;

        Ok(QueryResult {
            columns: vec![],
            rows: vec![],
            rows_affected: 0,
            transaction_id: tx_id.map(|s| s.to_string()),
        })
    }

    pub(crate) async fn exec_create_index(
        &self,
        stmt: CreateIndexStmt,
        tx_id: Option<&str>,
    ) -> SqlResult<QueryResult> {
        // Log to WAL
        {
            let db = self.db.read().await;
            db.log_operation(&WalRecord::CreateIndex {
                tx_id: tx_id.map(|s| s.to_string()),
                table: stmt.table.clone(),
                name: stmt.name.clone(),
                expressions: stmt.expressions.clone(),
                unique: stmt.unique,
                use_hash: matches!(stmt.index_type, IndexType::Hash),
                where_clause: stmt.where_clause.clone(),
            })
            .map_err(|e| SqlError::Storage(e.to_string()))?;
        }

        self.mutate_state(tx_id, |state| {
            let db_state_copy = state.clone();
            let table = state
                .get_table_mut(&stmt.table)
                .ok_or_else(|| SqlError::TableNotFound(stmt.table.clone()))?;

            table
                .create_index(
                    self as &dyn crate::sql::eval::Evaluator,
                    stmt.name,
                    stmt.expressions,
                    stmt.unique,
                    matches!(stmt.index_type, IndexType::Hash),
                    stmt.where_clause,
                    &db_state_copy,
                )
                .map_err(|e| SqlError::Storage(e.to_string()))?;
            Ok(())
        })
        .await?;

        Ok(QueryResult {
            columns: vec![],
            rows: vec![],
            rows_affected: 0,
            transaction_id: tx_id.map(|s| s.to_string()),
        })
    }
}
