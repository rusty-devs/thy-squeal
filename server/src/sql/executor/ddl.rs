use super::super::error::{SqlError, SqlResult};
use crate::squeal::{
    AlterAction, AlterTable, CreateIndex, CreateMaterializedView, CreateTable, DropTable,
    Expression, IndexType,
};
use super::{Executor, QueryResult, SelectQueryPlan, Session};
use crate::storage::{Table, WalRecord};

impl Executor {
    pub(crate) async fn exec_create_table(
        &self,
        stmt: CreateTable,
        tx_id: Option<&str>,
    ) -> SqlResult<QueryResult> {
        let name = stmt.name.clone();
        let columns = stmt.columns.clone();
        let primary_key = stmt.primary_key.clone();
        let foreign_keys = stmt.foreign_keys.clone();

        // 1. Log to WAL
        {
            let db = self.db.read().await;
            db.log_operation(&WalRecord::CreateTable {
                tx_id: tx_id.map(|s| s.to_string()),
                name: name.clone(),
                columns: columns.clone(),
                primary_key: primary_key.clone(),
                foreign_keys: foreign_keys.clone(),
            })?;
        }

        // 2. Mutate state
        self.mutate_state(tx_id, |state| {
            if state.tables.contains_key(&name) {
                return Err(SqlError::Storage(
                    crate::storage::error::StorageError::PersistenceError(format!(
                        "Table {} already exists",
                        name
                    )),
                ));
            }

            let mut table = Table::new(
                name.clone(),
                columns,
                primary_key.clone(),
                foreign_keys.clone(),
            );

            // Enable search index automatically
            if let Some(ref dir) = self.data_dir {
                table.enable_search_index(dir);
            }

            // If primary key is defined, create a unique B-Tree index for it
            if let Some(ref pk_cols) = primary_key {
                let pk_exprs: Vec<Expression> = pk_cols
                    .iter()
                    .map(|c| Expression::Column(c.clone()))
                    .collect();

                table.create_index(
                    self,
                    format!("pk_{}", name),
                    pk_exprs,
                    true,  // unique
                    false, // btree
                    None,  // no where clause
                    state,
                )?;
            }

            state.tables.insert(name, table);
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

    pub(crate) async fn exec_create_materialized_view(
        &self,
        stmt: CreateMaterializedView,
        tx_id: Option<&str>,
    ) -> SqlResult<QueryResult> {
        // 1. Log to WAL
        {
            let db = self.db.read().await;
            db.log_operation(&WalRecord::CreateMaterializedView {
                tx_id: tx_id.map(|s| s.to_string()),
                name: stmt.name.clone(),
                query: Box::new(stmt.query.clone()),
            })?;
        }

        self.mutate_state(tx_id, |state| {
            if state.tables.contains_key(&stmt.name) {
                return Err(SqlError::Storage(
                    crate::storage::error::StorageError::PersistenceError(format!(
                        "Table {} already exists",
                        stmt.name
                    )),
                ));
            }

            let plan = SelectQueryPlan::new(stmt.query.clone(), state, Session::root());
            let res = futures::executor::block_on(self.exec_select_recursive(plan))?;

            let mut cols = Vec::new();
            for col_name in &res.columns {
                cols.push(crate::storage::Column {
                    name: col_name.clone(),
                    data_type: crate::storage::DataType::Text,
                    is_auto_increment: false,
                });
            }

            let mut table = Table::new(stmt.name.clone(), cols, None, vec![]);
            table.data.rows = res
                .rows
                .into_iter()
                .enumerate()
                .map(|(i, values)| crate::storage::Row {
                    id: format!("mv_{}_{}", stmt.name, i),
                    values,
                })
                .collect();

            state.tables.insert(stmt.name.clone(), table);
            state
                .materialized_views
                .insert(stmt.name.clone(), stmt.query.clone());
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

    pub(crate) async fn exec_drop_table(
        &self,
        stmt: DropTable,
        tx_id: Option<&str>,
    ) -> SqlResult<QueryResult> {
        // 1. Log to WAL
        {
            let db = self.db.read().await;
            db.log_operation(&WalRecord::DropTable {
                tx_id: tx_id.map(|s| s.to_string()),
                name: stmt.name.clone(),
            })?;
        }

        // 2. Mutate state
        self.mutate_state(tx_id, |state| {
            state
                .tables
                .remove(&stmt.name)
                .ok_or_else(|| SqlError::TableNotFound(stmt.name.clone()))?;
            state.materialized_views.remove(&stmt.name);
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
        stmt: CreateIndex,
        tx_id: Option<&str>,
    ) -> SqlResult<QueryResult> {
        let table_name = stmt.table.clone();
        let index_name = stmt.name.clone();

        // 1. Log to WAL
        {
            let db = self.db.read().await;
            db.log_operation(&WalRecord::CreateIndex {
                tx_id: tx_id.map(|s| s.to_string()),
                table: table_name.clone(),
                name: index_name.clone(),
                expressions: stmt.expressions.clone(),
                unique: stmt.unique,
                use_hash: matches!(stmt.index_type, IndexType::Hash),
                where_clause: stmt.where_clause.clone(),
            })?;
        }

        // 2. Mutate state
        self.mutate_state(tx_id, |state| {
            let db_state_copy = state.clone();
            let table = state
                .get_table_mut(&table_name)
                .ok_or_else(|| SqlError::TableNotFound(table_name.clone()))?;

            table.create_index(
                self,
                index_name,
                stmt.expressions,
                stmt.unique,
                matches!(stmt.index_type, IndexType::Hash),
                stmt.where_clause,
                &db_state_copy,
            )?;
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
        stmt: AlterTable,
        tx_id: Option<&str>,
    ) -> SqlResult<QueryResult> {
        // 1. Log to WAL
        {
            let db = self.db.read().await;
            db.log_operation(&WalRecord::AlterTable {
                tx_id: tx_id.map(|s| s.to_string()),
                table: stmt.table.clone(),
                action: stmt.action.clone(),
            })?;
        }

        // 2. Mutate state
        self.mutate_state(tx_id, |state| {
            match stmt.action {
                AlterAction::AddColumn(col) => {
                    let table = state
                        .get_table_mut(&stmt.table)
                        .ok_or_else(|| SqlError::TableNotFound(stmt.table.clone()))?;
                    table.add_column(col)?;
                }
                AlterAction::DropColumn(col_name) => {
                    let table = state
                        .get_table_mut(&stmt.table)
                        .ok_or_else(|| SqlError::TableNotFound(stmt.table.clone()))?;
                    table.drop_column(&col_name)?;
                }
                AlterAction::RenameColumn { old_name, new_name } => {
                    let table = state
                        .get_table_mut(&stmt.table)
                        .ok_or_else(|| SqlError::TableNotFound(stmt.table.clone()))?;
                    table.rename_column(&old_name, &new_name)?;
                }
                AlterAction::RenameTable(new_name) => {
                    let table = state
                        .tables
                        .remove(&stmt.table)
                        .ok_or_else(|| SqlError::TableNotFound(stmt.table.clone()))?;
                    let mut table = table;
                    table.rename_table(new_name.clone());
                    state.tables.insert(new_name, table);
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
}
