use super::error::StorageError;
use super::persistence::{Persister, WalRecord};
use super::row::{Column, ForeignKey, Row};
use super::table::Table;
use super::types::DataType;
use super::value::Value;
use super::wal;
use crate::sql::eval::Evaluator;
use crate::squeal::{Expression, Select};
use serde::{Deserialize, Serialize};
use std::collections::HashMap;

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq, Hash)]
pub enum Privilege {
    Select,
    Insert,
    Update,
    Delete,
    Create,
    Drop,
    Grant,
    All,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct User {
    pub username: String,
    pub password_hash: String,
    pub global_privileges: Vec<Privilege>,
    pub table_privileges: HashMap<String, Vec<Privilege>>, // table_name -> privileges
}

#[derive(Debug, Clone, Serialize, Deserialize, Default)]
pub struct DatabaseState {
    pub tables: HashMap<String, Table>,
    #[serde(default)]
    pub materialized_views: HashMap<String, Select>,
    #[serde(default)]
    pub users: HashMap<String, User>,
}

impl DatabaseState {
    pub fn get_table(&self, name: &str) -> Option<&Table> {
        self.tables.get(name)
    }

    pub fn get_table_mut(&mut self, name: &str) -> Option<&mut Table> {
        self.tables.get_mut(name)
    }

    pub fn table_names(&self) -> Vec<&String> {
        self.tables.keys().collect()
    }
}

pub struct Database {
    state: DatabaseState,
    persister: Option<Box<dyn Persister>>,
    _data_dir: Option<String>,
}

impl Database {
    pub fn new() -> Self {
        let mut db = Self {
            state: DatabaseState::default(),
            persister: None,
            _data_dir: None,
        };
        db.ensure_root_user();
        db
    }

    pub fn with_persister(
        persister: Box<dyn Persister>,
        data_dir: String,
    ) -> Result<Self, StorageError> {
        let tables = persister.load_tables().unwrap_or_default();
        let mut db = Self {
            state: DatabaseState {
                tables,
                materialized_views: HashMap::new(),
                users: HashMap::new(),
            },
            persister: Some(persister),
            _data_dir: Some(data_dir.clone()),
        };

        // Replay WAL logs
        db.replay_logs()?;

        db.ensure_root_user();

        // Initialize search indices for each table (after WAL replay)
        for table in db.state.tables.values_mut() {
            let search_path = format!("{}/search_{}", data_dir, table.schema.name);
            table.enable_search_index(&search_path);
        }

        Ok(db)
    }

    fn ensure_root_user(&mut self) {
        if !self.state.users.contains_key("root") {
            let hashed = bcrypt::hash("root", bcrypt::DEFAULT_COST).unwrap();
            self.state.users.insert(
                "root".to_string(),
                User {
                    username: "root".to_string(),
                    password_hash: hashed,
                    global_privileges: vec![Privilege::All],
                    table_privileges: HashMap::new(),
                },
            );
        }
    }

    fn replay_logs(&mut self) -> Result<(), StorageError> {
        if let Some(ref persister) = self.persister {
            let logs = persister.load_logs()?;
            wal::replay_logs(&mut self.state, logs)?;
        }
        Ok(())
    }

    pub fn log_operation(&self, record: &WalRecord) -> Result<(), StorageError> {
        if let Some(ref persister) = self.persister {
            persister.log_operation(record)?;
        }
        Ok(())
    }

    pub fn save(&self) -> Result<(), StorageError> {
        if let Some(persister) = &self.persister {
            persister.save_tables(&self.state.tables)
        } else {
            Ok(())
        }
    }

    #[allow(dead_code)]
    pub fn create_materialized_view(
        &mut self,
        executor: &dyn Evaluator,
        name: String,
        query: Select,
    ) -> Result<(), StorageError> {
        if self.state.tables.contains_key(&name) {
            return Err(StorageError::DuplicateKey(name));
        }

        // 1. Execute query to get initial data
        let res = futures::executor::block_on(executor.exec_select_internal(
            query.clone(),
            &[],
            &[],
            &self.state,
        ))
        .map_err(|e| StorageError::PersistenceError(e.to_string()))?;

        // 2. Create a virtual table for the view
        let mut cols = Vec::new();
        for col_name in &res.columns {
            cols.push(Column {
                name: col_name.clone(),
                data_type: DataType::Text, // Default for MV
                is_auto_increment: false,
            });
        }

        let mut table = Table::new(name.clone(), cols, None, vec![]);
        table.data.rows = res
            .rows
            .into_iter()
            .enumerate()
            .map(|(i, values)| Row {
                id: format!("mv_{}_{}", name, i),
                values,
            })
            .collect();

        // 3. Store view metadata
        self.state.materialized_views.insert(name.clone(), query);
        self.state.tables.insert(name, table);
        self.save()
    }

    #[allow(dead_code)]
    pub fn create_table(
        &mut self,
        name: String,
        columns: Vec<Column>,
        primary_key: Option<Vec<String>>,
        foreign_keys: Vec<ForeignKey>,
    ) -> Result<(), StorageError> {
        if self.state.tables.contains_key(&name) {
            return Err(StorageError::DuplicateKey(name));
        }

        let mut table = Table::new(name.clone(), columns, primary_key, foreign_keys);

        if let Some(ref ref_data_dir) = self._data_dir {
            let search_path = format!("{}/search_{}", ref_data_dir, name);
            table.enable_search_index(&search_path);
        }

        // Auto-create PRIMARY index if PK is specified
        if let Some(ref pk_cols) = table.schema.primary_key.clone() {
            let expressions: Vec<Expression> = pk_cols
                .iter()
                .map(|c| Expression::Column(c.clone()))
                .collect();

            // Dummy evaluator for index creation on empty table
            struct DummyEvaluator;
            impl crate::sql::eval::Evaluator for DummyEvaluator {
                fn exec_select_internal<'a>(
                    &'a self,
                    _: Select,
                    _: &'a [(&'a Table, Option<&'a str>, &'a Row)],
                    _: &'a [Value],
                    _: &'a DatabaseState,
                ) -> futures::future::BoxFuture<
                    'a,
                    crate::sql::error::SqlResult<crate::sql::executor::QueryResult>,
                > {
                    unreachable!()
                }
            }

            table.create_index(
                &DummyEvaluator,
                "PRIMARY".to_string(),
                expressions,
                true,
                false,
                None,
                &self.state,
            )?;
        }

        self.state.tables.insert(name, table);
        self.save()
    }

    #[allow(dead_code)]
    pub fn drop_table(&mut self, name: &str) -> Result<(), StorageError> {
        self.state
            .tables
            .remove(name)
            .map(|_| ())
            .ok_or_else(|| StorageError::TableNotFound(name.to_string()))?;
        self.save()
    }

    #[allow(dead_code)]
    pub fn get_table(&self, name: &str) -> Option<&Table> {
        self.state.get_table(name)
    }

    #[allow(dead_code)]
    pub fn get_table_mut(&mut self, name: &str) -> Option<&mut Table> {
        self.state.get_table_mut(name)
    }

    #[allow(dead_code)]
    pub fn table_names(&self) -> Vec<&String> {
        self.state.table_names()
    }

    pub fn state(&self) -> &DatabaseState {
        &self.state
    }

    pub fn state_mut(&mut self) -> &mut DatabaseState {
        &mut self.state
    }

    pub fn set_state(&mut self, state: DatabaseState) -> Result<(), StorageError> {
        self.state = state;
        self.save()
    }

    // Direct mutation methods (already logged by Executor)
    #[allow(dead_code)]
    pub fn insert(
        &mut self,
        evaluator: &dyn Evaluator,
        table_name: &str,
        values: Vec<Value>,
        db_state: &DatabaseState,
    ) -> Result<String, StorageError> {
        let table = self
            .get_table_mut(table_name)
            .ok_or_else(|| StorageError::TableNotFound(table_name.to_string()))?;
        let id = table.insert(evaluator, values, db_state)?;
        self.save()?;
        Ok(id)
    }

    #[allow(dead_code)]
    pub fn update(
        &mut self,
        evaluator: &dyn Evaluator,
        table_name: &str,
        id: &str,
        values: Vec<Value>,
        db_state: &DatabaseState,
    ) -> Result<(), StorageError> {
        let table = self
            .get_table_mut(table_name)
            .ok_or_else(|| StorageError::TableNotFound(table_name.to_string()))?;
        table.update(evaluator, id, values, db_state)?;
        self.save()
    }

    #[allow(dead_code)]
    pub fn delete(
        &mut self,
        evaluator: &dyn Evaluator,
        table_name: &str,
        id: &str,
        db_state: &DatabaseState,
    ) -> Result<(), StorageError> {
        let table = self
            .get_table_mut(table_name)
            .ok_or_else(|| StorageError::TableNotFound(table_name.to_string()))?;
        table.delete(evaluator, id, db_state)?;
        self.save()
    }
}
