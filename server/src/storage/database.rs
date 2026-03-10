use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use crate::sql::eval::Evaluator;
use super::error::StorageError;
use super::persistence::{Persister, WalRecord};
use super::row::{Column, Row};
use super::table::Table;
use super::value::Value;
use super::wal;

#[derive(Debug, Clone, Serialize, Deserialize, Default)]
pub struct DatabaseState {
    pub tables: HashMap<String, Table>,
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
        Self {
            state: DatabaseState::default(),
            persister: None,
            _data_dir: None,
        }
    }

    pub fn with_persister(
        persister: Box<dyn Persister>,
        data_dir: String,
    ) -> Result<Self, StorageError> {
        let tables = persister.load_tables().unwrap_or_default();
        let mut db = Self {
            state: DatabaseState { tables },
            persister: Some(persister),
            _data_dir: Some(data_dir.clone()),
        };

        // Replay WAL logs
        db.replay_logs()?;

        // Initialize search indices for each table (after WAL replay)
        for (name, table) in &mut db.state.tables {
            let search_path = format!("{}/search_{}", data_dir, name);
            table
                .setup_search_index(&search_path)
                .map_err(|e| StorageError::PersistenceError(e.to_string()))?;
        }

        Ok(db)
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

    pub fn create_table(&mut self, name: String, columns: Vec<Column>) -> Result<(), StorageError> {
        if self.state.tables.contains_key(&name) {
            return Err(StorageError::DuplicateKey(name));
        }

        let mut table = Table::new(name.clone(), columns);

        if let Some(ref data_dir) = self._data_dir {
            let search_path = format!("{}/search_{}", data_dir, name);
            table
                .setup_search_index(&search_path)
                .map_err(|e| StorageError::PersistenceError(e.to_string()))?;
        }

        self.state.tables.insert(name, table);
        self.save()
    }

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
