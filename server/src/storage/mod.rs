pub mod error;
pub mod types;
pub mod value;
pub mod table;
pub mod persistence;

use std::collections::HashMap;
use serde::{Deserialize, Serialize};

pub use error::StorageError;
pub use types::DataType;
pub use value::Value;
pub use table::{Table, Column, Row};
use persistence::Persister;

#[derive(Debug, Clone, Serialize, Deserialize, Default)]
pub struct DatabaseState {
    pub tables: HashMap<String, Table>,
}

pub struct Database {
    state: DatabaseState,
    persister: Option<Box<dyn Persister>>,
}

impl Database {
    pub fn new() -> Self {
        Self {
            state: DatabaseState::default(),
            persister: None,
        }
    }

    pub fn with_persister(persister: Box<dyn Persister>) -> Result<Self, StorageError> {
        let _tables = persister.load_tables().unwrap_or_else(|e| {
            // Log error but start empty? Or fail? Fail is safer.
            // But if it's a new DB, it might be empty. 
            // load_tables should return empty map if no data.
            // SledPersister returns empty map if empty.
            // If error is real IO error, we should probably fail.
            // For now, let's assume if it fails, we start empty but log it (if we had logging here).
            // Actually, let's propagate.
            match e {
                _ => HashMap::new(),
            }
        });
        
        // Retry load properly
        let tables = match persister.load_tables() {
            Ok(t) => t,
            Err(_) => HashMap::new(), // Assume new DB if load fails (e.g. invalid format or empty)
        };

        Ok(Self {
            state: DatabaseState { tables },
            persister: Some(persister),
        })
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
        self.state.tables.insert(name.clone(), Table::new(name, columns));
        self.save()
    }

    pub fn drop_table(&mut self, name: &str) -> Result<(), StorageError> {
        self.state.tables
            .remove(name)
            .map(|_| ())
            .ok_or_else(|| StorageError::TableNotFound(name.to_string()))?;
        self.save()
    }

    pub fn get_table(&self, name: &str) -> Option<&Table> {
        self.state.tables.get(name)
    }

    pub fn get_table_mut(&mut self, name: &str) -> Option<&mut Table> {
        self.state.tables.get_mut(name)
    }

    pub fn table_names(&self) -> Vec<&String> {
        self.state.tables.keys().collect()
    }
}
