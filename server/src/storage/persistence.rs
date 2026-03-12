use super::error::StorageError;
use super::row::{Column, ForeignKey};
use super::table::Table;
use super::value::Value;
use crate::sql::squeal;
use serde::{Deserialize, Serialize};
use std::collections::HashMap;

#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum WalRecord {
    Begin {
        tx_id: String,
    },
    Commit {
        tx_id: String,
    },
    Rollback {
        tx_id: String,
    },
    CreateTable {
        tx_id: Option<String>,
        name: String,
        columns: Vec<Column>,
        primary_key: Option<Vec<String>>,
        foreign_keys: Vec<ForeignKey>,
    },
    CreateMaterializedView {
        tx_id: Option<String>,
        name: String,
        query: Box<squeal::Select>,
    },
    AlterTable {
        tx_id: Option<String>,
        table: String,
        action: squeal::AlterAction,
    },
    DropTable {
        tx_id: Option<String>,
        name: String,
    },
    Insert {
        tx_id: Option<String>,
        table: String,
        values: Vec<Value>,
    },
    Update {
        tx_id: Option<String>,
        table: String,
        id: String,
        values: Vec<Value>,
    },
    Delete {
        tx_id: Option<String>,
        table: String,
        id: String,
    },
    CreateIndex {
        tx_id: Option<String>,
        table: String,
        name: String,
        expressions: Vec<squeal::Expression>,
        unique: bool,
        use_hash: bool,
        where_clause: Option<squeal::Condition>,
    },
}

pub trait Persister: Send + Sync {
    fn save_tables(&self, tables: &HashMap<String, Table>) -> Result<(), StorageError>;
    fn load_tables(&self) -> Result<HashMap<String, Table>, StorageError>;

    // WAL support
    fn log_operation(&self, record: &WalRecord) -> Result<(), StorageError>;
    fn load_logs(&self) -> Result<Vec<WalRecord>, StorageError>;
    fn clear_logs(&self) -> Result<(), StorageError>;
}

pub struct SledPersister {
    db: sled::Db,
    wal: sled::Tree,
}

impl SledPersister {
    pub fn new(path: &str) -> Result<Self, StorageError> {
        let db = sled::open(path).map_err(|e| StorageError::PersistenceError(e.to_string()))?;
        let wal = db
            .open_tree("wal")
            .map_err(|e| StorageError::PersistenceError(e.to_string()))?;
        Ok(Self { db, wal })
    }
}

impl Persister for SledPersister {
    fn save_tables(&self, tables: &HashMap<String, Table>) -> Result<(), StorageError> {
        // Clear existing tables metadata
        for item in self.db.iter() {
            let (key, _) = item.map_err(|e| StorageError::PersistenceError(e.to_string()))?;
            self.db
                .remove(key)
                .map_err(|e| StorageError::PersistenceError(e.to_string()))?;
        }

        for (name, table) in tables {
            let serialized = serde_json::to_vec(table)
                .map_err(|e| StorageError::PersistenceError(e.to_string()))?;
            self.db
                .insert(name.as_bytes(), serialized)
                .map_err(|e| StorageError::PersistenceError(e.to_string()))?;
        }
        self.db
            .flush()
            .map_err(|e| StorageError::PersistenceError(e.to_string()))?;

        // After successful snapshot, we can clear the WAL
        self.clear_logs()?;

        Ok(())
    }

    fn load_tables(&self) -> Result<HashMap<String, Table>, StorageError> {
        let mut tables = HashMap::new();
        for item in self.db.iter() {
            let (key, value) = item.map_err(|e| StorageError::PersistenceError(e.to_string()))?;
            let name = String::from_utf8(key.to_vec())
                .map_err(|e| StorageError::PersistenceError(e.to_string()))?;
            let table: Table = serde_json::from_slice(&value)
                .map_err(|e| StorageError::PersistenceError(e.to_string()))?;
            tables.insert(name, table);
        }
        Ok(tables)
    }

    fn log_operation(&self, record: &WalRecord) -> Result<(), StorageError> {
        let serialized = serde_json::to_vec(record)
            .map_err(|e| StorageError::PersistenceError(e.to_string()))?;

        let id = self
            .db
            .generate_id()
            .map_err(|e| StorageError::PersistenceError(e.to_string()))?;
        let key = id.to_be_bytes();

        self.wal
            .insert(key, serialized)
            .map_err(|e| StorageError::PersistenceError(e.to_string()))?;
        self.wal
            .flush()
            .map_err(|e| StorageError::PersistenceError(e.to_string()))?;
        Ok(())
    }

    fn load_logs(&self) -> Result<Vec<WalRecord>, StorageError> {
        let mut logs = Vec::new();
        for item in self.wal.iter() {
            let (_, value) = item.map_err(|e| StorageError::PersistenceError(e.to_string()))?;
            let record: WalRecord = serde_json::from_slice(&value)
                .map_err(|e| StorageError::PersistenceError(e.to_string()))?;
            logs.push(record);
        }
        Ok(logs)
    }

    fn clear_logs(&self) -> Result<(), StorageError> {
        self.wal
            .clear()
            .map_err(|e| StorageError::PersistenceError(e.to_string()))?;
        self.wal
            .flush()
            .map_err(|e| StorageError::PersistenceError(e.to_string()))?;
        Ok(())
    }
}
