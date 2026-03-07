use super::table::Table;
use super::error::StorageError;
use std::collections::HashMap;

pub trait Persister: Send + Sync {
    fn save_tables(&self, tables: &HashMap<String, Table>) -> Result<(), StorageError>;
    fn load_tables(&self) -> Result<HashMap<String, Table>, StorageError>;
}

pub struct SledPersister {
    db: sled::Db,
}

impl SledPersister {
    pub fn new(path: &str) -> Result<Self, StorageError> {
        let db = sled::open(path).map_err(|e| StorageError::PersistenceError(e.to_string()))?;
        Ok(Self { db })
    }
}

impl Persister for SledPersister {
    fn save_tables(&self, tables: &HashMap<String, Table>) -> Result<(), StorageError> {
        // Clear existing tables metadata to avoid stale entries if tables were dropped
        self.db.clear().map_err(|e| StorageError::PersistenceError(e.to_string()))?;

        for (name, table) in tables {
            let serialized = serde_json::to_vec(table).map_err(|e| StorageError::PersistenceError(e.to_string()))?;
            self.db.insert(name.as_bytes(), serialized).map_err(|e| StorageError::PersistenceError(e.to_string()))?;
        }
        self.db.flush().map_err(|e| StorageError::PersistenceError(e.to_string()))?;
        Ok(())
    }

    fn load_tables(&self) -> Result<HashMap<String, Table>, StorageError> {
        let mut tables = HashMap::new();
        for item in self.db.iter() {
            let (key, value) = item.map_err(|e| StorageError::PersistenceError(e.to_string()))?;
            let name = String::from_utf8(key.to_vec()).map_err(|e| StorageError::PersistenceError(e.to_string()))?;
            let table: Table = serde_json::from_slice(&value).map_err(|e| StorageError::PersistenceError(e.to_string()))?;
            tables.insert(name, table);
        }
        Ok(tables)
    }
}
