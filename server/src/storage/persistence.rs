use super::error::StorageError;
use super::row::{Column, ForeignKey};
use super::table::Table;
use super::value::Value;
use crate::squeal;
use serde::{Deserialize, Serialize};
use std::collections::{HashMap, HashSet};

#[allow(clippy::type_complexity)]
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct SnapshotData {
    pub tables: HashMap<String, Table>,
    pub kv: HashMap<String, Value>,
    pub kv_expiry: HashMap<String, u64>,
    pub kv_hash: HashMap<String, HashMap<String, Value>>,
    pub kv_list: HashMap<String, Vec<Value>>,
    pub kv_set: HashMap<String, HashSet<String>>,
    pub kv_zset: HashMap<String, Vec<(f64, String)>>,
    pub kv_stream: HashMap<String, Vec<(u64, HashMap<String, Value>)>>,
    pub kv_stream_last_id: HashMap<String, u64>,
}

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
    KvSet {
        tx_id: Option<String>,
        key: String,
        value: Value,
    },
    KvDelete {
        tx_id: Option<String>,
        key: String,
    },
    KvExpire {
        tx_id: Option<String>,
        key: String,
        expiry: u64,
    },
    KvHashSet {
        tx_id: Option<String>,
        key: String,
        field: String,
        value: Value,
    },
    KvHashDelete {
        tx_id: Option<String>,
        key: String,
        fields: Vec<String>,
    },
    KvListPush {
        tx_id: Option<String>,
        key: String,
        values: Vec<Value>,
        left: bool,
    },
    KvSetAdd {
        tx_id: Option<String>,
        key: String,
        members: Vec<String>,
    },
    KvSetRemove {
        tx_id: Option<String>,
        key: String,
        members: Vec<String>,
    },
    KvZSetAdd {
        tx_id: Option<String>,
        key: String,
        members: Vec<(f64, String)>,
    },
    KvZSetRemove {
        tx_id: Option<String>,
        key: String,
        members: Vec<String>,
    },
    KvStreamAdd {
        tx_id: Option<String>,
        key: String,
        id: u64,
        fields: HashMap<String, Value>,
    },
}

pub trait Persister: Send + Sync {
    fn save_snapshot(&self, state: &SnapshotData) -> Result<(), StorageError>;
    fn load_snapshot(&self) -> Result<Option<SnapshotData>, StorageError>;
    #[allow(dead_code)]
    fn save_tables(&self, tables: &HashMap<String, Table>) -> Result<(), StorageError>;
    #[allow(dead_code)]
    fn load_tables(&self) -> Result<HashMap<String, Table>, StorageError>;

    // WAL support
    fn log_operation(&self, record: &WalRecord) -> Result<(), StorageError>;
    fn load_logs(&self) -> Result<Vec<WalRecord>, StorageError>;
    fn clear_logs(&self) -> Result<(), StorageError>;
}

pub struct SledPersister {
    db: sled::Db,
    wal: sled::Tree,
    snapshot: sled::Tree,
}

impl SledPersister {
    pub fn new(path: &str) -> Result<Self, StorageError> {
        let db = sled::open(path).map_err(|e| StorageError::PersistenceError(e.to_string()))?;
        let wal = db
            .open_tree("wal")
            .map_err(|e| StorageError::PersistenceError(e.to_string()))?;
        let snapshot = db
            .open_tree("snapshot")
            .map_err(|e| StorageError::PersistenceError(e.to_string()))?;
        Ok(Self { db, wal, snapshot })
    }
}

impl Persister for SledPersister {
    fn save_snapshot(&self, state: &SnapshotData) -> Result<(), StorageError> {
        let serialized =
            serde_json::to_vec(state).map_err(|e| StorageError::PersistenceError(e.to_string()))?;
        self.snapshot
            .insert(b"data", serialized.as_slice())
            .map_err(|e| StorageError::PersistenceError(e.to_string()))?;
        self.snapshot
            .flush()
            .map_err(|e| StorageError::PersistenceError(e.to_string()))?;

        // After successful snapshot, we can clear the WAL
        self.clear_logs()?;

        Ok(())
    }

    fn load_snapshot(&self) -> Result<Option<SnapshotData>, StorageError> {
        if let Some(data) = self
            .snapshot
            .get(b"data")
            .map_err(|e| StorageError::PersistenceError(e.to_string()))?
        {
            let state: SnapshotData = serde_json::from_slice(&data)
                .map_err(|e| StorageError::PersistenceError(e.to_string()))?;
            Ok(Some(state))
        } else {
            Ok(None)
        }
    }

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
