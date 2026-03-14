use crate::storage::Value;
use serde::{Deserialize, Serialize};
use std::collections::{BTreeMap, HashMap};

#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum TableIndex {
    BTree {
        unique: bool,
        expressions: Vec<serde_json::Value>, // Squeal::Expression as JSON
        where_clause: Option<serde_json::Value>, // Squeal::Condition as JSON
        data: BTreeMap<Vec<Value>, Vec<String>>, // key -> row_ids
    },
    Hash {
        unique: bool,
        expressions: Vec<serde_json::Value>,
        where_clause: Option<serde_json::Value>,
        data: HashMap<Vec<Value>, Vec<String>>,
    },
}

impl TableIndex {
    pub fn is_unique(&self) -> bool {
        match self {
            TableIndex::BTree { unique, .. } => *unique,
            TableIndex::Hash { unique, .. } => *unique,
        }
    }

    pub fn expressions(&self) -> Vec<crate::squeal::Expression> {
        let exprs = match self {
            TableIndex::BTree { expressions, .. } => expressions,
            TableIndex::Hash { expressions, .. } => expressions,
        };
        exprs
            .iter()
            .map(|v| serde_json::from_value(v.clone()).unwrap())
            .collect()
    }

    pub fn where_clause(&self) -> Option<crate::squeal::Condition> {
        let cond = match self {
            TableIndex::BTree { where_clause, .. } => where_clause,
            TableIndex::Hash { where_clause, .. } => where_clause,
        };
        cond.as_ref()
            .map(|v| serde_json::from_value(v.clone()).unwrap())
    }

    pub fn key_count(&self) -> usize {
        match self {
            TableIndex::BTree { data, .. } => data.len(),
            TableIndex::Hash { data, .. } => data.len(),
        }
    }

    pub fn total_rows(&self) -> usize {
        match self {
            TableIndex::BTree { data, .. } => data.values().map(|v| v.len()).sum(),
            TableIndex::Hash { data, .. } => data.values().map(|v| v.len()).sum(),
        }
    }

    pub fn get(&self, key: &[Value]) -> Option<Vec<String>> {
        match self {
            TableIndex::BTree { data, .. } => data.get(key).cloned(),
            TableIndex::Hash { data, .. } => data.get(key).cloned(),
        }
    }

    pub fn insert(
        &mut self,
        key: Vec<Value>,
        row_id: String,
    ) -> Result<(), super::error::StorageError> {
        let unique = self.is_unique();
        match self {
            TableIndex::BTree { data, .. } => {
                let entry = data.entry(key.clone()).or_default();
                if unique && !entry.is_empty() && !entry.contains(&row_id) {
                    return Err(super::error::StorageError::DuplicateKey(format!(
                        "{:?}",
                        key
                    )));
                }
                if !entry.contains(&row_id) {
                    entry.push(row_id);
                }
            }
            TableIndex::Hash { data, .. } => {
                let entry = data.entry(key.clone()).or_default();
                if unique && !entry.is_empty() && !entry.contains(&row_id) {
                    return Err(super::error::StorageError::DuplicateKey(format!(
                        "{:?}",
                        key
                    )));
                }
                if !entry.contains(&row_id) {
                    entry.push(row_id);
                }
            }
        }
        Ok(())
    }

    pub fn remove(&mut self, key: &[Value], row_id: &str) {
        match self {
            TableIndex::BTree { data, .. } => {
                if let Some(ids) = data.get_mut(key) {
                    ids.retain(|id| id != row_id);
                    if ids.is_empty() {
                        data.remove(key);
                    }
                }
            }
            TableIndex::Hash { data, .. } => {
                if let Some(ids) = data.get_mut(key) {
                    ids.retain(|id| id != row_id);
                    if ids.is_empty() {
                        data.remove(key);
                    }
                }
            }
        }
    }
}
