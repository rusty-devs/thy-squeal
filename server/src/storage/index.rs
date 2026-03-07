use crate::sql::ast::{Condition, Expression};
use crate::storage::value::Value;
use crate::storage::error::StorageError;
use serde::{Deserialize, Serialize};
use std::collections::{BTreeMap, HashMap};

#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum TableIndex {
    BTree {
        unique: bool,
        expressions: Vec<serde_json::Value>,    // Serialized Expressions
        where_clause: Option<serde_json::Value>, // Serialized Condition
        data: BTreeMap<Vec<Value>, Vec<String>>,
    },
    Hash {
        unique: bool,
        expressions: Vec<serde_json::Value>,
        where_clause: Option<serde_json::Value>,
        data: HashMap<Vec<Value>, Vec<String>>,
    },
}

impl TableIndex {
    pub fn expressions(&self) -> Vec<Expression> {
        let expr_jsons = match self {
            TableIndex::BTree { expressions, .. } => expressions,
            TableIndex::Hash { expressions, .. } => expressions,
        };
        expr_jsons
            .iter()
            .map(|j| serde_json::from_value(j.clone()).unwrap())
            .collect()
    }

    pub fn where_clause(&self) -> Option<Condition> {
        let where_json = match self {
            TableIndex::BTree { where_clause, .. } => where_clause,
            TableIndex::Hash { where_clause, .. } => where_clause,
        };
        where_json
            .as_ref()
            .and_then(|j| serde_json::from_value(j.clone()).ok())
    }

    pub fn is_unique(&self) -> bool {
        match self {
            TableIndex::BTree { unique, .. } => *unique,
            TableIndex::Hash { unique, .. } => *unique,
        }
    }

    pub fn insert(&mut self, key: Vec<Value>, row_id: String) -> Result<(), StorageError> {
        match self {
            TableIndex::BTree { unique, data, .. } => {
                if *unique && data.contains_key(&key) {
                    return Err(StorageError::DuplicateKey(format!("{:?}", key)));
                }
                data.entry(key).or_default().push(row_id);
            }
            TableIndex::Hash { unique, data, .. } => {
                if *unique && data.contains_key(&key) {
                    return Err(StorageError::DuplicateKey(format!("{:?}", key)));
                }
                data.entry(key).or_default().push(row_id);
            }
        }
        Ok(())
    }

    pub fn remove(&mut self, key: &Vec<Value>, row_id: &str) {
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

    pub fn get(&self, key: &Vec<Value>) -> Option<&Vec<String>> {
        match self {
            TableIndex::BTree { data, .. } => data.get(key),
            TableIndex::Hash { data, .. } => data.get(key),
        }
    }
}
