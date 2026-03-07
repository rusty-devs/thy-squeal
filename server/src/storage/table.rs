use serde::{Deserialize, Serialize};
use uuid::Uuid;
use std::collections::{BTreeMap, HashMap};
use std::sync::{Arc, Mutex};
use super::error::StorageError;
use super::types::DataType;
use super::value::Value;
use super::search::SearchIndex;

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Column {
    pub name: String,
    pub data_type: DataType,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct RowId(pub String);

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Row {
    pub id: String,
    pub values: Vec<Value>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum TableIndex {
    BTree {
        unique: bool,
        columns: Vec<String>,
        // Multi-column mapping: Vec<Value> is the key
        data: BTreeMap<Vec<Value>, Vec<String>>,
    },
    Hash {
        unique: bool,
        columns: Vec<String>,
        data: HashMap<Vec<Value>, Vec<String>>,
    },
}

impl TableIndex {
    pub fn columns(&self) -> &[String] {
        match self {
            TableIndex::BTree { columns, .. } => columns,
            TableIndex::Hash { columns, .. } => columns,
        }
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

pub struct Table {
    pub name: String,
    pub columns: Vec<Column>,
    pub rows: Vec<Row>,
    pub indexes: HashMap<String, TableIndex>, // index_name -> TableIndex
    pub search_index: Option<Arc<Mutex<SearchIndex>>>,
}

#[derive(Serialize, Deserialize)]
struct TableSerde {
    name: String,
    columns: Vec<Column>,
    rows: Vec<Row>,
    indexes: HashMap<String, TableIndex>,
}

impl From<TableSerde> for Table {
    fn from(s: TableSerde) -> Self {
        Self {
            name: s.name,
            columns: s.columns,
            rows: s.rows,
            indexes: s.indexes,
            search_index: None,
        }
    }
}

impl From<&Table> for TableSerde {
    fn from(t: &Table) -> Self {
        Self {
            name: t.name.clone(),
            columns: t.columns.clone(),
            rows: t.rows.clone(),
            indexes: t.indexes.clone(),
        }
    }
}

impl Serialize for Table {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: serde::Serializer,
    {
        TableSerde::from(self).serialize(serializer)
    }
}

impl<'de> Deserialize<'de> for Table {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: serde::Deserializer<'de>,
    {
        TableSerde::deserialize(deserializer).map(Table::from)
    }
}

impl Clone for Table {
    fn clone(&self) -> Self {
        Self {
            name: self.name.clone(),
            columns: self.columns.clone(),
            rows: self.rows.clone(),
            indexes: self.indexes.clone(),
            search_index: self.search_index.clone(),
        }
    }
}

impl std::fmt::Debug for Table {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("Table")
            .field("name", &self.name)
            .field("columns", &self.columns)
            .field("rows", &self.rows)
            .field("indexes", &self.indexes)
            .finish()
    }
}

impl Table {
    pub fn new(name: String, columns: Vec<Column>) -> Self {
        Self {
            name,
            columns,
            rows: Vec::new(),
            indexes: HashMap::new(),
            search_index: None,
        }
    }

    pub fn create_index(&mut self, name: String, columns: Vec<String>, unique: bool, use_hash: bool) -> Result<(), StorageError> {
        // Validate columns/paths
        for path in &columns {
            let base_col = path.split('.').next().unwrap();
            if self.column_index(base_col).is_none() {
                return Err(StorageError::ColumnNotFound(base_col.to_string()));
            }
        }

        let mut index = if use_hash {
            TableIndex::Hash { unique, columns: columns.clone(), data: HashMap::new() }
        } else {
            TableIndex::BTree { unique, columns: columns.clone(), data: BTreeMap::new() }
        };

        // Populate existing data
        for row in &self.rows {
            let key = self.extract_key(row, &columns)?;
            index.insert(key, row.id.clone())?;
        }

        self.indexes.insert(name, index);
        Ok(())
    }

    fn extract_key(&self, row: &Row, columns: &[String]) -> Result<Vec<Value>, StorageError> {
        self.extract_key_from_values(&row.values, columns)
    }

    fn extract_key_from_values(&self, values: &[Value], columns: &[String]) -> Result<Vec<Value>, StorageError> {
        let mut key = Vec::with_capacity(columns.len());
        for path in columns {
            let mut parts = path.split('.');
            let base_col_name = parts.next().unwrap();
            let base_idx = self.column_index(base_col_name).ok_or_else(|| StorageError::ColumnNotFound(base_col_name.to_string()))?;
            let mut current_val = values.get(base_idx).cloned().unwrap_or(Value::Null);

            // Traverse JSON path if any
            for part in parts {
                current_val = match current_val {
                    Value::Json(v) => {
                        if let Some(inner) = v.get(part) {
                            Value::from_json(inner.clone())
                        } else {
                            Value::Null
                        }
                    }
                    _ => Value::Null,
                };
                if current_val == Value::Null { break; }
            }
            key.push(current_val);
        }
        Ok(key)
    }

    pub fn setup_search_index(&mut self, path: &str) -> anyhow::Result<()> {
        let text_fields: Vec<String> = self.columns.iter()
            .filter(|c| c.data_type == DataType::Text || c.data_type == DataType::VarChar)
            .map(|c| c.name.clone())
            .collect();
        
        if !text_fields.is_empty() {
            let index = SearchIndex::new(path, &text_fields)?;
            self.search_index = Some(Arc::new(Mutex::new(index)));
            
            // Populate existing data
            let rows_to_index = self.rows.clone();
            for row in rows_to_index {
                self.index_row(&row)?;
            }
        }
        Ok(())
    }

    fn index_row(&self, row: &Row) -> anyhow::Result<()> {
        if let Some(ref search_index) = self.search_index {
            let mut field_values = Vec::new();
            for (i, col) in self.columns.iter().enumerate() {
                if col.data_type == DataType::Text || col.data_type == DataType::VarChar {
                    if let Some(val) = row.values.get(i).and_then(|v| v.as_text()) {
                        field_values.push((col.name.clone(), val.to_string()));
                    }
                }
            }
            search_index.lock().unwrap().add_document(&row.id, &field_values)?;
        }
        Ok(())
    }

    pub fn insert(&mut self, values: Vec<Value>) -> Result<String, StorageError> {
        if values.len() != self.columns.len() {
            return Err(StorageError::InvalidType(format!(
                "Expected {} columns, got {}",
                self.columns.len(),
                values.len()
            )));
        }

        // 1. Check unique constraints/indexes before inserting
        for index in self.indexes.values() {
            if index.is_unique() {
                let key = self.extract_key_from_values(&values, index.columns())?;
                if index.get(&key).map_or(false, |ids| !ids.is_empty()) {
                    return Err(StorageError::DuplicateKey(format!("{:?}", key)));
                }
            }
        }

        let id = Uuid::new_v4().to_string();
        
        // 2. Update all indexes
        let mut index_data = Vec::new();
        for index in self.indexes.values() {
            index_data.push(self.extract_key_from_values(&values, index.columns())?);
        }

        for (index, key) in self.indexes.values_mut().zip(index_data) {
            index.insert(key, id.clone())?;
        }

        let row = Row {
            id: id.clone(),
            values,
        };
        
        // 3. Update Search Index
        if let Err(e) = self.index_row(&row) {
            return Err(StorageError::PersistenceError(format!("Search index error: {}", e)));
        }

        self.rows.push(row);
        Ok(id)
    }

    pub fn update(&mut self, id: &str, values: Vec<Value>) -> Result<(), StorageError> {
        if values.len() != self.columns.len() {
            return Err(StorageError::InvalidType(format!(
                "Expected {} columns, got {}",
                self.columns.len(),
                values.len()
            )));
        }

        if let Some(pos) = self.rows.iter().position(|r| r.id == id) {
            let old_values = self.rows[pos].values.clone();
            
            // 1. Check unique constraints
            for index in self.indexes.values() {
                if index.is_unique() {
                    let old_key = self.extract_key_from_values(&old_values, index.columns())?;
                    let new_key = self.extract_key_from_values(&values, index.columns())?;
                    
                    if old_key != new_key && index.get(&new_key).is_some() {
                        return Err(StorageError::DuplicateKey(format!("{:?}", new_key)));
                    }
                }
            }

            // 2. Update all indexes
            let mut keys = Vec::new();
            for index in self.indexes.values() {
                let old_key = self.extract_key_from_values(&old_values, index.columns())?;
                let new_key = self.extract_key_from_values(&values, index.columns())?;
                keys.push((old_key, new_key));
            }

            for (index, (old_key, new_key)) in self.indexes.values_mut().zip(keys) {
                if old_key != new_key {
                    index.remove(&old_key, id);
                    index.insert(new_key, id.to_string())?;
                }
            }

            self.rows[pos].values = values;
            
            // 3. Update Search Index
            let updated_row = self.rows[pos].clone();
            if let Err(e) = self.index_row(&updated_row) {
                return Err(StorageError::PersistenceError(format!("Search index error: {}", e)));
            }

            Ok(())
        } else {
            Err(StorageError::RowNotFound(id.to_string()))
        }
    }

    pub fn delete(&mut self, id: &str) -> Result<(), StorageError> {
        if let Some(pos) = self.rows.iter().position(|r| r.id == id) {
            let values = self.rows[pos].values.clone();

            // 1. Update all indexes
            let mut keys = Vec::new();
            for index in self.indexes.values() {
                keys.push(self.extract_key_from_values(&values, index.columns())?);
            }

            for (index, key) in self.indexes.values_mut().zip(keys) {
                index.remove(&key, id);
            }

            // 2. Remove from Search Index
            if let Some(ref search_index) = self.search_index {
                if let Err(e) = search_index.lock().unwrap().delete_document(id) {
                    return Err(StorageError::PersistenceError(format!("Search index error: {}", e)));
                }
            }

            self.rows.remove(pos);
            Ok(())
        } else {
            Err(StorageError::RowNotFound(id.to_string()))
        }
    }

    pub fn get(&self, id: &str) -> Option<&Row> {
        self.rows.iter().find(|r| r.id == id)
    }

    pub fn column_index(&self, name: &str) -> Option<usize> {
        self.columns.iter().position(|c| c.name == name)
    }

    pub fn null_row(&self) -> Row {
        Row {
            id: "null".to_string(),
            values: vec![Value::Null; self.columns.len()],
        }
    }
}
