use serde::{Deserialize, Serialize};
use uuid::Uuid;
use std::collections::{BTreeMap, HashMap};
use super::error::StorageError;
use super::types::DataType;
use super::value::Value;

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Column {
    pub name: String,
    pub data_type: DataType,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Row {
    pub id: String,
    pub values: Vec<Value>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Table {
    pub name: String,
    pub columns: Vec<Column>,
    pub rows: Vec<Row>,
    #[serde(default)]
    pub indexes: HashMap<String, BTreeMap<Value, Vec<String>>>, // column_name -> { value -> [row_ids] }
}

impl Table {
    pub fn new(name: String, columns: Vec<Column>) -> Self {
        Self {
            name,
            columns,
            rows: Vec::new(),
            indexes: HashMap::new(),
        }
    }

    pub fn create_index(&mut self, column_name: &str) -> Result<(), StorageError> {
        let col_idx = self.column_index(column_name)
            .ok_or_else(|| StorageError::ColumnNotFound(column_name.to_string()))?;

        let mut index = BTreeMap::new();
        for row in &self.rows {
            let val = row.values.get(col_idx).cloned().unwrap_or(Value::Null);
            index.entry(val).or_insert_with(Vec::new).push(row.id.clone());
        }

        self.indexes.insert(column_name.to_string(), index);
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

        let id = Uuid::new_v4().to_string();
        
        // Use a temporary list of (col_idx, value) to avoid borrow checker issues
        let mut index_updates = Vec::new();
        for col_name in self.indexes.keys() {
            if let Some(col_idx) = self.column_index(col_name) {
                let val = values.get(col_idx).cloned().unwrap_or(Value::Null);
                index_updates.push((col_name.clone(), val));
            }
        }

        for (col_name, val) in index_updates {
            if let Some(index) = self.indexes.get_mut(&col_name) {
                index.entry(val).or_insert_with(Vec::new).push(id.clone());
            }
        }

        let row = Row {
            id: id.clone(),
            values,
        };
        self.rows.push(row);
        Ok(id)
    }

    pub fn select(&self) -> Vec<&Row> {
        self.rows.iter().collect()
    }

    pub fn select_where(&self, _condition: &str) -> Vec<&Row> {
        self.rows.iter().collect()
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
            
            let mut index_updates = Vec::new();
            for col_name in self.indexes.keys() {
                if let Some(col_idx) = self.column_index(col_name) {
                    let old_val = old_values.get(col_idx).cloned().unwrap_or(Value::Null);
                    let new_val = values.get(col_idx).cloned().unwrap_or(Value::Null);
                    index_updates.push((col_name.clone(), old_val, new_val));
                }
            }

            for (col_name, old_val, new_val) in index_updates {
                if let Some(index) = self.indexes.get_mut(&col_name) {
                    if let Some(ids) = index.get_mut(&old_val) {
                        ids.retain(|row_id| row_id != id);
                    }
                    index.entry(new_val).or_insert_with(Vec::new).push(id.to_string());
                }
            }

            self.rows[pos].values = values;
            Ok(())
        } else {
            Err(StorageError::RowNotFound(id.to_string()))
        }
    }

    pub fn delete(&mut self, id: &str) -> Result<(), StorageError> {
        if let Some(pos) = self.rows.iter().position(|r| r.id == id) {
            let old_values = self.rows[pos].values.clone();

            let mut index_updates = Vec::new();
            for col_name in self.indexes.keys() {
                if let Some(col_idx) = self.column_index(col_name) {
                    let old_val = old_values.get(col_idx).cloned().unwrap_or(Value::Null);
                    index_updates.push((col_name.clone(), old_val));
                }
            }

            for (col_name, old_val) in index_updates {
                if let Some(index) = self.indexes.get_mut(&col_name) {
                    if let Some(ids) = index.get_mut(&old_val) {
                        ids.retain(|row_id| row_id != id);
                    }
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
