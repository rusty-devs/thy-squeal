use chrono::{NaiveDate, NaiveDateTime};
use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use thiserror::Error;
use uuid::Uuid;

#[derive(Error, Debug)]
pub enum StorageError {
    #[error("Table not found: {0}")]
    TableNotFound(String),
    #[error("Row not found: {0}")]
    RowNotFound(String),
    #[error("Column not found: {0}")]
    ColumnNotFound(String),
    #[error("Invalid type: {0}")]
    InvalidType(String),
    #[error("Duplicate key: {0}")]
    DuplicateKey(String),
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub enum DataType {
    Int,
    Float,
    Bool,
    Date,
    DateTime,
    VarChar,
    Text,
    Blob,
    Json,
}

impl DataType {
    pub fn from_str(s: &str) -> Self {
        match s.to_uppercase().as_str() {
            "INT" | "INTEGER" => DataType::Int,
            "FLOAT" | "DOUBLE" | "REAL" => DataType::Float,
            "BOOL" | "BOOLEAN" => DataType::Bool,
            "DATE" => DataType::Date,
            "DATETIME" => DataType::DateTime,
            "VARCHAR" | "TEXT" | "STRING" => DataType::Text,
            "BLOB" | "BINARY" => DataType::Blob,
            "JSON" | "JSONB" => DataType::Json,
            _ => DataType::Text,
        }
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum Value {
    Null,
    Int(i64),
    Float(f64),
    Bool(bool),
    Date(NaiveDate),
    DateTime(NaiveDateTime),
    Text(String),
    Blob(Vec<u8>),
    Json(serde_json::Value),
}

impl Value {
    pub fn as_int(&self) -> Option<i64> {
        match self {
            Value::Int(i) => Some(*i),
            _ => None,
        }
    }

    pub fn as_float(&self) -> Option<f64> {
        match self {
            Value::Float(f) => Some(*f),
            _ => None,
        }
    }

    pub fn as_bool(&self) -> Option<bool> {
        match self {
            Value::Bool(b) => Some(*b),
            _ => None,
        }
    }

    pub fn as_text(&self) -> Option<&str> {
        match self {
            Value::Text(s) => Some(s),
            _ => None,
        }
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Column {
    pub name: String,
    pub data_type: DataType,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Table {
    pub name: String,
    pub columns: Vec<Column>,
    pub rows: Vec<Row>,
}

impl Table {
    pub fn new(name: String, columns: Vec<Column>) -> Self {
        Self {
            name,
            columns,
            rows: Vec::new(),
        }
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
        // TODO: implement where clause filtering
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

        if let Some(row) = self.rows.iter_mut().find(|r| r.id == id) {
            row.values = values;
            Ok(())
        } else {
            Err(StorageError::RowNotFound(id.to_string()))
        }
    }

    pub fn delete(&mut self, id: &str) -> Result<(), StorageError> {
        if let Some(pos) = self.rows.iter().position(|r| r.id == id) {
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
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Row {
    pub id: String,
    pub values: Vec<Value>,
}

#[derive(Debug, Clone, Serialize, Deserialize, Default)]
pub struct Database {
    tables: HashMap<String, Table>,
}

impl Database {
    pub fn new() -> Self {
        Self {
            tables: HashMap::new(),
        }
    }

    pub fn create_table(&mut self, name: String, columns: Vec<Column>) -> Result<(), StorageError> {
        if self.tables.contains_key(&name) {
            return Err(StorageError::DuplicateKey(name));
        }
        self.tables.insert(name.clone(), Table::new(name, columns));
        Ok(())
    }

    pub fn drop_table(&mut self, name: &str) -> Result<(), StorageError> {
        self.tables
            .remove(name)
            .map(|_| ())
            .ok_or_else(|| StorageError::TableNotFound(name.to_string()))
    }

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
