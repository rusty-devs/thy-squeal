pub mod index;
pub mod mutation;
pub mod search;

use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use std::sync::{Arc, Mutex};

use super::error::StorageError;
use super::index::TableIndex;
use super::row::{Column, Row};
use super::search::SearchIndex;
use super::value::Value;

/// Schema definitions for a table (metadata, columns, constraints)
#[derive(Serialize, Deserialize, Clone, Debug)]
pub struct TableSchema {
    pub name: String,
    pub columns: Vec<Column>,
    pub primary_key: Option<Vec<String>>,
    pub foreign_keys: Vec<crate::sql::ast::ForeignKey>,
}

/// Data storage for a table (rows, auto-increment state)
#[derive(Serialize, Deserialize, Clone, Default)]
pub struct TableData {
    pub rows: Vec<Row>,
    pub auto_inc_counters: HashMap<usize, u64>, // col_idx -> next_val
}

/// Secondary and search indexes for a table
pub struct TableIndexes {
    pub secondary: HashMap<String, TableIndex>, // index_name -> TableIndex
    pub search: Option<Arc<Mutex<SearchIndex>>>,
}

#[derive(Serialize, Deserialize, Clone)]
struct TableIndexesSerde {
    secondary: HashMap<String, TableIndex>,
}

pub struct Table {
    pub schema: TableSchema,
    pub data: TableData,
    pub indexes: TableIndexes,
}

#[derive(Serialize, Deserialize)]
struct TableSerde {
    schema: TableSchema,
    data: TableData,
    indexes: TableIndexesSerde,
}

impl From<TableSerde> for Table {
    fn from(s: TableSerde) -> Self {
        Self {
            schema: s.schema,
            data: s.data,
            indexes: TableIndexes {
                secondary: s.indexes.secondary,
                search: None,
            },
        }
    }
}

impl From<&Table> for TableSerde {
    fn from(t: &Table) -> Self {
        Self {
            schema: t.schema.clone(),
            data: t.data.clone(),
            indexes: TableIndexesSerde {
                secondary: t.indexes.secondary.clone(),
            },
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
            schema: self.schema.clone(),
            data: self.data.clone(),
            indexes: TableIndexes {
                secondary: self.indexes.secondary.clone(),
                search: self.indexes.search.clone(),
            },
        }
    }
}

impl std::fmt::Debug for Table {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("Table")
            .field("schema", &self.schema)
            .field("rows_count", &self.data.rows.len())
            .field("indexes_count", &self.indexes.secondary.len())
            .finish()
    }
}

impl Table {
    pub fn new(
        name: String,
        columns: Vec<Column>,
        primary_key: Option<Vec<String>>,
        foreign_keys: Vec<crate::sql::ast::ForeignKey>,
    ) -> Self {
        let mut auto_inc_counters = HashMap::new();
        for (i, col) in columns.iter().enumerate() {
            if col.is_auto_increment {
                auto_inc_counters.insert(i, 1);
            }
        }

        Self {
            schema: TableSchema {
                name,
                columns,
                primary_key,
                foreign_keys,
            },
            data: TableData {
                rows: Vec::new(),
                auto_inc_counters,
            },
            indexes: TableIndexes {
                secondary: HashMap::new(),
                search: None,
            },
        }
    }

    // Proxy methods for common access
    pub fn name(&self) -> &str {
        &self.schema.name
    }
    pub fn columns(&self) -> &[Column] {
        &self.schema.columns
    }
    pub fn rows(&self) -> &[Row] {
        &self.data.rows
    }

    pub fn generate_auto_inc(&mut self, col_idx: usize) -> Option<u64> {
        if let Some(counter) = self.data.auto_inc_counters.get_mut(&col_idx) {
            let val = *counter;
            *counter += 1;
            Some(val)
        } else {
            None
        }
    }

    pub fn add_column(&mut self, column: Column) -> Result<(), StorageError> {
        if self.column_index(&column.name).is_some() {
            return Err(StorageError::PersistenceError(format!(
                "Column {} already exists in table {}",
                column.name, self.schema.name
            )));
        }

        let new_idx = self.schema.columns.len();
        if column.is_auto_increment {
            self.data.auto_inc_counters.insert(new_idx, 1);
        }

        self.schema.columns.push(column);

        // Update existing rows with NULL
        for row in &mut self.data.rows {
            row.values.push(Value::Null);
        }

        Ok(())
    }

    pub fn drop_column(&mut self, name: &str) -> Result<(), StorageError> {
        let idx = self.column_index(name).ok_or_else(|| {
            StorageError::ColumnNotFound(format!("{}.{}", self.schema.name, name))
        })?;

        self.schema.columns.remove(idx);

        // Update existing rows
        for row in &mut self.data.rows {
            row.values.remove(idx);
        }

        // Rebuild auto_inc_counters indices
        let mut new_counters = HashMap::new();
        for (old_idx, val) in &self.data.auto_inc_counters {
            if *old_idx < idx {
                new_counters.insert(*old_idx, *val);
            } else if *old_idx > idx {
                new_counters.insert(*old_idx - 1, *val);
            }
        }
        self.data.auto_inc_counters = new_counters;

        Ok(())
    }

    pub fn rename_column(&mut self, old_name: &str, new_name: &str) -> Result<(), StorageError> {
        let idx = self.column_index(old_name).ok_or_else(|| {
            StorageError::ColumnNotFound(format!("{}.{}", self.schema.name, old_name))
        })?;

        if self.column_index(new_name).is_some() {
            return Err(StorageError::PersistenceError(format!(
                "Column {} already exists in table {}",
                new_name, self.schema.name
            )));
        }

        self.schema.columns[idx].name = new_name.to_string();
        Ok(())
    }

    pub fn rename_table(&mut self, new_name: String) {
        self.schema.name = new_name;
    }

    pub fn column_index(&self, name: &str) -> Option<usize> {
        self.schema.columns.iter().position(|c| c.name == name)
    }

    pub fn null_row(&self) -> Row {
        Row {
            id: "null".to_string(),
            values: vec![Value::Null; self.schema.columns.len()],
        }
    }
}
