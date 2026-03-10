use crate::sql::ast::{Condition, Expression};
use crate::sql::eval::{evaluate_condition_joined, evaluate_expression_joined, Evaluator};
use crate::storage::DatabaseState;
use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use std::sync::{Arc, Mutex};

use super::error::StorageError;
use super::search::SearchIndex;
use super::value::Value;
use super::index::TableIndex;
use super::row::{Column, Row};

pub struct Table {
    pub name: String,
    pub columns: Vec<Column>,
    pub rows: Vec<Row>,
    pub indexes: HashMap<String, TableIndex>, // index_name -> TableIndex
    pub search_index: Option<Arc<Mutex<SearchIndex>>>,
    pub auto_inc_counters: HashMap<usize, u64>, // col_idx -> next_val
}

#[derive(Serialize, Deserialize)]
struct TableSerde {
    name: String,
    columns: Vec<Column>,
    rows: Vec<Row>,
    indexes: HashMap<String, TableIndex>,
    #[serde(default)]
    auto_inc_counters: HashMap<usize, u64>,
}

impl From<TableSerde> for Table {
    fn from(s: TableSerde) -> Self {
        Self {
            name: s.name,
            columns: s.columns,
            rows: s.rows,
            indexes: s.indexes,
            search_index: None,
            auto_inc_counters: s.auto_inc_counters,
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
            auto_inc_counters: t.auto_inc_counters.clone(),
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
            auto_inc_counters: self.auto_inc_counters.clone(),
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
        let mut auto_inc_counters = HashMap::new();
        for (i, col) in columns.iter().enumerate() {
            if col.is_auto_increment {
                auto_inc_counters.insert(i, 1);
            }
        }

        Self {
            name,
            columns,
            rows: Vec::new(),
            indexes: HashMap::new(),
            search_index: None,
            auto_inc_counters,
        }
    }

    pub fn generate_auto_inc(&mut self, col_idx: usize) -> Option<u64> {
        if let Some(counter) = self.auto_inc_counters.get_mut(&col_idx) {
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
                column.name, self.name
            )));
        }

        let new_idx = self.columns.len();
        if column.is_auto_increment {
            self.auto_inc_counters.insert(new_idx, 1);
        }

        self.columns.push(column);

        // Update existing rows with NULL
        for row in &mut self.rows {
            row.values.push(Value::Null);
        }

        Ok(())
    }

    pub fn drop_column(&mut self, name: &str) -> Result<(), StorageError> {
        let idx = self
            .column_index(name)
            .ok_or_else(|| StorageError::ColumnNotFound(format!("{}.{}", self.name, name)))?;

        self.columns.remove(idx);

        // Update existing rows
        for row in &mut self.rows {
            row.values.remove(idx);
        }

        // Rebuild auto_inc_counters indices
        let mut new_counters = HashMap::new();
        for (old_idx, val) in &self.auto_inc_counters {
            if *old_idx < idx {
                new_counters.insert(*old_idx, *val);
            } else if *old_idx > idx {
                new_counters.insert(*old_idx - 1, *val);
            }
        }
        self.auto_inc_counters = new_counters;

        Ok(())
    }

    pub fn rename_column(&mut self, old_name: &str, new_name: &str) -> Result<(), StorageError> {
        let idx = self
            .column_index(old_name)
            .ok_or_else(|| StorageError::ColumnNotFound(format!("{}.{}", self.name, old_name)))?;

        if self.column_index(new_name).is_some() {
            return Err(StorageError::PersistenceError(format!(
                "Column {} already exists in table {}",
                new_name, self.name
            )));
        }

        self.columns[idx].name = new_name.to_string();
        Ok(())
    }

    pub fn rename_table(&mut self, new_name: String) {
        self.name = new_name;
    }

    #[allow(clippy::too_many_arguments)]
    pub fn create_index(
        &mut self,
        evaluator: &dyn Evaluator,
        name: String,
        expressions: Vec<Expression>,
        unique: bool,
        use_hash: bool,
        where_clause: Option<Condition>,
        db_state: &DatabaseState,
    ) -> Result<(), StorageError> {
        let expr_jsons: Vec<serde_json::Value> = expressions
            .iter()
            .map(|e| serde_json::to_value(e).unwrap())
            .collect();
        let where_json = where_clause
            .as_ref()
            .map(|c| serde_json::to_value(c).unwrap());

        let mut index = if use_hash {
            TableIndex::Hash {
                unique,
                expressions: expr_jsons,
                where_clause: where_json,
                data: HashMap::new(),
            }
        } else {
            TableIndex::BTree {
                unique,
                expressions: expr_jsons,
                where_clause: where_json,
                data: std::collections::BTreeMap::new(),
            }
        };

        // Populate existing data
        let exprs = index.expressions();
        let cond = index.where_clause();
        let table_ref: &Table = self;
        for row in &self.rows {
            // Check partial index condition
            if let Some(ref c) = cond {
                let context = [(table_ref, None, row)];
                if !evaluate_condition_joined(evaluator, c, &context, &[], &[], db_state).map_err(|e| {
                    StorageError::PersistenceError(format!("Index where clause evaluation error: {:?}", e))
                })? {
                    continue;
                }
            }

            let key = table_ref.extract_key(evaluator, row, &exprs, db_state)?;
            index.insert(key, row.id.clone())?;
        }

        self.indexes.insert(name, index);
        Ok(())
    }

    fn extract_key(
        &self,
        evaluator: &dyn Evaluator,
        row: &Row,
        expressions: &[Expression],
        db_state: &DatabaseState,
    ) -> Result<Vec<Value>, StorageError> {
        self.extract_key_from_values(evaluator, &row.values, expressions, db_state)
    }

    pub fn extract_key_from_values(
        &self,
        evaluator: &dyn Evaluator,
        values: &[Value],
        expressions: &[Expression],
        db_state: &DatabaseState,
    ) -> Result<Vec<Value>, StorageError> {
        let mut key = Vec::with_capacity(expressions.len());
        let row = Row {
            id: "".to_string(),
            values: values.to_vec(),
        };
        let context = [(self, None, &row)];

        for expr in expressions {
            let val = evaluate_expression_joined(evaluator, expr, &context, &[], &[], db_state).map_err(
                |e| StorageError::PersistenceError(format!("Index expression evaluation error: {:?}", e)),
            )?;
            key.push(val);
        }
        Ok(key)
    }

    pub fn setup_search_index(&mut self, path: &str) -> anyhow::Result<()> {
        let text_fields: Vec<String> = self
            .columns
            .iter()
            .filter(|c| c.data_type == crate::storage::DataType::Text || c.data_type == crate::storage::DataType::VarChar)
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

    pub(crate) fn index_row(&self, row: &Row) -> anyhow::Result<()> {
        if let Some(ref search_index) = self.search_index {
            let mut field_values = Vec::new();
            for (i, col) in self.columns.iter().enumerate() {
                if (col.data_type == crate::storage::DataType::Text || col.data_type == crate::storage::DataType::VarChar)
                    && let Some(val) = row.values.get(i).and_then(|v| v.as_text())
                {
                    field_values.push((col.name.clone(), val.to_string()));
                }
            }
            search_index
                .lock()
                .unwrap()
                .add_document(&row.id, &field_values)?;
        }
        Ok(())
    }

    #[allow(dead_code)]
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
