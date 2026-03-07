use crate::sql::ast::{Condition, Expression};
use crate::sql::eval::{evaluate_condition_joined, evaluate_expression_joined, Evaluator};
use crate::storage::DatabaseState;
use serde::{Deserialize, Serialize};
use std::collections::{BTreeMap, HashMap};
use std::sync::{Arc, Mutex};
use uuid::Uuid;

use super::error::StorageError;
use super::search::SearchIndex;
use super::types::DataType;
use super::value::Value;

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct Column {
    pub name: String,
    pub data_type: DataType,
}

#[allow(dead_code)]
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
                data: BTreeMap::new(),
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
                if !evaluate_condition_joined(evaluator, c, &context, &[], db_state).map_err(|e| {
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
            let val = evaluate_expression_joined(evaluator, expr, &context, &[], db_state).map_err(
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
            search_index
                .lock()
                .unwrap()
                .add_document(&row.id, &field_values)?;
        }
        Ok(())
    }

    pub fn insert(
        &mut self,
        evaluator: &dyn Evaluator,
        values: Vec<Value>,
        db_state: &DatabaseState,
    ) -> Result<String, StorageError> {
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
            values: values.to_vec(),
        };
        let table_ref: &Table = self;

        // 1. Check unique constraints/indexes before inserting
        for index in self.indexes.values() {
            // Check partial condition
            if let Some(cond) = index.where_clause() {
                let context = [(table_ref, None, &row)];
                if !evaluate_condition_joined(evaluator, &cond, &context, &[], db_state).map_err(
                    |e| StorageError::PersistenceError(format!("Index where clause evaluation error: {:?}", e)),
                )? {
                    continue;
                }
            }

            if index.is_unique() {
                let key =
                    table_ref.extract_key_from_values(evaluator, &values, &index.expressions(), db_state)?;
                if index.get(&key).is_some_and(|ids| !ids.is_empty()) {
                    return Err(StorageError::DuplicateKey(format!("{:?}", key)));
                }
            }
        }

        // 2. Update all indexes
        let mut index_keys = Vec::new();
        for index in self.indexes.values() {
            // Check partial condition
            if let Some(cond) = index.where_clause() {
                let context = [(table_ref, None, &row)];
                if !evaluate_condition_joined(evaluator, &cond, &context, &[], db_state).map_err(
                    |e| StorageError::PersistenceError(format!("Index where clause evaluation error: {:?}", e)),
                )? {
                    index_keys.push(None);
                    continue;
                }
            }

            let key =
                table_ref.extract_key_from_values(evaluator, &values, &index.expressions(), db_state)?;
            index_keys.push(Some(key));
        }

        for (index, key_opt) in self.indexes.values_mut().zip(index_keys) {
            if let Some(key) = key_opt {
                index.insert(key, id.clone())?;
            }
        }

        // 3. Update Search Index
        if let Err(e) = self.index_row(&row) {
            return Err(StorageError::PersistenceError(format!(
                "Search index error: {}",
                e
            )));
        }

        self.rows.push(row);
        Ok(id)
    }

    pub fn update(
        &mut self,
        evaluator: &dyn Evaluator,
        id: &str,
        values: Vec<Value>,
        db_state: &DatabaseState,
    ) -> Result<(), StorageError> {
        if values.len() != self.columns.len() {
            return Err(StorageError::InvalidType(format!(
                "Expected {} columns, got {}",
                self.columns.len(),
                values.len()
            )));
        }

        if let Some(pos) = self.rows.iter().position(|r| r.id == id) {
            let old_values = self.rows[pos].values.clone();
            let old_row = Row {
                id: id.to_string(),
                values: old_values.clone(),
            };
            let new_row = Row {
                id: id.to_string(),
                values: values.to_vec(),
            };
            let table_ref: &Table = self;

            // 1. Check unique constraints
            for index in self.indexes.values() {
                // Check if new row matches partial condition
                if let Some(cond) = index.where_clause() {
                    let context = [(table_ref, None, &new_row)];
                    if !evaluate_condition_joined(evaluator, &cond, &context, &[], db_state).map_err(
                        |e| StorageError::PersistenceError(format!("Index where clause evaluation error: {:?}", e)),
                    )? {
                        continue;
                    }
                }

                if index.is_unique() {
                    let new_key =
                        table_ref.extract_key_from_values(evaluator, &values, &index.expressions(), db_state)?;
                    let old_key = table_ref.extract_key_from_values(
                        evaluator,
                        &old_values,
                        &index.expressions(),
                        db_state,
                    )?;

                    if old_key != new_key && index.get(&new_key).is_some() {
                        return Err(StorageError::DuplicateKey(format!("{:?}", new_key)));
                    }
                }
            }

            // 2. Update all indexes
            let mut index_updates = Vec::new();
            for index in self.indexes.values() {
                let cond = index.where_clause();
                let old_match = if let Some(ref c) = cond {
                    let context = [(table_ref, None, &old_row)];
                    evaluate_condition_joined(evaluator, c, &context, &[], db_state).map_err(|e| {
                        StorageError::PersistenceError(format!("Index where clause evaluation error: {:?}", e))
                    })?
                } else {
                    true
                };
                let new_match = if let Some(ref c) = cond {
                    let context = [(table_ref, None, &new_row)];
                    evaluate_condition_joined(evaluator, c, &context, &[], db_state).map_err(|e| {
                        StorageError::PersistenceError(format!("Index where clause evaluation error: {:?}", e))
                    })?
                } else {
                    true
                };

                let old_key = table_ref.extract_key_from_values(
                    evaluator,
                    &old_values,
                    &index.expressions(),
                    db_state,
                )?;
                let new_key =
                    table_ref.extract_key_from_values(evaluator, &values, &index.expressions(), db_state)?;
                index_updates.push((old_match, new_match, old_key, new_key));
            }

            for (index, (old_match, new_match, old_key, new_key)) in
                self.indexes.values_mut().zip(index_updates)
            {
                if old_match && !new_match {
                    index.remove(&old_key, id);
                } else if !old_match && new_match {
                    index.insert(new_key, id.to_string())?;
                } else if old_match && new_match && old_key != new_key {
                    index.remove(&old_key, id);
                    index.insert(new_key, id.to_string())?;
                }
            }

            self.rows[pos].values = values;

            // 3. Update Search Index
            let updated_row = self.rows[pos].clone();
            if let Err(e) = self.index_row(&updated_row) {
                return Err(StorageError::PersistenceError(format!(
                    "Search index error: {}",
                    e
                )));
            }

            Ok(())
        } else {
            Err(StorageError::RowNotFound(id.to_string()))
        }
    }

    pub fn delete(
        &mut self,
        evaluator: &dyn Evaluator,
        id: &str,
        db_state: &DatabaseState,
    ) -> Result<(), StorageError> {
        if let Some(pos) = self.rows.iter().position(|r| r.id == id) {
            let values = self.rows[pos].values.clone();
            let row = Row {
                id: id.to_string(),
                values: values.clone(),
            };
            let table_ref: &Table = self;

            // 1. Update all indexes
            let mut index_to_remove = Vec::new();
            for index in self.indexes.values() {
                // Check partial condition
                if let Some(cond) = index.where_clause() {
                    let context = [(table_ref, None, &row)];
                    if !evaluate_condition_joined(evaluator, &cond, &context, &[], db_state).map_err(
                        |e| StorageError::PersistenceError(format!("Index where clause evaluation error: {:?}", e)),
                    )? {
                        index_to_remove.push(None);
                        continue;
                    }
                }

                let key =
                    table_ref.extract_key_from_values(evaluator, &values, &index.expressions(), db_state)?;
                index_to_remove.push(Some(key));
            }

            for (index, key_opt) in self.indexes.values_mut().zip(index_to_remove) {
                if let Some(key) = key_opt {
                    index.remove(&key, id);
                }
            }

            // 2. Remove from Search Index
            if let Some(ref search_index) = self.search_index
                && let Err(e) = search_index.lock().unwrap().delete_document(id)
            {
                return Err(StorageError::PersistenceError(format!(
                    "Search index error: {}",
                    e
                )));
            }

            self.rows.remove(pos);
            Ok(())
        } else {
            Err(StorageError::RowNotFound(id.to_string()))
        }
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
