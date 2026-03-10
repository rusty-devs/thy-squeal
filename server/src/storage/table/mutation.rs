use crate::sql::eval::{Evaluator, evaluate_condition_joined};
use crate::storage::DatabaseState;
use uuid::Uuid;

use super::super::error::StorageError;
use super::super::row::Row;
use super::super::value::Value;
use super::Table;

impl Table {
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
                if !evaluate_condition_joined(evaluator, &cond, &context, &[], &[], db_state)
                    .map_err(|e| {
                        StorageError::PersistenceError(format!(
                            "Index where clause evaluation error: {:?}",
                            e
                        ))
                    })?
                {
                    continue;
                }
            }

            if index.is_unique() {
                let key = table_ref.extract_key_from_values(
                    evaluator,
                    &values,
                    &index.expressions(),
                    db_state,
                )?;
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
                if !evaluate_condition_joined(evaluator, &cond, &context, &[], &[], db_state)
                    .map_err(|e| {
                        StorageError::PersistenceError(format!(
                            "Index where clause evaluation error: {:?}",
                            e
                        ))
                    })?
                {
                    index_keys.push(None);
                    continue;
                }
            }

            let key = table_ref.extract_key_from_values(
                evaluator,
                &values,
                &index.expressions(),
                db_state,
            )?;
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
                    if !evaluate_condition_joined(evaluator, &cond, &context, &[], &[], db_state)
                        .map_err(|e| {
                            StorageError::PersistenceError(format!(
                                "Index where clause evaluation error: {:?}",
                                e
                            ))
                        })?
                    {
                        continue;
                    }
                }

                if index.is_unique() {
                    let new_key = table_ref.extract_key_from_values(
                        evaluator,
                        &values,
                        &index.expressions(),
                        db_state,
                    )?;
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
                    evaluate_condition_joined(evaluator, c, &context, &[], &[], db_state).map_err(
                        |e| {
                            StorageError::PersistenceError(format!(
                                "Index where clause evaluation error: {:?}",
                                e
                            ))
                        },
                    )?
                } else {
                    true
                };
                let new_match = if let Some(ref c) = cond {
                    let context = [(table_ref, None, &new_row)];
                    evaluate_condition_joined(evaluator, c, &context, &[], &[], db_state).map_err(
                        |e| {
                            StorageError::PersistenceError(format!(
                                "Index where clause evaluation error: {:?}",
                                e
                            ))
                        },
                    )?
                } else {
                    true
                };

                let old_key = table_ref.extract_key_from_values(
                    evaluator,
                    &old_values,
                    &index.expressions(),
                    db_state,
                )?;
                let new_key = table_ref.extract_key_from_values(
                    evaluator,
                    &values,
                    &index.expressions(),
                    db_state,
                )?;
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
                    if !evaluate_condition_joined(evaluator, &cond, &context, &[], &[], db_state)
                        .map_err(|e| {
                            StorageError::PersistenceError(format!(
                                "Index where clause evaluation error: {:?}",
                                e
                            ))
                        })?
                    {
                        index_to_remove.push(None);
                        continue;
                    }
                }

                let key = table_ref.extract_key_from_values(
                    evaluator,
                    &values,
                    &index.expressions(),
                    db_state,
                )?;
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
}
