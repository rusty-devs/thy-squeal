use crate::sql::eval::{EvalContext, Evaluator, evaluate_condition_joined};
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
        if values.len() != self.schema.columns.len() {
            return Err(StorageError::InvalidType(format!(
                "Expected {} columns, got {}",
                self.schema.columns.len(),
                values.len()
            )));
        }

        let id = Uuid::new_v4().to_string();
        let row = Row {
            id: id.clone(),
            values: values.to_vec(),
        };
        let table_ref: &Table = self;

        // 1. Check Foreign Key constraints
        for fk in &self.schema.foreign_keys {
            let mut local_values = Vec::new();
            for col_name in &fk.columns {
                let idx = self.column_index(col_name).ok_or_else(|| {
                    StorageError::ColumnNotFound(format!("{}.{}", self.schema.name, col_name))
                })?;
                local_values.push(values[idx].clone());
            }

            let ref_table = db_state
                .get_table(&fk.ref_table)
                .ok_or_else(|| StorageError::TableNotFound(fk.ref_table.clone()))?;

            // Search for matching row in ref_table
            let mut found = false;
            for ref_row in ref_table.rows() {
                let mut matches = true;
                for (i, ref_col_name) in fk.ref_columns.iter().enumerate() {
                    let ref_idx = ref_table.column_index(ref_col_name).ok_or_else(|| {
                        StorageError::ColumnNotFound(format!("{}.{}", fk.ref_table, ref_col_name))
                    })?;
                    if ref_row.values[ref_idx] != local_values[i] {
                        matches = false;
                        break;
                    }
                }
                if matches {
                    found = true;
                    break;
                }
            }

            if !found {
                return Err(StorageError::PersistenceError(format!(
                    "Foreign key constraint violation: referenced row not found in {}",
                    fk.ref_table
                )));
            }
        }

        // 2. Check unique constraints/indexes before inserting
        for index in self.indexes.secondary.values() {
            // Check partial condition
            if let Some(cond) = index.where_clause() {
                let context_list = [(table_ref, None, &row)];
                let eval_ctx = EvalContext::new(&context_list, &[], &[], db_state);
                if !evaluate_condition_joined(evaluator, &cond, &eval_ctx).map_err(|e| {
                    StorageError::PersistenceError(format!(
                        "Index where clause evaluation error: {:?}",
                        e
                    ))
                })? {
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
        for index in self.indexes.secondary.values() {
            // Check partial condition
            if let Some(cond) = index.where_clause() {
                let context_list = [(table_ref, None, &row)];
                let eval_ctx = EvalContext::new(&context_list, &[], &[], db_state);
                if !evaluate_condition_joined(evaluator, &cond, &eval_ctx).map_err(|e| {
                    StorageError::PersistenceError(format!(
                        "Index where clause evaluation error: {:?}",
                        e
                    ))
                })? {
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

        for (index, key_opt) in self.indexes.secondary.values_mut().zip(index_keys) {
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

        self.data.rows.push(row);
        Ok(id)
    }

    pub fn update(
        &mut self,
        evaluator: &dyn Evaluator,
        id: &str,
        values: Vec<Value>,
        db_state: &DatabaseState,
    ) -> Result<(), StorageError> {
        if values.len() != self.schema.columns.len() {
            return Err(StorageError::InvalidType(format!(
                "Expected {} columns, got {}",
                self.schema.columns.len(),
                values.len()
            )));
        }

        if let Some(pos) = self.data.rows.iter().position(|r| r.id == id) {
            let old_values = self.data.rows[pos].values.clone();
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
            for index in self.indexes.secondary.values() {
                // Check if new row matches partial condition
                if let Some(cond) = index.where_clause() {
                    let context_list = [(table_ref, None, &new_row)];
                    let eval_ctx = EvalContext::new(&context_list, &[], &[], db_state);
                    if !evaluate_condition_joined(evaluator, &cond, &eval_ctx).map_err(|e| {
                        StorageError::PersistenceError(format!(
                            "Index where clause evaluation error: {:?}",
                            e
                        ))
                    })? {
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
            for index in self.indexes.secondary.values() {
                let cond = index.where_clause();
                let old_match = if let Some(ref c) = cond {
                    let context_list = [(table_ref, None, &old_row)];
                    let eval_ctx = EvalContext::new(&context_list, &[], &[], db_state);
                    evaluate_condition_joined(evaluator, c, &eval_ctx).map_err(|e| {
                        StorageError::PersistenceError(format!(
                            "Index where clause evaluation error: {:?}",
                            e
                        ))
                    })?
                } else {
                    true
                };
                let new_match = if let Some(ref c) = cond {
                    let context_list = [(table_ref, None, &new_row)];
                    let eval_ctx = EvalContext::new(&context_list, &[], &[], db_state);
                    evaluate_condition_joined(evaluator, c, &eval_ctx).map_err(|e| {
                        StorageError::PersistenceError(format!(
                            "Index where clause evaluation error: {:?}",
                            e
                        ))
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
                let new_key = table_ref.extract_key_from_values(
                    evaluator,
                    &values,
                    &index.expressions(),
                    db_state,
                )?;
                index_updates.push((old_match, new_match, old_key, new_key));
            }

            for (index, (old_match, new_match, old_key, new_key)) in
                self.indexes.secondary.values_mut().zip(index_updates)
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

            self.data.rows[pos].values = values;

            // 3. Update Search Index
            let updated_row = self.data.rows[pos].clone();
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
        if let Some(pos) = self.data.rows.iter().position(|r| r.id == id) {
            let values = self.data.rows[pos].values.clone();
            let row = Row {
                id: id.to_string(),
                values: values.clone(),
            };
            let table_ref: &Table = self;

            // 1. Update all indexes
            let mut index_to_remove = Vec::new();
            for index in self.indexes.secondary.values() {
                // Check partial condition
                if let Some(cond) = index.where_clause() {
                    let context_list = [(table_ref, None, &row)];
                    let eval_ctx = EvalContext::new(&context_list, &[], &[], db_state);
                    if !evaluate_condition_joined(evaluator, &cond, &eval_ctx).map_err(|e| {
                        StorageError::PersistenceError(format!(
                            "Index where clause evaluation error: {:?}",
                            e
                        ))
                    })? {
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

            for (index, key_opt) in self.indexes.secondary.values_mut().zip(index_to_remove) {
                if let Some(key) = key_opt {
                    index.remove(&key, id);
                }
            }

            // 2. Remove from Search Index
            if let Some(ref search_index) = self.indexes.search
                && let Err(e) = search_index.lock().unwrap().delete_document(id)
            {
                return Err(StorageError::PersistenceError(format!(
                    "Search index error: {}",
                    e
                )));
            }

            self.data.rows.remove(pos);
            Ok(())
        } else {
            Err(StorageError::RowNotFound(id.to_string()))
        }
    }
}
