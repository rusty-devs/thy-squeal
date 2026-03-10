use super::super::error::StorageError;
use super::super::index::TableIndex;
use super::super::row::Row;
use super::super::value::Value;
use super::Table;
use crate::sql::ast::{Condition, Expression};
use crate::sql::eval::{EvalContext, Evaluator, evaluate_condition_joined};
use crate::storage::DatabaseState;
use std::collections::HashMap;

impl Table {
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
        for row in &self.data.rows {
            // Check partial index condition
            if let Some(ref c) = cond {
                let context_list = [(table_ref, None, row)];
                let eval_ctx = EvalContext::new(&context_list, &[], &[], db_state);
                if !evaluate_condition_joined(evaluator, c, &eval_ctx).map_err(|e| {
                    StorageError::PersistenceError(format!(
                        "Index where clause evaluation error: {:?}",
                        e
                    ))
                })? {
                    continue;
                }
            }

            let key = table_ref.extract_key(evaluator, row, &exprs, db_state)?;
            index.insert(key, row.id.clone())?;
        }

        self.indexes.secondary.insert(name, index);
        Ok(())
    }

    pub fn extract_key(
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
        let context_list = [(self, None, &row)];
        let eval_ctx = EvalContext::new(&context_list, &[], &[], db_state);

        for expr in expressions {
            let val = crate::sql::eval::evaluate_expression_joined(evaluator, expr, &eval_ctx)
                .map_err(|e| {
                    StorageError::PersistenceError(format!(
                        "Index expression evaluation error: {:?}",
                        e
                    ))
                })?;
            key.push(val);
        }
        Ok(key)
    }
}
