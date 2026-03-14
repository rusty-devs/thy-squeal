use super::super::super::error::{SqlError, SqlResult};
use super::super::Evaluator;
use crate::squeal::Select;
use crate::storage::{DatabaseState, Row, Table, Value};

pub fn evaluate_subquery(
    executor: &dyn Evaluator,
    subquery: &Select,
    contexts: &[(&Table, Option<&str>, &Row)],
    params: &[Value],
    outer_contexts: &[(&Table, Option<&str>, &Row)],
    db_state: &DatabaseState,
) -> SqlResult<Value> {
    let mut combined_outer = outer_contexts.to_vec();
    combined_outer.extend_from_slice(contexts);

    let result = futures::executor::block_on(executor.exec_select_internal(
        subquery.clone(),
        &combined_outer,
        params,
        db_state,
    ))?;

    if result.rows.is_empty() {
        Ok(Value::Null)
    } else if result.rows.len() > 1 {
        Err(SqlError::Runtime(
            "Subquery returned more than one row".to_string(),
        ))
    } else if result.rows[0].is_empty() {
        Ok(Value::Null)
    } else {
        Ok(result.rows[0][0].clone())
    }
}
