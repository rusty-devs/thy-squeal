use crate::storage::{DatabaseState, Row, Table, Value};
use super::super::ast::{ComparisonOp, Condition, LogicalOp};
use super::super::error::{SqlError, SqlResult};
use super::Evaluator;
use super::expression::evaluate_expression_joined;

pub fn evaluate_condition_joined(
    executor: &dyn Evaluator,
    cond: &Condition,
    contexts: &[(&Table, Option<&str>, &Row)],
    outer_contexts: &[(&Table, Option<&str>, &Row)],
    db_state: &DatabaseState,
) -> SqlResult<bool> {
    match cond {
        Condition::Comparison(left, op, right) => {
            let left_val =
                evaluate_expression_joined(executor, left, contexts, outer_contexts, db_state)?;
            let right_val =
                evaluate_expression_joined(executor, right, contexts, outer_contexts, db_state)?;

            match op {
                ComparisonOp::Eq => Ok(left_val == right_val),
                ComparisonOp::NotEq => Ok(left_val != right_val),
                ComparisonOp::Lt => Ok(left_val < right_val),
                ComparisonOp::Gt => Ok(left_val > right_val),
                ComparisonOp::LtEq => Ok(left_val <= right_val),
                ComparisonOp::GtEq => Ok(left_val >= right_val),
                ComparisonOp::Like => {
                    let l = left_val.as_text().ok_or_else(|| {
                        SqlError::TypeMismatch("LIKE requires text on the left".to_string())
                    })?;
                    let r = right_val.as_text().ok_or_else(|| {
                        SqlError::TypeMismatch("LIKE requires text on the right".to_string())
                    })?;
                    if r.starts_with('%') && r.ends_with('%') {
                        let pat = &r[1..r.len() - 1];
                        Ok(l.contains(pat))
                    } else if let Some(pat) = r.strip_prefix('%') {
                        Ok(l.ends_with(pat))
                    } else if let Some(pat) = r.strip_suffix('%') {
                        Ok(l.starts_with(pat))
                    } else {
                        Ok(l == r)
                    }
                }
            }
        }
        Condition::IsNull(expr) => {
            let val = evaluate_expression_joined(executor, expr, contexts, outer_contexts, db_state)?;
            Ok(matches!(val, Value::Null))
        }
        Condition::IsNotNull(expr) => {
            let val = evaluate_expression_joined(executor, expr, contexts, outer_contexts, db_state)?;
            Ok(!matches!(val, Value::Null))
        }
        Condition::InSubquery(expr, subquery) => {
            let val = evaluate_expression_joined(executor, expr, contexts, outer_contexts, db_state)?;
            let mut combined_outer = outer_contexts.to_vec();
            combined_outer.extend_from_slice(contexts);

            let result = futures::executor::block_on(executor.exec_select_internal(
                (**subquery).clone(),
                &combined_outer,
                db_state,
            ))?;
            for row in result.rows {
                if !row.is_empty() && row[0] == val {
                    return Ok(true);
                }
            }
            Ok(false)
        }
        Condition::Logical(left, op, right) => {
            let l = evaluate_condition_joined(executor, left, contexts, outer_contexts, db_state)?;
            match op {
                LogicalOp::And => {
                    if !l {
                        return Ok(false);
                    }
                    evaluate_condition_joined(executor, right, contexts, outer_contexts, db_state)
                }
                LogicalOp::Or => {
                    if l {
                        return Ok(true);
                    }
                    evaluate_condition_joined(executor, right, contexts, outer_contexts, db_state)
                }
            }
        }
        Condition::Not(cond) => {
            Ok(!evaluate_condition_joined(executor, cond, contexts, outer_contexts, db_state)?)
        }
    }
}
