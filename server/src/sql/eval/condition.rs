use super::super::error::{SqlError, SqlResult};
use super::super::squeal::{ComparisonOp, Condition, IsOp};
use super::expression::evaluate_expression_joined;
use super::{EvalContext, Evaluator};
use crate::storage::Value;

pub fn evaluate_condition_joined(
    executor: &dyn Evaluator,
    cond: &Condition,
    ctx: &EvalContext<'_>,
) -> SqlResult<bool> {
    match cond {
        Condition::And(left, right) => {
            let l = evaluate_condition_joined(executor, left, ctx)?;
            if !l {
                return Ok(false);
            }
            evaluate_condition_joined(executor, right, ctx)
        }
        Condition::Or(left, right) => {
            let l = evaluate_condition_joined(executor, left, ctx)?;
            if l {
                return Ok(true);
            }
            evaluate_condition_joined(executor, right, ctx)
        }
        Condition::Not(c) => Ok(!evaluate_condition_joined(executor, c, ctx)?),
        Condition::Comparison(left, op, right) => {
            let left_val = evaluate_expression_joined(executor, left, ctx)?;
            let right_val = evaluate_expression_joined(executor, right, ctx)?;

            if matches!(left_val, Value::Null) || matches!(right_val, Value::Null) {
                return Ok(false);
            }

            match op {
                ComparisonOp::Eq => Ok(left_val == right_val),
                ComparisonOp::Neq => Ok(left_val != right_val),
                ComparisonOp::Lt => Ok(left_val < right_val),
                ComparisonOp::Gt => Ok(left_val > right_val),
                ComparisonOp::Lte => Ok(left_val <= right_val),
                ComparisonOp::Gte => Ok(left_val >= right_val),
            }
        }
        Condition::In(expr, values) => {
            let val = evaluate_expression_joined(executor, expr, ctx)?;
            for v_expr in values {
                let v = evaluate_expression_joined(executor, v_expr, ctx)?;
                if v == val {
                    return Ok(true);
                }
            }
            Ok(false)
        }
        Condition::InSubquery(expr, subquery) => {
            let val = evaluate_expression_joined(executor, expr, ctx)?;
            let mut combined_outer = ctx.outer_contexts.to_vec();
            combined_outer.extend_from_slice(ctx.contexts);

            let result = futures::executor::block_on(executor.exec_select_internal(
                (**subquery).clone(),
                &combined_outer,
                ctx.params,
                ctx.db_state,
            ))?;
            for row in &result.rows {
                if !row.is_empty() && row[0] == val {
                    return Ok(true);
                }
            }
            Ok(false)
        }
        Condition::Exists(subquery) => {
            let mut combined_outer = ctx.outer_contexts.to_vec();
            combined_outer.extend_from_slice(ctx.contexts);

            let result = futures::executor::block_on(executor.exec_select_internal(
                (**subquery).clone(),
                &combined_outer,
                ctx.params,
                ctx.db_state,
            ))?;
            Ok(!result.rows.is_empty())
        }
        Condition::Between(expr, low, high) => {
            let val = evaluate_expression_joined(executor, expr, ctx)?;
            let l = evaluate_expression_joined(executor, low, ctx)?;
            let h = evaluate_expression_joined(executor, high, ctx)?;
            Ok(val >= l && val <= h)
        }
        Condition::Is(expr, op) => {
            let val = evaluate_expression_joined(executor, expr, ctx)?;
            match op {
                IsOp::Null => Ok(matches!(val, Value::Null)),
                IsOp::NotNull => Ok(!matches!(val, Value::Null)),
                IsOp::True => Ok(matches!(val, Value::Bool(true))),
                IsOp::False => Ok(matches!(val, Value::Bool(false))),
            }
        }
        Condition::Like(expr, pattern) => {
            let val = evaluate_expression_joined(executor, expr, ctx)?;
            let l = val.as_text().ok_or_else(|| {
                SqlError::TypeMismatch("LIKE requires text on the left".to_string())
            })?;

            if pattern.starts_with('%') && pattern.ends_with('%') {
                let pat = &pattern[1..pattern.len() - 1];
                Ok(l.contains(pat))
            } else if let Some(pat) = pattern.strip_prefix('%') {
                Ok(l.ends_with(pat))
            } else if let Some(pat) = pattern.strip_suffix('%') {
                Ok(l.starts_with(pat))
            } else {
                Ok(l == *pattern)
            }
        }
        Condition::FullTextSearch(_field, _query) => {
            // This is usually handled at the table level using indexes,
            // but for manual evaluation (if needed):
            Err(SqlError::Runtime(
                "FullTextSearch must be handled by the storage engine".to_string(),
            ))
        }
    }
}
