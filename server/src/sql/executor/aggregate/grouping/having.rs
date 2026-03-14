use crate::sql::error::{SqlError, SqlResult};
use crate::sql::eval::{EvalContext, Evaluator, evaluate_expression_joined};
use crate::sql::executor::Executor;
use crate::squeal::{ComparisonOp, Condition, Expression, IsOp};
use crate::storage::{DatabaseState, Row, Table, Value};

impl Executor {
    pub(crate) async fn evaluate_having_joined(
        &self,
        cond: &Condition,
        contexts: &[Vec<(&Table, Option<&str>, &Row)>],
        params: &[Value],
        outer_contexts: &[(&Table, Option<&str>, &Row)],
        db_state: &DatabaseState,
    ) -> SqlResult<bool> {
        match cond {
            Condition::And(left, right) => {
                let l = Box::pin(self.evaluate_having_joined(
                    left,
                    contexts,
                    params,
                    outer_contexts,
                    db_state,
                ))
                .await?;
                if !l {
                    return Ok(false);
                }
                Box::pin(self.evaluate_having_joined(
                    right,
                    contexts,
                    params,
                    outer_contexts,
                    db_state,
                ))
                .await
            }
            Condition::Or(left, right) => {
                let l = Box::pin(self.evaluate_having_joined(
                    left,
                    contexts,
                    params,
                    outer_contexts,
                    db_state,
                ))
                .await?;
                if l {
                    return Ok(true);
                }
                Box::pin(self.evaluate_having_joined(
                    right,
                    contexts,
                    params,
                    outer_contexts,
                    db_state,
                ))
                .await
            }
            Condition::Not(c) => Ok(!Box::pin(self.evaluate_having_joined(
                c,
                contexts,
                params,
                outer_contexts,
                db_state,
            ))
            .await?),
            Condition::Comparison(left, op, right) => {
                let left_val = self
                    .evaluate_having_expression_joined(
                        left,
                        contexts,
                        params,
                        outer_contexts,
                        db_state,
                    )
                    .await?;
                let right_val = self
                    .evaluate_having_expression_joined(
                        right,
                        contexts,
                        params,
                        outer_contexts,
                        db_state,
                    )
                    .await?;

                match op {
                    ComparisonOp::Eq => Ok(left_val == right_val),
                    ComparisonOp::Neq => Ok(left_val != right_val),
                    ComparisonOp::Lt => Ok(left_val < right_val),
                    ComparisonOp::Gt => Ok(left_val > right_val),
                    ComparisonOp::Lte => Ok(left_val <= right_val),
                    ComparisonOp::Gte => Ok(left_val >= right_val),
                }
            }
            Condition::Is(expr, op) => {
                let val = self
                    .evaluate_having_expression_joined(
                        expr,
                        contexts,
                        params,
                        outer_contexts,
                        db_state,
                    )
                    .await?;
                match op {
                    IsOp::Null => Ok(matches!(val, Value::Null)),
                    IsOp::NotNull => Ok(!matches!(val, Value::Null)),
                    IsOp::True => Ok(matches!(val, Value::Bool(true))),
                    IsOp::False => Ok(matches!(val, Value::Bool(false))),
                }
            }
            Condition::InSubquery(expr, subquery) => {
                let val = self
                    .evaluate_having_expression_joined(
                        expr,
                        contexts,
                        params,
                        outer_contexts,
                        db_state,
                    )
                    .await?;
                let mut combined_outer = outer_contexts.to_vec();
                if let Some(first_ctx) = contexts.first() {
                    combined_outer.extend_from_slice(first_ctx);
                }
                let result = self
                    .exec_select_internal((**subquery).clone(), &combined_outer, params, db_state)
                    .await?;
                for row in &result.rows {
                    if !row.is_empty() && row[0] == val {
                        return Ok(true);
                    }
                }
                Ok(false)
            }
            Condition::In(expr, values) => {
                let val = self
                    .evaluate_having_expression_joined(
                        expr,
                        contexts,
                        params,
                        outer_contexts,
                        db_state,
                    )
                    .await?;
                for v_expr in values {
                    let v = self
                        .evaluate_having_expression_joined(
                            v_expr,
                            contexts,
                            params,
                            outer_contexts,
                            db_state,
                        )
                        .await?;
                    if v == val {
                        return Ok(true);
                    }
                }
                Ok(false)
            }
            Condition::Exists(subquery) => {
                let mut combined_outer = outer_contexts.to_vec();
                if let Some(first_ctx) = contexts.first() {
                    combined_outer.extend_from_slice(first_ctx);
                }
                let result = self
                    .exec_select_internal((**subquery).clone(), &combined_outer, params, db_state)
                    .await?;
                Ok(!result.rows.is_empty())
            }
            Condition::Between(expr, low, high) => {
                let val = self
                    .evaluate_having_expression_joined(
                        expr,
                        contexts,
                        params,
                        outer_contexts,
                        db_state,
                    )
                    .await?;
                let l = self
                    .evaluate_having_expression_joined(
                        low,
                        contexts,
                        params,
                        outer_contexts,
                        db_state,
                    )
                    .await?;
                let h = self
                    .evaluate_having_expression_joined(
                        high,
                        contexts,
                        params,
                        outer_contexts,
                        db_state,
                    )
                    .await?;
                Ok(val >= l && val <= h)
            }
            Condition::Like(expr, pattern) => {
                let val = self
                    .evaluate_having_expression_joined(
                        expr,
                        contexts,
                        params,
                        outer_contexts,
                        db_state,
                    )
                    .await?;
                let l = val
                    .as_text()
                    .ok_or_else(|| SqlError::TypeMismatch("LIKE requires text".to_string()))?;
                Ok(l.contains(&pattern.replace('%', "")))
            }
            Condition::FullTextSearch(_, _) => Err(SqlError::Runtime(
                "FullTextSearch not allowed in HAVING".to_string(),
            )),
        }
    }

    pub(crate) async fn evaluate_having_expression_joined(
        &self,
        expr: &Expression,
        contexts: &[Vec<(&Table, Option<&str>, &Row)>],
        params: &[Value],
        outer_contexts: &[(&Table, Option<&str>, &Row)],
        db_state: &DatabaseState,
    ) -> SqlResult<Value> {
        match expr {
            Expression::FunctionCall(fc) => {
                self.eval_aggregate_joined(fc, contexts, outer_contexts, db_state)
            }
            Expression::ScalarFunc(_sf) => {
                if let Some(first_ctx_list) = contexts.first() {
                    let eval_ctx =
                        EvalContext::new(first_ctx_list, params, outer_contexts, db_state);
                    evaluate_expression_joined(self as &dyn Evaluator, expr, &eval_ctx)
                } else {
                    Ok(Value::Null)
                }
            }
            Expression::Literal(v) => Ok(v.clone()),
            Expression::Subquery(subquery) => {
                let mut combined_outer = outer_contexts.to_vec();
                if let Some(first_ctx) = contexts.first() {
                    combined_outer.extend_from_slice(first_ctx);
                }
                let result = self
                    .exec_select_internal((**subquery).clone(), &combined_outer, params, db_state)
                    .await?;
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
            Expression::Column(_) | Expression::BinaryOp(_, _, _) => {
                if let Some(first_ctx_list) = contexts.first() {
                    let eval_ctx =
                        EvalContext::new(first_ctx_list, params, outer_contexts, db_state);
                    evaluate_expression_joined(self as &dyn Evaluator, expr, &eval_ctx)
                } else {
                    Ok(Value::Null)
                }
            }
            Expression::Placeholder(_) => {
                if let Some(first_ctx_list) = contexts.first() {
                    let eval_ctx =
                        EvalContext::new(first_ctx_list, params, outer_contexts, db_state);
                    evaluate_expression_joined(self as &dyn Evaluator, expr, &eval_ctx)
                } else {
                    Ok(Value::Null)
                }
            }
            Expression::Star => Err(SqlError::Runtime("Star not allowed in HAVING".to_string())),
        }
    }
}
