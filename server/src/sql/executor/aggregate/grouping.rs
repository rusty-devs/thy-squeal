use super::super::super::error::{SqlError, SqlResult};
use super::super::super::eval::{EvalContext, Evaluator, evaluate_expression_joined};
use super::super::super::squeal::{ComparisonOp, Condition, Expression, IsOp};
use super::super::select::JoinedContext;
use super::super::{Executor, QueryResult, SelectQueryPlan};
use crate::storage::{DatabaseState, Row, Table, Value};
use std::collections::HashMap;

impl Executor {
    pub(crate) async fn exec_select_with_grouping_owned(
        &self,
        plan: SelectQueryPlan<'_>,
        matched_rows: Vec<JoinedContext<'_>>,
        cte_tables: &HashMap<String, Table>,
    ) -> SqlResult<QueryResult> {
        let stmt = &plan.stmt;
        let outer_contexts = plan.outer_contexts;
        let params = plan.params;
        let db_state = plan.db_state;
        let session = &plan.session;

        let base_table = if let Some(t) = cte_tables.get(&stmt.table) {
            t
        } else if stmt.table.starts_with("information_schema.") {
            return Err(SqlError::Runtime(
                "GROUP BY with information_schema is not yet supported".to_string(),
            ));
        } else {
            db_state
                .get_table(&stmt.table)
                .ok_or_else(|| SqlError::TableNotFound(stmt.table.clone()))?
        };

        let mut result_rows = Vec::new();
        if stmt.group_by.is_empty() {
            // Global aggregation
            let eval_contexts: Vec<Vec<(&Table, Option<&str>, &Row)>> = matched_rows
                .iter()
                .map(|ctx: &JoinedContext<'_>| {
                    ctx.iter().map(|(t, a, r)| (*t, a.as_deref(), r)).collect()
                })
                .collect();

            let mut row_values = Vec::new();
            for col in &stmt.columns {
                match &col.expr {
                    Expression::FunctionCall(fc) => {
                        row_values.push(self.eval_aggregate_joined(
                            fc,
                            &eval_contexts,
                            outer_contexts,
                            db_state,
                        )?);
                    }
                    _ => {
                        if let Some(first_row_ctx_list) = eval_contexts.first() {
                            let eval_ctx = EvalContext::new(
                                first_row_ctx_list,
                                params,
                                outer_contexts,
                                db_state,
                            );
                            row_values
                                .push(evaluate_expression_joined(self, &col.expr, &eval_ctx)?);
                        } else {
                            row_values.push(Value::Null);
                        }
                    }
                }
            }

            let include_row = if let Some(ref having_cond) = stmt.having {
                self.evaluate_having_joined(
                    having_cond,
                    &eval_contexts,
                    params,
                    outer_contexts,
                    db_state,
                )
                .await?
            } else {
                true
            };

            if include_row {
                result_rows.push(row_values);
            }
        } else {
            // GROUP BY
            let mut groups: std::collections::HashMap<Vec<Value>, Vec<JoinedContext<'_>>> =
                std::collections::HashMap::new();
            for ctx in matched_rows {
                let eval_ctx_list: Vec<(&Table, Option<&str>, &Row)> =
                    ctx.iter().map(|(t, a, r)| (*t, a.as_deref(), r)).collect();
                let eval_ctx = EvalContext::new(&eval_ctx_list, params, outer_contexts, db_state);
                let mut group_key = Vec::new();
                for gb_expr in &stmt.group_by {
                    group_key.push(evaluate_expression_joined(self, gb_expr, &eval_ctx)?);
                }
                groups.entry(group_key).or_default().push(ctx);
            }

            for (_key, group_owned_contexts) in groups {
                let group_eval_contexts: Vec<Vec<(&Table, Option<&str>, &Row)>> =
                    group_owned_contexts
                        .iter()
                        .map(|ctx: &JoinedContext<'_>| {
                            ctx.iter().map(|(t, a, r)| (*t, a.as_deref(), r)).collect()
                        })
                        .collect();

                let include_group = if let Some(ref having_cond) = stmt.having {
                    self.evaluate_having_joined(
                        having_cond,
                        &group_eval_contexts,
                        params,
                        outer_contexts,
                        db_state,
                    )
                    .await?
                } else {
                    true
                };

                if include_group {
                    let mut row_values = Vec::new();
                    for col in &stmt.columns {
                        match &col.expr {
                            Expression::FunctionCall(fc) => {
                                row_values.push(self.eval_aggregate_joined(
                                    fc,
                                    &group_eval_contexts,
                                    outer_contexts,
                                    db_state,
                                )?);
                            }
                            _ => {
                                if let Some(first_ctx_list) = group_eval_contexts.first() {
                                    let eval_ctx = EvalContext::new(
                                        first_ctx_list,
                                        params,
                                        outer_contexts,
                                        db_state,
                                    );
                                    row_values.push(evaluate_expression_joined(
                                        self, &col.expr, &eval_ctx,
                                    )?);
                                } else {
                                    row_values.push(Value::Null);
                                }
                            }
                        }
                    }
                    result_rows.push(row_values);
                }
            }
        }

        if stmt.distinct {
            let mut seen = std::collections::HashSet::new();
            result_rows.retain(|row| seen.insert(row.clone()));
        }

        Ok(QueryResult {
            columns: self.get_result_column_names(
                stmt,
                base_table,
                &stmt.joins,
                db_state,
                cte_tables,
            ),
            rows: result_rows,
            rows_affected: 0,
            transaction_id: session.transaction_id.clone(),
        })
    }

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
