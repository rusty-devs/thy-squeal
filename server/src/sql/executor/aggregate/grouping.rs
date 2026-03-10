use super::super::super::ast::{self, SelectStmt};
use super::super::super::error::{SqlError, SqlResult};
use super::super::super::eval::{Evaluator, evaluate_expression_joined};
use super::super::Executor;
use super::super::QueryResult;
use super::super::select::JoinedContext;
use crate::storage::{DatabaseState, Row, Table, Value};

impl Executor {
    pub(crate) async fn exec_select_with_grouping_owned(
        &self,
        stmt: SelectStmt,
        matched_rows: Vec<JoinedContext<'_>>,
        outer_contexts: &[(&Table, Option<&str>, &Row)],
        params: &[Value],
        db_state: &DatabaseState,
        tx_id: Option<&str>,
    ) -> SqlResult<QueryResult> {
        let base_table = if stmt.table.starts_with("information_schema.") {
            return Err(SqlError::Runtime(
                "GROUP BY with information_schema is not yet supported".to_string(),
            ));
        } else {
            db_state.get_table(&stmt.table).unwrap()
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
                    ast::Expression::FunctionCall(fc) => {
                        row_values.push(self.eval_aggregate_joined(
                            fc,
                            &eval_contexts,
                            outer_contexts,
                            db_state,
                        )?);
                    }
                    _ => {
                        if let Some(first_row_ctx) = eval_contexts.first() {
                            row_values.push(evaluate_expression_joined(
                                self,
                                &col.expr,
                                first_row_ctx,
                                params,
                                outer_contexts,
                                db_state,
                            )?);
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
                let eval_ctx: Vec<(&Table, Option<&str>, &Row)> =
                    ctx.iter().map(|(t, a, r)| (*t, a.as_deref(), r)).collect();
                let mut group_key = Vec::new();
                for gb_expr in &stmt.group_by {
                    group_key.push(evaluate_expression_joined(
                        self,
                        gb_expr,
                        &eval_ctx,
                        params,
                        outer_contexts,
                        db_state,
                    )?);
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
                            ast::Expression::FunctionCall(fc) => {
                                row_values.push(self.eval_aggregate_joined(
                                    fc,
                                    &group_eval_contexts,
                                    outer_contexts,
                                    db_state,
                                )?);
                            }
                            _ => {
                                if let Some(first_ctx) = group_eval_contexts.first() {
                                    row_values.push(evaluate_expression_joined(
                                        self,
                                        &col.expr,
                                        first_ctx,
                                        params,
                                        outer_contexts,
                                        db_state,
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
            columns: self.get_result_column_names(&stmt, base_table, &stmt.joins, db_state),
            rows: result_rows,
            rows_affected: 0,
            transaction_id: tx_id.map(|s| s.to_string()),
        })
    }

    pub(crate) async fn evaluate_having_joined(
        &self,
        cond: &ast::Condition,
        contexts: &[Vec<(&Table, Option<&str>, &Row)>],
        params: &[Value],
        outer_contexts: &[(&Table, Option<&str>, &Row)],
        db_state: &DatabaseState,
    ) -> SqlResult<bool> {
        match cond {
            ast::Condition::Comparison(left, op, right) => {
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
                    ast::ComparisonOp::Eq => Ok(left_val == right_val),
                    ast::ComparisonOp::NotEq => Ok(left_val != right_val),
                    ast::ComparisonOp::Lt => Ok(left_val < right_val),
                    ast::ComparisonOp::Gt => Ok(left_val > right_val),
                    ast::ComparisonOp::LtEq => Ok(left_val <= right_val),
                    ast::ComparisonOp::GtEq => Ok(left_val >= right_val),
                    ast::ComparisonOp::Like => {
                        let l = left_val.as_text().ok_or_else(|| {
                            SqlError::TypeMismatch("LIKE requires text".to_string())
                        })?;
                        let r = right_val.as_text().ok_or_else(|| {
                            SqlError::TypeMismatch("LIKE requires text".to_string())
                        })?;
                        Ok(l.contains(&r.replace('%', "")))
                    }
                }
            }
            ast::Condition::Logical(left, op, right) => {
                let l = Box::pin(self.evaluate_having_joined(
                    left,
                    contexts,
                    params,
                    outer_contexts,
                    db_state,
                ))
                .await?;
                match op {
                    ast::LogicalOp::And => Ok(l
                        && Box::pin(self.evaluate_having_joined(
                            right,
                            contexts,
                            params,
                            outer_contexts,
                            db_state,
                        ))
                        .await?),
                    ast::LogicalOp::Or => Ok(l
                        || Box::pin(self.evaluate_having_joined(
                            right,
                            contexts,
                            params,
                            outer_contexts,
                            db_state,
                        ))
                        .await?),
                }
            }
            ast::Condition::Not(c) => Ok(!Box::pin(self.evaluate_having_joined(
                c,
                contexts,
                params,
                outer_contexts,
                db_state,
            ))
            .await?),
            ast::Condition::IsNull(e) => Ok(self
                .evaluate_having_expression_joined(e, contexts, params, outer_contexts, db_state)
                .await?
                == Value::Null),
            ast::Condition::IsNotNull(e) => Ok(self
                .evaluate_having_expression_joined(e, contexts, params, outer_contexts, db_state)
                .await?
                != Value::Null),
            ast::Condition::InSubquery(expr, subquery) => {
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
        }
    }

    pub(crate) async fn evaluate_having_expression_joined(
        &self,
        expr: &ast::Expression,
        contexts: &[Vec<(&Table, Option<&str>, &Row)>],
        params: &[Value],
        outer_contexts: &[(&Table, Option<&str>, &Row)],
        db_state: &DatabaseState,
    ) -> SqlResult<Value> {
        match expr {
            ast::Expression::FunctionCall(fc) => {
                self.eval_aggregate_joined(fc, contexts, outer_contexts, db_state)
            }
            ast::Expression::ScalarFunc(_sf) => {
                if let Some(first_ctx) = contexts.first() {
                    evaluate_expression_joined(
                        self,
                        expr,
                        first_ctx,
                        params,
                        outer_contexts,
                        db_state,
                    )
                } else {
                    Ok(Value::Null)
                }
            }
            ast::Expression::Literal(v) => Ok(v.clone()),
            ast::Expression::Subquery(subquery) => {
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
            ast::Expression::Column(_) | ast::Expression::BinaryOp(_, _, _) => {
                if let Some(first_ctx) = contexts.first() {
                    evaluate_expression_joined(
                        self,
                        expr,
                        first_ctx,
                        params,
                        outer_contexts,
                        db_state,
                    )
                } else {
                    Ok(Value::Null)
                }
            }
            ast::Expression::Placeholder(_) => {
                if let Some(first_ctx) = contexts.first() {
                    evaluate_expression_joined(
                        self,
                        expr,
                        first_ctx,
                        params,
                        outer_contexts,
                        db_state,
                    )
                } else {
                    Ok(Value::Null)
                }
            }
            ast::Expression::Star => {
                Err(SqlError::Runtime("Star not allowed in HAVING".to_string()))
            }
        }
    }
}
