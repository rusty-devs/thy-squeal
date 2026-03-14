use crate::sql::error::{SqlError, SqlResult};
use crate::sql::eval::{EvalContext, evaluate_expression_joined};
use crate::sql::executor::select::JoinedContext;
use crate::sql::executor::{Executor, QueryResult, SelectQueryPlan};
use crate::squeal::Expression;
use crate::storage::{Row, Table, Value};
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

        let mut result_rows: Vec<Vec<Value>> = Vec::new();
        if stmt.group_by.is_empty() {
            // Global aggregation
            let eval_contexts: Vec<Vec<(&Table, Option<&str>, &Row)>> = matched_rows
                .iter()
                .map(|ctx: &JoinedContext<'_>| {
                    ctx.iter()
                        .map(|(t, a, r): &(&Table, Option<String>, Row)| (*t, a.as_deref(), r))
                        .collect()
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
                let eval_ctx_list: Vec<(&Table, Option<&str>, &Row)> = ctx
                    .iter()
                    .map(|(t, a, r): &(&Table, Option<String>, Row)| (*t, a.as_deref(), r))
                    .collect();
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
                            ctx.iter()
                                .map(|(t, a, r): &(&Table, Option<String>, Row)| {
                                    (*t, a.as_deref(), r)
                                })
                                .collect()
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
            result_rows.retain(|row: &Vec<Value>| seen.insert(row.clone()));
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
}
