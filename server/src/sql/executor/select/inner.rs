pub mod base;
pub mod cte;
pub mod join;
pub mod project;

use crate::sql::error::SqlResult;
use crate::sql::eval::{EvalContext, evaluate_condition_joined, evaluate_expression_joined};
use crate::sql::executor::{Executor, QueryResult, SelectQueryPlan};
use crate::squeal;
use crate::storage::{Row, Table};
use futures::FutureExt;
use futures::future::BoxFuture;

pub use project::JoinedContext;

impl Executor {
    pub fn exec_select_recursive<'a>(
        &'a self,
        plan: SelectQueryPlan<'a>,
    ) -> BoxFuture<'a, SqlResult<QueryResult>> {
        async move {
            let stmt = &plan.stmt;
            let outer_contexts = plan.outer_contexts;
            let params = plan.params;
            let db_state = plan.db_state;
            let session = plan.session.clone();

            // 0. Resolve CTEs
            let cte_tables = self.resolve_ctes(&plan).await?;

            // 1. Resolve base table and initial rows
            let (base_resolved, initial_rows) = self.resolve_base_table(&plan, &cte_tables)?;
            let base_table = base_resolved.table();

            let base_alias_owned = stmt.table_alias.clone();

            let joined_rows = initial_rows
                .into_iter()
                .map(|r| vec![(base_table, base_alias_owned.clone(), r)])
                .collect();

            // 3. Process JOINS
            let joined_rows = self.process_joins(&plan, &cte_tables, joined_rows)?;

            // 4. Apply WHERE
            let mut matched_rows = Vec::new();
            if let Some(ref where_cond) = stmt.where_clause {
                for ctx in joined_rows {
                    let eval_ctx_list: Vec<(&Table, Option<&str>, &Row)> = ctx
                        .iter()
                        .map(|(t, a, r): &(&Table, Option<String>, Row)| (*t, a.as_deref(), r))
                        .collect();
                    let eval_ctx =
                        EvalContext::new(&eval_ctx_list, params, outer_contexts, db_state);
                    if evaluate_condition_joined(self, where_cond, &eval_ctx)? {
                        matched_rows.push(ctx);
                    }
                }
            } else {
                matched_rows = joined_rows;
            }

            // 5. Handle Aggregates and Grouping
            let has_aggregates = stmt
                .columns
                .iter()
                .any(|c| matches!(c.expr, squeal::Expression::FunctionCall(_)));

            if has_aggregates || !stmt.group_by.is_empty() {
                let group_plan = SelectQueryPlan::new(stmt.clone(), db_state, session);
                return self
                    .exec_select_with_grouping_owned(group_plan, matched_rows, &cte_tables)
                    .await;
            }

            // 6. Apply ORDER BY
            if !stmt.order_by.is_empty() {
                let mut err = None;
                matched_rows.sort_by(|a, b| {
                    let eval_ctx_list_a: Vec<(&Table, Option<&str>, &Row)> = a
                        .iter()
                        .map(|(t, al, r): &(&Table, Option<String>, Row)| (*t, al.as_deref(), r))
                        .collect();
                    let eval_ctx_list_b: Vec<(&Table, Option<&str>, &Row)> = b
                        .iter()
                        .map(|(t, al, r): &(&Table, Option<String>, Row)| (*t, al.as_deref(), r))
                        .collect();

                    let eval_ctx_a =
                        EvalContext::new(&eval_ctx_list_a, params, outer_contexts, db_state);
                    let eval_ctx_b =
                        EvalContext::new(&eval_ctx_list_b, params, outer_contexts, db_state);

                    for item in &stmt.order_by {
                        let val_a = match evaluate_expression_joined(self, &item.expr, &eval_ctx_a)
                        {
                            Ok(v) => v,
                            Err(e) => {
                                err = Some(e);
                                return std::cmp::Ordering::Equal;
                            }
                        };
                        let val_b = match evaluate_expression_joined(self, &item.expr, &eval_ctx_b)
                        {
                            Ok(v) => v,
                            Err(e) => {
                                err = Some(e);
                                return std::cmp::Ordering::Equal;
                            }
                        };

                        if let Some(ord) = val_a.partial_cmp(&val_b)
                            && ord != std::cmp::Ordering::Equal
                        {
                            return if item.order == squeal::Order::Desc {
                                ord.reverse()
                            } else {
                                ord
                            };
                        }
                    }
                    std::cmp::Ordering::Equal
                });
                if let Some(e) = err {
                    return Err(e);
                }
            }

            // 7. Apply LIMIT and OFFSET
            let final_rows = if let Some(ref limit) = stmt.limit {
                let offset = limit.offset.unwrap_or(0);
                matched_rows
                    .into_iter()
                    .skip(offset)
                    .take(limit.count)
                    .collect()
            } else {
                matched_rows
            };

            // 8. Project Columns
            let result_columns: Vec<String> =
                self.get_result_column_names(stmt, base_table, &stmt.joins, db_state, &cte_tables);

            let mut projected_rows = Vec::new();
            for ctx in final_rows {
                let eval_ctx_list: Vec<(&Table, Option<&str>, &Row)> = ctx
                    .iter()
                    .map(|(t, a, r): &(&Table, Option<String>, Row)| (*t, a.as_deref(), r))
                    .collect();
                let eval_ctx = EvalContext::new(&eval_ctx_list, params, outer_contexts, db_state);
                let mut row_values = Vec::new();
                for col in &stmt.columns {
                    match &col.expr {
                        squeal::Expression::Star => {
                            for (_table, _alias, row) in &ctx {
                                row_values.extend(row.values.clone());
                            }
                        }
                        _ => {
                            row_values
                                .push(evaluate_expression_joined(self, &col.expr, &eval_ctx)?);
                        }
                    }
                }
                projected_rows.push(row_values);
            }

            if stmt.distinct {
                let mut seen = std::collections::HashSet::new();
                projected_rows.retain(|row| seen.insert(row.clone()));
            }

            Ok(QueryResult {
                columns: result_columns,
                rows: projected_rows,
                rows_affected: 0,
                transaction_id: session.transaction_id,
            })
        }
        .boxed()
    }
}
