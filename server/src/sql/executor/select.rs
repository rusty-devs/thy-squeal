use super::super::error::{SqlError, SqlResult};
use super::super::eval::{EvalContext, evaluate_condition_joined, evaluate_expression_joined};
use super::super::squeal::{self, Select};
use super::{Executor, QueryResult, SelectQueryPlan};
use crate::storage::info_schema::get_info_schema_tables;
use crate::storage::{DatabaseState, Row, Table};
use futures::FutureExt;
use futures::future::BoxFuture;
use std::collections::HashMap;

pub type JoinedContext<'a> = Vec<(&'a Table, Option<String>, Row)>;

impl Executor {
    pub fn exec_select_recursive<'a>(
        &'a self,
        plan: SelectQueryPlan<'a>,
    ) -> BoxFuture<'a, SqlResult<QueryResult>> {
        async move {
            let stmt = plan.stmt;
            let outer_contexts = plan.outer_contexts;
            let params = plan.params;
            let db_state = plan.db_state;
            let session = plan.session;

            // 0. Resolve CTEs
            let mut cte_tables = HashMap::new();
            if let Some(with) = &stmt.with_clause {
                for cte in &with.ctes {
                    let sub_plan =
                        SelectQueryPlan::new(cte.query.clone(), db_state, session.clone())
                            .with_outer_contexts(outer_contexts)
                            .with_params(params);

                    let res = self.exec_select_recursive(sub_plan).await?;
                    let mut cols = Vec::new();
                    for name in &res.columns {
                        cols.push(crate::storage::Column {
                            name: name.clone(),
                            data_type: crate::storage::DataType::Text,
                            is_auto_increment: false,
                        });
                    }
                    let mut table = Table::new(cte.name.clone(), cols, None, vec![]);
                    table.data.rows = res
                        .rows
                        .into_iter()
                        .enumerate()
                        .map(|(i, values)| Row {
                            id: format!("cte_{}_{}", cte.name, i),
                            values,
                        })
                        .collect();
                    cte_tables.insert(cte.name.clone(), table);
                }
            }

            // 1. Resolve base table and initial rows
            let info_schema_storage;
            let dual_table_storage;

            let (base_table, initial_rows): (&Table, Vec<Row>) = if stmt.table.is_empty() {
                dual_table_storage = Table::new("dual".to_string(), vec![], None, vec![]);
                let rows = vec![Row {
                    id: "dual".to_string(),
                    values: vec![],
                }];
                (&dual_table_storage, rows)
            } else if let Some(t) = cte_tables.get(&stmt.table) {
                (t, t.data.rows.clone())
            } else if stmt.table.starts_with("information_schema.") {
                let table_name = stmt.table.strip_prefix("information_schema.").unwrap();
                info_schema_storage = get_info_schema_tables(db_state);
                let t = info_schema_storage
                    .get(table_name)
                    .ok_or_else(|| SqlError::TableNotFound(stmt.table.clone()))?;
                (t, t.data.rows.clone())
            } else {
                let t = db_state
                    .get_table(&stmt.table)
                    .ok_or_else(|| SqlError::TableNotFound(stmt.table.clone()))?;

                let rows = if stmt.joins.is_empty() {
                    let mut best_index = None;
                    let mut best_estimated_rows = t.data.rows.len();

                    if let Some(squeal::Condition::Comparison(
                        left_expr,
                        squeal::ComparisonOp::Eq,
                        squeal::Expression::Literal(val),
                    )) = &stmt.where_clause
                    {
                        for (idx_name, index) in &t.indexes.secondary {
                            let exprs = index.expressions();
                            if exprs.len() == 1 && &exprs[0] == left_expr {
                                let key = vec![val.clone()];
                                let estimated = if let Some(ids) = index.get(&key) {
                                    ids.len()
                                } else {
                                    0
                                };

                                if estimated < best_estimated_rows {
                                    best_estimated_rows = estimated;
                                    best_index = Some((idx_name, index, key));
                                }
                            }
                        }
                    }

                    let selectivity_threshold = (t.data.rows.len() as f64 * 0.3) as usize;

                    if let Some((_name, index, key)) = best_index
                        && (best_estimated_rows < selectivity_threshold || t.data.rows.len() < 10)
                    {
                        if let Some(row_ids) = index.get(&key) {
                            t.data
                                .rows
                                .iter()
                                .filter(|r| row_ids.contains(&r.id))
                                .cloned()
                                .collect()
                        } else {
                            vec![]
                        }
                    } else {
                        t.data.rows.clone()
                    }
                } else {
                    t.data.rows.clone()
                };
                (t, rows)
            };

            let base_alias_owned = stmt.table_alias.clone();

            let mut joined_rows: Vec<JoinedContext> = initial_rows
                .into_iter()
                .map(|r| vec![(base_table, base_alias_owned.clone(), r)])
                .collect();

            // 3. Process JOINS
            for join in &stmt.joins {
                let join_table = if let Some(t) = cte_tables.get(&join.table) {
                    t
                } else if join.table.starts_with("information_schema.") {
                    return Err(SqlError::Runtime(
                        "JOIN with information_schema is not yet supported".to_string(),
                    ));
                } else {
                    db_state
                        .get_table(&join.table)
                        .ok_or_else(|| SqlError::TableNotFound(join.table.clone()))?
                };

                let join_alias = join.table_alias.clone();
                let mut next_joined_rows = Vec::new();

                for existing_ctx in joined_rows {
                    let mut found_match = false;
                    for new_row in &join_table.data.rows {
                        let eval_ctx_list: Vec<(&Table, Option<&str>, &Row)> = existing_ctx
                            .iter()
                            .map(|(t, a, r)| (*t, a.as_deref(), r))
                            .chain(std::iter::once((
                                join_table,
                                join_alias.as_deref(),
                                new_row,
                            )))
                            .collect();

                        let eval_ctx =
                            EvalContext::new(&eval_ctx_list, params, outer_contexts, db_state);

                        if evaluate_condition_joined(self, &join.on, &eval_ctx)? {
                            let mut next_ctx = existing_ctx.clone();
                            next_ctx.push((join_table, join_alias.clone(), new_row.clone()));
                            next_joined_rows.push(next_ctx);
                            found_match = true;
                        }
                    }

                    if !found_match && join.join_type == squeal::JoinType::Left {
                        let mut next_ctx = existing_ctx.clone();
                        next_ctx.push((join_table, join_alias.clone(), join_table.null_row()));
                        next_joined_rows.push(next_ctx);
                    }
                }
                joined_rows = next_joined_rows;
            }

            // 4. Apply WHERE
            let mut matched_rows = Vec::new();
            if let Some(ref where_cond) = stmt.where_clause {
                for ctx in joined_rows {
                    let eval_ctx_list: Vec<(&Table, Option<&str>, &Row)> =
                        ctx.iter().map(|(t, a, r)| (*t, a.as_deref(), r)).collect();
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
                let group_plan = SelectQueryPlan::new(stmt, db_state, session);
                return self
                    .exec_select_with_grouping_owned(group_plan, matched_rows, &cte_tables)
                    .await;
            }

            // 6. Apply ORDER BY
            if !stmt.order_by.is_empty() {
                let mut err = None;
                matched_rows.sort_by(|a, b| {
                    let eval_ctx_list_a: Vec<(&Table, Option<&str>, &Row)> =
                        a.iter().map(|(t, al, r)| (*t, al.as_deref(), r)).collect();
                    let eval_ctx_list_b: Vec<(&Table, Option<&str>, &Row)> =
                        b.iter().map(|(t, al, r)| (*t, al.as_deref(), r)).collect();

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
                self.get_result_column_names(&stmt, base_table, &stmt.joins, db_state, &cte_tables);

            let mut projected_rows = Vec::new();
            for ctx in final_rows {
                let eval_ctx_list: Vec<(&Table, Option<&str>, &Row)> =
                    ctx.iter().map(|(t, a, r)| (*t, a.as_deref(), r)).collect();
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

    pub(crate) fn get_result_column_names(
        &self,
        stmt: &Select,
        base_table: &Table,
        joins: &[squeal::Join],
        db_state: &DatabaseState,
        cte_tables: &HashMap<String, Table>,
    ) -> Vec<String> {
        let mut names = Vec::new();
        for col in &stmt.columns {
            if let Some(alias) = &col.alias {
                names.push(alias.clone());
                continue;
            }

            match &col.expr {
                squeal::Expression::Star => {
                    names.extend(base_table.schema.columns.iter().map(|c| c.name.clone()));
                    for join in joins {
                        let join_table = if let Some(t) = cte_tables.get(&join.table) {
                            Some(t)
                        } else {
                            db_state.get_table(&join.table)
                        };

                        if let Some(t) = join_table {
                            names.extend(t.schema.columns.iter().map(|c| c.name.clone()));
                        }
                    }
                }
                squeal::Expression::Column(name) => names.push(name.clone()),
                squeal::Expression::FunctionCall(fc) => {
                    let name = format!("{:?}", fc.name).to_uppercase();
                    names.push(format!("{}(...)", name));
                }
                squeal::Expression::ScalarFunc(sf) => {
                    let name = format!("{:?}", sf.name).to_uppercase();
                    names.push(format!("{}(...)", name));
                }
                _ => names.push(format!("col_{}", names.len())),
            }
        }
        names
    }
}
