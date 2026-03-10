use super::super::ast::{self, SelectStmt};
use super::super::error::{SqlError, SqlResult};
use super::super::eval::{evaluate_condition_joined, evaluate_expression_joined};
use super::{Executor, QueryResult};
use crate::storage::info_schema::get_info_schema_tables;
use crate::storage::{DatabaseState, Row, Table, Value};
use futures::FutureExt;
use futures::future::BoxFuture;

pub type JoinedContext<'a> = Vec<(&'a Table, Option<String>, Row)>;

impl Executor {
    pub fn exec_select_recursive<'a>(
        &'a self,
        stmt: SelectStmt,
        outer_contexts: &'a [(&'a Table, Option<&'a str>, &'a Row)],
        params: &'a [Value],
        db_state: &'a DatabaseState,
        tx_id: Option<&'a str>,
    ) -> BoxFuture<'a, SqlResult<QueryResult>> {
        async move {
            // Check for information_schema
            let info_schema_tables;
            let target_table = if stmt.table.starts_with("information_schema.") {
                let table_name = stmt.table.strip_prefix("information_schema.").unwrap();
                info_schema_tables = get_info_schema_tables(db_state);
                info_schema_tables
                    .get(table_name)
                    .ok_or_else(|| SqlError::TableNotFound(stmt.table.clone()))?
            } else {
                db_state
                    .get_table(&stmt.table)
                    .ok_or_else(|| SqlError::TableNotFound(stmt.table.clone()))?
            };

            let base_table = target_table;
            let base_alias_owned = stmt.table_alias.clone();

            // 2. Identify candidate rows (Optimization: use index if possible)
            let initial_rows: Vec<&Row> = if stmt.joins.is_empty() {
                let mut result_rows = None;
                if let Some(ast::Condition::Comparison(
                    left_expr,
                    ast::ComparisonOp::Eq,
                    ast::Expression::Literal(val),
                )) = &stmt.where_clause
                {
                    for index in base_table.indexes.values() {
                        let exprs = index.expressions();
                        if exprs.len() == 1 && &exprs[0] == left_expr {
                            let key = vec![val.clone()];
                            if let Some(row_ids) = index.get(&key) {
                                result_rows = Some(
                                    base_table
                                        .rows
                                        .iter()
                                        .filter(|r| row_ids.contains(&r.id))
                                        .collect(),
                                );
                            } else {
                                result_rows = Some(vec![]); // Value not in index
                            }
                            break;
                        }
                    }
                }
                result_rows.unwrap_or_else(|| base_table.rows.iter().collect())
            } else {
                base_table.rows.iter().collect()
            };

            // Context is now Vec<(&Table, Option<String>, Row)>
            let mut joined_rows: Vec<JoinedContext> = initial_rows
                .into_iter()
                .map(|r| vec![(base_table, base_alias_owned.clone(), r.clone())])
                .collect();

            // 3. Process JOINS
            for join in &stmt.joins {
                let join_table = if join.table.starts_with("information_schema.") {
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
                    for new_row in &join_table.rows {
                        // Prepare context for evaluation
                        let eval_ctx: Vec<(&Table, Option<&str>, &Row)> = existing_ctx
                            .iter()
                            .map(|(t, a, r)| (*t, a.as_deref(), r))
                            .chain(std::iter::once((
                                join_table,
                                join_alias.as_deref(),
                                new_row,
                            )))
                            .collect();

                        if evaluate_condition_joined(
                            self,
                            &join.on,
                            &eval_ctx,
                            params,
                            outer_contexts,
                            db_state,
                        )? {
                            let mut next_ctx = existing_ctx.clone();
                            next_ctx.push((join_table, join_alias.clone(), new_row.clone()));
                            next_joined_rows.push(next_ctx);
                            found_match = true;
                        }
                    }

                    if !found_match && join.join_type == ast::JoinType::Left {
                        let mut next_ctx = existing_ctx.clone();
                        next_ctx.push((join_table, join_alias.clone(), join_table.null_row()));
                        next_joined_rows.push(next_ctx);
                    }
                }
                joined_rows = next_joined_rows;
            }

            // 4. Apply WHERE (again, to catch complex conditions or those not optimized by index)
            let mut matched_rows = Vec::new();
            if let Some(ref where_cond) = stmt.where_clause {
                for ctx in joined_rows {
                    let eval_ctx: Vec<(&Table, Option<&str>, &Row)> =
                        ctx.iter().map(|(t, a, r)| (*t, a.as_deref(), r)).collect();
                    if evaluate_condition_joined(
                        self,
                        where_cond,
                        &eval_ctx,
                        params,
                        outer_contexts,
                        db_state,
                    )? {
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
                .any(|c| matches!(c.expr, ast::Expression::FunctionCall(_)));

            if has_aggregates || !stmt.group_by.is_empty() {
                return self
                    .exec_select_with_grouping_owned(
                        stmt,
                        matched_rows,
                        outer_contexts,
                        params,
                        db_state,
                        tx_id,
                    )
                    .await;
            }

            // 6. Apply ORDER BY
            if !stmt.order_by.is_empty() {
                let mut err = None;
                matched_rows.sort_by(|a, b| {
                    let eval_a: Vec<(&Table, Option<&str>, &Row)> =
                        a.iter().map(|(t, al, r)| (*t, al.as_deref(), r)).collect();
                    let eval_b: Vec<(&Table, Option<&str>, &Row)> =
                        b.iter().map(|(t, al, r)| (*t, al.as_deref(), r)).collect();

                    for item in &stmt.order_by {
                        let val_a = match evaluate_expression_joined(
                            self,
                            &item.expr,
                            &eval_a,
                            params,
                            outer_contexts,
                            db_state,
                        ) {
                            Ok(v) => v,
                            Err(e) => {
                                err = Some(e);
                                return std::cmp::Ordering::Equal;
                            }
                        };
                        let val_b = match evaluate_expression_joined(
                            self,
                            &item.expr,
                            &eval_b,
                            params,
                            outer_contexts,
                            db_state,
                        ) {
                            Ok(v) => v,
                            Err(e) => {
                                err = Some(e);
                                return std::cmp::Ordering::Equal;
                            }
                        };

                        if let Some(ord) = val_a.partial_cmp(&val_b)
                            && ord != std::cmp::Ordering::Equal
                        {
                            return if item.order == ast::Order::Desc {
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
                self.get_result_column_names(&stmt, base_table, &stmt.joins, db_state);

            let mut projected_rows = Vec::new();
            for ctx in final_rows {
                let eval_ctx: Vec<(&Table, Option<&str>, &Row)> =
                    ctx.iter().map(|(t, a, r)| (*t, a.as_deref(), r)).collect();
                let mut row_values = Vec::new();
                for col in &stmt.columns {
                    match &col.expr {
                        ast::Expression::Star => {
                            for (_table, _alias, row) in &ctx {
                                row_values.extend(row.values.clone());
                            }
                        }
                        _ => {
                            row_values.push(evaluate_expression_joined(
                                self,
                                &col.expr,
                                &eval_ctx,
                                params,
                                outer_contexts,
                                db_state,
                            )?);
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
                transaction_id: tx_id.map(|s| s.to_string()),
            })
        }
        .boxed()
    }

    pub(crate) fn get_result_column_names(
        &self,
        stmt: &SelectStmt,
        base_table: &Table,
        joins: &[ast::Join],
        db_state: &DatabaseState,
    ) -> Vec<String> {
        let mut names = Vec::new();
        for col in &stmt.columns {
            if let Some(alias) = &col.alias {
                names.push(alias.clone());
                continue;
            }

            match &col.expr {
                ast::Expression::Star => {
                    names.extend(base_table.columns.iter().map(|c| c.name.clone()));
                    for join in joins {
                        if let Some(t) = db_state.get_table(&join.table) {
                            names.extend(t.columns.iter().map(|c| c.name.clone()));
                        }
                    }
                }
                ast::Expression::Column(name) => names.push(name.clone()),
                ast::Expression::FunctionCall(fc) => {
                    let name = format!("{:?}", fc.name).to_uppercase();
                    names.push(format!("{}(...)", name));
                }
                ast::Expression::ScalarFunc(sf) => {
                    let name = format!("{:?}", sf.name).to_uppercase();
                    names.push(format!("{}(...)", name));
                }
                _ => names.push(format!("col_{}", names.len())),
            }
        }
        names
    }
}
