use crate::storage::{DatabaseState, Row, Table, TableIndex, Value};
use crate::storage::info_schema::get_info_schema_tables;
use super::super::ast::{self, SelectStmt};
use super::super::error::{SqlError, SqlResult};
use super::super::eval::{evaluate_condition_joined, evaluate_expression_joined, Evaluator};
use super::{QueryResult, Executor};
use futures::future::BoxFuture;
use futures::FutureExt;

type JoinedContext<'a> = Vec<(&'a Table, Option<String>, Row)>;

impl Executor {
    pub(crate) async fn exec_search(
        &self,
        stmt: ast::SearchStmt,
        db_state: &DatabaseState,
        tx_id: Option<&str>,
    ) -> SqlResult<QueryResult> {
        let table = db_state
            .get_table(&stmt.table)
            .ok_or_else(|| SqlError::TableNotFound(stmt.table.clone()))?;

        let search_index = table.search_index.as_ref().ok_or_else(|| {
            SqlError::Runtime(format!(
                "Full-text search index not enabled for table: {}",
                stmt.table
            ))
        })?;

        let results = search_index
            .lock()
            .unwrap()
            .search(&stmt.query, 100)
            .map_err(|e| SqlError::Runtime(format!("Search error: {}", e)))?;

        let mut rows = Vec::new();
        for (row_id, score) in results {
            if let Some(row) = table.rows.iter().find(|r| r.id == row_id) {
                let mut values = row.values.clone();
                values.push(Value::Float(score as f64));
                rows.push(values);
            }
        }

        let mut columns: Vec<String> = table.columns.iter().map(|c| c.name.clone()).collect();
        columns.push("_score".to_string());

        Ok(QueryResult {
            columns,
            rows,
            rows_affected: 0,
            transaction_id: tx_id.map(|s| s.to_string()),
        })
    }

    pub(crate) async fn exec_explain(
        &self,
        stmt: SelectStmt,
        db_state: &DatabaseState,
        tx_id: Option<&str>,
    ) -> SqlResult<QueryResult> {
        let table = db_state
            .get_table(&stmt.table)
            .ok_or_else(|| SqlError::TableNotFound(stmt.table.clone()))?;

        let mut plan = Vec::new();

        // 1. Scan Type
        let mut scan_type = "Full Table Scan".to_string();
        let mut index_name = "None".to_string();
        if stmt.joins.is_empty()
            && let Some(ast::Condition::Comparison(
                left_expr,
                ast::ComparisonOp::Eq,
                ast::Expression::Literal(_),
            )) = &stmt.where_clause
        {
            for (name, index) in &table.indexes {
                let exprs = index.expressions();
                if exprs.len() == 1 && &exprs[0] == left_expr {
                    scan_type = match index {
                        TableIndex::BTree { .. } => "Index Lookup (BTree)".to_string(),
                        TableIndex::Hash { .. } => "Index Lookup (Hash)".to_string(),
                    };
                    index_name = name.clone();
                    break;
                }
            }
        }
        plan.push(vec![
            Value::Text("SCAN".to_string()),
            Value::Text(scan_type),
            Value::Text(format!("table: {}, index: {}", stmt.table, index_name)),
        ]);

        // 2. Joins
        for join in &stmt.joins {
            plan.push(vec![
                Value::Text("JOIN".to_string()),
                Value::Text("Inner Join".to_string()),
                Value::Text(format!("table: {}, condition: {:?}", join.table, join.on)),
            ]);
        }

        // 3. Filters
        if let Some(ref cond) = stmt.where_clause {
            plan.push(vec![
                Value::Text("FILTER".to_string()),
                Value::Text("WHERE".to_string()),
                Value::Text(format!("{:?}", cond)),
            ]);
        }

        // 4. Grouping/Aggregates
        let has_aggregates = stmt
            .columns
            .iter()
            .any(|c| matches!(c.expr, ast::Expression::FunctionCall(_)));
        if !stmt.group_by.is_empty() || has_aggregates {
            plan.push(vec![
                Value::Text("AGGREGATE".to_string()),
                Value::Text("GROUP BY / FUNC".to_string()),
                Value::Text(format!(
                    "groups: {:?}, cols: {:?}",
                    stmt.group_by, stmt.columns
                )),
            ]);
        }

        // 5. Having
        if let Some(ref cond) = stmt.having {
            plan.push(vec![
                Value::Text("FILTER".to_string()),
                Value::Text("HAVING".to_string()),
                Value::Text(format!("{:?}", cond)),
            ]);
        }

        // 6. Order
        if !stmt.order_by.is_empty() {
            plan.push(vec![
                Value::Text("ORDER".to_string()),
                Value::Text("SORT".to_string()),
                Value::Text(format!("{:?}", stmt.order_by)),
            ]);
        }

        // 7. Limit
        if let Some(ref limit) = stmt.limit {
            plan.push(vec![
                Value::Text("LIMIT".to_string()),
                Value::Text("SLICE".to_string()),
                Value::Text(format!("count: {}, offset: {:?}", limit.count, limit.offset)),
            ]);
        }

        Ok(QueryResult {
            columns: vec![
                "stage".to_string(),
                "operation".to_string(),
                "details".to_string(),
            ],
            rows: plan,
            rows_affected: 0,
            transaction_id: tx_id.map(|s| s.to_string()),
        })
    }

    pub fn exec_select_recursive<'a>(
        &'a self,
        stmt: SelectStmt,
        outer_contexts: &'a [(&'a Table, Option<&'a str>, &'a Row)],
        db_state: &'a DatabaseState,
        tx_id: Option<&'a str>,
    ) -> BoxFuture<'a, SqlResult<QueryResult>> {
        async move {
            // Check for information_schema
            let info_schema_tables;
            let target_table = if stmt.table.starts_with("information_schema.") {
                let table_name = stmt.table.strip_prefix("information_schema.").unwrap();
                info_schema_tables = get_info_schema_tables(db_state);
                info_schema_tables.get(table_name)
                    .ok_or_else(|| SqlError::TableNotFound(stmt.table.clone()))?
            } else {
                db_state
                    .get_table(&stmt.table)
                    .ok_or_else(|| SqlError::TableNotFound(stmt.table.clone()))?
            };

            let base_table = target_table;
            let base_alias = stmt.table_alias.clone();

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
                .map(|r| vec![(base_table, base_alias.clone(), r.clone())])
                .collect();

            // 3. Process JOINS
            for join in &stmt.joins {
                let join_table = if join.table.starts_with("information_schema.") {
                    return Err(SqlError::Runtime("JOIN with information_schema is not yet supported".to_string()));
                } else {
                    db_state.get_table(&join.table)
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
                            .chain(std::iter::once((join_table, join_alias.as_deref(), new_row)))
                            .collect();

                        if evaluate_condition_joined(
                            self,
                            &join.on,
                            &eval_ctx,
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
                    let eval_ctx: Vec<(&Table, Option<&str>, &Row)> = ctx.iter().map(|(t, a, r)| (*t, a.as_deref(), r)).collect();
                    if evaluate_condition_joined(
                        self,
                        where_cond,
                        &eval_ctx,
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
                        db_state,
                        tx_id,
                    )
                    .await;
            }

            // 6. Apply ORDER BY
            if !stmt.order_by.is_empty() {
                let mut err = None;
                matched_rows.sort_by(|a, b| {
                    let eval_a: Vec<(&Table, Option<&str>, &Row)> = a.iter().map(|(t, al, r)| (*t, al.as_deref(), r)).collect();
                    let eval_b: Vec<(&Table, Option<&str>, &Row)> = b.iter().map(|(t, al, r)| (*t, al.as_deref(), r)).collect();

                    for item in &stmt.order_by {
                        let val_a = match evaluate_expression_joined(
                            self,
                            &item.expr,
                            &eval_a,
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
                let eval_ctx: Vec<(&Table, Option<&str>, &Row)> = ctx.iter().map(|(t, a, r)| (*t, a.as_deref(), r)).collect();
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

    fn get_result_column_names(
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

    async fn exec_select_with_grouping_owned(
        &self,
        stmt: SelectStmt,
        matched_rows: Vec<JoinedContext<'_>>,
        outer_contexts: &[(&Table, Option<&str>, &Row)],
        db_state: &DatabaseState,
        tx_id: Option<&str>,
    ) -> SqlResult<QueryResult> {
        let base_table = if stmt.table.starts_with("information_schema.") {
            return Err(SqlError::Runtime("GROUP BY with information_schema is not yet supported".to_string()));
        } else {
            db_state.get_table(&stmt.table).unwrap()
        };

        let mut result_rows = Vec::new();
        if stmt.group_by.is_empty() {
            // Global aggregation
            let eval_contexts: Vec<Vec<(&Table, Option<&str>, &Row)>> = matched_rows
                .iter()
                .map(|ctx: &JoinedContext<'_>| ctx.iter().map(|(t, a, r)| (*t, a.as_deref(), r)).collect())
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
                self.evaluate_having_joined(having_cond, &eval_contexts, outer_contexts, db_state)
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
                let eval_ctx: Vec<(&Table, Option<&str>, &Row)> = ctx.iter().map(|(t, a, r)| (*t, a.as_deref(), r)).collect();
                let mut group_key = Vec::new();
                for gb_expr in &stmt.group_by {
                    group_key.push(evaluate_expression_joined(
                        self,
                        gb_expr,
                        &eval_ctx,
                        outer_contexts,
                        db_state,
                    )?);
                }
                groups.entry(group_key).or_default().push(ctx);
            }

            for (_key, group_owned_contexts) in groups {
                let group_eval_contexts: Vec<Vec<(&Table, Option<&str>, &Row)>> = group_owned_contexts
                    .iter()
                    .map(|ctx: &JoinedContext<'_>| ctx.iter().map(|(t, a, r)| (*t, a.as_deref(), r)).collect())
                    .collect();

                let include_group = if let Some(ref having_cond) = stmt.having {
                    self.evaluate_having_joined(
                        having_cond,
                        &group_eval_contexts,
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

    async fn evaluate_having_joined(
        &self,
        cond: &ast::Condition,
        contexts: &[Vec<(&Table, Option<&str>, &Row)>],
        outer_contexts: &[(&Table, Option<&str>, &Row)],
        db_state: &DatabaseState,
    ) -> SqlResult<bool> {
        match cond {
            ast::Condition::Comparison(left, op, right) => {
                let left_val = self
                    .evaluate_having_expression_joined(left, contexts, outer_contexts, db_state)
                    .await?;
                let right_val = self
                    .evaluate_having_expression_joined(right, contexts, outer_contexts, db_state)
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
                let l = Box::pin(self.evaluate_having_joined(left, contexts, outer_contexts, db_state))
                    .await?;
                match op {
                    ast::LogicalOp::And => Ok(l
                        && Box::pin(self.evaluate_having_joined(
                            right,
                            contexts,
                            outer_contexts,
                            db_state,
                        ))
                        .await?),
                    ast::LogicalOp::Or => Ok(l
                        || Box::pin(self.evaluate_having_joined(
                            right,
                            contexts,
                            outer_contexts,
                            db_state,
                        ))
                        .await?),
                }
            }
            ast::Condition::Not(c) => Ok(!Box::pin(self.evaluate_having_joined(
                c,
                contexts,
                outer_contexts,
                db_state,
            ))
            .await?),
            ast::Condition::IsNull(e) => Ok(self
                .evaluate_having_expression_joined(e, contexts, outer_contexts, db_state)
                .await?
                == Value::Null),
            ast::Condition::IsNotNull(e) => Ok(self
                .evaluate_having_expression_joined(e, contexts, outer_contexts, db_state)
                .await?
                != Value::Null),
            ast::Condition::InSubquery(expr, subquery) => {
                let val = self
                    .evaluate_having_expression_joined(expr, contexts, outer_contexts, db_state)
                    .await?;
                let mut combined_outer = outer_contexts.to_vec();
                if let Some(first_ctx) = contexts.first() {
                    combined_outer.extend_from_slice(first_ctx);
                }
                let result = self
                    .exec_select_internal((**subquery).clone(), &combined_outer, db_state)
                    .await?;
                for row in result.rows {
                    if !row.is_empty() && row[0] == val {
                        return Ok(true);
                    }
                }
                Ok(false)
            }
        }
    }

    async fn evaluate_having_expression_joined(
        &self,
        expr: &ast::Expression,
        contexts: &[Vec<(&Table, Option<&str>, &Row)>],
        outer_contexts: &[(&Table, Option<&str>, &Row)],
        db_state: &DatabaseState,
    ) -> SqlResult<Value> {
        match expr {
            ast::Expression::FunctionCall(fc) => {
                self.eval_aggregate_joined(fc, contexts, outer_contexts, db_state)
            }
            ast::Expression::ScalarFunc(_sf) => {
                if let Some(first_ctx) = contexts.first() {
                    evaluate_expression_joined(self, expr, first_ctx, outer_contexts, db_state)
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
                    .exec_select_internal((**subquery).clone(), &combined_outer, db_state)
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
                    evaluate_expression_joined(self, expr, first_ctx, outer_contexts, db_state)
                } else {
                    Ok(Value::Null)
                }
            }
            ast::Expression::Star => {
                Err(SqlError::Runtime("Star not allowed in HAVING".to_string()))
            }
        }
    }

    fn eval_aggregate_joined(
        &self,
        fc: &ast::FunctionCall,
        contexts: &[Vec<(&Table, Option<&str>, &Row)>],
        outer_contexts: &[(&Table, Option<&str>, &Row)],
        db_state: &DatabaseState,
    ) -> SqlResult<Value> {
        match fc.name {
            ast::AggregateType::Count => {
                if fc.args.len() == 1 && matches!(fc.args[0], ast::Expression::Star) {
                    Ok(Value::Int(contexts.len() as i64))
                } else {
                    let mut count = 0;
                    for ctx in contexts {
                        let val = evaluate_expression_joined(
                            self,
                            &fc.args[0],
                            ctx,
                            outer_contexts,
                            db_state,
                        )?;
                        if val != Value::Null {
                            count += 1;
                        }
                    }
                    Ok(Value::Int(count))
                }
            }
            ast::AggregateType::Sum => {
                let mut sum_f = 0.0;
                let mut sum_i = 0;
                let mut is_float = false;
                for ctx in contexts {
                    let val = evaluate_expression_joined(
                        self,
                        &fc.args[0],
                        ctx,
                        outer_contexts,
                        db_state,
                    )?;
                    match val {
                        Value::Int(i) => {
                            sum_i += i;
                            sum_f += i as f64;
                        }
                        Value::Float(f) => {
                            sum_f += f;
                            is_float = true;
                        }
                        Value::Null => {}
                        _ => {
                            return Err(SqlError::TypeMismatch(
                                "SUM requires numeric values".to_string(),
                            ))
                        }
                    }
                }
                if is_float {
                    Ok(Value::Float(sum_f))
                } else {
                    Ok(Value::Int(sum_i))
                }
            }
            ast::AggregateType::Min => {
                let mut min_val: Option<Value> = None;
                for ctx in contexts {
                    let val = evaluate_expression_joined(
                        self,
                        &fc.args[0],
                        ctx,
                        outer_contexts,
                        db_state,
                    )?;
                    if val == Value::Null {
                        continue;
                    }
                    if min_val.as_ref().is_none_or(|mv| &val < mv) {
                        min_val = Some(val);
                    }
                }
                Ok(min_val.unwrap_or(Value::Null))
            }
            ast::AggregateType::Max => {
                let mut max_val: Option<Value> = None;
                for ctx in contexts {
                    let val = evaluate_expression_joined(
                        self,
                        &fc.args[0],
                        ctx,
                        outer_contexts,
                        db_state,
                    )?;
                    if val == Value::Null {
                        continue;
                    }
                    if max_val.as_ref().is_none_or(|mv| &val > mv) {
                        max_val = Some(val);
                    }
                }
                Ok(max_val.unwrap_or(Value::Null))
            }
            ast::AggregateType::Avg => {
                let mut sum = 0.0;
                let mut count = 0;
                for ctx in contexts {
                    let val = evaluate_expression_joined(
                        self,
                        &fc.args[0],
                        ctx,
                        outer_contexts,
                        db_state,
                    )?;
                    match val {
                        Value::Int(i) => {
                            sum += i as f64;
                            count += 1;
                        }
                        Value::Float(f) => {
                            sum += f;
                            count += 1;
                        }
                        Value::Null => {}
                        _ => {
                            return Err(SqlError::TypeMismatch(
                                "AVG requires numeric values".to_string(),
                            ))
                        }
                    }
                }
                if count == 0 {
                    Ok(Value::Null)
                } else {
                    Ok(Value::Float(sum / count as f64))
                }
            }
        }
    }
}
