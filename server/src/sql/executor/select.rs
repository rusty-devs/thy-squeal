use crate::storage::{Value, Table, Row};
use super::super::ast::{self, SelectStmt};
use super::super::error::{SqlError, SqlResult};
use super::super::eval::{evaluate_condition_joined, evaluate_expression_joined};
use super::{QueryResult, Executor};

impl Executor {
    pub(crate) async fn exec_select(&self, stmt: SelectStmt) -> SqlResult<QueryResult> {
        let db = self.db.read().await;
        
        // 1. Get base table
        let base_table = db
            .get_table(&stmt.table)
            .ok_or_else(|| SqlError::TableNotFound(stmt.table.clone()))?;

        // A "joined row" is a list of (Table, Row) references
        let mut joined_rows: Vec<Vec<(&Table, &Row)>> = base_table.rows.iter()
            .map(|r| vec![(base_table, r)])
            .collect();

        // 2. Process JOINS
        for join in &stmt.joins {
            let join_table = db.get_table(&join.table)
                .ok_or_else(|| SqlError::TableNotFound(join.table.clone()))?;
            
            let mut next_joined_rows = Vec::new();
            
            for existing_row in joined_rows {
                for new_row in &join_table.rows {
                    // Create a potential combined context
                    let mut combined_context = existing_row.clone();
                    combined_context.push((join_table, new_row));
                    
                    // Evaluate ON condition
                    if evaluate_condition_joined(&join.on, &combined_context)? {
                        next_joined_rows.push(combined_context);
                    }
                }
            }
            joined_rows = next_joined_rows;
        }

        // 3. Apply WHERE
        let mut matched_rows = Vec::new();
        if let Some(ref where_cond) = stmt.where_clause {
            for row in joined_rows {
                if evaluate_condition_joined(where_cond, &row)? {
                    matched_rows.push(row);
                }
            }
        } else {
            matched_rows = joined_rows;
        }

        // 4. Handle Aggregates and Grouping
        let has_aggregates = stmt.columns.iter().any(|c| matches!(c.expr, ast::Expression::FunctionCall(_)));
        
        if has_aggregates || !stmt.group_by.is_empty() {
             return self.exec_select_with_grouping(stmt, matched_rows).await;
        }

        // 5. Apply ORDER BY
        if !stmt.order_by.is_empty() {
            let mut err = None;
            matched_rows.sort_by(|a, b| {
                for item in &stmt.order_by {
                    let val_a = match evaluate_expression_joined(&item.expr, a) {
                        Ok(v) => v,
                        Err(e) => { err = Some(e); return std::cmp::Ordering::Equal; }
                    };
                    let val_b = match evaluate_expression_joined(&item.expr, b) {
                        Ok(v) => v,
                        Err(e) => { err = Some(e); return std::cmp::Ordering::Equal; }
                    };

                    if let Some(ord) = val_a.partial_cmp(&val_b) {
                        if ord != std::cmp::Ordering::Equal {
                            return if item.order == ast::Order::Desc { ord.reverse() } else { ord };
                        }
                    }
                }
                std::cmp::Ordering::Equal
            });
            if let Some(e) = err { return Err(e); }
        }

        // 6. Apply LIMIT and OFFSET
        let final_rows = if let Some(ref limit) = stmt.limit {
            let offset = limit.offset.unwrap_or(0);
            matched_rows.iter().skip(offset).take(limit.count).cloned().collect()
        } else {
            matched_rows
        };

        // 7. Project Columns
        let result_columns: Vec<String> = self.get_result_column_names(&stmt, base_table, &stmt.joins, &db);

        let mut projected_rows = Vec::new();
        for context in final_rows {
            let mut row_values = Vec::new();
            for col in &stmt.columns {
                match &col.expr {
                    ast::Expression::Star => {
                        for (_table, row) in &context {
                            row_values.extend(row.values.clone());
                        }
                    }
                    _ => {
                        row_values.push(evaluate_expression_joined(&col.expr, &context)?);
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
        })
    }

    fn get_result_column_names(&self, stmt: &SelectStmt, base_table: &Table, joins: &[ast::Join], db: &crate::storage::Database) -> Vec<String> {
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
                        if let Some(t) = db.get_table(&join.table) {
                            names.extend(t.columns.iter().map(|c| c.name.clone()));
                        }
                    }
                }
                ast::Expression::Column(name) => names.push(name.clone()),
                ast::Expression::FunctionCall(fc) => {
                    let name = format!("{:?}", fc.name).to_uppercase();
                    names.push(format!("{}(...)", name));
                }
                _ => names.push(format!("col_{}", names.len())),
            }
        }
        names
    }

    async fn exec_select_with_grouping(&self, stmt: SelectStmt, matched_rows: Vec<Vec<(&Table, &Row)>>) -> SqlResult<QueryResult> {
        let mut result_rows = Vec::new();
        let db = self.db.read().await;
        let base_table = db.get_table(&stmt.table).unwrap();

        if stmt.group_by.is_empty() {
            // Global aggregation
            let mut row_values = Vec::new();
            for col in &stmt.columns {
                match &col.expr {
                    ast::Expression::FunctionCall(fc) => {
                        row_values.push(self.eval_aggregate_joined(fc, &matched_rows)?);
                    },
                    _ => {
                        if let Some(first_row_ctx) = matched_rows.first() {
                            row_values.push(evaluate_expression_joined(&col.expr, first_row_ctx)?);
                        } else {
                            row_values.push(Value::Null);
                        }
                    }
                }
            }
            
            let include_row = if let Some(ref having_cond) = stmt.having {
                self.evaluate_having_joined(having_cond, &matched_rows)?
            } else {
                true
            };

            if include_row {
                result_rows.push(row_values);
            }
        } else {
            // GROUP BY
            let mut groups: std::collections::HashMap<Vec<Value>, Vec<Vec<(&Table, &Row)>>> = std::collections::HashMap::new();
            for ctx in matched_rows {
                let mut group_key = Vec::new();
                for gb_expr in &stmt.group_by {
                    group_key.push(evaluate_expression_joined(gb_expr, &ctx)?);
                }
                groups.entry(group_key).or_default().push(ctx);
            }

            for (_key, group_contexts) in groups {
                let include_group = if let Some(ref having_cond) = stmt.having {
                    self.evaluate_having_joined(having_cond, &group_contexts)?
                } else {
                    true
                };

                if include_group {
                    let mut row_values = Vec::new();
                    for col in &stmt.columns {
                        match &col.expr {
                            ast::Expression::FunctionCall(fc) => {
                                row_values.push(self.eval_aggregate_joined(fc, &group_contexts)?);
                            },
                            _ => {
                                if let Some(first_ctx) = group_contexts.first() {
                                    row_values.push(evaluate_expression_joined(&col.expr, first_ctx)?);
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
            columns: self.get_result_column_names(&stmt, base_table, &stmt.joins, &db),
            rows: result_rows,
            rows_affected: 0,
        })
    }

    fn evaluate_having_joined(&self, cond: &ast::Condition, contexts: &[Vec<(&Table, &Row)>]) -> SqlResult<bool> {
        match cond {
            ast::Condition::Comparison(left, op, right) => {
                let left_val = self.evaluate_having_expression_joined(left, contexts)?;
                let right_val = self.evaluate_having_expression_joined(right, contexts)?;
                
                match op {
                    ast::ComparisonOp::Eq => Ok(left_val == right_val),
                    ast::ComparisonOp::NotEq => Ok(left_val != right_val),
                    ast::ComparisonOp::Lt => Ok(left_val < right_val),
                    ast::ComparisonOp::Gt => Ok(left_val > right_val),
                    ast::ComparisonOp::LtEq => Ok(left_val <= right_val),
                    ast::ComparisonOp::GtEq => Ok(left_val >= right_val),
                    ast::ComparisonOp::Like => {
                        let l = left_val.as_text().ok_or_else(|| SqlError::TypeMismatch("LIKE requires text".to_string()))?;
                        let r = right_val.as_text().ok_or_else(|| SqlError::TypeMismatch("LIKE requires text".to_string()))?;
                        Ok(l.contains(&r.replace("%", "")))
                    }
                }
            },
            ast::Condition::Logical(left, op, right) => {
                let l = self.evaluate_having_joined(left, contexts)?;
                match op {
                    ast::LogicalOp::And => Ok(l && self.evaluate_having_joined(right, contexts)?),
                    ast::LogicalOp::Or => Ok(l || self.evaluate_having_joined(right, contexts)?),
                }
            },
            ast::Condition::Not(c) => Ok(!self.evaluate_having_joined(c, contexts)?),
            ast::Condition::IsNull(e) => Ok(self.evaluate_having_expression_joined(e, contexts)? == Value::Null),
            ast::Condition::IsNotNull(e) => Ok(self.evaluate_having_expression_joined(e, contexts)? != Value::Null),
        }
    }

    fn evaluate_having_expression_joined(&self, expr: &ast::Expression, contexts: &[Vec<(&Table, &Row)>]) -> SqlResult<Value> {
        match expr {
            ast::Expression::FunctionCall(fc) => self.eval_aggregate_joined(fc, contexts),
            ast::Expression::Literal(v) => Ok(v.clone()),
            _ => {
                if let Some(first_ctx) = contexts.first() {
                    evaluate_expression_joined(expr, first_ctx)
                } else {
                    Ok(Value::Null)
                }
            }
        }
    }

    fn eval_aggregate_joined(&self, fc: &ast::FunctionCall, contexts: &[Vec<(&Table, &Row)>]) -> SqlResult<Value> {
        match fc.name {
            ast::AggregateType::Count => {
                if fc.args.len() == 1 && matches!(fc.args[0], ast::Expression::Star) {
                    Ok(Value::Int(contexts.len() as i64))
                } else {
                    let mut count = 0;
                    for ctx in contexts {
                        let val = evaluate_expression_joined(&fc.args[0], ctx)?;
                        if val != Value::Null {
                            count += 1;
                        }
                    }
                    Ok(Value::Int(count))
                }
            },
            ast::AggregateType::Sum => {
                let mut sum_f = 0.0;
                let mut sum_i = 0;
                let mut is_float = false;
                for ctx in contexts {
                    let val = evaluate_expression_joined(&fc.args[0], ctx)?;
                    match val {
                        Value::Int(i) => { sum_i += i; sum_f += i as f64; },
                        Value::Float(f) => { sum_f += f; is_float = true; },
                        Value::Null => {},
                        _ => return Err(SqlError::TypeMismatch("SUM requires numeric values".to_string())),
                    }
                }
                if is_float { Ok(Value::Float(sum_f)) } else { Ok(Value::Int(sum_i)) }
            },
            ast::AggregateType::Min => {
                let mut min_val: Option<Value> = None;
                for ctx in contexts {
                    let val = evaluate_expression_joined(&fc.args[0], ctx)?;
                    if val == Value::Null { continue; }
                    if min_val.is_none() || val < min_val.clone().unwrap() {
                        min_val = Some(val);
                    }
                }
                Ok(min_val.unwrap_or(Value::Null))
            },
            ast::AggregateType::Max => {
                let mut max_val: Option<Value> = None;
                for ctx in contexts {
                    let val = evaluate_expression_joined(&fc.args[0], ctx)?;
                    if val == Value::Null { continue; }
                    if max_val.is_none() || val > max_val.clone().unwrap() {
                        max_val = Some(val);
                    }
                }
                Ok(max_val.unwrap_or(Value::Null))
            },
            ast::AggregateType::Avg => {
                let mut sum = 0.0;
                let mut count = 0;
                for ctx in contexts {
                    let val = evaluate_expression_joined(&fc.args[0], ctx)?;
                    match val {
                        Value::Int(i) => { sum += i as f64; count += 1; },
                        Value::Float(f) => { sum += f; count += 1; },
                        Value::Null => {},
                        _ => return Err(SqlError::TypeMismatch("AVG requires numeric values".to_string())),
                    }
                }
                if count == 0 { Ok(Value::Null) } else { Ok(Value::Float(sum / count as f64)) }
            }
        }
    }
}
