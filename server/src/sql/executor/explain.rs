use super::super::ast::{self, SelectStmt};
use super::super::error::{SqlError, SqlResult};
use super::{Executor, QueryResult};
use crate::storage::{DatabaseState, TableIndex, Value};

impl Executor {
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
                Value::Text(format!(
                    "count: {}, offset: {:?}",
                    limit.count, limit.offset
                )),
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
}
