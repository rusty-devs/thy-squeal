use super::super::error::SqlResult;
use super::{Executor, QueryResult};
use crate::storage::TableIndex;

impl Executor {
    pub async fn dump(&self) -> SqlResult<String> {
        let db = self.db.read().await;
        let state = db.state();
        let mut sql = String::new();

        sql.push_str("-- thy-squeal SQL Dump\n");
        sql.push_str("-- Generated automatically\n\n");

        for (table_name, table) in &state.tables {
            sql.push_str(&format!("-- Table: {}\n", table_name));

            // 1. CREATE TABLE
            sql.push_str("CREATE TABLE ");
            sql.push_str(table_name);
            sql.push_str(" (");
            let cols: Vec<String> = table
                .columns
                .iter()
                .map(|c| format!("{} {}", c.name, c.data_type.to_sql()))
                .collect();
            sql.push_str(&cols.join(", "));
            sql.push_str(");\n");

            // 2. CREATE INDEXES
            for (index_name, index) in &table.indexes {
                let unique_str = if index.is_unique() { "UNIQUE " } else { "" };
                let type_str = match index {
                    TableIndex::BTree { .. } => "BTREE",
                    TableIndex::Hash { .. } => "HASH",
                };

                let exprs = index.expressions();
                let expr_strs: Vec<String> = exprs.iter().map(|e| e.to_sql()).collect();

                let mut create_idx = format!(
                    "CREATE {}INDEX {} ON {} ({}) USING {}",
                    unique_str,
                    index_name,
                    table_name,
                    expr_strs.join(", "),
                    type_str
                );

                if let Some(cond) = index.where_clause() {
                    create_idx.push_str(" WHERE ");
                    create_idx.push_str(&cond.to_sql());
                }

                sql.push_str(&create_idx);
                sql.push_str(";\n");
            }

            // 3. INSERT ROWS
            for row in &table.rows {
                sql.push_str("INSERT INTO ");
                sql.push_str(table_name);
                sql.push_str(" VALUES (");
                let vals: Vec<String> = row.values.iter().map(|v| v.to_sql()).collect();
                sql.push_str(&vals.join(", "));
                sql.push_str(");\n");
            }
            sql.push('\n');
        }

        Ok(sql)
    }

    pub async fn execute_batch(&self, sql: &str) -> SqlResult<QueryResult> {
        let mut last_res = QueryResult {
            columns: vec![],
            rows: vec![],
            rows_affected: 0,
            transaction_id: None,
        };

        let mut total_affected = 0;
        let mut current_stmt = String::new();

        for line in sql.lines() {
            let trimmed = line.trim();
            if trimmed.is_empty() || trimmed.starts_with("--") {
                continue;
            }

            current_stmt.push_str(trimmed);
            current_stmt.push(' ');

            if trimmed.ends_with(';') {
                let stmt_to_exec = current_stmt.trim().trim_end_matches(';');
                if !stmt_to_exec.is_empty() {
                    let res = self.execute(stmt_to_exec, vec![], None).await?;
                    total_affected += res.rows_affected;
                    last_res = res;
                }
                current_stmt.clear();
            }
        }

        last_res.rows_affected = total_affected;
        Ok(last_res)
    }
}
