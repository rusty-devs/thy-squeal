use super::super::error::SqlResult;
use super::{Executor, QueryResult};

impl Executor {
    pub async fn dump(&self) -> SqlResult<String> {
        let db = self.db.read().await;
        let state = db.state();
        let mut sql = String::new();

        // 1. Users
        for user in state.users.values() {
            if user.username == "root" {
                continue;
            }
            sql.push_str(&format!(
                "CREATE USER '{}' IDENTIFIED BY '...';\n",
                user.username
            ));
            // Privileges would need more complex serialization
        }

        // 2. Tables and Data
        for table in state.tables.values() {
            // CREATE TABLE
            sql.push_str(&format!("CREATE TABLE {} (\n", table.schema.name));
            for (i, col) in table.schema.columns.iter().enumerate() {
                sql.push_str(&format!("  {} {}", col.name, col.data_type.to_sql()));
                if col.is_auto_increment {
                    sql.push_str(" AUTO_INCREMENT");
                }
                if i < table.schema.columns.len() - 1 {
                    sql.push_str(",\n");
                } else {
                    sql.push('\n');
                }
            }
            sql.push_str(");\n");

            // Indexes
            for (index_name, index) in &table.indexes.secondary {
                let unique = if index.is_unique() { "UNIQUE " } else { "" };
                let exprs: Vec<String> = index.expressions().iter().map(|e| e.to_sql()).collect();
                sql.push_str(&format!(
                    "CREATE {}INDEX {} ON {} ({});\n",
                    unique,
                    index_name,
                    table.schema.name,
                    exprs.join(", ")
                ));
            }

            // INSERTs
            for row in &table.data.rows {
                let values: Vec<String> = row.values.iter().map(|v| v.to_sql()).collect();
                sql.push_str(&format!(
                    "INSERT INTO {} VALUES ({});\n",
                    table.schema.name,
                    values.join(", ")
                ));
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

        // Simple semicolon split (not perfect for strings containing semicolons)
        let mut current_stmt = String::new();
        for line in sql.lines() {
            let line = line.trim();
            if line.is_empty() || line.starts_with("--") {
                continue;
            }
            current_stmt.push_str(line);
            current_stmt.push(' ');

            if line.ends_with(';') {
                let stmt_sql = current_stmt.trim_end_matches(';').trim();
                if !stmt_sql.is_empty() {
                    let res = self.execute(stmt_sql, vec![], None, None).await?;
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
