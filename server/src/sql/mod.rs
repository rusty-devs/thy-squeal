use crate::storage::Database;
use crate::storage::Value;
use crate::storage::{Column, DataType};

#[derive(Debug)]
pub struct QueryResult {
    pub columns: Vec<String>,
    pub rows: Vec<Vec<Value>>,
    pub rows_affected: u64,
}

pub struct Executor {
    db: tokio::sync::RwLock<Database>,
}

impl Executor {
    pub fn new() -> Self {
        Self {
            db: tokio::sync::RwLock::new(Database::new()),
        }
    }

    pub fn db(&self) -> &tokio::sync::RwLock<Database> {
        &self.db
    }

    pub async fn execute(&self, sql: &str) -> Result<QueryResult, String> {
        let sql = sql.trim();
        
        // Simple SQL parsing
        let sql_upper = sql.to_uppercase();
        
        // CREATE TABLE
        if sql_upper.starts_with("CREATE TABLE") {
            return self.create_table(sql).await;
        }
        
        // DROP TABLE
        if sql_upper.starts_with("DROP TABLE") {
            return self.drop_table(sql).await;
        }
        
        // SELECT
        if sql_upper.starts_with("SELECT") {
            return self.select(sql).await;
        }
        
        // INSERT
        if sql_upper.starts_with("INSERT") {
            return self.insert(sql).await;
        }
        
        Err(format!("Unsupported SQL: {}", sql))
    }

    async fn create_table(&self, sql: &str) -> Result<QueryResult, String> {
        // Simple parser: CREATE TABLE name (col1 TYPE1, col2 TYPE2, ...)
        let sql = sql.trim().trim_end_matches(';');
        let rest = sql.strip_prefix("CREATE TABLE").unwrap().trim();
        
        let (name, columns_part) = rest.split_once('(').ok_or("Invalid CREATE TABLE syntax")?;
        let name = name.trim();
        let columns_part = columns_part.trim_end_matches(')');
        
        let mut columns = Vec::new();
        for col_def in columns_part.split(',') {
            let col_def = col_def.trim();
            let parts: Vec<&str> = col_def.split_whitespace().collect();
            if parts.len() >= 2 {
                let col_name = parts[0].to_string();
                let data_type = DataType::from_str(parts[1]);
                columns.push(Column { name: col_name, data_type });
            }
        }
        
        let mut db = self.db.write().await;
        db.create_table(name.to_string(), columns).map_err(|e| e.to_string())?;
        
        Ok(QueryResult {
            columns: vec![],
            rows: vec![],
            rows_affected: 0,
        })
    }

    async fn drop_table(&self, sql: &str) -> Result<QueryResult, String> {
        let sql = sql.trim().trim_end_matches(';');
        let name = sql.strip_prefix("DROP TABLE").unwrap().trim();
        
        let mut db = self.db.write().await;
        db.drop_table(name).map_err(|e| e.to_string())?;
        
        Ok(QueryResult {
            columns: vec![],
            rows: vec![],
            rows_affected: 0,
        })
    }

    async fn select(&self, sql: &str) -> Result<QueryResult, String> {
        let sql = sql.trim().trim_end_matches(';');
        
        // Simple parser: SELECT cols FROM table
        let parts: Vec<&str> = sql.splitn(4, |c| c == ' ' || c == ',').collect();
        
        let mut columns = Vec::new();
        let mut table_name = String::new();
        let mut in_cols = false;
        let mut in_from = false;
        
        for part in sql.split_whitespace() {
            let part_upper = part.to_uppercase();
            if part_upper == "SELECT" {
                in_cols = true;
                in_from = false;
            } else if part_upper == "FROM" {
                in_cols = false;
                in_from = true;
            } else if part_upper == "WHERE" || part_upper == "ORDER" || part_upper == "LIMIT" {
                break;
            } else if in_cols {
                if part != "," {
                    columns.push(part.to_string());
                }
            } else if in_from {
                table_name = part.trim_end_matches(';').to_string();
                break;
            }
        }
        
        if columns.is_empty() {
            columns.push("*".to_string());
        }
        
        let db = self.db.read().await;
        let table = db.get_table(&table_name)
            .ok_or_else(|| format!("Table not found: {}", table_name))?;
        
        let rows: Vec<Vec<Value>> = table.rows.iter().map(|row| {
            if columns.iter().any(|c| c == "*") {
                row.values.clone()
            } else {
                columns.iter().filter_map(|col| {
                    table.column_index(col).and_then(|idx| row.values.get(idx).cloned())
                }).collect()
            }
        }).collect();
        
        let result_columns = if columns.iter().any(|c| c == "*") {
            table.columns.iter().map(|c| c.name.clone()).collect()
        } else {
            columns
        };
        
        Ok(QueryResult {
            columns: result_columns,
            rows,
            rows_affected: 0,
        })
    }

    async fn insert(&self, sql: &str) -> Result<QueryResult, String> {
        // INSERT INTO table (col1, col2) VALUES (val1, val2)
        let sql = sql.trim().trim_end_matches(';');
        
        let parts: Vec<&str> = sql.splitn(5, |c| c == ' ' || c == '(' || c == ')' || c == ',').collect();
        
        // Find table name
        let mut table_name = String::new();
        let mut in_into = false;
        
        for part in sql.split_whitespace() {
            let part_upper = part.to_uppercase();
            if part_upper == "INTO" {
                in_into = true;
            } else if in_into && !part_upper.is_empty() {
                table_name = part.trim_end_matches('(').to_string();
                break;
            }
        }
        
        if table_name.is_empty() {
            return Err("Invalid INSERT syntax".to_string());
        }
        
        // Parse VALUES
        if let Some(values_part) = sql.split("VALUES").nth(1) {
            let values_str = values_part.trim().trim_start_matches('(').trim_end_matches(')');
            let values: Vec<Value> = values_str
                .split(',')
                .map(|v| {
                    let v = v.trim();
                    if v.starts_with('\'') {
                        Value::Text(v.trim_matches('\'').to_string())
                    } else if v == "NULL" {
                        Value::Null
                    } else if v.to_lowercase() == "true" || v.to_lowercase() == "false" {
                        Value::Bool(v.to_lowercase() == "true")
                    } else if let Ok(n) = v.parse::<i64>() {
                        Value::Int(n)
                    } else if let Ok(f) = v.parse::<f64>() {
                        Value::Float(f)
                    } else {
                        Value::Text(v.to_string())
                    }
                })
                .collect();
            
            let mut db = self.db.write().await;
            let table = db.get_table_mut(&table_name)
                .ok_or_else(|| format!("Table not found: {}", table_name))?;
            
            table.insert(values).map_err(|e| e.to_string())?;
            
            return Ok(QueryResult {
                columns: vec![],
                rows: vec![],
                rows_affected: 1,
            });
        }
        
        Err("Invalid INSERT syntax".to_string())
    }
}
