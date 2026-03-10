use super::super::error::{SqlError, SqlResult};
use crate::storage::{Row, Table, Value};

pub fn resolve_column(name: &str, contexts: &[(&Table, Option<&str>, &Row)]) -> SqlResult<Value> {
    if name.contains('.') {
        let parts: Vec<&str> = name.split('.').collect();
        // Case 1: table_or_alias.column[.json_path]
        for (table, alias, row) in contexts {
            // Rule: If alias exists, original name is hidden.
            let matches_table = match alias {
                Some(a) => *a == parts[0],
                None => table.name == parts[0],
            };

            if matches_table && let Some(idx) = table.column_index(parts[1]) {
                let mut current_val = row.values.get(idx).cloned().ok_or_else(|| {
                    SqlError::Runtime(format!("Value not found for column index: {}", idx))
                })?;

                // JSON path traversal
                for part in &parts[2..] {
                    current_val = match current_val {
                        Value::Json(v) => v
                            .get(*part)
                            .map(|inner| Value::from_json(inner.clone()))
                            .unwrap_or(Value::Null),
                        _ => Value::Null,
                    };
                    if current_val == Value::Null {
                        break;
                    }
                }
                return Ok(current_val);
            }
        }

        // Case 2: column.json_path (no table prefix)
        for (table, _alias, row) in contexts {
            if let Some(idx) = table.column_index(parts[0]) {
                let mut current_val = row.values.get(idx).cloned().ok_or_else(|| {
                    SqlError::Runtime(format!("Value not found for column index: {}", idx))
                })?;

                for part in &parts[1..] {
                    current_val = match current_val {
                        Value::Json(v) => v
                            .get(*part)
                            .map(|inner| Value::from_json(inner.clone()))
                            .unwrap_or(Value::Null),
                        _ => Value::Null,
                    };
                    if current_val == Value::Null {
                        break;
                    }
                }
                return Ok(current_val);
            }
        }
    } else {
        // Simple column
        for (table, _alias, row) in contexts {
            if let Some(idx) = table.column_index(name) {
                return row.values.get(idx).cloned().ok_or_else(|| {
                    SqlError::Runtime(format!("Value not found for column index: {}", idx))
                });
            }
        }
    }
    Err(SqlError::ColumnNotFound(name.to_string()))
}
