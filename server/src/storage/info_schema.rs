use crate::storage::{Column, DatabaseState, DataType, Row, Table, Value};
use std::collections::HashMap;

pub fn get_info_schema_tables(db_state: &DatabaseState) -> HashMap<String, Table> {
    let mut tables = HashMap::new();

    // 1. information_schema.tables
    let tables_cols = vec![
        Column { name: "table_name".to_string(), data_type: DataType::Text },
        Column { name: "table_type".to_string(), data_type: DataType::Text },
        Column { name: "row_count".to_string(), data_type: DataType::Int },
    ];
    let mut tables_table = Table::new("tables".to_string(), tables_cols);
    for (name, table) in &db_state.tables {
        tables_table.rows.push(Row {
            id: name.clone(),
            values: vec![
                Value::Text(name.clone()),
                Value::Text("BASE TABLE".to_string()),
                Value::Int(table.rows.len() as i64),
            ],
        });
    }
    // Add info_schema tables themselves
    tables_table.rows.push(Row {
        id: "tables".to_string(),
        values: vec![Value::Text("tables".to_string()), Value::Text("SYSTEM VIEW".to_string()), Value::Int(0)],
    });
    tables_table.rows.push(Row {
        id: "columns".to_string(),
        values: vec![Value::Text("columns".to_string()), Value::Text("SYSTEM VIEW".to_string()), Value::Int(0)],
    });
    tables_table.rows.push(Row {
        id: "indexes".to_string(),
        values: vec![Value::Text("indexes".to_string()), Value::Text("SYSTEM VIEW".to_string()), Value::Int(0)],
    });
    tables.insert("tables".to_string(), tables_table);

    // 2. information_schema.columns
    let columns_cols = vec![
        Column { name: "table_name".to_string(), data_type: DataType::Text },
        Column { name: "column_name".to_string(), data_type: DataType::Text },
        Column { name: "data_type".to_string(), data_type: DataType::Text },
        Column { name: "ordinal_position".to_string(), data_type: DataType::Int },
    ];
    let mut columns_table = Table::new("columns".to_string(), columns_cols);
    for (t_name, table) in &db_state.tables {
        for (i, col) in table.columns.iter().enumerate() {
            columns_table.rows.push(Row {
                id: format!("{}_{}", t_name, col.name),
                values: vec![
                    Value::Text(t_name.clone()),
                    Value::Text(col.name.clone()),
                    Value::Text(format!("{:?}", col.data_type).to_uppercase()),
                    Value::Int((i + 1) as i64),
                ],
            });
        }
    }
    tables.insert("columns".to_string(), columns_table);

    // 3. information_schema.indexes
    let indexes_cols = vec![
        Column { name: "table_name".to_string(), data_type: DataType::Text },
        Column { name: "index_name".to_string(), data_type: DataType::Text },
        Column { name: "is_unique".to_string(), data_type: DataType::Bool },
        Column { name: "index_type".to_string(), data_type: DataType::Text },
    ];
    let mut indexes_table = Table::new("indexes".to_string(), indexes_cols);
    for (t_name, table) in &db_state.tables {
        for (idx_name, index) in &table.indexes {
            let (is_unique, idx_type) = match index {
                crate::storage::TableIndex::BTree { unique, .. } => (*unique, "BTREE"),
                crate::storage::TableIndex::Hash { unique, .. } => (*unique, "HASH"),
            };
            indexes_table.rows.push(Row {
                id: format!("{}_{}", t_name, idx_name),
                values: vec![
                    Value::Text(t_name.clone()),
                    Value::Text(idx_name.clone()),
                    Value::Bool(is_unique),
                    Value::Text(idx_type.to_string()),
                ],
            });
        }
    }
    tables.insert("indexes".to_string(), indexes_table);

    tables
}
