use crate::storage::{Column, DataType, DatabaseState, Row, Table, Value};
use std::collections::HashMap;

pub fn get_info_schema_tables(db_state: &DatabaseState) -> HashMap<String, Table> {
    let mut tables = HashMap::new();

    // 1. information_schema.schemata
    let schemata_cols = vec![
        Column {
            name: "catalog_name".to_string(),
            data_type: DataType::Text,
            is_auto_increment: false,
        },
        Column {
            name: "schema_name".to_string(),
            data_type: DataType::Text,
            is_auto_increment: false,
        },
        Column {
            name: "default_character_set_name".to_string(),
            data_type: DataType::Text,
            is_auto_increment: false,
        },
    ];
    let mut schemata_table = Table::new("schemata".to_string(), schemata_cols, None, vec![]);
    schemata_table.data.rows.push(Row {
        id: "def".to_string(),
        values: vec![
            Value::Text("def".to_string()),
            Value::Text("default".to_string()),
            Value::Text("utf8".to_string()),
        ],
    });
    tables.insert("schemata".to_string(), schemata_table);

    // 2. information_schema.tables
    let tables_cols = vec![
        Column {
            name: "table_schema".to_string(),
            data_type: DataType::Text,
            is_auto_increment: false,
        },
        Column {
            name: "table_name".to_string(),
            data_type: DataType::Text,
            is_auto_increment: false,
        },
        Column {
            name: "table_type".to_string(),
            data_type: DataType::Text,
            is_auto_increment: false,
        },
        Column {
            name: "row_count".to_string(),
            data_type: DataType::Int,
            is_auto_increment: false,
        },
    ];
    let mut tables_table = Table::new("tables".to_string(), tables_cols, None, vec![]);
    for (name, table) in &db_state.tables {
        tables_table.data.rows.push(Row {
            id: name.clone(),
            values: vec![
                Value::Text("default".to_string()),
                Value::Text(name.clone()),
                Value::Text("BASE TABLE".to_string()),
                Value::Int(table.data.rows.len() as i64),
            ],
        });
    }
    // Add system views
    for sys_view in &[
        "tables",
        "columns",
        "indexes",
        "schemata",
        "statistics",
        "key_column_usage",
    ] {
        tables_table.data.rows.push(Row {
            id: sys_view.to_string(),
            values: vec![
                Value::Text("information_schema".to_string()),
                Value::Text(sys_view.to_string()),
                Value::Text("SYSTEM VIEW".to_string()),
                Value::Int(0),
            ],
        });
    }
    tables.insert("tables".to_string(), tables_table);

    // 3. information_schema.columns
    let columns_cols = vec![
        Column {
            name: "table_schema".to_string(),
            data_type: DataType::Text,
            is_auto_increment: false,
        },
        Column {
            name: "table_name".to_string(),
            data_type: DataType::Text,
            is_auto_increment: false,
        },
        Column {
            name: "column_name".to_string(),
            data_type: DataType::Text,
            is_auto_increment: false,
        },
        Column {
            name: "data_type".to_string(),
            data_type: DataType::Text,
            is_auto_increment: false,
        },
        Column {
            name: "ordinal_position".to_string(),
            data_type: DataType::Int,
            is_auto_increment: false,
        },
        Column {
            name: "is_auto_increment".to_string(),
            data_type: DataType::Bool,
            is_auto_increment: false,
        },
    ];
    let mut columns_table = Table::new("columns".to_string(), columns_cols, None, vec![]);
    for (t_name, table) in &db_state.tables {
        for (i, col) in table.schema.columns.iter().enumerate() {
            columns_table.data.rows.push(Row {
                id: format!("{}_{}", t_name, col.name),
                values: vec![
                    Value::Text("default".to_string()),
                    Value::Text(t_name.clone()),
                    Value::Text(col.name.clone()),
                    Value::Text(format!("{:?}", col.data_type).to_uppercase()),
                    Value::Int((i + 1) as i64),
                    Value::Bool(col.is_auto_increment),
                ],
            });
        }
    }
    tables.insert("columns".to_string(), columns_table);

    // 4. information_schema.statistics (Index details)
    let stats_cols = vec![
        Column {
            name: "table_schema".to_string(),
            data_type: DataType::Text,
            is_auto_increment: false,
        },
        Column {
            name: "table_name".to_string(),
            data_type: DataType::Text,
            is_auto_increment: false,
        },
        Column {
            name: "non_unique".to_string(),
            data_type: DataType::Int,
            is_auto_increment: false,
        },
        Column {
            name: "index_name".to_string(),
            data_type: DataType::Text,
            is_auto_increment: false,
        },
        Column {
            name: "seq_in_index".to_string(),
            data_type: DataType::Int,
            is_auto_increment: false,
        },
        Column {
            name: "column_name".to_string(),
            data_type: DataType::Text,
            is_auto_increment: false,
        },
        Column {
            name: "index_type".to_string(),
            data_type: DataType::Text,
            is_auto_increment: false,
        },
        Column {
            name: "cardinality".to_string(),
            data_type: DataType::Int,
            is_auto_increment: false,
        },
        Column {
            name: "total_rows".to_string(),
            data_type: DataType::Int,
            is_auto_increment: false,
        },
    ];
    let mut stats_table = Table::new("statistics".to_string(), stats_cols, None, vec![]);
    for (t_name, table) in &db_state.tables {
        for (idx_name, index) in &table.indexes.secondary {
            let non_unique = if index.is_unique() { 0 } else { 1 };
            let idx_type = match index {
                crate::storage::TableIndex::BTree { .. } => "BTREE",
                crate::storage::TableIndex::Hash { .. } => "HASH",
            };

            let cardinality = index.key_count();
            let total_rows = index.total_rows();

            // Extract column names from index expressions if possible
            for (i, expr) in index.expressions().iter().enumerate() {
                let col_name: String = match expr {
                    crate::sql::squeal::Expression::Column(c) => c.clone(),
                    _ => format!("expr_{}", i),
                };

                stats_table.data.rows.push(Row {
                    id: format!("{}_{}_{}", t_name, idx_name, i),
                    values: vec![
                        Value::Text("default".to_string()),
                        Value::Text(t_name.clone()),
                        Value::Int(non_unique as i64),
                        Value::Text(idx_name.clone()),
                        Value::Int((i + 1) as i64),
                        Value::Text(col_name),
                        Value::Text(idx_type.to_string()),
                        Value::Int(cardinality as i64),
                        Value::Int(total_rows as i64),
                    ],
                });
            }
        }
    }
    tables.insert("statistics".to_string(), stats_table);

    // 5. information_schema.key_column_usage
    let kcu_cols = vec![
        Column {
            name: "constraint_schema".to_string(),
            data_type: DataType::Text,
            is_auto_increment: false,
        },
        Column {
            name: "constraint_name".to_string(),
            data_type: DataType::Text,
            is_auto_increment: false,
        },
        Column {
            name: "table_schema".to_string(),
            data_type: DataType::Text,
            is_auto_increment: false,
        },
        Column {
            name: "table_name".to_string(),
            data_type: DataType::Text,
            is_auto_increment: false,
        },
        Column {
            name: "column_name".to_string(),
            data_type: DataType::Text,
            is_auto_increment: false,
        },
        Column {
            name: "referenced_table_schema".to_string(),
            data_type: DataType::Text,
            is_auto_increment: false,
        },
        Column {
            name: "referenced_table_name".to_string(),
            data_type: DataType::Text,
            is_auto_increment: false,
        },
        Column {
            name: "referenced_column_name".to_string(),
            data_type: DataType::Text,
            is_auto_increment: false,
        },
    ];
    let mut kcu_table = Table::new("key_column_usage".to_string(), kcu_cols, None, vec![]);
    for (t_name, table) in &db_state.tables {
        // Primary Key
        if let Some(ref pk_cols) = table.schema.primary_key {
            for col_name in pk_cols {
                let col_name: String = col_name.clone();
                kcu_table.data.rows.push(Row {
                    id: format!("{}_pk_{}", t_name, col_name),
                    values: vec![
                        Value::Text("default".to_string()),
                        Value::Text("PRIMARY".to_string()),
                        Value::Text("default".to_string()),
                        Value::Text(t_name.clone()),
                        Value::Text(col_name),
                        Value::Null,
                        Value::Null,
                        Value::Null,
                    ],
                });
            }
        }

        // Foreign Keys
        for fk in &table.schema.foreign_keys {
            let constraint_name = format!("fk_{}_{}", t_name, fk.ref_table);
            for (i, col_name) in fk.columns.iter().enumerate() {
                kcu_table.data.rows.push(Row {
                    id: format!("{}_{}_{}", t_name, constraint_name, i),
                    values: vec![
                        Value::Text("default".to_string()),
                        Value::Text(constraint_name.clone()),
                        Value::Text("default".to_string()),
                        Value::Text(t_name.clone()),
                        Value::Text(col_name.clone()),
                        Value::Text("default".to_string()),
                        Value::Text(fk.ref_table.clone()),
                        Value::Text(fk.ref_columns[i].clone()),
                    ],
                });
            }
        }
    }
    tables.insert("key_column_usage".to_string(), kcu_table);

    // 6. information_schema.indexes (compatibility with earlier versions)
    let indexes_cols = vec![
        Column {
            name: "table_name".to_string(),
            data_type: DataType::Text,
            is_auto_increment: false,
        },
        Column {
            name: "index_name".to_string(),
            data_type: DataType::Text,
            is_auto_increment: false,
        },
        Column {
            name: "is_unique".to_string(),
            data_type: DataType::Bool,
            is_auto_increment: false,
        },
        Column {
            name: "index_type".to_string(),
            data_type: DataType::Text,
            is_auto_increment: false,
        },
    ];
    let mut indexes_table = Table::new("indexes".to_string(), indexes_cols, None, vec![]);
    for (t_name, table) in &db_state.tables {
        for (idx_name, index) in &table.indexes.secondary {
            let (is_unique, idx_type) = match index {
                crate::storage::TableIndex::BTree { unique, .. } => (*unique, "BTREE"),
                crate::storage::TableIndex::Hash { unique, .. } => (*unique, "HASH"),
            };
            indexes_table.data.rows.push(Row {
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
