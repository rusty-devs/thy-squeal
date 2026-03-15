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
    for sys_view in &[
        "tables",
        "columns",
        "indexes",
        "schemata",
        "statistics",
        "key_column_usage",
        "kv_strings",
        "kv_hash",
        "kv_list",
        "kv_set",
        "kv_zset",
        "kv_stream",
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

    // 4. information_schema.statistics
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
            for (i, expr) in index.expressions().iter().enumerate() {
                let col_name: String = match expr {
                    crate::squeal::Expression::Column(c) => c.clone(),
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

    // 6. information_schema.indexes
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

    // 7. information_schema.kv_strings
    let kv_strings_cols = vec![
        Column {
            name: "key".to_string(),
            data_type: DataType::Text,
            is_auto_increment: false,
        },
        Column {
            name: "value".to_string(),
            data_type: DataType::Text,
            is_auto_increment: false,
        },
        Column {
            name: "expiry".to_string(),
            data_type: DataType::Int,
            is_auto_increment: false,
        },
    ];
    let mut kv_strings_table = Table::new("kv_strings".to_string(), kv_strings_cols, None, vec![]);
    let now = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap()
        .as_millis() as u64;
    for (key, value) in &db_state.kv {
        let expiry = db_state.kv_expiry.get(key).map(|&e| e as i64).unwrap_or(-1);
        let expiry_display = if expiry > 0 && (expiry as u64) < now {
            -2
        } else {
            expiry
        };
        kv_strings_table.data.rows.push(Row {
            id: key.clone(),
            values: vec![
                Value::Text(key.clone()),
                Value::Text(format!("{:?}", value)),
                Value::Int(expiry_display),
            ],
        });
    }
    tables.insert("kv_strings".to_string(), kv_strings_table);

    // 8. information_schema.kv_hash
    let kv_hash_cols = vec![
        Column {
            name: "key".to_string(),
            data_type: DataType::Text,
            is_auto_increment: false,
        },
        Column {
            name: "field".to_string(),
            data_type: DataType::Text,
            is_auto_increment: false,
        },
        Column {
            name: "value".to_string(),
            data_type: DataType::Text,
            is_auto_increment: false,
        },
    ];
    let mut kv_hash_table = Table::new("kv_hash".to_string(), kv_hash_cols, None, vec![]);
    for (key, hash) in &db_state.kv_hash {
        for (field, value) in hash {
            kv_hash_table.data.rows.push(Row {
                id: format!("{}_{}", key, field),
                values: vec![
                    Value::Text(key.clone()),
                    Value::Text(field.clone()),
                    Value::Text(format!("{:?}", value)),
                ],
            });
        }
    }
    tables.insert("kv_hash".to_string(), kv_hash_table);

    // 9. information_schema.kv_list
    let kv_list_cols = vec![
        Column {
            name: "key".to_string(),
            data_type: DataType::Text,
            is_auto_increment: false,
        },
        Column {
            name: "index".to_string(),
            data_type: DataType::Int,
            is_auto_increment: false,
        },
        Column {
            name: "value".to_string(),
            data_type: DataType::Text,
            is_auto_increment: false,
        },
    ];
    let mut kv_list_table = Table::new("kv_list".to_string(), kv_list_cols, None, vec![]);
    for (key, list) in &db_state.kv_list {
        for (i, value) in list.iter().enumerate() {
            kv_list_table.data.rows.push(Row {
                id: format!("{}_{}", key, i),
                values: vec![
                    Value::Text(key.clone()),
                    Value::Int(i as i64),
                    Value::Text(format!("{:?}", value)),
                ],
            });
        }
    }
    tables.insert("kv_list".to_string(), kv_list_table);

    // 10. information_schema.kv_set
    let kv_set_cols = vec![
        Column {
            name: "key".to_string(),
            data_type: DataType::Text,
            is_auto_increment: false,
        },
        Column {
            name: "member".to_string(),
            data_type: DataType::Text,
            is_auto_increment: false,
        },
    ];
    let mut kv_set_table = Table::new("kv_set".to_string(), kv_set_cols, None, vec![]);
    for (key, set) in &db_state.kv_set {
        for member in set {
            kv_set_table.data.rows.push(Row {
                id: format!("{}_{}", key, member),
                values: vec![Value::Text(key.clone()), Value::Text(member.clone())],
            });
        }
    }
    tables.insert("kv_set".to_string(), kv_set_table);

    // 11. information_schema.kv_zset
    let kv_zset_cols = vec![
        Column {
            name: "key".to_string(),
            data_type: DataType::Text,
            is_auto_increment: false,
        },
        Column {
            name: "score".to_string(),
            data_type: DataType::Float,
            is_auto_increment: false,
        },
        Column {
            name: "member".to_string(),
            data_type: DataType::Text,
            is_auto_increment: false,
        },
    ];
    let mut kv_zset_table = Table::new("kv_zset".to_string(), kv_zset_cols, None, vec![]);
    for (key, zset) in &db_state.kv_zset {
        for (score, member) in zset {
            kv_zset_table.data.rows.push(Row {
                id: format!("{}_{}", key, member),
                values: vec![
                    Value::Text(key.clone()),
                    Value::Float(*score),
                    Value::Text(member.clone()),
                ],
            });
        }
    }
    tables.insert("kv_zset".to_string(), kv_zset_table);

    // 12. information_schema.kv_stream
    let kv_stream_cols = vec![
        Column {
            name: "key".to_string(),
            data_type: DataType::Text,
            is_auto_increment: false,
        },
        Column {
            name: "id".to_string(),
            data_type: DataType::Int,
            is_auto_increment: false,
        },
        Column {
            name: "field".to_string(),
            data_type: DataType::Text,
            is_auto_increment: false,
        },
        Column {
            name: "value".to_string(),
            data_type: DataType::Text,
            is_auto_increment: false,
        },
    ];
    let mut kv_stream_table = Table::new("kv_stream".to_string(), kv_stream_cols, None, vec![]);
    for (key, stream) in &db_state.kv_stream {
        for (id, fields) in stream {
            for (field, value) in fields {
                kv_stream_table.data.rows.push(Row {
                    id: format!("{}_{}_{}", key, id, field),
                    values: vec![
                        Value::Text(key.clone()),
                        Value::Int(*id as i64),
                        Value::Text(field.clone()),
                        Value::Text(format!("{:?}", value)),
                    ],
                });
            }
        }
    }
    tables.insert("kv_stream".to_string(), kv_stream_table);

    tables
}
