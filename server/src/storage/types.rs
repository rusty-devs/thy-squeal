use serde::{Deserialize, Serialize};

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub enum DataType {
    Int,
    Float,
    Bool,
    Date,
    DateTime,
    VarChar,
    Text,
    Blob,
    Json,
}

impl DataType {
    pub fn from_str(s: &str) -> Self {
        match s.to_uppercase().as_str() {
            "INT" | "INTEGER" => DataType::Int,
            "FLOAT" | "DOUBLE" | "REAL" => DataType::Float,
            "BOOL" | "BOOLEAN" => DataType::Bool,
            "DATE" => DataType::Date,
            "DATETIME" => DataType::DateTime,
            "VARCHAR" | "TEXT" | "STRING" => DataType::Text,
            "BLOB" | "BINARY" => DataType::Blob,
            "JSON" | "JSONB" => DataType::Json,
            _ => DataType::Text,
        }
    }

    #[allow(dead_code)]
    pub fn to_sql(&self) -> String {
        match self {
            DataType::Int => "INT".to_string(),
            DataType::Float => "FLOAT".to_string(),
            DataType::Bool => "BOOL".to_string(),
            DataType::Date => "DATE".to_string(),
            DataType::DateTime => "DATETIME".to_string(),
            DataType::VarChar => "VARCHAR".to_string(),
            DataType::Text => "TEXT".to_string(),
            DataType::Blob => "BLOB".to_string(),
            DataType::Json => "JSON".to_string(),
        }
    }
}
