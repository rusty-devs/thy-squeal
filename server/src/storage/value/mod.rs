pub mod cast;
pub mod ops;

use super::types::DataType;
use serde::{Deserialize, Serialize};

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub enum Value {
    Null,
    Int(i64),
    Float(f64),
    Text(String),
    Bool(bool),
    DateTime(chrono::DateTime<chrono::Utc>),
    Json(serde_json::Value),
}

impl Value {
    pub fn data_type(&self) -> DataType {
        match self {
            Value::Null => DataType::Text, // Null can be any type, default to Text
            Value::Int(_) => DataType::Int,
            Value::Float(_) => DataType::Float,
            Value::Text(_) => DataType::Text,
            Value::Bool(_) => DataType::Bool,
            Value::DateTime(_) => DataType::DateTime,
            Value::Json(_) => DataType::Json,
        }
    }

    pub fn from_json(v: serde_json::Value) -> Self {
        match v {
            serde_json::Value::Null => Value::Null,
            serde_json::Value::Bool(b) => Value::Bool(b),
            serde_json::Value::Number(n) => {
                if let Some(i) = n.as_i64() {
                    Value::Int(i)
                } else if let Some(f) = n.as_f64() {
                    Value::Float(f)
                } else {
                    Value::Null
                }
            }
            serde_json::Value::String(s) => Value::Text(s),
            _ => Value::Json(v),
        }
    }
}

impl Eq for Value {}
