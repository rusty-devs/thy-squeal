use super::types::DataType;
use super::value::Value;
use serde::{Deserialize, Serialize};

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct Column {
    pub name: String,
    pub data_type: DataType,
}

#[allow(dead_code)]
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct RowId(pub String);

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Row {
    pub id: String,
    pub values: Vec<Value>,
}
