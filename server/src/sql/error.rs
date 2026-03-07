use serde::Serialize;
use thiserror::Error;
use crate::storage::StorageError;

#[derive(Error, Debug, Serialize)]
#[serde(tag = "type", content = "details")]
pub enum SqlError {
    #[error("Parse error: {0}")]
    Parse(String),

    #[error("Table not found: {0}")]
    TableNotFound(String),

    #[error("Column not found: {0}")]
    ColumnNotFound(String),

    #[error("Type mismatch: {0}")]
    TypeMismatch(String),

    #[error("Execution error: {0}")]
    Runtime(String),

    #[error("Internal storage error: {0}")]
    Storage(String),
}

impl From<StorageError> for SqlError {
    fn from(err: StorageError) -> Self {
        match err {
            StorageError::TableNotFound(name) => SqlError::TableNotFound(name),
            StorageError::ColumnNotFound(name) => SqlError::ColumnNotFound(name),
            StorageError::RowNotFound(id) => SqlError::Runtime(format!("Row not found: {}", id)),
            StorageError::InvalidType(msg) => SqlError::TypeMismatch(msg),
            StorageError::DuplicateKey(msg) => SqlError::Runtime(format!("Duplicate key: {}", msg)),
            StorageError::PersistenceError(msg) => SqlError::Storage(msg),
        }
    }
}

impl From<String> for SqlError {
    fn from(err: String) -> Self {
        SqlError::Runtime(err)
    }
}

pub type SqlResult<T> = Result<T, SqlError>;
