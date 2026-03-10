use thiserror::Error;

pub type SqlResult<T> = std::result::Result<T, SqlError>;

#[derive(Error, Debug, Clone, serde::Serialize, serde::Deserialize)]
pub enum SqlError {
    #[error("Parse error: {0}")]
    Parse(String),

    #[error("Table not found: {0}")]
    TableNotFound(String),

    #[error("Column not found: {0}")]
    ColumnNotFound(String),

    #[error("Duplicate key: {0}")]
    DuplicateKey(String),

    #[error("Type mismatch: {0}")]
    TypeMismatch(String),

    #[error("Execution error: {0}")]
    Runtime(String),

    #[error("Permission denied: {0}")]
    PermissionDenied(String),

    #[error("Internal storage error: {0}")]
    Storage(String),
}
