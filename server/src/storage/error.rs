use thiserror::Error;

#[derive(Error, Debug)]
pub enum StorageError {
    #[error("Table not found: {0}")]
    TableNotFound(String),
    #[error("Row not found: {0}")]
    RowNotFound(String),
    #[error("Column not found: {0}")]
    ColumnNotFound(String),
    #[error("Invalid type: {0}")]
    InvalidType(String),
    #[error("Duplicate key: {0}")]
    DuplicateKey(String),
    #[error("Persistence error: {0}")]
    PersistenceError(String),
}
