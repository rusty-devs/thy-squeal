use crate::storage::{Column, DataType};
use super::super::ast::{CreateIndexStmt, CreateTableStmt, DropTableStmt, SqlStmt, IndexType};
use super::super::error::{SqlError, SqlResult};
use super::super::parser::Rule;
use super::utils::expect_identifier;

pub fn parse_create_table(pair: pest::iterators::Pair<Rule>) -> SqlResult<SqlStmt> {
    let mut inner = pair.into_inner();
    let name = inner
        .find(|p| p.as_rule() == Rule::table_name)
        .map(|p| p.as_str().trim().to_string())
        .ok_or_else(|| SqlError::Parse("Missing table name".to_string()))?;
    let column_defs = inner
        .find(|p| p.as_rule() == Rule::column_defs)
        .ok_or_else(|| SqlError::Parse("Missing column definitions".to_string()))?
        .into_inner();

    let mut columns = Vec::new();
    for col_def in column_defs {
        if col_def.as_rule() != Rule::column_def {
            continue;
        }
        let mut col_inner = col_def.into_inner();
        let col_name = expect_identifier(col_inner.find(|p| p.as_rule() == Rule::identifier), "column name")?;
        let type_str = col_inner
            .find(|p| p.as_rule() == Rule::data_type)
            .ok_or_else(|| SqlError::Parse("Missing column type".to_string()))?
            .as_str()
            .to_uppercase();
        columns.push(Column {
            name: col_name,
            data_type: DataType::from_str(&type_str),
        });
    }

    Ok(SqlStmt::CreateTable(CreateTableStmt { name, columns }))
}

pub fn parse_drop_table(pair: pest::iterators::Pair<Rule>) -> SqlResult<SqlStmt> {
    let inner = pair.into_inner();
    let name = inner
        .filter(|p| p.as_rule() == Rule::table_name)
        .last()
        .map(|p| p.as_str().trim().to_string())
        .ok_or_else(|| SqlError::Parse("Missing table name".to_string()))?;
    Ok(SqlStmt::DropTable(DropTableStmt { name }))
}

pub fn parse_create_index(pair: pest::iterators::Pair<Rule>) -> SqlResult<SqlStmt> {
    let mut inner = pair.into_inner().peekable();
    
    // Skip KW_CREATE
    let _ = inner.next();

    // Check for optional UNIQUE
    let mut unique = false;
    if let Some(next) = inner.peek() {
        if next.as_rule() == Rule::unique {
            unique = true;
            inner.next(); // consume unique
        }
    }

    // Skip KW_INDEX
    let _ = inner.next();

    // Index name
    let index_name = inner.find(|p| p.as_rule() == Rule::identifier)
        .map(|p| p.as_str().trim().to_string())
        .ok_or_else(|| SqlError::Parse("Missing index name".to_string()))?;
    
    // Table name
    let table = inner.find(|p| p.as_rule() == Rule::table_name)
        .map(|p| p.as_str().trim().to_string())
        .ok_or_else(|| SqlError::Parse("Missing table name".to_string()))?;
    
    // Columns list (may include paths)
    let id_list = inner.find(|p| p.as_rule() == Rule::index_column_list)
        .ok_or_else(|| SqlError::Parse("Missing column list for index".to_string()))?;
    
    let columns: Vec<String> = id_list.into_inner()
        .filter(|p| p.as_rule() == Rule::index_column)
        .map(|p| p.as_str().trim().to_string())
        .collect();

    if columns.is_empty() {
        return Err(SqlError::Parse("Index must have at least one column".to_string()));
    }

    // Optional USING clause
    let mut index_type = IndexType::BTree;
    if let Some(type_clause) = inner.find(|p| p.as_rule() == Rule::index_type_clause) {
        let type_inner = type_clause.into_inner().find(|p| p.as_rule() == Rule::index_type)
            .ok_or_else(|| SqlError::Parse("Missing index type".to_string()))?;
        
        let type_str = type_inner.as_str().to_uppercase();
        if type_str == "HASH" {
            index_type = IndexType::Hash;
        }
    }

    Ok(SqlStmt::CreateIndex(CreateIndexStmt { 
        name: index_name, 
        table, 
        columns, 
        unique, 
        index_type 
    }))
}
