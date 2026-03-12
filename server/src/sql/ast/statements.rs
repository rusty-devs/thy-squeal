use super::condition::Condition;
use super::expression::Expression;
use crate::storage::{Column, ForeignKey, Privilege};
use serde::{Deserialize, Serialize};

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct PrepareStmt {
    pub name: String,
    pub sql: String,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct ExecuteStmt {
    pub name: String,
    pub params: Vec<Expression>,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct SearchStmt {
    pub table: String,
    pub query: String,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct UpdateStmt {
    pub table: String,
    pub assignments: Vec<(String, Expression)>,
    pub where_clause: Option<Condition>,
}

impl UpdateStmt {
    pub fn resolve_placeholders(&mut self, counter: &mut usize) {
        for (_, expr) in &mut self.assignments {
            expr.resolve_placeholders(counter);
        }
        if let Some(c) = &mut self.where_clause {
            c.resolve_placeholders(counter);
        }
    }
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct DeleteStmt {
    pub table: String,
    pub where_clause: Option<Condition>,
}

impl DeleteStmt {
    pub fn resolve_placeholders(&mut self, counter: &mut usize) {
        if let Some(c) = &mut self.where_clause {
            c.resolve_placeholders(counter);
        }
    }
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct CreateTableStmt {
    pub name: String,
    pub columns: Vec<Column>,
    pub primary_key: Option<Vec<String>>,
    pub foreign_keys: Vec<ForeignKey>,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct CreateMaterializedViewStmt {
    pub name: String,
    pub query: SelectStmt,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct AlterTableStmt {
    pub table: String,
    pub action: AlterAction,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub enum AlterAction {
    AddColumn(Column),
    DropColumn(String),
    RenameColumn { old_name: String, new_name: String },
    RenameTable(String),
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct CreateIndexStmt {
    pub name: String,
    pub table: String,
    pub expressions: Vec<Expression>,
    pub unique: bool,
    pub index_type: IndexType,
    pub where_clause: Option<Condition>,
}

impl CreateIndexStmt {
    pub fn resolve_placeholders(&mut self, counter: &mut usize) {
        for expr in &mut self.expressions {
            expr.resolve_placeholders(counter);
        }
        if let Some(c) = &mut self.where_clause {
            c.resolve_placeholders(counter);
        }
    }
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub enum IndexType {
    BTree,
    Hash,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct DropTableStmt {
    pub name: String,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct SelectStmt {
    pub with_clause: Option<WithClause>,
    pub columns: Vec<SelectColumn>,
    pub table: String,
    pub table_alias: Option<String>,
    pub distinct: bool,
    pub joins: Vec<Join>,
    pub where_clause: Option<Condition>,
    pub group_by: Vec<Expression>,
    pub having: Option<Condition>,
    pub order_by: Vec<OrderByItem>,
    pub limit: Option<LimitClause>,
}

impl SelectStmt {
    pub fn resolve_placeholders(&mut self, counter: &mut usize) {
        if let Some(with) = &mut self.with_clause {
            for cte in &mut with.ctes {
                cte.query.resolve_placeholders(counter);
            }
        }
        for col in &mut self.columns {
            col.expr.resolve_placeholders(counter);
        }
        for join in &mut self.joins {
            join.on.resolve_placeholders(counter);
        }
        if let Some(c) = &mut self.where_clause {
            c.resolve_placeholders(counter);
        }
        for expr in &mut self.group_by {
            expr.resolve_placeholders(counter);
        }
        if let Some(c) = &mut self.having {
            c.resolve_placeholders(counter);
        }
        for item in &mut self.order_by {
            item.expr.resolve_placeholders(counter);
        }
    }
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct WithClause {
    pub ctes: Vec<Cte>,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct Cte {
    pub name: String,
    pub query: SelectStmt,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct Join {
    pub table: String,
    pub table_alias: Option<String>,
    pub join_type: JoinType,
    pub on: Condition,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub enum JoinType {
    Inner,
    Left,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct SelectColumn {
    pub expr: Expression,
    pub alias: Option<String>,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct CreateUserStmt {
    pub username: String,
    pub password: String,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct DropUserStmt {
    pub username: String,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct GrantStmt {
    pub privileges: Vec<Privilege>,
    pub table: Option<String>, // None means GLOBAL
    pub username: String,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct RevokeStmt {
    pub privileges: Vec<Privilege>,
    pub table: Option<String>,
    pub username: String,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct OrderByItem {
    pub expr: Expression,
    pub order: Order,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub enum Order {
    Asc,
    Desc,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct LimitClause {
    pub count: usize,
    pub offset: Option<usize>,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct InsertStmt {
    pub table: String,
    pub columns: Option<Vec<String>>,
    pub values: Vec<Expression>,
}

impl InsertStmt {
    pub fn resolve_placeholders(&mut self, counter: &mut usize) {
        for expr in &mut self.values {
            expr.resolve_placeholders(counter);
        }
    }
}
