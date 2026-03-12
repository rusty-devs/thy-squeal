use super::cond::Condition;
use super::expr::Expression;
use crate::storage::{Column, ForeignKey, Privilege};
use serde::{Deserialize, Serialize};

/// Squeal Internal Representation (IR) of a query.
/// This layer decouples the execution engine from the parser AST.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub enum Squeal {
    CreateTable(CreateTable),
    CreateMaterializedView(CreateMaterializedView),
    AlterTable(AlterTable),
    DropTable(DropTable),
    CreateIndex(CreateIndex),
    CreateUser(CreateUser),
    DropUser(DropUser),
    Grant(Grant),
    Revoke(Revoke),
    Select(Select),
    Insert(Insert),
    Update(Update),
    Delete(Delete),
    Explain(Select),
    Search(Search),
    Prepare(Prepare),
    Execute(Execute),
    Deallocate(String),
    Begin,
    Commit,
    Rollback,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct Select {
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

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct WithClause {
    pub ctes: Vec<Cte>,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct Cte {
    pub name: String,
    pub query: Select,
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
pub struct Insert {
    pub table: String,
    pub columns: Option<Vec<String>>,
    pub values: Vec<Expression>,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct Update {
    pub table: String,
    pub assignments: Vec<(String, Expression)>,
    pub where_clause: Option<Condition>,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct Delete {
    pub table: String,
    pub where_clause: Option<Condition>,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct CreateTable {
    pub name: String,
    pub columns: Vec<Column>,
    pub primary_key: Option<Vec<String>>,
    pub foreign_keys: Vec<ForeignKey>,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct CreateMaterializedView {
    pub name: String,
    pub query: Select,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct AlterTable {
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
pub struct CreateIndex {
    pub name: String,
    pub table: String,
    pub expressions: Vec<Expression>,
    pub unique: bool,
    pub index_type: IndexType,
    pub where_clause: Option<Condition>,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub enum IndexType {
    BTree,
    Hash,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct DropTable {
    pub name: String,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct CreateUser {
    pub username: String,
    pub password: String,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct DropUser {
    pub username: String,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct Grant {
    pub privileges: Vec<Privilege>,
    pub table: Option<String>,
    pub username: String,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct Revoke {
    pub privileges: Vec<Privilege>,
    pub table: Option<String>,
    pub username: String,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct Search {
    pub table: String,
    pub query: String,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct Prepare {
    pub name: String,
    pub sql: String,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct Execute {
    pub name: String,
    pub params: Vec<Expression>,
}
