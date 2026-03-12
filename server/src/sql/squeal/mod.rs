use super::ast::{self, SqlStmt};
use crate::storage::{Column, Privilege, Value, ForeignKey};
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

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub enum Expression {
    Literal(Value),
    Placeholder(usize),
    Column(String),
    BinaryOp(Box<Expression>, BinaryOp, Box<Expression>),
    FunctionCall(FunctionCall),
    ScalarFunc(ScalarFunction),
    Star,
    Subquery(Box<Select>),
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct FunctionCall {
    pub name: AggregateType,
    pub args: Vec<Expression>,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct ScalarFunction {
    pub name: ScalarFuncType,
    pub args: Vec<Expression>,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub enum ScalarFuncType {
    Lower, Upper, Length, Abs, Now, Concat, Coalesce, Replace,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub enum AggregateType {
    Count, Sum, Avg, Min, Max,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub enum BinaryOp {
    Add, Sub, Mul, Div,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub enum Condition {
    And(Box<Condition>, Box<Condition>),
    Or(Box<Condition>, Box<Condition>),
    Not(Box<Condition>),
    Comparison(Expression, ComparisonOp, Expression),
    In(Expression, Vec<Expression>),
    InSubquery(Expression, Box<Select>),
    Exists(Box<Select>),
    Between(Expression, Expression, Expression),
    Is(Expression, IsOp),
    Like(Expression, String),
    FullTextSearch(String, String), // field, query
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub enum ComparisonOp {
    Eq, Neq, Lt, Lte, Gt, Gte,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub enum IsOp {
    Null,
    NotNull,
    True,
    False,
}

impl Expression {
    pub fn to_sql(&self) -> String {
        match self {
            Expression::Literal(v) => format!("{:?}", v),
            Expression::Placeholder(i) => format!("?{}", i),
            Expression::Column(c) => c.clone(),
            Expression::BinaryOp(l, op, r) => format!("({} {} {})", l.to_sql(), op.to_sql(), r.to_sql()),
            Expression::FunctionCall(f) => format!("{:?}({:?})", f.name, f.args),
            Expression::ScalarFunc(f) => format!("{:?}({:?})", f.name, f.args),
            Expression::Star => "*".to_string(),
            Expression::Subquery(_) => "(subquery)".to_string(),
        }
    }
}

impl BinaryOp {
    pub fn to_sql(&self) -> &str {
        match self {
            BinaryOp::Add => "+",
            BinaryOp::Sub => "-",
            BinaryOp::Mul => "*",
            BinaryOp::Div => "/",
        }
    }
}

// Conversions from AST to Squeal IR
impl From<SqlStmt> for Squeal {
    fn from(stmt: SqlStmt) -> Self {
        match stmt {
            SqlStmt::CreateTable(s) => Squeal::CreateTable(s.into()),
            SqlStmt::CreateMaterializedView(s) => Squeal::CreateMaterializedView(s.into()),
            SqlStmt::AlterTable(s) => Squeal::AlterTable(s.into()),
            SqlStmt::DropTable(s) => Squeal::DropTable(s.into()),
            SqlStmt::CreateIndex(s) => Squeal::CreateIndex(s.into()),
            SqlStmt::CreateUser(s) => Squeal::CreateUser(s.into()),
            SqlStmt::DropUser(s) => Squeal::DropUser(s.into()),
            SqlStmt::Grant(s) => Squeal::Grant(s.into()),
            SqlStmt::Revoke(s) => Squeal::Revoke(s.into()),
            SqlStmt::Select(s) => Squeal::Select(s.into()),
            SqlStmt::Insert(s) => Squeal::Insert(s.into()),
            SqlStmt::Update(s) => Squeal::Update(s.into()),
            SqlStmt::Delete(s) => Squeal::Delete(s.into()),
            SqlStmt::Explain(s) => Squeal::Explain(s.into()),
            SqlStmt::Search(s) => Squeal::Search(s.into()),
            SqlStmt::Prepare(s) => Squeal::Prepare(s.into()),
            SqlStmt::Execute(s) => Squeal::Execute(s.into()),
            SqlStmt::Deallocate(s) => Squeal::Deallocate(s),
            SqlStmt::Begin => Squeal::Begin,
            SqlStmt::Commit => Squeal::Commit,
            SqlStmt::Rollback => Squeal::Rollback,
        }
    }
}

impl From<ast::SelectStmt> for Select {
    fn from(s: ast::SelectStmt) -> Self {
        Select {
            with_clause: s.with_clause.map(|w| w.into()),
            columns: s.columns.into_iter().map(|c| c.into()).collect(),
            table: s.table,
            table_alias: s.table_alias,
            distinct: s.distinct,
            joins: s.joins.into_iter().map(|j| j.into()).collect(),
            where_clause: s.where_clause.map(|w| w.into()),
            group_by: s.group_by.into_iter().map(|g| g.into()).collect(),
            having: s.having.map(|h| h.into()),
            order_by: s.order_by.into_iter().map(|o| o.into()).collect(),
            limit: s.limit.map(|l| l.into()),
        }
    }
}

impl From<ast::WithClause> for WithClause {
    fn from(w: ast::WithClause) -> Self {
        WithClause {
            ctes: w.ctes.into_iter().map(|c| c.into()).collect(),
        }
    }
}

impl From<ast::Cte> for Cte {
    fn from(c: ast::Cte) -> Self {
        Cte {
            name: c.name,
            query: c.query.into(),
        }
    }
}

impl From<ast::SelectColumn> for SelectColumn {
    fn from(c: ast::SelectColumn) -> Self {
        SelectColumn {
            expr: c.expr.into(),
            alias: c.alias,
        }
    }
}

impl From<ast::Join> for Join {
    fn from(j: ast::Join) -> Self {
        Join {
            table: j.table,
            table_alias: j.table_alias,
            join_type: match j.join_type {
                ast::JoinType::Inner => JoinType::Inner,
                ast::JoinType::Left => JoinType::Left,
            },
            on: j.on.into(),
        }
    }
}

impl From<ast::OrderByItem> for OrderByItem {
    fn from(o: ast::OrderByItem) -> Self {
        OrderByItem {
            expr: o.expr.into(),
            order: match o.order {
                ast::Order::Asc => Order::Asc,
                ast::Order::Desc => Order::Desc,
            },
        }
    }
}

impl From<ast::LimitClause> for LimitClause {
    fn from(l: ast::LimitClause) -> Self {
        LimitClause {
            count: l.count,
            offset: l.offset,
        }
    }
}

impl From<ast::Expression> for Expression {
    fn from(e: ast::Expression) -> Self {
        match e {
            ast::Expression::Literal(v) => Expression::Literal(v),
            ast::Expression::Placeholder(i) => Expression::Placeholder(i),
            ast::Expression::Column(c) => Expression::Column(c),
            ast::Expression::BinaryOp(l, op, r) => Expression::BinaryOp(
                Box::new((*l).into()),
                match op {
                    ast::BinaryOp::Add => BinaryOp::Add,
                    ast::BinaryOp::Sub => BinaryOp::Sub,
                    ast::BinaryOp::Mul => BinaryOp::Mul,
                    ast::BinaryOp::Div => BinaryOp::Div,
                },
                Box::new((*r).into()),
            ),
            ast::Expression::FunctionCall(f) => Expression::FunctionCall(FunctionCall {
                name: match f.name {
                    ast::AggregateType::Count => AggregateType::Count,
                    ast::AggregateType::Sum => AggregateType::Sum,
                    ast::AggregateType::Avg => AggregateType::Avg,
                    ast::AggregateType::Min => AggregateType::Min,
                    ast::AggregateType::Max => AggregateType::Max,
                },
                args: f.args.into_iter().map(|a| a.into()).collect(),
            }),
            ast::Expression::ScalarFunc(f) => Expression::ScalarFunc(ScalarFunction {
                name: match f.name {
                    ast::ScalarFuncType::Lower => ScalarFuncType::Lower,
                    ast::ScalarFuncType::Upper => ScalarFuncType::Upper,
                    ast::ScalarFuncType::Length => ScalarFuncType::Length,
                    ast::ScalarFuncType::Abs => ScalarFuncType::Abs,
                    ast::ScalarFuncType::Now => ScalarFuncType::Now,
                    ast::ScalarFuncType::Concat => ScalarFuncType::Concat,
                    ast::ScalarFuncType::Coalesce => ScalarFuncType::Coalesce,
                    ast::ScalarFuncType::Replace => ScalarFuncType::Replace,
                },
                args: f.args.into_iter().map(|a| a.into()).collect(),
            }),
            ast::Expression::Star => Expression::Star,
            ast::Expression::Subquery(s) => Expression::Subquery(Box::new((*s).into())),
        }
    }
}

impl From<ast::Condition> for Condition {
    fn from(c: ast::Condition) -> Self {
        match c {
            ast::Condition::And(l, r) => Condition::And(Box::new((*l).into()), Box::new((*r).into())),
            ast::Condition::Or(l, r) => Condition::Or(Box::new((*l).into()), Box::new((*r).into())),
            ast::Condition::Not(c) => Condition::Not(Box::new((*c).into())),
            ast::Condition::Comparison(l, op, r) => Condition::Comparison(
                l.into(),
                match op {
                    ast::ComparisonOp::Eq => ComparisonOp::Eq,
                    ast::ComparisonOp::Neq | ast::ComparisonOp::NotEq => ComparisonOp::Neq,
                    ast::ComparisonOp::Gt => ComparisonOp::Gt,
                    ast::ComparisonOp::Gte | ast::ComparisonOp::GtEq => ComparisonOp::Gte,
                    ast::ComparisonOp::Lt => ComparisonOp::Lt,
                    ast::ComparisonOp::Lte | ast::ComparisonOp::LtEq => ComparisonOp::Lte,
                    ast::ComparisonOp::Like => ComparisonOp::Eq, // LIKE handled specially
                },
                r.into(),
            ),
            ast::Condition::In(e, v) => Condition::In(e.into(), v.into_iter().map(|x: ast::Expression| x.into()).collect()),
            ast::Condition::InSubquery(e, s) => Condition::InSubquery(e.into(), Box::new((*s).into())),
            ast::Condition::Exists(s) => Condition::Exists(Box::new((*s).into())),
            ast::Condition::Between(e, l, h) => Condition::Between(e.into(), l.into(), h.into()),
            ast::Condition::Is(e, op) => Condition::Is(
                e.into(),
                match op {
                    ast::IsOp::Null => IsOp::Null,
                    ast::IsOp::NotNull => IsOp::NotNull,
                    ast::IsOp::True => IsOp::True,
                    ast::IsOp::False => IsOp::False,
                },
            ),
            ast::Condition::Like(e, s) => Condition::Like(e.into(), s),
            ast::Condition::FullTextSearch(f, q) => Condition::FullTextSearch(f, q),
            ast::Condition::Logical(l, op, r) => match op {
                ast::LogicalOp::And => Condition::And(Box::new((*l).into()), Box::new((*r).into())),
                ast::LogicalOp::Or => Condition::Or(Box::new((*l).into()), Box::new((*r).into())),
            },
            ast::Condition::IsNull(e) => Condition::Is(e.into(), IsOp::Null),
            ast::Condition::IsNotNull(e) => Condition::Is(e.into(), IsOp::NotNull),
        }
    }
}

impl From<ast::InsertStmt> for Insert {
    fn from(s: ast::InsertStmt) -> Self {
        Insert {
            table: s.table,
            columns: s.columns,
            values: s.values.into_iter().map(|v| v.into()).collect(),
        }
    }
}

impl From<ast::UpdateStmt> for Update {
    fn from(s: ast::UpdateStmt) -> Self {
        Update {
            table: s.table,
            assignments: s.assignments.into_iter().map(|(c, e)| (c, e.into())).collect(),
            where_clause: s.where_clause.map(|w| w.into()),
        }
    }
}

impl From<ast::DeleteStmt> for Delete {
    fn from(s: ast::DeleteStmt) -> Self {
        Delete {
            table: s.table,
            where_clause: s.where_clause.map(|w| w.into()),
        }
    }
}

impl From<ast::CreateTableStmt> for CreateTable {
    fn from(s: ast::CreateTableStmt) -> Self {
        CreateTable {
            name: s.name,
            columns: s.columns,
            primary_key: s.primary_key,
            foreign_keys: s.foreign_keys,
        }
    }
}

impl From<ast::CreateMaterializedViewStmt> for CreateMaterializedView {
    fn from(s: ast::CreateMaterializedViewStmt) -> Self {
        CreateMaterializedView {
            name: s.name,
            query: s.query.into(),
        }
    }
}

impl From<ast::AlterTableStmt> for AlterTable {
    fn from(s: ast::AlterTableStmt) -> Self {
        AlterTable {
            table: s.table,
            action: match s.action {
                ast::AlterAction::AddColumn(c) => AlterAction::AddColumn(c),
                ast::AlterAction::DropColumn(c) => AlterAction::DropColumn(c),
                ast::AlterAction::RenameColumn { old_name, new_name } => AlterAction::RenameColumn { old_name, new_name },
                ast::AlterAction::RenameTable(t) => AlterAction::RenameTable(t),
            },
        }
    }
}

impl From<ast::CreateIndexStmt> for CreateIndex {
    fn from(s: ast::CreateIndexStmt) -> Self {
        CreateIndex {
            name: s.name,
            table: s.table,
            expressions: s.expressions.into_iter().map(|e| e.into()).collect(),
            unique: s.unique,
            index_type: match s.index_type {
                ast::IndexType::BTree => IndexType::BTree,
                ast::IndexType::Hash => IndexType::Hash,
            },
            where_clause: s.where_clause.map(|w| w.into()),
        }
    }
}

impl From<ast::DropTableStmt> for DropTable {
    fn from(s: ast::DropTableStmt) -> Self {
        DropTable {
            name: s.name,
        }
    }
}

impl From<ast::CreateUserStmt> for CreateUser {
    fn from(s: ast::CreateUserStmt) -> Self {
        CreateUser {
            username: s.username,
            password: s.password,
        }
    }
}

impl From<ast::DropUserStmt> for DropUser {
    fn from(s: ast::DropUserStmt) -> Self {
        DropUser {
            username: s.username,
        }
    }
}

impl From<ast::GrantStmt> for Grant {
    fn from(s: ast::GrantStmt) -> Self {
        Grant {
            privileges: s.privileges,
            table: s.table,
            username: s.username,
        }
    }
}

impl From<ast::RevokeStmt> for Revoke {
    fn from(s: ast::RevokeStmt) -> Self {
        Revoke {
            privileges: s.privileges,
            table: s.table,
            username: s.username,
        }
    }
}

impl From<ast::SearchStmt> for Search {
    fn from(s: ast::SearchStmt) -> Self {
        Search {
            table: s.table,
            query: s.query,
        }
    }
}

impl From<ast::PrepareStmt> for Prepare {
    fn from(s: ast::PrepareStmt) -> Self {
        Prepare {
            name: s.name,
            sql: s.sql,
        }
    }
}

impl From<ast::ExecuteStmt> for Execute {
    fn from(s: ast::ExecuteStmt) -> Self {
        Execute {
            name: s.name,
            params: s.params.into_iter().map(|p| p.into()).collect(),
        }
    }
}
