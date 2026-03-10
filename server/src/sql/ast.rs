use crate::storage::{Column, Value};
use serde::{Deserialize, Serialize};

/// Parsed SQL statement AST.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub enum SqlStmt {
    CreateTable(CreateTableStmt),
    DropTable(DropTableStmt),
    CreateIndex(CreateIndexStmt),
    Select(SelectStmt),
    Insert(InsertStmt),
    Update(UpdateStmt),
    Delete(DeleteStmt),
    Explain(SelectStmt),
    Search(SearchStmt),
    Prepare(PrepareStmt),
    Execute(ExecuteStmt),
    Deallocate(String),
    Begin,
    Commit,
    Rollback,
}

impl SqlStmt {
    pub fn resolve_placeholders(&mut self) {
        let mut counter = 0;
        match self {
            SqlStmt::Select(s) => s.resolve_placeholders(&mut counter),
            SqlStmt::Update(u) => u.resolve_placeholders(&mut counter),
            SqlStmt::Delete(d) => d.resolve_placeholders(&mut counter),
            SqlStmt::Explain(s) => s.resolve_placeholders(&mut counter),
            SqlStmt::CreateIndex(ci) => ci.resolve_placeholders(&mut counter),
            SqlStmt::Insert(i) => i.resolve_placeholders(&mut counter),
            // No placeholders in these statements
            SqlStmt::CreateTable(_) | SqlStmt::DropTable(_) | SqlStmt::Search(_) | SqlStmt::Begin | SqlStmt::Commit | SqlStmt::Rollback | SqlStmt::Prepare(_) | SqlStmt::Execute(_) | SqlStmt::Deallocate(_) => {}
        }
    }
}

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
pub enum Expression {
    Literal(Value),
    Placeholder(usize),
    Column(String),
    BinaryOp(Box<Expression>, BinaryOp, Box<Expression>),
    FunctionCall(FunctionCall),
    ScalarFunc(ScalarFunction),
    Star,
    Subquery(Box<SelectStmt>),
}

impl Expression {
    pub fn resolve_placeholders(&mut self, counter: &mut usize) {
        match self {
            Expression::Placeholder(i) => {
                if *i == 0 {
                    *counter += 1;
                    *i = *counter;
                }
            }
            Expression::BinaryOp(l, _, r) => {
                l.resolve_placeholders(counter);
                r.resolve_placeholders(counter);
            }
            Expression::FunctionCall(fc) => {
                for arg in &mut fc.args {
                    arg.resolve_placeholders(counter);
                }
            }
            Expression::ScalarFunc(sf) => {
                sf.arg.resolve_placeholders(counter);
            }
            Expression::Subquery(s) => {
                s.resolve_placeholders(counter);
            }
            _ => {}
        }
    }

    pub fn to_sql(&self) -> String {
        match self {
            Expression::Literal(v) => v.to_sql(),
            Expression::Placeholder(i) => format!("${}", i),
            Expression::Column(c) => c.clone(),
            Expression::BinaryOp(l, op, r) => {
                format!("({} {} {})", l.to_sql(), op.to_sql(), r.to_sql())
            }
            Expression::FunctionCall(fc) => {
                let args: Vec<String> = fc.args.iter().map(|a| a.to_sql()).collect();
                format!("{:?}({})", fc.name, args.join(", ")).to_uppercase()
            }
            Expression::ScalarFunc(sf) => {
                format!("{:?}({})", sf.name, sf.arg.to_sql()).to_uppercase()
            }
            Expression::Star => "*".to_string(),
            Expression::Subquery(_) => "(SELECT ...)".to_string(), // Simplified for now
        }
    }
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct FunctionCall {
    pub name: AggregateType,
    pub args: Vec<Expression>,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct ScalarFunction {
    pub name: ScalarFuncType,
    pub arg: Box<Expression>,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub enum ScalarFuncType {
    Lower,
    Upper,
    Length,
    Abs,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub enum AggregateType {
    Count,
    Sum,
    Avg,
    Min,
    Max,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub enum BinaryOp {
    Add,
    Sub,
    Mul,
    Div,
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

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub enum Condition {
    Comparison(Expression, ComparisonOp, Expression),
    IsNull(Expression),
    IsNotNull(Expression),
    InSubquery(Expression, Box<SelectStmt>),
    Logical(Box<Condition>, LogicalOp, Box<Condition>),
    Not(Box<Condition>),
}

impl Condition {
    pub fn resolve_placeholders(&mut self, counter: &mut usize) {
        match self {
            Condition::Comparison(l, _, r) => {
                l.resolve_placeholders(counter);
                r.resolve_placeholders(counter);
            }
            Condition::IsNull(e) => e.resolve_placeholders(counter),
            Condition::IsNotNull(e) => e.resolve_placeholders(counter),
            Condition::InSubquery(e, s) => {
                e.resolve_placeholders(counter);
                s.resolve_placeholders(counter);
            }
            Condition::Logical(l, _, r) => {
                l.resolve_placeholders(counter);
                r.resolve_placeholders(counter);
            }
            Condition::Not(c) => c.resolve_placeholders(counter),
        }
    }

    pub fn to_sql(&self) -> String {
        match self {
            Condition::Comparison(l, op, r) => {
                format!("{} {} {}", l.to_sql(), op.to_sql(), r.to_sql())
            }
            Condition::IsNull(e) => format!("{} IS NULL", e.to_sql()),
            Condition::IsNotNull(e) => format!("{} IS NOT NULL", e.to_sql()),
            Condition::InSubquery(e, _) => format!("{} IN (SELECT ...)", e.to_sql()),
            Condition::Logical(l, op, r) => {
                format!("({} {} {})", l.to_sql(), op.to_sql(), r.to_sql())
            }
            Condition::Not(c) => format!("NOT ({})", c.to_sql()),
        }
    }
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub enum ComparisonOp {
    Eq,
    NotEq,
    Lt,
    Gt,
    LtEq,
    GtEq,
    Like,
}

impl ComparisonOp {
    pub fn to_sql(&self) -> &str {
        match self {
            ComparisonOp::Eq => "=",
            ComparisonOp::NotEq => "!=",
            ComparisonOp::Lt => "<",
            ComparisonOp::Gt => ">",
            ComparisonOp::LtEq => "<=",
            ComparisonOp::GtEq => ">=",
            ComparisonOp::Like => "LIKE",
        }
    }
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub enum LogicalOp {
    And,
    Or,
}

impl LogicalOp {
    pub fn to_sql(&self) -> &str {
        match self {
            LogicalOp::And => "AND",
            LogicalOp::Or => "OR",
        }
    }
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
