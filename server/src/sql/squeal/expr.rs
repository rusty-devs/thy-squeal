use super::stmt::Select;
use crate::storage::Value;
use serde::{Deserialize, Serialize};

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
    Lower,
    Upper,
    Length,
    Abs,
    Now,
    Concat,
    Coalesce,
    Replace,
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

impl Expression {
    pub fn to_sql(&self) -> String {
        match self {
            Expression::Literal(v) => format!("{:?}", v),
            Expression::Placeholder(i) => format!("?{}", i),
            Expression::Column(c) => c.clone(),
            Expression::BinaryOp(l, op, r) => {
                format!("({} {} {})", l.to_sql(), op.to_sql(), r.to_sql())
            }
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
