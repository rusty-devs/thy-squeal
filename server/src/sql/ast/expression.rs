use serde::{Deserialize, Serialize};
use crate::storage::Value;
use super::statements::SelectStmt;

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
            Expression::Subquery(_) => "(SELECT ...)".to_string(),
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
