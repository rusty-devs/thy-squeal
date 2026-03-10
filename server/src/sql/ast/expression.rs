use super::statements::SelectStmt;
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
                for arg in &mut sf.args {
                    arg.resolve_placeholders(counter);
                }
            }
            Expression::Subquery(s) => {
                s.resolve_placeholders(counter);
            }
            _ => {}
        }
    }

    #[allow(dead_code)]
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
                format!("{}({})", fc.name.to_sql(), args.join(", "))
            }
            Expression::ScalarFunc(sf) => {
                let args: Vec<String> = sf.args.iter().map(|a| a.to_sql()).collect();
                format!("{}({})", sf.name.to_sql(), args.join(", "))
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

impl ScalarFuncType {
    pub fn to_sql(&self) -> &str {
        match self {
            ScalarFuncType::Lower => "LOWER",
            ScalarFuncType::Upper => "UPPER",
            ScalarFuncType::Length => "LENGTH",
            ScalarFuncType::Abs => "ABS",
            ScalarFuncType::Now => "NOW",
            ScalarFuncType::Concat => "CONCAT",
            ScalarFuncType::Coalesce => "COALESCE",
            ScalarFuncType::Replace => "REPLACE",
        }
    }
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub enum AggregateType {
    Count,
    Sum,
    Avg,
    Min,
    Max,
}

impl AggregateType {
    pub fn to_sql(&self) -> &str {
        match self {
            AggregateType::Count => "COUNT",
            AggregateType::Sum => "SUM",
            AggregateType::Avg => "AVG",
            AggregateType::Min => "MIN",
            AggregateType::Max => "MAX",
        }
    }
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub enum BinaryOp {
    Add,
    Sub,
    Mul,
    Div,
}

impl BinaryOp {
    #[allow(dead_code)]
    pub fn to_sql(&self) -> &str {
        match self {
            BinaryOp::Add => "+",
            BinaryOp::Sub => "-",
            BinaryOp::Mul => "*",
            BinaryOp::Div => "/",
        }
    }
}
