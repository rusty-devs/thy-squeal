use super::expr::Expression;
use super::stmt::Select;
use serde::{Deserialize, Serialize};

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
    Eq,
    Neq,
    Lt,
    Lte,
    Gt,
    Gte,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub enum IsOp {
    Null,
    NotNull,
    True,
    False,
}
