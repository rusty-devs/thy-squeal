use super::expression::Expression;
use super::statements::SelectStmt;
use serde::{Deserialize, Serialize};

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub enum Condition {
    Comparison(Expression, ComparisonOp, Expression),
    Logical(Box<Condition>, LogicalOp, Box<Condition>),
    Not(Box<Condition>),
    IsNull(Expression),
    IsNotNull(Expression),
    InSubquery(Expression, Box<SelectStmt>),
}

impl Condition {
    pub fn resolve_placeholders(&mut self, counter: &mut usize) {
        match self {
            Condition::Comparison(l, _, r) => {
                l.resolve_placeholders(counter);
                r.resolve_placeholders(counter);
            }
            Condition::Logical(l, _, r) => {
                l.resolve_placeholders(counter);
                r.resolve_placeholders(counter);
            }
            Condition::Not(c) => c.resolve_placeholders(counter),
            Condition::IsNull(e) => e.resolve_placeholders(counter),
            Condition::IsNotNull(e) => e.resolve_placeholders(counter),
            Condition::InSubquery(e, s) => {
                e.resolve_placeholders(counter);
                s.resolve_placeholders(counter);
            }
        }
    }

    #[allow(dead_code)]
    pub fn to_sql(&self) -> String {
        match self {
            Condition::Comparison(l, op, r) => {
                format!("{} {} {}", l.to_sql(), op.to_sql(), r.to_sql())
            }
            Condition::Logical(l, op, r) => {
                format!("({} {} {})", l.to_sql(), op.to_sql(), r.to_sql())
            }
            Condition::Not(c) => format!("NOT ({})", c.to_sql()),
            Condition::IsNull(e) => format!("{} IS NULL", e.to_sql()),
            Condition::IsNotNull(e) => format!("{} IS NOT NULL", e.to_sql()),
            Condition::InSubquery(e, _) => format!("{} IN (SELECT ...)", e.to_sql()),
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
    #[allow(dead_code)]
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
    #[allow(dead_code)]
    pub fn to_sql(&self) -> &str {
        match self {
            LogicalOp::And => "AND",
            LogicalOp::Or => "OR",
        }
    }
}
