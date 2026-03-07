use chrono::{NaiveDate, NaiveDateTime};
use serde::{Deserialize, Serialize};

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub enum Value {
    Null,
    Int(i64),
    Float(f64),
    Bool(bool),
    Date(NaiveDate),
    DateTime(NaiveDateTime),
    Text(String),
    Blob(Vec<u8>),
    Json(serde_json::Value),
}

impl Eq for Value {}

impl Ord for Value {
    fn cmp(&self, other: &Self) -> std::cmp::Ordering {
        self.partial_cmp(other).unwrap_or_else(|| {
            // Discriminant is not Ord, use manual order for variants
            let self_rank = self.variant_rank();
            let other_rank = other.variant_rank();
            self_rank.cmp(&other_rank)
        })
    }
}

impl Value {
    fn variant_rank(&self) -> u8 {
        match self {
            Value::Null => 0,
            Value::Int(_) => 1,
            Value::Float(_) => 2,
            Value::Bool(_) => 3,
            Value::Date(_) => 4,
            Value::DateTime(_) => 5,
            Value::Text(_) => 6,
            Value::Blob(_) => 7,
            Value::Json(_) => 8,
        }
    }
}

impl std::hash::Hash for Value {
    fn hash<H: std::hash::Hasher>(&self, state: &mut H) {
        self.variant_rank().hash(state);
        match self {
            Value::Null => {}
            Value::Int(i) => i.hash(state),
            Value::Float(f) => f.to_bits().hash(state),
            Value::Bool(b) => b.hash(state),
            Value::Date(d) => d.hash(state),
            Value::DateTime(dt) => dt.hash(state),
            Value::Text(s) => s.hash(state),
            Value::Blob(b) => b.hash(state),
            Value::Json(j) => j.to_string().hash(state),
        }
    }
}

impl PartialOrd for Value {
    fn partial_cmp(&self, other: &Self) -> Option<std::cmp::Ordering> {
        match (self, other) {
            (Value::Null, Value::Null) => Some(std::cmp::Ordering::Equal),
            (Value::Int(a), Value::Int(b)) => a.partial_cmp(b),
            (Value::Int(a), Value::Float(b)) => (*a as f64).partial_cmp(b),
            (Value::Float(a), Value::Int(b)) => a.partial_cmp(&(*b as f64)),
            (Value::Float(a), Value::Float(b)) => a.partial_cmp(b),
            (Value::Bool(a), Value::Bool(b)) => a.partial_cmp(b),
            (Value::Text(a), Value::Text(b)) => a.partial_cmp(b),
            (Value::Date(a), Value::Date(b)) => a.partial_cmp(b),
            (Value::DateTime(a), Value::DateTime(b)) => a.partial_cmp(b),
            // Otherwise, they are not comparable or one is Null
            _ => None,
        }
    }
}

impl Value {
    pub fn as_int(&self) -> Option<i64> {
        match self {
            Value::Int(i) => Some(*i),
            _ => None,
        }
    }

    pub fn as_float(&self) -> Option<f64> {
        match self {
            Value::Float(f) => Some(*f),
            _ => None,
        }
    }

    pub fn as_bool(&self) -> Option<bool> {
        match self {
            Value::Bool(b) => Some(*b),
            _ => None,
        }
    }

    pub fn as_text(&self) -> Option<&str> {
        match self {
            Value::Text(s) => Some(s),
            _ => None,
        }
    }
}
