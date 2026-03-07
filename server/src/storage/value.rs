use super::types::DataType;
use serde::{Deserialize, Serialize};
use std::cmp::Ordering;
use std::hash::{Hash, Hasher};

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub enum Value {
    Null,
    Int(i64),
    Float(f64),
    Text(String),
    Bool(bool),
    Json(serde_json::Value),
}

impl Value {
    pub fn from_json(v: serde_json::Value) -> Self {
        match v {
            serde_json::Value::Null => Value::Null,
            serde_json::Value::Number(n) => {
                if let Some(i) = n.as_i64() {
                    Value::Int(i)
                } else {
                    Value::Float(n.as_f64().unwrap_or(0.0))
                }
            }
            serde_json::Value::String(s) => Value::Text(s),
            serde_json::Value::Bool(b) => Value::Bool(b),
            _ => Value::Json(v),
        }
    }

    fn variant_rank(&self) -> u8 {
        match self {
            Value::Null => 0,
            Value::Bool(_) => 1,
            Value::Int(_) | Value::Float(_) => 2, // Group numbers
            Value::Text(_) => 3,
            Value::Json(_) => 4,
        }
    }

    pub fn cast(self, target: &DataType) -> Result<Self, String> {
        if matches!(self, Value::Null) {
            return Ok(Value::Null);
        }

        match target {
            DataType::Int => {
                if let Some(i) = self.as_int() {
                    Ok(Value::Int(i))
                } else if let Value::Text(s) = &self {
                    s.parse::<i64>().map(Value::Int).map_err(|e| e.to_string())
                } else {
                    Err(format!("Cannot cast {:?} to INT", self))
                }
            }
            DataType::Float => {
                if let Some(f) = self.as_float() {
                    Ok(Value::Float(f))
                } else if let Value::Text(s) = &self {
                    s.parse::<f64>().map(Value::Float).map_err(|e| e.to_string())
                } else {
                    Err(format!("Cannot cast {:?} to FLOAT", self))
                }
            }
            DataType::Bool => {
                if let Some(b) = self.as_bool() {
                    Ok(Value::Bool(b))
                } else if let Value::Text(s) = &self {
                    match s.to_lowercase().as_str() {
                        "true" | "1" => Ok(Value::Bool(true)),
                        "false" | "0" => Ok(Value::Bool(false)),
                        _ => Err(format!("Invalid boolean string: {}", s)),
                    }
                } else {
                    Err(format!("Cannot cast {:?} to BOOL", self))
                }
            }
            DataType::Text | DataType::VarChar => Ok(Value::Text(self.to_string_repr())),
            DataType::Json => match self {
                Value::Json(v) => Ok(Value::Json(v)),
                Value::Text(s) => {
                    let v: serde_json::Value = serde_json::from_str(&s).map_err(|e| e.to_string())?;
                    Ok(Value::from_json(v))
                }
                _ => Err(format!("Cannot cast {:?} to JSON", self)),
            },
            _ => Ok(self), // Default to current for other types
        }
    }

    pub fn to_string_repr(&self) -> String {
        match self {
            Value::Null => "NULL".to_string(),
            Value::Int(i) => i.to_string(),
            Value::Float(f) => f.to_string(),
            Value::Text(s) => s.clone(),
            Value::Bool(b) => b.to_string(),
            Value::Json(j) => j.to_string(),
        }
    }
}

impl Eq for Value {}

impl Hash for Value {
    fn hash<H: Hasher>(&self, state: &mut H) {
        self.variant_rank().hash(state);
        match self {
            Value::Null => {}
            Value::Int(i) => i.hash(state),
            Value::Float(f) => f.to_bits().hash(state),
            Value::Text(s) => s.hash(state),
            Value::Bool(b) => b.hash(state),
            Value::Json(v) => v.to_string().hash(state),
        }
    }
}

#[allow(clippy::non_canonical_partial_ord_impl)]
impl PartialOrd for Value {
    fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
        Some(self.cmp(other))
    }
}

impl Ord for Value {
    fn cmp(&self, other: &Self) -> Ordering {
        let r1 = self.variant_rank();
        let r2 = other.variant_rank();

        if r1 != r2 {
            return r1.cmp(&r2);
        }

        match (self, other) {
            (Value::Null, Value::Null) => Ordering::Equal,
            (Value::Bool(a), Value::Bool(b)) => a.cmp(b),
            (Value::Int(a), Value::Int(b)) => a.cmp(b),
            (Value::Float(a), Value::Float(b)) => a.partial_cmp(b).unwrap_or(Ordering::Equal),
            (Value::Int(a), Value::Float(b)) => (*a as f64).partial_cmp(b).unwrap_or(Ordering::Equal),
            (Value::Float(a), Value::Int(b)) => a.partial_cmp(&(*b as f64)).unwrap_or(Ordering::Equal),
            (Value::Text(a), Value::Text(b)) => a.cmp(b),
            (Value::Json(a), Value::Json(b)) => a.to_string().cmp(&b.to_string()),
            _ => Ordering::Equal,
        }
    }
}

impl Value {
    pub fn as_text(&self) -> Option<&str> {
        match self {
            Value::Text(s) => Some(s),
            _ => None,
        }
    }

    pub fn as_int(&self) -> Option<i64> {
        match self {
            Value::Int(i) => Some(*i),
            _ => None,
        }
    }

    pub fn as_float(&self) -> Option<f64> {
        match self {
            Value::Float(f) => Some(*f),
            Value::Int(i) => Some(*i as f64),
            _ => None,
        }
    }

    pub fn as_bool(&self) -> Option<bool> {
        match self {
            Value::Bool(b) => Some(*b),
            _ => None,
        }
    }
}
