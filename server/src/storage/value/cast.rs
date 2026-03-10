use super::super::types::DataType;
use super::Value;

impl Value {
    pub fn cast(self, target_type: &DataType) -> anyhow::Result<Self> {
        match (self, target_type) {
            (v, t) if v.data_type() == *t => Ok(v),
            (Value::Null, _) => Ok(Value::Null),
            (Value::Int(i), DataType::Float) => Ok(Value::Float(i as f64)),
            (Value::Float(f), DataType::Int) => Ok(Value::Int(f as i64)),
            (Value::Text(s), DataType::Int) => Ok(Value::Int(s.parse()?)),
            (Value::Text(s), DataType::Float) => Ok(Value::Float(s.parse()?)),
            (v, DataType::Text) | (v, DataType::VarChar) => Ok(Value::Text(v.to_string_repr())),
            (Value::Text(s), DataType::DateTime) => Ok(Value::DateTime(s.parse()?)),
            (Value::Text(s), DataType::Json) => Ok(Value::Json(serde_json::from_str(&s)?)),
            (v, t) => Err(anyhow::anyhow!("Cannot cast {:?} to {:?}", v, t)),
        }
    }

    pub fn to_string_repr(&self) -> String {
        match self {
            Value::Null => "NULL".to_string(),
            Value::Int(i) => i.to_string(),
            Value::Float(f) => f.to_string(),
            Value::Text(s) => s.clone(),
            Value::Bool(b) => b.to_string(),
            Value::DateTime(d) => d.to_rfc3339(),
            Value::Json(j) => j.to_string(),
        }
    }

    #[allow(dead_code)]
    pub fn to_sql(&self) -> String {
        match self {
            Value::Null => "NULL".to_string(),
            Value::Int(i) => i.to_string(),
            Value::Float(f) => f.to_string(),
            Value::Text(s) => format!("'{}'", s.replace('\'', "''")),
            Value::Bool(b) => b.to_string().to_uppercase(),
            Value::DateTime(d) => format!("'{}'", d.to_rfc3339()),
            Value::Json(j) => format!("'{}'", j.to_string().replace('\'', "''")),
        }
    }

    pub fn as_text(&self) -> Option<&str> {
        match self {
            Value::Text(s) => Some(s),
            _ => None,
        }
    }

    #[allow(dead_code)]
    pub fn as_f64(&self) -> Option<f64> {
        match self {
            Value::Float(f) => Some(*f),
            Value::Int(i) => Some(*i as f64),
            _ => None,
        }
    }

    #[allow(dead_code)]
    pub fn as_bool(&self) -> Option<bool> {
        match self {
            Value::Bool(b) => Some(*b),
            _ => None,
        }
    }
}

impl std::fmt::Display for Value {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}", self.to_string_repr())
    }
}
