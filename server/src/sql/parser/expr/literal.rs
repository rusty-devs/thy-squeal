use super::super::super::error::{SqlError, SqlResult};
use super::super::Rule;
use crate::storage::Value;

pub fn parse_literal(pair: pest::iterators::Pair<Rule>) -> SqlResult<Value> {
    let mut inner = pair.clone().into_inner();
    let p = match inner.next() {
        Some(p) => p,
        None => {
            let s = pair.as_str().trim();
            if s.to_uppercase() == "NULL" {
                return Ok(Value::Null);
            }
            if s.to_lowercase() == "true" { return Ok(Value::Bool(true)); }
            if s.to_lowercase() == "false" { return Ok(Value::Bool(false)); }
            
            if s.starts_with('\'') && s.ends_with('\'') {
                return Ok(Value::Text(s[1..s.len()-1].to_string()));
            }
            
            // Try number
            if let Ok(i) = s.parse::<i64>() {
                return Ok(Value::Int(i));
            }
            if let Ok(f) = s.parse::<f64>() {
                return Ok(Value::Float(f));
            }
            
            return Err(SqlError::Parse(format!("Could not parse literal: {}", s)));
        }
    };

    match p.as_rule() {
        Rule::string_literal => Ok(Value::Text(p.as_str().trim_matches('\'').to_string())),
        Rule::number_literal => {
            let s = p.as_str().trim();
            if s.contains('.') {
                s.parse::<f64>()
                    .map(Value::Float)
                    .map_err(|_| SqlError::Parse(format!("Invalid number: {}", s)))
            } else {
                s.parse::<i64>()
                    .map(Value::Int)
                    .map_err(|_| SqlError::Parse(format!("Invalid integer: {}", s)))
            }
        }
        Rule::boolean_literal => {
            let s = p.as_str().to_lowercase();
            Ok(Value::Bool(s == "true"))
        }
        Rule::KW_NULL => Ok(Value::Null),
        _ => Err(SqlError::Parse(format!(
            "Unknown literal rule: {:?}",
            p.as_rule()
        ))),
    }
}
