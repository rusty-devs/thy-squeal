use super::super::super::ast::ScalarFuncType;
use super::super::super::error::{SqlError, SqlResult};
use crate::storage::Value;

pub fn evaluate_scalar_func(name: &ScalarFuncType, val: Value) -> SqlResult<Value> {
    match name {
        ScalarFuncType::Lower => {
            let s = val
                .as_text()
                .ok_or_else(|| SqlError::TypeMismatch("LOWER requires text".to_string()))?;
            Ok(Value::Text(s.to_lowercase()))
        }
        ScalarFuncType::Upper => {
            let s = val
                .as_text()
                .ok_or_else(|| SqlError::TypeMismatch("UPPER requires text".to_string()))?;
            Ok(Value::Text(s.to_uppercase()))
        }
        ScalarFuncType::Length => {
            let s = val
                .as_text()
                .ok_or_else(|| SqlError::TypeMismatch("LENGTH requires text".to_string()))?;
            Ok(Value::Int(s.len() as i64))
        }
        ScalarFuncType::Abs => match val {
            Value::Int(i) => Ok(Value::Int(i.abs())),
            Value::Float(f) => Ok(Value::Float(f.abs())),
            _ => Err(SqlError::TypeMismatch(
                "ABS requires numeric value".to_string(),
            )),
        },
    }
}
