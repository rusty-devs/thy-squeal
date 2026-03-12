use super::super::super::error::{SqlError, SqlResult};
use super::super::super::squeal::ScalarFuncType;
use crate::storage::Value;

pub fn evaluate_scalar_func(name: &ScalarFuncType, args: &[Value]) -> SqlResult<Value> {
    match name {
        ScalarFuncType::Lower => {
            let val = args
                .first()
                .ok_or_else(|| SqlError::Runtime("LOWER requires 1 argument".to_string()))?;
            let s = val
                .as_text()
                .ok_or_else(|| SqlError::TypeMismatch("LOWER requires text".to_string()))?;
            Ok(Value::Text(s.to_lowercase()))
        }
        ScalarFuncType::Upper => {
            let val = args
                .first()
                .ok_or_else(|| SqlError::Runtime("UPPER requires 1 argument".to_string()))?;
            let s = val
                .as_text()
                .ok_or_else(|| SqlError::TypeMismatch("UPPER requires text".to_string()))?;
            Ok(Value::Text(s.to_uppercase()))
        }
        ScalarFuncType::Length => {
            let val = args
                .first()
                .ok_or_else(|| SqlError::Runtime("LENGTH requires 1 argument".to_string()))?;
            let s = val
                .as_text()
                .ok_or_else(|| SqlError::TypeMismatch("LENGTH requires text".to_string()))?;
            Ok(Value::Int(s.len() as i64))
        }
        ScalarFuncType::Abs => {
            let val = args
                .first()
                .ok_or_else(|| SqlError::Runtime("ABS requires 1 argument".to_string()))?;
            match val {
                Value::Int(i) => Ok(Value::Int(i.abs())),
                Value::Float(f) => Ok(Value::Float(f.abs())),
                _ => Err(SqlError::TypeMismatch(
                    "ABS requires numeric value".to_string(),
                )),
            }
        }
        ScalarFuncType::Now => Ok(Value::DateTime(chrono::Utc::now())),
        ScalarFuncType::Concat => {
            let mut result = String::new();
            for arg in args {
                result.push_str(&arg.to_string_repr());
            }
            Ok(Value::Text(result))
        }
        ScalarFuncType::Coalesce => {
            for arg in args {
                if !matches!(arg, Value::Null) {
                    return Ok(arg.clone());
                }
            }
            Ok(Value::Null)
        }
        ScalarFuncType::Replace => {
            if args.len() != 3 {
                return Err(SqlError::Runtime(
                    "REPLACE requires 3 arguments".to_string(),
                ));
            }
            let s = args[0].as_text().ok_or_else(|| {
                SqlError::TypeMismatch("REPLACE first arg must be text".to_string())
            })?;
            let from = args[1].as_text().ok_or_else(|| {
                SqlError::TypeMismatch("REPLACE second arg must be text".to_string())
            })?;
            let to = args[2].as_text().ok_or_else(|| {
                SqlError::TypeMismatch("REPLACE third arg must be text".to_string())
            })?;
            Ok(Value::Text(s.replace(from, to)))
        }
    }
}
