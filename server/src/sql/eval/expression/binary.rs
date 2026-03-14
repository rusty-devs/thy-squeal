use super::super::super::error::{SqlError, SqlResult};
use crate::squeal::BinaryOp;
use crate::storage::Value;

pub fn evaluate_binary_op(l: Value, op: &BinaryOp, r: Value) -> SqlResult<Value> {
    match (l, r) {
        (Value::Int(a), Value::Int(b)) => match op {
            BinaryOp::Add => Ok(Value::Int(a + b)),
            BinaryOp::Sub => Ok(Value::Int(a - b)),
            BinaryOp::Mul => Ok(Value::Int(a * b)),
            BinaryOp::Div => {
                if b == 0 {
                    return Err(SqlError::Runtime("Division by zero".to_string()));
                }
                Ok(Value::Int(a / b))
            }
        },
        (Value::Float(a), Value::Float(b)) => match op {
            BinaryOp::Add => Ok(Value::Float(a + b)),
            BinaryOp::Sub => Ok(Value::Float(a - b)),
            BinaryOp::Mul => Ok(Value::Float(a * b)),
            BinaryOp::Div => Ok(Value::Float(a / b)),
        },
        (Value::Int(a), Value::Float(b)) => {
            let a = a as f64;
            match op {
                BinaryOp::Add => Ok(Value::Float(a + b)),
                BinaryOp::Sub => Ok(Value::Float(a - b)),
                BinaryOp::Mul => Ok(Value::Float(a * b)),
                BinaryOp::Div => Ok(Value::Float(a / b)),
            }
        }
        (Value::Float(a), Value::Int(b)) => {
            let b = b as f64;
            match op {
                BinaryOp::Add => Ok(Value::Float(a + b)),
                BinaryOp::Sub => Ok(Value::Float(a - b)),
                BinaryOp::Mul => Ok(Value::Float(a * b)),
                BinaryOp::Div => Ok(Value::Float(a / b)),
            }
        }
        _ => Err(SqlError::TypeMismatch(
            "Unsupported types for binary operation".to_string(),
        )),
    }
}
