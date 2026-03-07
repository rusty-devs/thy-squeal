use crate::storage::{DatabaseState, Row, Table, Value};
use super::super::ast::{BinaryOp, Expression, ScalarFuncType};
use super::super::error::{SqlError, SqlResult};
use super::Evaluator;
use super::column::resolve_column;

pub fn evaluate_expression_joined(
    executor: &dyn Evaluator,
    expr: &Expression,
    contexts: &[(&Table, Option<&str>, &Row)],
    outer_contexts: &[(&Table, Option<&str>, &Row)],
    db_state: &DatabaseState,
) -> SqlResult<Value> {
    match expr {
        Expression::Literal(v) => Ok(v.clone()),
        Expression::Column(name) => {
            if let Ok(val) = resolve_column(name, contexts) {
                return Ok(val);
            }
            if let Ok(val) = resolve_column(name, outer_contexts) {
                return Ok(val);
            }

            Err(SqlError::ColumnNotFound(name.clone()))
        }
        Expression::Subquery(subquery) => {
            let mut combined_outer = outer_contexts.to_vec();
            combined_outer.extend_from_slice(contexts);

            let result = futures::executor::block_on(executor.exec_select_internal(
                (**subquery).clone(),
                &combined_outer,
                db_state,
            ))?;
            if result.rows.is_empty() {
                Ok(Value::Null)
            } else if result.rows.len() > 1 {
                Err(SqlError::Runtime(
                    "Subquery returned more than one row".to_string(),
                ))
            } else if result.rows[0].is_empty() {
                Ok(Value::Null)
            } else {
                Ok(result.rows[0][0].clone())
            }
        }
        Expression::BinaryOp(left, op, right) => {
            let l =
                evaluate_expression_joined(executor, left, contexts, outer_contexts, db_state)?;
            let r =
                evaluate_expression_joined(executor, right, contexts, outer_contexts, db_state)?;

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
        Expression::ScalarFunc(sf) => {
            let val = evaluate_expression_joined(executor, &sf.arg, contexts, outer_contexts, db_state)?;
            match sf.name {
                ScalarFuncType::Lower => {
                    let s = val.as_text().ok_or_else(|| {
                        SqlError::TypeMismatch("LOWER requires text".to_string())
                    })?;
                    Ok(Value::Text(s.to_lowercase()))
                }
                ScalarFuncType::Upper => {
                    let s = val.as_text().ok_or_else(|| {
                        SqlError::TypeMismatch("UPPER requires text".to_string())
                    })?;
                    Ok(Value::Text(s.to_uppercase()))
                }
                ScalarFuncType::Length => {
                    let s = val.as_text().ok_or_else(|| {
                        SqlError::TypeMismatch("LENGTH requires text".to_string())
                    })?;
                    Ok(Value::Int(s.len() as i64))
                }
                ScalarFuncType::Abs => match val {
                    Value::Int(i) => Ok(Value::Int(i.abs())),
                    Value::Float(f) => Ok(Value::Float(f.abs())),
                    _ => Err(SqlError::TypeMismatch("ABS requires numeric value".to_string())),
                },
            }
        }
        Expression::FunctionCall(_) => Err(SqlError::Runtime(
            "Aggregate functions must be evaluated at the top level".to_string(),
        )),
        Expression::Star => Err(SqlError::Runtime(
            "Star expression must be evaluated at the top level".to_string(),
        )),
    }
}
