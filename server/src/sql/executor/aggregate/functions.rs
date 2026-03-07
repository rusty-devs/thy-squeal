use crate::storage::{Row, Table, Value, DatabaseState};
use super::super::super::ast;
use super::super::super::error::{SqlError, SqlResult};
use super::super::super::eval::{evaluate_expression_joined};
use super::super::Executor;

impl Executor {
    pub(crate) fn eval_aggregate_joined(
        &self,
        fc: &ast::FunctionCall,
        contexts: &[Vec<(&Table, Option<&str>, &Row)>],
        outer_contexts: &[(&Table, Option<&str>, &Row)],
        db_state: &DatabaseState,
    ) -> SqlResult<Value> {
        match fc.name {
            ast::AggregateType::Count => {
                if fc.args.len() == 1 && matches!(fc.args[0], ast::Expression::Star) {
                    Ok(Value::Int(contexts.len() as i64))
                } else {
                    let mut count = 0;
                    for ctx in contexts {
                        let val = evaluate_expression_joined(
                            self,
                            &fc.args[0],
                            ctx,
                            outer_contexts,
                            db_state,
                        )?;
                        if val != Value::Null {
                            count += 1;
                        }
                    }
                    Ok(Value::Int(count))
                }
            }
            ast::AggregateType::Sum => {
                let mut sum_f = 0.0;
                let mut sum_i = 0;
                let mut is_float = false;
                for ctx in contexts {
                    let val = evaluate_expression_joined(
                        self,
                        &fc.args[0],
                        ctx,
                        outer_contexts,
                        db_state,
                    )?;
                    match val {
                        Value::Int(i) => {
                            sum_i += i;
                            sum_f += i as f64;
                        }
                        Value::Float(f) => {
                            sum_f += f;
                            is_float = true;
                        }
                        Value::Null => {}
                        _ => {
                            return Err(SqlError::TypeMismatch(
                                "SUM requires numeric values".to_string(),
                            ))
                        }
                    }
                }
                if is_float {
                    Ok(Value::Float(sum_f))
                } else {
                    Ok(Value::Int(sum_i))
                }
            }
            ast::AggregateType::Min => {
                let mut min_val: Option<Value> = None;
                for ctx in contexts {
                    let val = evaluate_expression_joined(
                        self,
                        &fc.args[0],
                        ctx,
                        outer_contexts,
                        db_state,
                    )?;
                    if val == Value::Null {
                        continue;
                    }
                    if min_val.as_ref().is_none_or(|mv| &val < mv) {
                        min_val = Some(val);
                    }
                }
                Ok(min_val.unwrap_or(Value::Null))
            }
            ast::AggregateType::Max => {
                let mut max_val: Option<Value> = None;
                for ctx in contexts {
                    let val = evaluate_expression_joined(
                        self,
                        &fc.args[0],
                        ctx,
                        outer_contexts,
                        db_state,
                    )?;
                    if val == Value::Null {
                        continue;
                    }
                    if max_val.as_ref().is_none_or(|mv| &val > mv) {
                        max_val = Some(val);
                    }
                }
                Ok(max_val.unwrap_or(Value::Null))
            }
            ast::AggregateType::Avg => {
                let mut sum = 0.0;
                let mut count = 0;
                for ctx in contexts {
                    let val = evaluate_expression_joined(
                        self,
                        &fc.args[0],
                        ctx,
                        outer_contexts,
                        db_state,
                    )?;
                    match val {
                        Value::Int(i) => {
                            sum += i as f64;
                            count += 1;
                        }
                        Value::Float(f) => {
                            sum += f;
                            count += 1;
                        }
                        Value::Null => {}
                        _ => {
                            return Err(SqlError::TypeMismatch(
                                "AVG requires numeric values".to_string(),
                            ))
                        }
                    }
                }
                if count == 0 {
                    Ok(Value::Null)
                } else {
                    Ok(Value::Float(sum / count as f64))
                }
            }
        }
    }
}
