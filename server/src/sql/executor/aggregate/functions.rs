use super::super::super::error::{SqlError, SqlResult};
use super::super::super::eval::{EvalContext, evaluate_expression_joined};
use super::super::Executor;
use crate::squeal::{AggregateType, Expression, FunctionCall};
use crate::storage::{DatabaseState, Row, Table, Value};

impl Executor {
    pub(crate) fn eval_aggregate_joined(
        &self,
        fc: &FunctionCall,
        contexts: &[Vec<(&Table, Option<&str>, &Row)>],
        outer_contexts: &[(&Table, Option<&str>, &Row)],
        db_state: &DatabaseState,
    ) -> SqlResult<Value> {
        match fc.name {
            AggregateType::Count => {
                if fc.args.len() == 1 && matches!(fc.args[0], Expression::Star) {
                    Ok(Value::Int(contexts.len() as i64))
                } else {
                    let mut count = 0;
                    for ctx_list in contexts {
                        let eval_ctx = EvalContext::new(ctx_list, &[], outer_contexts, db_state);
                        let val = evaluate_expression_joined(self, &fc.args[0], &eval_ctx)?;
                        if val != Value::Null {
                            count += 1;
                        }
                    }
                    Ok(Value::Int(count))
                }
            }
            AggregateType::Sum => {
                let mut sum_f = 0.0;
                let mut sum_i = 0;
                let mut is_float = false;
                for ctx_list in contexts {
                    let eval_ctx = EvalContext::new(ctx_list, &[], outer_contexts, db_state);
                    let val = evaluate_expression_joined(self, &fc.args[0], &eval_ctx)?;
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
                            ));
                        }
                    }
                }
                if is_float {
                    Ok(Value::Float(sum_f))
                } else {
                    Ok(Value::Int(sum_i))
                }
            }
            AggregateType::Min => {
                let mut min_val: Option<Value> = None;
                for ctx_list in contexts {
                    let eval_ctx = EvalContext::new(ctx_list, &[], outer_contexts, db_state);
                    let val = evaluate_expression_joined(self, &fc.args[0], &eval_ctx)?;
                    if val == Value::Null {
                        continue;
                    }
                    if min_val.as_ref().is_none_or(|mv| &val < mv) {
                        min_val = Some(val);
                    }
                }
                Ok(min_val.unwrap_or(Value::Null))
            }
            AggregateType::Max => {
                let mut max_val: Option<Value> = None;
                for ctx_list in contexts {
                    let eval_ctx = EvalContext::new(ctx_list, &[], outer_contexts, db_state);
                    let val = evaluate_expression_joined(self, &fc.args[0], &eval_ctx)?;
                    if val == Value::Null {
                        continue;
                    }
                    if max_val.as_ref().is_none_or(|mv| &val > mv) {
                        max_val = Some(val);
                    }
                }
                Ok(max_val.unwrap_or(Value::Null))
            }
            AggregateType::Avg => {
                let mut sum = 0.0;
                let mut count = 0;
                for ctx_list in contexts {
                    let eval_ctx = EvalContext::new(ctx_list, &[], outer_contexts, db_state);
                    let val = evaluate_expression_joined(self, &fc.args[0], &eval_ctx)?;
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
                            ));
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
