pub mod binary;
pub mod function;
pub mod subquery;

use super::super::ast::Expression;
use super::super::error::{SqlError, SqlResult};
use super::Evaluator;
use super::column::resolve_column;
use crate::storage::{DatabaseState, Row, Table, Value};

pub fn evaluate_expression_joined(
    executor: &dyn Evaluator,
    expr: &Expression,
    contexts: &[(&Table, Option<&str>, &Row)],
    params: &[Value],
    outer_contexts: &[(&Table, Option<&str>, &Row)],
    db_state: &DatabaseState,
) -> SqlResult<Value> {
    match expr {
        Expression::Literal(v) => Ok(v.clone()),
        Expression::Placeholder(i) => {
            if *i == 0 {
                return Err(SqlError::Runtime(
                    "Positional placeholder '?' was not correctly numbered".to_string(),
                ));
            }
            params.get(*i - 1).cloned().ok_or_else(|| {
                SqlError::Runtime(format!("Missing parameter for placeholder ${}", i))
            })
        }
        Expression::Column(name) => {
            if let Ok(val) = resolve_column(name, contexts) {
                return Ok(val);
            }
            if let Ok(val) = resolve_column(name, outer_contexts) {
                return Ok(val);
            }

            Err(SqlError::ColumnNotFound(name.clone()))
        }
        Expression::Subquery(subquery) => subquery::evaluate_subquery(
            executor,
            subquery,
            contexts,
            params,
            outer_contexts,
            db_state,
        ),
        Expression::BinaryOp(left, op, right) => {
            let l = evaluate_expression_joined(
                executor,
                left,
                contexts,
                params,
                outer_contexts,
                db_state,
            )?;
            let r = evaluate_expression_joined(
                executor,
                right,
                contexts,
                params,
                outer_contexts,
                db_state,
            )?;
            binary::evaluate_binary_op(l, op, r)
        }
        Expression::ScalarFunc(sf) => {
            let val = evaluate_expression_joined(
                executor,
                &sf.arg,
                contexts,
                params,
                outer_contexts,
                db_state,
            )?;
            function::evaluate_scalar_func(&sf.name, val)
        }
        Expression::FunctionCall(_) => Err(SqlError::Runtime(
            "Aggregate functions must be evaluated at the top level".to_string(),
        )),
        Expression::Star => Err(SqlError::Runtime(
            "Star expression must be evaluated at the top level".to_string(),
        )),
    }
}
