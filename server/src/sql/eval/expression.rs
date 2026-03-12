pub mod binary;
pub mod function;
pub mod subquery;

use super::super::error::{SqlError, SqlResult};
use super::super::squeal::Expression;
use super::column::resolve_column;
use super::{EvalContext, Evaluator};
use crate::storage::Value;

pub fn evaluate_expression_joined(
    executor: &dyn Evaluator,
    expr: &Expression,
    ctx: &EvalContext<'_>,
) -> SqlResult<Value> {
    match expr {
        Expression::Literal(v) => Ok(v.clone()),
        Expression::Placeholder(i) => {
            if *i == 0 {
                return Err(SqlError::Runtime(
                    "Positional placeholder '?' was not correctly numbered".to_string(),
                ));
            }
            ctx.params.get(*i - 1).cloned().ok_or_else(|| {
                SqlError::Runtime(format!("Missing parameter for placeholder ${}", i))
            })
        }
        Expression::Column(name) => {
            if let Ok(val) = resolve_column(name, ctx.contexts) {
                return Ok(val);
            }
            if let Ok(val) = resolve_column(name, ctx.outer_contexts) {
                return Ok(val);
            }

            Err(SqlError::ColumnNotFound(name.clone()))
        }
        Expression::Subquery(subquery) => subquery::evaluate_subquery(
            executor,
            subquery,
            ctx.contexts,
            ctx.params,
            ctx.outer_contexts,
            ctx.db_state,
        ),
        Expression::BinaryOp(left, op, right) => {
            let l = evaluate_expression_joined(executor, left, ctx)?;
            let r = evaluate_expression_joined(executor, right, ctx)?;
            binary::evaluate_binary_op(l, op, r)
        }
        Expression::ScalarFunc(sf) => {
            let mut eval_args = Vec::new();
            for arg in &sf.args {
                eval_args.push(evaluate_expression_joined(executor, arg, ctx)?);
            }
            function::evaluate_scalar_func(&sf.name, &eval_args)
        }
        Expression::FunctionCall(_) => Err(SqlError::Runtime(
            "Aggregate functions must be evaluated at the top level".to_string(),
        )),
        Expression::Star => Err(SqlError::Runtime(
            "Star expression must be evaluated at the top level".to_string(),
        )),
    }
}
