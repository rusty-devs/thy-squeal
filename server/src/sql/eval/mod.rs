pub mod column;
pub mod condition;
pub mod expression;

use crate::storage::{DatabaseState, Row, Table, Value};
use super::ast::{Condition, Expression};
use super::error::{SqlError, SqlResult};
use futures::future::BoxFuture;
use futures::FutureExt;

pub use condition::evaluate_condition_joined;
pub use expression::evaluate_expression_joined;

/// Trait for evaluating expressions, implemented by Executor and RecoveryEvaluator
pub trait Evaluator: Send + Sync {
    fn exec_select_internal<'a>(
        &'a self,
        stmt: super::ast::SelectStmt,
        outer_contexts: &'a [(&'a Table, Option<&'a str>, &'a Row)],
        db_state: &'a DatabaseState,
    ) -> BoxFuture<'a, SqlResult<super::executor::QueryResult>>;
}

/// A simple evaluator used during WAL recovery when a full Executor is not yet available.
/// It does not support subqueries.
pub struct RecoveryEvaluator;

impl Evaluator for RecoveryEvaluator {
    fn exec_select_internal<'a>(
        &'a self,
        _stmt: super::ast::SelectStmt,
        _outer_contexts: &'a [(&'a Table, Option<&'a str>, &'a Row)],
        _db_state: &'a DatabaseState,
    ) -> BoxFuture<'a, SqlResult<super::executor::QueryResult>> {
        async {
            Err(SqlError::Runtime(
                "Subqueries are not supported during WAL recovery".to_string(),
            ))
        }
        .boxed()
    }
}

#[allow(dead_code)]
pub fn evaluate_condition(
    executor: &dyn Evaluator,
    cond: &Condition,
    table: &Table,
    table_alias: Option<&str>,
    row: &Row,
    db_state: &DatabaseState,
) -> SqlResult<bool> {
    evaluate_condition_joined(executor, cond, &[(table, table_alias, row)], &[], db_state)
}

#[allow(dead_code)]
pub fn evaluate_expression(
    executor: &dyn Evaluator,
    expr: &Expression,
    table: &Table,
    table_alias: Option<&str>,
    row: &Row,
    db_state: &DatabaseState,
) -> SqlResult<Value> {
    evaluate_expression_joined(executor, expr, &[(table, table_alias, row)], &[], db_state)
}
