use super::project::JoinedContext;
use crate::sql::error::{SqlError, SqlResult};
use crate::sql::eval::{EvalContext, evaluate_condition_joined};
use crate::sql::executor::{Executor, SelectQueryPlan};
use crate::squeal;
use crate::storage::{Row, Table};
use std::collections::HashMap;

impl Executor {
    pub(crate) fn process_joins<'b>(
        &self,
        plan: &SelectQueryPlan<'b>,
        cte_tables: &'b HashMap<String, Table>,
        mut joined_rows: Vec<JoinedContext<'b>>,
    ) -> SqlResult<Vec<JoinedContext<'b>>> {
        let stmt = &plan.stmt;
        let outer_contexts = plan.outer_contexts;
        let params = plan.params;
        let db_state = plan.db_state;

        for join in &stmt.joins {
            let join_table = if let Some(t) = cte_tables.get(&join.table) {
                t
            } else if join.table.starts_with("information_schema.") {
                return Err(SqlError::Runtime(
                    "JOIN with information_schema is not yet supported".to_string(),
                ));
            } else {
                db_state
                    .get_table(&join.table)
                    .ok_or_else(|| SqlError::TableNotFound(join.table.clone()))?
            };

            let join_alias = join.table_alias.clone();
            let mut next_joined_rows = Vec::new();

            for existing_ctx in joined_rows {
                let mut found_match = false;
                for new_row in &join_table.data.rows {
                    let eval_ctx_list: Vec<(&Table, Option<&str>, &Row)> = existing_ctx
                        .iter()
                        .map(|(t, a, r)| (*t, a.as_deref(), r))
                        .chain(std::iter::once((
                            join_table,
                            join_alias.as_deref(),
                            new_row,
                        )))
                        .collect();

                    let eval_ctx =
                        EvalContext::new(&eval_ctx_list, params, outer_contexts, db_state);

                    if evaluate_condition_joined(self, &join.on, &eval_ctx)? {
                        let mut next_ctx = existing_ctx.clone();
                        next_ctx.push((join_table, join_alias.clone(), new_row.clone()));
                        next_joined_rows.push(next_ctx);
                        found_match = true;
                    }
                }

                if !found_match && join.join_type == squeal::JoinType::Left {
                    let mut next_ctx = existing_ctx.clone();
                    next_ctx.push((join_table, join_alias.clone(), join_table.null_row()));
                    next_joined_rows.push(next_ctx);
                }
            }
            joined_rows = next_joined_rows;
        }
        Ok(joined_rows)
    }
}
