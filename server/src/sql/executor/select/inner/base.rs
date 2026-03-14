use crate::sql::error::{SqlError, SqlResult};
use crate::sql::executor::{Executor, SelectQueryPlan};
use crate::squeal;
use crate::storage::info_schema::get_info_schema_tables;
use crate::storage::{Row, Table};
use std::collections::HashMap;

pub enum ResolvedTable<'b> {
    Physical(&'b Table),
    Cte(&'b Table),
    Virtual(Box<Table>),
}

impl<'b> ResolvedTable<'b> {
    pub fn table(&self) -> &Table {
        match self {
            ResolvedTable::Physical(t) => t,
            ResolvedTable::Cte(t) => t,
            ResolvedTable::Virtual(t) => t,
        }
    }
}

impl Executor {
    pub(crate) fn resolve_base_table<'b>(
        &self,
        plan: &SelectQueryPlan<'b>,
        cte_tables: &'b HashMap<String, Table>,
    ) -> SqlResult<(ResolvedTable<'b>, Vec<Row>)> {
        let stmt = &plan.stmt;
        let db_state = plan.db_state;

        if stmt.table.is_empty() {
            let dual_table = Table::new("dual".to_string(), vec![], None, vec![]);
            let rows = vec![Row {
                id: "dual".to_string(),
                values: vec![],
            }];
            Ok((ResolvedTable::Virtual(Box::new(dual_table)), rows))
        } else if let Some(t) = cte_tables.get(&stmt.table) {
            Ok((ResolvedTable::Cte(t), t.data.rows.clone()))
        } else if stmt.table.starts_with("information_schema.") {
            let table_name = stmt.table.strip_prefix("information_schema.").unwrap();
            let info_schema_storage = get_info_schema_tables(db_state);
            let t = info_schema_storage
                .get(table_name)
                .ok_or_else(|| SqlError::TableNotFound(stmt.table.clone()))?;
            Ok((
                ResolvedTable::Virtual(Box::new(t.clone())),
                t.data.rows.clone(),
            ))
        } else {
            let t = db_state
                .get_table(&stmt.table)
                .ok_or_else(|| SqlError::TableNotFound(stmt.table.clone()))?;

            let rows = if stmt.joins.is_empty() {
                self.apply_index_optimization(t, stmt)
            } else {
                t.data.rows.clone()
            };
            Ok((ResolvedTable::Physical(t), rows))
        }
    }

    fn apply_index_optimization(&self, t: &Table, stmt: &squeal::Select) -> Vec<Row> {
        let mut best_index = None;
        let mut best_estimated_rows = t.data.rows.len();

        if let Some(squeal::Condition::Comparison(
            left_expr,
            squeal::ComparisonOp::Eq,
            squeal::Expression::Literal(val),
        )) = &stmt.where_clause
        {
            for (idx_name, index) in &t.indexes.secondary {
                let exprs = index.expressions();
                if exprs.len() == 1 && &exprs[0] == left_expr {
                    let key = vec![val.clone()];
                    let estimated = if let Some(ids) = index.get(&key) {
                        ids.len()
                    } else {
                        0
                    };

                    if estimated < best_estimated_rows {
                        best_estimated_rows = estimated;
                        best_index = Some((idx_name, index, key));
                    }
                }
            }
        }

        let selectivity_threshold = (t.data.rows.len() as f64 * 0.3) as usize;

        if let Some((_name, index, key)) = best_index
            && (best_estimated_rows < selectivity_threshold || t.data.rows.len() < 10)
        {
            if let Some(row_ids) = index.get(&key) {
                t.data
                    .rows
                    .iter()
                    .filter(|r| row_ids.contains(&r.id))
                    .cloned()
                    .collect()
            } else {
                vec![]
            }
        } else {
            t.data.rows.clone()
        }
    }
}
