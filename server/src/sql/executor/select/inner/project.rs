use crate::sql::executor::Executor;
use crate::squeal;
use crate::squeal::Select;
use crate::storage::{DatabaseState, Row, Table};
use std::collections::HashMap;

pub type JoinedContext<'a> = Vec<(&'a Table, Option<String>, Row)>;

impl Executor {
    pub(crate) fn get_result_column_names(
        &self,
        stmt: &Select,
        base_table: &Table,
        joins: &[squeal::Join],
        db_state: &DatabaseState,
        cte_tables: &HashMap<String, Table>,
    ) -> Vec<String> {
        let mut names = Vec::new();
        for col in &stmt.columns {
            if let Some(alias) = &col.alias {
                names.push(alias.clone());
                continue;
            }

            match &col.expr {
                squeal::Expression::Star => {
                    names.extend(base_table.schema.columns.iter().map(|c| c.name.clone()));
                    for join in joins {
                        let join_table = if let Some(t) = cte_tables.get(&join.table) {
                            Some(t)
                        } else {
                            db_state.get_table(&join.table)
                        };

                        if let Some(t) = join_table {
                            names.extend(t.schema.columns.iter().map(|c| c.name.clone()));
                        }
                    }
                }
                squeal::Expression::Column(name) => names.push(name.clone()),
                squeal::Expression::FunctionCall(fc) => {
                    let name = format!("{:?}", fc.name).to_uppercase();
                    names.push(format!("{}(...)", name));
                }
                squeal::Expression::ScalarFunc(sf) => {
                    let name = format!("{:?}", sf.name).to_uppercase();
                    names.push(format!("{}(...)", name));
                }
                _ => names.push(format!("col_{}", names.len())),
            }
        }
        names
    }
}
