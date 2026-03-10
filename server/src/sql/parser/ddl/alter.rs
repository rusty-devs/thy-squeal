use super::super::super::ast::{AlterAction, AlterTableStmt, SqlStmt};
use super::super::super::error::{SqlError, SqlResult};
use super::super::super::parser::Rule;
use super::create::parse_column_def;

pub fn parse_alter_table(pair: pest::iterators::Pair<Rule>) -> SqlResult<SqlStmt> {
    let mut inner = pair.into_inner();
    // Skip KW_ALTER, KW_TABLE
    let _ = inner.next();
    let _ = inner.next();

    let table = inner
        .next()
        .map(|p| p.as_str().trim().to_string())
        .ok_or_else(|| SqlError::Parse("Missing table name in ALTER TABLE".to_string()))?;

    let action_pair = inner
        .next()
        .ok_or_else(|| SqlError::Parse("Missing action in ALTER TABLE".to_string()))?;

    let action = match action_pair.as_rule() {
        Rule::alter_add_column => {
            let mut action_inner = action_pair.into_inner();
            // Skip KW_ADD, maybe KW_COLUMN
            let mut next = action_inner.next().unwrap();
            if next.as_rule() == Rule::KW_ADD {
                next = action_inner.next().unwrap();
            }
            if next.as_rule() == Rule::KW_COLUMN {
                next = action_inner.next().unwrap();
            }
            
            AlterAction::AddColumn(parse_column_def(next)?)
        }
        Rule::alter_drop_column => {
            let mut action_inner = action_pair.into_inner();
            let mut next = action_inner.next().unwrap();
            // Skip KW_DROP, maybe KW_COLUMN
            if next.as_rule() == Rule::KW_DROP {
                next = action_inner.next().unwrap();
            }
            if next.as_rule() == Rule::KW_COLUMN {
                next = action_inner.next().unwrap();
            }
            AlterAction::DropColumn(next.as_str().trim().to_string())
        }
        Rule::alter_rename_column => {
            let mut action_inner = action_pair.into_inner();
            // Skip KW_RENAME, maybe KW_COLUMN
            let mut next = action_inner.next().unwrap();
            if next.as_rule() == Rule::KW_RENAME {
                next = action_inner.next().unwrap();
            }
            if next.as_rule() == Rule::KW_COLUMN {
                next = action_inner.next().unwrap();
            }
            let old_name = next.as_str().trim().to_string();
            // Skip KW_TO
            let _ = action_inner.next();
            let new_name = action_inner
                .next()
                .ok_or_else(|| SqlError::Parse("Missing new column name in RENAME COLUMN".to_string()))?
                .as_str()
                .trim()
                .to_string();
            AlterAction::RenameColumn { old_name, new_name }
        }
        Rule::alter_rename_table => {
            let mut action_inner = action_pair.into_inner();
            // Skip KW_RENAME, KW_TO
            let _ = action_inner.next();
            let _ = action_inner.next();
            let new_name = action_inner
                .next()
                .ok_or_else(|| SqlError::Parse("Missing new table name in RENAME TABLE".to_string()))?
                .as_str()
                .trim()
                .to_string();
            AlterAction::RenameTable(new_name)
        }
        _ => return Err(SqlError::Parse("Unknown ALTER TABLE action".to_string())),
    };

    Ok(SqlStmt::AlterTable(AlterTableStmt { table, action }))
}
