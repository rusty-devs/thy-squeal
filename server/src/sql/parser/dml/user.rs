use super::super::super::ast::{CreateUserStmt, DropUserStmt, GrantStmt, RevokeStmt, SqlStmt};
use super::super::super::error::{SqlError, SqlResult};
use crate::sql::parser::Rule;
use crate::storage::Privilege;

pub fn parse_create_user(pair: pest::iterators::Pair<Rule>) -> SqlResult<SqlStmt> {
    let mut inner = pair.into_inner();
    let _ = inner.next(); // KW_CREATE
    let _ = inner.next(); // KW_USER
    
    let username = inner.next().unwrap().as_str().trim_matches('\'').to_string();
    let _ = inner.next(); // KW_IDENTIFIED
    let _ = inner.next(); // KW_BY
    let password = inner.next().unwrap().as_str().trim_matches('\'').to_string();

    Ok(SqlStmt::CreateUser(CreateUserStmt { username, password }))
}

pub fn parse_drop_user(pair: pest::iterators::Pair<Rule>) -> SqlResult<SqlStmt> {
    let mut inner = pair.into_inner();
    let _ = inner.next(); // KW_DROP
    let _ = inner.next(); // KW_USER
    let username = inner.next().unwrap().as_str().trim_matches('\'').to_string();

    Ok(SqlStmt::DropUser(DropUserStmt { username }))
}

pub fn parse_grant(pair: pest::iterators::Pair<Rule>) -> SqlResult<SqlStmt> {
    let mut inner = pair.into_inner();
    let _ = inner.next(); // KW_GRANT
    
    let privileges = parse_privilege_list(inner.next().unwrap())?;
    let _ = inner.next(); // KW_ON
    
    let target = inner.next().unwrap();
    let table = if target.as_rule() == Rule::table_name {
        Some(target.as_str().trim().to_string())
    } else {
        // KW_ALL KW_PRIVILEGES
        let _ = inner.next(); // Consume second part
        None
    };

    let _ = inner.next(); // KW_TO
    let username = inner.next().unwrap().as_str().trim_matches('\'').to_string();

    Ok(SqlStmt::Grant(GrantStmt { privileges, table, username }))
}

pub fn parse_revoke(pair: pest::iterators::Pair<Rule>) -> SqlResult<SqlStmt> {
    let mut inner = pair.into_inner();
    let _ = inner.next(); // KW_REVOKE
    
    let privileges = parse_privilege_list(inner.next().unwrap())?;
    let _ = inner.next(); // KW_ON
    
    let target = inner.next().unwrap();
    let table = if target.as_rule() == Rule::table_name {
        Some(target.as_str().trim().to_string())
    } else {
        // KW_ALL KW_PRIVILEGES
        let _ = inner.next(); // Consume second part
        None
    };

    let _ = inner.next(); // KW_FROM
    let username = inner.next().unwrap().as_str().trim_matches('\'').to_string();

    Ok(SqlStmt::Revoke(RevokeStmt { privileges, table, username }))
}

fn parse_privilege_list(pair: pest::iterators::Pair<Rule>) -> SqlResult<Vec<Privilege>> {
    let mut privs = Vec::new();
    for p in pair.into_inner() {
        if p.as_rule() == Rule::privilege {
            let s = p.as_str().to_uppercase();
            let priv_enum = match s.as_str() {
                "SELECT" => Privilege::Select,
                "INSERT" => Privilege::Insert,
                "UPDATE" => Privilege::Update,
                "DELETE" => Privilege::Delete,
                "CREATE" => Privilege::Create,
                "DROP" => Privilege::Drop,
                "GRANT" => Privilege::Grant,
                "ALL" => Privilege::All,
                _ => return Err(SqlError::Parse(format!("Unknown privilege: {}", s))),
            };
            privs.push(priv_enum);
        }
    }
    Ok(privs)
}
