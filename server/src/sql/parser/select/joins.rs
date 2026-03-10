use super::super::super::ast::{Join, JoinType};
use super::super::super::error::{SqlError, SqlResult};
use super::super::super::parser::Rule;
use super::super::expr::parse_condition;
use super::columns::parse_alias;

pub fn parse_join(pair: pest::iterators::Pair<Rule>) -> SqlResult<Join> {
    let mut inner = pair.into_inner();
    let type_pair = inner.next().unwrap();
    let mut join_type = JoinType::Inner;
    
    let mut type_inner = type_pair.into_inner();
    if let Some(t) = type_inner.next() {
        if t.as_rule() == Rule::KW_LEFT {
            join_type = JoinType::Left;
        }
    }

    let table_pair = inner.next().unwrap();
    let mut table_inner = table_pair.into_inner();
    let table = table_inner.next().unwrap().as_str().trim().to_string();
    let mut table_alias = None;
    if let Some(alias_pair) = table_inner.next() {
        table_alias = Some(parse_alias(alias_pair)?);
    }

    // Skip KW_ON
    let _ = inner.next();
    let on = parse_condition(inner.next().unwrap())?;

    Ok(Join {
        table,
        table_alias,
        join_type,
        on,
    })
}
