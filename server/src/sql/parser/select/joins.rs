use super::super::super::ast::{Join, JoinType};
use super::super::super::error::{SqlError, SqlResult};
use super::super::super::parser::Rule;
use super::super::expr::parse_condition;
use super::columns::parse_alias;

pub fn parse_join(pair: pest::iterators::Pair<Rule>) -> SqlResult<Join> {
    let inner = pair.into_inner();
    let mut join_type = JoinType::Inner;
    let mut table_name_pair = None;
    let mut on_condition_pair = None;

    for p in inner {
        match p.as_rule() {
            Rule::KW_INNER | Rule::KW_LEFT => {
                // If the grammar has them as separate tokens in join_clause
                if p.as_rule() == Rule::KW_LEFT {
                    join_type = JoinType::Left;
                }
            }
            Rule::table_name_with_alias => {
                table_name_pair = Some(p);
            }
            Rule::condition => {
                on_condition_pair = Some(p);
            }
            // Check nested join type (e.g. ((KW_INNER | KW_LEFT)? ~ KW_JOIN))
            _ if p.as_str().to_uppercase().contains("LEFT") => {
                join_type = JoinType::Left;
            }
            _ => {}
        }
    }

    let table_pair =
        table_name_pair.ok_or_else(|| SqlError::Parse("Missing table name in JOIN".to_string()))?;
    let mut table_inner = table_pair.into_inner();
    let table = table_inner
        .next()
        .ok_or_else(|| SqlError::Parse("Missing table name in JOIN".to_string()))?
        .as_str()
        .trim()
        .to_string();

    let mut table_alias = None;
    if let Some(alias_pair) = table_inner.next() {
        table_alias = Some(parse_alias(alias_pair)?);
    }

    let on_pair = on_condition_pair
        .ok_or_else(|| SqlError::Parse("Missing condition in JOIN ON".to_string()))?;
    let on = parse_condition(on_pair)?;

    Ok(Join {
        table,
        table_alias,
        join_type,
        on,
    })
}
