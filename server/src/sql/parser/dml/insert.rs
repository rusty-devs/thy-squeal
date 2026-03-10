use super::super::super::ast::{Expression, InsertStmt, SqlStmt};
use super::super::super::error::{SqlError, SqlResult};
use super::super::super::parser::Rule;
use super::super::expr::parse_any_expression;

pub fn parse_insert(pair: pest::iterators::Pair<Rule>) -> SqlResult<SqlStmt> {
    let inner = pair.into_inner();

    let mut table = None;
    let mut columns = None;
    let mut values = Vec::new();

    for p in inner {
        match p.as_rule() {
            Rule::table_name => table = Some(p.as_str().trim().to_string()),
            Rule::column_list => {
                let mut cols = Vec::new();
                for col_pair in p.into_inner() {
                    if col_pair.as_rule() == Rule::column_expr {
                        let expr_pair = col_pair.into_inner().next().unwrap();
                        cols.push(expr_pair.as_str().trim().to_string());
                    }
                }
                columns = Some(cols);
            }
            Rule::value_list => {
                values = parse_value_list(p)?;
            }
            _ => {}
        }
    }

    let table = table.ok_or_else(|| SqlError::Parse("Missing table name".to_string()))?;
    if values.is_empty() {
        return Err(SqlError::Parse("Missing values".to_string()));
    }

    Ok(SqlStmt::Insert(InsertStmt { table, columns, values }))
}

pub fn parse_value_list(
    pair: pest::iterators::Pair<Rule>,
) -> SqlResult<Vec<Expression>> {
    let inner = pair.into_inner();
    let mut values = Vec::new();
    for p in inner {
        match p.as_rule() {
            Rule::literal 
            | Rule::string_literal 
            | Rule::number_literal 
            | Rule::boolean_literal 
            | Rule::KW_NULL 
            | Rule::placeholder => {
                values.push(parse_any_expression(p)?);
            }
            _ => {}
        }
    }
    Ok(values)
}
