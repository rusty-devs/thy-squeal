use super::super::ast::{Join, JoinType, SelectColumn, SelectStmt, SqlStmt, OrderByItem, LimitClause};
use super::super::error::{SqlError, SqlResult};
use super::super::parser::Rule;
use super::utils::expect_identifier;

pub fn parse_select(pair: pest::iterators::Pair<Rule>) -> SqlResult<SqlStmt> {
    let inner = if pair.as_rule() == Rule::select_stmt {
        pair.into_inner().next().unwrap().into_inner()
    } else {
        pair.into_inner()
    };

    let mut distinct = false;
    let mut columns = Vec::new();
    let mut table_name = String::new();
    let mut table_alias = None;
    let mut joins = Vec::new();
    let mut where_clause = None;
    let mut order_by = Vec::new();
    let mut limit = None;
    let mut group_by = Vec::new();
    let mut having = None;

    for p in inner {
        match p.as_rule() {
            Rule::distinct => distinct = true,
            Rule::select_columns => {
                columns = parse_select_columns(p)?;
            }
            Rule::table_name_with_alias => {
                let mut table_inner = p.into_inner();
                let table_pair = table_inner.next().ok_or_else(|| SqlError::Parse("Missing table name".to_string()))?;
                table_name = table_pair.as_str().trim().to_string();
                if let Some(alias_pair) = table_inner.next() {
                    table_alias = Some(expect_identifier(alias_pair.into_inner().next(), "table alias")?);
                }
            }
            Rule::join_clause => {
                joins.push(parse_join(p)?);
            }
            Rule::where_clause => {
                where_clause = Some(super::expr::parse_where_clause(p)?);
            }
            Rule::group_by_clause => {
                group_by = parse_group_by(p)?;
            }
            Rule::having_clause => {
                having = Some(parse_having(p)?);
            }
            Rule::order_by_clause => {
                order_by = parse_order_by(p)?;
            }
            Rule::limit_clause => {
                limit = Some(parse_limit(p)?);
            }
            _ => {}
        }
    }

    if table_name.is_empty() {
        return Err(SqlError::Parse("Missing table name in SELECT".to_string()));
    }
    if columns.is_empty() {
        return Err(SqlError::Parse("Missing columns in SELECT".to_string()));
    }

    Ok(SqlStmt::Select(SelectStmt {
        table: table_name,
        table_alias,
        columns,
        joins,
        where_clause,
        order_by,
        limit,
        distinct,
        group_by,
        having,
    }))
}

pub fn parse_select_columns(pair: pest::iterators::Pair<Rule>) -> SqlResult<Vec<SelectColumn>> {
    let mut columns = Vec::new();
    let inner = pair.into_inner();
    
    for p in inner {
        match p.as_rule() {
            Rule::star => {
                columns.push(SelectColumn {
                    expr: crate::sql::ast::Expression::Star,
                    alias: None,
                });
            }
            Rule::column_list => {
                for col_expr_pair in p.into_inner() {
                    if col_expr_pair.as_rule() == Rule::column_expr {
                        let mut col_inner = col_expr_pair.into_inner();
                        let expr = super::expr::parse_expression(
                            col_inner
                                .next()
                                .ok_or_else(|| SqlError::Parse("Missing expression in column".to_string()))?,
                        )?;
                        let alias = col_inner
                            .find(|p| p.as_rule() == Rule::alias)
                            .and_then(|p| p.into_inner().find(|p| p.as_rule() == Rule::identifier))
                            .map(|p| p.as_str().trim().to_string());
                        columns.push(SelectColumn { expr, alias });
                    }
                }
            }
            _ => {
                if p.as_str() == "*" {
                    columns.push(SelectColumn {
                        expr: crate::sql::ast::Expression::Star,
                        alias: None,
                    });
                }
            }
        }
    }
    
    Ok(columns)
}

fn parse_join(pair: pest::iterators::Pair<Rule>) -> SqlResult<Join> {
    let mut inner = pair.into_inner();
    
    let mut join_type = JoinType::Inner;
    
    let next = inner.next().ok_or_else(|| SqlError::Parse("Empty JOIN clause".to_string()))?;
    
    let mut table_with_alias_pair = next;
    
    if table_with_alias_pair.as_rule() == Rule::KW_INNER {
        join_type = JoinType::Inner;
        inner.next(); // skip KW_JOIN
        table_with_alias_pair = inner.next().ok_or_else(|| SqlError::Parse("Missing table name in JOIN".to_string()))?;
    } else if table_with_alias_pair.as_rule() == Rule::KW_LEFT {
        join_type = JoinType::Left;
        inner.next(); // skip KW_JOIN
        table_with_alias_pair = inner.next().ok_or_else(|| SqlError::Parse("Missing table name in JOIN".to_string()))?;
    } else if table_with_alias_pair.as_rule() == Rule::KW_JOIN {
        table_with_alias_pair = inner.next().ok_or_else(|| SqlError::Parse("Missing table name in JOIN".to_string()))?;
    }

    let mut table_inner = table_with_alias_pair.into_inner();
    let table_pair = table_inner.next().ok_or_else(|| SqlError::Parse("Missing table name in JOIN".to_string()))?;
    let table = table_pair.as_str().trim().to_string();
    let mut table_alias = None;
    if let Some(alias_pair) = table_inner.next() {
        table_alias = Some(expect_identifier(alias_pair.into_inner().next(), "table alias")?);
    }

    let on = super::expr::parse_condition(
        inner
            .find(|p| p.as_rule() == Rule::condition)
            .ok_or_else(|| SqlError::Parse("Missing JOIN condition".to_string()))?,
    )?;

    Ok(Join {
        table,
        table_alias,
        on,
        join_type,
    })
}

pub fn parse_group_by(pair: pest::iterators::Pair<Rule>) -> SqlResult<Vec<super::super::ast::Expression>> {
    let mut exprs = Vec::new();
    let mut inner = pair.into_inner();
    
    let column_list = inner
        .find(|p| p.as_rule() == Rule::column_list)
        .ok_or_else(|| SqlError::Parse("Missing column list in GROUP BY".to_string()))?;
        
    for p in column_list.into_inner() {
        if p.as_rule() == Rule::column_expr {
            // GROUP BY expressions are just expressions, ignore aliases if present
            let expr_pair = p.into_inner().next().unwrap();
            exprs.push(super::expr::parse_expression(expr_pair)?);
        }
    }
    Ok(exprs)
}

pub fn parse_having(pair: pest::iterators::Pair<Rule>) -> SqlResult<super::super::ast::Condition> {
    let mut inner = pair.into_inner();
    let cond_pair = inner
        .find(|p| p.as_rule() == Rule::condition)
        .ok_or_else(|| SqlError::Parse("Missing HAVING condition".to_string()))?;
    super::expr::parse_condition(cond_pair)
}

pub fn parse_order_by(pair: pest::iterators::Pair<Rule>) -> SqlResult<Vec<OrderByItem>> {
    let mut items = Vec::new();
    let mut inner = pair.into_inner();
    
    let order_by_list = inner
        .find(|p| p.as_rule() == Rule::order_by_list)
        .ok_or_else(|| SqlError::Parse("Missing order by list".to_string()))?;
        
    for p in order_by_list.into_inner() {
        if p.as_rule() == Rule::order_by_item {
            let mut item_inner = p.into_inner();
            let expr = super::expr::parse_expression(item_inner.next().unwrap())?;
            let order = if let Some(o) = item_inner.next() {
                if o.as_str().to_uppercase() == "DESC" {
                    super::super::ast::Order::Desc
                } else {
                    super::super::ast::Order::Asc
                }
            } else {
                super::super::ast::Order::Asc
            };
            items.push(OrderByItem { expr, order });
        }
    }
    Ok(items)
}

pub fn parse_limit(pair: pest::iterators::Pair<Rule>) -> SqlResult<LimitClause> {
    let mut inner = pair.into_inner();
    let count = inner
        .find(|p| p.as_rule() == Rule::limit_count)
        .ok_or_else(|| SqlError::Parse("Missing LIMIT count".to_string()))?
        .as_str()
        .parse()
        .map_err(|_| SqlError::Parse("Invalid LIMIT count".to_string()))?;
        
    let offset = if let Some(o) = inner.find(|p| p.as_rule() == Rule::offset_clause) {
        Some(
            o.into_inner()
                .find(|p| p.as_rule() == Rule::limit_count)
                .unwrap()
                .as_str()
                .parse()
                .map_err(|_| SqlError::Parse("Invalid OFFSET".to_string()))?,
        )
    } else {
        None
    };
    Ok(LimitClause { count, offset })
}
