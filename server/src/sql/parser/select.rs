use super::super::ast::{
    Expression, Join, JoinType, LimitClause, Order, OrderByItem, SelectColumn, SelectStmt, SqlStmt,
};
use super::super::error::{SqlError, SqlResult};
use super::super::parser::Rule;
use super::expr::{parse_expression, parse_where_clause, parse_condition};

pub fn parse_select(pair: pest::iterators::Pair<Rule>) -> SqlResult<SqlStmt> {
    let inner = pair.into_inner();

    let distinct = inner.clone().find(|p| p.as_rule() == Rule::distinct).is_some();

    let select_columns_pair = inner.clone().find(|p| p.as_rule() == Rule::select_columns).ok_or_else(|| SqlError::Parse("Missing SELECT columns".to_string()))?;
    let columns = parse_select_columns(select_columns_pair)?;

    let table = inner
        .clone()
        .find(|p| p.as_rule() == Rule::table_name)
        .map(|p| p.as_str().trim().to_string())
        .ok_or_else(|| SqlError::Parse("Missing table name".to_string()))?;

    let joins = inner
        .clone()
        .filter(|p| p.as_rule() == Rule::join_clause)
        .map(parse_join)
        .collect::<SqlResult<Vec<Join>>>()?;

    let where_clause = if let Some(p) = inner.clone().find(|p| p.as_rule() == Rule::where_clause) {
        Some(parse_where_clause(p)?)
    } else {
        None
    };

    let group_by = if let Some(p) = inner.clone().find(|p| p.as_rule() == Rule::group_by_clause) {
        parse_group_by(p)?
    } else {
        Vec::new()
    };

    let having = if let Some(p) = inner.clone().find(|p| p.as_rule() == Rule::having_clause) {
        Some(parse_having(p)?)
    } else {
        None
    };

    let order_by = if let Some(p) = inner.clone().find(|p| p.as_rule() == Rule::order_by_clause) {
        parse_order_by(p)?
    } else {
        Vec::new()
    };

    let limit = if let Some(p) = inner.clone().find(|p| p.as_rule() == Rule::limit_clause) {
        Some(parse_limit(p)?)
    } else {
        None
    };

    Ok(SqlStmt::Select(SelectStmt {
        columns,
        table,
        distinct,
        joins,
        where_clause,
        group_by,
        having,
        order_by,
        limit,
    }))
}

pub fn parse_join(pair: pest::iterators::Pair<Rule>) -> SqlResult<Join> {
    let mut inner = pair.into_inner();
    
    // KW_JOIN might be preceded by KW_INNER
    let first = inner.next().ok_or_else(|| SqlError::Parse("Empty join clause".to_string()))?;
    if first.as_rule() == Rule::KW_INNER {
        inner.next().ok_or_else(|| SqlError::Parse("Missing JOIN keyword after INNER".to_string()))?;
    }
    
    let table = inner
        .find(|p| p.as_rule() == Rule::table_name)
        .map(|p| p.as_str().trim().to_string())
        .ok_or_else(|| SqlError::Parse("Missing join table name".to_string()))?;

    let on_cond = inner
        .find(|p| p.as_rule() == Rule::condition)
        .ok_or_else(|| SqlError::Parse("Missing JOIN ON condition".to_string()))?;
    let on = parse_condition(on_cond)?;

    Ok(Join {
        table,
        join_type: JoinType::Inner,
        on,
    })
}

pub fn parse_group_by(pair: pest::iterators::Pair<Rule>) -> SqlResult<Vec<Expression>> {
    let mut inner = pair.into_inner();
    let column_list = inner.find(|p| p.as_rule() == Rule::column_list).ok_or_else(|| SqlError::Parse("Missing GROUP BY column list".to_string()))?;
    let mut exprs = Vec::new();
    for col_expr in column_list.into_inner() {
        if col_expr.as_rule() == Rule::column_expr {
            let mut ce_inner = col_expr.into_inner();
            let expr = parse_expression(ce_inner.find(|p| p.as_rule() == Rule::expression).ok_or_else(|| SqlError::Parse("Empty GROUP BY column expression".to_string()))?)?;
            exprs.push(expr);
        }
    }
    Ok(exprs)
}

pub fn parse_having(pair: pest::iterators::Pair<Rule>) -> SqlResult<super::super::ast::Condition> {
    let mut inner = pair.into_inner();
    let cond_pair = inner.find(|p| p.as_rule() == Rule::condition).ok_or_else(|| SqlError::Parse("Missing HAVING condition".to_string()))?;
    parse_condition(cond_pair)
}

pub fn parse_order_by(pair: pest::iterators::Pair<Rule>) -> SqlResult<Vec<OrderByItem>> {
    let mut inner = pair.into_inner();
    let list = inner.find(|p| p.as_rule() == Rule::order_by_list).ok_or_else(|| SqlError::Parse("Missing ORDER BY list".to_string()))?;
    let mut items = Vec::new();
    for item in list.into_inner() {
        if item.as_rule() == Rule::order_by_item {
            let mut it_inner = item.into_inner();
            let expr = parse_expression(it_inner.find(|p| p.as_rule() == Rule::expression).ok_or_else(|| SqlError::Parse("Missing ORDER BY expression".to_string()))?)?;
            let order = if let Some(op) = it_inner.find(|p| matches!(p.as_rule(), Rule::KW_ASC | Rule::KW_DESC)) {
                if op.as_rule() == Rule::KW_DESC {
                    Order::Desc
                } else {
                    Order::Asc
                }
            } else {
                Order::Asc
            };
            items.push(OrderByItem { expr, order });
        }
    }
    Ok(items)
}

pub fn parse_limit(pair: pest::iterators::Pair<Rule>) -> SqlResult<LimitClause> {
    let mut inner = pair.into_inner();
    let count: usize = inner
        .find(|p| p.as_rule() == Rule::limit_count)
        .ok_or_else(|| SqlError::Parse("Missing LIMIT count".to_string()))?
        .as_str()
        .parse()
        .map_err(|e| SqlError::Parse(format!("Invalid LIMIT count: {}", e)))?;

    let offset = if let Some(off_pair) = inner.find(|p| p.as_rule() == Rule::offset_clause) {
        let off: usize = off_pair
            .into_inner()
            .find(|p| p.as_rule() == Rule::limit_count)
            .ok_or_else(|| SqlError::Parse("Missing OFFSET count".to_string()))?
            .as_str()
            .parse()
            .map_err(|e| SqlError::Parse(format!("Invalid OFFSET count: {}", e)))?;
        Some(off)
    } else {
        None
    };

    Ok(LimitClause { count, offset })
}

pub fn parse_select_columns(pair: pest::iterators::Pair<Rule>) -> SqlResult<Vec<SelectColumn>> {
    let mut inner = pair.clone().into_inner();
    let first = match inner.next() {
        Some(p) => p,
        None => {
            if pair.as_str().trim() == "*" {
                return Ok(vec![SelectColumn { expr: Expression::Star, alias: None }]);
            }
            return Err(SqlError::Parse("Empty select columns".to_string()));
        }
    };

    if first.as_rule() == Rule::column_list {
        let mut cols = Vec::new();
        for col_expr in first.into_inner() {
            if col_expr.as_rule() == Rule::column_expr {
                let mut ce_inner = col_expr.into_inner();
                let expr = parse_expression(ce_inner.find(|p| p.as_rule() == Rule::expression).ok_or_else(|| SqlError::Parse("Empty column expression".to_string()))?)?;
                let alias = ce_inner.find(|p| p.as_rule() == Rule::alias).and_then(|p| {
                    p.into_inner().find(|p2| p2.as_rule() == Rule::identifier).map(|p3| p3.as_str().to_string())
                });
                cols.push(SelectColumn { expr, alias });
            }
        }
        Ok(cols)
    } else if first.as_str().trim() == "*" {
        Ok(vec![SelectColumn { expr: Expression::Star, alias: None }])
    } else {
        let expr = parse_expression(first)?;
        Ok(vec![SelectColumn { expr, alias: None }])
    }
}
