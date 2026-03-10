use super::super::super::ast::{Expression, LimitClause, Order, OrderByItem};
use super::super::super::error::SqlResult;
use super::super::expr::parse_expression;
use crate::sql::parser::Rule;

pub fn parse_group_by(pair: pest::iterators::Pair<Rule>) -> SqlResult<Vec<Expression>> {
    let mut group_by = Vec::new();
    let inner = pair.into_inner();
    // Skip KW_GROUP, KW_BY
    let mut inner = inner.skip(2);
    if let Some(list_pair) = inner.next() {
        for col_pair in list_pair.into_inner() {
            let mut col_inner = col_pair.into_inner();
            let expr = parse_expression(col_inner.next().unwrap())?;
            group_by.push(expr);
        }
    }
    Ok(group_by)
}

pub fn parse_order_by(pair: pest::iterators::Pair<Rule>) -> SqlResult<Vec<OrderByItem>> {
    let mut order_by = Vec::new();
    let inner = pair.into_inner();
    // Skip KW_ORDER, KW_BY
    let mut inner = inner.skip(2);
    if let Some(list_pair) = inner.next() {
        for item in list_pair.into_inner() {
            let mut item_inner = item.into_inner();
            let expr = parse_expression(item_inner.next().unwrap())?;
            let mut order = Order::Asc;
            if let Some(o) = item_inner.next()
                && o.as_rule() == Rule::KW_DESC
            {
                order = Order::Desc;
            }
            order_by.push(OrderByItem { expr, order });
        }
    }
    Ok(order_by)
}

pub fn parse_limit(pair: pest::iterators::Pair<Rule>) -> SqlResult<LimitClause> {
    let mut inner = pair.into_inner();
    // Skip KW_LIMIT
    let _ = inner.next();
    let count = inner.next().unwrap().as_str().parse::<usize>().unwrap();
    let mut offset = None;
    if let Some(offset_pair) = inner.next() {
        let mut offset_inner = offset_pair.into_inner();
        // Skip KW_OFFSET
        let _ = offset_inner.next();
        offset = Some(
            offset_inner
                .next()
                .unwrap()
                .as_str()
                .parse::<usize>()
                .unwrap(),
        );
    }
    Ok(LimitClause { count, offset })
}
