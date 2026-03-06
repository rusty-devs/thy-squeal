use pest::Parser;
use pest_derive::Parser;

#[derive(Parser)]
#[grammar = "sql.pest"]
pub struct SqlParser;

#[derive(Debug, Clone, Copy)]
pub enum StatementKind {
    Select,
    Insert,
    Update,
    Delete,
    CreateTable,
    DropTable,
}

pub fn parse_statement_kind(input: &str) -> Result<StatementKind, String> {
    let mut pairs = SqlParser::parse(crate::sql::parser::Rule::statement, input)
        .map_err(|e| e.to_string())?;

    let stmt_pair = pairs
        .next()
        .ok_or_else(|| "Empty SQL statement".to_string())?;

    let mut inner = stmt_pair.into_inner();
    let kind_pair = inner
        .next()
        .ok_or_else(|| "Unable to determine statement type".to_string())?;

    match kind_pair.as_rule() {
        crate::sql::parser::Rule::select_stmt => Ok(StatementKind::Select),
        crate::sql::parser::Rule::insert_stmt => Ok(StatementKind::Insert),
        crate::sql::parser::Rule::update_stmt => Ok(StatementKind::Update),
        crate::sql::parser::Rule::delete_stmt => Ok(StatementKind::Delete),
        crate::sql::parser::Rule::create_table_stmt => Ok(StatementKind::CreateTable),
        crate::sql::parser::Rule::drop_table_stmt => Ok(StatementKind::DropTable),
        _ => Err("Unsupported SQL statement".to_string()),
    }
}

