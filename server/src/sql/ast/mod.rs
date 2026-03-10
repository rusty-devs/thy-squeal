pub mod condition;
pub mod expression;
pub mod statements;

pub use condition::*;
pub use expression::*;
pub use statements::*;

use serde::{Deserialize, Serialize};

/// Parsed SQL statement AST.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub enum SqlStmt {
    CreateTable(CreateTableStmt),
    CreateMaterializedView(CreateMaterializedViewStmt),
    AlterTable(AlterTableStmt),
    DropTable(DropTableStmt),
    CreateIndex(CreateIndexStmt),
    CreateUser(CreateUserStmt),
    DropUser(DropUserStmt),
    Grant(GrantStmt),
    Revoke(RevokeStmt),
    Select(SelectStmt),
    Insert(InsertStmt),
    Update(UpdateStmt),
    Delete(DeleteStmt),
    Explain(SelectStmt),
    Search(SearchStmt),
    Prepare(PrepareStmt),
    Execute(ExecuteStmt),
    Deallocate(String),
    Begin,
    Commit,
    Rollback,
}

impl SqlStmt {
    pub fn resolve_placeholders(&mut self) {
        let mut counter = 0;
        match self {
            SqlStmt::Select(s) => s.resolve_placeholders(&mut counter),
            SqlStmt::Update(u) => u.resolve_placeholders(&mut counter),
            SqlStmt::Delete(d) => d.resolve_placeholders(&mut counter),
            SqlStmt::Explain(s) => s.resolve_placeholders(&mut counter),
            SqlStmt::CreateIndex(ci) => ci.resolve_placeholders(&mut counter),
            SqlStmt::CreateMaterializedView(mv) => mv.query.resolve_placeholders(&mut counter),
            SqlStmt::Insert(i) => i.resolve_placeholders(&mut counter),
            // No placeholders in these statements
            SqlStmt::CreateTable(_)
            | SqlStmt::AlterTable(_)
            | SqlStmt::DropTable(_)
            | SqlStmt::CreateUser(_)
            | SqlStmt::DropUser(_)
            | SqlStmt::Grant(_)
            | SqlStmt::Revoke(_)
            | SqlStmt::Search(_)
            | SqlStmt::Begin
            | SqlStmt::Commit
            | SqlStmt::Rollback
            | SqlStmt::Prepare(_)
            | SqlStmt::Execute(_)
            | SqlStmt::Deallocate(_) => {}
        }
    }
}
