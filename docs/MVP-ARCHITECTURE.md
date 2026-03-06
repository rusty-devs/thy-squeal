# MVP Architecture Suggestions

This document suggests architecture changes to reach a viable **MVP** — a minimal but usable SQL server over HTTP.

---

## Current Architecture (Summary)

```
Client (--http -e "SQL")  →  POST /_query  →  Executor::execute(sql)
                                                    ↓
                                    Hand-rolled string parsing
                                                    ↓
                                    Database (in-memory HashMap<Table>)
```

**Gaps**: No Pest integration; no WHERE/UPDATE/DELETE at executor level; REPL does not execute SQL; no tests.

---

## Recommended Architecture Changes for MVP

### 1. Wire Pest Parser into the Executor (High Priority)

**Why**: The grammar in `sql.pest` already defines SELECT, INSERT, UPDATE, DELETE, WHERE, ORDER BY, LIMIT. The executor ignores it and uses brittle string matching. Wiring Pest gives you a single source of truth and unlocks WHERE, UPDATE, DELETE, ORDER BY, LIMIT with minimal new logic.

**Steps**:
1. Create `server/src/sql/parser.rs`:
   - Derive `SqlParser` with `#[grammar = "sql.pest"]`
   - Implement `parse(sql: &str) -> Result<SqlAst, ParseError>` returning an enum (e.g. `SelectStmt`, `InsertStmt`, `UpdateStmt`, `DeleteStmt`, `CreateTableStmt`, `DropTableStmt`)
2. Create `server/src/sql/ast.rs`:
   - Define `SqlAst` and statement variants
   - Map Pest parse tree to Rust structs (column refs, expressions, conditions, literals)
3. In `sql/mod.rs`:
   - Replace `sql_upper.starts_with("SELECT")` etc. with `let ast = parser::parse(sql)?`
   - Dispatch `match ast { SelectStmt(...) => self.execute_select(ast).await, ... }`

**Outcome**: Clean separation of parsing and execution; easy to add new statement types by extending grammar and AST.

---

### 2. Add Expression Evaluator and WHERE Filtering

**Why**: `Table::select_where` is a stub. MVP needs row-level filtering.

**Steps**:
1. Add `server/src/sql/expr.rs`:
   - `eval_condition(condition: &Condition, row: &Row, table: &Table) -> bool`
   - Support `=`, `!=`, `<`, `>`, `<=`, `>=`, `IS NULL`, `IS NOT NULL`, `AND`, `OR`
2. In `execute_select`:
   - If `WHERE` is present, iterate rows and keep only those where `eval_condition` is true
3. Optionally: `ORDER BY` (sort rows by column), `LIMIT`/`OFFSET` (slice result)

**Outcome**: `SELECT * FROM users WHERE age > 18` works.

---

### 3. Add UPDATE and DELETE to the Executor

**Why**: Storage already has `Table::update` and `Table::delete`. Executor only needs to parse and dispatch.

**Steps**:
1. Extend AST with `UpdateStmt`, `DeleteStmt`
2. In `execute`:
   - `UpdateStmt { table, set_list, where }` → lock table, find rows matching `where`, apply `set_list`, call `table.update`
   - `DeleteStmt { table, where }` → lock table, find rows matching `where`, call `table.delete`

**Outcome**: Full CRUD at SQL level.

---

### 4. Structured Error Handling

**Why**: Executor returns `Result<QueryResult, String>`. HTTP layer maps everything to `"EXECUTION_ERROR"`. Structured errors improve debuggability and allow clients to distinguish parse vs runtime errors.

**Steps**:
1. Add `server/src/sql/error.rs`:
   - `SqlError { kind: ParseError | SemanticError | RuntimeError, message, position? }`
2. Change `Executor::execute` to `Result<QueryResult, SqlError>`
3. In HTTP handler, map `SqlError` to `QueryError { code: "PARSE_ERROR" | "RUNTIME_ERROR", message, position }`

**Outcome**: Better diagnostics for users and tests.

---

### 5. Wire REPL to Execute SQL

**Why**: REPL exists but prints "(Not implemented yet)" for SQL. MVP should allow interactive use.

**Steps**:
1. In `repl.rs`, when the user enters SQL (non-dot command):
   - Call `http::execute_query(host, port, &sql)` (reuse existing HTTP client)
2. Load `host`/`port` from `config::load_config()` or CLI args passed into REPL
3. Pass connection params from `main.rs` into `repl::start(host, port)`

**Outcome**: Interactive SQL sessions via REPL.

---

### 6. Add Tests

**Why**: No tests today. Regression risk is high as you add Pest, WHERE, UPDATE, DELETE.

**Steps**:
1. Add `server/src/sql/mod.rs` unit tests:
   - `#[cfg(test)] mod tests { ... }` for `Executor::execute` with CREATE TABLE, INSERT, SELECT, SELECT with WHERE, UPDATE, DELETE
2. Add `tests/integration.rs`:
   - Spawn server in background (or use `axum::test`), POST to `/_query`, assert response JSON

**Outcome**: Confidence when refactoring.

---

## What to Defer for MVP

| Feature | Rationale |
|---------|-----------|
| Storage trait / pluggable backends | Concrete `Database` is fine for MVP; add abstraction when you add persistence |
| TCP SQL protocol | HTTP JSON API is enough for MVP |
| REST CRUD endpoints | `POST /_query` covers CRUD via SQL |
| KV store, full-text search | Out of scope for SQL MVP |
| Parameterized queries (?) | Nice for security; can add after MVP if needed |
| GET /_stats | Low priority; add when you have cache/stats to expose |

---

## Suggested MVP Milestone

**Definition of done**:
- [ ] Pest parser wired; executor uses AST
- [ ] WHERE, ORDER BY, LIMIT work for SELECT
- [ ] UPDATE, DELETE work
- [ ] REPL executes SQL over HTTP
- [ ] At least 5–10 tests (unit + integration)
- [ ] Structured errors surfaced in JSON response

**Estimated scope**: Medium. Parser/AST wiring is the bulk; WHERE/UPDATE/DELETE and REPL wiring are incremental.

---

## File Layout After MVP

```
server/src/
├── main.rs
├── config.rs
├── storage/
│   └── mod.rs
├── sql/
│   ├── mod.rs       # Executor, public API
│   ├── parser.rs    # Pest parser, parse() -> SqlAst
│   ├── ast.rs       # SqlAst, SelectStmt, InsertStmt, etc.
│   ├── expr.rs      # eval_condition, eval_expression
│   └── error.rs     # SqlError
└── sql.pest
```
