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

### 1. Wire Pest Parser into the Executor (Completed)

**Outcome**: Clean separation of parsing and execution; Pest grammar is used for all operations.

---

### 2. Add Expression Evaluator and WHERE Filtering (Completed)

**Outcome**: `SELECT * FROM users WHERE age > 18` works with basic operators.

---

### 3. Add UPDATE and DELETE to the Executor (Completed)

**Outcome**: Full CRUD at SQL level.

---

### 4. Structured Error Handling (Pending)

**Why**: Executor returns `Result<QueryResult, String>`. HTTP layer maps everything to `"EXECUTION_ERROR"`. Structured errors improve debuggability and allow clients to distinguish parse vs runtime errors.

**Steps**:
1. Add `server/src/sql/error.rs`:
   - `SqlError { kind: ParseError | SemanticError | RuntimeError, message, position? }`
2. Change `Executor::execute` to `Result<QueryResult, SqlError>`
3. In HTTP handler, map `SqlError` to `QueryError { code: "PARSE_ERROR" | "RUNTIME_ERROR", message, position }`

**Outcome**: Better diagnostics for users and tests.

---

### 5. Wire REPL to Execute SQL (Pending)

**Why**: REPL exists but prints "(Not implemented yet)" for SQL. MVP should allow interactive use.

**Steps**:
1. In `repl.rs`, when the user enters SQL (non-dot command):
   - Call `http::execute_query(host, port, &sql)` (reuse existing HTTP client)
2. Load `host`/`port` from `config::load_config()` or CLI args passed into REPL
3. Pass connection params from `main.rs` into `repl::start(host, port)`

**Outcome**: Interactive SQL sessions via REPL.

---

### 6. Add Tests (Partially Completed)

**Why**: No tests initially. Added unit tests for Executor in `server/src/sql/mod.rs`.

**Steps**:
1. [x] Add `server/src/sql/mod.rs` unit tests for CREATE TABLE, INSERT, SELECT, SELECT with WHERE, UPDATE, DELETE.
2. [ ] Add `tests/integration.rs`: Spawn server in background (or use `axum::test`), POST to `/_query`, assert response JSON.

**Outcome**: Confidence when refactoring.

---

## Suggested MVP Milestone

**Definition of done**:
- [x] Pest parser wired; executor uses AST
- [x] WHERE works for SELECT, UPDATE, DELETE
- [ ] ORDER BY, LIMIT work for SELECT
- [ ] REPL executes SQL over HTTP
- [x] Unit tests for SQL operations
- [ ] Integration tests
- [ ] Structured errors surfaced in JSON response

**Estimated scope**: Low. Remaining tasks are ORDER BY/LIMIT, REPL wiring, and integration tests.

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
