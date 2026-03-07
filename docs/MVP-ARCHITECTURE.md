# MVP Architecture Suggestions

This document suggests architecture changes to reach a viable **MVP** — a minimal but usable SQL server over HTTP.

---

## Current Architecture (Summary)

```
Client (--http -e "SQL")  →  POST /_query  →  Executor::execute(sql)
                                                    ↓
                                    Pest-based SQL Parser (sql.pest)
                                                    ↓
                                    Database (in-memory HashMap<Table>)
```

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

### 4. Structured Error Handling (Completed)

**Outcome**: Better diagnostics for users and tests via `SqlError` enum.

---

### 5. Wire REPL to Execute SQL (Completed)

**Outcome**: Interactive SQL sessions via REPL.

---

### 6. Add Tests (Partially Completed)

**Why**: No tests initially. Added unit tests for Executor in `server/src/sql/mod.rs`.

**Steps**:
1. [x] Add `server/src/sql/mod.rs` unit tests for CREATE TABLE, INSERT, SELECT, SELECT with WHERE, UPDATE, DELETE.
2. [ ] Add `tests/integration.rs`: Spawn server in background (or use `axum::test`), POST to `/_query`, assert response JSON.

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
- [x] Pest parser wired; executor uses AST
- [x] WHERE works for SELECT, UPDATE, DELETE
- [x] ORDER BY, LIMIT work for SELECT
- [x] REPL executes SQL over HTTP
- [x] Unit tests for SQL operations
- [x] Integration tests
- [x] Structured errors surfaced in JSON response

**Estimated scope**: Low. Remaining tasks are integration tests and finishing Phase 2/3 features.

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
│   ├── eval.rs      # evaluate_condition, evaluate_expression
│   └── error.rs     # SqlError
└── sql.pest
```
