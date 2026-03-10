# MVP Architecture Suggestions

This document reflects the current architecture of **thy-squeal**, reaching beyond the initial MVP goals into a highly modular and robust SQL server.

---

## Current Architecture (Summary)

```
Client (CLI/REPL)  →  POST /_query  →  Executor::execute(sql)
                                              ↓
                              Pest-based SQL Parser (sql/parser/)
                                              ↓
                              SQL Executor (sql/executor/)
                                              ↓
                              Storage Engine (storage/)
```

---

## Architectural Pillars

### 1. Modular SQL Engine
**Outcome**: Clean separation of parsing, evaluation, and execution.
- **Parser**: Split into statement-specific modules (`ddl`, `dml`, `select`) and expression parsing (`expr/`).
- **Evaluator**: Dedicated modules for column resolution, condition filtering, and expression evaluation.
- **Executor**: Highly decomposed into command-specific handlers, including specialized logic for aggregation, joins, and search.
- **Prepared Statements**: Efficient server-side query caching within the `Executor` via AST storage.

---

### 2. Robust Storage & Indexing
**Outcome**: High-performance in-memory storage with durable persistence.
- **Indexes**: Supports B-Tree and Hash indexes, including advanced features like JSON path, functional, and partial indexing.
- **Durability**: Synchronous Write-Ahead Logging (WAL) ensures data integrity across restarts.
- **Information Schema**: System metadata exposed via standard SQL queries.

---

### 3. ACID Transactions
**Outcome**: Atomicity and Isolation for complex operations.
- Uses `DatabaseState` snapshotting for transactional isolation.
- WAL logging for atomic `COMMIT` / `ROLLBACK` support.

---

## File Layout (Current)

```
server/src/
├── main.rs          # Server Entry Point
├── config.rs        # Configuration Management
├── http.rs          # Axum HTTP API Handlers
├── sql/             # SQL Engine
│   ├── ast.rs       # Abstract Syntax Tree
│   ├── eval/        # Runtime Evaluation (Modular)
│   │   ├── column.rs
│   │   ├── condition.rs
│   │   └── expression.rs
│   ├── executor/    # Statement Execution (Modular)
│   │   ├── aggregate/    # Grouping/Aggregates
│   │   ├── dml/          # Insert/Update/Delete
│   │   ├── select.rs     # SELECT logic
│   │   └── tests/        # Unit tests by feature
│   └── parser/      # Pest Parser (Modular)
└── storage/         # Storage Engine
    ├── mod.rs       # Database Entry Point
    ├── table.rs     # Table Metadata
    ├── row.rs       # Data Structures
    ├── index.rs     # Indexing Logic
    ├── mutation.rs  # Update/Delete logic
    ├── wal.rs       # WAL Management
    └── info_schema.rs # Metadata Tables
```

---

## Next Steps

| Feature | Status | Description |
|---------|--------|-------------|
| SQL Dump/Restore | ✅ Done | Export/Import database state as .sql scripts |
| MySQL Protocol | ✅ Done | Support standard MySQL clients over TCP port 3306 |
| Parameterized Queries | ✅ Done | Prevention of SQL injection and query reuse |
| AUTO_INCREMENT | ✅ Done | Automated ID generation for integer columns |
| ALTER TABLE | ✅ Done | Non-destructive schema evolution |
| Query Optimization | 🏗 Todo | Cost-based optimizer for join ordering |
