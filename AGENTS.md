# AGENTS.md - Developer Guidelines for thy-squeal

## Project Overview

thy-squeal is a SQL server with HTTP JSON API, built with Rust. It's a Cargo workspace with:
- `server/` - Server binary with Axum HTTP server; in-memory storage; SQL execution (Pest-based parsing)
- `client/` - CLI client with REPL; `--http -e "SQL"` for one-off queries

### Current Implementation Notes
- **SQL parsing**: Uses Pest grammar (`server/src/sql/sql.pest`). Supports SELECT, INSERT, UPDATE, DELETE, CREATE TABLE, DROP TABLE, CREATE INDEX, EXPLAIN, SEARCH, BEGIN, COMMIT, ROLLBACK, PREPARE, EXECUTE, DEALLOCATE.
- **SQL Execution**: Highly modularized executor supporting JOINs, Subqueries, Aggregations, GROUP BY, HAVING, ORDER BY, and LIMIT/OFFSET.
- **Placeholders**: Supports positional (`?`) and named (`$1`) placeholders via `resolve_placeholders` AST pass.
- **Prepared Statements**: Server-side storage of parsed `SqlStmt` ASTs in the `Executor` for efficient reuse.
- **Storage**: Hybrid in-memory storage with Sled-based Write-Ahead Logging (WAL) and snapshotting. Supports B-Tree, Hash, JSON Path, Functional, and Partial indexes.
- **Information Schema**: Provides metadata via virtual `information_schema` tables (tables, columns, indexes).

## Project Structure (Server)

```
server/src/
├── main.rs          # Entry point
├── config.rs        # Configuration
├── http.rs          # Axum HTTP handlers
├── sql/             # SQL Engine
│   ├── ast.rs       # Abstract Syntax Tree
│   ├── error.rs     # SQL Errors
│   ├── sql.pest     # Pest Grammar
│   ├── eval/        # Expression Evaluation
│   │   ├── column.rs     # Scoped Column Resolution
│   │   ├── condition.rs  # WHERE/HAVING filters
│   │   └── expression.rs # Math/Functions/Subqueries
│   ├── executor/    # Statement Execution
│   │   ├── aggregate/    # Grouping and Aggregates
│   │   ├── dml/          # Insert/Update/Delete
│   │   ├── ddl.rs        # Table/Index creation
│   │   ├── select.rs     # SELECT orchestration
│   │   ├── explain.rs    # EXPLAIN plan
│   │   └── search.rs     # Full-text search
│   └── parser/      # Pest-based parsing
│       ├── expr/         # Expression parsing
│       ├── select.rs     # SELECT parsing
│       ├── dml.rs        # INSERT/UPDATE/DELETE parsing
│       └── ddl.rs        # CREATE/DROP parsing
├── storage/         # Storage Engine
│   ├── mod.rs       # Database entry point
│   ├── table.rs     # Table metadata and search index
│   ├── row.rs       # Row and Column definitions
│   ├── index.rs     # BTree/Hash index implementations
│   ├── mutation.rs  # Table mutation logic
│   ├── wal.rs       # WAL recovery and log application
│   ├── persistence.rs # Sled storage backend
│   └── info_schema.rs # Information Schema virtual tables
└── tests/           # Integration tests
```

## Build, Test, and Development Commands

### Workspace Commands
```bash
# Build all binaries
cargo build

# Build specific binary
cargo build -p thy-squeal          # Server
cargo build -p thy-squeal-client   # Client

# Run server (HTTP on port 9200)
cargo run -p thy-squeal

# Run client
cargo run -p thy-squeal-client
```

### Testing
```bash
# Run all tests (29+ integration and unit tests)
cargo test

# Run tests with output
cargo test -- --nocapture
```

### Linting and Formatting
```bash
# Run clippy for linting (Workspace is kept -D warnings clean)
cargo clippy -- -D warnings

# Format code
cargo fmt
```

## Code Style Guidelines

- **Simplicity**: Keep logic focused and modular.
- **Ownership**: Be careful with `DatabaseState` clones during mutation blocks to satisfy the borrow checker.
- **Documentation**: Update relevant Markdown files when changing architecture.
- **Error Handling**: Use `SqlError` and `StorageError` for structured error reporting.
- **Testing**: Add unit tests in `executor/tests/` and integration tests in `tests/`.
