# AGENTS.md - Developer Guidelines for thy-squeal

## Project Overview

thy-squeal is a SQL server with HTTP JSON API, built with Rust. It's a Cargo workspace with:
- `server/` - Server binary with Axum HTTP server; in-memory storage; SQL execution (Pest-based parsing)
- `client/` - CLI client with REPL; `--http -e "SQL"` for one-off queries

### Current Implementation Notes
- **SQL parsing**: Uses Pest grammar (`server/src/sql/sql.pest`). Supports SELECT, INSERT, UPDATE, DELETE, CREATE TABLE, ALTER TABLE, DROP TABLE, CREATE INDEX, EXPLAIN, SEARCH, BEGIN, COMMIT, ROLLBACK, PREPARE, EXECUTE, DEALLOCATE.
- **SQL Execution**: Highly modularized executor supporting JOINs (INNER/LEFT), Subqueries (IN/Correlated), Aggregations, GROUP BY, HAVING, ORDER BY, and LIMIT/OFFSET.
- **Auto-Increment**: Support for `AUTO_INCREMENT` attribute and `SERIAL` data type.
- **MySQL Protocol**: Native TCP support on port 3306.
- **Storage**: Hybrid in-memory storage with Sled-based Write-Ahead Logging (WAL) and snapshotting.
- **Information Schema**: Provides metadata via virtual `information_schema` tables (tables, columns, indexes).

## Project Structure (Server)

```
server/src/
├── main.rs          # Entry point
├── http.rs          # Axum HTTP handlers
├── mysql/           # MySQL Protocol handler
├── sql/             # SQL Engine
│   ├── ast/         # Decomposed AST definitions
│   ├── error.rs     # SQL Errors
│   ├── eval/        # Modular expression evaluation
│   ├── executor/    # Specialized statement executors
│   └── parser/      # Modular Pest-based parsing
└── storage/         # Storage Engine
    ├── database.rs  # Database state management
    ├── table/       # Modular table, index, and mutation logic
    └── value/       # Modular data type handling
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
