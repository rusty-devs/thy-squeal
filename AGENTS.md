# AGENTS.md - Developer Guidelines for thy-squeal

## Project Overview

thy-squeal is a SQL server with HTTP JSON API, built with Rust. It's a Cargo workspace with:
- `server/` - Server binary with Axum HTTP server; in-memory storage; SQL execution (Pest-based parsing)
- `client/` - CLI client with REPL; `--http -e "SQL"` for one-off queries

### Current Implementation Notes
- **SQL Parsing**: Uses Pest grammar (`server/src/sql/sql.pest`). Maps SQL strings to **Squeal IR**.
- **JSqueal**: Programmatic JSON-based query interface that maps directly to **Squeal IR**, bypassing the parser.
- **Squeal IR**: Unified Internal Representation for all queries, decoupling the surface syntax from execution logic.
- **SQL Execution**: Highly modularized executor processing Squeal IR. Supports JOINs (INNER/LEFT), Subqueries (IN/Correlated), Aggregations, GROUP BY, HAVING, ORDER BY, and LIMIT/OFFSET.
- **Materialized Views**: Support for pre-calculated views with automatic data refresh on source table mutations.
- **Auto-Increment**: Support for `AUTO_INCREMENT` attribute and `SERIAL` data type.
- **MySQL Protocol**: Native TCP support on port 3306.
- **Storage**: Hybrid in-memory storage with Sled-based WAL and snapshotting. Natively uses Squeal IR types, decoupled from the SQL AST.
- **Information Schema**: Provides metadata via virtual `information_schema` tables (tables, columns, indexes).

## Project Structure (Server)

```
server/src/
├── main.rs          # Entry point
├── http.rs          # Axum HTTP handlers (SQL & JSqueal)
├── mysql/           # MySQL Protocol handler
├── squeal/          # Internal Representation (IR)
├── sql/             # SQL Engine
│   ├── ast/         # Decomposed AST definitions
│   ├── error.rs     # SQL Errors
│   ├── eval/        # Modular IR expression evaluation
│   ├── executor/    # Specialized IR statement executors
│   └── parser/      # Modular Pest-based parsing (SQL -> IR)
└── storage/         # Storage Engine (Decoupled from AST)
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
